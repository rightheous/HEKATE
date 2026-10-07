from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Mapping

from hekate.application import evidence as evidence_app
from hekate.application.operations import record_dispatch_accepted, record_dispatch_send_intent
from hekate.application.results import process_business_result_inbox, process_pending_results, receive_business_result, receive_missing_business_result
from hekate.application.lifecycle import (
    confirm_deletion, create_from_intent, maintain_critic_workflows, maintain_deliberation_steps,
)
from hekate.application.runtime_inbox import InboxBinding, RuntimeInboxPayload, process_runtime_observation
from hekate.application.projections import claim_pending_projections, project_position
from hekate.application.tasks import process_pending_execution_tasks
from hekate.application.turns import prepare_critic_workflow_steps, prepare_deliberation_steps, prepare_queued_tasks
from hekate.domain.bridge_contracts import BridgeEvent
from hekate.domain.contracts import canonical_json
from hekate.domain.errors import PolicyDenied, StaleInput
from hekate.domain.models import ExecutionEnvelope, Lease, OutboxJob, RuntimeBinding
from hekate.domain.types import AttemptId, OperationId, ProviderAgentId, RegistryId, ScopeId, TaskId
from hekate.infrastructure.letta.adapter import LettaRuntimeAdapter
from hekate.ports.store import UowFactory
from hekate.settings import configured_critic_execution, configured_deliberation, configured_local_actor, configured_task_execution
from hekate.bootstrap import Container

_LOG = logging.getLogger(__name__)
CLAIM_BATCH = 4
CLAIM_LEASE_SECONDS = 30
EVIDENCE_MAINTENANCE_INTERVAL_SECONDS = 60
EVIDENCE_MAINTENANCE_BATCH = 100
REGISTRY_LEASE_SECONDS = 45
HEARTBEAT_SECONDS = 10
SHUTDOWN_DRAIN_SECONDS = 5


def _binding(value: Mapping[str, object]) -> RuntimeBinding:
    return RuntimeBinding(
        task_id=TaskId(str(value["task_id"])),
        attempt_id=AttemptId(str(value["attempt_id"])),
        agent_registry_id=RegistryId(str(value["agent_registry_id"])),
        provider_agent_id=ProviderAgentId(str(value["provider_agent_id"])),
        conversation_id=str(value["conversation_id"]),
        input_revision=int(value["input_revision"]),
        fence=int(value["fence"]),
    )


async def _record_execution(
    container: Container,
    binding: RuntimeBinding,
    operation_id: OperationId,
    worker: str,
    state: str,
    source: str,
    *,
    outcome: str | None = None,
    reason: str | None = None,
) -> None:
    payload = RuntimeInboxPayload(
        event_type="execution",
        operation_id=str(operation_id),
        accounting_call_id=f"execution:{operation_id}",
        source=source,
        observation_identity=f"execution:{operation_id}:{state.lower()}:{outcome or 'none'}",
        binding=InboxBinding.from_binding(binding),
        state=state,
        outcome=outcome,
        reason=reason,
        lease_owner=worker,
        observer_fence=binding.fence,
    )
    await process_runtime_observation(
        container.uow_factory, "letta-bridge", payload.observation_identity, payload,
        processor_owner=worker,
    )


async def _collect_turn(
    container: Container, binding: RuntimeBinding, operation_id: OperationId, worker: str,
    deadline: datetime, hekate_config=None, critic_config=None, deliberation_config=None,
) -> None:
    runtime: LettaRuntimeAdapter = container.runtime
    saw_business_result = False
    while datetime.now(UTC) < deadline:
        result = await runtime.collect(binding, operation_id, wait_ms=500)
        for value in result["events"]:
            if not isinstance(value, dict):
                raise ValueError("bridge event is not an object")
            event_binding = value.get("binding")
            expected_binding = {
                "task_id": str(binding.task_id),
                "attempt_id": str(binding.attempt_id),
                "agent_registry_id": str(binding.agent_registry_id),
                "provider_agent_id": str(binding.provider_agent_id),
                "conversation_id": binding.conversation_id,
                "input_revision": binding.input_revision,
                "fence": binding.fence,
            }
            if event_binding != expected_binding:
                raise ValueError("bridge event binding differs from admitted runtime")
            if value.get("event_type") == "business_result":
                event = BridgeEvent.model_validate(value, strict=True)
                if event.operation_id != str(operation_id):
                    raise ValueError("business result operation differs from admitted runtime")
                await receive_business_result(
                    container.uow_factory, event,
                    hekate_config=hekate_config, critic_config=critic_config,
                    deliberation_config=deliberation_config,
                )
                saw_business_result = True
            usage = value.get("usage")
            if isinstance(usage, dict) and usage.get("accounting_call_id"):
                cost = usage.get("cost_usd")
                usage_value = {
                    "source": usage.get("source") or "runtime_reported",
                    "completeness": usage.get("completeness", "UNKNOWN"),
                    "input_tokens": usage.get("input_tokens"),
                    "output_tokens": usage.get("output_tokens"),
                    "total_tokens": usage.get("total_tokens"),
                    **({"reported_cost_usd": cost} if isinstance(cost, str) else {}),
                }
                payload = RuntimeInboxPayload(
                    event_type="runtime_usage",
                    operation_id=str(operation_id),
                    accounting_call_id=str(usage["accounting_call_id"]),
                    source="bridge_event",
                    observation_identity=str(value["event_id"]),
                    binding=InboxBinding.from_binding(binding),
                    provider_call_id=usage.get("provider_call_id"),
                    usage=usage_value,
                )
                await process_runtime_observation(
                    container.uow_factory,
                    "letta-bridge",
                    f"runtime:{operation_id}:{value['event_id']}",
                    payload,
                )

        state = result.get("state")
        if state == "RUNNING":
            await _record_execution(container, binding, operation_id, worker, "RUNNING", "bridge_running")
            continue
        if state in {"COMPLETE", "FAILED"}:
            await _record_execution(
                container, binding, operation_id, worker, "QUIESCENT", "bridge_terminal",
                outcome="SUCCEEDED" if state == "COMPLETE" else "FAILED",
            )
            if not saw_business_result:
                await receive_missing_business_result(
                    container.uow_factory, operation_id,
                    {
                        "task_id": str(binding.task_id), "attempt_id": str(binding.attempt_id),
                        "agent_registry_id": str(binding.agent_registry_id),
                        "provider_agent_id": str(binding.provider_agent_id),
                        "conversation_id": binding.conversation_id,
                        "input_revision": binding.input_revision, "fence": binding.fence,
                    },
                    "structured_output_error" if state == "COMPLETE" else "error",
                    hekate_config=hekate_config, critic_config=critic_config,
                    deliberation_config=deliberation_config,
                )
            return
        await _record_execution(
            container, binding, operation_id, worker, "UNKNOWN", "bridge_disconnect",
            reason="bridge_returned_no_terminal_execution_evidence",
        )
        return
    await _record_execution(
        container, binding, operation_id, worker, "UNKNOWN", "bridge_disconnect",
        reason="operation_deadline_elapsed_without_terminal_evidence",
    )


async def dispatch_job(
    container: Container, job: OutboxJob, worker: str, hekate_config=None, critic_config=None,
    deliberation_config=None,
) -> None:
    binding = _binding(job.payload["binding"])
    operation_id = job.operation_id
    try:
        await record_dispatch_send_intent(container.uow_factory, job, worker)
    except (PolicyDenied, StaleInput) as error:
        # A send-intent rejection proves this outbox row never crossed the
        # provider boundary. Converge its local attempt and release only
        # unconsumed allocations; the Task repository decides whether the
        # current revision is still eligible for a policy failure.
        reason = str(error)[:240] or "dispatch_policy_rejected"
        try:
            await _record_execution(
                container, binding, operation_id, worker, "QUIESCENT", "pre_dispatch_policy",
                outcome="FAILED", reason=reason,
            )
            if job.claim_fence is not None:
                async with container.uow_factory() as uow:
                    await uow.delivery.ack_job(job, worker, job.claim_fence)
                    await uow.commit()
        except Exception as convergence_error:
            _LOG.error(
                "dispatch rejection did not converge for %s: %s: %s",
                operation_id, type(convergence_error).__name__, str(convergence_error)[:240],
            )
        return
    except Exception as error:
        _LOG.error("send intent was not confirmed for operation %s: %s: %s", operation_id, type(error).__name__, str(error)[:240])
        return
    payload = job.payload.get("payload")
    if not isinstance(payload, dict):
        await _record_execution(container, binding, operation_id, worker, "UNKNOWN", "bridge_disconnect", reason="dispatch_payload_malformed")
        return
    message = payload.get("message", payload.get("prompt", payload.get("objective")))
    if not isinstance(message, str) or not message:
        await _record_execution(container, binding, operation_id, worker, "UNKNOWN", "bridge_disconnect", reason="dispatch_message_missing")
        return
    try:
        envelope = ExecutionEnvelope.model_validate_json(canonical_json(job.payload["envelope"]), strict=True)
        result = await container.runtime.start_turn(binding, {"message": message}, envelope)
        if not result.get("accepted") or result.get("state") != "DISPATCHED":
            await _record_execution(container, binding, operation_id, worker, "UNKNOWN", "bridge_disconnect", reason="bridge_acceptance_unconfirmed")
            return
        await record_dispatch_accepted(container.uow_factory, job, worker)
        await _collect_turn(
            container, binding, operation_id, worker, envelope.deadline,
            hekate_config, critic_config, deliberation_config,
        )
    except asyncio.CancelledError:
        try:
            await asyncio.shield(_record_execution(
                container, binding, operation_id, worker, "UNKNOWN", "bridge_disconnect", reason="worker_shutdown_during_dispatch",
            ))
        except Exception:
            pass
        raise
    except Exception as error:
        _LOG.error("dispatch outcome unknown for operation %s: %s: %s", operation_id, type(error).__name__, str(error)[:240])
        try:
            await _record_execution(
                container, binding, operation_id, worker, "UNKNOWN", "bridge_disconnect",
                reason="bridge_or_database_outcome_unconfirmed",
            )
        except Exception:
            # The committed send intent itself blocks outbox redelivery if the database is unavailable.
            _LOG.error("could not persist UNKNOWN for operation %s", operation_id)


async def heartbeat_leases(factory: UowFactory, worker: str, active: dict[str, Lease], stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), HEARTBEAT_SECONDS)
            return
        except TimeoutError:
            pass
        for registry_id, lease in tuple(active.items()):
            async with factory() as uow:
                renewed = await uow.agents.renew_lease(Lease(
                    registry_id=lease.registry_id,
                    owner=worker,
                    fence=lease.fence,
                    expires_at=datetime.now(UTC) + timedelta(seconds=REGISTRY_LEASE_SECONDS),
                ))
                await uow.commit()
            active[registry_id] = renewed


async def process_pending_inbox(
    factory: UowFactory, limit: int = 100, processor_owner: str | None = None,
    *, hekate_config=None, critic_config=None, deliberation_config=None,
    owner_scope: ScopeId | None = None,
) -> int:
    async with factory() as uow:
        rows = await uow.delivery.pending_inbox(limit, owner_scope)
        await uow.commit()
    processed = 0
    for row in rows:
        result = await process_inbox_row(
            factory, row, processor_owner=processor_owner,
            hekate_config=hekate_config, critic_config=critic_config,
            deliberation_config=deliberation_config,
        )
        if result["processed"]:
            processed += 1
    return processed


async def process_inbox_row(
    factory: UowFactory, row: Mapping[str, object], *, processor_owner: str | None = None,
    hekate_config=None, critic_config=None, deliberation_config=None,
) -> dict[str, object]:
    payload = row["payload"]
    if payload.get("event_type") == "business_result":
        return await process_business_result_inbox(
            factory, row["id"], hekate_config=hekate_config, critic_config=critic_config,
            deliberation_config=deliberation_config,
        )
    if processor_owner and payload.get("event_type") == "execution" and payload.get("state") == "QUIESCENT":
        binding = payload["binding"]
        async with factory() as uow:
            lease = await uow.agents.acquire_lease(
                RegistryId(binding["agent_registry_id"]), processor_owner, REGISTRY_LEASE_SECONDS,
            )
            if lease is None:
                await uow.delivery.defer_inbox(row["id"], "processor_lease_unavailable")
                await uow.commit()
                return {"inbox_id": row["id"], "processed": False, "pending_reason": "processor_lease_unavailable"}
            await uow.commit()
        from hekate.application.runtime_inbox import _apply_terminal_inbox

        return await _apply_terminal_inbox(factory, row["id"], processor_owner)
    return await process_runtime_observation(
        factory, row["provider_scope"], row["stable_event_key"], payload,
        processor_owner=processor_owner,
    )


async def _process_lifecycle_job(container: Container, job: OutboxJob, worker: str) -> None:
    try:
        if job.kind == "critic_create":
            await create_from_intent(container.uow_factory, container.runtime, job, worker)
        elif job.kind == "critic_delete":
            await confirm_deletion(container.uow_factory, container.runtime, job, worker)
        else:
            raise ValueError("unsupported Critic lifecycle outbox kind")
    except asyncio.CancelledError:
        raise
    except Exception as error:
        _LOG.warning(
            "Critic lifecycle job %s remains pending: %s: %s",
            job.operation_id, type(error).__name__, str(error)[:240],
        )
        if job.claim_fence is not None:
            try:
                async with container.uow_factory() as uow:
                    await uow.delivery.reschedule_job(
                        job, worker, job.claim_fence, type(error).__name__, 10,
                    )
                    await uow.commit()
            except Exception as reschedule_error:
                _LOG.error(
                    "Critic lifecycle reschedule failed for %s: %s",
                    job.operation_id, type(reschedule_error).__name__,
                )


async def run_evidence_maintenance_tick(
    factory: UowFactory, archive_root: Path, *, now: datetime | None = None,
) -> dict[str, object]:
    return await evidence_app.expire(
        factory, archive_root, now or datetime.now(UTC), EVIDENCE_MAINTENANCE_BATCH,
    )


async def run_worker(container: Container, stop_event: asyncio.Event) -> None:
    await container.runtime.verify_compatibility()
    worker = container.settings.worker_id
    actor = None
    execution_config = None
    try:
        actor = configured_local_actor(container.settings)
    except ValueError as error:
        _LOG.warning("local worker identity is unavailable: %s", str(error)[:240])
    if actor is not None:
        try:
            execution_config = configured_task_execution(container.settings)
        except ValueError as error:
            _LOG.warning("new Task preparation is disabled: %s", str(error)[:240])
    try:
        critic_config = configured_critic_execution(container.settings)
    except ValueError as error:
        critic_config = None
        _LOG.error("new Critic spawns are disabled: %s", str(error)[:240])
    try:
        deliberation_config = configured_deliberation(container.settings)
    except ValueError as error:
        deliberation_config = None
        _LOG.error("bounded deliberation is disabled: %s", str(error)[:240])
    await process_pending_inbox(
        container.uow_factory, processor_owner=worker,
        hekate_config=execution_config, critic_config=critic_config,
        deliberation_config=deliberation_config,
    )
    try:
        await maintain_deliberation_steps(container.uow_factory)
    except Exception as error:
        _LOG.error("standalone deliberation maintenance failed: %s: %s", type(error).__name__, str(error)[:240])
    await process_pending_execution_tasks(container.uow_factory)
    active: dict[str, Lease] = {}
    heartbeat_stop = asyncio.Event()
    heartbeat = asyncio.create_task(heartbeat_leases(container.uow_factory, worker, active, heartbeat_stop))
    heartbeat.add_done_callback(lambda task: stop_event.set() if not task.cancelled() and task.exception() else None)
    dispatches: set[asyncio.Task[None]] = set()
    lifecycle_jobs: set[asyncio.Task[None]] = set()
    projection_jobs: set[asyncio.Task[None]] = set()
    next_maintenance = 0.0
    next_evidence_maintenance = 0.0
    next_projection_maintenance = 0.0
    try:
        while not stop_event.is_set():
            now = asyncio.get_running_loop().time()
            if now >= next_projection_maintenance:
                if actor is not None and container.settings.memory_projection_enabled:
                    try:
                        projection_capacity = max(0, CLAIM_BATCH - len(projection_jobs))
                        if projection_capacity:
                            claimed = await claim_pending_projections(
                                container.uow_factory, actor, worker,
                                enabled=True, limit=projection_capacity,
                            )
                            projection_jobs.update(
                                asyncio.create_task(project_position(
                                    container.uow_factory, container.runtime, actor, job,
                                ))
                                for job in claimed
                            )
                    except Exception as error:
                        _LOG.error("Position memory projection maintenance failed: %s: %s", type(error).__name__, str(error)[:240])
                next_projection_maintenance = now + 1.0
            if now >= next_maintenance:
                try:
                    await process_pending_inbox(
                        container.uow_factory, processor_owner=worker,
                        hekate_config=execution_config, critic_config=critic_config,
                        deliberation_config=deliberation_config,
                        owner_scope=actor.scope if actor is not None else None,
                    )
                    await process_pending_results(
                        container.uow_factory, hekate_config=execution_config,
                        critic_config=critic_config, deliberation_config=deliberation_config,
                        owner_scope=actor.scope if actor is not None else None,
                    )
                    try:
                        await maintain_deliberation_steps(container.uow_factory)
                    except Exception as error:
                        _LOG.error("standalone deliberation maintenance failed: %s: %s", type(error).__name__, str(error)[:240])
                    await process_pending_execution_tasks(container.uow_factory)
                    await maintain_critic_workflows(container.uow_factory)
                except Exception as error:
                    _LOG.error("pending result processing failed: %s: %s", type(error).__name__, str(error)[:240])
                if actor is not None and execution_config is not None:
                    try:
                        prepared_leases = await prepare_queued_tasks(
                            container.uow_factory, container.runtime, actor, execution_config, worker,
                            deliberation_config,
                            archive_root=container.settings.archive_dir,
                        )
                        active.update({str(lease.registry_id): lease for lease in prepared_leases})
                    except Exception as error:
                        _LOG.error("queued Task preparation failed: %s: %s", type(error).__name__, str(error)[:240])
                if actor is not None:
                    try:
                        prepared_leases = await prepare_critic_workflow_steps(
                            container.uow_factory, container.runtime, actor, execution_config,
                            critic_config, worker, deliberation_config,
                            archive_root=container.settings.archive_dir,
                        )
                        active.update({str(lease.registry_id): lease for lease in prepared_leases})
                    except Exception as error:
                        _LOG.error("Critic workflow preparation failed: %s: %s", type(error).__name__, str(error)[:240])
                    try:
                        prepared_leases = await prepare_deliberation_steps(
                            container.uow_factory, container.runtime, actor, execution_config,
                            critic_config, deliberation_config, worker,
                            archive_root=container.settings.archive_dir,
                        )
                        active.update({str(lease.registry_id): lease for lease in prepared_leases})
                    except Exception as error:
                        _LOG.error("deliberation preparation failed: %s: %s", type(error).__name__, str(error)[:240])
                next_maintenance = now + 1.0
            if now >= next_evidence_maintenance:
                try:
                    cleanup = await run_evidence_maintenance_tick(
                        container.uow_factory, container.settings.archive_dir,
                    )
                    if cleanup["failed_artifacts"]:
                        _LOG.warning(
                            "Evidence archive cleanup failed for %d artifact(s)",
                            len(cleanup["failed_artifacts"]),
                        )
                except Exception as error:
                    _LOG.error("Evidence maintenance failed: %s: %s", type(error).__name__, str(error)[:240])
                next_evidence_maintenance = now + EVIDENCE_MAINTENANCE_INTERVAL_SECONDS
            dispatches = {task for task in dispatches if not task.done()}
            lifecycle_jobs = {task for task in lifecycle_jobs if not task.done()}
            projection_jobs = {task for task in projection_jobs if not task.done()}
            if len(lifecycle_jobs) < CLAIM_BATCH:
                async with container.uow_factory() as uow:
                    jobs = await uow.delivery.claim_lifecycle_jobs(
                        worker, CLAIM_BATCH - len(lifecycle_jobs), CLAIM_LEASE_SECONDS,
                    )
                    await uow.commit()
                lifecycle_jobs.update(
                    asyncio.create_task(_process_lifecycle_job(container, job, worker))
                    for job in jobs
                )
            if len(dispatches) < CLAIM_BATCH:
                async with container.uow_factory() as uow:
                    jobs = await uow.delivery.claim_jobs(worker, CLAIM_BATCH - len(dispatches), CLAIM_LEASE_SECONDS)
                    await uow.commit()
                for job in jobs:
                    value = job.payload.get("binding")
                    if isinstance(value, dict):
                        registry_id = str(value["agent_registry_id"])
                        lease = await _read_lease(container.uow_factory, RegistryId(registry_id))
                        if lease is not None and lease.owner == worker and lease.fence == int(value["fence"]):
                            active[registry_id] = lease
                    task = asyncio.create_task(_dispatch_and_release(
                        container, job, worker, active, execution_config, critic_config,
                        deliberation_config,
                    ))
                    dispatches.add(task)
            try:
                await asyncio.wait_for(stop_event.wait(), 0.25)
            except TimeoutError:
                pass
    finally:
        deadline = datetime.now(UTC) + timedelta(seconds=SHUTDOWN_DRAIN_SECONDS)
        await drain_shutdown(dispatches, deadline)
        await drain_shutdown(lifecycle_jobs, deadline)
        await drain_shutdown(projection_jobs, deadline)
        heartbeat_stop.set()
        heartbeat.cancel()
        await asyncio.gather(heartbeat, return_exceptions=True)
        active.clear()


async def _dispatch_and_release(
    container: Container, job: OutboxJob, worker: str, active: dict[str, Lease],
    hekate_config=None, critic_config=None, deliberation_config=None,
) -> None:
    value = job.payload.get("binding")
    registry_key = str(value["agent_registry_id"]) if isinstance(value, dict) else ""
    try:
        await dispatch_job(container, job, worker, hekate_config, critic_config, deliberation_config)
    finally:
        lease = active.get(registry_key)
        if lease is not None and lease.owner == worker:
            try:
                async with container.uow_factory() as uow:
                    await uow.agents.release_lease(lease)
                    await uow.commit()
            except Exception as error:
                _LOG.error("registry lease release failed for %s: %s", registry_key, type(error).__name__)
            active.pop(registry_key, None)


async def _read_lease(factory: UowFactory, registry_id: RegistryId) -> Lease | None:
    async with factory() as uow:
        lease = await uow.agents.get_lease(registry_id)
        await uow.commit()
        return lease


async def drain_shutdown(dispatches: set[asyncio.Task[None]], deadline: datetime) -> None:
    if not dispatches:
        return
    remaining = max(0, (deadline - datetime.now(UTC)).total_seconds())
    done, pending = await asyncio.wait(dispatches, timeout=remaining)
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)
