from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Mapping

from hekate.application.operations import record_dispatch_accepted, record_dispatch_send_intent
from hekate.application.results import process_business_result_inbox, process_pending_results, receive_business_result, receive_missing_business_result
from hekate.application.runtime_inbox import InboxBinding, RuntimeInboxPayload, process_runtime_observation
from hekate.application.tasks import process_pending_execution_tasks
from hekate.application.turns import prepare_queued_tasks
from hekate.domain.bridge_contracts import BridgeEvent
from hekate.domain.contracts import canonical_json
from hekate.domain.errors import PolicyDenied
from hekate.domain.models import ExecutionEnvelope, Lease, OutboxJob, RuntimeBinding
from hekate.domain.types import AttemptId, OperationId, ProviderAgentId, RegistryId, TaskId
from hekate.infrastructure.letta.adapter import LettaRuntimeAdapter
from hekate.ports.store import UowFactory
from hekate.settings import configured_local_actor, configured_task_execution
from hekate.bootstrap import Container

_LOG = logging.getLogger(__name__)
CLAIM_BATCH = 4
CLAIM_LEASE_SECONDS = 30
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


async def _collect_turn(container: Container, binding: RuntimeBinding, operation_id: OperationId, worker: str, deadline: datetime) -> None:
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
                await receive_business_result(container.uow_factory, event)
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


async def dispatch_job(container: Container, job: OutboxJob, worker: str) -> None:
    binding = _binding(job.payload["binding"])
    operation_id = job.operation_id
    try:
        await record_dispatch_send_intent(container.uow_factory, job, worker)
    except PolicyDenied as error:
        if str(error) in {"evidence_reference_unavailable", "authorization_snapshot_changed", "context_manifest_missing"}:
            await _record_execution(
                container, binding, operation_id, worker, "QUIESCENT", "pre_dispatch_policy",
                outcome="FAILED", reason="evidence_reference_unavailable",
            )
            if job.claim_fence is not None:
                async with container.uow_factory() as uow:
                    await uow.delivery.ack_job(job, worker, job.claim_fence)
                    await uow.commit()
            return
        _LOG.error("dispatch policy denied for operation %s: %s", operation_id, str(error)[:240])
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
        await _collect_turn(container, binding, operation_id, worker, envelope.deadline)
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


async def process_pending_inbox(factory: UowFactory, limit: int = 100, processor_owner: str | None = None) -> int:
    async with factory() as uow:
        rows = await uow.delivery.pending_inbox(limit)
        await uow.commit()
    processed = 0
    for row in rows:
        payload = row["payload"]
        if payload.get("event_type") == "business_result":
            result = await process_business_result_inbox(factory, row["id"])
        elif processor_owner and payload.get("event_type") == "execution" and payload.get("state") == "QUIESCENT":
            binding = payload["binding"]
            async with factory() as uow:
                lease = await uow.agents.acquire_lease(
                    RegistryId(binding["agent_registry_id"]), processor_owner, REGISTRY_LEASE_SECONDS,
                )
                if lease is None:
                    await uow.delivery.defer_inbox(row["id"], "processor_lease_unavailable")
                    await uow.commit()
                    continue
                await uow.commit()
            from hekate.application.runtime_inbox import _apply_terminal_inbox

            result = await _apply_terminal_inbox(factory, row["id"], processor_owner)
        else:
            result = await process_runtime_observation(
                factory, row["provider_scope"], row["stable_event_key"], payload,
                processor_owner=processor_owner,
            )
        if result["processed"]:
            processed += 1
    return processed


async def run_worker(container: Container, stop_event: asyncio.Event) -> None:
    await container.runtime.verify_compatibility()
    worker = container.settings.worker_id
    try:
        actor = configured_local_actor(container.settings)
        execution_config = configured_task_execution(container.settings)
    except ValueError as error:
        actor = None
        execution_config = None
        _LOG.warning("queued Task preparation is disabled: %s", str(error)[:240])
    await process_pending_inbox(container.uow_factory, processor_owner=worker)
    await process_pending_execution_tasks(container.uow_factory)
    active: dict[str, Lease] = {}
    heartbeat_stop = asyncio.Event()
    heartbeat = asyncio.create_task(heartbeat_leases(container.uow_factory, worker, active, heartbeat_stop))
    heartbeat.add_done_callback(lambda task: stop_event.set() if not task.cancelled() and task.exception() else None)
    dispatches: set[asyncio.Task[None]] = set()
    next_maintenance = 0.0
    try:
        while not stop_event.is_set():
            now = asyncio.get_running_loop().time()
            if now >= next_maintenance:
                try:
                    await process_pending_inbox(container.uow_factory, processor_owner=worker)
                    await process_pending_results(container.uow_factory)
                    await process_pending_execution_tasks(container.uow_factory)
                except Exception as error:
                    _LOG.error("pending result processing failed: %s: %s", type(error).__name__, str(error)[:240])
                if actor is not None and execution_config is not None:
                    try:
                        prepared_leases = await prepare_queued_tasks(
                            container.uow_factory, container.runtime, actor, execution_config, worker,
                            archive_root=container.settings.archive_dir,
                        )
                        active.update({str(lease.registry_id): lease for lease in prepared_leases})
                    except Exception as error:
                        _LOG.error("queued Task preparation failed: %s: %s", type(error).__name__, str(error)[:240])
                next_maintenance = now + 1.0
            dispatches = {task for task in dispatches if not task.done()}
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
                    task = asyncio.create_task(_dispatch_and_release(container, job, worker, active))
                    dispatches.add(task)
            try:
                await asyncio.wait_for(stop_event.wait(), 0.25)
            except TimeoutError:
                pass
    finally:
        deadline = datetime.now(UTC) + timedelta(seconds=SHUTDOWN_DRAIN_SECONDS)
        await drain_shutdown(dispatches, deadline)
        heartbeat_stop.set()
        heartbeat.cancel()
        await asyncio.gather(heartbeat, return_exceptions=True)
        active.clear()


async def _dispatch_and_release(container: Container, job: OutboxJob, worker: str, active: dict[str, Lease]) -> None:
    value = job.payload.get("binding")
    registry_key = str(value["agent_registry_id"]) if isinstance(value, dict) else ""
    try:
        await dispatch_job(container, job, worker)
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
