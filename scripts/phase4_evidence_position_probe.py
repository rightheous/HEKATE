from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import threading
import traceback
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import phase3_runtime_probe as p3
import phase3b_single_hekate_probe as p3b

from hekate.application import evidence as evidence_app, positions, results as result_app, tasks as task_app
from hekate.application.results import process_pending_results
from hekate.application.operations import record_dispatch_send_intent
from hekate.application.turns import prepare_queued_tasks
from hekate.domain.contracts import canonical_json_hash
from hekate.domain.errors import Conflict, PolicyDenied
from hekate.domain.models import (
    AuthorizationSnapshot, Confidence, ConclusionCapsule, EvidenceInput, GuardBinding, InputChange, PositionBody,
    PositionCommitRequest, PositionVersionRecord, ReadLimits, UserMessage, VersionCursor,
)
from hekate.domain.types import (
    ActorContext, AttemptId, DomainId, EvidenceId, OperationId, PrincipalId, RegistryId, ScopeId,
    StopReason, TaskId, TopicId,
)
from hekate.infrastructure.letta.adapter import LettaRuntimeAdapter
from hekate.infrastructure.letta.bridge_protocol import BridgeClient
from hekate.infrastructure.letta import provider_gateway as gateway_implementation
from hekate.infrastructure.letta.provider_gateway import ProviderGatewayProfile
from hekate.infrastructure import archive as archive_store
from hekate.infrastructure.postgres.database import create_engine, create_uow_factory
from hekate.settings import configured_local_actor, configured_task_execution, load_settings
from hekate.worker import service as worker_implementation
from hekate.worker.service import dispatch_job, run_evidence_maintenance_tick, run_worker


def _strings(value: object):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for child in value.values():
            yield from _strings(child)
    elif isinstance(value, list):
        for child in value:
            yield from _strings(child)


def _request_capsule(request: dict[str, object]) -> tuple[str, dict[str, object]] | None:
    for content in _strings(request.get("messages")):
        marker = "Task Capsule JSON:\n"
        if marker in content:
            prompt = content
            capsule, _ = json.JSONDecoder().raw_decode(content.split(marker, 1)[1].lstrip())
            if not isinstance(capsule, dict):
                raise ValueError("Task Capsule in provider request is not an object")
            return prompt, capsule
    return None


def _commit_output(
    request: dict[str, object], observations: list[dict[str, object]],
    expected_evidence: dict[str, str],
) -> str:
    turn = _request_capsule(request)
    if turn is None:
        return "Synthetic-only context compaction."
    prompt, capsule = turn
    schema_marker = "HEKATE server output contract (generated from the strict Python HekateTurnOutput model):\n"
    if schema_marker not in prompt:
        raise ValueError("provider request omitted the server-generated turn schema")
    schema_text = prompt.split(schema_marker, 1)[1].split("\n\nServer output policy:\n", 1)[0]
    schema = json.loads(schema_text)
    actual_schema = p3b.export_schemas()["hekate-turn-output.v1.schema.json"]
    if canonical_json_hash(schema) != canonical_json_hash(actual_schema):
        raise ValueError("provider request schema differs from the generated contract")
    policy = prompt.split("\n\nServer output policy:\n", 1)[1].split("\n\nTrusted runtime binding:", 1)[0]
    allowed_actions = ["answer", "request_information", "abstain", "commit"]
    if any(action not in policy for action in allowed_actions) or "spawn" in policy or "continue" in policy:
        raise ValueError("provider request output policy has an incorrect action boundary")
    match = re.search(
        r"Trusted runtime binding: task_id=([^;]+); attempt_id=([^;]+); agent_registry_id=([^;]+); input_revision=(\d+)\.",
        prompt,
    )
    operation_match = re.search(r"Commit operation_id: ([^\n]+)", prompt)
    if match is None or operation_match is None:
        raise ValueError("provider request omitted trusted identity or commit operation")
    task_id, attempt_id, registry_id, revision_text = match.groups()
    revision = int(revision_text)
    if (task_id, attempt_id, revision) != (
        capsule.get("task_id"), capsule.get("attempt_id"), capsule.get("input_revision"),
    ):
        raise ValueError("provider request binding and Task Capsule disagree")
    evidence_refs = [str(item) for item in capsule.get("evidence_refs", [])]
    task_data = capsule.get("task_data", {})
    excerpts = task_data.get("evidence", []) if isinstance(task_data, dict) else []
    if sum(len(item.get("excerpt", "").encode("utf-8")) for item in excerpts) > 32_768:
        raise ValueError("provider request exceeds the per-Task Evidence excerpt limit")
    excerpt_by_id = {item.get("evidence_id"): item for item in excerpts if isinstance(item, dict)}
    if set(evidence_refs) != set(expected_evidence) or set(excerpt_by_id) != set(evidence_refs):
        raise ValueError("provider request Evidence selection differs from the submitted Task")
    for evidence_id, required_text in expected_evidence.items():
        excerpt = excerpt_by_id[evidence_id]
        if required_text not in excerpt.get("excerpt", ""):
            raise ValueError(f"provider request omitted required Evidence excerpt {evidence_id}")
    target = capsule.get("target_position")
    dissent = task_data.get("dissent", []) if isinstance(task_data, dict) else []
    if target is None:
        if capsule.get("mode") != "independent_exploration" or dissent:
            raise ValueError("new topic was not provided as independent exploration")
    else:
        if capsule.get("mode") != "targeted_review" or target.get("version") != 1:
            raise ValueError("restarted Task did not receive Position v1 for targeted review")
        if target.get("body", {}).get("statement") != "Saved Position v1 for main-topic.":
            raise ValueError("Task Capsule did not contain the database Position v1")
        if not dissent or dissent[0].get("objection", {}).get("condition") != "when the archive is stale":
            raise ValueError("Task Capsule omitted the persistent objection content")
    body_dissent = [item["id"] for item in dissent[:1]] if target is not None else []
    position = {
        "statement": f"Saved Position v{int(capsule.get('base_position_version', 0)) + 1} for {capsule['topic_id']}.",
        "applicability": ["within the submitted Task scope"],
        "confidence": {"level": "high", "basis": ["reviewed task evidence"]},
        "evidence_refs": evidence_refs,
        "assumptions": [],
        "dissent_refs": body_dissent,
        "uncertainty": None,
    }
    objections = []
    if capsule["topic_id"] == "main-topic" and target is None:
        objections = [{
            "id": "O1", "severity": "moderate", "claim": "The source could be outdated.",
            "condition": "when the archive is stale", "suggested_validation": "check the source timestamp",
        }]
    output = {
        "schema_version": "1",
        "proposal": {
            "schema_version": "1", "action": "commit", "operation_id": operation_match.group(1),
            "task_id": task_id, "topic_id": capsule["topic_id"],
            "base_version": capsule["base_position_version"], "input_revision": revision,
            "proposed_position": position, "reason_for_change": "The fake reviewed the supplied context.",
        },
        "conclusion": {
            "schema_version": "1", "task_id": task_id, "attempt_id": attempt_id,
            "agent_id": registry_id, "status": "done",
            "assessment": {
                "statement": "The submitted context supports this provisional Position.",
                "confidence": {"level": "high", "basis": ["synthetic fixture assertions"]},
            },
            "evidence_used": evidence_refs, "objections": objections,
            "assumptions": [], "unresolved": [],
            "recommended_next_step": {"type": "none"},
            "position_recommendation": {"action": "update", "summary": position["statement"]},
        },
    }
    # Exercise the same strict parser that the result path uses before replying.
    from hekate.domain.capsules import parse_hekate_turn_output
    parsed = parse_hekate_turn_output(json.dumps(output, ensure_ascii=False).encode())
    if parsed.proposal.action != "commit":
        raise ValueError("fake fixture did not create a commit proposal")
    observations.append({
        "request_hash": hashlib.sha256(json.dumps(
            request, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode()).hexdigest(),
        "task_id": task_id, "attempt_id": attempt_id, "registry_id": registry_id,
        "topic_id": capsule["topic_id"], "base_position_version": capsule["base_position_version"],
        "mode": capsule["mode"], "evidence_ids": evidence_refs,
        "evidence_excerpt_bytes": sum(len(item.get("excerpt", "").encode()) for item in excerpts),
        "evidence_excerpts_truncated": [item.get("truncated") for item in excerpts],
        "target_position_version": target.get("version") if isinstance(target, dict) else None,
        "persistent_dissent_ids": [item.get("id") for item in dissent],
        "schema_hash_matches_generated": True,
        "allowed_actions": allowed_actions,
        "prompt_utf8_bytes": len(prompt.encode("utf-8")),
    })
    return json.dumps(output, ensure_ascii=False, separators=(",", ":"))


async def _ask(env: dict[str, str], key: str, topic: str, evidence_ids: tuple[str, ...], question: str = "Review this topic using the supplied records.") -> dict[str, object]:
    args = ["ask", "--request-key", key, "--wait-seconds", "0", "--topic-id", topic]
    for evidence_id in evidence_ids:
        args.extend(["--evidence-id", evidence_id])
    return await p3b._cli(env, *args, stdin=question.encode("utf-8"))


async def _wait_for_state(factory, actor: ActorContext, task_id: TaskId, timeout: float = 120) -> dict[str, object]:
    return await p3b._wait_for_state(factory, actor, task_id, {"COMPLETED", "FAILED", "CANCELLED"}, timeout)


async def _run_worker_until(container, factory, actor, task_id: TaskId) -> dict[str, object]:
    stop = asyncio.Event()
    task = asyncio.create_task(run_worker(container, stop))
    try:
        return await _wait_for_state(factory, actor, task_id)
    finally:
        stop.set()
        await asyncio.wait_for(task, 20)


async def _run_worker_after_maintenance_failure(container, factory, actor, task_id: TaskId):
    original_tick = worker_implementation.run_evidence_maintenance_tick
    state = {"calls": 0, "injected_failure": False}

    async def fail_once(uow_factory, archive_root, *, now=None):
        state["calls"] += 1
        if state["calls"] == 1:
            state["injected_failure"] = True
            raise OSError("injected maintenance failure before dispatch")
        return await original_tick(uow_factory, archive_root, now=now)

    with patch.object(worker_implementation, "run_evidence_maintenance_tick", new=fail_once):
        view = await _run_worker_until(container, factory, actor, task_id)
    return view, state


async def _manual_prepare(factory, container, actor, config, env, worker: str, key: str, topic: str, evidence_ids: tuple[str, ...]):
    ask = await _ask(env, key, topic, evidence_ids)
    task_id = TaskId(str(ask["receipt"]["task_id"]))
    leases = await prepare_queued_tasks(
        factory, container.runtime, actor, config, worker, limit=1,
        archive_root=container.settings.archive_dir,
    )
    if len(leases) != 1:
        raise AssertionError(f"manual dispatch expected one admitted Task, got {len(leases)}")
    job = await p3.claim_one(factory, worker)
    if job is None or job.payload.get("binding", {}).get("task_id") != str(task_id):
        raise AssertionError("manual dispatch did not claim the submitted Task")
    async with factory() as uow:
        preparation = (await uow.session.execute(text(
            "SELECT operation_id, attempt_id FROM task_preparations WHERE task_id=:task AND input_revision=1"
        ), {"task": str(task_id)})).mappings().one()
        await uow.commit()
    return task_id, OperationId(preparation["operation_id"]), leases[0], job


async def _manual_dispatch(factory, container, actor, config, env, worker: str, key: str, topic: str, evidence_ids: tuple[str, ...]):
    task_id, operation_id, lease, job = await _manual_prepare(
        factory, container, actor, config, env, worker, key, topic, evidence_ids,
    )
    await dispatch_job(container, job, worker)
    async with factory() as uow:
        await uow.session.execute(text(
            "UPDATE turn_results SET next_attempt_at=now() WHERE operation_id=:operation"
        ), {"operation": str(operation_id)})
        await uow.commit()
    return task_id, operation_id, lease


async def _set_evidence_availability(engine, evidence_ids: tuple[str, ...], *, available: bool) -> None:
    async with engine.begin() as connection:
        for evidence_id in sorted(set(evidence_ids)):
            if available:
                await connection.execute(text(
                    "UPDATE evidence SET availability='AVAILABLE', expiry_at=now()+interval '1 day' WHERE id=:id"
                ), {"id": evidence_id})
            else:
                await connection.execute(text(
                    "UPDATE evidence SET availability='EXPIRED', expiry_at=now()-interval '1 second' WHERE id=:id"
                ), {"id": evidence_id})


async def _deadline_after_position_effect(
    factory, engine, container, actor, config, env, worker: str, run_id: str,
    evidence_ids: tuple[str, ...], fake,
) -> dict[str, object]:
    topic = f"deadline-after-effect-{run_id}"
    task_id, operation_id, lease = await _manual_dispatch(
        factory, container, actor, config, env, worker,
        f"phase4-{run_id}-deadline-after-effect", topic, evidence_ids,
    )
    deadline = datetime.now(UTC) + timedelta(seconds=90)
    async with engine.begin() as connection:
        await connection.execute(text("UPDATE tasks SET deadline=:deadline WHERE id=:task"), {
            "deadline": deadline, "task": str(task_id),
        })
        inbox_id = await connection.scalar(text(
            "SELECT inbox_id FROM turn_results WHERE operation_id=:operation"
        ), {"operation": str(operation_id)})
        await connection.execute(text("UPDATE turn_results SET next_attempt_at=now() WHERE inbox_id=:inbox"), {
            "inbox": inbox_id,
        })
    if inbox_id is None:
        raise AssertionError("deadline probe runtime did not persist a business result")
    clock_trace: list[str] = []

    def step_clock() -> datetime:
        clock_trace.append("before_deadline" if len(clock_trace) < 2 else "after_deadline")
        return deadline - timedelta(microseconds=1) if len(clock_trace) <= 2 else deadline + timedelta(microseconds=1)

    provider_count_before = fake.count()
    accounting_before = await p3b._accounting_effects(engine, task_id)
    result = await result_app.apply_turn_result(factory, str(inbox_id), clock=step_clock)
    counts_after = await _position_counts(engine, topic)
    task_view = await task_app.get_task(factory, actor, task_id)
    async with engine.connect() as connection:
        position_state = (await connection.execute(text("""
            SELECT COALESCE((SELECT current_version FROM position_topics WHERE scope=:scope AND topic_id=:topic),0) AS current_version,
                   (SELECT count(*) FROM position_versions WHERE scope=:scope AND topic_id=:topic) AS versions,
                   (SELECT count(*) FROM position_commit_receipts WHERE operation_id=:commit_op) AS receipts,
                   (SELECT count(*) FROM memory_projections WHERE scope=:scope AND topic_id=:topic) AS projections,
                   (SELECT count(*) FROM outbox WHERE kind='position_projection' AND operation_id=:operation) AS projection_intents,
                   (SELECT count(*) FROM task_responses WHERE task_id=:task AND outcome<>'FAILED') AS success_responses,
                   (SELECT processing_state FROM turn_results WHERE inbox_id=:inbox) AS result_state,
                   (SELECT eligible FROM conclusions WHERE id=(SELECT conclusion_id FROM turn_results WHERE inbox_id=:inbox)) AS conclusion_eligible,
                   (SELECT stop_reason FROM tasks WHERE id=:task) AS stop_reason
        """), {
            "scope": str(actor.scope), "topic": topic,
            "commit_op": f"{operation_id}:position.commit", "operation": str(operation_id),
            "task": str(task_id), "inbox": str(inbox_id),
        })).mappings().one()
    replay = await result_app.apply_turn_result(factory, str(inbox_id), clock=step_clock)
    accounting_after = await p3b._accounting_effects(engine, task_id)
    provider_count_after = fake.count()
    await _release(factory, lease)
    state = dict(position_state)
    return {
        "task_id": str(task_id), "operation_id": str(operation_id), "topic_id": topic,
        "deadline": deadline.isoformat(), "clock_trace": clock_trace,
        "initial_apply": result, "replay": replay, "task_state": task_view["state"],
        "position_effects_after_late": state, "position_counts": counts_after,
        "provider_requests_before_after_replay": [provider_count_before, provider_count_after],
        "accounting_before_after_replay": [accounting_before, accounting_after],
        "passed": result.get("state") == "LATE" and replay.get("state") == "LATE"
        and task_view["state"] == "FAILED" and state["stop_reason"] == StopReason.DEADLINE.value
        and state["current_version"] == 0 and state["versions"] == 0 and state["receipts"] == 0
        and state["projections"] == 0 and state["projection_intents"] == 0
        and state["success_responses"] == 0 and state["result_state"] == "LATE"
        and state["conclusion_eligible"] is False
        and provider_count_before == provider_count_after and accounting_before == accounting_after,
    }


async def _task_adoption_failure_rolls_back_position(
    factory, engine, container, actor, config, env, worker: str, run_id: str,
    evidence_ids: tuple[str, ...], fake,
) -> dict[str, object]:
    topic = f"adoption-failure-{run_id}"
    task_id, operation_id, lease = await _manual_dispatch(
        factory, container, actor, config, env, worker,
        f"phase4-{run_id}-adoption-failure", topic, evidence_ids,
    )
    async with engine.connect() as connection:
        inbox_id = await connection.scalar(text(
            "SELECT inbox_id FROM turn_results WHERE operation_id=:operation"
        ), {"operation": str(operation_id)})
    if inbox_id is None:
        raise AssertionError("adoption failure probe runtime did not persist a business result")
    from hekate.application.tasks import complete as persist_complete_task

    async def persist_then_fail(*args, **kwargs):
        accepted = await persist_complete_task(*args, **kwargs)
        if accepted:
            raise RuntimeError("injected failure after final Task response write")
        return accepted

    failure_observed = False
    try:
        with patch.object(result_app, "complete_task", new=persist_then_fail):
            await result_app.apply_turn_result(factory, str(inbox_id))
    except RuntimeError as error:
        failure_observed = "after final Task response write" in str(error)
    async with engine.connect() as connection:
        before_retry = (await connection.execute(text("""
            SELECT t.status,
                   (SELECT current_version FROM position_topics WHERE scope=t.owner_scope AND topic_id=t.topic_id) AS current_version,
                   (SELECT count(*) FROM position_versions WHERE scope=t.owner_scope AND topic_id=t.topic_id) AS versions,
                   (SELECT count(*) FROM position_commit_receipts WHERE operation_id=:commit_op) AS receipts,
                   (SELECT count(*) FROM memory_projections WHERE scope=t.owner_scope AND topic_id=t.topic_id) AS projections,
                   (SELECT count(*) FROM outbox WHERE kind='position_projection' AND operation_id=:operation) AS projection_intents,
                   (SELECT count(*) FROM task_responses WHERE task_id=:task) AS responses,
                   (SELECT processing_state FROM turn_results WHERE inbox_id=:inbox) AS result_state
            FROM tasks t WHERE t.id=:task
        """), {
            "commit_op": f"{operation_id}:position.commit", "operation": str(operation_id),
            "task": str(task_id), "inbox": str(inbox_id),
        })).mappings().one()
    accounting_before_retry = await p3b._accounting_effects(engine, task_id)
    retry = await result_app.apply_turn_result(factory, str(inbox_id))
    after_retry = await _position_counts(engine, topic)
    replay = await result_app.apply_turn_result(factory, str(inbox_id))
    after_replay = await _position_counts(engine, topic)
    accounting_after_replay = await p3b._accounting_effects(engine, task_id)
    task_view = await task_app.get_task(factory, actor, task_id)
    await _release(factory, lease)
    before = dict(before_retry)
    return {
        "task_id": str(task_id), "operation_id": str(operation_id), "topic_id": topic,
        "injected_failure_after_task_response_write": failure_observed,
        "state_after_rollback_before_retry": before,
        "retry": retry, "replay": replay, "position_counts_after_retry": after_retry,
        "position_counts_after_replay": after_replay, "task_state_after_retry": task_view["state"],
        "accounting_before_retry_after_replay": [accounting_before_retry, accounting_after_replay],
        "passed": failure_observed and before["status"] == "RUNNING"
        and before["current_version"] is None and before["versions"] == 0 and before["receipts"] == 0
        and before["projections"] == 0 and before["projection_intents"] == 0
        and before["responses"] == 0 and before["result_state"] == "WAITING_EXECUTION"
        and retry.get("state") == "ACCEPTED" and replay.get("state") == "ACCEPTED"
        and task_view["state"] == "COMPLETED"
        and after_retry == after_replay and after_retry == {"versions": 1, "receipts": 1, "responses": 1}
        and accounting_before_retry == accounting_after_replay,
    }


async def _permit_evidence_boundaries(
    factory, engine, container, actor, config, env, worker: str, run_id: str,
    evidence_ids: tuple[str, ...], gateway_address: str, gateway_port: int,
    private_token: str, gateway_app, fake,
) -> dict[str, object]:
    topic = f"permit-evidence-{run_id}"
    task_id, operation_id, lease, job = await _manual_prepare(
        factory, container, actor, config, env, worker,
        f"phase4-{run_id}-permit-evidence", topic, evidence_ids,
    )
    await record_dispatch_send_intent(factory, job, worker)
    binding = GuardBinding(**job.payload["binding"])
    operation_text = str(operation_id)
    forward_start = gateway_app.state.metrics["upstream_forward_attempts"]
    fake_start = fake.count()
    original_authorize = gateway_implementation.authorize_provider_call

    async def request(kind: str, call_suffix: str) -> tuple[int, bytes]:
        call_id = f"phase4:{run_id}:{call_suffix}"
        return await p3.gateway_request(
            gateway_address, gateway_port, private_token,
            p3.runtime_headers(binding, operation_text, call_id, kind, 16),
            {"model": p3.FAKE_MODEL, "stream": False, "max_tokens": 16,
             "messages": [{"role": "user", "content": "isolated permit boundary check"}]},
        )

    async def expire_before_authorize(uow_factory, intent):
        await _set_evidence_availability(engine, (evidence_ids[0],), available=False)
        return await original_authorize(uow_factory, intent)

    with patch.object(gateway_implementation, "authorize_provider_call", new=expire_before_authorize):
        issue_status, _ = await request("turn", "issue-expired")
    issue_rows = await p3.fetch_operation(engine, operation_text)
    issue_call_rows = [row for row in issue_rows["rows"] if row.get("accounting_call_id")]

    await _set_evidence_availability(engine, evidence_ids, available=True)
    permit_observed: dict[str, object] = {}

    async def authorize_then_expire(uow_factory, intent):
        permit = await original_authorize(uow_factory, intent)
        permit_observed["permit_id"] = str(permit.permit_id)
        await _set_evidence_availability(engine, (evidence_ids[0],), available=False)
        return permit

    with patch.object(gateway_implementation, "authorize_provider_call", new=authorize_then_expire):
        consume_status, _ = await request("turn", "consume-expired")
    consume_rows = await p3.fetch_operation(engine, operation_text)
    consume_row = next((row for row in consume_rows["rows"] if row.get("accounting_call_id") == f"phase4:{run_id}:consume-expired"), {})

    await _set_evidence_availability(engine, evidence_ids, available=True)
    async def expire_compaction_before_authorize(uow_factory, intent):
        await _set_evidence_availability(engine, (evidence_ids[0],), available=False)
        return await original_authorize(uow_factory, intent)

    with patch.object(gateway_implementation, "authorize_provider_call", new=expire_compaction_before_authorize):
        compaction_status, _ = await request("compaction", "compaction-expired")
    compaction_rows = await p3.fetch_operation(engine, operation_text)
    compaction_row = next((row for row in compaction_rows["rows"] if row.get("accounting_call_id") == f"phase4:{run_id}:compaction-expired"), {})

    await _set_evidence_availability(engine, evidence_ids, available=True)
    await _release(factory, lease)
    forward_end = gateway_app.state.metrics["upstream_forward_attempts"]
    fake_end = fake.count()
    return {
        "task_id": str(task_id), "operation_id": operation_text,
        "send_intent_recorded": True,
        "issue_boundary": {"gateway_status": issue_status, "provider_rows": issue_call_rows},
        "consume_boundary": {
            "gateway_status": consume_status, "permit_id_issued": permit_observed.get("permit_id"),
            "call_status": consume_row.get("call_status"), "permit_state": consume_row.get("permit_state"),
        },
        "compaction_boundary": {
            "gateway_status": compaction_status, "provider_row": compaction_row,
            "planned_compaction_calls": job.payload.get("payload", {}).get("call_plan", {}).get("compaction_calls"),
        },
        "upstream_forward_attempts_before_after": [forward_start, forward_end],
        "fake_provider_requests_before_after": [fake_start, fake_end],
        "passed": issue_status == 402 and not issue_call_rows
        and consume_status == 402 and bool(permit_observed.get("permit_id"))
        and consume_row.get("call_status") == "ALLOCATED" and consume_row.get("permit_state") == "ISSUED"
        and compaction_status == 402 and not compaction_row
        and job.payload.get("payload", {}).get("call_plan", {}).get("compaction_calls", 0) >= 1
        and forward_start == forward_end and fake_start == fake_end,
    }


async def _register_fixture_evidence(factory, actor, path: Path, archive_root: Path, content: bytes, request_key: str):
    path.write_bytes(content)
    now = datetime.now(UTC)
    digest = hashlib.sha256(content).hexdigest()
    return await evidence_app.register(
        factory, actor,
        EvidenceInput(
            id=EvidenceId(str(uuid.uuid4())), kind="document", source_uri=path.resolve().as_uri(),
            retrieved_at=now, content_hash=digest, access_scope="probe-untrusted-claim",
            retention_class="fixture-retained", expiry_at=now + timedelta(days=2),
        ),
        content, archive_root, request_key=request_key,
    )


async def _yield_event_loop_once() -> None:
    loop = asyncio.get_running_loop()
    checkpoint = loop.create_future()
    loop.call_soon(checkpoint.set_result, None)
    await checkpoint


async def _maintenance_contention_scenarios(
    factory, engine, actor, archive_root: Path, state_path: Path, run_id: str,
) -> dict[str, object]:
    competing_content = f"competing archive cleanup {run_id}\n".encode()
    competing_path = state_path / f"competing-{run_id}.txt"
    competing = await _register_fixture_evidence(
        factory, actor, competing_path, archive_root, competing_content, f"phase4-{run_id}-competing",
    )
    async with engine.begin() as connection:
        await connection.execute(text("UPDATE evidence SET expiry_at=now()-interval '1 second' WHERE id=:id"), {
            "id": str(competing.id),
        })

    acquire = archive_store.acquire_lock
    delete = archive_store.delete_locked
    delete_unlinked = threading.Event()
    second_waiting = threading.Event()
    finish_delete = threading.Event()
    counter_lock = threading.Lock()
    lock_calls = 0
    delete_calls = 0

    def tracked_acquire(root: Path, ref: str):
        nonlocal lock_calls
        with counter_lock:
            lock_calls += 1
            if lock_calls == 2:
                second_waiting.set()
        return acquire(root, ref)

    def unlink_then_pause(root: Path, ref: str):
        nonlocal delete_calls
        with counter_lock:
            delete_calls += 1
        result = delete(root, ref)
        delete_unlinked.set()
        if not finish_delete.wait(15):
            raise TimeoutError("archive cleanup race probe did not release its deterministic barrier")
        return result

    with patch.object(archive_store, "acquire_lock", new=tracked_acquire), patch.object(
        archive_store, "delete_locked", new=unlink_then_pause,
    ):
        first = asyncio.create_task(run_evidence_maintenance_tick(factory, archive_root))
        if not await asyncio.to_thread(delete_unlinked.wait, 15):
            finish_delete.set()
            await asyncio.gather(first, return_exceptions=True)
            raise AssertionError("first maintenance tick did not reach archive unlink")
        second = asyncio.create_task(run_evidence_maintenance_tick(factory, archive_root))
        if not await asyncio.to_thread(second_waiting.wait, 15):
            finish_delete.set()
            await asyncio.gather(first, second, return_exceptions=True)
            raise AssertionError("second maintenance tick did not contend for the same artifact lock")
        finish_delete.set()
        first_result, second_result = await asyncio.gather(first, second)
    async with engine.connect() as connection:
        competing_state = await connection.scalar(text(
            "SELECT storage_state FROM artifacts WHERE artifact_ref=:ref"
        ), {"ref": competing.artifact_ref})
    competing_file_missing = False
    try:
        archive_store.read(archive_root, competing.artifact_ref, len(competing_content) + 1)
    except FileNotFoundError:
        competing_file_missing = True

    stale_content = f"stale cleaner cannot delete re-registered artifact {run_id}\n".encode()
    stale_path = state_path / f"stale-{run_id}.txt"
    stale = await _register_fixture_evidence(
        factory, actor, stale_path, archive_root, stale_content, f"phase4-{run_id}-stale-old",
    )
    async with engine.begin() as connection:
        await connection.execute(text("UPDATE evidence SET expiry_at=now()-interval '1 second' WHERE id=:id"), {
            "id": str(stale.id),
        })

    delayed_waiting = threading.Event()
    release_delayed = threading.Event()
    acquire_count = 0
    delayed_lock = threading.Lock()

    def delay_first_acquire(root: Path, ref: str):
        nonlocal acquire_count
        with delayed_lock:
            acquire_count += 1
            first_call = acquire_count == 1
        if first_call:
            delayed_waiting.set()
            if not release_delayed.wait(15):
                raise TimeoutError("stale cleanup barrier was not released")
        return acquire(root, ref)

    with patch.object(archive_store, "acquire_lock", new=delay_first_acquire):
        stale_cleaner = asyncio.create_task(run_evidence_maintenance_tick(factory, archive_root))
        if not await asyncio.to_thread(delayed_waiting.wait, 15):
            release_delayed.set()
            await asyncio.gather(stale_cleaner, return_exceptions=True)
            raise AssertionError("stale maintenance tick did not reach its pre-lock barrier")
        fresh_cleaner = await run_evidence_maintenance_tick(factory, archive_root)
        restored = await _register_fixture_evidence(
            factory, actor, stale_path, archive_root, stale_content, f"phase4-{run_id}-stale-new",
        )
        release_delayed.set()
        stale_result = await stale_cleaner
    async with engine.connect() as connection:
        stale_artifact_state = await connection.scalar(text(
            "SELECT storage_state FROM artifacts WHERE artifact_ref=:ref"
        ), {"ref": stale.artifact_ref})
        restored_state = await connection.scalar(text(
            "SELECT availability FROM evidence WHERE id=:id"
        ), {"id": str(restored.id)})
    restored_bytes = archive_store.read(archive_root, stale.artifact_ref, len(stale_content) + 1)

    acquire_cancel_content = f"cancel while acquiring archive lock {run_id}\n".encode()
    acquire_cancel_path = state_path / f"cancel-acquire-{run_id}.txt"
    acquire_cancel_evidence = await _register_fixture_evidence(
        factory, actor, acquire_cancel_path, archive_root, acquire_cancel_content,
        f"phase4-{run_id}-cancel-acquire",
    )
    async with engine.begin() as connection:
        await connection.execute(text("UPDATE evidence SET expiry_at=now()-interval '1 second' WHERE id=:id"), {
            "id": str(acquire_cancel_evidence.id),
        })

    acquire = archive_store.acquire_lock
    release = archive_store.release_lock
    acquire_waiting = threading.Event()
    acquire_returned = threading.Event()
    acquired_fds: list[int] = []
    released_fds: list[int] = []
    fd_lock = threading.Lock()
    owner_fd = await asyncio.to_thread(acquire, archive_root, acquire_cancel_evidence.artifact_ref)

    def tracked_acquire(root: Path, ref: str):
        acquire_waiting.set()
        fd = acquire(root, ref)
        with fd_lock:
            acquired_fds.append(fd)
        acquire_returned.set()
        return fd

    def tracked_release(fd: int):
        with fd_lock:
            released_fds.append(fd)
        return release(fd)

    acquisition_task = None
    owner_released = False
    acquisition_cancel_received = False
    child_release_count = 0
    acquisition_error = None
    cancel_waited_for_owner = False
    fd_closed_before_repair = False
    try:
        with patch.object(archive_store, "acquire_lock", new=tracked_acquire), patch.object(
            archive_store, "release_lock", new=tracked_release,
        ):
            acquisition_task = asyncio.create_task(run_evidence_maintenance_tick(factory, archive_root))
            if not await asyncio.to_thread(acquire_waiting.wait, 15):
                raise TimeoutError("cancelled cleanup did not reach the blocked archive lock")
            acquisition_task.cancel()
            await _yield_event_loop_once()
            cancel_waited_for_owner = not acquisition_task.done()
            await asyncio.to_thread(release, owner_fd)
            owner_released = True
            try:
                await asyncio.wait_for(acquisition_task, 15)
            except asyncio.CancelledError:
                acquisition_cancel_received = True
            except Exception as error:
                acquisition_error = f"{type(error).__name__}: {error}"
            if not await asyncio.to_thread(acquire_returned.wait, 15):
                raise TimeoutError("archive lock acquisition thread did not return after unlock")
            with fd_lock:
                child_fd = acquired_fds[0] if acquired_fds else None
                child_release_count = released_fds.count(child_fd) if child_fd is not None else 0
            if child_fd is not None:
                try:
                    os.fstat(child_fd)
                except OSError:
                    fd_closed_before_repair = True
                if not fd_closed_before_repair:
                    # Keep a regression run from leaking a real descriptor/lock.
                    await asyncio.to_thread(release, child_fd)
    finally:
        if not owner_released:
            await asyncio.to_thread(release, owner_fd)
        if acquisition_task is not None and not acquisition_task.done():
            try:
                await asyncio.wait_for(acquisition_task, 15)
            except BaseException:
                pass

    lock_reacquirable = False
    if acquired_fds and not fd_closed_before_repair:
        # A pre-fix run may have lost this descriptor; close it before probing
        # the actual flock path so the diagnostic itself cannot hang.
        try:
            await asyncio.to_thread(release, acquired_fds[0])
        except OSError:
            pass
    probe_fd = await asyncio.wait_for(
        asyncio.to_thread(acquire, archive_root, acquire_cancel_evidence.artifact_ref), 15,
    )
    lock_reacquirable = True
    await asyncio.to_thread(release, probe_fd)
    acquisition_cleanup = await run_evidence_maintenance_tick(factory, archive_root)

    delete_cancel_content = f"cancel during archive deletion {run_id}\n".encode()
    delete_cancel_path = state_path / f"cancel-delete-{run_id}.txt"
    delete_cancel_evidence = await _register_fixture_evidence(
        factory, actor, delete_cancel_path, archive_root, delete_cancel_content,
        f"phase4-{run_id}-cancel-delete-old",
    )
    async with engine.begin() as connection:
        await connection.execute(text("UPDATE evidence SET expiry_at=now()-interval '1 second' WHERE id=:id"), {
            "id": str(delete_cancel_evidence.id),
        })

    real_delete = archive_store.delete_locked
    first_delete_entered = threading.Event()
    second_lock_waiting = threading.Event()
    allow_first_delete = threading.Event()
    first_delete_finished = threading.Event()
    lock_call_count = 0
    delete_call_count = 0
    race_lock = threading.Lock()

    def tracked_race_acquire(root: Path, ref: str):
        nonlocal lock_call_count
        with race_lock:
            lock_call_count += 1
            if lock_call_count == 2:
                second_lock_waiting.set()
        return acquire(root, ref)

    def pause_first_delete(root: Path, ref: str):
        nonlocal delete_call_count
        with race_lock:
            delete_call_count += 1
            call = delete_call_count
        if call == 1:
            first_delete_entered.set()
            if not allow_first_delete.wait(15):
                raise TimeoutError("cancelled archive deletion barrier was not released")
        result = real_delete(root, ref)
        if call == 1:
            first_delete_finished.set()
        return result

    first = second = None
    cancellation_received = False
    first_cancel_deferred = False
    repeated_cancel_deferred = False
    second_blocked = False
    second_result = None
    pending_reregister_denied = False
    premature_reregister = None
    active_file_deleted_by_old_cleanup = False
    final_reregister = None
    first_delete_observed = False
    try:
        with patch.object(archive_store, "acquire_lock", new=tracked_race_acquire), patch.object(
            archive_store, "delete_locked", new=pause_first_delete,
        ):
            first = asyncio.create_task(run_evidence_maintenance_tick(factory, archive_root))
            if not await asyncio.to_thread(first_delete_entered.wait, 15):
                raise TimeoutError("first cleanup did not reach the pre-unlink barrier")
            second = asyncio.create_task(run_evidence_maintenance_tick(factory, archive_root))
            if not await asyncio.to_thread(second_lock_waiting.wait, 15):
                raise TimeoutError("second cleanup did not wait on the same artifact lock")
            first.cancel()
            await _yield_event_loop_once()
            first_cancel_deferred = not first.done()
            second_blocked = not second.done()
            first.cancel()
            await _yield_event_loop_once()
            repeated_cancel_deferred = not first.done()
            pending_path = state_path / f"cancel-delete-pending-reregister-{run_id}.txt"
            pending_path.write_bytes(delete_cancel_content)
            try:
                premature_reregister = await evidence_app.register(
                    factory, actor,
                    EvidenceInput(
                        id=EvidenceId(str(uuid.uuid4())), kind="document",
                        source_uri=pending_path.resolve().as_uri(), retrieved_at=datetime.now(UTC),
                        content_hash=hashlib.sha256(delete_cancel_content).hexdigest(),
                        access_scope="ignored-client-scope", retention_class="fixture-retained",
                        expiry_at=datetime.now(UTC) + timedelta(days=2),
                    ),
                    delete_cancel_content, archive_root,
                    request_key=f"phase4-{run_id}-cancel-delete-pending-reregister",
                )
            except Conflict:
                pending_reregister_denied = True
            allow_first_delete.set()
            if not await asyncio.to_thread(first_delete_finished.wait, 15):
                raise TimeoutError("first archive delete did not finish after releasing its barrier")
            try:
                await asyncio.wait_for(first, 15)
            except asyncio.CancelledError:
                cancellation_received = True
            second_result = await asyncio.wait_for(second, 15)
            first_delete_observed = True
            if premature_reregister is not None:
                try:
                    archive_store.read(archive_root, delete_cancel_evidence.artifact_ref, len(delete_cancel_content) + 1)
                except FileNotFoundError:
                    active_file_deleted_by_old_cleanup = True
            final_reregister = await _register_fixture_evidence(
                factory, actor, delete_cancel_path, archive_root, delete_cancel_content,
                f"phase4-{run_id}-cancel-delete-new",
            )
    finally:
        allow_first_delete.set()
        for task in (first, second):
            if task is not None and not task.done():
                try:
                    await asyncio.wait_for(task, 15)
                except BaseException:
                    pass

    async with engine.connect() as connection:
        acquisition_state = await connection.scalar(text(
            "SELECT storage_state FROM artifacts WHERE artifact_ref=:ref"
        ), {"ref": acquire_cancel_evidence.artifact_ref})
        deletion_rows = (await connection.execute(text(
            "SELECT availability FROM evidence WHERE artifact_ref=:ref ORDER BY registered_at"
        ), {"ref": delete_cancel_evidence.artifact_ref})).scalars().all()
        deletion_state = await connection.scalar(text(
            "SELECT storage_state FROM artifacts WHERE artifact_ref=:ref"
        ), {"ref": delete_cancel_evidence.artifact_ref})
    final_bytes = archive_store.read(archive_root, delete_cancel_evidence.artifact_ref, len(delete_cancel_content) + 1)
    cancellation = {
        "lock_acquisition": {
            "waiter_reached_real_flock": acquire_waiting.is_set(),
            "cancel_waited_for_owner_unlock": cancel_waited_for_owner,
            "cancel_delivered": acquisition_cancel_received,
            "fd_closed_before_repair": fd_closed_before_repair,
            "release_calls_for_acquired_fd": child_release_count if acquired_fds else 0,
            "lock_reacquirable_after_cancel": lock_reacquirable,
            "cleanup_retry": acquisition_cleanup,
            "artifact_state_after_retry": acquisition_state,
            "cleanup_error": acquisition_error,
            "passed": acquire_waiting.is_set() and cancel_waited_for_owner and acquisition_cancel_received
            and fd_closed_before_repair and child_release_count == 1 and lock_reacquirable
            and acquisition_state == "DELETED" and not acquisition_error,
        },
        "delete_cancel_and_reregister": {
            "delete_entered_before_unlink": first_delete_entered.is_set(),
            "second_cleaner_waited_for_lock": second_lock_waiting.is_set(),
            "first_cancel_deferred": first_cancel_deferred,
            "repeated_cancel_deferred": repeated_cancel_deferred,
            "second_cleaner_blocked_until_delete_finished": second_blocked,
            "reregister_rejected_while_delete_pending": pending_reregister_denied,
            "premature_reregister_succeeded": premature_reregister is not None,
            "old_cleanup_deleted_premature_active_file": active_file_deleted_by_old_cleanup,
            "cancel_delivered_after_delete_and_unlock": cancellation_received,
            "first_delete_finished": first_delete_observed,
            "physical_delete_calls": delete_call_count,
            "second_cleanup_result": second_result,
            "old_evidence_state": deletion_rows[0] if deletion_rows else None,
            "new_evidence_state": deletion_rows[-1] if final_reregister is not None and deletion_rows else None,
            "artifact_state": deletion_state,
            "active_file_present": final_bytes == delete_cancel_content,
            "digest_matches": hashlib.sha256(final_bytes).hexdigest() == str(delete_cancel_evidence.content_hash),
            "source_input_retained": delete_cancel_path.exists(),
            "passed": first_delete_entered.is_set() and second_lock_waiting.is_set()
            and first_cancel_deferred and repeated_cancel_deferred and second_blocked
            and pending_reregister_denied and cancellation_received and first_delete_observed
            and premature_reregister is None and not active_file_deleted_by_old_cleanup
            and second_result is not None and not second_result["deleted_artifacts"]
            and delete_call_count == 1 and deletion_rows == ["EXPIRED", "AVAILABLE"]
            and deletion_state == "AVAILABLE" and final_bytes == delete_cancel_content
            and hashlib.sha256(final_bytes).hexdigest() == str(delete_cancel_evidence.content_hash)
            and delete_cancel_path.exists(),
        },
    }
    return {
        "same_intent_contention": {
            "first_tick": first_result, "second_tick": second_result,
            "filesystem_delete_calls": delete_calls, "artifact_state": competing_state,
            "archive_file_missing": competing_file_missing,
            "source_input_retained": competing_path.exists(),
            "passed": delete_calls == 1 and competing_state == "DELETED" and competing_file_missing
            and competing_path.exists() and not second_result["deleted_artifacts"],
        },
        "stale_cleaner_after_reregistration": {
            "fresh_cleanup": fresh_cleaner, "stale_cleanup": stale_result,
            "old_evidence_id": str(stale.id), "new_evidence_id": str(restored.id),
            "artifact_state": stale_artifact_state, "new_evidence_state": restored_state,
            "content_hash_matches": hashlib.sha256(restored_bytes).hexdigest() == str(stale.content_hash),
            "source_input_retained": stale_path.exists(),
            "passed": stale_artifact_state == "AVAILABLE" and restored_state == "AVAILABLE"
            and restored_bytes == stale_content and hashlib.sha256(restored_bytes).hexdigest() == str(stale.content_hash)
            and stale_path.exists() and not stale_result["deleted_artifacts"],
        },
        "cancellation_safety": cancellation,
    }


async def _hardening_maintenance_checks(
    factory, engine, actor, archive_root: Path, state_path: Path, run_id: str,
    primary_id: str, derived_id: str, evidence_file: Path, evidence_text: str,
    history_topic: str,
) -> dict[str, object]:
    shared = await _register_fixture_evidence(
        factory, actor, state_path / f"shared-{run_id}.txt", archive_root,
        evidence_file.read_bytes(), f"phase4-{run_id}-shared-maintenance",
    )
    async with engine.begin() as connection:
        await connection.execute(text("UPDATE evidence SET expiry_at=now()-interval '1 second' WHERE id=:id"), {
            "id": str(shared.id),
        })
    shared_tick = await run_evidence_maintenance_tick(factory, archive_root)
    async with engine.connect() as connection:
        shared_state = await connection.scalar(text("SELECT storage_state FROM artifacts WHERE artifact_ref=:ref"), {
            "ref": shared.artifact_ref,
        })
        primary_state = await connection.scalar(text("SELECT availability FROM evidence WHERE id=:id"), {
            "id": primary_id,
        })

    failed_content = f"retry archive deletion {run_id}\n".encode()
    failed_path = state_path / f"delete-retry-{run_id}.txt"
    failed_evidence = await _register_fixture_evidence(
        factory, actor, failed_path, archive_root, failed_content, f"phase4-{run_id}-delete-retry",
    )
    async with engine.begin() as connection:
        await connection.execute(text("UPDATE evidence SET expiry_at=now()-interval '1 second' WHERE id=:id"), {
            "id": str(failed_evidence.id),
        })
    with patch.object(archive_store, "delete_locked", side_effect=OSError("injected archive delete failure")):
        failure_tick = await run_evidence_maintenance_tick(factory, archive_root)
    async with engine.connect() as connection:
        failed_storage_state = await connection.scalar(text("SELECT storage_state FROM artifacts WHERE artifact_ref=:ref"), {
            "ref": failed_evidence.artifact_ref,
        })
    retry_tick = await run_evidence_maintenance_tick(factory, archive_root)
    archive_after_retry_missing = False
    try:
        archive_store.read(archive_root, failed_evidence.artifact_ref, len(failed_content) + 1)
    except FileNotFoundError:
        archive_after_retry_missing = True

    contention = await _maintenance_contention_scenarios(factory, engine, actor, archive_root, state_path, run_id)

    async with engine.begin() as connection:
        for evidence_id in (primary_id, derived_id):
            await connection.execute(text(
                "UPDATE evidence SET expiry_at=now()-interval '1 second', availability='AVAILABLE' WHERE id=:id"
            ), {"id": evidence_id})
    position_expiry_tick = await run_evidence_maintenance_tick(factory, archive_root)
    history = await positions.read_history(
        factory, actor, TopicId(history_topic), VersionCursor(after_version=0, limit=50),
    )
    async with engine.connect() as connection:
        tombstones = (await connection.execute(text(
            "SELECT id,availability FROM evidence WHERE id IN (:primary,:derived) ORDER BY id"
        ), {"primary": primary_id, "derived": derived_id})).mappings().all()
    inputs_retained = evidence_file.exists() and failed_path.exists()
    result = {
        "tick_interval_seconds": 60, "batch_limit": 100,
        "shared_artifact": {
            "tick": shared_tick, "artifact_state": shared_state, "other_reference_state": primary_state,
            "source_input_retained": (state_path / f"shared-{run_id}.txt").exists(),
            "passed": shared_state == "AVAILABLE" and primary_state == "AVAILABLE",
        },
        "delete_failure_and_retry": {
            "failure_tick": failure_tick, "state_after_failure": failed_storage_state,
            "retry_tick": retry_tick, "state_after_retry": "DELETED" if archive_after_retry_missing else "present",
            "source_input_retained": failed_path.exists(),
            "passed": failed_evidence.artifact_ref in failure_tick["failed_artifacts"]
            and failed_storage_state == "DELETE_FAILED"
            and failed_evidence.artifact_ref in retry_tick["deleted_artifacts"]
            and archive_after_retry_missing and failed_path.exists(),
        },
        "concurrent_cleanup": contention,
        "expired_position_evidence": {
            "tick": position_expiry_tick, "tombstones": [dict(row) for row in tombstones],
            "position_history_versions": [item.version for item in history.items],
            "source_inputs_retained": inputs_retained,
            "passed": {str(row["id"]): row["availability"] for row in tombstones}
            == {primary_id: "EXPIRED", derived_id: "EXPIRED"}
            and bool(history.items) and inputs_retained,
        },
    }
    result["passed"] = (
        result["shared_artifact"]["passed"] and result["delete_failure_and_retry"]["passed"]
        and contention["same_intent_contention"]["passed"]
        and contention["stale_cleaner_after_reregistration"]["passed"]
        and contention["cancellation_safety"]["lock_acquisition"]["passed"]
        and contention["cancellation_safety"]["delete_cancel_and_reregister"]["passed"]
        and result["expired_position_evidence"]["passed"]
    )
    return result


async def _release(factory, lease) -> None:
    async with factory() as uow:
        await uow.agents.release_lease(lease)
        await uow.commit()


async def _position_counts(engine, topic: str) -> dict[str, int]:
    async with engine.connect() as connection:
        row = (await connection.execute(text("""
            SELECT (SELECT count(*) FROM position_versions WHERE topic_id=:topic) AS versions,
                   (SELECT count(*) FROM position_commit_receipts WHERE receipt->>'topic_id'=:topic) AS receipts,
                   (SELECT count(*) FROM task_responses r JOIN tasks t ON t.id=r.task_id WHERE t.topic_id=:topic) AS responses
        """), {"topic": topic})).mappings().one()
    return {key: int(row[key]) for key in row.keys()}


async def _competing_repository_commit(factory, *, scope, topic: str, task_id, registry_id, conclusion_id, body: PositionBody) -> bool:
    from hekate.domain.types import TopicId

    async with factory() as uow:
        current = await uow.knowledge.lock_topic(scope, TopicId(topic))
        if current != 0:
            await uow.commit()
            return False
        await uow.knowledge.append_position(PositionVersionRecord(
            scope=scope, topic_id=TopicId(topic), version=1, base_version=0, body=body,
            operation_id=OperationId(f"repository-race:{uuid.uuid4()}"), task_id=task_id,
            input_revision=1, registry_id=registry_id, conclusion_id=conclusion_id,
            reason_for_change="PostgreSQL concurrency probe", created_at=datetime.now(UTC),
        ))
        await uow.knowledge.cas_current(scope, TopicId(topic), 0, 1)
        await uow.commit()
        return True


async def _repository_race(factory, *, scope, topic: str, task_id, registry_id, conclusion_id) -> dict[str, object]:
    barrier = asyncio.Barrier(2)
    body = PositionBody(
        statement="One transaction wins the base version race.",
        confidence=Confidence(level="high", basis=("PostgreSQL row lock",)),
    )

    async def contend():
        await barrier.wait()
        return await _competing_repository_commit(
            factory, scope=scope, topic=topic, task_id=task_id,
            registry_id=registry_id, conclusion_id=conclusion_id, body=body,
        )

    results = await asyncio.gather(contend(), contend())
    return {"outcomes": results, "winner_count": sum(results), "passed": sum(results) == 1}


async def _verify_commit_receipt_replay(factory, engine, task_id: TaskId):
    from hekate.application.budgets import _restore_binding

    async with factory() as uow:
        row = (await uow.session.execute(text("""
            SELECT p.operation_id, p.attempt_id, tr.proposal, tr.conclusion_id, cm.manifest,
                   c.capsule, o.id AS runtime_operation_id
            FROM task_preparations p
            JOIN turn_results tr ON tr.operation_id=p.operation_id
            JOIN context_manifests cm ON cm.operation_id=p.operation_id
            JOIN conclusions c ON c.id=tr.conclusion_id
            JOIN operations o ON o.id=p.operation_id
            WHERE p.task_id=:task AND p.input_revision=1
        """), {"task": str(task_id)})).mappings().one()
        operation = await uow.delivery.lock_operation(OperationId(row["operation_id"]))
        binding = _restore_binding(operation)
        attempt = await uow.tasks.get_attempt(AttemptId(row["attempt_id"]), for_update=True)
        await uow.commit()
    proposal = row["proposal"]
    request = PositionCommitRequest.model_validate_json(json.dumps({
        "schema_version": "1", "operation_id": proposal["operation_id"],
        "task_id": proposal["task_id"], "topic_id": proposal["topic_id"],
        "base_version": proposal["base_version"], "input_revision": proposal["input_revision"],
        "proposed_position": proposal["proposed_position"], "reason_for_change": proposal["reason_for_change"],
    }, separators=(",", ":")), strict=True)
    capsule = ConclusionCapsule.model_validate_json(json.dumps(row["capsule"], separators=(",", ":")), strict=True)
    actor = ActorContext(
        principal_id=binding.principal_id, scope=binding.scope,
        authenticated_agent_registry_id=binding.agent_registry_id,
        task_id=binding.task_id, attempt_id=binding.attempt_id,
        input_revision=binding.input_revision, policy_version=binding.policy_version,
        authz_epoch=binding.authz_epoch, fence=binding.fence,
    )
    async with engine.connect() as connection:
        before = (await connection.execute(text("""
            SELECT (SELECT count(*) FROM position_versions) AS versions,
                   (SELECT count(*) FROM position_commit_receipts) AS receipts,
                   (SELECT count(*) FROM task_responses) AS responses,
                   (SELECT count(*) FROM provider_calls) AS provider_calls
        """))).mappings().one()
    receipt = await positions.propose_commit(
        factory, actor, request, runtime_operation_id=OperationId(row["runtime_operation_id"]),
        binding=binding, attempt=attempt, conclusion_id=DomainId(row["conclusion_id"]),
        conclusion_evidence_used=capsule.evidence_used, manifest=row["manifest"],
    )
    async with engine.connect() as connection:
        counts = (await connection.execute(text("""
            SELECT (SELECT count(*) FROM position_versions) AS versions,
                   (SELECT count(*) FROM position_commit_receipts) AS receipts,
                   (SELECT count(*) FROM task_responses) AS responses,
                   (SELECT count(*) FROM provider_calls) AS provider_calls
        """))).mappings().one()
    return receipt.model_dump(mode="json"), {key: int(before[key]) for key in before.keys()}, {key: int(counts[key]) for key in counts.keys()}


async def _code_identity() -> str:
    code_paths = ["src", "scripts", "tests", "migrations", "contracts", "bridge"]
    diff = subprocess.check_output(["git", "diff", "HEAD", "--binary", "--", *code_paths], cwd=ROOT)
    paths = subprocess.check_output(
        ["git", "ls-files", "--others", "--exclude-standard", "--", *code_paths],
        cwd=ROOT, text=True,
    ).splitlines()
    digest = hashlib.sha256(diff)
    for name in sorted(paths):
        path = ROOT / name
        if path.is_file():
            digest.update(name.encode())
            digest.update(path.read_bytes())
    return digest.hexdigest()


async def _run(
    database_url: str, node: str, image: str, node_archive: Path,
    artifact: Path, run_id: str, *, hardening_only: bool = False,
) -> dict[str, object]:
    if not artifact.is_absolute():
        artifact = ROOT / artifact
    branch = p3.p1.run(["git", "branch", "--show-current"])
    head = p3.p1.run(["git", "rev-parse", "HEAD"])
    report: dict[str, object] = {
        "schema_version": "1", "probe": "phase4-evidence-position",
        "run_id": run_id, "executed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "base_sha": "675cadfb1606884c6b7d3ff32bec2b8bb2b1630e", "checked_out_head": head,
        "code_fingerprint_sha256": await _code_identity(), "branch": branch,
        "real_provider_calls": 0, "fake_provider_requests": 0,
        "execution": {
            "command": "uv run python scripts/phase4_evidence_position_probe.py" + (" --hardening-only" if hardening_only else ""),
            "environment_variables": ["HEKATE_TEST_DATABASE_URL", "HEKATE_NODE_BIN", "HEKATE_NODE_ARCHIVE"],
            "result": "running",
        },
        "results": {}, "overall_status": "blocked",
        "limitations": [
            "Provider traffic used only the pinned local App Server, private permit gateway, and synthetic fake provider.",
            "Production dispatch remains closed; no real provider credentials or route were configured.",
            "G7 same-execution resume and G8 exact full-request tokenization remain follow-up boundaries.",
            "Task frame byte limits are not asserted as exact input token limits.",
            "Letta memory projection remains unsupported and durably pending; PostgreSQL is authoritative.",
        ],
    }
    engine = bridge = sandbox = fake = gateway_server = gateway_task = gateway_app = None
    temporary = tempfile.TemporaryDirectory(prefix="hekate-phase4-")
    state_path = Path(temporary.name)
    evidence_archive = state_path / "archive"
    worker_tasks: list[tuple[asyncio.Task, asyncio.Event]] = []
    try:
        runtime_lock = p3._locked_runtime(image, node, node_archive)
        report["runtime"] = runtime_lock
        p3.p1.build_bridge(node)
        engine = create_engine(database_url)
        factory = create_uow_factory(engine)
        await p3._truncate(factory)
        async with engine.connect() as connection:
            head_value = await connection.scalar(text("SELECT version_num FROM alembic_version LIMIT 1"))
            pg_version = await connection.scalar(text("SHOW server_version"))
        if head_value != "0006_phase4_knowledge":
            raise ValueError("Phase 4 database migration head is not current")
        report["database"] = {"database_name": make_url(database_url).database, "postgres_version": pg_version, "migration_head": head_value}

        scope_text, principal_text = f"phase4:{run_id}", f"principal:{run_id}"
        policy_version = "phase4-fake-policy-v1"
        async with factory() as uow:
            await uow.tasks.insert_scope(AuthorizationSnapshot(
                scope=ScopeId(scope_text), principal_id=PrincipalId(principal_text),
                policy_version=policy_version, authz_epoch=1,
            ))
            await uow.tasks.insert_scope(AuthorizationSnapshot(
                scope=ScopeId(f"phase4-other:{run_id}"), principal_id=PrincipalId(f"other:{run_id}"),
                policy_version=policy_version, authz_epoch=1,
            ))
            await uow.commit()

        config_dir = state_path / "config"
        config_dir.mkdir()
        (config_dir / "local.yaml").write_text(yaml.safe_dump({"identity": {
            "principal_id": principal_text, "scope_id": scope_text,
            "policy_version": policy_version, "authz_epoch": 1,
        }}), encoding="utf-8")
        (config_dir / "policy.yaml").write_text(yaml.safe_dump({"limits": {
            "task_budget_usd": "1.00", "system_daily_budget_usd": "10.00", "task_deadline_seconds": 240,
        }}), encoding="utf-8")
        (config_dir / "models.yaml").write_text(yaml.safe_dump({"hekate": {
            "profile_id": "phase4-fake-v1", "model": f"openai-compatible/{p3.FAKE_MODEL}",
            "provider_model": p3.FAKE_MODEL, "max_input_tokens": 32768,
            "max_output_tokens": 2048, "max_compaction_calls": 1,
        }}), encoding="utf-8")
        (config_dir / "pricing.yaml").write_text(yaml.safe_dump({"version": "phase4-synthetic-v1", "prices": {
            p3.FAKE_MODEL: {"input_usd_per_million": "1", "output_usd_per_million": "2"},
        }}), encoding="utf-8")

        evidence_file = state_path / "evidence.txt"
        evidence_text = "Primary source: the amber seal remains valid through the listed date."
        evidence_file.write_text(evidence_text + "\n", encoding="utf-8")
        env = os.environ.copy()
        env.update({
            "HEKATE_DATABASE_URL": database_url, "HEKATE_NODE_BIN": node,
            "HEKATE_BRIDGE_ENTRY": str(ROOT / "bridge/letta/dist/main.js"),
            "HEKATE_WORKER_ID": f"phase4-worker-{run_id}", "HEKATE_RUNTIME_MODE": "test",
            "HEKATE_CONFIG_DIR": str(config_dir), "HEKATE_ARCHIVE_DIR": str(evidence_archive),
        })
        env["PATH"] = f"{Path(node).parent}:{env.get('PATH', '')}"
        settings = load_settings(env, config_dir)
        actor = configured_local_actor(settings)
        execution_config = configured_task_execution(settings)

        expected_evidence: dict[str, str] = {}
        turn_observations: list[dict[str, object]] = []
        fake = p3.FakeProvider()
        fake.set_response_factory(lambda request: _commit_output(request, turn_observations, expected_evidence))
        fake.start()
        sandbox = p3.Phase3Sandbox(state_path, run_id, image)
        sandbox.start_network()
        gateway_port = p3.reserve_port(sandbox.gateway_address)
        private_token = __import__("secrets").token_urlsafe(40)
        profile = ProviderGatewayProfile(
            profile_id="phase4-fake-v1",
            price_table=p3.PriceTable(
                model=p3.FAKE_MODEL, version="phase4-synthetic-v1",
                input_usd_per_million=Decimal("1"), output_usd_per_million=Decimal("2"), synthetic=True,
            ),
            upstream_base_url=f"http://127.0.0.1:{fake.port}", upstream_api_key="isolated-fake-only",
            max_input_tokens=32768, max_output_tokens=2048, test_only=True,
        )
        gateway_app = p3.create_provider_gateway(factory, profile, private_token, allow_test_profile=True)
        gateway_server, gateway_task = await p3.start_gateway_server(gateway_app, sandbox.gateway_address, gateway_port)
        sandbox.start_pinned_app_server(gateway_port, private_token)
        letta_url = f"ws://{sandbox.container_address}:{p3.p1.APP_PORT}"
        bridge = BridgeClient(node, ROOT / "bridge/letta/dist/main.js", env={
            "HEKATE_LETTA_URL": letta_url, "HEKATE_LETTA_TOKEN": private_token,
            "HEKATE_REQUIRE_PROVIDER_BINDING": "1",
        })
        runtime = LettaRuntimeAdapter(bridge)
        await runtime.verify_compatibility()
        from hekate.bootstrap import Container
        container = Container(settings=settings, runtime=runtime, uow_factory=factory, database=engine)
        worker = settings.worker_id

        evidence_request_key = f"phase4-{run_id}-evidence"
        expiration = (datetime.now(UTC) + timedelta(days=1)).isoformat().replace("+00:00", "Z")
        imported = await p3b._cli(env, "evidence", "import", str(evidence_file), "--request-key", evidence_request_key,
                                  "--kind", "document", "--retention-class", "fixture-retained", "--expires-at", expiration)
        evidence_id = str(imported["id"])
        expected_evidence[evidence_id] = "Primary source: the amber seal"
        replay_import = await p3b._cli(env, "evidence", "import", str(evidence_file), "--request-key", evidence_request_key,
                                       "--kind", "document", "--retention-class", "fixture-retained", "--expires-at", expiration)
        changed_import_conflict = False
        changed_file = state_path / "changed.txt"
        changed_file.write_text("Different bytes under the same import request key.", encoding="utf-8")
        try:
            await p3b._cli(env, "evidence", "import", str(changed_file), "--request-key", evidence_request_key,
                           "--kind", "document", "--retention-class", "fixture-retained", "--expires-at", expiration)
        except RuntimeError:
            changed_import_conflict = True

        derived_file = state_path / "derived.txt"
        derived_text = "Derived review: the seal condition is satisfied."
        derived_file.write_text(derived_text + "\n", encoding="utf-8")
        derived_id = EvidenceId(str(uuid.uuid4()))
        derived_bytes = derived_file.read_bytes()
        derived = EvidenceInput(
            id=derived_id, kind="summary", source_uri=derived_file.resolve().as_uri(),
            retrieved_at=datetime.now(UTC), content_hash=hashlib.sha256(derived_bytes).hexdigest(),
            derived_from=(EvidenceId(evidence_id),), access_scope="client-claimed-scope",
            retention_class="fixture-retained", expiry_at=datetime.now(UTC) + timedelta(days=1),
        )
        derived_record = await evidence_app.register(
            factory, actor, derived, derived_bytes, evidence_archive,
            request_key=f"phase4-{run_id}-derived",
        )
        expected_evidence[str(derived_id)] = "Derived review: the seal"
        view = await evidence_app.read_scoped(factory, actor, EvidenceId(evidence_id), ReadLimits(), evidence_archive)
        resolved = await evidence_app.resolve_references(factory, actor, (EvidenceId(evidence_id), derived_id))
        other_actor = ActorContext(
            principal_id=PrincipalId(f"other:{run_id}"), scope=ScopeId(f"phase4-other:{run_id}"),
            authenticated_agent_registry_id=None, task_id=None, attempt_id=None, input_revision=None,
            policy_version=policy_version, authz_epoch=1, fence=0,
        )
        scope_denied = False
        try:
            await evidence_app.read_scoped(factory, other_actor, EvidenceId(evidence_id), ReadLimits(), evidence_archive)
        except PolicyDenied:
            scope_denied = True
        report["results"]["evidence_registration_and_scope"] = {
            "evidence_id": evidence_id, "derived_evidence_id": str(derived_id),
            "same_request_key_same_content_returns_same_id": replay_import["id"] == evidence_id,
            "same_request_key_changed_content_conflicts": changed_import_conflict,
            "access_scope_is_server_owned": derived_record.access_scope == f"scope:{scope_text}",
            "derived_root_source_ids": [str(item) for item in derived_record.root_source_ids],
            "root_source_preserved": derived_record.root_source_ids == (EvidenceId(evidence_id),),
            "bounded_readable_view": view.readable and not view.truncated and evidence_text.strip() in (view.content or ""),
            "reference_validation": resolved.valid and len(resolved.references) == 2,
            "cross_scope_body_denied": scope_denied,
            "passed": replay_import["id"] == evidence_id and changed_import_conflict
            and derived_record.access_scope == f"scope:{scope_text}"
            and derived_record.root_source_ids == (EvidenceId(evidence_id),)
            and view.readable and resolved.valid and scope_denied,
        }

        evidence_ids = (evidence_id, str(derived_id))
        if hardening_only:
            baseline_key = f"phase4-{run_id}-hardening-baseline"
            baseline = await _ask(env, baseline_key, f"hardening-baseline-{run_id}", evidence_ids)
            baseline_task = TaskId(str(baseline["receipt"]["task_id"]))
            baseline_view, maintenance_failover = await _run_worker_after_maintenance_failure(
                container, factory, actor, baseline_task,
            )
            baseline_position = await positions.read_current(factory, actor, TopicId(f"hardening-baseline-{run_id}"))
            baseline_passed = baseline_view["state"] == "COMPLETED" and baseline_position.current_version == 1 and maintenance_failover["injected_failure"]
            report["results"]["hardening_baseline_commit"] = {
                "task_id": str(baseline_task), "position_version": baseline_position.current_version,
                "task_state": baseline_view["state"], "worker_after_maintenance_failure": maintenance_failover,
                "passed": baseline_passed,
            }
            deadline_result = await _deadline_after_position_effect(
                factory, engine, container, actor, execution_config, env, worker, run_id, evidence_ids, fake,
            )
            report["results"]["deadline_after_position_effect"] = deadline_result
            adoption_result = await _task_adoption_failure_rolls_back_position(
                factory, engine, container, actor, execution_config, env, worker,
                run_id, evidence_ids, fake,
            )
            report["results"]["task_adoption_failure_rolls_back_position"] = adoption_result
            permit_result = await _permit_evidence_boundaries(
                factory, engine, container, actor, execution_config, env, worker, run_id,
                evidence_ids, sandbox.gateway_address, gateway_port, private_token, gateway_app, fake,
            )
            report["results"]["provider_permit_evidence_revalidation"] = permit_result
            maintenance_result = await _hardening_maintenance_checks(
                factory, engine, actor, evidence_archive, state_path, run_id,
                evidence_id, str(derived_id), evidence_file, evidence_text,
                f"hardening-baseline-{run_id}",
            )
            report["results"]["worker_evidence_maintenance"] = maintenance_result
            async with engine.connect() as connection:
                pending_projections = int(await connection.scalar(text(
                    "SELECT count(*) FROM outbox WHERE kind='position_projection' AND status='PENDING'"
                )))
                unresolved = int(await connection.scalar(text("""
                    SELECT count(*) FROM provider_calls p LEFT JOIN usage_projections u USING (accounting_call_id)
                    WHERE p.status<>'QUIESCENT' OR u.settlement_state IS DISTINCT FROM 'SETTLED'
                """)))
            focus_names = [
                "hardening_baseline_commit", "deadline_after_position_effect",
                "task_adoption_failure_rolls_back_position",
                "provider_permit_evidence_revalidation", "worker_evidence_maintenance",
            ]
            all_passed = all(bool(report["results"][name].get("passed")) for name in focus_names)
            report["position_projection"] = {
                "pending_projection_outbox_rows": pending_projections,
                "applied_watermark": 0, "projection_calls": 0,
                "passed": pending_projections > 0,
            }
            report["accounting"] = {
                "unresolved_or_unsettled_provider_calls": unresolved,
                "note": "The permit-consumption race intentionally leaves one issued, unconsumed fixture allocation; it was not normalized or settled without execution evidence.",
            }
            report["fake_provider"] = {
                "http_requests": fake.count(), "task_turn_requests": len(turn_observations),
                "real_provider_calls": 0, "gateway_route": "isolated-local-fake-only",
                "turn_observations": turn_observations,
            }
            report["overall_status"] = "pass" if all_passed and report["position_projection"]["passed"] else "failed"
            report["summary"] = {
                "required_scenarios": focus_names,
                "passed_count": sum(bool(report["results"][name].get("passed")) for name in focus_names),
                "failed_scenarios": [name for name in focus_names if not report["results"][name].get("passed")],
            }
            return report

        main_key = f"phase4-{run_id}-task-main"
        duplicate = await _ask(env, main_key, "main-topic", evidence_ids)
        duplicate_again = await _ask(env, main_key, "main-topic", evidence_ids)
        main_task = TaskId(str(duplicate["receipt"]["task_id"]))
        different_topic_conflict = False
        different_evidence_conflict = False
        try:
            await task_app.submit(
                factory, actor,
                UserMessage(text="Review this topic using the supplied records.", topic_id=TopicId("other-topic"), evidence_refs=tuple(EvidenceId(item) for item in evidence_ids)),
                main_key, execution_config,
            )
        except Conflict:
            different_topic_conflict = True
        try:
            await _ask(env, main_key, "main-topic", (evidence_id,))
        except RuntimeError:
            different_evidence_conflict = True

        first_view, maintenance_failover = await _run_worker_after_maintenance_failure(
            container, factory, actor, main_task,
        )
        fake_after_first = fake.count()
        first_position = await p3b._cli(env, "position", "show", "main-topic")
        first_history = await p3b._cli(env, "position", "history", "main-topic", "--after-version", "0", "--limit", "50")
        if first_view["state"] != "COMPLETED" or first_position["current_version"] != 1:
            raise AssertionError("first Task did not atomically create Position v1")

        counts_before_replay = await _position_counts(engine, "main-topic")
        accounting_before_replay = await p3b._accounting_effects(engine, main_task)
        async with engine.connect() as connection:
            inbox_id = await connection.scalar(text("SELECT source_inbox_id FROM task_responses WHERE task_id=:task"), {"task": str(main_task)})
        inbox_replay = await result_app.apply_turn_result(factory, str(inbox_id))
        replayed_commit, commit_counts_before, commit_counts_after = await _verify_commit_receipt_replay(factory, engine, main_task)
        await result_app.process_pending_results(factory)
        counts_after_replay = await _position_counts(engine, "main-topic")
        accounting_after_replay = await p3b._accounting_effects(engine, main_task)
        task_replay = await _ask(env, main_key, "main-topic", evidence_ids)
        report["results"]["position_v1_and_replay"] = {
            "task_id": str(main_task), "registry_id": first_position["current"]["registry_id"],
            "position_id": {"scope": scope_text, "topic_id": "main-topic", "version": first_position["current_version"]},
            "position_version": first_position["current_version"],
            "history_versions": [item["version"] for item in first_history["items"]],
            "task_response": first_view["response"], "duplicate_submission_same_task": duplicate_again["receipt"]["task_id"] == str(main_task),
            "worker_after_injected_maintenance_failure": maintenance_failover,
            "different_topic_same_key_conflicts": different_topic_conflict,
            "different_evidence_same_key_conflicts": different_evidence_conflict,
            "commit_receipt_replay": replayed_commit, "inbox_replay": inbox_replay,
            "commit_counts_before_receipt_replay": commit_counts_before,
            "commit_counts_after_receipt_replay": commit_counts_after,
            "counts_before_replay": counts_before_replay, "counts_after_replay": counts_after_replay,
            "accounting_before_replay": accounting_before_replay, "accounting_after_replay": accounting_after_replay,
            "provider_count_unchanged": fake.count() == fake_after_first,
            "task_request_replay_same_task": task_replay["receipt"]["task_id"] == str(main_task),
            "commit_receipt_inserted_once": commit_counts_before == commit_counts_after,
            "passed": first_view["state"] == "COMPLETED" and first_position["current_version"] == 1
            and maintenance_failover["injected_failure"]
            and [item["version"] for item in first_history["items"]] == [1]
            and duplicate_again["receipt"]["task_id"] == str(main_task) and different_topic_conflict and different_evidence_conflict
            and replayed_commit.get("replayed") is True and inbox_replay.get("state") == "ACCEPTED"
            and counts_before_replay == counts_after_replay and accounting_before_replay == accounting_after_replay
            and fake.count() == fake_after_first and task_replay["receipt"]["task_id"] == str(main_task),
        }

        # Restart the Python bridge/worker while retaining the pinned App Server's persistent HEKATE.
        await bridge.close()
        bridge = BridgeClient(node, ROOT / "bridge/letta/dist/main.js", env={
            "HEKATE_LETTA_URL": letta_url, "HEKATE_LETTA_TOKEN": private_token,
            "HEKATE_REQUIRE_PROVIDER_BINDING": "1",
        })
        runtime = LettaRuntimeAdapter(bridge)
        await runtime.verify_compatibility()
        container = Container(settings=settings, runtime=runtime, uow_factory=factory, database=engine)
        second_key = f"phase4-{run_id}-task-second"
        second = await _ask(env, second_key, "main-topic", evidence_ids, "Review the saved position and update only if needed.")
        second_task = TaskId(str(second["receipt"]["task_id"]))
        second_view = await _run_worker_until(container, factory, actor, second_task)
        second_position = await p3b._cli(env, "position", "show", "main-topic")
        history = await p3b._cli(env, "position", "history", "main-topic", "--after-version", "0", "--limit", "50")
        request_contexts = [item for item in turn_observations if item["topic_id"] == "main-topic"]
        async with engine.connect() as connection:
            identity = (await connection.execute(text("""
                SELECT count(DISTINCT r.registry_id) AS registries, count(DISTINCT a.provider_agent_id) AS providers
                FROM task_responses r JOIN agent_registry a ON a.id=r.registry_id
                WHERE r.task_id IN (:first,:second)
            """), {"first": str(main_task), "second": str(second_task)})).mappings().one()
            projection = (await connection.execute(text("""
                SELECT desired_version, applied_version, state, pending_reason
                FROM memory_projections WHERE scope=:scope AND topic_id='main-topic'
            """), {"scope": scope_text})).mappings().one()
            operation_ids = (await connection.execute(text("""
                SELECT task_id, operation_id FROM task_preparations WHERE task_id IN (:first,:second) ORDER BY task_id
            """), {"first": str(main_task), "second": str(second_task)})).mappings().all()
        report["results"]["restart_position_reuse_and_history"] = {
            "task_ids": [str(main_task), str(second_task)], "registry_provider_counts": dict(identity),
            "position_ids": [
                {"scope": scope_text, "topic_id": "main-topic", "version": item["version"]}
                for item in history["items"]
            ],
            "position_version": second_position["current_version"],
            "history_versions": [item["version"] for item in history["items"]],
            "history_provenance": [{key: item[key] for key in ("task_id", "input_revision", "registry_id", "conclusion_id", "operation_id", "reason_for_change")} for item in history["items"]],
            "history_evidence_refs": [[str(value) for value in item["evidence_refs"]] for item in history["items"]],
            "history_dissent_refs": [[str(value) for value in item["dissent_refs"]] for item in history["items"]],
            "provider_request_contexts": request_contexts,
            "runtime_operation_ids": [dict(item) for item in operation_ids],
            "projection": dict(projection),
            "worker_bridge_restarted": True,
            "passed": second_view["state"] == "COMPLETED" and second_position["current_version"] == 2
            and [item["version"] for item in history["items"]] == [1, 2]
            and identity["registries"] == identity["providers"] == 1
            and len(request_contexts) == 2 and request_contexts[0]["mode"] == "independent_exploration"
            and request_contexts[1]["mode"] == "targeted_review"
            and request_contexts[1]["target_position_version"] == 1
            and bool(request_contexts[1]["persistent_dissent_ids"])
            and projection["desired_version"] == 2 and projection["applied_version"] == 0
            and projection["state"] == "PENDING_UNSUPPORTED"
            and projection["pending_reason"] == "memory_projection_not_implemented",
        }

        immutable_update_rejected = immutable_delete_rejected = False
        async with engine.connect() as connection:
            transaction = await connection.begin()
            for statement, is_update in (
                ("UPDATE position_versions SET reason_for_change='tampered' WHERE scope=:scope AND topic_id='main-topic' AND version=1", True),
                ("DELETE FROM position_versions WHERE scope=:scope AND topic_id='main-topic' AND version=1", False),
            ):
                savepoint = await connection.begin_nested()
                rejected = False
                try:
                    await connection.execute(text(statement), {"scope": scope_text})
                except DBAPIError:
                    rejected = True
                await savepoint.rollback()
                if is_update:
                    immutable_update_rejected = rejected
                else:
                    immutable_delete_rejected = rejected
            await transaction.rollback()
        report["results"]["position_history_immutable"] = {
            "update_rejected": immutable_update_rejected,
            "delete_rejected": immutable_delete_rejected,
            "passed": immutable_update_rejected and immutable_delete_rejected,
        }

        first_conclusion_id = history["items"][0]["conclusion_id"]
        race = await _repository_race(
            factory, scope=ScopeId(scope_text), topic=f"repository-race-{run_id}",
            task_id=main_task, registry_id=RegistryId(first_position["current"]["registry_id"]),
            conclusion_id=DomainId(first_conclusion_id),
        )
        report["results"]["postgres_base_version_race"] = race

        rollback_topic = f"rollback-{run_id}"
        rollback_task, rollback_operation, rollback_lease = await _manual_dispatch(
            factory, container, actor, execution_config, env, worker,
            f"phase4-{run_id}-rollback", rollback_topic, evidence_ids,
        )
        rollback_failure = False
        from hekate.infrastructure.postgres.knowledge_repository import PostgresKnowledgeRepository
        append_position = PostgresKnowledgeRepository.append_position

        async def injected_failure(repository, record):
            await append_position(repository, record)
            if record.topic_id == TopicId(rollback_topic):
                raise RuntimeError("phase4 injected failure after Position insert")

        try:
            with patch.object(PostgresKnowledgeRepository, "append_position", new=injected_failure):
                await process_pending_results(factory)
        except RuntimeError as error:
            rollback_failure = "after Position insert" in str(error)
        rollback_before_retry = await _position_counts(engine, rollback_topic)
        async with engine.connect() as connection:
            rollback_state = (await connection.execute(text("""
                SELECT t.status, (SELECT count(*) FROM position_commit_receipts WHERE operation_id=:commit_op) AS receipt_count,
                       (SELECT count(*) FROM memory_projections WHERE topic_id=:topic) AS projection_count,
                       (SELECT count(*) FROM outbox WHERE kind='position_projection' AND operation_id=:operation) AS outbox_count
                FROM tasks t WHERE t.id=:task
            """), {"commit_op": f"{rollback_operation}:position.commit", "topic": rollback_topic,
                   "operation": str(rollback_operation), "task": str(rollback_task)})).mappings().one()
        rollback_atomic = rollback_failure and rollback_before_retry == {"versions": 0, "receipts": 0, "responses": 0} \
            and rollback_state["status"] == "RUNNING" and rollback_state["receipt_count"] == 0 \
            and rollback_state["projection_count"] == 0 and rollback_state["outbox_count"] == 0
        retry_processed = await process_pending_results(factory)
        rollback_after_retry = await _position_counts(engine, rollback_topic)
        await _release(factory, rollback_lease)
        report["results"]["position_task_atomic_rollback"] = {
            "task_id": str(rollback_task), "operation_id": str(rollback_operation),
            "injected_failure_after_position_insert": rollback_failure,
            "state_before_retry": dict(rollback_state), "counts_before_retry": rollback_before_retry,
            "retry_processed": retry_processed, "counts_after_retry": rollback_after_retry,
            "passed": rollback_atomic and retry_processed >= 1 and rollback_after_retry["versions"] == 1
            and rollback_after_retry["receipts"] == rollback_after_retry["responses"] == 1,
        }

        async def stale_case(name: str) -> dict[str, object]:
            topic = f"{name}-{run_id}"
            task_id, operation_id, lease = await _manual_dispatch(
                factory, container, actor, execution_config, env, worker,
                f"phase4-{run_id}-{name}", topic, evidence_ids,
            )
            if name == "cancel":
                await task_app.cancel(factory, actor, task_id, StopReason.USER_CANCELLED)
            elif name == "revision":
                await task_app.revise(factory, actor, task_id, 1, InputChange(
                    text="The user changed this Task before accepting its result.", expected_revision=1,
                ))
            else:
                async with engine.begin() as connection:
                    await connection.execute(text("UPDATE tasks SET deadline=now()-interval '1 second' WHERE id=:task"), {"task": str(task_id)})
            revision_context_preserved = None
            if name == "revision":
                async with engine.connect() as connection:
                    input_row = (await connection.execute(text(
                        "SELECT topic_id,evidence_refs FROM task_inputs WHERE task_id=:task AND revision=2"
                    ), {"task": str(task_id)})).mappings().one()
                revision_context_preserved = input_row["topic_id"] == topic and tuple(sorted(input_row["evidence_refs"])) == tuple(sorted(evidence_ids))
            async with engine.begin() as connection:
                await connection.execute(text(
                    "UPDATE turn_results SET next_attempt_at=now() WHERE operation_id=:operation"
                ), {"operation": str(operation_id)})
            processed = await process_pending_results(factory)
            counts = await _position_counts(engine, topic)
            view = await task_app.get_task(factory, actor, task_id)
            async with engine.connect() as connection:
                result_state = await connection.scalar(text("SELECT processing_state FROM turn_results WHERE operation_id=:operation"), {"operation": str(operation_id)})
            if name == "revision" and view["state"] == "RUNNING":
                await task_app.cancel(factory, actor, task_id, StopReason.USER_CANCELLED)
                view = await task_app.get_task(factory, actor, task_id)
            await _release(factory, lease)
            return {
                "task_id": str(task_id), "operation_id": str(operation_id), "task_state_after_boundary": view["state"],
                "result_state": result_state, "position_counts": counts, "processed": processed,
                "revision_preserved_topic_and_evidence": revision_context_preserved,
                "passed": result_state == "LATE" and counts["versions"] == 0 and counts["receipts"] == 0
                and counts["responses"] == 0 and (name != "revision" or revision_context_preserved),
            }

        cancellation = await stale_case("cancel")
        revision = await stale_case("revision")
        deadline = await stale_case("deadline")
        report["results"]["cancel_revision_deadline_prevent_commit"] = {
            "cancel": cancellation, "revision": revision, "deadline": deadline,
            "passed": all(item["passed"] for item in (cancellation, revision, deadline)),
        }

        deadline_after_effect = await _deadline_after_position_effect(
            factory, engine, container, actor, execution_config, env, worker,
            run_id, evidence_ids, fake,
        )
        report["results"]["deadline_after_position_effect"] = deadline_after_effect
        adoption_after_effect = await _task_adoption_failure_rolls_back_position(
            factory, engine, container, actor, execution_config, env, worker,
            run_id, evidence_ids, fake,
        )
        report["results"]["task_adoption_failure_rolls_back_position"] = adoption_after_effect

        conflict_topic = f"conflict-{run_id}"
        conflict_task, conflict_operation, conflict_lease = await _manual_dispatch(
            factory, container, actor, execution_config, env, worker,
            f"phase4-{run_id}-version-conflict", conflict_topic, evidence_ids,
        )
        async with engine.connect() as connection:
            preparation = (await connection.execute(text("""
                SELECT p.attempt_id, tr.conclusion_id, o.binding
                FROM task_preparations p JOIN turn_results tr ON tr.operation_id=p.operation_id
                JOIN operations o ON o.id=p.operation_id WHERE p.task_id=:task
            """), {"task": str(conflict_task)})).mappings().one()
        from hekate.domain.capsules import parse_hekate_turn_output
        async with engine.connect() as connection:
            raw = await connection.scalar(text("SELECT raw_output FROM turn_results WHERE operation_id=:operation"), {"operation": str(conflict_operation)})
        parsed_output = parse_hekate_turn_output(raw.encode())
        async with factory() as uow:
            await uow.knowledge.lock_topic(ScopeId(scope_text), TopicId(conflict_topic))
            await uow.knowledge.append_position(PositionVersionRecord(
                scope=ScopeId(scope_text), topic_id=TopicId(conflict_topic), version=1, base_version=0,
                body=PositionBody(statement="Competing saved Position.", confidence=Confidence(
                    level="high", basis=("competing transaction",),
                )),
                operation_id=OperationId(f"competing:{run_id}"), task_id=conflict_task, input_revision=1,
                registry_id=RegistryId(first_position["current"]["registry_id"]),
                conclusion_id=DomainId(preparation["conclusion_id"]), reason_for_change="Competing transaction",
                created_at=datetime.now(UTC),
            ))
            await uow.knowledge.cas_current(ScopeId(scope_text), TopicId(conflict_topic), 0, 1)
            await uow.commit()
        await process_pending_results(factory)
        conflict_view = await task_app.get_task(factory, actor, conflict_task)
        conflict_position = await positions.read_current(factory, actor, TopicId(conflict_topic))
        async with engine.connect() as connection:
            conflict_receipt = await connection.scalar(text("SELECT receipt FROM position_commit_receipts WHERE operation_id=:operation"), {"operation": str(parsed_output.proposal.operation_id)})
        await _release(factory, conflict_lease)
        report["results"]["version_conflict_closes_for_user"] = {
            "task_id": str(conflict_task), "commit_operation_id": str(parsed_output.proposal.operation_id),
            "task_state": conflict_view["state"], "task_outcome": conflict_view["outcome"],
            "current_version": conflict_position.current_version,
            "current_statement": conflict_position.current.body.statement if conflict_position.current else None,
            "receipt": conflict_receipt,
            "passed": conflict_view["state"] == "COMPLETED" and conflict_view["outcome"] == "NEEDS_USER_INPUT"
            and conflict_position.current_version == 1 and conflict_position.current.body.statement == "Competing saved Position."
            and conflict_receipt["conflict"] is True and conflict_receipt["current_version"] == 1,
        }

        auth_topic = f"authorization-change-{run_id}"
        auth_task, auth_operation, auth_lease, auth_job = await _manual_prepare(
            factory, container, actor, execution_config, env, worker,
            f"phase4-{run_id}-authorization-change", auth_topic, (),
        )
        async with engine.begin() as connection:
            await connection.execute(text(
                "UPDATE authorization_scopes SET authz_epoch=2 WHERE id=:scope"
            ), {"scope": scope_text})
        provider_before_auth_denial = fake.count()
        await dispatch_job(container, auth_job, worker)
        async with engine.begin() as connection:
            await connection.execute(text(
                "UPDATE authorization_scopes SET authz_epoch=1 WHERE id=:scope"
            ), {"scope": scope_text})
        auth_task_view = await task_app.get_task(factory, actor, auth_task)
        async with engine.connect() as connection:
            auth_provider_rows = await connection.scalar(text(
                "SELECT count(*) FROM provider_calls WHERE operation_id=:operation"
            ), {"operation": str(auth_operation)})
        await _release(factory, auth_lease)
        report["results"]["authorization_change_blocks_provider"] = {
            "task_id": str(auth_task), "operation_id": str(auth_operation),
            "task_state": auth_task_view["state"], "stop_reason": auth_task_view["stop_reason"],
            "provider_requests_before_and_after": [provider_before_auth_denial, fake.count()],
            "provider_call_rows": auth_provider_rows,
            "passed": auth_task_view["state"] == "FAILED" and auth_task_view["stop_reason"] == "POLICY"
            and provider_before_auth_denial == fake.count() and auth_provider_rows == 0,
        }

        shared_file = evidence_file
        shared_expiry = (datetime.now(UTC) + timedelta(days=2)).isoformat().replace("+00:00", "Z")
        shared = await p3b._cli(env, "evidence", "import", str(shared_file), "--request-key", f"phase4-{run_id}-shared",
                                "--kind", "document", "--retention-class", "fixture-retained", "--expires-at", shared_expiry)
        shared_id = EvidenceId(str(shared["id"]))
        async with engine.begin() as connection:
            await connection.execute(text("UPDATE evidence SET expiry_at=now()-interval '1 second' WHERE id=:id"), {"id": str(shared_id)})
        first_expire = await run_evidence_maintenance_tick(factory, evidence_archive)
        async with engine.connect() as connection:
            shared_artifact_state = await connection.scalar(text("SELECT storage_state FROM artifacts WHERE artifact_ref=:ref"), {"ref": imported["artifact_ref"]})
        commit_topic = f"expired-commit-{run_id}"
        expired_result_task, expired_result_operation, expired_result_lease = await _manual_dispatch(
            factory, container, actor, execution_config, env, worker,
            f"phase4-{run_id}-expiry-before-commit", commit_topic, evidence_ids,
        )
        stale_topic = f"expired-reference-{run_id}"
        stale_task, stale_operation, stale_lease, stale_job = await _manual_prepare(
            factory, container, actor, execution_config, env, worker,
            f"phase4-{run_id}-expiry-after-prepare", stale_topic, evidence_ids,
        )
        async with engine.begin() as connection:
            await connection.execute(text(
                "UPDATE evidence SET expiry_at=now()-interval '1 second' WHERE id IN (:primary,:derived)"
            ), {"primary": evidence_id, "derived": str(derived_id)})
        with patch.object(archive_store, "delete_locked", side_effect=OSError("injected archive delete failure")):
            expiry_failure = await run_evidence_maintenance_tick(factory, evidence_archive)
        expired_result_processed = await process_pending_results(factory)
        expired_result_view = await task_app.get_task(factory, actor, expired_result_task)
        expired_result_counts = await _position_counts(engine, commit_topic)
        async with engine.connect() as connection:
            expired_result_state = await connection.scalar(text(
                "SELECT processing_state FROM turn_results WHERE operation_id=:operation"
            ), {"operation": str(expired_result_operation)})
        provider_before_expired_dispatch = fake.count()
        await dispatch_job(container, stale_job, worker)
        expired_task_view = await task_app.get_task(factory, actor, stale_task)
        await _release(factory, stale_lease)
        expiry_retry = await run_evidence_maintenance_tick(factory, evidence_archive)
        deleted_read_denied = False
        try:
            await evidence_app.read_scoped(factory, actor, EvidenceId(evidence_id), ReadLimits(), evidence_archive)
        except PolicyDenied:
            deleted_read_denied = True
        main_history_after_expiry = await positions.read_history(factory, actor, TopicId("main-topic"), VersionCursor(after_version=0, limit=50))
        async with engine.connect() as connection:
            archive_state = await connection.scalar(text("SELECT storage_state FROM artifacts WHERE artifact_ref=:ref"), {"ref": imported["artifact_ref"]})
            metadata_state = await connection.scalar(text("SELECT availability FROM evidence WHERE id=:id"), {"id": evidence_id})
            derived_state = await connection.scalar(text("SELECT availability FROM evidence WHERE id=:id"), {"id": str(derived_id)})
            expired_position_refs = await connection.scalar(text("SELECT count(*) FROM position_evidence WHERE evidence_id=:id"), {"id": evidence_id})
            stale_operation_calls = await connection.scalar(text("SELECT count(*) FROM provider_calls WHERE operation_id=:operation"), {"operation": str(stale_operation)})
        maintenance_races = await _maintenance_contention_scenarios(
            factory, engine, actor, evidence_archive, state_path, run_id,
        )
        report["results"]["expiry_reference_and_archive_retry"] = {
            "expired_shared_evidence_id": str(shared_id), "shared_artifact_state_while_primary_active": shared_artifact_state,
            "expired_shared_run": first_expire, "delete_failure_run": expiry_failure,
            "retry_run": expiry_retry, "archive_state_after_retry": archive_state,
            "evidence_tombstone_state": metadata_state, "derived_evidence_tombstone_state": derived_state,
            "position_reference_count_after_expiry": expired_position_refs,
            "maintenance_contention": maintenance_races,
            "expired_result_commit": {
                "task_id": str(expired_result_task), "operation_id": str(expired_result_operation),
                "task_state": expired_result_view["state"], "stop_reason": expired_result_view["stop_reason"],
                "result_state": expired_result_state, "position_counts": expired_result_counts,
                "processed": expired_result_processed,
            },
            "expired_prepared_task_state": expired_task_view["state"],
            "provider_requests_before_and_after_expired_dispatch": [provider_before_expired_dispatch, fake.count()],
            "provider_call_rows_for_expired_dispatch": stale_operation_calls,
            "position_history_versions_after_expiry": [item.version for item in main_history_after_expiry.items],
            "expired_read_denied": deleted_read_denied,
            "passed": first_expire["expired_evidence"] == 1 and shared_artifact_state == "AVAILABLE"
            and expiry_failure["expired_evidence"] == 2 and expiry_failure["failed_artifacts"] and expiry_retry["deleted_artifacts"]
            and archive_state == "DELETED" and metadata_state == derived_state == "EXPIRED" and expired_position_refs == 4
            and expired_result_view["state"] == "FAILED" and expired_result_view["stop_reason"] == "POLICY"
            and expired_result_state == "REJECTED" and expired_result_processed >= 1
            and expired_result_counts["versions"] == expired_result_counts["receipts"] == 0
            and expired_result_counts["responses"] == 1
            and expired_task_view["state"] == "FAILED" and expired_task_view["stop_reason"] == "POLICY"
            and fake.count() == provider_before_expired_dispatch and stale_operation_calls == 0
            and [item.version for item in main_history_after_expiry.items] == [1, 2] and deleted_read_denied
            and maintenance_races["same_intent_contention"]["passed"]
            and maintenance_races["stale_cleaner_after_reregistration"]["passed"]
            and maintenance_races["cancellation_safety"]["lock_acquisition"]["passed"]
            and maintenance_races["cancellation_safety"]["delete_cancel_and_reregister"]["passed"],
        }

        permit_file = state_path / f"permit-evidence-{run_id}.txt"
        permit_content = f"Permit boundary evidence {run_id}.".encode("utf-8")
        permit_evidence = await _register_fixture_evidence(
            factory, actor, permit_file, evidence_archive, permit_content,
            f"phase4-{run_id}-permit-evidence",
        )
        permit_boundaries = await _permit_evidence_boundaries(
            factory, engine, container, actor, execution_config, env, worker, run_id,
            (str(permit_evidence.id),), sandbox.gateway_address, gateway_port,
            private_token, gateway_app, fake,
        )
        report["results"]["provider_permit_evidence_revalidation"] = permit_boundaries

        async with factory() as uow:
            pending_claim = await uow.delivery.claim_jobs(worker, 20, 30)
            await uow.commit()
        async with engine.connect() as connection:
            unsupported_projection = await connection.scalar(text("""
                SELECT count(*) FROM outbox WHERE kind='position_projection' AND status='PENDING'
            """))
            receipt_count = await connection.scalar(text("SELECT count(*) FROM position_commit_receipts"))
            position_count = await connection.scalar(text("SELECT count(*) FROM position_versions"))
            unresolved = await connection.execute(text("""
                SELECT count(*) FROM provider_calls p LEFT JOIN usage_projections u USING (accounting_call_id)
                WHERE p.status<>'QUIESCENT' OR u.settlement_state IS DISTINCT FROM 'SETTLED'
            """))
            unresolved_count = int(unresolved.scalar_one())
            provider_count_db = int(await connection.scalar(text("SELECT count(*) FROM provider_calls")))
        report["fake_provider"] = {
            "http_requests": fake.count(), "task_turn_requests": len(turn_observations),
            "real_provider_calls": 0, "gateway_route": "isolated-local-fake-only",
            "turn_observations": turn_observations,
        }
        report["position_projection"] = {
            "pending_projection_outbox_rows": int(unsupported_projection),
            "dispatch_claimed_rows": [job.kind for job in pending_claim],
            "applied_watermark": 0, "projection_calls": 0,
            "passed": int(unsupported_projection) > 0 and not any(job.kind != "dispatch" for job in pending_claim),
        }
        report["accounting"] = {
            "unresolved_or_unsettled_provider_calls": unresolved_count,
            "total_provider_calls_db": provider_count_db,
            "position_commit_rows": int(position_count), "position_commit_receipts": int(receipt_count),
            "pending_settlement_note": "The permit-consumption boundary probe intentionally leaves one ISSUED/ALLOCATED fixture call without execution evidence. The report preserves it; no UNKNOWN or unsettled row was normalized.",
        }
        named_results = [
            "evidence_registration_and_scope", "position_v1_and_replay", "restart_position_reuse_and_history",
            "position_history_immutable", "postgres_base_version_race", "position_task_atomic_rollback",
            "cancel_revision_deadline_prevent_commit", "deadline_after_position_effect",
            "task_adoption_failure_rolls_back_position",
            "version_conflict_closes_for_user", "authorization_change_blocks_provider",
            "expiry_reference_and_archive_retry", "provider_permit_evidence_revalidation",
        ]
        passed = all(bool(report["results"].get(name, {}).get("passed")) for name in named_results)
        passed = passed and bool(report["position_projection"]["passed"]) and len(turn_observations) == 10 and fake.count() >= len(turn_observations)
        report["overall_status"] = "pass" if passed else "failed"
        report["summary"] = {
            "required_scenarios": named_results,
            "passed_count": sum(bool(report["results"].get(name, {}).get("passed")) for name in named_results),
            "failed_scenarios": [name for name in named_results if not report["results"].get(name, {}).get("passed")],
            "task_operation_ids": [dict(item) for item in operation_ids],
            "final_main_position_versions": [item.version for item in main_history_after_expiry.items],
        }
    except BaseException as error:
        report["overall_status"] = "failed"
        report["error"] = {"type": type(error).__name__, "message": str(error)[:2000], "traceback": traceback.format_exc()[-6000:]}
    finally:
        for task, stop in worker_tasks:
            stop.set()
            if not task.done():
                await asyncio.gather(task, return_exceptions=True)
        if bridge is not None:
            await bridge.close()
        if gateway_server is not None:
            gateway_server.should_exit = True
            if gateway_task is not None:
                await asyncio.gather(gateway_task, return_exceptions=True)
        if fake is not None:
            fake.release_block.set()
            fake.stop()
        if sandbox is not None:
            sandbox.stop()
            try:
                sandbox.clear_state()
            except Exception as error:
                report["cleanup_error"] = type(error).__name__
        if engine is not None:
            try:
                async with engine.connect() as connection:
                    report["final_counts"] = {
                        "tasks": await connection.scalar(text("SELECT count(*) FROM tasks")),
                        "evidence": await connection.scalar(text("SELECT count(*) FROM evidence")),
                        "position_versions": await connection.scalar(text("SELECT count(*) FROM position_versions")),
                        "position_receipts": await connection.scalar(text("SELECT count(*) FROM position_commit_receipts")),
                        "provider_calls": await connection.scalar(text("SELECT count(*) FROM provider_calls")),
                    }
            except Exception as error:
                report["final_measurement_error"] = type(error).__name__
            await engine.dispose()
        temporary.cleanup()
        report["fake_provider_requests"] = fake.count() if fake is not None else 0
        report["real_provider_calls"] = 0
        report["executed_at_finished"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        report["execution"]["result"] = report["overall_status"]
        report["artifact"] = artifact.relative_to(ROOT).as_posix()
        artifact.parent.mkdir(parents=True, exist_ok=True)
        artifact.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Exercise Phase 4 Evidence and Position through PostgreSQL and the pinned fake-provider runtime.")
    parser.add_argument("--database-url", default=os.environ.get("HEKATE_TEST_DATABASE_URL", ""))
    parser.add_argument("--node-bin", default=os.environ.get("HEKATE_NODE_BIN", ""))
    parser.add_argument("--node-archive", type=Path, default=Path(os.environ.get("HEKATE_NODE_ARCHIVE", "/tmp/hekate-node-v22.19.0-linux-x64.tar.xz")))
    parser.add_argument("--image", default=f"hekate/letta-code-p1:{p3.p1.LOCK['app_server']['source_commit'][:8]}-{p3.p1.LOCK['patches'][0]['sha256'][:8]}")
    parser.add_argument("--artifact", type=Path)
    parser.add_argument("--hardening-only", action="store_true", help="run only the three Phase 4 hardening boundaries through PostgreSQL and the pinned fake-provider runtime")
    args = parser.parse_args()
    if not args.database_url or not args.node_bin:
        parser.error("set HEKATE_TEST_DATABASE_URL and HEKATE_NODE_BIN")
    parsed = make_url(args.database_url)
    if not parsed.database.startswith("hekate_phase4_") or parsed.host not in {"127.0.0.1", "localhost"}:
        parser.error("probe requires a dedicated loopback hekate_phase4_* database")
    os.environ["PATH"] = f"{Path(args.node_bin).resolve().parent}{os.pathsep}{os.environ.get('PATH', '')}"
    run_id = f"p4-{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    artifact = args.artifact or ROOT / "integration/runtime/artifacts" / f"{run_id}.json"
    cfg = p3.Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("sqlalchemy.url", args.database_url.replace("%", "%%"))
    os.environ["HEKATE_DATABASE_URL"] = args.database_url
    p3.command.upgrade(cfg, "head")
    report = asyncio.run(_run(
        args.database_url, args.node_bin, args.image, args.node_archive, artifact, run_id,
        hardening_only=args.hardening_only,
    ))
    artifact_path = artifact if artifact.is_absolute() else ROOT / artifact
    print(json.dumps({"artifact": str(artifact_path.relative_to(ROOT)), "status": report["overall_status"], "fake_provider_requests": report.get("fake_provider_requests", 0), "real_provider_calls": 0}, sort_keys=True))
    return 0 if report["overall_status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
