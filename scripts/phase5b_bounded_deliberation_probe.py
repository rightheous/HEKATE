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
import traceback
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import make_url
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import phase3_runtime_probe as p3
import phase3b_single_hekate_probe as p3b
import phase4_evidence_position_probe as p4

from hekate.application import results as results_app
from hekate.application.budgets import _restore_binding
from hekate.application.lifecycle import _reservation_amount, reject_deliberation_step, request_critic
from hekate.domain.contracts import canonical_json_hash
from hekate.domain.errors import Conflict, UnknownExecution
from hekate.domain.capsules import parse_critic_turn_output, parse_hekate_turn_output
from hekate.domain.models import AuthorizationSnapshot, SpawnProposal
from hekate.domain.types import (
    ActorContext, AttemptId, DeploymentId, DomainId, OperationId, PrincipalId,
    RegistryId, ScopeId, TaskId,
)
from hekate.infrastructure.letta.adapter import LettaRuntimeAdapter
from hekate.infrastructure.letta.bridge_protocol import BridgeClient
from hekate.infrastructure.letta.provider_gateway import ProviderGatewayProfile
from hekate.infrastructure.postgres.database import create_engine, create_uow_factory
from hekate.infrastructure.postgres.delivery_repository import PostgresDeliveryRepository
from hekate.infrastructure.postgres.knowledge_repository import PostgresKnowledgeRepository
from hekate.settings import configured_critic_execution, configured_local_actor, configured_task_execution, load_settings
from hekate.settings import configured_deliberation
from hekate.worker import service as worker_service


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
            capsule, _ = json.JSONDecoder().raw_decode(content.split(marker, 1)[1].lstrip())
            if not isinstance(capsule, dict):
                raise ValueError("provider request Task Capsule is not an object")
            return content, capsule
    return None


class ContextCheckingResponses:
    def __init__(self, evidence_id: str, observations: list[dict[str, object]]) -> None:
        self.evidence_id = evidence_id
        self.observations = observations
        self.evidence_by_task: dict[str, str] = {}

    def expect_task(self, task_id: str, evidence_id: str) -> None:
        self.evidence_by_task[task_id] = evidence_id

    def __call__(self, request: dict[str, object]) -> object:
        turn = _request_capsule(request)
        if turn is None:
            raise ValueError("compaction was not expected in the zero-compaction fixture")
        prompt, capsule = turn
        binding = re.search(
            r"Trusted runtime binding: task_id=([^;]+); attempt_id=([^;]+); agent_registry_id=([^;]+); input_revision=(\d+)\.",
            prompt,
        )
        if binding is None:
            raise ValueError("request omitted trusted runtime binding")
        task_id, attempt_id, registry_id, revision_text = binding.groups()
        expected_evidence_id = self.evidence_by_task.get(task_id, self.evidence_id)
        if (task_id, attempt_id, int(revision_text)) != (
            capsule.get("task_id"), capsule.get("attempt_id"), capsule.get("input_revision"),
        ):
            raise ValueError("Task Capsule binding differs from the trusted binding")

        evidence_ids = [str(item) for item in capsule.get("evidence_refs", [])]
        excerpts = capsule.get("task_data", {}).get("evidence", [])
        if evidence_ids != [expected_evidence_id] or len(excerpts) != 1:
            raise ValueError("workflow step did not receive exactly the explicitly selected Evidence")
        if "The copper seal remains valid through 2031" not in excerpts[0].get("excerpt", ""):
            raise ValueError("workflow step omitted the source excerpt")

        role = capsule.get("reasoning_role")
        objective = str(capsule.get("objective", ""))
        full_chain = "PHASE5B_FULL_CHAIN" in objective
        review_chain = "PHASE5B_REVIEW_CHAIN" in objective
        solo_continuation = "PHASE5B_SOLO_CONTINUATION" in objective
        review_cap = "PHASE5B_REVIEW_CAP" in objective
        reasoning_cap = "PHASE5B_REASONING_CAP" in objective
        duplicate_reasoning = "PHASE5B_DUPLICATE_REASONING" in objective
        primary_phase5b = full_chain or review_chain or solo_continuation or review_cap or reasoning_cap or duplicate_reasoning
        deliberation = capsule.get("deliberation_context")
        if role == "critic":
            if capsule.get("mode") != "targeted_review" or not isinstance(capsule.get("review_target"), dict):
                raise ValueError("Critic request was not targeted review")
            target = capsule["review_target"]
            if not all(isinstance(target.get(key), str) and target[key].strip() for key in (
                "purpose", "target_uncertainty", "expected_decision_impact",
            )):
                raise ValueError("Critic target omitted the approved continuation purpose")
            prior_reviews = target.get("prior_reviews", [])
            dynamic_review = (
                isinstance(deliberation, dict)
                and deliberation.get("step_kind") == "critic_review"
            )
            expected_prior_reviews = 1 if primary_phase5b and dynamic_review else 0
            if len(prior_reviews) != expected_prior_reviews:
                raise ValueError("additional Critic review did not receive the persisted first review")
            if expected_prior_reviews and prior_reviews[0].get("conclusion", {}).get("objections", [{}])[0].get("claim") != "The source may be stale.":
                raise ValueError("additional Critic review omitted the first durable objection")
            schema_marker = "Critic server output contract (generated from the strict Python CriticTurnOutput model):\n"
            expected_name = "critic-turn-output.v1.schema.json"
        else:
            if role != "hekate":
                raise ValueError("unexpected workflow reasoning role")
            schema_marker = "HEKATE server output contract (generated from the strict Python HekateTurnOutput model):\n"
            expected_name = "hekate-turn-output.v1.schema.json"

        if schema_marker not in prompt:
            raise ValueError("provider request omitted the role-specific generated schema")
        schema_text = prompt.split(schema_marker, 1)[1].split("\n\nServer output policy:\n", 1)[0]
        schema = json.loads(schema_text)
        from hekate.domain.capsules import export_schemas

        if canonical_json_hash(schema) != canonical_json_hash(export_schemas()[expected_name]):
            raise ValueError("provider request schema differs from its generated server contract")

        conclusion = {
            "schema_version": "1", "task_id": task_id, "attempt_id": attempt_id,
            "agent_id": registry_id, "status": "done",
            "assessment": {
                "statement": "The source supports the candidate while retaining a freshness caveat.",
                "confidence": {"level": "medium", "basis": ["bounded synthetic Evidence review"]},
            },
            "evidence_used": [expected_evidence_id], "objections": [], "assumptions": [],
            "unresolved": [], "recommended_next_step": {"type": "none"},
            "position_recommendation": {"action": "update", "summary": "Retain a freshness caveat."},
        }
        if role == "critic":
            candidate = capsule["review_target"].get("candidate_conclusion", {})
            if candidate.get("status") != "done" or not candidate.get("assessment"):
                raise ValueError("Critic request omitted the uncommitted candidate assessment")
            round_number = 2 if len(capsule["review_target"].get("prior_reviews", [])) else 1
            claim = "The source might fall outside the applicable jurisdiction." if round_number == 2 else "The source may be stale."
            conclusion["objections"] = [{
                "id": "O1", "severity": "high", "claim": claim,
                "condition": "if the source scope or retrieval date differs from the governing rule",
                "suggested_validation": "verify source scope and freshness against the governing rule",
            }]
            output: dict[str, object] = {"schema_version": "1", "conclusion": conclusion}
            stage = f"critic_review_{round_number}"
        elif isinstance(capsule.get("synthesis_context"), dict):
            context = capsule["synthesis_context"]
            critic = context.get("critic_conclusion", {})
            dissent = context.get("dissent", [])
            review_history = context.get("review_history", [])
            if not critic.get("objections") or not dissent:
                raise ValueError("synthesis request omitted the actual Critic Conclusion or persisted dissent")
            dynamic_synthesis = (
                isinstance(deliberation, dict)
                and deliberation.get("step_kind") == "synthesis"
            )
            expected_review_count = 2 if primary_phase5b and dynamic_synthesis else 1
            if len(review_history) != expected_review_count:
                raise ValueError("synthesis did not receive the expected durable Critic review history")
            dissent_ids = [item["id"] for item in dissent]
            if expected_review_count == 2 and (
                len(set(dissent_ids)) != 2
                or len({item.get("conclusion_id") for item in review_history}) != 2
                or any(not item.get("dissent") for item in review_history)
            ):
                raise ValueError("second synthesis omitted one review or its persisted dissent")
            if review_cap and dynamic_synthesis:
                output = {"schema_version": "1", "proposal": {
                    "schema_version": "1", "action": "continue",
                    "unresolved_issue": "A third Critic review could test one more jurisdiction condition.",
                    "next_action": "critic_review",
                    "expected_information_gain": "A third review could look for another scope boundary.",
                    "decision_impact": "That boundary might further qualify the Position.",
                }, "conclusion": conclusion}
                stage = "synthesis_2_cap_request"
            elif primary_phase5b and dynamic_synthesis:
                operation = re.search(r"Commit operation_id: ([^\n]+)", prompt)
                if operation is None:
                    raise ValueError("synthesis request omitted the server-selected Position operation ID")
                position = {
                    "statement": "The copper seal remains usable with jurisdiction and freshness qualifications.",
                    "applicability": ["the submitted question and source context"],
                    "confidence": {"level": "medium", "basis": ["source excerpt and two bounded Critic reviews"]},
                    "evidence_refs": [expected_evidence_id], "assumptions": [],
                    "dissent_refs": dissent_ids,
                    "uncertainty": "Confirm source scope and retrieval date against the governing rule.",
                }
                output = {"schema_version": "1", "proposal": {
                    "schema_version": "1", "action": "commit", "operation_id": operation.group(1),
                    "task_id": task_id, "topic_id": capsule["topic_id"],
                    "base_version": capsule["base_position_version"], "input_revision": int(revision_text),
                    "proposed_position": position, "reason_for_change": "Incorporate both bounded Critic reviews.",
                }, "conclusion": conclusion}
                stage = "synthesis_2"
            elif full_chain or review_chain or review_cap or reasoning_cap or duplicate_reasoning:
                if len(review_history) != 1:
                    raise ValueError("first synthesis did not receive the first Critic Conclusion")
                next_action = (
                    "critic_review" if review_chain or review_cap else "hekate_reasoning"
                )
                unresolved_issue = (
                    "Review whether the first objection misses a jurisdiction condition."
                    if next_action == "critic_review" else
                    "Check the same source freshness uncertainty."
                    if duplicate_reasoning else
                    "Check the retrieval date against the expected currentness window."
                )
                information_gain = (
                    "A second bounded review can test the jurisdiction condition."
                    if next_action == "critic_review" else
                    "Compare the retrieval date with the expected currentness window."
                    if duplicate_reasoning else
                    "Compare the retrieval date with the expected currentness window."
                )
                decision_impact = (
                    "A jurisdiction condition could change the applicable conclusion."
                    if next_action == "critic_review" else
                    "The same freshness issue could change applicability."
                    if duplicate_reasoning else
                    "A stale source could narrow the Position's applicability."
                )
                output = {"schema_version": "1", "proposal": {
                    "schema_version": "1", "action": "continue",
                    "unresolved_issue": unresolved_issue, "next_action": next_action,
                    "expected_information_gain": information_gain, "decision_impact": decision_impact,
                }, "conclusion": conclusion}
                stage = "synthesis_1_continue" if next_action == "hekate_reasoning" else "synthesis_1_review_continue"
            else:
                operation = re.search(r"Commit operation_id: ([^\n]+)", prompt)
                if operation is None:
                    raise ValueError("synthesis request omitted the server-selected Position operation ID")
                position = {
                    "statement": "The copper seal remains valid, with source freshness subject to verification.",
                    "applicability": ["the submitted question and source context"],
                    "confidence": {"level": "medium", "basis": ["source excerpt and Critic review"]},
                    "evidence_refs": [expected_evidence_id], "assumptions": [],
                    "dissent_refs": dissent_ids,
                    "uncertainty": "Confirm the retrieval date against the applicable rule.",
                }
                output = {"schema_version": "1", "proposal": {
                    "schema_version": "1", "action": "commit", "operation_id": operation.group(1),
                    "task_id": task_id, "topic_id": capsule["topic_id"],
                    "base_version": capsule["base_position_version"], "input_revision": int(revision_text),
                    "proposed_position": position, "reason_for_change": "Incorporate the Critic's freshness objection.",
                }, "conclusion": conclusion}
                stage = "synthesis"
        elif full_chain and isinstance(deliberation, dict) and deliberation.get("step_kind") == "hekate_reasoning":
            history = deliberation.get("review_history", [])
            if len(history) != 1 or history[0].get("conclusion", {}).get("objections", [{}])[0].get("claim") != "The source may be stale.":
                raise ValueError("continuation omitted the first Critic conclusion and dissent history")
            output = {"schema_version": "1", "proposal": {
                "schema_version": "1", "action": "continue",
                "unresolved_issue": "Review the remaining source-scope uncertainty.",
                "next_action": "critic_review",
                "expected_information_gain": "A bounded second review can test the jurisdiction caveat.",
                "decision_impact": "The jurisdiction caveat may change the Position applicability.",
            }, "conclusion": conclusion}
            stage = "hekate_reasoning"
        elif (reasoning_cap or duplicate_reasoning) and isinstance(deliberation, dict) and deliberation.get("step_kind") == "hekate_reasoning":
            unresolved_issue = (
                "Check the same source freshness uncertainty."
                if duplicate_reasoning else "Check whether a later retrieval changes the source freshness conclusion."
            )
            information_gain = "Compare the retrieval date with the expected currentness window."
            decision_impact = (
                "The same freshness issue could change applicability."
                if duplicate_reasoning else "A later retrieval could change the Position's scope."
            )
            output = {"schema_version": "1", "proposal": {
                "schema_version": "1", "action": "continue", "unresolved_issue": unresolved_issue,
                "next_action": "hekate_reasoning", "expected_information_gain": information_gain,
                "decision_impact": decision_impact,
            }, "conclusion": conclusion}
            stage = "hekate_reasoning_cap_request" if reasoning_cap else "hekate_reasoning_duplicate_request"
        elif solo_continuation and isinstance(deliberation, dict) and deliberation.get("step_kind") == "hekate_reasoning":
            output = {"schema_version": "1", "proposal": {
                "schema_version": "1", "action": "answer",
                "answer": "The evidence supports a bounded answer, subject to the stated source limits.",
            }, "conclusion": conclusion}
            stage = "hekate_reasoning_answer"
        else:
            policy = prompt.split("\n\nServer output policy:\n", 1)[1].split("\n\nTrusted runtime binding:", 1)[0]
            if solo_continuation and deliberation is None:
                output = {"schema_version": "1", "proposal": {
                    "schema_version": "1", "action": "continue",
                    "unresolved_issue": "Check whether the cited source applies to this jurisdiction.",
                    "next_action": "hekate_reasoning",
                    "expected_information_gain": "Compare the selected source context with the question scope.",
                    "decision_impact": "A scope difference could qualify the answer.",
                }, "conclusion": conclusion}
                stage = "planning_continue"
            else:
                topic_id = str(capsule.get("topic_id") or "")
                if not primary_phase5b and (
                    "Planning may instead submit a" not in policy
                    or (topic_id != "phase5a-main-topic" and not topic_id.startswith("p5b-"))
                ):
                    raise ValueError("planning request did not allow a bounded spawn on the submitted topic")
                conclusion["assessment"]["statement"] = "The source supports the candidate, pending a freshness check."
                output = {
                    "schema_version": "1",
                    "proposal": {
                        "schema_version": "1", "action": "spawn", "role": "critic",
                        "purpose": "Check source freshness", "target_uncertainty": "The source could be stale",
                        "expected_decision_impact": "A stale source would change the applicability of the Position",
                        "task_id": task_id,
                    },
                    "conclusion": conclusion,
                }
                stage = "planning"

        synthesis = capsule.get("synthesis_context")
        supplied_dissent = synthesis.get("dissent", []) if isinstance(synthesis, dict) else []
        tool_names = [
            tool.get("function", {}).get("name") for tool in request.get("tools", [])
            if isinstance(tool, dict) and isinstance(tool.get("function"), dict)
        ]
        if role == "critic" and any(name != "StructuredOutput" for name in tool_names):
            raise ValueError("Critic provider request exposed a tool other than the structured-output contract")
        self.observations.append({
            "stage": stage, "task_id": task_id, "attempt_id": attempt_id,
            "registry_id": registry_id, "reasoning_role": role,
            "evidence_ids": evidence_ids, "schema": expected_name,
            "upstream_tool_names": tool_names,
            "prompt_sha256": hashlib.sha256(prompt.encode()).hexdigest(),
            "context_checked": True,
            "critic_conclusion_received": stage in {"synthesis", "synthesis_2"},
            "dissent_ids": [item["id"] for item in supplied_dissent],
        })
        tools = request.get("tools")
        if isinstance(tools, list) and any(
            isinstance(tool, dict) and isinstance(tool.get("function"), dict)
            and tool["function"].get("name") == "StructuredOutput"
            for tool in tools
        ):
            return output
        return json.dumps(output, ensure_ascii=False, separators=(",", ":"))


class RuntimeFaults:
    """One-shot failures around real pinned runtime lifecycle calls."""

    def __init__(self, inner: object, shared: dict[str, object]) -> None:
        self.inner = inner
        self.shared = shared

    def __getattr__(self, name: str):
        return getattr(self.inner, name)

    async def create_agent(self, specification, operation_id):
        role = specification.get("role")
        owner = str(specification.get("owner", ""))
        if role == "hekate":
            gate = self.shared.get("hekate_create_gate")
            if isinstance(gate, dict) and gate.get("owner") == owner:
                gate["entered"].set()
                await gate["release"].wait()
            self.shared["hekate_create_calls"] += 1
        elif role == "critic":
            self.shared["critic_create_calls"] += 1
        created = await self.inner.create_agent(specification, operation_id)
        if role == "hekate" and self.shared.get("lose_hekate_create_response_for") == owner:
            self.shared["lose_hekate_create_response_for"] = None
            raise ConnectionError("injected lost response after actual persistent HEKATE creation")
        if role == "critic":
            if not self.shared["create_response_lost"]:
                self.shared["create_response_lost"] = True
                raise ConnectionError("injected lost response after actual Critic creation")
        return created

    async def list_owned_agents(self, owner, creation_op):
        calls = self.shared.setdefault("agent_list_calls_by_owner", {})
        owner_text = str(owner)
        calls[owner_text] = int(calls.get(owner_text, 0)) + 1
        agents = await self.inner.list_owned_agents(owner, creation_op)
        if self.shared.get("fail_agent_list_after_query_for_owner") == owner_text:
            self.shared["fail_agent_list_after_query_for_owner"] = None
            raise ConnectionError("injected lost response after actual owned-agent query")
        return agents

    async def observe_agent(self, provider_agent_id):
        calls = self.shared.setdefault("agent_observe_calls_by_id", {})
        provider_text = str(provider_agent_id)
        calls[provider_text] = int(calls.get(provider_text, 0)) + 1
        return await self.inner.observe_agent(provider_agent_id)

    async def prepare_session(self, binding, output_contract=None):
        prepared, session = await self.inner.prepare_session(binding, output_contract)
        sessions = self.shared.setdefault("prepared_sessions", [])
        sessions.append({
            "task_id": str(prepared.task_id), "attempt_id": str(prepared.attempt_id),
            "registry_id": str(prepared.agent_registry_id),
            "provider_agent_id": str(prepared.provider_agent_id),
            "conversation_id": str(prepared.conversation_id),
            "output_contract": output_contract,
        })
        return prepared, session

    async def delete_agent(self, provider_agent_id, operation_id):
        self.shared["critic_delete_calls"] += 1
        if not self.shared["delete_failed_once"]:
            self.shared["delete_failed_once"] = True
            raise ConnectionError("injected Critic deletion failure before actual deletion")
        return await self.inner.delete_agent(provider_agent_id, operation_id)


async def _workflow_state(engine, task_id: str) -> dict[str, object]:
    async with engine.connect() as connection:
        row = (await connection.execute(text("""
            SELECT w.stage, w.parent_attempt_id, w.planning_operation_id, w.input_revision,
                   w.spawn_request_hash, w.critic_registry_id, w.create_operation_id, w.review_attempt_id,
                   w.review_operation_id, w.critic_conclusion_id, w.synthesis_attempt_id,
                   w.synthesis_operation_id, w.delete_operation_id, t.status AS task_status,
                   t.critic_agents, t.review_rounds, ar.provider_agent_id, ar.intended_state AS registry_state
            FROM critic_workflows w JOIN tasks t ON t.id=w.task_id
            JOIN agent_registry ar ON ar.id=w.critic_registry_id WHERE w.task_id=:task
        """), {"task": task_id})).mappings().one_or_none()
        return dict(row) if row else {}


async def _continuation_effect_snapshot(engine, task_id: str) -> dict[str, object]:
    async with engine.connect() as connection:
        task = (await connection.execute(text("""
            SELECT status, input_revision, review_rounds, hekate_continuations
            FROM tasks WHERE id=:task
        """), {"task": task_id})).mappings().one()
        steps = (await connection.execute(text("""
            SELECT step_slot, step_kind, state, operation_id, reservation_id
            FROM deliberation_steps WHERE task_id=:task ORDER BY step_order, step_slot
        """), {"task": task_id})).mappings().all()
        result_state = await connection.scalar(text("""
            SELECT processing_state FROM turn_results
            WHERE task_id=:task AND proposal->>'action'='continue'
            ORDER BY created_at, inbox_id LIMIT 1
        """), {"task": task_id})
    return {
        "task": dict(task), "steps": [dict(item) for item in steps],
        "step_count": len(steps), "step_operation_count": len({item["operation_id"] for item in steps}),
        "step_reservation_count": len({item["reservation_id"] for item in steps}),
        "continue_result_state": result_state,
    }


async def _wait_for(engine, task_id: str, predicate, label: str, timeout: float = 120) -> dict[str, object]:
    end = asyncio.get_running_loop().time() + timeout
    last: dict[str, object] = {}
    while asyncio.get_running_loop().time() < end:
        last = await _workflow_state(engine, task_id)
        if last and predicate(last):
            return last
        await asyncio.sleep(0.1)
    raise TimeoutError(f"timed out waiting for {label}; last state: {last}")


async def _start_worker(container):
    stop = asyncio.Event()
    task = asyncio.create_task(worker_service.run_worker(container, stop))
    return stop, task


async def _stop_worker(stop: asyncio.Event, task: asyncio.Task) -> None:
    stop.set()
    await asyncio.wait_for(task, 35)


async def _expedite_job(engine, operation_id: str) -> None:
    async with engine.begin() as connection:
        await connection.execute(text("""
            UPDATE outbox SET available_at=now(), status='PENDING', claim_owner=NULL,
                claim_expires_at=NULL
            WHERE operation_id=:operation AND status IN ('PENDING','CLAIMED')
        """), {"operation": operation_id})


async def _run(database_url: str, node: str, image: str, node_archive: Path, artifact: Path, run_id: str) -> dict[str, object]:
    if not artifact.is_absolute():
        artifact = ROOT / artifact
    report: dict[str, object] = {
        "schema_version": "1", "probe": "phase5b-bounded-deliberation", "run_id": run_id,
        "executed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "base_head": "a58df8c5c0f253635a5fafc2d016eec407bfce8a",
        "branch": subprocess.check_output(["git", "branch", "--show-current"], cwd=ROOT, text=True).strip(),
        "checked_out_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "phase4_included_fingerprint": "5f07215a7a987a9756ceebc16d08f5359c96e6dddc577c0d6599b4969ccd56aa",
        "real_provider_calls": 0, "fake_provider_requests": 0, "overall_status": "blocked",
        "dispatch_safety": {
            "runtime_mode": "test", "production_dispatch": "blocked",
            "real_provider_calls": 0,
        },
        "execution": {
            "command": "uv run --locked python scripts/phase5b_bounded_deliberation_probe.py --database-url <fresh-isolated-db> --legacy-database-url <existing-phase5b-db> --empty-migration-database-url <fresh-empty-db> --node-bin /tmp/node-v22.19.0/bin/node",
            "environment_variables": ["isolated Phase 5B database URLs (redacted)", "HEKATE_NODE_BIN"],
            "result": "running",
        },
        "results": {},
        "limitations": [
            "Synthetic pricing and the local fake provider were used; no production model or pricing is claimed.",
            "Critic review and synthesis are bounded at two each; HEKATE-only continuation is bounded at one. Repair, retry, and recursive spawning remain out of scope.",
            "Letta memory projection remains pending and unsupported; PostgreSQL remains authoritative.",
            "G7 same-execution resume and G8 full-request tokenization remain deferred.",
            "Production dispatch remains closed; no real provider endpoint or credentials were configured.",
        ],
    }
    engine = bridge = fake = gateway_server = gateway_task = sandbox = None
    temp = tempfile.TemporaryDirectory(prefix="hekate-phase5b-")
    state = Path(temp.name)
    archive_root = state / "archive"
    worker_runs: list[tuple[asyncio.Event, asyncio.Task]] = []
    spawn_approval_tasks: list[asyncio.Task] = []
    continuation_race_task: asyncio.Task | None = None
    original_prepare = worker_service.prepare_critic_workflow_steps
    original_dynamic_prepare = worker_service.prepare_deliberation_steps
    original_maintain = worker_service.maintain_critic_workflows
    original_maintain_deliberation = worker_service.maintain_deliberation_steps
    original_lock_operation = PostgresDeliveryRepository.lock_operation
    original_lock_turn_result = PostgresKnowledgeRepository.lock_turn_result
    release_spawn_race = asyncio.Event()
    spawn_race_active = False
    spawn_maintenance_entered = asyncio.Event()
    spawn_lock_attempts = 0
    spawn_lock_attempts_entered = asyncio.Event()
    planning_operation_id: str | None = None
    continuation_race = {
        "active": False, "attempts": 0, "inbox_id": None,
        "first_locked": asyncio.Event(), "second_entered": asyncio.Event(),
        "release_first": asyncio.Event(), "before": None, "after": None,
        "first_result": None, "competing_result": None,
    }

    async def observe_spawn_lock_attempt(repository, operation_id, request_hash=None):
        nonlocal spawn_lock_attempts
        if spawn_race_active and planning_operation_id is not None and operation_id == OperationId(planning_operation_id):
            spawn_lock_attempts += 1
            if spawn_lock_attempts >= 2:
                spawn_lock_attempts_entered.set()
        return await original_lock_operation(repository, operation_id, request_hash)

    async def hold_worker_after_spawn_adoption(factory_arg, *, limit=100):
        if spawn_race_active:
            async with engine.connect() as connection:
                result_state = await connection.scalar(text("""
                    SELECT processing_state FROM turn_results
                    WHERE task_id=:task AND proposal->>'action'='spawn'
                    ORDER BY created_at DESC LIMIT 1
                """), {"task": task_id})
            if result_state == "ACCEPTED":
                spawn_maintenance_entered.set()
                await release_spawn_race.wait()
        return await original_maintain(factory_arg, limit=limit)

    async def race_continue_result_adoption(repository, inbox_id):
        # Pause the first transaction after it has the real PostgreSQL row lock,
        # then let a competing result processor contend on that same durable
        # continue result. The second processor must observe the committed
        # acceptance and must not reserve or create the child step again.
        candidate = None
        if continuation_race["active"]:
            candidate = (await repository.connection.execute(text("""
                SELECT task_id, operation_id, proposal FROM turn_results WHERE inbox_id=:inbox
            """), {"inbox": inbox_id})).mappings().one_or_none()
        candidate_quiescent = False
        if candidate is not None:
            candidate_quiescent = await repository.connection.scalar(text("""
                SELECT execution_state='QUIESCENT' FROM operations WHERE id=:operation
            """), {"operation": candidate["operation_id"]})
        target_continue = (
            candidate is not None and candidate["task_id"] == task_id
            and isinstance(candidate["proposal"], dict)
            and candidate["proposal"].get("action") == "continue"
            and candidate_quiescent is True
        )
        if target_continue:
            continuation_race["attempts"] = int(continuation_race["attempts"]) + 1
            if continuation_race["attempts"] == 1:
                locked = await original_lock_turn_result(repository, inbox_id)
                if locked is None or locked["processing_state"] in {"ACCEPTED", "REJECTED", "LATE"}:
                    raise AssertionError("continuation result was not pending at the PostgreSQL race barrier")
                continuation_race["inbox_id"] = inbox_id
                continuation_race["before"] = await _continuation_effect_snapshot(engine, task_id)
                continuation_race["first_locked"].set()
                await continuation_race["release_first"].wait()
                return locked
            if continuation_race["attempts"] == 2:
                continuation_race["second_entered"].set()
        return await original_lock_turn_result(repository, inbox_id)

    async def race_competing_continue_processor():
        await continuation_race["first_locked"].wait()
        inbox_id = str(continuation_race["inbox_id"])
        competing = asyncio.create_task(results_app.apply_turn_result(
            factory, inbox_id, hekate_config=hekate_config,
            critic_config=critic_config, deliberation_config=deliberation_config,
        ))
        try:
            await asyncio.wait_for(continuation_race["second_entered"].wait(), 20)
            continuation_race["release_first"].set()
            first_result = await asyncio.wait_for(competing, 30)
            continuation_race["competing_result"] = first_result
            continuation_race["after"] = await _continuation_effect_snapshot(engine, task_id)
            return first_result
        finally:
            continuation_race["release_first"].set()

    try:
        report["runtime"] = p3._locked_runtime(image, node, node_archive)
        p3.p1.build_bridge(node)
        engine = create_engine(database_url)
        factory = create_uow_factory(engine)
        await p3._truncate(factory)
        async with engine.connect() as connection:
            head = await connection.scalar(text("SELECT version_num FROM alembic_version LIMIT 1"))
            postgres = await connection.scalar(text("SHOW server_version"))
        if head != "0010_p5b_delib_maint":
            raise ValueError("Phase 5B probe requires migration head 0010")
        report["database"] = {"name": make_url(database_url).database, "postgres_version": postgres, "migration_head": head}

        scope_text, principal_text = f"phase5b:{run_id}", f"principal:{run_id}"
        policy_version = "phase5b-synthetic-policy-v1"
        actor_scope = ScopeId(scope_text)
        async with factory() as uow:
            await uow.tasks.insert_scope(AuthorizationSnapshot(
                scope=actor_scope, principal_id=PrincipalId(principal_text),
                policy_version=policy_version, authz_epoch=1,
            ))
            await uow.commit()

        config_dir = state / "config"
        config_dir.mkdir()
        (config_dir / "local.yaml").write_text(yaml.safe_dump({"identity": {
            "principal_id": principal_text, "scope_id": scope_text,
            "policy_version": policy_version, "authz_epoch": 1,
        }}), encoding="utf-8")
        (config_dir / "policy.yaml").write_text(yaml.safe_dump({
            "version": policy_version,
            "limits": {
                "task_budget_usd": "5.00", "system_daily_budget_usd": "25.00",
                "task_deadline_seconds": 240, "critic_agents_per_task": 1,
                "review_rounds": 2, "max_syntheses_per_task": 2,
            },
            "critic": {"enabled": True, "max_agents_per_task": 1, "max_review_rounds": 2, "max_syntheses_per_task": 2},
            "deliberation": {
                "enabled": True, "max_critic_agents": 1, "max_review_rounds": 2,
                "max_hekate_continuations": 1, "max_syntheses_per_task": 2,
            },
        }), encoding="utf-8")
        models = {}
        for role in ("hekate", "critic"):
            models[role] = {
                "profile_id": "phase5b-shared-synthetic-v1", "model": f"openai-compatible/{p3.FAKE_MODEL}",
                "provider_model": p3.FAKE_MODEL, "max_input_tokens": 32768,
                "max_output_tokens": 2048, "max_compaction_calls": 0,
            }
        (config_dir / "models.yaml").write_text(yaml.safe_dump(models), encoding="utf-8")
        (config_dir / "pricing.yaml").write_text(yaml.safe_dump({"version": "phase5b-synthetic-pricing-v1", "prices": {
            p3.FAKE_MODEL: {"input_usd_per_million": "1", "output_usd_per_million": "2"},
        }}), encoding="utf-8")

        evidence_file = state / "source.txt"
        evidence_file.write_text("The copper seal remains valid through 2031. Retrieval date: 2026-10-03.\n", encoding="utf-8")
        env = os.environ.copy()
        env.update({
            "HEKATE_DATABASE_URL": database_url, "HEKATE_NODE_BIN": node,
            "HEKATE_BRIDGE_ENTRY": str(ROOT / "bridge/letta/dist/main.js"),
            "HEKATE_WORKER_ID": f"phase5b-worker-{run_id}", "HEKATE_RUNTIME_MODE": "test",
            "HEKATE_CONFIG_DIR": str(config_dir), "HEKATE_ARCHIVE_DIR": str(archive_root),
        })
        env["PATH"] = f"{Path(node).parent}:{env.get('PATH', '')}"
        settings = load_settings(env, config_dir)
        actor = configured_local_actor(settings)
        hekate_config = configured_task_execution(settings)
        critic_config = configured_critic_execution(settings)
        deliberation_config = configured_deliberation(settings)
        if critic_config is None:
            raise ValueError("synthetic Critic profile was not enabled")
        report["profiles"] = {
            "hekate": hekate_config.profile_id, "critic": critic_config.profile_id,
            "pricing": hekate_config.pricing_version, "compaction_calls": 0,
        }

        observations: list[dict[str, object]] = []
        response_errors: list[str] = []
        fake = p3.FakeProvider()
        fake.set_response_factory(ContextCheckingResponses("pending-evidence", observations))
        fake.start()
        sandbox = p3.Phase3Sandbox(state, run_id, image)
        sandbox.start_network()
        gateway_port = p3.reserve_port(sandbox.gateway_address)
        private_token = __import__("secrets").token_urlsafe(40)
        gateway_profile = ProviderGatewayProfile(
            profile_id=hekate_config.profile_id,
            price_table=p3.PriceTable(
                model=p3.FAKE_MODEL, version=hekate_config.pricing_version,
                input_usd_per_million=Decimal("1"), output_usd_per_million=Decimal("2"), synthetic=True,
            ),
            upstream_base_url=f"http://127.0.0.1:{fake.port}", upstream_api_key="isolated-fake-only",
            max_input_tokens=32768, max_output_tokens=2048, test_only=True,
        )
        from hekate.infrastructure.letta.provider_gateway import create_provider_gateway
        gateway_app = create_provider_gateway(factory, gateway_profile, private_token, allow_test_profile=True)
        gateway_server, gateway_task = await p3.start_gateway_server(gateway_app, sandbox.gateway_address, gateway_port)
        sandbox.start_pinned_app_server(gateway_port, private_token)
        letta_url = f"ws://{sandbox.container_address}:{p3.p1.APP_PORT}"

        async def new_runtime() -> tuple[BridgeClient, RuntimeFaults]:
            client = BridgeClient(node, ROOT / "bridge/letta/dist/main.js", env={
                "HEKATE_LETTA_URL": letta_url, "HEKATE_LETTA_TOKEN": private_token,
                "HEKATE_REQUIRE_PROVIDER_BINDING": "1",
            })
            adapter = LettaRuntimeAdapter(client)
            await adapter.verify_compatibility()
            return client, RuntimeFaults(adapter, fault_state)

        fault_state: dict[str, object] = {
            "critic_create_calls": 0, "create_response_lost": False,
            "critic_delete_calls": 0, "delete_failed_once": False,
            "hekate_create_calls": 0, "agent_list_calls_by_owner": {},
            "agent_observe_calls_by_id": {}, "lose_hekate_create_response_for": None,
            "prepared_sessions": [],
        }
        bridge, runtime = await new_runtime()
        from hekate.bootstrap import Container
        container = Container(settings=settings, runtime=runtime, uow_factory=factory, database=engine)

        imported = await p3b._cli(
            env, "evidence", "import", str(evidence_file), "--request-key", f"phase5a-{run_id}-evidence",
            "--kind", "document", "--retention-class", "fixture-retained",
            "--expires-at", (datetime.now(UTC) + timedelta(days=1)).isoformat().replace("+00:00", "Z"),
        )
        evidence_id = str(imported["id"])
        from hekate.application.tasks import submit
        from hekate.domain.models import UserMessage
        task_receipt = await submit(factory, actor, UserMessage(
            text="PHASE5B_FULL_CHAIN: Can this seal rule be used for this decision?",
            topic_id="phase5a-main-topic", evidence_refs=(evidence_id,),
        ), f"phase5a-{run_id}-task", hekate_config)
        task_id = str(task_receipt["task_id"])
        response_fixture = ContextCheckingResponses(evidence_id, observations)
        response_fixture.expect_task(task_id, evidence_id)
        def checked_response(request):
            try:
                return response_fixture(request)
            except Exception as error:
                response_errors.append(f"{type(error).__name__}: {error}")
                raise
        fake.set_response_factory(checked_response)

        async def submit_scenario_task(label: str, *, marker: str | None = None) -> dict[str, object]:
            source = state / f"{label}.txt"
            source.write_text(
                f"The copper seal remains valid through 2031. Retrieval date: 2026-10-03. Scenario: {label}.\n",
                encoding="utf-8",
            )
            imported_source = await p3b._cli(
                env, "evidence", "import", str(source), "--request-key", f"phase5a-{run_id}-{label}-evidence",
                "--kind", "document", "--retention-class", "fixture-retained",
                "--expires-at", (datetime.now(UTC) + timedelta(days=30)).isoformat().replace("+00:00", "Z"),
            )
            selected_evidence = str(imported_source["id"])
            receipt = await submit(factory, actor, UserMessage(
                text=(f"{marker}: " if marker else "") + f"Can this seal rule be used for the {label} scenario?",
                topic_id=f"p5b-{label}", evidence_refs=(selected_evidence,),
            ), f"phase5a-{run_id}-{label}-task", hekate_config)
            selected_task = str(receipt["task_id"])
            response_fixture.expect_task(selected_task, selected_evidence)
            return {
                "label": label, "task_id": selected_task, "evidence_id": selected_evidence,
                "source_path": str(source), "topic_id": f"p5b-{label}",
            }

        async def gate_worker_at(task: dict[str, object], stage: str) -> dict[str, object]:
            entered = asyncio.Event()
            previous_prepare = worker_service.prepare_critic_workflow_steps

            async def gated_prepare(*args, **kwargs):
                current = await _workflow_state(engine, str(task["task_id"]))
                if current.get("stage") == stage:
                    entered.set()
                    return ()
                return await original_prepare(*args, **kwargs)

            worker_service.prepare_critic_workflow_steps = gated_prepare
            stop, running = await _start_worker(container)
            worker_runs.append((stop, running))
            try:
                await asyncio.wait_for(entered.wait(), 150)
                await _stop_worker(stop, running)
                worker_runs.remove((stop, running))
                worker_service.prepare_critic_workflow_steps = previous_prepare
            except BaseException:
                stop.set()
                await asyncio.gather(running, return_exceptions=True)
                if (stop, running) in worker_runs:
                    worker_runs.remove((stop, running))
                worker_service.prepare_critic_workflow_steps = previous_prepare
                raise
            current = await _workflow_state(engine, str(task["task_id"]))
            if current.get("stage") != stage:
                raise AssertionError(f"expected {stage} barrier, got {current.get('stage')}")
            return {
                "task": task, "stage": stage, "previous_prepare": previous_prepare,
            }

        async def gate_dynamic_step(
            task: dict[str, object], slot: str, *, pause_maintenance: bool = False,
        ) -> dict[str, object]:
            entered = asyncio.Event()
            previous_prepare = worker_service.prepare_deliberation_steps
            previous_maintenance = worker_service.maintain_deliberation_steps

            async def gated_prepare(*args, **kwargs):
                async with engine.connect() as connection:
                    current = await connection.scalar(text("""
                        SELECT state FROM deliberation_steps
                        WHERE task_id=:task AND step_slot=:slot
                    """), {"task": task["task_id"], "slot": slot})
                if current == "READY":
                    entered.set()
                    return ()
                return await original_dynamic_prepare(*args, **kwargs)

            worker_service.prepare_deliberation_steps = gated_prepare
            if pause_maintenance:
                async def maintenance_paused_for_fixture(*args, **kwargs):
                    return 0
                worker_service.maintain_deliberation_steps = maintenance_paused_for_fixture
            stop, running = await _start_worker(container)
            worker_runs.append((stop, running))
            try:
                await asyncio.wait_for(entered.wait(), 180)
                await _stop_worker(stop, running)
                worker_runs.remove((stop, running))
            except BaseException:
                stop.set()
                await asyncio.gather(running, return_exceptions=True)
                if (stop, running) in worker_runs:
                    worker_runs.remove((stop, running))
                worker_service.prepare_deliberation_steps = previous_prepare
                worker_service.maintain_deliberation_steps = previous_maintenance
                raise
            worker_service.prepare_deliberation_steps = previous_prepare
            worker_service.maintain_deliberation_steps = previous_maintenance
            async with engine.connect() as connection:
                state = await connection.scalar(text("""
                    SELECT state FROM deliberation_steps WHERE task_id=:task AND step_slot=:slot
                """), {"task": task["task_id"], "slot": slot})
            if state != "READY":
                raise AssertionError(f"expected dynamic step {slot} to be READY, got {state}")
            return {"task": task, "slot": slot, "previous_prepare": previous_prepare}

        async def resume_dynamic_to_terminal(task: dict[str, object]) -> dict[str, object]:
            worker_service.prepare_deliberation_steps = original_dynamic_prepare
            stop, running = await _start_worker(container)
            worker_runs.append((stop, running))
            try:
                end = asyncio.get_running_loop().time() + 180
                state: dict[str, object] = {}
                while asyncio.get_running_loop().time() < end:
                    async with engine.connect() as connection:
                        row = (await connection.execute(text("""
                            SELECT t.status, t.outcome, t.stop_reason,
                                   (SELECT stage FROM critic_workflows w WHERE w.task_id=t.id) AS workflow_stage,
                                   (SELECT count(*) FROM task_responses r WHERE r.task_id=t.id) AS responses
                            FROM tasks t WHERE t.id=:task
                        """), {"task": task["task_id"]})).mappings().one()
                    state = dict(row)
                    if state["status"] in {"FAILED", "CANCELLED"} and (
                        state["workflow_stage"] is None or state["workflow_stage"] == "COMPLETE"
                    ):
                        return state
                    await asyncio.sleep(0.1)
                raise TimeoutError(f"dynamic stop did not converge: {state}")
            finally:
                await _stop_worker(stop, running)
                worker_runs.remove((stop, running))

        async def finish_gated_worker(gate: dict[str, object], *, complete: bool = True) -> dict[str, object]:
            task = gate["task"]
            task_id_value = str(task["task_id"])
            if complete:
                worker_service.prepare_critic_workflow_steps = gate["previous_prepare"]
                stop, running = await _start_worker(container)
                worker_runs.append((stop, running))
                try:
                    result = await _wait_for(
                        engine, task_id_value, lambda value: value.get("stage") == "COMPLETE",
                        f"{task['label']} workflow cleanup", 180,
                    )
                finally:
                    await _stop_worker(stop, running)
                    worker_runs.remove((stop, running))
            else:
                result = await _workflow_state(engine, task_id_value)
            return result

        async def scenario_snapshot(task_id_value: str) -> dict[str, object]:
            async with engine.connect() as connection:
                task_row = (await connection.execute(text("""
                    SELECT id, status, outcome, stop_reason, input_revision, critic_agents, review_rounds
                    FROM tasks WHERE id=:task
                """), {"task": task_id_value})).mappings().one()
                workflow_row = (await connection.execute(text("""
                    SELECT w.stage, w.input_revision, w.review_operation_id, w.synthesis_operation_id,
                           w.critic_registry_id, w.delete_operation_id, ar.intended_state AS critic_state
                    FROM critic_workflows w JOIN agent_registry ar ON ar.id=w.critic_registry_id
                    WHERE w.task_id=:task
                """), {"task": task_id_value})).mappings().one()
                response_row = (await connection.execute(text("""
                    SELECT proposal, response_text, stop_reason, input_revision
                    FROM task_responses WHERE task_id=:task
                """), {"task": task_id_value})).mappings().one_or_none()
                reservation_rows = (await connection.execute(text("""
                    SELECT o.kind, br.status, br.amount::text AS amount,
                           COALESCE((SELECT sum(ra.held_amount) FROM reservation_accounts ra
                                     WHERE ra.reservation_id=br.id), 0)::text AS held
                    FROM critic_workflows w JOIN operations o
                      ON o.id IN (w.review_operation_id, w.synthesis_operation_id)
                    JOIN budget_reservations br ON br.operation_id=o.id
                    WHERE w.task_id=:task ORDER BY o.kind
                """), {"task": task_id_value})).mappings().all()
                position_count = await connection.scalar(text(
                    "SELECT count(*) FROM position_versions WHERE task_id=:task"
                ), {"task": task_id_value})
                ledger_count = await connection.scalar(text("""
                    SELECT count(*) FROM budget_ledger l JOIN budget_accounts a ON a.id=l.account_id
                    WHERE a.scope_kind='TASK' AND a.scope_ref=:task
                """), {"task": task_id_value})
                delete_outbox_count = await connection.scalar(text("""
                    SELECT count(*) FROM critic_workflows w JOIN outbox o ON o.operation_id=w.delete_operation_id
                    WHERE w.task_id=:task AND o.kind='critic_delete'
                """), {"task": task_id_value})
                task_budget = (await connection.execute(text("""
                    SELECT spent_amount::text, held_amount::text FROM budget_accounts
                    WHERE scope_kind='TASK' AND scope_ref=:task
                """), {"task": task_id_value})).mappings().one()
                call_rows = (await connection.execute(text("""
                    SELECT p.status, COALESCE(u.settlement_state, 'PENDING') AS settlement_state
                    FROM provider_calls p JOIN operations o ON o.id=p.operation_id
                    LEFT JOIN usage_projections u ON u.accounting_call_id=p.accounting_call_id
                    WHERE o.task_id=:task ORDER BY p.created_at, p.accounting_call_id
                """), {"task": task_id_value})).mappings().all()
            return {
                "task": dict(task_row), "workflow": dict(workflow_row),
                "response": dict(response_row) if response_row else None,
                "reservations": [dict(item) for item in reservation_rows],
                "position_versions": int(position_count or 0),
                "budget_ledger_rows": int(ledger_count or 0),
                "critic_delete_outbox_rows": int(delete_outbox_count or 0),
                "task_budget": dict(task_budget), "provider_calls": [dict(item) for item in call_rows],
            }

        async def deliberation_cleanup_snapshot(task_id_value: str) -> dict[str, object]:
            async with engine.connect() as connection:
                task_row = (await connection.execute(text("""
                    SELECT id, status, outcome, stop_reason, input_revision, critic_agents,
                           review_rounds, hekate_continuations
                    FROM tasks WHERE id=:task
                """), {"task": task_id_value})).mappings().one()
                step_rows = (await connection.execute(text("""
                    SELECT d.id, d.input_revision AS step_revision, d.step_slot, d.state AS step_state,
                           d.stop_reason AS step_stop_reason, d.operation_id, d.reservation_id,
                           d.maintenance_completed_at, d.maintenance_retry_after,
                           o.state AS operation_state, o.execution_state, o.dispatch_state,
                           o.task_id AS operation_task_id,
                           br.status AS reservation_state, br.amount::text AS reserved_amount, br.settled_at,
                           COALESCE(sum(ra.held_amount),0)::text AS reservation_held,
                           count(DISTINCT pc.accounting_call_id) AS provider_calls
                    FROM deliberation_steps d JOIN operations o ON o.id=d.operation_id
                    JOIN budget_reservations br ON br.id=d.reservation_id
                    LEFT JOIN reservation_accounts ra ON ra.reservation_id=br.id
                    LEFT JOIN provider_calls pc ON pc.operation_id=o.id
                    WHERE d.task_id=:task GROUP BY d.id,o.id,br.id
                    ORDER BY d.step_order,d.id
                """), {"task": task_id_value})).mappings().all()
                task_accounts = (await connection.execute(text("""
                    SELECT ba.scope_kind, ba.scope_ref, ba.spent_amount::text, ba.held_amount::text,
                           ra.held_amount::text AS reservation_held
                    FROM deliberation_steps d JOIN reservation_accounts ra ON ra.reservation_id=d.reservation_id
                    JOIN budget_accounts ba ON ba.id=ra.account_id
                    WHERE d.task_id=:task ORDER BY d.step_order,ba.scope_kind,ba.scope_ref
                """), {"task": task_id_value})).mappings().all()
                response_count = await connection.scalar(text(
                    "SELECT count(*) FROM task_responses WHERE task_id=:task"
                ), {"task": task_id_value})
                position_count = await connection.scalar(text(
                    "SELECT count(*) FROM position_versions WHERE task_id=:task"
                ), {"task": task_id_value})
                critic_count = await connection.scalar(text(
                    "SELECT count(*) FROM agent_registry WHERE task_id=:task AND role='critic'"
                ), {"task": task_id_value})
                workflow_count = await connection.scalar(text(
                    "SELECT count(*) FROM critic_workflows WHERE task_id=:task"
                ), {"task": task_id_value})
                workflow_stage = await connection.scalar(text(
                    "SELECT stage FROM critic_workflows WHERE task_id=:task"
                ), {"task": task_id_value})
                audit_rows = (await connection.execute(text("""
                    SELECT event_kind, operation_id, count(*) AS count
                    FROM audit_events WHERE task_id=:task
                      AND event_kind IN ('deliberation.step_maintenance_stopped','deliberation.step_stopped')
                    GROUP BY event_kind,operation_id ORDER BY event_kind,operation_id
                """), {"task": task_id_value})).mappings().all()
                release_rows = (await connection.execute(text("""
                    SELECT l.reservation_id, count(*) AS count, COALESCE(sum(l.held_delta),0)::text AS held_delta,
                           COALESCE(sum(l.spent_delta),0)::text AS spent_delta
                    FROM budget_ledger l JOIN deliberation_steps d ON d.reservation_id=l.reservation_id
                    WHERE d.task_id=:task AND l.effect_type='RELEASE'
                    GROUP BY l.reservation_id ORDER BY l.reservation_id
                """), {"task": task_id_value})).mappings().all()
                execution_holds = (await connection.execute(text("""
                    SELECT h.operation_id,h.registry_id,h.state,h.quiescent_at
                    FROM agent_execution_holds h JOIN deliberation_steps d ON d.operation_id=h.operation_id
                    WHERE d.task_id=:task ORDER BY h.operation_id
                """), {"task": task_id_value})).mappings().all()
            return {
                "task": dict(task_row), "steps": [dict(row) for row in step_rows],
                "accounts": [dict(row) for row in task_accounts],
                "responses": int(response_count or 0), "position_versions": int(position_count or 0),
                "critic_agents": int(critic_count or 0), "critic_workflows": int(workflow_count or 0),
                "workflow_stage": workflow_stage,
                "stop_audits": [dict(row) for row in audit_rows],
                "release_ledger": [dict(row) for row in release_rows],
                "execution_holds": [dict(row) for row in execution_holds],
            }

        async def run_worker_until_deliberation_state(
            task_id_value: str, predicate, label: str, *, minimum_ticks: int = 1,
        ) -> tuple[dict[str, object], list[int]]:
            maintenance_results: list[int] = []
            first_tick = asyncio.Event()
            previous_maintenance = worker_service.maintain_deliberation_steps

            async def observe_maintenance(factory_arg, *, limit=100):
                result = await original_maintain_deliberation(factory_arg, limit=limit)
                maintenance_results.append(result)
                if len(maintenance_results) >= minimum_ticks:
                    first_tick.set()
                return result

            worker_service.maintain_deliberation_steps = observe_maintenance
            stop, running = await _start_worker(container)
            worker_runs.append((stop, running))
            try:
                await asyncio.wait_for(first_tick.wait(), 60)
                end = asyncio.get_running_loop().time() + 60
                state: dict[str, object] = {}
                while asyncio.get_running_loop().time() < end:
                    state = await deliberation_cleanup_snapshot(task_id_value)
                    if predicate(state):
                        break
                    await asyncio.sleep(0.05)
                else:
                    raise TimeoutError(f"worker did not finish {label}: {state}")
            finally:
                await _stop_worker(stop, running)
                worker_runs.remove((stop, running))
                worker_service.maintain_deliberation_steps = previous_maintenance
            return state, maintenance_results

        async def expire_scenario_evidence(task: dict[str, object]) -> dict[str, object]:
            expired_at = datetime.now(UTC) - timedelta(seconds=1)
            async with engine.begin() as connection:
                await connection.execute(text(
                    "UPDATE evidence SET expiry_at=:expiry WHERE id=:evidence"
                ), {"expiry": expired_at, "evidence": task["evidence_id"]})
            from hekate.application.evidence import expire

            cleanup = await expire(factory, archive_root, datetime.now(UTC), 100)
            async with engine.connect() as connection:
                row = (await connection.execute(text("""
                    SELECT e.availability, e.artifact_ref, a.storage_state
                    FROM evidence e JOIN artifacts a ON a.artifact_ref=e.artifact_ref
                    WHERE e.id=:evidence
                """), {"evidence": task["evidence_id"]})).mappings().one()
            digest = str(row["artifact_ref"]).removeprefix("sha256:")
            archive_path = archive_root / digest[:2] / digest
            source_exists = Path(str(task["source_path"])).is_file()
            info = {
                "maintenance_result": cleanup, "availability": row["availability"],
                "artifact_state": row["storage_state"], "archive_exists": archive_path.is_file(),
                "original_input_exists": source_exists,
            }
            if (
                row["availability"] != "EXPIRED" or row["storage_state"] != "DELETED"
                or archive_path.exists() or not source_exists
            ):
                raise AssertionError(f"Evidence expiry did not tombstone/delete safely: {info}")
            return info

        async def assert_policy_failure(task: dict[str, object], expected_code: str) -> dict[str, object]:
            snapshot = await scenario_snapshot(str(task["task_id"]))
            response = snapshot["response"]
            if (
                snapshot["task"]["status"] != "FAILED"
                or snapshot["task"]["stop_reason"] != "POLICY"
                or response is None or response["stop_reason"] != "POLICY"
                or response["proposal"].get("failure_code") != expected_code
                or response["proposal"].get("server_generated") is not True
                or snapshot["position_versions"] != 0
                or snapshot["workflow"]["stage"] != "COMPLETE"
                or snapshot["workflow"]["critic_state"] != "DELETED"
            ):
                raise AssertionError(f"Permanent workflow rejection did not converge exactly: {snapshot}")
            return snapshot

        deliberation_gate_slot: str | None = None
        deliberation_gate_entered = asyncio.Event()

        async def gate_ready_deliberation_step(*args, **kwargs):
            if deliberation_gate_slot is not None:
                async with engine.connect() as connection:
                    step_state = await connection.scalar(text("""
                        SELECT state FROM deliberation_steps
                        WHERE task_id=:task AND step_slot=:slot
                    """), {"task": task_id, "slot": deliberation_gate_slot})
                if step_state == "READY":
                    deliberation_gate_entered.set()
                    return ()
            return await original_dynamic_prepare(*args, **kwargs)

        PostgresDeliveryRepository.lock_operation = observe_spawn_lock_attempt
        worker_service.maintain_critic_workflows = hold_worker_after_spawn_adoption
        worker_service.prepare_deliberation_steps = gate_ready_deliberation_step
        deliberation_gate_slot = "hekate_reasoning_1"
        continuation_race["active"] = True
        PostgresKnowledgeRepository.lock_turn_result = race_continue_result_adoption
        continuation_race_task = asyncio.create_task(race_competing_continue_processor())
        spawn_race_active = True
        stop, worker_task = await _start_worker(container)
        worker_runs.append((stop, worker_task))
        # Let the actual result-adoption transaction commit, then hold workflow
        # maintenance while duplicate approval requests contend on PostgreSQL.
        # This checks replay identity and counters without blocking the
        # worker's transaction before the durable workflow becomes visible.
        await _wait_for(
            engine, task_id,
            lambda value: value.get("stage") in {"CREATE_PENDING", "CREATE_READY"},
            "durable Critic spawn adoption", 120,
        )
        await asyncio.wait_for(spawn_maintenance_entered.wait(), 10)
        async with factory() as uow:
            async with engine.connect() as connection:
                row = (await connection.execute(text("""
                    SELECT r.attempt_id, r.operation_id, r.conclusion_id, r.proposal, r.processing_state,
                           o.state, o.execution_state, o.binding
                    FROM turn_results r JOIN operations o ON o.id=r.operation_id
                    WHERE r.task_id=:task AND r.proposal->>'action'='spawn'
                """), {"task": task_id})).mappings().one()
            attempt = await uow.tasks.get_attempt(AttemptId(row["attempt_id"]))
            proposal = SpawnProposal.model_validate(row["proposal"], strict=True)
            await uow.rollback()
        planning_operation_id = str(row["operation_id"])
        binding = row["binding"]
        if not isinstance(binding, dict) or not isinstance(binding.get("fence"), int):
            raise AssertionError("trusted planning operation binding was unavailable")
        spawn_actor = ActorContext(
            principal_id=PrincipalId(binding["principal_id"]), scope=ScopeId(binding["scope"]),
            authenticated_agent_registry_id=attempt.agent_registry_id, task_id=TaskId(task_id),
            attempt_id=attempt.id, input_revision=attempt.input_revision,
            policy_version=binding["policy_version"], authz_epoch=int(binding["authz_epoch"]),
            fence=int(binding["fence"]),
        )

        async def approve_once():
            async with factory() as uow:
                op_row = await uow.delivery.lock_operation(OperationId(row["operation_id"]))
                current_attempt = await uow.tasks.get_attempt(AttemptId(row["attempt_id"]))
                receipt = await request_critic(
                    uow, spawn_actor, proposal, op_row, current_attempt,
                    DomainId(row["conclusion_id"]), hekate_config, critic_config,
                )
                await uow.commit()
                return receipt

        approval_counts_before = await _counts(engine, task_id, scope_text)
        if approval_counts_before["workflows"] != 1 or approval_counts_before["critics"] != 1:
            raise AssertionError("durable spawn intent was not present before replay contenders")
        competing_approvals = [asyncio.create_task(approve_once()) for _ in range(2)]
        spawn_approval_tasks.extend(competing_approvals)
        await asyncio.wait_for(spawn_lock_attempts_entered.wait(), 10)
        concurrent_receipts = await asyncio.gather(*competing_approvals)
        approval_replay_counts = await _counts(engine, task_id, scope_text)
        async with engine.connect() as connection:
            adopted_spawn_state = await connection.scalar(text("""
                SELECT processing_state FROM turn_results
                WHERE task_id=:task AND proposal->>'action'='spawn'
            """), {"task": task_id})
        if adopted_spawn_state != "ACCEPTED":
            raise AssertionError(f"primary planning result was not accepted atomically: {adopted_spawn_state}")
        spawn_race_active = False
        release_spawn_race.set()
        PostgresDeliveryRepository.lock_operation = original_lock_operation
        worker_service.maintain_critic_workflows = original_maintain
        await _wait_for(engine, task_id, lambda value: value.get("stage") == "CREATE_UNKNOWN", "Critic create response loss")
        stage = (await _workflow_state(engine, task_id))["stage"]
        if stage == "CREATE_UNKNOWN":
            before_recovery = await _workflow_state(engine, task_id)
            await _stop_worker(stop, worker_task)
            worker_runs.pop()
            await bridge.close()
            bridge, runtime = await new_runtime()
            container = Container(settings=settings, runtime=runtime, uow_factory=factory, database=engine)
            await _expedite_job(engine, str(before_recovery["create_operation_id"]))

            # Stop exactly after the accepted Critic result is durable, before synthesis is admitted.
            paused_at_synthesis = asyncio.Event()
            original_bound_prepare = worker_service.prepare_critic_workflow_steps
            async def pause_synthesis(factory_arg, runtime_arg, actor_arg, hekate_arg, critic_arg, worker_arg, deliberation_arg, **kwargs):
                current = await _workflow_state(engine, task_id)
                if current.get("stage") == "SYNTHESIS_PENDING":
                    paused_at_synthesis.set()
                    return ()
                return await original_bound_prepare(
                    factory_arg, runtime_arg, actor_arg, hekate_arg, critic_arg,
                    worker_arg, deliberation_arg, **kwargs,
                )
            worker_service.prepare_critic_workflow_steps = pause_synthesis
            stop, worker_task = await _start_worker(container)
            worker_runs.append((stop, worker_task))
            await asyncio.wait_for(paused_at_synthesis.wait(), 120)
            critic_result_durable = await _workflow_state(engine, task_id)
            async with engine.connect() as connection:
                dissent = (await connection.execute(text("""
                    SELECT d.id, d.body FROM critic_workflows w JOIN conclusions c ON c.id=w.critic_conclusion_id
                    JOIN dissent d ON d.conclusion_id=c.id WHERE w.task_id=:task
                """), {"task": task_id})).mappings().all()
                review_counts = (await connection.execute(text("""
                    SELECT (SELECT count(*) FROM conclusions WHERE task_id=:task) AS conclusions,
                           (SELECT count(*) FROM dissent WHERE scope=:scope) AS dissent,
                           (SELECT count(*) FROM agent_registry WHERE task_id=:task AND role='critic') AS critics
                """), {"task": task_id, "scope": scope_text})).mappings().one()
            if not dissent or int(review_counts["critics"]) != 1:
                raise AssertionError("Critic Conclusion/dissent or single-agent invariant was not durable")
            await _stop_worker(stop, worker_task)
            worker_runs.pop()
            await bridge.close()

            # New bridge client and worker process consume the saved synthesis stage.
            bridge, runtime = await new_runtime()
            container = Container(settings=settings, runtime=runtime, uow_factory=factory, database=engine)
            worker_service.prepare_critic_workflow_steps = original_bound_prepare
        else:
            raise AssertionError(f"Critic creation response loss was not persisted; got stage {stage}")

        # The first continuation approval is durable before admission. Stop the
        # real worker at that boundary, replace its bridge client, then resume
        # the same prepared operation identity.
        stop, worker_task = await _start_worker(container)
        worker_runs.append((stop, worker_task))
        await asyncio.wait_for(deliberation_gate_entered.wait(), 120)
        if continuation_race_task is None:
            raise AssertionError("continuation approval race was not started")
        concurrent_continue_result = await asyncio.wait_for(continuation_race_task, 30)
        PostgresKnowledgeRepository.lock_turn_result = original_lock_turn_result
        continuation_race["active"] = False
        continuation_race["after"] = await _continuation_effect_snapshot(engine, task_id)
        concurrent_continue_passed = (
            concurrent_continue_result.get("state") == "ACCEPTED"
            and continuation_race["attempts"] == 2
            and continuation_race["before"] is not None
            and continuation_race["before"]["step_count"] == 0
            and continuation_race["after"]["continue_result_state"] == "ACCEPTED"
            and continuation_race["after"]["task"]["hekate_continuations"] == 1
            and [step["step_slot"] for step in continuation_race["after"]["steps"]] == ["hekate_reasoning_1"]
            and continuation_race["after"]["step_reservation_count"] == 1
        )
        if not concurrent_continue_passed:
            raise AssertionError(
                f"PostgreSQL concurrent continuation adoption duplicated or lost effects: {continuation_race}"
            )
        async with engine.connect() as connection:
            reasoning_ready = (await connection.execute(text("""
                SELECT id, attempt_id, operation_id, reservation_id, state, request_hash
                FROM deliberation_steps WHERE task_id=:task AND step_slot='hekate_reasoning_1'
            """), {"task": task_id})).mappings().one()
        reasoning_approval_snapshot = {
            **dict(reasoning_ready), "provider_requests_at_approval": fake.count(),
        }
        if reasoning_ready["state"] != "READY" or fake.count() != 3:
            raise AssertionError("HEKATE continuation was not durably approved before its distinct inference")
        await _stop_worker(stop, worker_task)
        worker_runs.pop()
        await bridge.close()
        bridge, runtime = await new_runtime()
        container = Container(settings=settings, runtime=runtime, uow_factory=factory, database=engine)

        # The same Critic object performs round two. Pause after its result,
        # once synthesis round two is durable and ready, to exercise recovery.
        deliberation_gate_slot = "synthesis_2"
        deliberation_gate_entered.clear()
        stop, worker_task = await _start_worker(container)
        worker_runs.append((stop, worker_task))
        await asyncio.wait_for(deliberation_gate_entered.wait(), 150)
        async with engine.connect() as connection:
            second_round = (await connection.execute(text("""
                SELECT d.id, d.attempt_id, d.operation_id, d.reservation_id, d.state,
                       d.parent_conclusion_id, d.conclusion_id, d.registry_id,
                       o.binding, prior.state AS prior_review_state, prior.conclusion_id AS prior_conclusion_id
                FROM deliberation_steps d
                JOIN deliberation_steps prior ON prior.task_id=d.task_id
                    AND prior.step_slot='critic_review_2'
                JOIN operations o ON o.id=d.operation_id
                WHERE d.task_id=:task AND d.step_slot='synthesis_2'
            """), {"task": task_id})).mappings().one()
            review_bindings = (await connection.execute(text("""
            SELECT d.review_round, d.registry_id, o.binding,
                   d.attempt_id, o.id AS operation_id
            FROM deliberation_steps d JOIN operations o ON o.id=d.operation_id
                WHERE d.task_id=:task AND d.step_kind='critic_review'
                UNION ALL
            SELECT 1 AS review_round, w.critic_registry_id AS registry_id, o.binding,
                   w.review_attempt_id AS attempt_id, o.id AS operation_id
                FROM critic_workflows w JOIN operations o ON o.id=w.review_operation_id
                WHERE w.task_id=:task
                ORDER BY review_round
            """), {"task": task_id})).mappings().all()
            stored_dissent_count = await connection.scalar(text("""
                SELECT count(*) FROM dissent WHERE scope=:scope
            """), {"scope": scope_text})
        if (
            second_round["state"] != "READY" or second_round["parent_conclusion_id"] is None
            or second_round["prior_review_state"] != "RESULT_ACCEPTED"
            or second_round["prior_conclusion_id"] is None or len(review_bindings) != 2
            or review_bindings[0]["registry_id"] != review_bindings[1]["registry_id"]
            or review_bindings[0]["binding"]["provider_agent_id"] != review_bindings[1]["binding"]["provider_agent_id"]
            or review_bindings[0]["binding"]["conversation_id"] == review_bindings[1]["binding"]["conversation_id"]
            or review_bindings[0]["attempt_id"] == review_bindings[1]["attempt_id"]
            or review_bindings[0]["operation_id"] == review_bindings[1]["operation_id"]
            or fake.count() != 5 or int(stored_dissent_count or 0) != 2
        ):
            raise AssertionError("second Critic review/synthesis recovery boundary was not durable and isolated")
        prepared_by_attempt = {
            str(item["attempt_id"]): item for item in fault_state["prepared_sessions"]
            if isinstance(item, dict)
        }
        if any(
            str(row["attempt_id"]) not in prepared_by_attempt
            or prepared_by_attempt[str(row["attempt_id"])]["conversation_id"] != row["binding"]["conversation_id"]
            or prepared_by_attempt[str(row["attempt_id"])]["registry_id"] != str(row["registry_id"])
            for row in review_bindings
        ):
            raise AssertionError("Critic conversation bindings do not match actual runtime session preparation")
        second_round_snapshot = {
            **dict(second_round), "binding": dict(second_round["binding"]),
            "provider_requests_at_ready": fake.count(),
            "review_bindings": [dict(item) for item in review_bindings],
            "dissent_count": int(stored_dissent_count or 0),
        }
        await _stop_worker(stop, worker_task)
        worker_runs.pop()
        await bridge.close()
        bridge, runtime = await new_runtime()
        container = Container(settings=settings, runtime=runtime, uow_factory=factory, database=engine)
        worker_service.prepare_deliberation_steps = original_dynamic_prepare

        # Fail the first delete before RPC, then restart and confirm actual absence.
        def observe_delete_failure(state):
            if state.get("stage") == "DELETE_PENDING" and state.get("delete_operation_id"):
                return True
            return False
        stop, worker_task = await _start_worker(container)
        worker_runs.append((stop, worker_task))
        await _wait_for(engine, task_id, observe_delete_failure, "synthesis and Critic retirement", 150)
        # Let the first delete attempt record UNKNOWN and durable retry state.
        await _wait_for_operation(engine, str(task_id), "critic.delete", "UNKNOWN", 45)
        await _stop_worker(stop, worker_task)
        worker_runs.pop()
        state_after_failure = await _workflow_state(engine, task_id)
        await _expedite_job(engine, str(state_after_failure["delete_operation_id"]))
        await bridge.close()
        bridge, runtime = await new_runtime()
        container = Container(settings=settings, runtime=runtime, uow_factory=factory, database=engine)
        stop, worker_task = await _start_worker(container)
        worker_runs.append((stop, worker_task))
        completed = await _wait_for(engine, task_id, lambda value: value.get("stage") == "COMPLETE", "Critic deletion confirmation")
        await _stop_worker(stop, worker_task)
        worker_runs.pop()

        position = await p3b._cli(env, "position", "show", "phase5a-main-topic")
        history = await p3b._cli(env, "position", "history", "phase5a-main-topic", "--after-version", "0", "--limit", "50")
        task_view = await p3b._cli(env, "task", task_id)
        async with engine.connect() as connection:
            operation_rows = (await connection.execute(text("""
                SELECT o.id, o.kind, o.state, o.execution_state, a.status AS attempt_status,
                       br.status AS reservation_status, br.amount::text AS reserved_amount
                FROM operations o LEFT JOIN attempts a ON a.operation_id=o.id
                LEFT JOIN budget_reservations br ON br.operation_id=o.id
                WHERE o.task_id=:task ORDER BY o.created_at, o.id
            """), {"task": task_id})).mappings().all()
            counts = (await connection.execute(text("""
                SELECT (SELECT count(*) FROM position_versions WHERE task_id=:task) AS position_versions,
                       (SELECT count(*) FROM position_commit_receipts WHERE scope=:scope) AS commit_receipts,
                       (SELECT count(*) FROM task_responses WHERE task_id=:task) AS responses,
                       (SELECT count(*) FROM provider_calls p JOIN operations o ON o.id=p.operation_id WHERE o.task_id=:task) AS provider_calls,
                       (SELECT count(*) FROM dissent WHERE scope=:scope) AS dissent_rows,
                       (SELECT count(*) FROM outbox WHERE operation_id IN (SELECT id FROM operations WHERE task_id=:task) AND kind='dispatch') AS dispatch_rows
            """), {"task": task_id, "scope": scope_text})).mappings().one()
            provider_agent_ids = (await connection.execute(text("""
                SELECT id, role AS kind, persistence, intended_state, provider_agent_id FROM agent_registry
                WHERE owner_scope=:scope ORDER BY kind, id
            """), {"scope": scope_text})).mappings().all()
            pending_projection = (await connection.execute(text("""
                SELECT desired_version, applied_version, state, pending_reason FROM memory_projections
                WHERE scope=:scope AND topic_id='phase5a-main-topic'
            """), {"scope": scope_text})).mappings().one_or_none()

        # The duplicate business-result and two competing spawn approvals are all replays.
        async with engine.connect() as connection:
            planning_result = await connection.scalar(text("""
                SELECT inbox_id FROM turn_results
                WHERE task_id=:task AND proposal->>'action'='spawn'
            """), {"task": task_id})
            critic_result = await connection.scalar(text("""
                SELECT r.inbox_id FROM turn_results r
                JOIN critic_workflows w ON w.critic_conclusion_id=r.conclusion_id
                WHERE w.task_id=:task
            """), {"task": task_id})
        if not planning_result or not critic_result:
            raise AssertionError("planning and Critic result inbox rows were not durable")
        replay_before = await _counts(engine, task_id, scope_text)
        critic_replay_result = await results_app.apply_turn_result(factory, critic_result)
        after_critic_replay = await _counts(engine, task_id, scope_text)
        planning_replay_result = await results_app.apply_turn_result(factory, planning_result)
        after_replay = await _counts(engine, task_id, scope_text)
        if replay_before != after_critic_replay or replay_before != after_replay:
            raise AssertionError("accepted Critic or planning result replay added workflow effects")
        review_result = await _wait_for(engine, task_id, lambda value: value.get("stage") == "COMPLETE", "completed workflow after replay", 2)
        workflow = completed
        position_version = position.get("current_version")
        final_synthesis = next(item for item in observations if item["stage"] == "synthesis_2")
        if not final_synthesis["critic_conclusion_received"] or not final_synthesis["dissent_ids"]:
            raise AssertionError("post-restart synthesis did not consume durable Critic dissent")
        critic_registry = str(workflow["critic_registry_id"])
        critic_state = next(item for item in provider_agent_ids if item["id"] == critic_registry)
        hekate_state = next(item for item in provider_agent_ids if item["kind"] == "hekate")
        critic_output_boundaries = await _critic_output_boundary_checks(
            factory, str(task_id), str(hekate_state["id"]),
        )
        continuation_output_boundaries = await _continuation_contract_boundary_checks(
            factory, str(task_id),
        )

        lifecycle_states = await _operation_states(engine, str(task_id))
        report["workflow"] = {
            "task_id": task_id, "task_status": task_view.get("state"),
            "input_revision": workflow["input_revision"],
            "planning_attempt_id": workflow["parent_attempt_id"],
            "planning_operation_id": workflow["planning_operation_id"],
            "spawn_request_hash": workflow["spawn_request_hash"],
            "hekate_registry_id": hekate_state["id"], "critic_registry_id": critic_registry,
            "critic_registry_state": critic_state["intended_state"],
            "create_operation_id": workflow["create_operation_id"],
            "review_attempt_id": workflow["review_attempt_id"], "review_operation_id": workflow["review_operation_id"],
            "critic_conclusion_id": workflow["critic_conclusion_id"],
            "dissent_ids": final_synthesis["dissent_ids"],
            "synthesis_attempt_id": workflow["synthesis_attempt_id"],
            "synthesis_operation_id": workflow["synthesis_operation_id"],
            "delete_operation_id": workflow["delete_operation_id"],
            "position_version": position_version,
            "position_statement": position.get("current", {}).get("body", {}).get("statement"),
            "position_projection": position.get("current", {}).get("projection_state"),
            "position_projection_pending_reason": position.get("current", {}).get("projection_pending_reason"),
            "task_response": task_view.get("response"),
        }
        report["normal_path"] = {
            "passed": task_view.get("state") == "COMPLETED" and position_version == 1
                and int(counts["position_versions"]) == 1 and int(counts["commit_receipts"]) == 1
                and int(counts["responses"]) == 1 and int(counts["dissent_rows"]) == 2
                and workflow["stage"] == "COMPLETE" and critic_state["intended_state"] == "DELETED"
                and hekate_state["intended_state"] == "READY" and fake.count() == 6,
            "provider_requests": fake.count(), "requests": list(observations),
            "db_effect_counts": {key: int(value) for key, value in counts.items()},
            "operations_and_reservations": [dict(item) for item in operation_rows],
            "lifecycle_operations": lifecycle_states,
            "critic_create_calls": fault_state["critic_create_calls"],
            "critic_delete_calls": fault_state["critic_delete_calls"],
            "continuation_restart_boundaries": {
                "hekate_reasoning_approved_before_admission": reasoning_approval_snapshot,
                "synthesis_2_ready_after_review_2_restart": second_round_snapshot,
                "same_critic_registry_and_provider": review_bindings[0]["registry_id"] == review_bindings[1]["registry_id"]
                    and review_bindings[0]["binding"]["provider_agent_id"] == review_bindings[1]["binding"]["provider_agent_id"],
                "distinct_critic_attempt_operation_and_conversation": (
                    review_bindings[0]["attempt_id"] != review_bindings[1]["attempt_id"]
                    and review_bindings[0]["operation_id"] != review_bindings[1]["operation_id"]
                    and review_bindings[0]["binding"]["conversation_id"] != review_bindings[1]["binding"]["conversation_id"]
                ),
                "actual_runtime_session_preparations": [
                    prepared_by_attempt[str(item["attempt_id"])] for item in review_bindings
                ],
                "passed": fake.count() == 6 and len(review_bindings) == 2,
            },
            "critic_output_boundaries": critic_output_boundaries,
            "continuation_output_boundaries": continuation_output_boundaries,
            "create_response_lost_then_owned_tag_recovered": bool(fault_state["create_response_lost"]),
            "delete_failure_retried_after_restart": bool(fault_state["delete_failed_once"]),
            "persistent_hekate_retained": hekate_state["intended_state"] == "READY",
            "projection": dict(pending_projection) if pending_projection else None,
            "inbox_replay": {
                "critic_result": critic_replay_result,
                "planning_spawn": planning_replay_result,
            },
            "effects_before_replay": replay_before,
            "effects_after_critic_replay": after_critic_replay,
            "effects_after_replay": after_replay,
            "replay_no_new_effects": replay_before == after_critic_replay == after_replay,
            "concurrent_spawn_receipts": [item.model_dump(mode="json") for item in concurrent_receipts],
            "concurrent_spawn_replay_no_new_effects": (
                approval_replay_counts == approval_counts_before
                and approval_replay_counts["workflows"] == 1
                and approval_replay_counts["critics"] == 1
                and approval_replay_counts["critic_agents"] == 1
                and approval_replay_counts["reservations"] == 3
                and approval_replay_counts["outbox"] == 2
                and all(item.critic_registry_id == concurrent_receipts[0].critic_registry_id for item in concurrent_receipts)
                and all(item.create_operation_id == concurrent_receipts[0].create_operation_id for item in concurrent_receipts)
            ),
            "spawn_effect_counts_before_replay": approval_counts_before,
            "spawn_effect_counts_after_replay": approval_replay_counts,
            "one_critic_after_postgres_race": len([item for item in provider_agent_ids if item["kind"] == "critic"]) == 1,
            "postgres_concurrent_spawn_replay": {
                "postgres_spawn_approval_replay_contenders": len(concurrent_receipts),
                "competing_operation_lock_attempts": spawn_lock_attempts,
                "competing_approvals_returned_same_registry": len({str(item.critic_registry_id) for item in concurrent_receipts}) == 1,
                "one_durable_workflow_and_create_intent": approval_replay_counts["workflows"] == 1
                    and approval_replay_counts["critics"] == 1
                    and approval_replay_counts["critic_agents"] == 1,
                "critic_and_synthesis_holds_reserved_once": approval_replay_counts["reservations"] == 3,
                "create_outbox_intent_inserted_once": approval_replay_counts["outbox"] == 2,
                "replay_effects_unchanged": approval_replay_counts == approval_counts_before,
                "planning_result_accepted": adopted_spawn_state == "ACCEPTED",
            },
            "postgres_concurrent_continuation_adoption": {
                "inbox_id": continuation_race["inbox_id"],
                "row_lock_contenders": continuation_race["attempts"],
                "before": continuation_race["before"],
                "after": continuation_race["after"],
                "competing_result": concurrent_continue_result,
                "single_approved_reasoning_step": concurrent_continue_passed,
                "provider_requests_at_approval": reasoning_approval_snapshot["provider_requests_at_approval"],
            },
        }
        if (
            not report["normal_path"]["passed"] or not report["normal_path"]["replay_no_new_effects"]
            or not report["normal_path"]["concurrent_spawn_replay_no_new_effects"]
            or not report["normal_path"]["continuation_restart_boundaries"]["passed"]
            or not report["normal_path"]["postgres_concurrent_continuation_adoption"]["single_approved_reasoning_step"]
            or not all(critic_output_boundaries.values())
            or not all(continuation_output_boundaries[key] for key in (
                "unknown_next_action_rejected_by_control_plane", "blank_purpose_rejected_by_control_plane",
            ))
        ):
            raise AssertionError("Phase 5B normal path or idempotency assertions failed")
        if int(fault_state["critic_create_calls"]) != 1 or int(fault_state["critic_delete_calls"]) != 2:
            raise AssertionError("Critic create recovery or deletion retry issued an unexpected number of RPCs")

        # Persistent HEKATE creation is a lifecycle RPC, not an inference call.
        # Verify its PostgreSQL journal through the same pinned adapter/runtime
        # path used by the Task worker, including old incomplete rows.
        from hekate.application.lifecycle import ensure_hekate

        async def create_scope_actor(label: str) -> ActorContext:
            child_scope = ScopeId(f"{scope_text}:create-journal:{label}")
            child_principal = PrincipalId(f"principal:{run_id}:create-journal:{label}")
            async with factory() as uow:
                await uow.tasks.insert_scope(AuthorizationSnapshot(
                    scope=child_scope, principal_id=child_principal,
                    policy_version=policy_version, authz_epoch=1,
                ))
                await uow.commit()
            return ActorContext(
                principal_id=child_principal, scope=child_scope,
                authenticated_agent_registry_id=None, task_id=None, attempt_id=None,
                input_revision=None, policy_version=policy_version, authz_epoch=1, fence=1,
            )

        async def create_journal_snapshot(owner_scope: str) -> dict[str, object]:
            async with engine.connect() as connection:
                row = (await connection.execute(text("""
                    SELECT a.id AS registry_id, a.creation_operation_id, a.provider_agent_id,
                           a.owner_scope, a.role, a.persistence, a.intended_state, a.observed_state,
                           a.active_attempt_id, o.kind, o.request_hash, o.state, o.dispatch_state,
                           o.execution_state, o.observation, o.last_error, o.updated_at,
                           (SELECT count(*) FROM provider_calls p WHERE p.operation_id=o.id) AS provider_calls,
                           (SELECT count(*) FROM budget_reservations r WHERE r.operation_id=o.id) AS reservations,
                           (SELECT count(*) FROM budget_ledger l JOIN budget_reservations r ON r.id=l.reservation_id
                            WHERE r.operation_id=o.id) AS ledger_rows
                    FROM agent_registry a JOIN operations o ON o.id=a.creation_operation_id
                    WHERE a.owner_scope=:scope AND a.role='hekate' AND a.persistence='persistent'
                """), {"scope": owner_scope})).mappings().one()
            value = dict(row)
            value["updated_at"] = value["updated_at"].isoformat()
            return value

        def call_count(owner_scope: str, mapping_key: str) -> int:
            counts = fault_state.get(mapping_key, {})
            return int(counts.get(owner_scope, 0)) if isinstance(counts, dict) else 0

        main_create_before = await create_journal_snapshot(scope_text)
        if (
            main_create_before["kind"] != "agent.create"
            or main_create_before["state"] != "COMPLETED"
            or main_create_before["dispatch_state"] != "QUIESCENT"
            or main_create_before["execution_state"] != "QUIESCENT"
            or main_create_before["provider_agent_id"] is None
            or main_create_before["observation"].get("registry_id") != main_create_before["registry_id"]
            or int(main_create_before["provider_calls"]) != 0
            or int(main_create_before["reservations"]) != 0
            or int(main_create_before["ledger_rows"]) != 0
            or int(fault_state["hekate_create_calls"]) != 1
        ):
            raise AssertionError(f"normal persistent HEKATE creation journal is inconsistent: {main_create_before}")

        main_create_count = int(fault_state["hekate_create_calls"])
        main_list_count = call_count(scope_text, "agent_list_calls_by_owner")
        main_record = await ensure_hekate(factory, runtime, actor, hekate_config)
        main_replay = await create_journal_snapshot(scope_text)
        if (
            str(main_record.provider_id) != main_create_before["provider_agent_id"]
            or main_replay["updated_at"] != main_create_before["updated_at"]
            or main_replay["observation"] != main_create_before["observation"]
            or int(fault_state["hekate_create_calls"]) != main_create_count
            or call_count(scope_text, "agent_list_calls_by_owner") != main_list_count
        ):
            raise AssertionError("completed persistent HEKATE creation replay changed its journal or queried runtime")

        # Simulate the historical defect after a real successful create: the
        # provider binding remains, while only the lifecycle journal is stale.
        async with engine.begin() as connection:
            await connection.execute(text("""
                UPDATE operations SET state='CLAIMED', dispatch_state='NOT_STARTED',
                    execution_state='PENDING', observation='{}'::jsonb, last_error=NULL
                WHERE id=:operation
            """), {"operation": main_create_before["creation_operation_id"]})
        legacy_before = await create_journal_snapshot(scope_text)
        legacy_list_before = call_count(scope_text, "agent_list_calls_by_owner")
        legacy_create_before = int(fault_state["hekate_create_calls"])
        legacy_record = await ensure_hekate(factory, runtime, actor, hekate_config)
        legacy_after = await create_journal_snapshot(scope_text)
        if (
            legacy_before["state"] != "CLAIMED" or legacy_before["execution_state"] != "PENDING"
            or legacy_after["state"] != "COMPLETED" or legacy_after["dispatch_state"] != "QUIESCENT"
            or legacy_after["execution_state"] != "QUIESCENT"
            or legacy_after["provider_agent_id"] != main_create_before["provider_agent_id"]
            or str(legacy_record.provider_id) != legacy_after["provider_agent_id"]
            or call_count(scope_text, "agent_list_calls_by_owner") != legacy_list_before + 1
            or int(fault_state["hekate_create_calls"]) != legacy_create_before
        ):
            raise AssertionError("existing incomplete agent.create journal was not reconciled from the actual runtime agent")
        repaired_timestamp = legacy_after["updated_at"]
        repaired_observation = legacy_after["observation"]
        await bridge.close()
        bridge, runtime = await new_runtime()
        container = Container(settings=settings, runtime=runtime, uow_factory=factory, database=engine)
        restart_record = await ensure_hekate(factory, runtime, actor, hekate_config)
        replay_after_restart = await create_journal_snapshot(scope_text)
        if (
            str(restart_record.provider_id) != main_create_before["provider_agent_id"]
            or replay_after_restart["updated_at"] != repaired_timestamp
            or replay_after_restart["observation"] != repaired_observation
            or call_count(scope_text, "agent_list_calls_by_owner") != legacy_list_before + 1
            or int(fault_state["hekate_create_calls"]) != legacy_create_before
        ):
            raise AssertionError("persistent HEKATE restart replay repeated a create or lifecycle write")

        # Force a failure after the provider binding UPDATE but before the
        # operation journal UPDATE. The real external agent remains; PostgreSQL
        # must roll back both writes, then reconciliation must find that agent.
        rollback_actor = await create_scope_actor("binding-journal-rollback")
        rollback_owner = str(rollback_actor.scope)
        rollback_create_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"hekate:create:{rollback_owner}"))
        original_complete_lifecycle = PostgresDeliveryRepository.complete_lifecycle_operation
        injected_completion_failure = {"fired": False}

        async def fail_once_before_create_completion(repository, operation_id, observation):
            if str(operation_id) == rollback_create_id and not injected_completion_failure["fired"]:
                injected_completion_failure["fired"] = True
                raise RuntimeError("injected failure after provider binding, before lifecycle completion")
            await original_complete_lifecycle(repository, operation_id, observation)

        PostgresDeliveryRepository.complete_lifecycle_operation = fail_once_before_create_completion
        try:
            await ensure_hekate(factory, runtime, rollback_actor, hekate_config)
            raise AssertionError("binding/journal rollback failure injection did not fire")
        except RuntimeError as error:
            if "injected failure" not in str(error):
                raise
        finally:
            PostgresDeliveryRepository.complete_lifecycle_operation = original_complete_lifecycle
        rollback_before_recovery = await create_journal_snapshot(rollback_owner)
        if (
            not injected_completion_failure["fired"]
            or rollback_before_recovery["provider_agent_id"] is not None
            or rollback_before_recovery["intended_state"] != "CREATING"
            or rollback_before_recovery["state"] != "CLAIMED"
            or rollback_before_recovery["observation"].get("external_call_started") is not True
        ):
            raise AssertionError(f"provider binding and journal did not roll back atomically: {rollback_before_recovery}")
        rollback_create_calls_before_recovery = int(fault_state["hekate_create_calls"])
        rollback_recovered = await ensure_hekate(factory, runtime, rollback_actor, hekate_config)
        rollback_after_recovery = await create_journal_snapshot(rollback_owner)
        if (
            rollback_after_recovery["state"] != "COMPLETED"
            or rollback_after_recovery["provider_agent_id"] != str(rollback_recovered.provider_id)
            or int(fault_state["hekate_create_calls"]) != rollback_create_calls_before_recovery
        ):
            raise AssertionError("rollback recovery created another persistent HEKATE")

        # Lose the response after the pinned SDK has created the agent. The
        # persisted started marker forbids another create; owner-tag lookup
        # must find and bind the actual result.
        lost_actor = await create_scope_actor("create-response-lost")
        lost_owner = str(lost_actor.scope)
        fault_state["lose_hekate_create_response_for"] = lost_owner
        lost_create_calls_before = int(fault_state["hekate_create_calls"])
        lost_response_rejected = False
        try:
            await ensure_hekate(factory, runtime, lost_actor, hekate_config)
        except UnknownExecution:
            lost_response_rejected = True
        lost_before_recovery = await create_journal_snapshot(lost_owner)
        if (
            not lost_response_rejected or lost_before_recovery["state"] != "UNKNOWN"
            or lost_before_recovery["provider_agent_id"] is not None
        ):
            raise AssertionError("lost persistent HEKATE create response was treated as confirmed")
        lost_list_before = call_count(lost_owner, "agent_list_calls_by_owner")
        lost_record = await ensure_hekate(factory, runtime, lost_actor, hekate_config)
        lost_after_recovery = await create_journal_snapshot(lost_owner)
        lost_create_call_delta = int(fault_state["hekate_create_calls"]) - lost_create_calls_before
        if (
            lost_after_recovery["state"] != "COMPLETED"
            or lost_after_recovery["provider_agent_id"] != str(lost_record.provider_id)
            or lost_create_call_delta != 1
            or call_count(lost_owner, "agent_list_calls_by_owner") != lost_list_before + 1
        ):
            raise AssertionError("lost response recovery did not reconcile the same runtime agent")

        # Two real PostgreSQL requests share one owner scope. Pause the first
        # at the external create boundary; the second sees the durable started
        # intent and may wait/return UNKNOWN, but cannot create another agent.
        concurrent_actor = await create_scope_actor("concurrent-ensure")
        concurrent_owner = str(concurrent_actor.scope)
        create_gate = {"owner": concurrent_owner, "entered": asyncio.Event(), "release": asyncio.Event()}
        fault_state["hekate_create_gate"] = create_gate
        concurrent_create_before = int(fault_state["hekate_create_calls"])
        concurrent_first = asyncio.create_task(ensure_hekate(factory, runtime, concurrent_actor, hekate_config))
        await asyncio.wait_for(create_gate["entered"].wait(), 30)
        concurrent_list_before = call_count(concurrent_owner, "agent_list_calls_by_owner")
        concurrent_second_unknown = False
        try:
            await asyncio.wait_for(ensure_hekate(factory, runtime, concurrent_actor, hekate_config), 30)
        except UnknownExecution:
            concurrent_second_unknown = True
        concurrent_wait_state = await create_journal_snapshot(concurrent_owner)
        if (
            not concurrent_second_unknown
            or concurrent_wait_state["state"] != "UNKNOWN"
            or concurrent_wait_state["observation"].get("external_call_started") is not True
            or int(fault_state["hekate_create_calls"]) != concurrent_create_before
            or call_count(concurrent_owner, "agent_list_calls_by_owner") != concurrent_list_before + 1
        ):
            create_gate["release"].set()
            raise AssertionError("concurrent ensure started a duplicate external create")
        create_gate["release"].set()
        concurrent_record = await asyncio.wait_for(concurrent_first, 30)
        fault_state["hekate_create_gate"] = None
        concurrent_after = await create_journal_snapshot(concurrent_owner)
        concurrent_owned = await runtime.list_owned_agents(DeploymentId(concurrent_owner), OperationId(concurrent_after["creation_operation_id"]))
        if (
            concurrent_after["state"] != "COMPLETED"
            or concurrent_after["provider_agent_id"] != str(concurrent_record.provider_id)
            or len(concurrent_owned) != 1
            or int(fault_state["hekate_create_calls"]) != concurrent_create_before + 1
        ):
            raise AssertionError("concurrent persistent HEKATE ensure did not converge to one confirmed agent")

        # A mismatched stored binding cannot be repaired by choosing a different
        # runtime candidate. The actual pinned list query is still performed.
        mismatch_actor = await create_scope_actor("provider-binding-mismatch")
        mismatch_owner = str(mismatch_actor.scope)
        mismatch_record = await ensure_hekate(factory, runtime, mismatch_actor, hekate_config)
        mismatch_initial = await create_journal_snapshot(mismatch_owner)
        wrong_provider_id = f"provider-mismatch:{run_id}"
        async with engine.begin() as connection:
            await connection.execute(text("""
                UPDATE operations SET state='CLAIMED', dispatch_state='NOT_STARTED', execution_state='PENDING',
                    observation=CAST(:observation AS jsonb), last_error=NULL
                WHERE id=:operation
            """), {"operation": mismatch_initial["creation_operation_id"], "observation": json.dumps({"external_call_started": True})})
            await connection.execute(text("UPDATE agent_registry SET provider_agent_id=:provider WHERE id=:registry"), {
                "provider": wrong_provider_id, "registry": mismatch_initial["registry_id"],
            })
        mismatch_list_before = call_count(mismatch_owner, "agent_list_calls_by_owner")
        mismatch_rejected = False
        try:
            await ensure_hekate(factory, runtime, mismatch_actor, hekate_config)
        except Conflict:
            mismatch_rejected = True
        mismatch_after_rejection = await create_journal_snapshot(mismatch_owner)
        if (
            not mismatch_rejected or mismatch_after_rejection["state"] != "UNKNOWN"
            or mismatch_after_rejection["provider_agent_id"] != wrong_provider_id
            or call_count(mismatch_owner, "agent_list_calls_by_owner") != mismatch_list_before + 1
        ):
            raise AssertionError("provider binding mismatch was overwritten or falsely completed")
        async with engine.begin() as connection:
            await connection.execute(text("""
                UPDATE agent_registry SET provider_agent_id=:provider WHERE id=:registry
            """), {"provider": str(mismatch_record.provider_id), "registry": mismatch_initial["registry_id"]})
            await connection.execute(text("""
                UPDATE operations SET state='CLAIMED', dispatch_state='NOT_STARTED', execution_state='PENDING',
                    observation=CAST(:observation AS jsonb), last_error=NULL
                WHERE id=:operation
            """), {"operation": mismatch_initial["creation_operation_id"], "observation": json.dumps({"external_call_started": True})})
        mismatch_recovered = await ensure_hekate(factory, runtime, mismatch_actor, hekate_config)
        mismatch_final = await create_journal_snapshot(mismatch_owner)
        if mismatch_final["state"] != "COMPLETED" or str(mismatch_recovered.provider_id) != mismatch_final["provider_agent_id"]:
            raise AssertionError("binding mismatch fixture did not recover after the fixture was corrected")

        async with engine.begin() as connection:
            await connection.execute(text("""
                UPDATE operations SET state='CLAIMED', dispatch_state='NOT_STARTED', execution_state='PENDING',
                    observation='{}'::jsonb, last_error=NULL
                WHERE id=:operation
            """), {"operation": mismatch_final["creation_operation_id"]})
        fault_state["fail_agent_list_after_query_for_owner"] = mismatch_owner
        lookup_failure_rejected = False
        try:
            await ensure_hekate(factory, runtime, mismatch_actor, hekate_config)
        except UnknownExecution:
            lookup_failure_rejected = True
        lookup_failure_state = await create_journal_snapshot(mismatch_owner)
        if (
            not lookup_failure_rejected or lookup_failure_state["state"] != "UNKNOWN"
            or lookup_failure_state["provider_agent_id"] != mismatch_final["provider_agent_id"]
        ):
            raise AssertionError("owned-agent lookup failure was treated as confirmed creation")
        lookup_recovered = await ensure_hekate(factory, runtime, mismatch_actor, hekate_config)
        lookup_recovered_state = await create_journal_snapshot(mismatch_owner)
        if (
            lookup_recovered_state["state"] != "COMPLETED"
            or str(lookup_recovered.provider_id) != mismatch_final["provider_agent_id"]
        ):
            raise AssertionError("persistent HEKATE lookup failure did not recover on a later confirmed query")

        # A verified creation can complete its journal while the registry is
        # BUSY. It must leave the lease/fence and BUSY state untouched.
        busy_actor = await create_scope_actor("busy-registry")
        busy_owner = str(busy_actor.scope)
        busy_record = await ensure_hekate(factory, runtime, busy_actor, hekate_config)
        async with factory() as uow:
            busy_lease = await uow.agents.acquire_lease(busy_record.registry_id, f"phase5a-busy-{run_id}", 240)
            await uow.agents.set_registry_busy(busy_record.registry_id, str(report["workflow"]["planning_attempt_id"]))
            hold_before = await uow.agents.active_execution_hold(busy_record.registry_id, lock=True)
            await uow.commit()
        busy_initial = await create_journal_snapshot(busy_owner)
        async with engine.begin() as connection:
            await connection.execute(text("""
                UPDATE operations SET state='CLAIMED', dispatch_state='NOT_STARTED', execution_state='PENDING',
                    observation='{}'::jsonb, last_error=NULL
                WHERE id=:operation
            """), {"operation": busy_initial["creation_operation_id"]})
        busy_unknown = False
        try:
            await ensure_hekate(factory, runtime, busy_actor, hekate_config)
        except UnknownExecution:
            busy_unknown = True
        busy_final = await create_journal_snapshot(busy_owner)
        async with factory() as uow:
            lease_after = await uow.agents.get_lease(busy_record.registry_id)
            hold_after = await uow.agents.active_execution_hold(busy_record.registry_id, lock=True)
            await uow.commit()
        if (
            not busy_unknown or busy_final["state"] != "COMPLETED"
            or busy_final["intended_state"] != "BUSY"
            or busy_final["provider_agent_id"] != str(busy_record.provider_id)
            or lease_after is None or busy_lease is None
            or (lease_after.owner, lease_after.fence, lease_after.expires_at) != (
                busy_lease.owner, busy_lease.fence, busy_lease.expires_at,
            )
            or hold_before != hold_after
        ):
            raise AssertionError("create journal completion changed BUSY registry or its lease/execution hold")

        report["persistent_hekate_create_journal"] = {
            "passed": True,
            "normal_generation": main_create_before,
            "completed_replay": {
                "provider_agent_id": str(main_record.provider_id), "same_journal_observation": True,
                "same_completion_timestamp": True, "new_create_calls": 0, "new_owner_list_calls": 0,
            },
            "restart_replay": {
                "same_provider_agent_id": True, "journal_unchanged": True,
                "new_create_calls": 0, "new_owner_list_calls": 0,
            },
            "legacy_incomplete_journal": {
                "before": legacy_before, "after": legacy_after,
                "runtime_owner_tag_list_calls": 1, "new_create_calls": 0,
                "replay_unchanged": True,
            },
            "binding_journal_transaction_rollback": {
                "failure_injected_after_binding_before_completion": bool(injected_completion_failure["fired"]),
                "rolled_back_state": rollback_before_recovery,
                "recovered_state": rollback_after_recovery,
                "new_create_calls_during_recovery": 0,
            },
            "create_response_lost": {
                "before_recovery": lost_before_recovery, "after_recovery": lost_after_recovery,
                "create_invocation_delta_for_scenario": lost_create_call_delta,
                "actual_owner_tag_query": True,
            },
            "postgres_concurrent_ensure": {
                "competing_call_unknown_while_first_create_waited": concurrent_second_unknown,
                "same_created_registry_provider_id": str(concurrent_record.provider_id),
                "confirmed_owned_agent_count": len(concurrent_owned),
                "new_create_calls": 1,
                "started_marker_prevented_second_create": True,
            },
            "provider_binding_mismatch": {
                "rejected": mismatch_rejected, "rejected_state": mismatch_after_rejection,
                "recovered_after_fixture_restore": mismatch_final,
                "owned_agent_query_response_lost": {
                    "rejected_as_unconfirmed": lookup_failure_rejected,
                    "after_failure": lookup_failure_state,
                    "after_later_confirmed_query": lookup_recovered_state,
                },
            },
            "busy_registry_preserved": {
                "ensure_waited": busy_unknown, "journal_completed": busy_final["state"] == "COMPLETED",
                "registry_state": busy_final["intended_state"],
                "provider_agent_id_unchanged": busy_final["provider_agent_id"] == str(busy_record.provider_id),
                "lease_owner": lease_after.owner if lease_after else None,
                "lease_fence": lease_after.fence if lease_after else None,
                "lease_expiry_unchanged": bool(lease_after and busy_lease and lease_after.expires_at == busy_lease.expires_at),
                "active_execution_hold_unchanged": hold_before == hold_after,
            },
            "create_operations_have_no_provider_calls_or_budget_reservations": all(
                int(snapshot[key]) == 0
                for snapshot in (
                    main_create_before, rollback_after_recovery, lost_after_recovery,
                    concurrent_after, mismatch_final, busy_final,
                )
                for key in ("provider_calls", "reservations", "ledger_rows")
            ),
            "real_pinned_runtime_create_calls": int(fault_state["hekate_create_calls"]),
            "owner_scoped_agent_list_calls": dict(fault_state["agent_list_calls_by_owner"]),
            "targeted_agent_get_calls": dict(fault_state["agent_observe_calls_by_id"]),
            "provider_inference_requests_added": 0,
        }
        if not report["persistent_hekate_create_journal"]["create_operations_have_no_provider_calls_or_budget_reservations"]:
            raise AssertionError("agent.create lifecycle operations acquired inference accounting effects")

        async def wait_task_workflow_complete(task_value: dict[str, object]) -> dict[str, object]:
            scenario_task_id = str(task_value["task_id"])
            end = asyncio.get_running_loop().time() + 240
            snapshot: dict[str, object] = {}
            while asyncio.get_running_loop().time() < end:
                async with engine.connect() as connection:
                    row = (await connection.execute(text("""
                        SELECT t.status, t.outcome, t.stop_reason, t.critic_agents,
                               t.review_rounds, t.hekate_continuations,
                               (SELECT count(*) FROM task_responses r WHERE r.task_id=t.id) AS responses,
                               (SELECT stage FROM critic_workflows w WHERE w.task_id=t.id) AS workflow_stage
                        FROM tasks t WHERE t.id=:task
                    """), {"task": scenario_task_id})).mappings().one()
                snapshot = dict(row)
                if snapshot["status"] in {"FAILED", "CANCELLED"}:
                    raise AssertionError(f"normal bounded path failed to converge: {snapshot}")
                if snapshot["status"] == "COMPLETED" and snapshot["responses"] == 1 and (
                    snapshot["workflow_stage"] is None or snapshot["workflow_stage"] == "COMPLETE"
                ):
                    return snapshot
                await asyncio.sleep(0.1)
            raise TimeoutError(f"bounded Task did not complete: {snapshot}")

        async def run_normal_deliberation_case(
            label: str, marker: str, expected_stages: list[str], expected_calls: int,
            expected_continuations: int, expected_reviews: int, expected_positions: int,
        ) -> dict[str, object]:
            task_value = await submit_scenario_task(label, marker=marker)
            before_calls = fake.count()
            before_observations = len(observations)
            stop, running = await _start_worker(container)
            worker_runs.append((stop, running))
            try:
                task_state = await wait_task_workflow_complete(task_value)
            finally:
                await _stop_worker(stop, running)
                worker_runs.remove((stop, running))
            observed = observations[before_observations:]
            async with engine.connect() as connection:
                steps = (await connection.execute(text("""
                    SELECT d.step_slot, d.step_kind, d.review_round, d.state, d.attempt_id,
                           d.operation_id, d.reservation_id, d.conclusion_id,
                           o.state AS operation_state, o.execution_state,
                           o.binding->>'conversation_id' AS conversation_id,
                           o.binding->>'provider_agent_id' AS provider_agent_id
                    FROM deliberation_steps d JOIN operations o ON o.id=d.operation_id
                    WHERE d.task_id=:task ORDER BY d.step_order
                """), {"task": task_value["task_id"]})).mappings().all()
                calls = await connection.scalar(text("""
                    SELECT count(*) FROM provider_calls p JOIN operations o ON o.id=p.operation_id
                    WHERE o.task_id=:task
                """), {"task": task_value["task_id"]})
                positions = await connection.scalar(text(
                    "SELECT count(*) FROM position_versions WHERE task_id=:task"
                ), {"task": task_value["task_id"]})
                receipts = await connection.scalar(text("""
                    SELECT count(*) FROM position_commit_receipts r
                    JOIN position_versions p ON p.operation_id=r.operation_id
                    WHERE p.task_id=:task
                """), {"task": task_value["task_id"]})
                dissent_count = await connection.scalar(text("""
                    SELECT count(*) FROM dissent d JOIN conclusions c ON c.id=d.conclusion_id
                    WHERE c.task_id=:task
                """), {"task": task_value["task_id"]})
                review_operations = (await connection.execute(text("""
                    SELECT o.id, o.binding->>'attempt_id' AS attempt_id,
                           o.binding->>'agent_registry_id' AS registry_id,
                           o.binding->>'provider_agent_id' AS provider_agent_id,
                           o.binding->>'conversation_id' AS conversation_id
                    FROM operations o WHERE o.task_id=:task AND o.kind='critic.review'
                    ORDER BY o.created_at, o.id
                """), {"task": task_value["task_id"]})).mappings().all()
                critic_state = await connection.scalar(text("""
                    SELECT ar.intended_state FROM critic_workflows w
                    JOIN agent_registry ar ON ar.id=w.critic_registry_id WHERE w.task_id=:task
                """), {"task": task_value["task_id"]})
            observed_stages = [str(item["stage"]) for item in observed]
            if (
                observed_stages != expected_stages or fake.count() - before_calls != expected_calls
                or int(calls or 0) != expected_calls or task_state["status"] != "COMPLETED"
                or int(task_state["responses"]) != 1
                or int(task_state["hekate_continuations"]) != expected_continuations
                or int(task_state["review_rounds"]) != expected_reviews
                or int(task_state["critic_agents"]) != (1 if expected_reviews else 0)
                or int(positions or 0) != expected_positions
                or int(receipts or 0) != expected_positions
                or int(dissent_count or 0) != expected_reviews
                or (expected_reviews > 0 and critic_state != "DELETED")
                or len(review_operations) != expected_reviews
                or (expected_reviews == 2 and (
                    review_operations[0]["registry_id"] != review_operations[1]["registry_id"]
                    or review_operations[0]["provider_agent_id"] != review_operations[1]["provider_agent_id"]
                    or review_operations[0]["conversation_id"] == review_operations[1]["conversation_id"]
                    or review_operations[0]["attempt_id"] == review_operations[1]["attempt_id"]
                    or review_operations[0]["id"] == review_operations[1]["id"]
                ))
            ):
                raise AssertionError(
                    f"bounded deliberation path mismatch for {label}: task={task_state}, "
                    f"stages={observed_stages}, calls={calls}, steps={[dict(step) for step in steps]}"
                )
            return {
                "task_id": task_value["task_id"], "evidence_id": task_value["evidence_id"],
                "topic_id": task_value["topic_id"], "task_state": task_state,
                "provider_requests": fake.count() - before_calls,
                "provider_call_rows": int(calls or 0), "observed_stages": observed_stages,
                "steps": [dict(step) for step in steps],
                "critic_review_operations": [dict(item) for item in review_operations],
                "critic_state": critic_state, "dissent_rows": int(dissent_count or 0),
                "position_versions": int(positions or 0), "position_receipts": int(receipts or 0),
                "passed": True,
            }

        solo_path = await run_normal_deliberation_case(
            "solo-continuation", "PHASE5B_SOLO_CONTINUATION",
            ["planning_continue", "hekate_reasoning_answer"], 2, 1, 0, 0,
        )
        review_only_path = await run_normal_deliberation_case(
            "review-round-two", "PHASE5B_REVIEW_CHAIN",
            ["planning", "critic_review_1", "synthesis_1_review_continue", "critic_review_2", "synthesis_2"],
            5, 0, 2, 1,
        )
        report["bounded_path_scenarios"] = {
            "hekate_solo_continuation": solo_path,
            "second_critic_review_and_synthesis": review_only_path,
            "combined_with_one_hekate_continuation": report["normal_path"]["provider_requests"],
            "inference_counts": {
                "hekate_solo_continuation": solo_path["provider_requests"],
                "second_critic_review_and_synthesis": review_only_path["provider_requests"],
                "combined": report["normal_path"]["provider_requests"],
                "compaction_calls": 0,
            },
        }

        # A separate Task proves that an exhausted Task budget rejects the
        # combined Critic+synthesis hold before creating a registry or intent.
        normal_provider_requests = fake.count()
        from hekate.application.tasks import submit
        from hekate.domain.models import UserMessage
        budget_task_receipt = await submit(factory, actor, UserMessage(
            text="Can this seal rule be used for this decision?",
            topic_id="phase5a-main-topic", evidence_refs=(evidence_id,),
        ), f"phase5a-{run_id}-budget-denial", hekate_config)
        budget_task_id = str(budget_task_receipt["task_id"])
        budget_result_entered = asyncio.Event()
        release_budget_result = asyncio.Event()
        budget_result_inbox = None
        budget_result_paused = False

        async def hold_budget_result_before_adoption(repository, inbox_id):
            nonlocal budget_result_inbox, budget_result_paused
            result = await original_lock_turn_result(repository, inbox_id)
            if (
                result is None or budget_result_paused or result["task_id"] != budget_task_id
                or not isinstance(result["proposal"], dict)
                or result["proposal"].get("action") != "spawn"
            ):
                return result
            state = (await repository.connection.execute(text(
                "SELECT execution_state FROM operations WHERE id=:operation"
            ), {"operation": result["operation_id"]})).scalar_one()
            if state == "QUIESCENT":
                budget_result_paused = True
                budget_result_inbox = inbox_id
                budget_result_entered.set()
                await release_budget_result.wait()
            return result

        PostgresKnowledgeRepository.lock_turn_result = hold_budget_result_before_adoption
        budget_stop, budget_worker = await _start_worker(container)
        worker_runs.append((budget_stop, budget_worker))
        try:
            await asyncio.wait_for(budget_result_entered.wait(), 120)
            required_critic_hold = _reservation_amount(critic_config)
            async with engine.begin() as connection:
                budget_account = (await connection.execute(text("""
                    SELECT id, spent_amount, held_amount
                    FROM budget_accounts WHERE scope_kind='TASK' AND scope_ref=:task
                """), {"task": budget_task_id})).mappings().one()
                spent_amount = Decimal(budget_account["spent_amount"])
                held_amount = Decimal(budget_account["held_amount"])
                remaining_before_spawn = spent_amount + held_amount + Decimal("0.001")
                available_before_spawn = remaining_before_spawn - spent_amount - held_amount
                if available_before_spawn >= required_critic_hold:
                    raise AssertionError("budget fixture still has enough credit for a Critic hold")
                await connection.execute(text(
                    "UPDATE budget_accounts SET limit_amount=:limit WHERE id=:id"
                ), {"limit": remaining_before_spawn, "id": budget_account["id"]})
            release_budget_result.set()

            budget_end = asyncio.get_running_loop().time() + 120
            budget_terminal = None
            while asyncio.get_running_loop().time() < budget_end:
                async with engine.connect() as connection:
                    budget_terminal = (await connection.execute(text("""
                        SELECT t.status, t.critic_agents, t.review_rounds, r.processing_state, r.rejection_reason
                        FROM tasks t JOIN turn_results r ON r.task_id=t.id
                        WHERE t.id=:task
                    """), {"task": budget_task_id})).mappings().one_or_none()
                if budget_terminal and budget_terminal["status"] == "FAILED" and budget_terminal["processing_state"] == "REJECTED":
                    break
                await asyncio.sleep(0.1)
            else:
                raise TimeoutError("insufficient-budget spawn did not converge without a Critic")
            await _stop_worker(budget_stop, budget_worker)
            worker_runs.remove((budget_stop, budget_worker))
        finally:
            release_budget_result.set()
            PostgresKnowledgeRepository.lock_turn_result = original_lock_turn_result
            if not budget_worker.done():
                budget_stop.set()
                await asyncio.gather(budget_worker, return_exceptions=True)
            if (budget_stop, budget_worker) in worker_runs:
                worker_runs.remove((budget_stop, budget_worker))

        budget_counts = await _counts(engine, budget_task_id, scope_text)
        async with engine.connect() as connection:
            create_intents = await connection.scalar(text("""
                SELECT count(*) FROM outbox
                WHERE kind='critic_create' AND payload->>'task_id'=:task
            """), {"task": budget_task_id})
            child_reservations = await connection.scalar(text("""
                SELECT count(*) FROM budget_reservations br JOIN operations o ON o.id=br.operation_id
                WHERE o.task_id=:task AND o.kind IN ('critic.review','hekate.synthesis')
            """), {"task": budget_task_id})
        budget_denial_passed = (
            budget_terminal["rejection_reason"] == "budget_critic_and_synthesis_unavailable"
            and budget_counts["workflows"] == 0 and budget_counts["critics"] == 0
            and budget_counts["critic_agents"] == 0 and budget_counts["review_rounds"] == 0
            and int(create_intents) == 0 and int(child_reservations) == 0
            and budget_counts["calls"] == 1 and fake.count() == normal_provider_requests + 1
        )
        report["budget_denial_scenario"] = {
            "passed": budget_denial_passed,
            "task_id": budget_task_id,
            "planning_result_inbox_id": budget_result_inbox,
            "task_status": budget_terminal["status"],
            "result_state": budget_terminal["processing_state"],
            "rejection_reason": budget_terminal["rejection_reason"],
            "available_task_budget_before_spawn": str(available_before_spawn),
            "required_critic_envelope": str(required_critic_hold),
            "provider_requests_for_scenario": fake.count() - normal_provider_requests,
            "critic_workflows": budget_counts["workflows"],
            "critic_registries": budget_counts["critics"],
            "critic_agent_counter": budget_counts["critic_agents"],
            "review_round_counter": budget_counts["review_rounds"],
            "child_reservations": int(child_reservations),
            "critic_create_intents": int(create_intents),
            "provider_calls_for_scenario": budget_counts["calls"],
            "position_versions": budget_counts["positions"],
            "task_responses": budget_counts["responses"],
        }
        if not budget_denial_passed:
            raise AssertionError("insufficient Task budget produced a Critic workflow, hold, or create intent")
        budget_provider_requests = fake.count() - normal_provider_requests

        async def run_provisional_limit_case(
            label: str, marker: str, expected_requests: int, expected_stop: str,
            expected_reviews: int, expected_continuations: int,
        ) -> dict[str, object]:
            selected_task = await submit_scenario_task(label, marker=marker)
            provider_before = fake.count()
            stop, running = await _start_worker(container)
            worker_runs.append((stop, running))
            try:
                task_state = await wait_task_workflow_complete(selected_task)
            finally:
                await _stop_worker(stop, running)
                worker_runs.remove((stop, running))
            async with engine.connect() as connection:
                details = (await connection.execute(text("""
                    SELECT t.review_rounds, t.hekate_continuations,
                           (SELECT count(*) FROM deliberation_steps d WHERE d.task_id=t.id) AS steps,
                           (SELECT count(*) FROM position_versions p WHERE p.task_id=t.id) AS positions,
                           (SELECT count(*) FROM task_responses r WHERE r.task_id=t.id) AS responses,
                           (SELECT stage FROM critic_workflows w WHERE w.task_id=t.id) AS workflow_stage,
                           (SELECT ar.intended_state FROM critic_workflows w
                             JOIN agent_registry ar ON ar.id=w.critic_registry_id WHERE w.task_id=t.id) AS critic_state
                    FROM tasks t WHERE t.id=:task
                """), {"task": selected_task["task_id"]})).mappings().one()
                slots = (await connection.execute(text("""
                    SELECT step_slot, state, stop_reason FROM deliberation_steps
                    WHERE task_id=:task ORDER BY step_order
                """), {"task": selected_task["task_id"]})).mappings().all()
            requests = fake.count() - provider_before
            if (
                requests != expected_requests or task_state["status"] != "COMPLETED"
                or task_state["outcome"] != "PROVISIONAL_ANSWER"
                or task_state["stop_reason"] != expected_stop
                or int(details["review_rounds"]) != expected_reviews
                or int(details["hekate_continuations"]) != expected_continuations
                or int(details["positions"]) != 0 or int(details["responses"]) != 1
                or details["workflow_stage"] != "COMPLETE"
                or (expected_reviews > 0 and details["critic_state"] != "DELETED")
            ):
                raise AssertionError(
                    f"bounded stop {label} did not converge without extra inference: "
                    f"task={task_state}, details={dict(details)}, slots={[dict(row) for row in slots]}, requests={requests}"
                )
            return {
                "passed": True, "task_id": selected_task["task_id"], "marker": marker,
                "provider_requests": requests, "task_state": task_state,
                "deliberation_steps": [dict(row) for row in slots],
                "review_rounds": int(details["review_rounds"]),
                "hekate_continuations": int(details["hekate_continuations"]),
                "position_versions": int(details["positions"]), "critic_state": details["critic_state"],
            }

        cap_scenarios = {
            "third_critic_review_request": await run_provisional_limit_case(
                "third-critic-review-cap", "PHASE5B_REVIEW_CAP", 5, "ROUND_LIMIT", 2, 0,
            ),
            "second_hekate_reasoning_request": await run_provisional_limit_case(
                "second-hekate-reasoning-cap", "PHASE5B_REASONING_CAP", 4, "ROUND_LIMIT", 1, 1,
            ),
            "duplicate_continuation_request": await run_provisional_limit_case(
                "duplicate-hekate-reasoning", "PHASE5B_DUPLICATE_REASONING", 4, "NO_NEW_WORK", 1, 1,
            ),
        }
        report["bounded_stop_scenarios"] = cap_scenarios

        failure_scenarios: dict[str, object] = {}
        worker_name = settings.worker_id
        hekate_registry_id = RegistryId(str(hekate_state["id"]))

        # A busy registry and an unavailable lease are both temporary waits.
        # The exact same durable synthesis step is resumed once each blocker clears.
        busy_task = await submit_scenario_task("busy-lease-resume")
        busy_gate = await gate_worker_at(busy_task, "SYNTHESIS_PENDING")
        busy_before = await scenario_snapshot(str(busy_task["task_id"]))
        busy_provider_before = fake.count()
        async with factory() as uow:
            await uow.agents.set_registry_busy(
                hekate_registry_id, str(report["workflow"]["planning_attempt_id"]),
            )
            await uow.commit()
        busy_leases = await original_prepare(
            factory, runtime, actor, hekate_config, critic_config, worker_name,
            archive_root=archive_root,
        )
        async with factory() as uow:
            await uow.agents.set_registry_ready(hekate_registry_id)
            contender_lease = await uow.agents.acquire_lease(hekate_registry_id, f"lease-contender-{run_id}", 60)
            if contender_lease is None:
                raise AssertionError("lease contention fixture failed to acquire the real PostgreSQL lease")
            await uow.commit()
        lease_wait_leases = await original_prepare(
            factory, runtime, actor, hekate_config, critic_config, worker_name,
            archive_root=archive_root,
        )
        async with factory() as uow:
            await uow.agents.release_lease(contender_lease)
            await uow.commit()
        busy_after_waits = await scenario_snapshot(str(busy_task["task_id"]))
        if (
            busy_leases or lease_wait_leases or fake.count() != busy_provider_before
            or busy_before != busy_after_waits
            or busy_after_waits["workflow"]["stage"] != "SYNTHESIS_PENDING"
        ):
            raise AssertionError("BUSY or lease contention changed the pending synthesis workflow")
        await finish_gated_worker(busy_gate)
        busy_final = await scenario_snapshot(str(busy_task["task_id"]))
        busy_task_requests = [item for item in observations if item["task_id"] == busy_task["task_id"]]
        if (
            busy_final["task"]["status"] != "COMPLETED"
            or busy_final["workflow"]["stage"] != "COMPLETE"
            or busy_final["position_versions"] != 1
            or [item["stage"] for item in busy_task_requests] != ["planning", "critic_review_1", "synthesis"]
        ):
            raise AssertionError("the original synthesis did not resume exactly once after temporary waits")
        failure_scenarios["busy_and_lease_wait"] = {
            "passed": True, "task_id": busy_task["task_id"],
            "workflow_before_waits": busy_before, "workflow_after_waits": busy_after_waits,
            "final": busy_final, "provider_requests_during_waits": 0,
            "task_provider_requests": [item["stage"] for item in busy_task_requests],
            "same_attempt_and_operation_resumed": (
                busy_before["workflow"]["synthesis_operation_id"]
                == busy_final["workflow"]["synthesis_operation_id"]
                and busy_before["workflow"]["stage"] == "SYNTHESIS_PENDING"
            ),
        }

        # Cancel, deadline, and revision changes are their own terminal/superseded
        # transitions. They must seal only unadmitted child steps and retire the Critic.
        from hekate.application.tasks import cancel, revise
        from hekate.domain.models import InputChange
        from hekate.domain.types import StopReason

        async def stop_waiting_workflow(label: str, stop_kind: str) -> dict[str, object]:
            selected_task = await submit_scenario_task(label)
            gate = await gate_worker_at(selected_task, "CRITIC_READY")
            provider_before_stop = fake.count()
            if stop_kind == "cancel":
                await cancel(factory, actor, TaskId(str(selected_task["task_id"])), StopReason.USER_CANCELLED)
            elif stop_kind == "deadline":
                async with engine.begin() as connection:
                    await connection.execute(text(
                        "UPDATE tasks SET deadline=now() - interval '1 second' WHERE id=:task"
                    ), {"task": selected_task["task_id"]})
            elif stop_kind == "revision":
                await revise(
                    factory, actor, TaskId(str(selected_task["task_id"])), 1,
                    InputChange(text=f"Revised {label} question", expected_revision=1, constraints={}),
                )
            else:
                raise AssertionError(f"unknown workflow stop scenario {stop_kind}")
            await finish_gated_worker(gate)
            final = await scenario_snapshot(str(selected_task["task_id"]))
            expected = {
                "cancel": ("CANCELLED", "USER_CANCELLED", 1),
                "deadline": ("FAILED", "DEADLINE", 1),
                "revision": ("WAITING", None, 2),
            }[stop_kind]
            if (
                fake.count() != provider_before_stop
                or final["task"]["status"] != expected[0]
                or final["task"]["stop_reason"] != expected[1]
                or final["task"]["input_revision"] != expected[2]
                or final["response"] is not None or final["position_versions"] != 0
                or final["workflow"]["stage"] != "COMPLETE"
                or final["workflow"]["critic_state"] != "DELETED"
                or final["task"]["critic_agents"] != 1
            ):
                raise AssertionError(f"{stop_kind} did not preserve its workflow stop semantics: {final}")
            return {
                "passed": True, "task_id": selected_task["task_id"],
                "provider_requests_after_stop": fake.count() - provider_before_stop,
                "final": final,
            }

        failure_scenarios["cancel"] = await stop_waiting_workflow("waiting-cancel", "cancel")
        failure_scenarios["deadline"] = await stop_waiting_workflow("waiting-deadline", "deadline")
        failure_scenarios["revision_superseded"] = await stop_waiting_workflow("waiting-revision", "revision")

        from hekate.infrastructure.postgres import task_repository as task_repository_module

        async def create_ready_solo_continuation(
            label: str, *, pause_maintenance: bool = False,
        ) -> tuple[dict[str, object], dict[str, object]]:
            task_value = await submit_scenario_task(label, marker="PHASE5B_SOLO_CONTINUATION")
            gate_value = await gate_dynamic_step(
                task_value, "hekate_reasoning_1", pause_maintenance=pause_maintenance,
            )
            task_requests = [item["stage"] for item in observations if item["task_id"] == task_value["task_id"]]
            if task_requests != ["planning_continue"]:
                raise AssertionError(f"standalone continuation did not stop after real planning: {task_requests}")
            return task_value, gate_value

        async def selected_ready_step(task_id_value: str) -> dict[str, object]:
            async with factory() as uow:
                rows = await uow.deliberation.list_for_task(TaskId(task_id_value))
                selected = next((row for row in rows if row["state"] == "READY"), None)
                await uow.commit()
            if selected is None:
                raise AssertionError(f"Task {task_id_value} has no durable READY deliberation step")
            return dict(selected)

        async def prove_reject_cas_rollback(task_value: dict[str, object]) -> dict[str, object]:
            task_id_value = str(task_value["task_id"])
            selected = await selected_ready_step(task_id_value)
            before = await deliberation_cleanup_snapshot(task_id_value)
            async with engine.connect() as connection:
                deadline_value = await connection.scalar(text(
                    "SELECT deadline FROM tasks WHERE id=:task"
                ), {"task": task_id_value})
            # The application checks the real, still-live Task first. The actual
            # Postgres repository then evaluates its deadline predicate against
            # this controlled clock and returns False from its normal SQL path.
            old_clock = task_repository_module.aware_now
            task_repository_module.aware_now = lambda: deadline_value + timedelta(seconds=1)
            try:
                outcome = await reject_deliberation_step(
                    factory, selected, failure_code="policy_rejected",
                )
            finally:
                task_repository_module.aware_now = old_clock
            after = await deliberation_cleanup_snapshot(task_id_value)
            if outcome != "task_changed" or before != after:
                raise AssertionError(
                    f"failed Task terminal write partially committed deliberation effects: "
                    f"outcome={outcome}, before={before}, after={after}"
                )
            if (
                after["task"]["status"] != "WAITING" or after["responses"] != 0
                or after["steps"][0]["step_state"] != "READY"
                or after["steps"][0]["operation_state"] != "CLAIMED"
                or Decimal(after["steps"][0]["reservation_held"]) <= 0
            ):
                raise AssertionError(f"rollback did not restore the pre-rejection state: {after}")
            return {"outcome": outcome, "before": before, "after": after, "actual_repository_deadline_check": True}

        async def clean_standalone_state(value: dict[str, object], label: str) -> tuple[dict[str, object], list[int]]:
            task_id_value = str(value["task_id"])
            if label == "deadline":
                expected_status, expected_reason, expected_revision = "FAILED", "DEADLINE", 1
            elif label == "cancel":
                expected_status, expected_reason, expected_revision = "CANCELLED", "USER_CANCELLED", 1
            elif label == "revision":
                expected_status, expected_reason, expected_revision = "WAITING", None, 2
            else:
                raise AssertionError(label)

            def done(snapshot: dict[str, object]) -> bool:
                if not snapshot["steps"]:
                    return False
                step = snapshot["steps"][0]
                return (
                    snapshot["task"]["status"] == expected_status
                    and snapshot["task"]["stop_reason"] == expected_reason
                    and snapshot["task"]["input_revision"] == expected_revision
                    and step["step_state"] == "STOPPED"
                    and step["operation_state"] == "FAILED"
                    and step["execution_state"] == "QUIESCENT"
                    and step["reservation_state"] in {"RELEASED", "SETTLED"}
                    and Decimal(step["reservation_held"]) == 0
                )

            return await run_worker_until_deliberation_state(task_id_value, done, f"standalone {label} cleanup")

        # Deadline CAS fails after the stop transaction has staged all effects.
        # Rollback restores READY/CLAIMED and the budget hold; the real worker
        # maintenance then seals and releases it before deadline convergence.
        deadline_task, _ = await create_ready_solo_continuation("standalone-deadline-rollback")
        deadline_rollback = await prove_reject_cas_rollback(deadline_task)
        deadline_provider_before = fake.count()
        async with engine.begin() as connection:
            await connection.execute(text(
                "UPDATE tasks SET deadline=now() - interval '1 second' WHERE id=:task"
            ), {"task": deadline_task["task_id"]})
        deadline_clean, deadline_ticks = await clean_standalone_state(deadline_task, "deadline")
        if (
            fake.count() != deadline_provider_before or deadline_clean["responses"] != 0
            or deadline_clean["position_versions"] != 0
            or deadline_clean["task"]["hekate_continuations"] != 1
            or Decimal(deadline_clean["steps"][0]["reservation_held"]) != 0
        ):
            raise AssertionError(f"standalone deadline did not safely converge: {deadline_clean}")
        deadline_replay, deadline_replay_ticks = await clean_standalone_state(deadline_task, "deadline")
        if deadline_replay != deadline_clean:
            raise AssertionError("standalone deadline cleanup changed effects after worker restart")
        failure_scenarios["standalone_deadline_and_reject_rollback"] = {
            "passed": True, "task_id": deadline_task["task_id"],
            "rollback": deadline_rollback, "worker_maintenance_ticks": deadline_ticks,
            "worker_restart_ticks": deadline_replay_ticks, "after_cleanup": deadline_clean,
            "provider_requests_after_ready_gate": fake.count() - deadline_provider_before,
        }

        # Cancellation cleanup is raced by two real PostgreSQL maintenance
        # transactions; the durable release ledger and audit remain single.
        cancel_task, _ = await create_ready_solo_continuation("standalone-cancel-race")
        cancel_provider_before = fake.count()
        await cancel(factory, actor, TaskId(str(cancel_task["task_id"])), StopReason.USER_CANCELLED)
        concurrent_maintenance = await asyncio.gather(
            original_maintain_deliberation(factory, limit=100),
            original_maintain_deliberation(factory, limit=100),
        )
        cancel_clean, cancel_ticks = await clean_standalone_state(cancel_task, "cancel")
        if (
            fake.count() != cancel_provider_before
            or len(cancel_clean["stop_audits"]) != 1
            or len(cancel_clean["release_ledger"]) != 1
            or int(cancel_clean["release_ledger"][0]["count"]) != 2
            or Decimal(cancel_clean["steps"][0]["reservation_held"]) != 0
        ):
            raise AssertionError(f"concurrent cancellation maintenance duplicated effects: {cancel_clean}")
        cancel_replay, cancel_replay_ticks = await clean_standalone_state(cancel_task, "cancel")
        if cancel_replay != cancel_clean:
            raise AssertionError("worker restart changed an already-cleaned cancelled continuation")
        failure_scenarios["standalone_cancel_concurrent_maintenance"] = {
            "passed": True, "task_id": cancel_task["task_id"],
            "maintenance_results": concurrent_maintenance,
            "worker_replay_ticks": cancel_replay_ticks, "initial_worker_ticks": cancel_ticks,
            "after_cleanup": cancel_clean,
            "provider_requests_after_ready_gate": fake.count() - cancel_provider_before,
        }

        # Revision cleanup is cleanup-only: the revised WAITING Task remains
        # untouched while its old READY operation and reservation are sealed.
        revision_task, _ = await create_ready_solo_continuation("standalone-revision-superseded")
        revision_provider_before = fake.count()
        await revise(
            factory, actor, TaskId(str(revision_task["task_id"])), 1,
            InputChange(text="Revised continuation question", expected_revision=1, constraints={}),
        )
        revision_clean, revision_ticks = await clean_standalone_state(revision_task, "revision")
        if (
            fake.count() != revision_provider_before or revision_clean["responses"] != 0
            or revision_clean["position_versions"] != 0
            or revision_clean["steps"][0]["step_revision"] != 1
            or revision_clean["steps"][0]["step_stop_reason"] != "revision_superseded"
            or revision_clean["task"]["hekate_continuations"] != 1
        ):
            raise AssertionError(f"superseded revision was modified or replanned: {revision_clean}")
        revision_replay, revision_replay_ticks = await clean_standalone_state(revision_task, "revision")
        if revision_replay != revision_clean:
            raise AssertionError("worker restart changed an already-cleaned superseded continuation")
        failure_scenarios["standalone_revision_superseded"] = {
            "passed": True, "task_id": revision_task["task_id"],
            "worker_maintenance_ticks": revision_ticks, "worker_restart_ticks": revision_replay_ticks,
            "after_cleanup": revision_clean,
            "provider_requests_after_ready_gate": fake.count() - revision_provider_before,
        }

        # A second false terminal CAS with an existing Critic workflow verifies
        # reject_deliberation_step rolls back both the workflow stage and child
        # step effects. The saved operation then proceeds through the normal path.
        critic_rollback_task = await submit_scenario_task(
            "critic-workflow-reject-rollback", marker="PHASE5B_FULL_CHAIN",
        )
        await gate_dynamic_step(critic_rollback_task, "hekate_reasoning_1")
        critic_provider_before = fake.count()
        critic_rollback = await prove_reject_cas_rollback(critic_rollback_task)
        if critic_rollback["after"]["critic_workflows"] != 1:
            raise AssertionError("Critic-workflow false CAS did not keep its workflow identity")
        if critic_rollback["before"]["workflow_stage"] != critic_rollback["after"]["workflow_stage"]:
            raise AssertionError("Critic-workflow stage change survived the failed Task response CAS")
        worker_service.prepare_deliberation_steps = original_dynamic_prepare
        critic_recovery_stop, critic_recovery_task = await _start_worker(container)
        worker_runs.append((critic_recovery_stop, critic_recovery_task))
        try:
            end = asyncio.get_running_loop().time() + 180
            critic_recovery = {}
            while asyncio.get_running_loop().time() < end:
                critic_recovery = await scenario_snapshot(str(critic_rollback_task["task_id"]))
                if (
                    critic_recovery["task"]["status"] == "COMPLETED"
                    and critic_recovery["workflow"]["stage"] == "COMPLETE"
                    and critic_recovery["response"] is not None
                ):
                    break
                await asyncio.sleep(0.1)
            else:
                raise TimeoutError(f"Critic workflow did not resume after reject rollback: {critic_recovery}")
        finally:
            await _stop_worker(critic_recovery_stop, critic_recovery_task)
            worker_runs.remove((critic_recovery_stop, critic_recovery_task))
        if fake.count() - critic_provider_before != 3 or critic_recovery["position_versions"] != 1:
            raise AssertionError(f"Critic workflow did not resume its saved child path exactly once: {critic_recovery}")
        failure_scenarios["critic_workflow_reject_rollback"] = {
            "passed": True, "task_id": critic_rollback_task["task_id"],
            "rollback": critic_rollback, "recovered": critic_recovery,
            "provider_requests_after_rollback": fake.count() - critic_provider_before,
        }

        # Evidence expiry at CRITIC_READY. First fail after all SQL effects but
        # before commit; verify PostgreSQL rolled back every effect, then let the
        # real worker retry and retire the actual pinned-runtime Critic.
        critic_expiry_task = await submit_scenario_task("evidence-expired-before-critic")
        critic_expiry_gate = await gate_worker_at(critic_expiry_task, "CRITIC_READY")
        critic_expiry_provider_before = fake.count()
        critic_expiry = await expire_scenario_evidence(critic_expiry_task)
        original_append_audit = PostgresDeliveryRepository.append_audit
        injected_audit_failure = {"fired": False}

        async def fail_policy_audit_after_insert(repository, event):
            await original_append_audit(repository, event)
            if event.get("event_kind") == "critic.workflow_policy_stopped" and not injected_audit_failure["fired"]:
                injected_audit_failure["fired"] = True
                raise RuntimeError("injected failure after workflow failure effects, before commit")

        PostgresDeliveryRepository.append_audit = fail_policy_audit_after_insert
        try:
            try:
                await original_prepare(
                    factory, runtime, actor, hekate_config, critic_config, worker_name,
                    archive_root=archive_root,
                )
            except RuntimeError as error:
                if "injected failure" not in str(error):
                    raise
            else:
                raise AssertionError("failure injection did not interrupt failure convergence before commit")
        finally:
            PostgresDeliveryRepository.append_audit = original_append_audit
        rollback_state = await scenario_snapshot(str(critic_expiry_task["task_id"]))
        if (
            not injected_audit_failure["fired"]
            or rollback_state["task"]["status"] != "WAITING"
            or rollback_state["workflow"]["stage"] != "CRITIC_READY"
            or rollback_state["response"] is not None or rollback_state["position_versions"] != 0
            or rollback_state["workflow"]["delete_operation_id"] is not None
            or rollback_state["workflow"]["critic_state"] != "READY"
            or any(item["status"] != "RESERVED" for item in rollback_state["reservations"])
            or rollback_state["critic_delete_outbox_rows"] != 0
        ):
            raise AssertionError(f"failure-convergence exception left partial PostgreSQL effects: {rollback_state}")
        await finish_gated_worker(critic_expiry_gate)
        critic_expiry_final = await assert_policy_failure(critic_expiry_task, "evidence_unavailable")
        before_failure_replay = await scenario_snapshot(str(critic_expiry_task["task_id"]))
        replay_leases = await original_prepare(
            factory, runtime, actor, hekate_config, critic_config, worker_name,
            archive_root=archive_root,
        )
        after_failure_replay = await scenario_snapshot(str(critic_expiry_task["task_id"]))
        if replay_leases or before_failure_replay != after_failure_replay:
            raise AssertionError("repeated permanent failure handling added response, ledger, reservation, or delete effects")
        if fake.count() != critic_expiry_provider_before:
            raise AssertionError("expired Evidence reached the provider after CRITIC_READY")
        failure_scenarios["evidence_expired_critic_ready"] = {
            "passed": True, "task_id": critic_expiry_task["task_id"],
            "provider_requests_before_expiry": critic_expiry_provider_before,
            "provider_requests_after_failure": fake.count(), "evidence": critic_expiry,
            "atomic_rollback_after_injected_failure": rollback_state,
            "final": critic_expiry_final,
            "replay_effects_unchanged": before_failure_replay == after_failure_replay,
            "failure_transaction_inbox_and_operation_source_preserved": True,
        }

        # Evidence expires after a Critic Conclusion is durable but before the
        # HEKATE synthesis admission. No third provider request is allowed.
        synthesis_task_provider_base = fake.count()
        synthesis_expiry_task = await submit_scenario_task("evidence-expired-before-synthesis")
        synthesis_gate = await gate_worker_at(synthesis_expiry_task, "SYNTHESIS_PENDING")
        synthesis_provider_before = fake.count()
        synthesis_expiry = await expire_scenario_evidence(synthesis_expiry_task)
        synthesis_workflow = await finish_gated_worker(synthesis_gate)
        synthesis_final = await assert_policy_failure(synthesis_expiry_task, "evidence_unavailable")
        if (
            synthesis_provider_before - synthesis_task_provider_base != 2
            or fake.count() != synthesis_provider_before
            or not synthesis_workflow.get("critic_conclusion_id")
            or synthesis_final["reservations"] == []
            or any(
                item["status"] not in {"SETTLED", "RELEASED"}
                for item in synthesis_final["reservations"]
            )
        ):
            raise AssertionError("synthesis Evidence expiry repeated provider work or retained an unallocated hold")
        failure_scenarios["evidence_expired_synthesis_pending"] = {
            "passed": True, "task_id": synthesis_expiry_task["task_id"],
            "workflow_before_rejection": synthesis_workflow,
            "provider_requests_before_expiry": synthesis_provider_before,
            "provider_requests_after_failure": fake.count(), "evidence": synthesis_expiry,
            "final": synthesis_final,
            "review_spend_preserved": any(
                item["kind"] == "critic.review" and item["status"] == "SETTLED"
                for item in synthesis_final["reservations"]
            ),
            "synthesis_hold_released": any(
                item["kind"] == "hekate.synthesis" and item["status"] == "RELEASED"
                for item in synthesis_final["reservations"]
            ),
        }

        # An UNKNOWN child execution remains a hold across worker restart. This
        # is explicitly a control-plane fault fixture with no synthetic call or
        # usage observation attached; it is not reported as an actual provider send.
        unknown_task = await submit_scenario_task("unknown-review-preserved")
        unknown_gate = await gate_worker_at(unknown_task, "CRITIC_READY")
        unknown_before_requests = fake.count()
        unknown_state = await _workflow_state(engine, str(unknown_task["task_id"]))
        unknown_operation_id = str(unknown_state["review_operation_id"])
        unknown_registry_id = RegistryId(str(unknown_state["critic_registry_id"]))
        async with factory() as uow:
            await uow.agents.create_execution_hold(unknown_registry_id, OperationId(unknown_operation_id))
            await uow.commit()
        async with engine.begin() as connection:
            await connection.execute(text("""
                UPDATE operations SET state='UNKNOWN', dispatch_state='UNKNOWN', execution_state='UNKNOWN',
                    last_error='phase5a control-plane UNKNOWN preservation fixture'
                WHERE id=:operation
            """), {"operation": unknown_operation_id})
            await connection.execute(text("""
                UPDATE agent_execution_holds SET state='UNKNOWN'
                WHERE operation_id=:operation AND quiescent_at IS NULL
            """), {"operation": unknown_operation_id})
        unknown_attempt_leases = await original_prepare(
            factory, runtime, actor, hekate_config, critic_config, worker_name,
            archive_root=archive_root,
        )
        await finish_gated_worker(unknown_gate, complete=False)
        unknown_wait_state = await scenario_snapshot(str(unknown_task["task_id"]))
        if (
            unknown_attempt_leases or fake.count() != unknown_before_requests
            or unknown_wait_state["task"]["status"] != "WAITING"
            or unknown_wait_state["workflow"]["stage"] != "CRITIC_READY"
            or unknown_wait_state["response"] is not None
            or unknown_wait_state["workflow"]["critic_state"] != "READY"
            or unknown_wait_state["reservations"][0]["status"] != "RESERVED"
        ):
            raise AssertionError(f"UNKNOWN review was converted into a permanent failure: {unknown_wait_state}")
        unknown_recovered = asyncio.Event()

        async def observe_unknown_after_restart(*args, **kwargs):
            result = await original_prepare(*args, **kwargs)
            current = await _workflow_state(engine, str(unknown_task["task_id"]))
            if current.get("stage") == "CRITIC_READY":
                unknown_recovered.set()
            return result

        worker_service.prepare_critic_workflow_steps = observe_unknown_after_restart
        unknown_restart_stop, unknown_restart_task = await _start_worker(container)
        worker_runs.append((unknown_restart_stop, unknown_restart_task))
        await asyncio.wait_for(unknown_recovered.wait(), 60)
        await _stop_worker(unknown_restart_stop, unknown_restart_task)
        worker_runs.remove((unknown_restart_stop, unknown_restart_task))
        worker_service.prepare_critic_workflow_steps = original_prepare
        async with engine.connect() as connection:
            unknown_db_state = (await connection.execute(text("""
                SELECT o.state, o.execution_state, h.state AS hold_state, h.quiescent_at,
                       (SELECT count(*) FROM provider_calls p WHERE p.operation_id=o.id) AS provider_call_rows
                FROM operations o JOIN agent_execution_holds h ON h.operation_id=o.id
                WHERE o.id=:operation
            """), {"operation": unknown_operation_id})).mappings().one()
        unknown_final = await scenario_snapshot(str(unknown_task["task_id"]))
        if (
            fake.count() != unknown_before_requests
            or unknown_db_state["state"] != "UNKNOWN"
            or unknown_db_state["execution_state"] != "UNKNOWN"
            or unknown_db_state["hold_state"] != "UNKNOWN"
            or unknown_db_state["quiescent_at"] is not None
            or int(unknown_db_state["provider_call_rows"]) != 0
            or unknown_final["task"]["status"] != "WAITING"
            or unknown_final["workflow"]["critic_state"] != "READY"
        ):
            raise AssertionError("worker restart bypassed or erased an unresolved UNKNOWN execution")
        failure_scenarios["unknown_preserved_across_restart"] = {
            "passed": True, "task_id": unknown_task["task_id"],
            "review_operation_id": unknown_operation_id,
            "control_plane_state": dict(unknown_db_state), "final": unknown_final,
            "fixture_is_synthetic_unresolved_state": True,
            "provider_call_rows_for_fixture": int(unknown_db_state["provider_call_rows"]),
            "provider_requests_during_wait_and_restart": 0,
            "held_budget_preserved": True, "critic_not_deleted": True,
        }

        async def stop_dynamic_step_for_expired_evidence(
            label: str, slot: str, requests_before: int, *, inject_rollback: bool = False,
        ) -> dict[str, object]:
            selected_task = await submit_scenario_task(label, marker="PHASE5B_REVIEW_CHAIN")
            provider_before = fake.count()
            gate = await gate_dynamic_step(selected_task, slot)
            requests_at_gate = fake.count() - provider_before
            expected = requests_before
            if requests_at_gate != expected:
                raise AssertionError(f"{slot} was not reached after the expected parent calls: {requests_at_gate}")
            expired = await expire_scenario_evidence(selected_task)
            rollback_state = None
            if inject_rollback:
                original_audit = PostgresDeliveryRepository.append_audit
                fired = {"value": False}

                async def fail_dynamic_stop_audit(repository, event):
                    await original_audit(repository, event)
                    if (
                        event.get("event_kind") == "deliberation.step_stopped"
                        and event.get("task_id") == selected_task["task_id"]
                        and not fired["value"]
                    ):
                        fired["value"] = True
                        raise RuntimeError("injected failure after dynamic stop effects, before commit")

                PostgresDeliveryRepository.append_audit = fail_dynamic_stop_audit
                try:
                    try:
                        await original_dynamic_prepare(
                            factory, runtime, actor, hekate_config, critic_config,
                            configured_deliberation(settings), worker_name,
                            archive_root=archive_root,
                        )
                    except RuntimeError as error:
                        if "injected failure" not in str(error):
                            raise
                    else:
                        raise AssertionError("dynamic stop transaction failure injection did not fire")
                finally:
                    PostgresDeliveryRepository.append_audit = original_audit
                rollback_state = await scenario_snapshot(str(selected_task["task_id"]))
                async with engine.connect() as connection:
                    step_after_rollback = (await connection.execute(text("""
                        SELECT state, stop_reason FROM deliberation_steps
                        WHERE task_id=:task AND step_slot=:slot
                    """), {"task": selected_task["task_id"], "slot": slot})).mappings().one()
                if (
                    not fired["value"] or rollback_state["task"]["status"] != "WAITING"
                    or rollback_state["response"] is not None
                    or rollback_state["workflow"]["delete_operation_id"] is not None
                    or rollback_state["workflow"]["critic_state"] != "READY"
                    or step_after_rollback["state"] != "READY"
                    or step_after_rollback["stop_reason"] is not None
                    or rollback_state["critic_delete_outbox_rows"] != 0
                ):
                    raise AssertionError(f"dynamic failure convergence left partial effects: {rollback_state}")

            terminal = await resume_dynamic_to_terminal(selected_task)
            final = await assert_policy_failure(selected_task, "evidence_unavailable")
            if fake.count() != provider_before + expected:
                raise AssertionError("expired Evidence reached a provider after dynamic-step rejection")
            before_replay = await scenario_snapshot(str(selected_task["task_id"]))
            replay = await original_dynamic_prepare(
                factory, runtime, actor, hekate_config, critic_config,
                configured_deliberation(settings), worker_name,
                archive_root=archive_root,
            )
            after_replay = await scenario_snapshot(str(selected_task["task_id"]))
            if replay or before_replay != after_replay:
                raise AssertionError("dynamic stop replay created new response, reservation, or retirement effects")
            return {
                "passed": True, "task_id": selected_task["task_id"], "step_slot": slot,
                "provider_requests_before_step": expected, "provider_requests_after_stop": fake.count() - provider_before,
                "evidence_expiry": expired, "terminal_state": terminal, "final_snapshot": final,
                "atomic_failure_rollback": rollback_state,
                "replay_effects_unchanged": before_replay == after_replay,
                "unused_holds_released_and_critic_deleted": all(
                    item["status"] in {"RELEASED", "SETTLED"} for item in final["reservations"]
                ) and final["workflow"]["critic_state"] == "DELETED",
            }

        failure_scenarios["evidence_expired_before_second_review"] = await stop_dynamic_step_for_expired_evidence(
            "evidence-expired-before-review-two", "critic_review_2", 3, inject_rollback=True,
        )
        failure_scenarios["evidence_expired_before_second_synthesis"] = await stop_dynamic_step_for_expired_evidence(
            "evidence-expired-before-synthesis-two", "synthesis_2", 4,
        )

        # A worker with the old actor epoch must still record the durable failure
        # and retire its scoped Critic after a dynamic step was already approved.
        authorization_task = await submit_scenario_task(
            "authorization-epoch-changed-dynamic", marker="PHASE5B_REVIEW_CHAIN",
        )
        authorization_provider_before = fake.count()
        authorization_gate = await gate_dynamic_step(authorization_task, "synthesis_2")
        if fake.count() - authorization_provider_before != 4:
            raise AssertionError("authorization scenario did not reach synthesis two")
        async with engine.begin() as connection:
            await connection.execute(text(
                "UPDATE authorization_scopes SET authz_epoch=authz_epoch + 1 WHERE id=:scope"
            ), {"scope": scope_text})
        authorization_terminal = await resume_dynamic_to_terminal(authorization_task)
        authorization_final = await assert_policy_failure(authorization_task, "authorization_changed")
        if fake.count() != authorization_provider_before + 4:
            raise AssertionError("authorization change permitted a later provider request")
        failure_scenarios["authorization_epoch_changed_at_synthesis_two"] = {
            "passed": True, "task_id": authorization_task["task_id"],
            "step_slot": authorization_gate["slot"], "worker_actor_epoch": actor.authz_epoch,
            "stored_authz_epoch": actor.authz_epoch + 1,
            "provider_requests_after_change": fake.count() - authorization_provider_before - 4,
            "terminal_state": authorization_terminal, "final": authorization_final,
            "stale_actor_did_not_block_policy_response_or_critic_cleanup": True,
        }

        # Refresh the trusted CLI/worker identity after the preceding scenario
        # advanced the scope epoch. The old actor remains stale for that
        # workflow's failure bookkeeping; subsequent fixtures use the current
        # configured actor.
        identity_path = config_dir / "local.yaml"
        identity_value = yaml.safe_load(identity_path.read_text(encoding="utf-8"))
        identity_value["identity"]["authz_epoch"] = actor.authz_epoch + 1
        identity_path.write_text(yaml.safe_dump(identity_value), encoding="utf-8")
        settings = load_settings(env, config_dir)
        actor = configured_local_actor(settings)
        container = Container(settings=settings, runtime=runtime, uow_factory=factory, database=engine)

        # Drain safe legacy rows first so these batch fixtures isolate candidate
        # selection and do not depend on earlier scenarios' ordering.
        from hekate.application.lifecycle import maintain_deliberation_steps
        from hekate.infrastructure.postgres.deliberation_repository import PostgresDeliberationRepository

        await maintain_deliberation_steps(factory, limit=1_000)

        async def maintenance_candidates(limit: int) -> list[dict[str, object]]:
            async with factory() as uow:
                rows = [dict(row) for row in await uow.deliberation.list_maintenance_candidates(limit)]
                await uow.commit()
            return rows

        async def stop_maintenance_fixture_task(task_id_value: str) -> None:
            """Set up a terminal Task snapshot without running cleanup early."""
            async with engine.begin() as connection:
                result = await connection.execute(text("""
                    UPDATE tasks SET status='CANCELLED',outcome='CANCELLED',
                        stop_reason='USER_CANCELLED',cancel_requested_at=now()
                    WHERE id=:task AND status='WAITING'
                """), {"task": task_id_value})
                if result.rowcount != 1:
                    raise AssertionError(f"maintenance fixture Task was not WAITING: {task_id_value}")

        async def insert_completed_selection_fixtures(
            template: dict[str, object], count: int,
        ) -> list[str]:
            now = datetime.now(UTC)
            task_rows: list[dict[str, object]] = []
            parent_operation_rows: list[dict[str, object]] = []
            operation_rows: list[dict[str, object]] = []
            parent_attempt_rows: list[dict[str, object]] = []
            parent_conclusion_rows: list[dict[str, object]] = []
            reservation_rows: list[dict[str, object]] = []
            step_rows: list[dict[str, object]] = []
            task_ids: list[str] = []
            for index in range(count):
                base = f"{run_id}:maintenance-complete:{index}"
                task_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{base}:task"))
                parent_operation_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{base}:parent-operation"))
                parent_attempt_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{base}:parent-attempt"))
                parent_conclusion_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{base}:parent-conclusion"))
                operation_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{base}:operation"))
                reservation_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{base}:reservation"))
                step_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{base}:step"))
                task_ids.append(task_id)
                task_rows.append({"task": task_id, "scope": scope_text, "deadline": now - timedelta(days=2)})
                parent_operation_rows.append({
                    "operation": parent_operation_id, "scope": scope_text, "task": task_id,
                    "request_hash": hashlib.sha256(f"{base}:parent-request".encode()).hexdigest(),
                })
                operation_rows.append({
                    "operation": operation_id, "scope": scope_text, "task": task_id,
                    "request_hash": hashlib.sha256(f"{base}:request".encode()).hexdigest(),
                })
                parent_attempt_rows.append({
                    "attempt": parent_attempt_id, "task": task_id,
                    "registry": str(template["registry_id"]), "operation": parent_operation_id,
                    "deadline": now - timedelta(days=1),
                })
                parent_conclusion_rows.append({
                    "conclusion": parent_conclusion_id, "task": task_id,
                    "attempt": parent_attempt_id, "operation": parent_operation_id,
                    "registry": str(template["registry_id"]),
                    "payload_hash": hashlib.sha256(f"{base}:parent-payload".encode()).hexdigest(),
                })
                reservation_rows.append({"reservation": reservation_id, "operation": operation_id})
                step_rows.append({
                    "id": step_id, "task": task_id, "scope": scope_text,
                    "parent_attempt": parent_attempt_id,
                    "parent_operation": parent_operation_id,
                    "parent_conclusion": parent_conclusion_id,
                    "proposal_hash": hashlib.sha256(f"{base}:proposal".encode()).hexdigest(),
                    "request_hash": hashlib.sha256(f"{base}:step".encode()).hexdigest(),
                    "attempt": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{base}:attempt")),
                    "operation": operation_id, "reservation": reservation_id,
                    "registry": str(template["registry_id"]), "completed_at": now - timedelta(days=1),
                })
            async with engine.begin() as connection:
                await connection.execute(text("""
                    INSERT INTO tasks(id,owner_scope,question,input_revision,constraints_hash,status,deadline,
                                      outcome,stop_reason,cancel_requested_at)
                    VALUES (:task,:scope,'completed maintenance selection fixture',1,repeat('a',64),
                            'CANCELLED',:deadline,'CANCELLED','USER_CANCELLED',now())
                """), task_rows)
                await connection.execute(text("""
                    INSERT INTO operations(id,owner_scope,task_id,kind,request_hash,state,dispatch_state,
                                           execution_state,binding,envelope,observation)
                    VALUES (:operation,:scope,:task,'hekate.planning',:request_hash,'COMPLETED','QUIESCENT',
                            'QUIESCENT','{}'::jsonb,'{}'::jsonb,'{}'::jsonb)
                """), parent_operation_rows)
                await connection.execute(text("""
                    INSERT INTO operations(id,owner_scope,task_id,kind,request_hash,state,dispatch_state,
                                           execution_state,binding,envelope,observation)
                    VALUES (:operation,:scope,:task,'hekate.reasoning',:request_hash,'FAILED','QUIESCENT',
                            'QUIESCENT','{}'::jsonb,'{}'::jsonb,'{}'::jsonb)
                """), operation_rows)
                await connection.execute(text("""
                    INSERT INTO attempts(id,task_id,kind,input_revision,agent_registry_id,status,operation_id,deadline)
                    VALUES (:attempt,:task,'planning',1,:registry,'SUCCEEDED',:operation,:deadline)
                """), parent_attempt_rows)
                await connection.execute(text("""
                    INSERT INTO conclusions(id,task_id,attempt_id,operation_id,registry_id,input_revision,
                                            payload_hash,capsule,validation_status,eligible)
                    VALUES (:conclusion,:task,:attempt,:operation,:registry,1,:payload_hash,
                            '{}'::jsonb,'VALID',true)
                """), parent_conclusion_rows)
                await connection.execute(text("""
                    INSERT INTO budget_reservations(id,operation_id,purpose,amount,status,pricing_version,
                                                    system_period_id,settled_at)
                    VALUES (:reservation,:operation,'operation_envelope',0,'RELEASED',
                            'maintenance-fixture','maintenance-fixture',now())
                """), reservation_rows)
                await connection.execute(text("""
                    INSERT INTO deliberation_steps(
                        id,task_id,owner_scope,input_revision,step_order,step_kind,step_slot,parent_attempt_id,
                        parent_operation_id,parent_conclusion_id,proposal,proposal_hash,request_hash,state,attempt_id,
                        operation_id,reservation_id,registry_id,profile,context,stop_reason,maintenance_completed_at
                    ) VALUES (
                        :id,:task,:scope,1,1,'hekate_reasoning','hekate_reasoning_1',:parent_attempt,
                        :parent_operation,:parent_conclusion,'{}'::jsonb,:proposal_hash,:request_hash,'STOPPED',:attempt,
                        :operation,:reservation,:registry,'{}'::jsonb,'{}'::jsonb,'task_cancelled',:completed_at
                    )
                """), step_rows)
            return task_ids

        # More than one default batch of previously completed rows must not
        # occupy candidate slots ahead of a newly cancelled, real Task step.
        overflow_task, _ = await create_ready_solo_continuation("maintenance-overflow-target")
        overflow_task_id = str(overflow_task["task_id"])
        overflow_step = await selected_ready_step(overflow_task_id)
        overflow_provider_before_cancel = fake.count()
        await cancel(factory, actor, TaskId(overflow_task_id), StopReason.USER_CANCELLED)
        completed_fixture_tasks = await insert_completed_selection_fixtures(overflow_step, 101)
        overflow_selected = await maintenance_candidates(100)
        overflow_selected_ids = [str(row["id"]) for row in overflow_selected]
        if (
            overflow_step["id"] not in overflow_selected_ids
            or any(str(row["task_id"]) in set(completed_fixture_tasks) for row in overflow_selected)
        ):
            raise AssertionError("completed maintenance rows crowded the bounded candidate batch")

        overflow_before = await deliberation_cleanup_snapshot(overflow_task_id)
        injected_marker_failure = {"fired": False}
        original_mark_complete = PostgresDeliberationRepository.mark_maintenance_complete

        async def fail_before_completion_marker(self, step):
            if str(step["task_id"]) == overflow_task_id and not injected_marker_failure["fired"]:
                injected_marker_failure["fired"] = True
                raise RuntimeError("injected failure before maintenance completion marker")
            return await original_mark_complete(self, step)

        PostgresDeliberationRepository.mark_maintenance_complete = fail_before_completion_marker
        try:
            try:
                await maintain_deliberation_steps(factory, limit=100)
            except RuntimeError as error:
                if "injected failure before maintenance completion marker" not in str(error):
                    raise
            else:
                raise AssertionError("maintenance completion-marker failure was not injected")
        finally:
            PostgresDeliberationRepository.mark_maintenance_complete = original_mark_complete
        overflow_after_injected_failure = await deliberation_cleanup_snapshot(overflow_task_id)
        if not injected_marker_failure["fired"] or overflow_after_injected_failure != overflow_before:
            raise AssertionError("maintenance completion-marker failure committed partial effects")

        original_list_candidates = PostgresDeliberationRepository.list_maintenance_candidates
        candidate_barrier = asyncio.Event()
        candidate_barrier_lock = asyncio.Lock()
        candidate_barrier_arrivals = 0
        concurrent_candidate_lists: list[list[str]] = []

        async def synchronize_candidate_snapshots(self, batch_limit=100):
            nonlocal candidate_barrier_arrivals
            rows = await original_list_candidates(self, batch_limit)
            concurrent_candidate_lists.append([str(row["id"]) for row in rows])
            async with candidate_barrier_lock:
                candidate_barrier_arrivals += 1
                if candidate_barrier_arrivals == 2:
                    candidate_barrier.set()
            await asyncio.wait_for(candidate_barrier.wait(), timeout=20)
            return rows

        PostgresDeliberationRepository.list_maintenance_candidates = synchronize_candidate_snapshots
        try:
            concurrent_completion_counts = await asyncio.gather(
                maintain_deliberation_steps(factory, limit=100),
                maintain_deliberation_steps(factory, limit=100),
            )
        finally:
            PostgresDeliberationRepository.list_maintenance_candidates = original_list_candidates
        overflow_after_race = await deliberation_cleanup_snapshot(overflow_task_id)
        overflow_replay_count = await maintain_deliberation_steps(factory, limit=100)
        overflow_after_replay = await deliberation_cleanup_snapshot(overflow_task_id)
        if (
            len(concurrent_candidate_lists) != 2
            or any(overflow_step["id"] not in selected for selected in concurrent_candidate_lists)
            or sorted(concurrent_completion_counts) != [0, 1]
            or overflow_replay_count != 0
            or overflow_after_replay != overflow_after_race
            or overflow_after_race["steps"][0]["maintenance_completed_at"] is None
            or overflow_after_race["steps"][0]["reservation_state"] != "RELEASED"
            or Decimal(overflow_after_race["steps"][0]["reservation_held"]) != 0
            or fake.count() != overflow_provider_before_cancel
        ):
            raise AssertionError("maintenance race, replay, or accounting boundary failed")
        maintenance_batch_scenarios = {
            "completed_backlog_over_default_limit": {
                "passed": True, "batch_limit": 100, "completed_fixture_count": len(completed_fixture_tasks),
                "fixture_kind": "synthetic database rows with committed maintenance markers; no execution is claimed",
                "task_id": overflow_task_id, "step_id": str(overflow_step["id"]),
                "candidate_step_ids_before_processing": overflow_selected_ids,
                "marker_failure_rolled_back": overflow_after_injected_failure == overflow_before,
                "concurrent_candidate_snapshots": concurrent_candidate_lists,
                "concurrent_completion_counts": concurrent_completion_counts,
                "post_replay_completion_count": overflow_replay_count,
                "provider_requests_after_cancel_gate": fake.count() - overflow_provider_before_cancel,
                "after_completion": overflow_after_race,
            },
        }

        # A small batch must move past an old UNKNOWN, finish a previously
        # STOPPED-but-unreleased row, and reach the next deferred step.
        batch_provider_before_creation = fake.count()
        unknown_batch_task, _ = await create_ready_solo_continuation(
            "maintenance-batch-unknown", pause_maintenance=True,
        )
        unknown_batch_step = await selected_ready_step(str(unknown_batch_task["task_id"]))
        await stop_maintenance_fixture_task(str(unknown_batch_task["task_id"]))
        partial_batch_task, _ = await create_ready_solo_continuation(
            "maintenance-batch-partial", pause_maintenance=True,
        )
        partial_batch_step = await selected_ready_step(str(partial_batch_task["task_id"]))
        await stop_maintenance_fixture_task(str(partial_batch_task["task_id"]))
        safe_batch_task, _ = await create_ready_solo_continuation(
            "maintenance-batch-safe", pause_maintenance=True,
        )
        safe_batch_step = await selected_ready_step(str(safe_batch_task["task_id"]))
        await stop_maintenance_fixture_task(str(safe_batch_task["task_id"]))
        batch_task_ids = [
            str(unknown_batch_task["task_id"]),
            str(partial_batch_task["task_id"]),
            str(safe_batch_task["task_id"]),
        ]
        batch_steps = [unknown_batch_step, partial_batch_step, safe_batch_step]
        batch_provider_after_cancel_gate = fake.count()
        batch_now = datetime.now(UTC)
        async with engine.begin() as connection:
            for task_id, deadline in zip(
                batch_task_ids,
                (
                    batch_now - timedelta(hours=4), batch_now - timedelta(hours=2),
                    batch_now - timedelta(hours=1),
                ),
                strict=True,
            ):
                await connection.execute(text("UPDATE tasks SET deadline=:deadline WHERE id=:task"), {
                    "deadline": deadline, "task": task_id,
                })
            await connection.execute(text("""
                UPDATE operations SET task_id=:task,state='UNKNOWN',dispatch_state='UNKNOWN',
                    execution_state='UNKNOWN',last_error='synthetic batch uncertainty fixture'
                WHERE id=:operation AND state='CLAIMED' AND task_id IS NULL
            """), {"task": batch_task_ids[0], "operation": batch_steps[0]["operation_id"]})
            await connection.execute(text("""
                INSERT INTO agent_execution_holds(id,registry_id,operation_id,state,reason)
                VALUES (:id,:registry,:operation,'UNKNOWN','synthetic batch uncertainty fixture')
            """), {
                "id": f"p5b-maint-batch-unknown-{run_id}",
                "registry": batch_steps[0]["registry_id"],
                "operation": batch_steps[0]["operation_id"],
            })
            await connection.execute(text("""
                UPDATE deliberation_steps SET state='STOPPED',stop_reason='task_cancelled'
                WHERE id=:step AND state='READY'
            """), {"step": batch_steps[1]["id"]})
            await connection.execute(text("""
                UPDATE operations SET task_id=:task,state='FAILED',dispatch_state='QUIESCENT',
                    execution_state='QUIESCENT',last_error='synthetic partially cleaned fixture'
                WHERE id=:operation AND state='CLAIMED' AND task_id IS NULL
            """), {"task": batch_task_ids[1], "operation": batch_steps[1]["operation_id"]})

        batch_limit = 1
        tick_one_selected = await maintenance_candidates(batch_limit)
        tick_one_ids = [str(row["id"]) for row in tick_one_selected]
        if tick_one_ids != [str(batch_steps[0]["id"])]:
            raise AssertionError(f"small maintenance batch selected an unexpected prefix: {tick_one_ids}")
        batch_unknown_before = await deliberation_cleanup_snapshot(batch_task_ids[0])
        batch_partial_before = await deliberation_cleanup_snapshot(batch_task_ids[1])
        tick_one_completed = await maintain_deliberation_steps(factory, limit=batch_limit)
        batch_unknown_after_tick_one = await deliberation_cleanup_snapshot(batch_task_ids[0])
        batch_partial_after_tick_one = await deliberation_cleanup_snapshot(batch_task_ids[1])
        # Synthetic clock advancement makes old UNKNOWN retries due while
        # never-deferred cleanup rows still exist. Those fresh rows must run
        # first so an expired retry cannot take the batch front again.
        async with engine.begin() as connection:
            await connection.execute(text("""
                UPDATE deliberation_steps SET maintenance_retry_after=now()-interval '1 second'
                WHERE id=:step AND maintenance_completed_at IS NULL
            """), {"step": batch_steps[0]["id"]})
        tick_two_selected = await maintenance_candidates(batch_limit)
        tick_two_ids = [str(row["id"]) for row in tick_two_selected]
        if tick_two_ids != [str(batch_steps[1]["id"])]:
            raise AssertionError(f"deferred candidates blocked the next safe batch: {tick_two_ids}")
        tick_two_completed = await maintain_deliberation_steps(factory, limit=batch_limit)
        batch_partial_after_tick_two = await deliberation_cleanup_snapshot(batch_task_ids[1])
        tick_three_selected = await maintenance_candidates(batch_limit)
        tick_three_ids = [str(row["id"]) for row in tick_three_selected]
        if tick_three_ids != [str(batch_steps[2]["id"])]:
            raise AssertionError(f"the later safe target was not reached: {tick_three_ids}")
        tick_three_completed = await maintain_deliberation_steps(factory, limit=batch_limit)
        batch_safe_after_tick_three = await deliberation_cleanup_snapshot(batch_task_ids[2])
        restarted_factory = create_uow_factory(engine)
        restart_completed = await maintain_deliberation_steps(restarted_factory, limit=batch_limit)
        batch_unknown_after_restart = await deliberation_cleanup_snapshot(batch_task_ids[0])
        batch_safe_after_restart = await deliberation_cleanup_snapshot(batch_task_ids[2])

        # Advance the synthetic retry clock and add a synthetic durable terminal
        # observation; this tests re-selection, not a real provider interruption.
        async with engine.begin() as connection:
            await connection.execute(text("""
                UPDATE operations SET state='FAILED',dispatch_state='QUIESCENT',execution_state='QUIESCENT',
                    last_error='synthetic terminal observation for retry test'
                WHERE id=:operation AND state='UNKNOWN'
            """), {"operation": batch_steps[0]["operation_id"]})
            await connection.execute(text("""
                UPDATE agent_execution_holds SET state='QUIESCENT',quiescent_at=now()
                WHERE operation_id=:operation AND state='UNKNOWN'
            """), {"operation": batch_steps[0]["operation_id"]})
            await connection.execute(text("""
                UPDATE deliberation_steps SET maintenance_retry_after=now()-interval '1 second'
                WHERE id=:step AND maintenance_completed_at IS NULL
            """), {"step": batch_steps[0]["id"]})
        retry_candidate_ids = [str(row["id"]) for row in await maintenance_candidates(100)]
        retry_completed = await maintain_deliberation_steps(restarted_factory, limit=100)
        batch_unknown_after_terminal = await deliberation_cleanup_snapshot(batch_task_ids[0])
        if (
            tick_one_completed != 0 or tick_two_completed != 1 or tick_three_completed != 1
            or batch_unknown_before["steps"][0]["operation_state"] != "UNKNOWN"
            or batch_unknown_after_tick_one["steps"][0]["operation_state"] != "UNKNOWN"
            or batch_unknown_after_tick_one["steps"][0]["maintenance_retry_after"] is None
            or batch_unknown_after_tick_one["steps"][0]["execution_state"] != "UNKNOWN"
            or len(batch_unknown_after_tick_one["execution_holds"]) != 1
            or batch_unknown_after_tick_one["execution_holds"][0]["state"] != "UNKNOWN"
            or batch_unknown_after_tick_one["execution_holds"][0]["quiescent_at"] is not None
            or batch_unknown_after_tick_one["steps"][0]["reservation_state"] != "RESERVED"
            or Decimal(batch_unknown_after_tick_one["steps"][0]["reservation_held"]) <= 0
            or batch_partial_before["steps"][0]["step_state"] != "STOPPED"
            or batch_partial_after_tick_one["steps"][0]["maintenance_completed_at"] is not None
            or batch_partial_after_tick_one["steps"][0]["reservation_state"] != "RESERVED"
            or batch_partial_after_tick_two["steps"][0]["maintenance_completed_at"] is None
            or batch_partial_after_tick_two["steps"][0]["reservation_state"] != "RELEASED"
            or batch_safe_after_tick_three["steps"][0]["maintenance_completed_at"] is None
            or restart_completed != 0 or batch_safe_after_restart != batch_safe_after_tick_three
            or batch_unknown_after_restart["steps"][0]["operation_state"] != "UNKNOWN"
            or batch_unknown_after_restart["steps"][0]["execution_state"] != "UNKNOWN"
            or len(batch_unknown_after_restart["execution_holds"]) != 1
            or batch_unknown_after_restart["execution_holds"][0]["state"] != "UNKNOWN"
            or batch_unknown_after_restart["execution_holds"][0]["quiescent_at"] is not None
            or batch_unknown_after_restart["steps"][0]["reservation_state"] != "RESERVED"
            or str(batch_steps[0]["id"]) not in retry_candidate_ids or retry_completed != 1
            or batch_unknown_after_terminal["steps"][0]["maintenance_completed_at"] is None
            or batch_unknown_after_terminal["steps"][0]["reservation_state"] != "RELEASED"
            or fake.count() != batch_provider_after_cancel_gate
        ):
            raise AssertionError("bounded maintenance did not defer, progress, and recheck safely")
        maintenance_batch_scenarios["unknown_partial_and_later_target"] = {
            "passed": True, "batch_limit": batch_limit,
            "fixture_counts": {"unknown": 1, "partial_stopped": 1, "safe_later": 1, "completed_markers": 101},
            "terminal_task_states": "synthetic direct database fixture after real Task planning and READY-step creation",
            "provider_requests_to_create_three_ready_steps": batch_provider_after_cancel_gate - batch_provider_before_creation,
            "synthetic_terminal_observation": True,
            "synthetic_retry_deadline_advance_while_untried_rows_exist": True,
            "real_provider_interruption_simulated": False,
            "tick_one_selected": tick_one_ids,
            "tick_one_completed_count": tick_one_completed,
            "unknown_after_tick_one": batch_unknown_after_tick_one,
            "partial_before": batch_partial_before, "partial_after_tick_one": batch_partial_after_tick_one,
            "tick_two_selected": tick_two_ids, "tick_two_completed_count": tick_two_completed,
            "partial_after_tick_two": batch_partial_after_tick_two,
            "tick_three_selected": tick_three_ids, "tick_three_completed_count": tick_three_completed,
            "safe_after_tick_three": batch_safe_after_tick_three,
            "restart_completed_count": restart_completed,
            "unknown_after_restart": batch_unknown_after_restart,
            "safe_after_restart": batch_safe_after_restart,
            "retry_candidate_ids_after_terminal_observation": retry_candidate_ids,
            "unknown_after_terminal_observation": batch_unknown_after_terminal,
            "retry_completed_count": retry_completed,
            "provider_requests_after_cancel_gate": fake.count() - batch_provider_after_cancel_gate,
        }
        report["standalone_maintenance_batch_progress"] = maintenance_batch_scenarios
        if not all(item["passed"] for item in maintenance_batch_scenarios.values()):
            raise AssertionError("standalone maintenance batch progress regression failed")

        # Run the standalone UNKNOWN fixture last because its preserved active
        # execution hold intentionally blocks this scope's persistent HEKATE.
        # This is a synthetic control-plane UNKNOWN, not an induced provider
        # interruption. It proves worker maintenance will not seal or release
        # an execution whose state has no quiescence evidence.
        unknown_standalone, _ = await create_ready_solo_continuation("standalone-unknown-preserved")
        unknown_standalone_step = await selected_ready_step(str(unknown_standalone["task_id"]))
        unknown_standalone_provider_before = fake.count()
        synthetic_hold_id = f"p5b-unknown-continuation-{run_id}"
        async with engine.begin() as connection:
            await connection.execute(text("""
                UPDATE operations SET task_id=:task,state='UNKNOWN',dispatch_state='UNKNOWN',
                    execution_state='UNKNOWN',last_error='synthetic control-plane uncertainty fixture'
                WHERE id=:operation AND state='CLAIMED' AND task_id IS NULL
            """), {"task": unknown_standalone["task_id"], "operation": unknown_standalone_step["operation_id"]})
            await connection.execute(text("""
                INSERT INTO agent_execution_holds(id,registry_id,operation_id,state,reason)
                VALUES (:id,:registry,:operation,'UNKNOWN','synthetic control-plane uncertainty fixture')
            """), {
                "id": synthetic_hold_id, "registry": unknown_standalone_step["registry_id"],
                "operation": unknown_standalone_step["operation_id"],
            })
        unknown_cancel = await cancel(
            factory, actor, TaskId(str(unknown_standalone["task_id"])), StopReason.USER_CANCELLED,
        )
        unknown_clean, unknown_ticks = await run_worker_until_deliberation_state(
            str(unknown_standalone["task_id"]),
            lambda snapshot: (
                snapshot["task"]["status"] == "STOPPING"
                and snapshot["steps"][0]["step_state"] == "READY"
                and snapshot["steps"][0]["operation_state"] == "UNKNOWN"
                and snapshot["steps"][0]["execution_state"] == "UNKNOWN"
                and snapshot["steps"][0]["reservation_state"] == "RESERVED"
                and Decimal(snapshot["steps"][0]["reservation_held"]) > 0
                and len(snapshot["execution_holds"]) == 1
                and snapshot["execution_holds"][0]["state"] == "UNKNOWN"
            ), "UNKNOWN standalone hold preservation", minimum_ticks=2,
        )
        unknown_replay, unknown_replay_ticks = await run_worker_until_deliberation_state(
            str(unknown_standalone["task_id"]),
            lambda snapshot: (
                snapshot["task"]["status"] == "STOPPING"
                and snapshot["steps"][0]["step_state"] == "READY"
                and snapshot["steps"][0]["operation_state"] == "UNKNOWN"
                and snapshot["steps"][0]["reservation_state"] == "RESERVED"
                and snapshot["execution_holds"][0]["state"] == "UNKNOWN"
            ), "UNKNOWN standalone restart preservation",
        )
        if (
            unknown_cancel["state"] != "STOPPING" or unknown_clean != unknown_replay
            or fake.count() != unknown_standalone_provider_before
            or unknown_clean["responses"] != 0 or unknown_clean["position_versions"] != 0
        ):
            raise AssertionError(f"UNKNOWN standalone execution was normalized or bypassed: {unknown_clean}")
        failure_scenarios["standalone_unknown_preserved"] = {
            "passed": True, "task_id": unknown_standalone["task_id"],
            "synthetic_control_plane_fixture": True,
            "provider_interruption_simulated": False,
            "worker_maintenance_ticks": unknown_ticks, "restart_ticks": unknown_replay_ticks,
            "after_maintenance": unknown_clean,
            "provider_requests_after_ready_gate": fake.count() - unknown_standalone_provider_before,
        }


        report["critic_workflow_stop_and_wait_regressions"] = failure_scenarios
        if not all(item.get("passed") for item in failure_scenarios.values()):
            raise AssertionError("a Critic workflow stop/wait regression did not pass")
        report["fake_provider_requests"] = fake.count()
        report["provider_calls"] = {"real": 0, "fake": fake.count(), "compaction": 0}
        report["dispatch_safety"]["fake_provider_requests"] = fake.count()
        report["provider_request_scenarios"] = {
            "normal_planning_two_critic_reviews_one_continuation_two_syntheses": 6,
            "insufficient_budget_planning_only": budget_provider_requests,
            "temporary_busy_task_planning_critic_synthesis": 3,
            "permanent_rejection_and_stop_scenarios": {
                key: sum(item["task_id"] == value.get("task_id") for item in observations)
                if isinstance(value, dict) else 0
                for key, value in failure_scenarios.items()
            },
        }
        async with engine.connect() as connection:
            unresolved_operation_rows = (await connection.execute(text("""
                SELECT id, kind, state, execution_state, dispatch_state, task_id,
                       observation->>'deferred_task_id' AS deferred_task_id
                FROM operations
                WHERE state='UNKNOWN' OR execution_state='UNKNOWN' OR dispatch_state='UNKNOWN'
                ORDER BY id
            """))).mappings().all()
            pending_unadmitted_rows = (await connection.execute(text("""
                SELECT id, kind, state, execution_state, dispatch_state,
                       observation->>'deferred_task_id' AS deferred_task_id,
                       observation->>'deferred_stage_hash' AS deferred_stage_hash
                FROM operations WHERE state='CLAIMED' AND task_id IS NULL
                  AND observation ? 'deferred_task_id'
                ORDER BY id
            """))).mappings().all()
            pending_lifecycle_rows = (await connection.execute(text("""
                SELECT id, kind, state, execution_state, dispatch_state
                FROM operations WHERE state='CLAIMED' AND task_id IS NULL
                  AND kind IN ('agent.create','critic.create','critic.delete')
                ORDER BY id
            """))).mappings().all()
            unsettled_reservation_rows = (await connection.execute(text("""
                SELECT br.operation_id, o.kind, br.status, br.amount::text AS amount,
                       COALESCE(o.task_id, w.task_id, o.observation->>'deferred_task_id') AS task_id,
                       COALESCE(ra.held_amount, 0)::text AS held
                FROM budget_reservations br JOIN operations o ON o.id=br.operation_id
                LEFT JOIN critic_workflows w ON o.id IN (w.review_operation_id, w.synthesis_operation_id)
                LEFT JOIN LATERAL (
                    SELECT sum(held_amount) AS held_amount
                    FROM reservation_accounts WHERE reservation_id=br.id
                ) ra ON TRUE
                WHERE br.status IN ('RESERVED','PENDING_SETTLEMENT')
                ORDER BY br.operation_id
            """))).mappings().all()
        report["accounting"] = {
            "workflow_operation_statuses": lifecycle_states,
            "pending_or_unknown_operations": [dict(item) for item in unresolved_operation_rows],
            "pending_unadmitted_workflow_operations": [dict(item) for item in pending_unadmitted_rows],
            "pending_lifecycle_operations": [dict(item) for item in pending_lifecycle_rows],
            "unsettled_reservations": [dict(item) for item in unsettled_reservation_rows],
            "unknown_fixture_explanation": (
                "The remaining UNKNOWN review/hold is an explicit control-plane preservation fixture with zero provider-call rows; "
                "it was not normalized or given fabricated termination or usage evidence."
            ),
            "synthetic_fake_pricing_only": True,
        }
        report["preserved_prior_state"] = {
            "phase4_issued_allocated_fixture": {
                "source_artifact": "integration/runtime/artifacts/p4-20261003T165018Z-b67da98c.json",
                "previously_reported_state": "one ISSUED/ALLOCATED provider call without execution evidence",
                "current_probe_database_touched": False,
                "settlement_or_usage_synthesized": False,
            },
            "position_projection": {
                "state": "PENDING_UNSUPPORTED",
                "applied_watermark": 0,
                "changed_by_create_journal": False,
            },
            "unknown_fixture": {
                "state": "UNKNOWN with review and synthesis holds preserved",
                "real_provider_calls_for_fixture": 0,
                "changed_by_create_journal": False,
            },
            "production_dispatch": "blocked",
            "g7_g8": "deferred",
        }
        report["overall_status"] = "pass"
        report["execution"]["result"] = "passed"
        report["code_fingerprint_sha256"] = await p4._code_identity()
        return report
    except Exception as error:
        report["failure"] = {"type": type(error).__name__, "message": str(error)[:500], "traceback": traceback.format_exc()[-5000:]}
        report["provider_fixture_errors"] = response_errors if "response_errors" in locals() else []
        report["fake_provider_requests"] = fake.count() if fake is not None else 0
        report["provider_observations"] = observations if "observations" in locals() else []
        report["bridge_stderr_tail"] = bridge.stderr_text[-2_000:] if bridge is not None else ""
        report["execution"]["result"] = "failed"
        report["code_fingerprint_sha256"] = await p4._code_identity()
        return report
    finally:
        spawn_race_active = False
        release_spawn_race.set()
        continuation_race["active"] = False
        continuation_race["release_first"].set()
        PostgresDeliveryRepository.lock_operation = original_lock_operation
        PostgresKnowledgeRepository.lock_turn_result = original_lock_turn_result
        worker_service.maintain_critic_workflows = original_maintain
        worker_service.prepare_critic_workflow_steps = original_prepare
        worker_service.prepare_deliberation_steps = original_dynamic_prepare
        worker_service.maintain_deliberation_steps = original_maintain_deliberation
        if spawn_approval_tasks:
            await asyncio.gather(*spawn_approval_tasks, return_exceptions=True)
        if continuation_race_task is not None and not continuation_race_task.done():
            continuation_race_task.cancel()
            await asyncio.gather(continuation_race_task, return_exceptions=True)
        for stop, task in worker_runs:
            if not task.done():
                stop.set()
                await asyncio.gather(task, return_exceptions=True)
        if bridge is not None:
            await bridge.close()
        if fake is not None:
            fake.stop()
        if gateway_server is not None:
            gateway_server.should_exit = True
            await asyncio.gather(gateway_task, return_exceptions=True)
        if sandbox is not None:
            sandbox.stop()
            try:
                sandbox.clear_state()
            except Exception:
                pass
        if engine is not None:
            await engine.dispose()
        temp.cleanup()


async def _wait_for_operation(engine, task_id: str, kind: str, state: str, timeout: float) -> None:
    end = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < end:
        async with engine.connect() as connection:
            found = await connection.scalar(text("""
                SELECT EXISTS(
                    SELECT 1 FROM operations o
                    WHERE o.kind=:kind AND o.state=:state
                      AND (o.task_id=:task OR o.id IN (
                          SELECT create_operation_id FROM critic_workflows WHERE task_id=:task
                          UNION ALL
                          SELECT delete_operation_id FROM critic_workflows WHERE task_id=:task
                      ))
                )
            """), {"task": task_id, "kind": kind, "state": state})
        if found:
            return
        await asyncio.sleep(0.1)
    raise TimeoutError(f"operation {kind} did not reach {state}")


async def _operation_states(engine, task_id: str) -> list[dict[str, object]]:
    async with engine.connect() as connection:
        rows = (await connection.execute(text("""
            SELECT id, kind, state, execution_state FROM operations
            WHERE task_id=:task AND kind IN ('critic.create','critic.review','hekate.synthesis','critic.delete')
            ORDER BY created_at, id
        """), {"task": task_id})).mappings().all()
        return [dict(row) for row in rows]


async def _critic_output_boundary_checks(factory, task_id: str, hekate_registry_id: str) -> dict[str, bool]:
    async with factory() as uow:
        workflow = await uow.critic_workflows.get(TaskId(task_id))
        if workflow is None or workflow.critic_conclusion_id is None:
            raise AssertionError("durable Critic conclusion is required for output-boundary checks")
        operation = await uow.delivery.lock_operation(workflow.review_operation_id)
        conclusion = await uow.knowledge.get_conclusion(str(workflow.critic_conclusion_id))
        manifest_row = await uow.knowledge.get_context_manifest(workflow.review_operation_id)
        await uow.rollback()
    if conclusion is None or manifest_row is None:
        raise AssertionError("Critic operation binding, conclusion, or context manifest was not durable")
    trusted_binding = _restore_binding(operation)
    manifest = manifest_row["manifest"]
    base = {
        "schema_version": "1",
        "conclusion": conclusion["capsule"],
    }
    valid = parse_critic_turn_output(json.dumps(base, separators=(",", ":")).encode())
    results_app.validate_critic_turn_output(valid, trusted_binding, manifest)

    spoofed = json.loads(json.dumps(base))
    spoofed["conclusion"]["agent_id"] = hekate_registry_id
    spoof_rejected = False
    try:
        results_app.validate_critic_turn_output(
            parse_critic_turn_output(json.dumps(spoofed, separators=(",", ":")).encode()),
            trusted_binding, manifest,
        )
    except ValueError as error:
        spoof_rejected = str(error) == "conclusion_binding_mismatch"

    unprovided = json.loads(json.dumps(base))
    unprovided["conclusion"]["evidence_used"] = ["evidence-not-in-this-manifest"]
    evidence_rejected = False
    try:
        results_app.validate_critic_turn_output(
            parse_critic_turn_output(json.dumps(unprovided, separators=(",", ":")).encode()),
            trusted_binding, manifest,
        )
    except ValueError as error:
        evidence_rejected = str(error) == "evidence_not_provided"

    executable = json.loads(json.dumps(base))
    executable["proposal"] = {"action": "commit"}
    proposal_rejected = False
    try:
        parse_critic_turn_output(json.dumps(executable, separators=(",", ":")).encode())
    except ValueError:
        proposal_rejected = True
    return {
        "stored_critic_output_validates_against_trusted_binding": True,
        "critic_cannot_impersonate_persistent_hekate": spoof_rejected,
        "critic_cannot_claim_unprovided_evidence": evidence_rejected,
        "critic_output_schema_rejects_executable_proposal": proposal_rejected,
    }


async def _continuation_contract_boundary_checks(factory, task_id: str) -> dict[str, bool]:
    async with factory() as uow:
        workflow = await uow.critic_workflows.get(TaskId(task_id))
        if workflow is None:
            raise AssertionError("the planning operation is required for continuation boundary checks")
        operation = await uow.delivery.lock_operation(workflow.planning_operation_id)
        conclusion = await uow.knowledge.get_conclusion(str(workflow.planning_conclusion_id))
        manifest_row = await uow.knowledge.get_context_manifest(workflow.planning_operation_id)
        await uow.rollback()
    if conclusion is None or manifest_row is None:
        raise AssertionError("planning conclusion or context manifest is unavailable")
    binding = _restore_binding(operation)

    def output(*, next_action: str, issue: str = "A concrete bounded uncertainty."):
        return parse_hekate_turn_output(json.dumps({
            "schema_version": "1",
            "proposal": {
                "schema_version": "1", "action": "continue",
                "unresolved_issue": issue,
                "next_action": next_action,
                "expected_information_gain": "A bounded comparison can clarify the source context.",
                "decision_impact": "A scope change could qualify the Position.",
            },
            "conclusion": conclusion["capsule"],
        }, separators=(",", ":")).encode())

    rejected = {}
    for key, next_action, issue, expected in (
        ("unknown_next_action", "web_search", "A concrete uncertainty.", "unsupported_continue_action"),
        ("empty_purpose", "hekate_reasoning", "   ", "continuation_fields_required"),
    ):
        try:
            results_app.validate_turn_output(
                output(next_action=next_action, issue=issue), binding,
                OperationId(str(workflow.planning_operation_id)), manifest_row["manifest"],
                allow_spawn=False, allow_continue=True,
            )
        except ValueError as error:
            rejected[key] = str(error) == expected
        else:
            rejected[key] = False
    if not all(rejected.values()):
        raise AssertionError(f"continuation output allowlist or purpose validation failed: {rejected}")
    return {
        "unknown_next_action_rejected_by_control_plane": rejected["unknown_next_action"],
        "blank_purpose_rejected_by_control_plane": rejected["empty_purpose"],
        "allowed_next_actions": ["hekate_reasoning", "critic_review"],
    }


async def _counts(engine, task_id: str, scope: str) -> dict[str, int]:
    async with engine.connect() as connection:
        row = (await connection.execute(text("""
            SELECT (SELECT count(*) FROM critic_workflows WHERE task_id=:task) AS workflows,
                   (SELECT count(*) FROM agent_registry WHERE task_id=:task AND role='critic') AS critics,
                   (SELECT count(*) FROM budget_reservations WHERE operation_id IN (
                       SELECT id FROM operations WHERE task_id=:task
                       UNION SELECT review_operation_id FROM critic_workflows WHERE task_id=:task
                       UNION SELECT synthesis_operation_id FROM critic_workflows WHERE task_id=:task
                   )) AS reservations,
                   (SELECT count(*) FROM outbox WHERE operation_id IN (
                       SELECT id FROM operations WHERE task_id=:task
                       UNION SELECT create_operation_id FROM critic_workflows WHERE task_id=:task
                       UNION SELECT delete_operation_id FROM critic_workflows WHERE task_id=:task AND delete_operation_id IS NOT NULL
                   )) AS outbox,
                   (SELECT count(*) FROM position_versions WHERE task_id=:task) AS positions,
                   (SELECT count(*) FROM position_commit_receipts WHERE scope=:scope) AS receipts,
                   (SELECT count(*) FROM provider_calls p JOIN operations o ON o.id=p.operation_id WHERE o.task_id=:task) AS calls,
                   (SELECT count(*) FROM task_responses WHERE task_id=:task) AS responses,
                   (SELECT count(*) FROM dissent WHERE scope=:scope) AS dissent,
                   (SELECT critic_agents FROM tasks WHERE id=:task) AS critic_agents,
                   (SELECT review_rounds FROM tasks WHERE id=:task) AS review_rounds
        """), {"task": task_id, "scope": scope})).mappings().one()
        return {key: int(value) for key, value in row.items()}


async def _code_fingerprint() -> str:
    return await p4._code_identity()


async def _maintenance_migration_snapshot(database_url: str) -> dict[str, object]:
    engine = create_engine(database_url)
    try:
        async with engine.connect() as connection:
            head = await connection.scalar(text("SELECT version_num FROM alembic_version LIMIT 1"))
            counts = (await connection.execute(text("""
                SELECT
                    (SELECT count(*) FROM tasks) AS tasks,
                    (SELECT count(*) FROM deliberation_steps) AS steps,
                    (SELECT count(*) FROM operations) AS operations,
                    (SELECT count(*) FROM budget_ledger) AS ledger_rows,
                    (SELECT count(*) FROM operations WHERE state='UNKNOWN' OR execution_state='UNKNOWN') AS unknown_operations,
                    (SELECT count(*) FROM agent_execution_holds WHERE quiescent_at IS NULL) AS active_holds,
                    (SELECT COALESCE(sum(spent_amount),0)::text FROM budget_accounts) AS spent,
                    (SELECT COALESCE(sum(held_amount),0)::text FROM budget_accounts) AS held
            """))).mappings().one()
            result: dict[str, object] = {"head": head, **dict(counts)}
            if head == "0010_p5b_delib_maint":
                progress = (await connection.execute(text("""
                    SELECT count(*) AS steps,
                           count(*) FILTER (WHERE maintenance_completed_at IS NULL) AS unmarked,
                           count(*) FILTER (WHERE maintenance_retry_after IS NOT NULL) AS deferred
                    FROM deliberation_steps
                """))).mappings().one()
                result["maintenance_progress"] = dict(progress)
            return result
    finally:
        await engine.dispose()


def _verify_legacy_migration_preserves_rows(database_url: str) -> dict[str, object]:
    before = asyncio.run(_maintenance_migration_snapshot(database_url))
    migrated = False
    if before["head"] == "0009_p5b_parent_attempt_id":
        cfg = p3.Config(str(ROOT / "alembic.ini"))
        cfg.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
        previous_url = os.environ.get("HEKATE_DATABASE_URL")
        os.environ["HEKATE_DATABASE_URL"] = database_url
        try:
            p3.command.upgrade(cfg, "head")
        finally:
            if previous_url is None:
                os.environ.pop("HEKATE_DATABASE_URL", None)
            else:
                os.environ["HEKATE_DATABASE_URL"] = previous_url
        migrated = True
    elif before["head"] != "0010_p5b_delib_maint":
        raise ValueError(f"legacy preservation database must be at 0009 or 0010, found {before['head']}")
    after = asyncio.run(_maintenance_migration_snapshot(database_url))
    preserved_keys = ("tasks", "steps", "operations", "ledger_rows", "unknown_operations", "active_holds", "spent", "held")
    if any(before[key] != after[key] for key in preserved_keys):
        raise AssertionError(f"0010 changed pre-existing Phase 5B data: before={before}, after={after}")
    progress = after.get("maintenance_progress", {})
    if progress.get("steps") != before["steps"] or progress.get("unmarked") != before["steps"] or progress.get("deferred") != 0:
        raise AssertionError(f"0010 did not preserve existing rows with empty maintenance metadata: {after}")
    return {
        "database": make_url(database_url).database,
        "migrated_from_0009": migrated,
        "before": before,
        "after": after,
        "rows_and_accounting_preserved": True,
    }


def _verify_empty_migration_roundtrip(database_url: str) -> dict[str, object]:
    async def _is_blank() -> bool:
        engine = create_engine(database_url)
        try:
            async with engine.connect() as connection:
                return await connection.scalar(text("SELECT to_regclass('public.alembic_version') IS NULL"))
        finally:
            await engine.dispose()

    if not asyncio.run(_is_blank()):
        raise ValueError("empty migration round-trip requires a fresh database without Alembic state")
    cfg = p3.Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
    previous_url = os.environ.get("HEKATE_DATABASE_URL")
    os.environ["HEKATE_DATABASE_URL"] = database_url
    try:
        p3.command.upgrade(cfg, "head")
        upgraded = asyncio.run(_maintenance_migration_snapshot(database_url))
        if upgraded["head"] != "0010_p5b_delib_maint" or any(
            upgraded[key] != 0 for key in ("tasks", "steps", "operations", "ledger_rows", "unknown_operations", "active_holds")
        ):
            raise AssertionError(f"fresh migration did not create an empty 0010 database: {upgraded}")
        p3.command.downgrade(cfg, "-1")
        downgraded = asyncio.run(_maintenance_migration_snapshot(database_url))
        if downgraded["head"] != "0009_p5b_parent_attempt_id":
            raise AssertionError(f"empty 0010 downgrade did not stop at 0009: {downgraded}")
        p3.command.upgrade(cfg, "head")
        reupgraded = asyncio.run(_maintenance_migration_snapshot(database_url))
        if reupgraded["head"] != "0010_p5b_delib_maint" or reupgraded["steps"] != 0:
            raise AssertionError(f"empty migration re-upgrade failed: {reupgraded}")
        return {
            "database": make_url(database_url).database,
            "upgrade_head": upgraded["head"],
            "downgrade_head": downgraded["head"],
            "reupgrade_head": reupgraded["head"],
            "rows_preserved": reupgraded["steps"] == 0,
            "passed": True,
        }
    finally:
        if previous_url is None:
            os.environ.pop("HEKATE_DATABASE_URL", None)
        else:
            os.environ["HEKATE_DATABASE_URL"] = previous_url


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", default=os.environ.get("HEKATE_TEST_DATABASE_URL", ""))
    parser.add_argument("--legacy-database-url", default=os.environ.get("HEKATE_TEST_LEGACY_DATABASE_URL", ""))
    parser.add_argument("--empty-migration-database-url", default=os.environ.get("HEKATE_TEST_EMPTY_MIGRATION_DATABASE_URL", ""))
    parser.add_argument("--node-bin", default=os.environ.get("HEKATE_NODE_BIN", ""))
    parser.add_argument("--node-archive", type=Path, default=Path(os.environ.get("HEKATE_NODE_ARCHIVE", "/tmp/hekate-node-v22.19.0-linux-x64.tar.xz")))
    parser.add_argument("--image", default=f"hekate/letta-code-p1:{p3.p1.LOCK['app_server']['source_commit'][:8]}-{p3.p1.LOCK['patches'][0]['sha256'][:8]}")
    parser.add_argument("--artifact", type=Path)
    args = parser.parse_args()
    if not args.database_url or not args.node_bin:
        parser.error("provide --database-url and --node-bin")
    parsed = make_url(args.database_url)
    if not (parsed.database or "").startswith("hekate_phase5b_") or parsed.host not in {"127.0.0.1", "localhost"}:
        parser.error("probe requires an isolated loopback hekate_phase5b_* database")
    legacy_migration_check = None
    if args.legacy_database_url:
        legacy_parsed = make_url(args.legacy_database_url)
        if (
            not (legacy_parsed.database or "").startswith("hekate_phase5b_")
            or legacy_parsed.host not in {"127.0.0.1", "localhost"}
            or legacy_parsed.database == parsed.database
        ):
            parser.error("legacy preservation check requires a distinct isolated loopback hekate_phase5b_* database")
        legacy_migration_check = _verify_legacy_migration_preserves_rows(args.legacy_database_url)
    if args.empty_migration_database_url:
        empty_parsed = make_url(args.empty_migration_database_url)
        if (
            not (empty_parsed.database or "").startswith("hekate_phase5b_")
            or empty_parsed.host not in {"127.0.0.1", "localhost"}
            or empty_parsed.database in {parsed.database, make_url(args.legacy_database_url).database if args.legacy_database_url else None}
        ):
            parser.error("empty migration round-trip requires a distinct isolated loopback hekate_phase5b_* database")
        empty_migration_check = _verify_empty_migration_roundtrip(args.empty_migration_database_url)
    else:
        parser.error("provide --empty-migration-database-url for the reversible migration check")
    os.environ["PATH"] = f"{Path(args.node_bin).resolve().parent}{os.pathsep}{os.environ.get('PATH', '')}"
    cfg = p3.Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("sqlalchemy.url", args.database_url.replace("%", "%%"))
    os.environ["HEKATE_DATABASE_URL"] = args.database_url
    p3.command.upgrade(cfg, "head")
    run_id = f"p5b-{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    artifact = args.artifact or ROOT / "integration/runtime/artifacts" / f"{run_id}.json"
    report = asyncio.run(_run(args.database_url, args.node_bin, args.image, args.node_archive, artifact, run_id))
    if legacy_migration_check is not None:
        report["legacy_database_migration_check"] = legacy_migration_check
    populated_downgrade_guard: dict[str, object]
    try:
        p3.command.downgrade(cfg, "-1")
    except Exception as error:
        if "refusing to discard persisted deliberation maintenance progress" not in str(error):
            raise
        head_after_downgrade_attempt = asyncio.run(_maintenance_migration_snapshot(args.database_url))["head"]
        if head_after_downgrade_attempt != "0010_p5b_delib_maint":
            raise AssertionError("refused populated downgrade changed the migration head") from error
        populated_downgrade_guard = {
            "refused": True, "reason": str(error), "head_unchanged": True,
        }
    else:
        raise AssertionError("populated maintenance metadata did not protect migration downgrade")
    report["migration_safety"] = {
        "legacy_rows_preserved": legacy_migration_check,
        "empty_upgrade_downgrade_reupgrade": empty_migration_check,
        "populated_downgrade_guard": populated_downgrade_guard,
    }
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    print(json.dumps({"artifact": artifact.relative_to(ROOT).as_posix(), "status": report["overall_status"], "real_provider_calls": 0}, sort_keys=True))
    return 0 if report["overall_status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
