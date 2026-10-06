from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from urllib.parse import urlsplit
from uuid import NAMESPACE_URL, uuid5

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import ValidationError
from starlette.background import BackgroundTask

from hekate.application.budgets import authorize_provider_call, consume_call_permit
from hekate.application.budgets import _restore_binding
from hekate.application.projections import authorize_runtime_memory_write, record_runtime_memory_write_result
from hekate.application.runtime_inbox import InboxBinding, RuntimeInboxPayload, process_runtime_observation
from hekate.domain.contracts import canonical_json
from hekate.domain.errors import HekateError
from hekate.domain.models import (
    ExecutionEnvelope,
    NormalizedUsage,
    PriceTable,
    ProjectionWriteAuthorization,
    ProviderCallPlan,
    RuntimeLimits,
    BillableCallIntent,
    ProviderExecutionProfile,
)
from hekate.domain.budget_math import price_usage
from hekate.domain.types import AccountingCallId, OperationId, PermitId, ProviderCallId
from hekate.ports.store import UowFactory
from .token_accounting import measure_test_request, validate_measurement, validate_profile
from .qwen_ollama import (
    measure_qwen35_request,
)

MAX_REQUEST_BYTES = 2_097_152
_LOG = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ProviderGatewayProfile:
    profile_id: str
    price_table: PriceTable
    upstream_base_url: str
    upstream_api_key: str
    max_input_tokens: int
    max_output_tokens: int
    test_only: bool
    execution_profile: ProviderExecutionProfile | None = None
    permit_ttl_seconds: int = 30

    def validate(self, *, allow_test_profile: bool = False) -> None:
        parsed = urlsplit(self.upstream_base_url)
        if (
            parsed.scheme != "http"
            or parsed.hostname not in {"127.0.0.1", "localhost"}
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or not self.profile_id
            or not self.upstream_api_key
        ):
            raise ValueError("provider gateway upstream must be a fixed loopback endpoint")
        if self.test_only != self.price_table.synthetic:
            raise ValueError("synthetic prices must be test-only")
        if self.test_only and not allow_test_profile:
            raise ValueError("test provider profiles require explicit test-mode construction")
        if not self.test_only:
            raise ValueError("production provider dispatch remains blocked pending an approved model profile")
        if self.execution_profile is None:
            raise ValueError("provider execution profile is missing")
        if self.max_input_tokens < 0 or self.max_output_tokens < 1 or self.permit_ttl_seconds < 1:
            raise ValueError("provider gateway profile limits are invalid")
        validate_profile(self.execution_profile, self.price_table, allow_test_profile=allow_test_profile)
        if (
            self.execution_profile.profile_id != self.profile_id
            or self.execution_profile.model != self.price_table.model
            or self.execution_profile.max_input_tokens != self.max_input_tokens
            or self.execution_profile.max_output_tokens != self.max_output_tokens
        ):
            raise ValueError("gateway limits or model differ from the immutable execution profile")


def _strict_json(payload: bytes) -> object:
    def pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    return json.loads(payload.decode("utf-8", "strict"), object_pairs_hook=pairs)


async def _read_body(request: Request) -> bytes:
    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > MAX_REQUEST_BYTES:
            raise ValueError("provider request exceeds 2 MiB")
        chunks.append(chunk)
    return b"".join(chunks)


def _claims(request: Request) -> dict[str, str]:
    names = {
        "task_id": "x-hekate-task-id",
        "attempt_id": "x-hekate-attempt-id",
        "operation_id": "x-hekate-operation-id",
        "agent_registry_id": "x-hekate-agent-registry-id",
        "provider_agent_id": "x-hekate-provider-agent-id",
        "conversation_id": "x-hekate-conversation-id",
        "accounting_call_id": "x-hekate-accounting-call-id",
        "call_kind": "x-hekate-call-kind",
        "model": "x-hekate-model",
        "input_revision": "x-hekate-input-revision",
        "fence": "x-hekate-fence",
        "max_output_tokens": "x-hekate-max-output-tokens",
    }
    values = {key: request.headers.get(header, "") for key, header in names.items()}
    if any(not values[name] for name in names if name != "max_output_tokens"):
        raise ValueError("provider request is missing runtime identity headers")
    return values


async def _trusted_call(factory: UowFactory, claims: dict[str, str], plan_profile_id: str, plan_profile_digest: str):
    operation_id = OperationId(claims["operation_id"])
    async with factory() as uow:
        operation = await uow.delivery.lock_operation(operation_id)
        binding = _restore_binding(operation)
        expected = {
            "task_id": str(binding.task_id),
            "attempt_id": str(binding.attempt_id),
            "agent_registry_id": str(binding.agent_registry_id),
            "provider_agent_id": str(binding.provider_agent_id),
            "conversation_id": binding.conversation_id or "",
            "input_revision": str(binding.input_revision),
            "fence": str(binding.fence),
        }
        if any(claims[name] != value for name, value in expected.items()):
            raise ValueError("runtime metadata differs from admitted binding")
        if operation["state"] != "ADMITTED" or operation["dispatch_state"] not in {"SEND_INTENT", "DISPATCHED"}:
            raise ValueError("operation has no durable send intent")
        dispatch = await uow.delivery.get_dispatch_payload(operation_id)
        if not isinstance(dispatch, dict) or not isinstance(dispatch.get("payload"), dict):
            raise ValueError("operation dispatch payload is unavailable")
        raw_plan = dispatch["payload"].get("call_plan")
        plan = ProviderCallPlan.model_validate_json(canonical_json(raw_plan), strict=True)
        if plan.profile_id != plan_profile_id or plan.profile_digest != plan_profile_digest:
            raise ValueError("provider profile differs from admitted plan")
        attempt = await uow.tasks.get_attempt(binding.attempt_id)
        registry = await uow.agents.get_registry(binding.agent_registry_id)
        if (
            registry is None
            or attempt.task_id != binding.task_id
            or attempt.operation_id != operation_id
            or attempt.input_revision != binding.input_revision
            or attempt.agent_registry_id != binding.agent_registry_id
            or registry.owner_scope != binding.scope
            or registry.provider_id != binding.provider_agent_id
        ):
            raise ValueError("provider output contract binding is inconsistent")
        if plan.output_contract is not None:
            expected_contracts = {
                ("hekate.turn", "planning"): ("hekate", "persistent", "hekate_turn_output_v1"),
                ("hekate.synthesis", "synthesis"): ("hekate", "persistent", "hekate_turn_output_v1"),
                ("hekate.reasoning", "hekate_reasoning"): ("hekate", "persistent", "hekate_turn_output_v1"),
                ("hekate.synthesis", "synthesis_round2"): ("hekate", "persistent", "hekate_turn_output_v1"),
                ("critic.review", "critic_review"): ("critic", "ephemeral", "critic_turn_output_v1"),
            }
            expected = expected_contracts.get((operation["kind"], attempt.kind))
            if (
                expected is None
                or plan.output_contract != expected[2]
                or registry.kind != expected[0]
                or registry.persistence != expected[1]
            ):
                raise ValueError("admitted operation output contract differs from trusted attempt and registry")
        envelope = ExecutionEnvelope.model_validate_json(canonical_json(operation["envelope"]), strict=True)
        lease = await uow.agents.get_lease(binding.agent_registry_id)
        if lease is None or lease.fence != binding.fence or lease.expires_at <= datetime.now(UTC):
            raise ValueError("runtime registry lease is stale")
        return binding, envelope, plan, lease.owner


def _no_redirect_handler():
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            return None

    return urllib.request.build_opener(NoRedirect)


def _usage_from(value: object, source: str) -> dict[str, object] | None:
    if not isinstance(value, dict):
        return None
    usage = value.get("usage")
    if not isinstance(usage, dict):
        return None
    fields = {
        "input_tokens": usage.get("prompt_tokens", usage.get("input_tokens")),
        "output_tokens": usage.get("completion_tokens", usage.get("output_tokens")),
        "total_tokens": usage.get("total_tokens"),
    }
    input_details = usage.get("prompt_tokens_details", usage.get("input_tokens_details"))
    output_details = usage.get("completion_tokens_details", usage.get("output_tokens_details"))
    if input_details is not None:
        if not isinstance(input_details, dict) or set(input_details) - {"cached_tokens"}:
            return None
        fields["cache_tokens"] = input_details.get("cached_tokens")
    if output_details is not None:
        if not isinstance(output_details, dict) or set(output_details) - {"reasoning_tokens"}:
            return None
        fields["reasoning_tokens"] = output_details.get("reasoning_tokens")
    if any(item is not None and (type(item) is not int or item < 0) for item in fields.values()):
        return None
    provided = [item is not None for item in fields.values()]
    completeness = "COMPLETE" if all(provided) else "PARTIAL" if any(provided) else "UNKNOWN"
    if completeness == "UNKNOWN":
        return None
    return {"source": source, "completeness": completeness, **fields}


class _ProviderSSEObserver:
    """Bounded SSE parser that records content digests and terminal metadata only."""

    def __init__(self) -> None:
        self._pending = bytearray()
        self._discarding_oversize_line = False
        self._choices: dict[int, dict[str, object]] = {}
        self._hashes: dict[int, object] = {}
        self._reasoning_hashes: dict[tuple[int, str], object] = {}
        self._wire_hash = hashlib.sha256()
        self._wire_bytes = 0
        self._done_seen = False
        self._event_count = 0
        self._event_order: list[dict[str, object]] = []
        self._malformed_event_count = 0
        self._oversize_line_count = 0

    def feed(self, chunk: bytes) -> list[dict[str, object]]:
        self._wire_hash.update(chunk)
        self._wire_bytes += len(chunk)
        self._pending.extend(chunk)
        parsed: list[dict[str, object]] = []
        while True:
            try:
                newline = self._pending.index(0x0A)
            except ValueError:
                break
            line = bytes(self._pending[:newline]).rstrip(b"\r")
            del self._pending[:newline + 1]
            if self._discarding_oversize_line:
                self._discarding_oversize_line = False
                continue
            record = self._data_line(line)
            if record is not None:
                parsed.append(record)
        if len(self._pending) > 65_536:
            self._pending[:] = self._pending[-65_536:]
            self._discarding_oversize_line = True
            self._oversize_line_count += 1
        return parsed

    def finish(self) -> list[dict[str, object]]:
        if not self._pending or self._discarding_oversize_line:
            self._pending.clear()
            return []
        line = bytes(self._pending).rstrip(b"\r")
        self._pending.clear()
        record = self._data_line(line)
        return [record] if record is not None else []

    def _data_line(self, line: bytes) -> dict[str, object] | None:
        if not line.startswith(b"data:"):
            return None
        data = line[5:].strip()
        if data == b"[DONE]":
            self._done_seen = True
            self._event_order.append({
                "sequence": len(self._event_order) + 1,
                "event": "done_marker",
                "received_monotonic_ns": time.monotonic_ns(),
            })
            return None
        try:
            record = _strict_json(data)
        except (ValueError, json.JSONDecodeError, UnicodeDecodeError):
            self._malformed_event_count += 1
            return None
        if not isinstance(record, dict):
            self._malformed_event_count += 1
            return None
        self._event_count += 1
        ordered_event: dict[str, object] = {
            "sequence": len(self._event_order) + 1,
            "event": "provider_sse_data",
            "received_monotonic_ns": time.monotonic_ns(),
            "provider_response_id": record.get("id"),
            "choice_events": [],
            "usage_present": isinstance(record.get("usage"), dict),
        }
        choices = record.get("choices")
        if isinstance(choices, list):
            for choice in choices:
                if not isinstance(choice, dict):
                    continue
                index = choice.get("index")
                if type(index) is not int or index < 0:
                    continue
                summary = self._choices.setdefault(index, {
                    "content_delta_events": 0,
                    "content_utf8_bytes": 0,
                    "finish_reason": None,
                    "reasoning_channels": {},
                })
                finish_reason = choice.get("finish_reason")
                if isinstance(finish_reason, str):
                    summary["finish_reason"] = finish_reason
                delta = choice.get("delta")
                content = delta.get("content") if isinstance(delta, dict) else None
                choice_event: dict[str, object] = {
                    "choice_index": index,
                    "channel": "assistant" if isinstance(content, str) and content else "metadata",
                    "content_delta_utf8_bytes": 0,
                    "content_delta_sha256": hashlib.sha256(b"").hexdigest(),
                    "finish_reason": finish_reason,
                }
                if isinstance(content, str) and content:
                    content_bytes = content.encode("utf-8", "strict")
                    choice_event["content_delta_utf8_bytes"] = len(content_bytes)
                    choice_event["content_delta_sha256"] = hashlib.sha256(content_bytes).hexdigest()
                    summary["content_delta_events"] = int(summary["content_delta_events"]) + 1
                    summary["content_utf8_bytes"] = int(summary["content_utf8_bytes"]) + len(content_bytes)
                    digest = self._hashes.get(index)
                    if digest is None:
                        digest = hashlib.sha256()
                        self._hashes[index] = digest
                    digest.update(content_bytes)
                if isinstance(delta, dict):
                    channels = summary["reasoning_channels"]
                    if not isinstance(channels, dict):
                        channels = {}
                        summary["reasoning_channels"] = channels
                    for field in ("reasoning", "reasoning_content", "analysis"):
                        reasoning = delta.get(field)
                        if not isinstance(reasoning, str) or not reasoning:
                            continue
                        reasoning_bytes = reasoning.encode("utf-8", "strict")
                        channel = channels.setdefault(field, {"delta_events": 0, "utf8_bytes": 0})
                        channel["delta_events"] = int(channel["delta_events"]) + 1
                        channel["utf8_bytes"] = int(channel["utf8_bytes"]) + len(reasoning_bytes)
                        digest = self._reasoning_hashes.get((index, field))
                        if digest is None:
                            digest = hashlib.sha256()
                            self._reasoning_hashes[(index, field)] = digest
                        digest.update(reasoning_bytes)
                        choice_event["channel"] = "reasoning" if choice_event["channel"] == "metadata" else "assistant_and_reasoning"
                ordered_event["choice_events"].append(choice_event)
        self._event_order.append(ordered_event)
        return record

    @property
    def done_seen(self) -> bool:
        return self._done_seen

    def snapshot(self) -> dict[str, object]:
        choices = []
        for index in sorted(self._choices):
            item = dict(self._choices[index])
            digest = self._hashes.get(index)
            item["index"] = index
            item["content_sha256"] = digest.hexdigest() if digest is not None else hashlib.sha256(b"").hexdigest()
            channels = item.get("reasoning_channels")
            if isinstance(channels, dict):
                item["reasoning_channels"] = {
                    field: {
                        **channel,
                        "sha256": self._reasoning_hashes[(index, field)].hexdigest(),
                    }
                    for field, channel in sorted(channels.items())
                    if isinstance(channel, dict) and (index, field) in self._reasoning_hashes
                }
            choices.append(item)
        return {
            "received_and_yielded_wire_utf8_bytes": self._wire_bytes,
            "received_and_yielded_wire_sha256": self._wire_hash.hexdigest(),
            "event_count": self._event_count,
            "event_order": list(self._event_order),
            "malformed_event_count": self._malformed_event_count,
            "oversize_line_count": self._oversize_line_count,
            "done_seen": self._done_seen,
            "choices": choices,
        }


def create_provider_gateway(
    factory: UowFactory,
    profile: ProviderGatewayProfile,
    private_token: str,
    *,
    allow_test_profile: bool = False,
) -> FastAPI:
    profile.validate(allow_test_profile=allow_test_profile)
    if len(private_token) < 32:
        raise ValueError("provider gateway private token must be at least 32 characters")
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    app.state.metrics = {
        "upstream_forward_attempts": 0,
        "observation_write_attempts": 0,
        "observation_write_failures": 0,
        "observation_conflicts": 0,
    }
    capture_stream_observations = allow_test_profile and profile.test_only
    app.state.last_provider_stream_observation = None
    app.state.provider_stream_observations = [] if capture_stream_observations else None
    opener = _no_redirect_handler()

    def authenticated(request: Request) -> bool:
        return request.headers.get("authorization") == f"Bearer {private_token}"

    @app.get("/v1/models")
    async def models(request: Request):
        if not authenticated(request):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        return {"object": "list", "data": [{"id": profile.price_table.model, "object": "model"}]}

    @app.post("/internal/memory-projection/authorize")
    async def authorize_memory_projection(request: Request):
        if not authenticated(request):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        try:
            raw = _strict_json(await _read_body(request))
            if not isinstance(raw, dict):
                raise ValueError("projection write authorization must be an object")
            identity = ProjectionWriteAuthorization.model_validate(raw, strict=True)
            grant_id = await authorize_runtime_memory_write(factory, identity)
            return {"grant_id": grant_id, "decision": "AUTHORIZED"}
        except (ValueError, ValidationError) as error:
            return JSONResponse({"error": str(error)[:240]}, status_code=400)
        except HekateError as error:
            return JSONResponse({"error": str(error)[:240]}, status_code=403)
        except Exception as error:
            _LOG.exception("projection write authorization failed")
            return JSONResponse({"error": type(error).__name__}, status_code=503)

    @app.post("/internal/memory-projection/complete")
    async def complete_memory_projection(request: Request):
        if not authenticated(request):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        try:
            raw = _strict_json(await _read_body(request))
            if not isinstance(raw, dict) or set(raw) - {"grant_id", "state", "observation"} or not {
                "grant_id", "state",
            }.issubset(raw):
                raise ValueError("projection write completion has an invalid shape")
            grant_id = raw["grant_id"]
            state = raw["state"]
            observation = raw.get("observation")
            if not isinstance(grant_id, str) or not grant_id or state not in {"EFFECT_CONFIRMED", "NO_EFFECT"}:
                raise ValueError("projection write completion identity is invalid")
            if observation is not None and not isinstance(observation, dict):
                raise ValueError("projection write observation must be an object")
            await record_runtime_memory_write_result(factory, grant_id, state, observation)
            return {"grant_id": grant_id, "state": state}
        except (ValueError, ValidationError) as error:
            return JSONResponse({"error": str(error)[:240]}, status_code=400)
        except HekateError as error:
            return JSONResponse({"error": str(error)[:240]}, status_code=409)
        except Exception as error:
            _LOG.exception("projection write completion failed")
            return JSONResponse({"error": type(error).__name__}, status_code=503)

    @app.api_route("/v1/{endpoint:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
    async def proxy(request: Request, endpoint: str):
        if not authenticated(request):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        if request.method != "POST" or endpoint != "chat/completions":
            return JSONResponse({"error": "unsupported provider endpoint"}, status_code=404)
        try:
            if not request.headers.get("content-type", "").lower().startswith("application/json"):
                raise ValueError("provider request must use application/json")
            if request.url.query:
                raise ValueError("provider query parameters are not supported")
            raw_body = await _read_body(request)
            body = _strict_json(raw_body)
            claims = _claims(request)
            if not isinstance(body, dict):
                raise ValueError("provider body must be a JSON object")
            model = body.get("model")
            if model != claims["model"] or model != profile.price_table.model:
                raise ValueError("provider model differs from admitted model")
            token_fields = [key for key in ("max_tokens", "max_completion_tokens") if key in body]
            if len(token_fields) != 1:
                raise ValueError("provider request must specify exactly one output-token limit")
            output_limit = body[token_fields[0]]
            if type(output_limit) is not int or output_limit < 1:
                raise ValueError("provider output-token limit must be a positive integer")
            if claims["max_output_tokens"]:
                if not claims["max_output_tokens"].isdigit() or output_limit > int(claims["max_output_tokens"]):
                    raise ValueError(
                        f"provider token limit {output_limit} exceeds runtime metadata ceiling {claims['max_output_tokens']}"
                    )
            if profile.execution_profile is None:
                raise ValueError("provider execution profile is unavailable")
            binding, envelope, plan, lease_owner = await _trusted_call(
                factory, claims, profile.profile_id, profile.execution_profile.content_digest,
            )
            if (
                plan.model != model
                or plan.pricing_version != profile.price_table.version
                or plan.max_input_tokens > profile.max_input_tokens
                or plan.max_output_tokens > profile.max_output_tokens
                or output_limit > plan.max_output_tokens
                or output_limit > envelope.max_output_tokens
            ):
                raise ValueError("provider request exceeds the fixed profile or call plan")
            if profile.execution_profile.provider == "ollama-local":
                _normalized_body, final_body, measurement = measure_qwen35_request(
                    body, profile.execution_profile, output_limit,
                    output_contract=plan.output_contract,
                )
            else:
                final_body = raw_body
                measurement = measure_test_request(raw_body, body, profile.execution_profile, output_limit)
            validate_measurement(
                measurement, profile.execution_profile,
                plan_max_input=plan.max_input_tokens,
                envelope_max_input=envelope.max_input_tokens,
                plan_max_output=plan.max_output_tokens,
                envelope_max_output=envelope.max_output_tokens,
            )
            call_kind = claims["call_kind"]
            accounting_id = AccountingCallId(claims["accounting_call_id"])
            permit_id = PermitId(str(uuid5(NAMESPACE_URL, f"hekate:permit:{accounting_id}")))
            limits = RuntimeLimits(
                max_input_tokens=plan.max_input_tokens,
                max_output_tokens=output_limit,
                max_billable_calls=envelope.billable_call_slots,
                deadline=envelope.deadline,
            )
            allocation = price_usage(NormalizedUsage(
                completeness="COMPLETE",
                input_tokens=plan.max_input_tokens,
                output_tokens=output_limit,
                total_tokens=plan.max_input_tokens + output_limit,
            ), profile.price_table).amount
            intent = BillableCallIntent(
                accounting_call_id=accounting_id,
                permit_id=permit_id,
                operation_id=OperationId(claims["operation_id"]),
                call_kind=call_kind,
                slot_key=f"{call_kind}:{accounting_id}",
                binding=binding,
                model=str(model),
                allocation_amount=allocation,
                limits=limits,
                price_table=profile.price_table,
                permit_expires_at=min(envelope.deadline, datetime.now(UTC) + timedelta(seconds=profile.permit_ttl_seconds)),
                lease_owner=lease_owner,
                test_only=profile.test_only,
                reservation_id=envelope.reservation_id,
                measurement=measurement,
            )
            permit = await authorize_provider_call(factory, intent)
            await consume_call_permit(
                factory, binding, lease_owner, permit.permit_id, accounting_id,
                expected_request_digest=measurement.request_digest,
                expected_profile_digest=measurement.profile_digest,
            )
        except (ValueError, ValidationError, HekateError, KeyError, TypeError) as error:
            _LOG.warning("provider request denied: %s: %s", type(error).__name__, str(error)[:240])
            return JSONResponse({"error": "provider request denied"}, status_code=402)

        upstream_url = profile.upstream_base_url.rstrip("/") + "/v1/chat/completions"
        upstream_request = urllib.request.Request(
            upstream_url,
            data=final_body,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {profile.upstream_api_key}", "Accept": request.headers.get("accept", "application/json")},
            method="POST",
        )
        app.state.metrics["upstream_forward_attempts"] += 1
        try:
            upstream = await asyncio.to_thread(opener.open, upstream_request, timeout=60)
        except urllib.error.HTTPError as error:
            upstream = error
        except Exception:
            await _record_call(factory, profile, claims, binding, lease_owner, "UNKNOWN", None, None, app.state.metrics)
            return JSONResponse({"error": "provider outcome is unknown"}, status_code=502)

        content_type = upstream.headers.get("Content-Type", "application/json")
        provider_call_id: str | None = None
        latest_usage: dict[str, object] | None = None
        is_sse = "text/event-stream" in content_type.lower()
        if is_sse:
            await _record_call(factory, profile, claims, binding, lease_owner, "RUNNING", None, None, app.state.metrics)
            completed = False
            stream_observer = _ProviderSSEObserver()

            async def stream_body():
                nonlocal provider_call_id, latest_usage, completed
                try:
                    while True:
                        chunk = await asyncio.to_thread(upstream.read, 8192)
                        if not chunk:
                            break
                        for record in stream_observer.feed(chunk):
                            candidate = record.get("id")
                            if isinstance(candidate, str):
                                provider_call_id = candidate
                            latest_usage = _usage_from(record, "provider_reported") or latest_usage
                        yield chunk
                    for record in stream_observer.finish():
                        candidate = record.get("id")
                        if isinstance(candidate, str):
                            provider_call_id = candidate
                        latest_usage = _usage_from(record, "provider_reported") or latest_usage
                    completed = stream_observer.done_seen
                except asyncio.CancelledError:
                    await _record_call(factory, profile, claims, binding, lease_owner, "UNKNOWN", provider_call_id, latest_usage, app.state.metrics)
                    raise
                except Exception:
                    await _record_call(factory, profile, claims, binding, lease_owner, "UNKNOWN", provider_call_id, latest_usage, app.state.metrics)
                    raise
                finally:
                    observation = {
                        **stream_observer.snapshot(),
                        "operation_id": claims["operation_id"],
                        "accounting_call_id": claims["accounting_call_id"],
                        "provider_call_id": provider_call_id,
                        "choice_scope": "choice index as received; reasoning channels summarized separately",
                    }
                    if capture_stream_observations:
                        app.state.last_provider_stream_observation = observation
                        app.state.provider_stream_observations.append(observation)
                    await asyncio.to_thread(upstream.close)

            async def record_stream_completion():
                await _record_call(
                    factory, profile, claims, binding, lease_owner,
                    "QUIESCENT" if completed else "UNKNOWN", provider_call_id, latest_usage, app.state.metrics,
                )

            return StreamingResponse(
                stream_body(),
                status_code=upstream.status,
                media_type="text/event-stream",
                headers={key: upstream.headers[key] for key in ("Cache-Control", "X-Accel-Buffering") if key in upstream.headers},
                background=BackgroundTask(record_stream_completion),
            )

        try:
            content = await asyncio.to_thread(upstream.read)
        except Exception:
            await asyncio.to_thread(upstream.close)
            await _record_call(factory, profile, claims, binding, lease_owner, "UNKNOWN", None, None, app.state.metrics)
            return JSONResponse({"error": "provider outcome is unknown"}, status_code=502)
        await asyncio.to_thread(upstream.close)
        try:
            decoded = _strict_json(content)
        except (ValueError, json.JSONDecodeError, UnicodeDecodeError):
            decoded = None
        if isinstance(decoded, dict):
            value = decoded.get("id")
            provider_call_id = value if isinstance(value, str) else None
            latest_usage = _usage_from(decoded, "provider_reported")
        await _record_call(factory, profile, claims, binding, lease_owner, "QUIESCENT", provider_call_id, latest_usage, app.state.metrics)
        status = upstream.status
        if 300 <= status < 400:
            return JSONResponse({"error": "provider redirects are blocked"}, status_code=502)
        return Response(
            content=content,
            status_code=status,
            headers={"content-type": content_type},
        )

    return app


async def _record_call(factory, profile, claims, binding, lease_owner, state, provider_call_id, usage, metrics=None):
    call_id = claims["accounting_call_id"]
    event_type = "provider_call"
    payload = RuntimeInboxPayload(
        event_type=event_type,
        operation_id=claims["operation_id"],
        accounting_call_id=call_id,
        source="provider_response",
        observation_identity=f"provider:{call_id}:{state.lower()}",
        binding=InboxBinding.from_binding(binding),
        provider_call_id=provider_call_id,
        state=state,
        lease_owner=lease_owner,
        observer_fence=binding.fence,
        usage=usage,
    )
    if metrics is not None:
        metrics["observation_write_attempts"] += 1
    try:
        result = await process_runtime_observation(factory, profile.profile_id, payload.observation_identity, payload)
        if result["conflict"]:
            if metrics is not None:
                metrics["observation_conflicts"] += 1
            _LOG.error("provider observation conflict for accounting call %s", call_id)
    except Exception as error:
        if metrics is not None:
            metrics["observation_write_failures"] += 1
        # ponytail: provider response already exists; retain the consumed permit and let reconciliation handle the gap.
        _LOG.error(
            "provider observation could not be persisted for accounting call %s: %s: %s",
            call_id, type(error).__name__, str(error)[:240],
        )
