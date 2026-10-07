from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from functools import lru_cache
from importlib.metadata import version
from pathlib import Path
from typing import Mapping

import tiktoken
from tiktoken.core import Encoding
from tiktoken.load import load_tiktoken_bpe

from hekate.domain.contracts import canonical_json, canonical_json_hash
from hekate.domain.models import PriceTable, ProviderExecutionProfile, ProviderRequestMeasurement

_ASSETS = Path(__file__).with_name("assets")
_CONTRACT_PATH = _ASSETS / "test_chat_contract.v1.json"
_TOKENIZER_PATH = _ASSETS / "cl100k_base.tiktoken"
_TOKENIZER_SHA256 = "223921b76ee99bde995b7ff738513eef100fb51d18c93597a113bcffe865b2a7"
_PATTERN = r"'(?i:[sdmt]|ll|ve|re)|[^\r\n\p{L}\p{N}]?+\p{L}++|\p{N}{1,3}+| ?[^\s\p{L}\p{N}]++[\r\n]*+|\s++$|\s*[\r\n]|\s+(?!\S)|\s"
_SPECIAL_TOKENS = {
    "<|endoftext|>": 100257,
    "<|fim_prefix|>": 100258,
    "<|fim_middle|>": 100259,
    "<|fim_suffix|>": 100260,
    "<|endofprompt|>": 100276,
}
_TOP_LEVEL_ALLOWED = {
    "model", "messages", "max_tokens", "max_completion_tokens", "stream", "stream_options",
    "tools", "tool_choice", "parallel_tool_calls", "response_format", "temperature", "top_p", "n",
    "stop", "presence_penalty", "frequency_penalty", "seed", "user", "logprobs", "top_logprobs",
    "service_tier", "store",
}
_GENERATION_CONTROLS = {
    "model", "max_tokens", "max_completion_tokens", "stream", "stream_options", "temperature", "top_p",
    "n", "stop", "presence_penalty", "frequency_penalty", "seed", "user", "logprobs", "top_logprobs",
    "service_tier", "store",
}


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _token_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _contract() -> tuple[dict[str, object], bytes]:
    raw = _CONTRACT_PATH.read_bytes()
    value = json.loads(raw)
    if not isinstance(value, dict) or value.get("contract_id") != "hekate.fake.chat-json.v1":
        raise ValueError("fake provider token contract is missing or malformed")
    return value, raw


@lru_cache(maxsize=1)
def _encoding() -> Encoding:
    if version("tiktoken") != "0.14.0":
        raise ValueError("pinned tiktoken version does not match the test profile")
    if not _TOKENIZER_PATH.is_file():
        raise ValueError("pinned tokenizer asset is missing")
    asset = _TOKENIZER_PATH.read_bytes()
    if _sha256(asset) != _TOKENIZER_SHA256:
        raise ValueError("pinned tokenizer asset digest mismatch")
    contract, _ = _contract()
    tokenizer = contract.get("tokenizer")
    if not isinstance(tokenizer, dict) or tokenizer.get("asset_sha256") != _TOKENIZER_SHA256:
        raise ValueError("tokenizer contract and checked asset disagree")
    ranks = load_tiktoken_bpe(str(_TOKENIZER_PATH), expected_hash=_TOKENIZER_SHA256)
    encoding = Encoding(
        name="hekate-cl100k-base-0.14.0",
        pat_str=_PATTERN,
        mergeable_ranks=ranks,
        special_tokens=_SPECIAL_TOKENS,
    )
    for item in contract.get("vectors", []):
        if not isinstance(item, dict) or not isinstance(item.get("text"), str) or type(item.get("tokens")) is not int:
            raise ValueError("tokenizer vector contract is malformed")
        if len(encoding.encode(item["text"], disallowed_special=())) != item["tokens"]:
            raise ValueError("tokenizer token vector mismatch")
    return encoding


def test_execution_profile(
    *,
    profile_id: str,
    model: str,
    model_revision: str,
    context_window_tokens: int,
    max_input_tokens: int,
    max_output_tokens: int,
    pricing_version: str,
    input_usd_per_million,
    output_usd_per_million,
    pricing_effective_at: str,
) -> tuple[ProviderExecutionProfile, PriceTable]:
    """Build the explicit fake-chat test profile; never an operational provider profile."""
    encoding = _encoding()
    if not all(isinstance(item, str) and item for item in (profile_id, model, model_revision, pricing_version, pricing_effective_at)):
        raise ValueError("test execution profile identity is incomplete")
    if min(context_window_tokens, max_input_tokens, max_output_tokens) < 1:
        raise ValueError("test execution profile limits must be explicit positive integers")
    contract, raw_contract = _contract()
    renderer_digest = _sha256(Path(__file__).read_bytes())
    pricing_values = {
        "model": model,
        "version": pricing_version,
        "input_usd_per_million": str(input_usd_per_million),
        "output_usd_per_million": str(output_usd_per_million),
        "currency": "USD",
        "unit": "USD_PER_MILLION_AGGREGATE_TOKENS",
        "effective_at": pricing_effective_at,
        "usage_semantics": "aggregate_input_output_v1",
    }
    pricing_digest = canonical_json_hash(pricing_values)
    profile = ProviderExecutionProfile(
        profile_id=profile_id,
        test_only=True,
        provider="hekate-fake-provider",
        request_protocol="openai-compatible-chat-completions-v1",
        model=model,
        model_revision=model_revision,
        context_window_tokens=context_window_tokens,
        max_input_tokens=max_input_tokens,
        max_output_tokens=max_output_tokens,
        output_ceiling_includes_reasoning=contract["output_ceiling"]["includes_reasoning_tokens"],
        additional_reserved_tokens=contract["output_ceiling"]["additional_reserved_tokens"],
        tokenizer_implementation="tiktoken",
        tokenizer_version=version("tiktoken"),
        tokenizer_encoding=encoding.name,
        tokenizer_asset_revision=contract["tokenizer"]["asset_revision"],
        tokenizer_asset_sha256=_TOKENIZER_SHA256,
        renderer_id=contract["renderer"]["id"],
        renderer_revision=contract["renderer"]["revision"],
        renderer_sha256=renderer_digest,
        pricing_version=pricing_version,
        pricing_digest=pricing_digest,
        currency="USD",
        pricing_unit="USD_PER_MILLION_AGGREGATE_TOKENS",
        pricing_effective_at=pricing_effective_at,
        usage_semantics="aggregate_input_output_v1",
        verification_state="TEST_CONTRACT_VERIFIED",
        evidence_digests=(_sha256(raw_contract), _TOKENIZER_SHA256),
    )
    table = PriceTable(
        model=model, version=pricing_version,
        input_usd_per_million=input_usd_per_million,
        output_usd_per_million=output_usd_per_million,
        synthetic=True, currency="USD", unit="USD_PER_MILLION_AGGREGATE_TOKENS",
        effective_at=pricing_effective_at, usage_semantics="aggregate_input_output_v1",
    )
    return profile, table


def validate_profile(
    profile: ProviderExecutionProfile,
    price_table: PriceTable,
    *,
    allow_test_profile: bool,
    allow_local_tariff: bool = False,
) -> None:
    if profile.provider == "ollama-local":
        if not allow_test_profile:
            raise ValueError("Qwen request contracts are enabled only for explicit test-only gateway construction")
        from .qwen_ollama import (
            is_qwen35_native_json_schema_profile,
            validate_qwen35_native_json_schema_profile,
            validate_qwen_test_profile,
        )
        from .reviewed_qwen_profile import (
            is_qwen35_reviewed_native_json_schema_profile,
            validate_qwen35_reviewed_native_json_schema_profile,
        )

        if is_qwen35_native_json_schema_profile(profile):
            if allow_local_tariff:
                from .qwen_local_profile import validate_qwen35_native_json_schema_local_profile

                validate_qwen35_native_json_schema_local_profile(profile, price_table)
            else:
                validate_qwen35_native_json_schema_profile(profile, price_table)
        elif is_qwen35_reviewed_native_json_schema_profile(profile):
            if allow_local_tariff:
                from .qwen_local_profile import validate_qwen35_reviewed_native_json_schema_local_profile

                validate_qwen35_reviewed_native_json_schema_local_profile(profile, price_table)
            else:
                validate_qwen35_reviewed_native_json_schema_profile(profile, price_table)
        else:
            validate_qwen_test_profile(profile, price_table)
        return
    if not profile.test_only or profile.verification_state != "TEST_CONTRACT_VERIFIED" or not allow_test_profile:
        raise ValueError("only the explicitly enabled verified fake-chat test profile is available")
    encoding = _encoding()
    contract, raw_contract = _contract()
    renderer_digest = _sha256(Path(__file__).read_bytes())
    pricing_values = {
        "model": price_table.model,
        "version": price_table.version,
        "input_usd_per_million": str(price_table.input_usd_per_million),
        "output_usd_per_million": str(price_table.output_usd_per_million),
        "currency": price_table.currency,
        "unit": price_table.unit,
        "effective_at": price_table.effective_at,
        "usage_semantics": price_table.usage_semantics,
    }
    if (
        profile.tokenizer_version != version("tiktoken")
        or profile.tokenizer_encoding != encoding.name
        or profile.tokenizer_asset_sha256 != _TOKENIZER_SHA256
        or profile.renderer_sha256 != renderer_digest
        or profile.renderer_id != contract["renderer"]["id"]
        or profile.renderer_revision != contract["renderer"]["revision"]
        or profile.tokenizer_asset_revision != contract["tokenizer"]["asset_revision"]
        or profile.provider != "hekate-fake-provider"
        or profile.request_protocol != "openai-compatible-chat-completions-v1"
        or profile.output_ceiling_includes_reasoning is not True
        or profile.additional_reserved_tokens != 0
        or profile.evidence_digests != (_sha256(raw_contract), _TOKENIZER_SHA256)
        or profile.model != price_table.model
        or profile.pricing_version != price_table.version
        or profile.pricing_digest != canonical_json_hash(pricing_values)
        or not price_table.synthetic
        or price_table.currency != "USD"
        or price_table.unit != "USD_PER_MILLION_AGGREGATE_TOKENS"
        or price_table.usage_semantics != "aggregate_input_output_v1"
    ):
        raise ValueError("provider profile, tokenizer, renderer, or pricing contract changed")


def test_profile_and_price_for_config(config) -> tuple[ProviderExecutionProfile, PriceTable]:
    profile, _ = test_execution_profile(
        profile_id=config.profile_id, model=config.model, model_revision=config.model_revision,
        context_window_tokens=config.context_window_tokens, max_input_tokens=config.max_input_tokens,
        max_output_tokens=config.max_output_tokens, pricing_version=config.pricing_version,
        input_usd_per_million=config.input_usd_per_million,
        output_usd_per_million=config.output_usd_per_million,
        pricing_effective_at=config.pricing_effective_at,
    )
    if profile.content_digest != config.profile_digest:
        raise ValueError("persisted task execution profile digest does not match its immutable fields")
    return profile, _


def _string(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"provider {name} must be text")
    return value


def _message(message: object) -> dict[str, object]:
    if not isinstance(message, dict) or set(message) - {"role", "content", "name", "tool_call_id", "tool_calls", "refusal"}:
        raise ValueError("provider message has unsupported fields")
    role = message.get("role")
    if role not in {"system", "developer", "user", "assistant", "tool"}:
        raise ValueError("provider message role is unsupported")
    content = message.get("content")
    if content is not None and not isinstance(content, str):
        if not isinstance(content, list):
            raise ValueError("provider message content is not supported by the text renderer")
        blocks = []
        for block in content:
            if not isinstance(block, dict) or set(block) != {"type", "text"} or block.get("type") != "text":
                raise ValueError("provider multimodal or unknown content block is unsupported")
            blocks.append({"type": "text", "text": _string(block["text"], "text block")})
        content = blocks
    result: dict[str, object] = {"role": role, "content": content}
    for key in ("name", "tool_call_id", "refusal"):
        if key in message:
            result[key] = _string(message[key], key)
    calls = message.get("tool_calls")
    if calls is not None:
        if not isinstance(calls, list):
            raise ValueError("provider tool calls must be a list")
        normalized = []
        for call in calls:
            if not isinstance(call, dict) or set(call) != {"id", "type", "function"} or call.get("type") != "function":
                raise ValueError("provider tool call shape is unsupported")
            function = call.get("function")
            if not isinstance(function, dict) or set(function) != {"name", "arguments"}:
                raise ValueError("provider tool function shape is unsupported")
            normalized.append({
                "id": _string(call["id"], "tool call id"), "type": "function",
                "function": {"name": _string(function["name"], "tool name"),
                             "arguments": _string(function["arguments"], "tool arguments")},
            })
        result["tool_calls"] = normalized
    if role == "tool" and "tool_call_id" not in result:
        raise ValueError("tool result is missing its tool call identity")
    return result


def render_test_chat_input(body: Mapping[str, object]) -> str:
    unknown = set(body) - _TOP_LEVEL_ALLOWED
    if unknown:
        raise ValueError(f"provider request has unsupported token-bearing fields: {sorted(unknown)}")
    messages = body.get("messages")
    if not isinstance(messages, list) or not messages:
        raise ValueError("provider request must contain messages")
    doc: dict[str, object] = {
        "format": "hekate.chat-input.v1",
        "messages": [_message(item) for item in messages],
    }
    tools = body.get("tools")
    if tools is not None:
        if not isinstance(tools, list):
            raise ValueError("provider tools must be a list")
        for tool in tools:
            if not isinstance(tool, dict) or set(tool) != {"type", "function"} or tool.get("type") != "function":
                raise ValueError("provider tool declaration is unsupported")
            function = tool.get("function")
            if not isinstance(function, dict) or set(function) - {"name", "description", "parameters", "strict"}:
                raise ValueError("provider function declaration has unsupported fields")
            if not isinstance(function.get("name"), str) or not isinstance(function.get("parameters"), dict):
                raise ValueError("provider function declaration is incomplete")
            if "description" in function and not isinstance(function["description"], str):
                raise ValueError("provider function description must be text")
            if "strict" in function and type(function["strict"]) is not bool:
                raise ValueError("provider function strict flag must be boolean")
        doc["tools"] = tools
    tool_choice = body.get("tool_choice")
    if tool_choice is not None:
        if isinstance(tool_choice, str):
            if tool_choice not in {"none", "auto", "required"}:
                raise ValueError("provider tool choice is unsupported")
        elif not (
            isinstance(tool_choice, dict)
            and set(tool_choice) == {"type", "function"}
            and tool_choice.get("type") == "function"
            and isinstance(tool_choice.get("function"), dict)
            and set(tool_choice["function"]) == {"name"}
            and isinstance(tool_choice["function"].get("name"), str)
        ):
            raise ValueError("provider tool choice shape is unsupported")
        doc["tool_choice"] = tool_choice
    response_format = body.get("response_format")
    if response_format is not None:
        if not isinstance(response_format, dict) or response_format.get("type") not in {"text", "json_object", "json_schema"}:
            raise ValueError("provider response format is unsupported")
        if set(response_format) - {"type", "json_schema"}:
            raise ValueError("provider response format has unsupported fields")
        if response_format.get("type") == "json_schema":
            schema = response_format.get("json_schema")
            if not isinstance(schema, dict) or set(schema) - {"name", "description", "schema", "strict"}:
                raise ValueError("provider structured-output schema is unsupported")
            if not isinstance(schema.get("schema"), dict):
                raise ValueError("provider structured-output schema is incomplete")
        doc["response_format"] = response_format
    for key, value in body.items():
        if key == "stop" and value is not None and not (
            isinstance(value, str) or isinstance(value, list) and all(isinstance(item, str) for item in value)
        ):
            raise ValueError("provider stop control is invalid")
        if key in {"user", "service_tier"} and value is not None and not isinstance(value, str):
            raise ValueError(f"provider {key} control is invalid")
        if key in {"temperature", "top_p", "presence_penalty", "frequency_penalty"} and value is not None:
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"provider {key} control is invalid")
        if key in {"n", "seed", "top_logprobs"} and value is not None and type(value) is not int:
            raise ValueError(f"provider {key} control is invalid")
        if key in {"stream", "parallel_tool_calls", "logprobs", "store"} and value is not None and type(value) is not bool:
            raise ValueError(f"provider {key} control is invalid")
        if key == "stream_options" and value is not None and (
            not isinstance(value, dict) or set(value) - {"include_usage"}
            or "include_usage" in value and type(value["include_usage"]) is not bool
        ):
            raise ValueError("provider stream options are unsupported")
    # These controls affect generation or transport, not the input token document. Their names and values are
    # nevertheless constrained by the fake chat API and are never silently ignored as unknown fields.
    for key in ("max_tokens", "max_completion_tokens"):
        if key in body and (type(body[key]) is not int or body[key] < 1):
            raise ValueError("provider output token ceiling is invalid")
    return _token_json(doc)


def measure_test_request(
    raw_body: bytes,
    body: Mapping[str, object],
    profile: ProviderExecutionProfile,
    requested_output_tokens: int,
) -> ProviderRequestMeasurement:
    # The actual table is checked when the gateway profile is constructed. Recheck that
    # static assets and renderer have not changed at request time.
    _encoding()
    if profile.verification_state != "TEST_CONTRACT_VERIFIED" or not profile.test_only:
        raise ValueError("request profile is not a verified test contract")
    rendered = render_test_chat_input(body)
    input_tokens = len(_encoding().encode(rendered, disallowed_special=()))
    now = datetime.now(UTC)
    return ProviderRequestMeasurement(
        request_digest=_sha256(raw_body), profile_digest=profile.content_digest,
        tokenizer_identity=f"tiktoken/{version('tiktoken')}:{profile.tokenizer_encoding}:{profile.tokenizer_asset_sha256}",
        renderer_identity=f"{profile.renderer_id}/{profile.renderer_revision}:{profile.renderer_sha256}",
        pricing_version=profile.pricing_version, pricing_digest=profile.pricing_digest,
        usage_semantics=profile.usage_semantics,
        measured_input_tokens=input_tokens, requested_output_tokens=requested_output_tokens,
        context_window_tokens=profile.context_window_tokens,
        additional_reserved_tokens=profile.additional_reserved_tokens,
        verification_state="TEST_CONTRACT_VERIFIED", measured_at=now,
    )


def validate_measurement(measurement: ProviderRequestMeasurement, profile: ProviderExecutionProfile,
                         *, plan_max_input: int, envelope_max_input: int, plan_max_output: int,
                         envelope_max_output: int) -> None:
    count = measurement.measured_input_tokens
    if count > min(profile.max_input_tokens, plan_max_input, envelope_max_input):
        raise ValueError("measured provider input exceeds an admitted input-token ceiling")
    output = measurement.requested_output_tokens
    if output > min(profile.max_output_tokens, plan_max_output, envelope_max_output):
        raise ValueError("provider output ceiling exceeds an admitted limit")
    if count + output + profile.additional_reserved_tokens > profile.context_window_tokens:
        raise ValueError("provider request exceeds the fixed context window")
