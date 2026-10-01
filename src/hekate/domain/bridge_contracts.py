from __future__ import annotations

from typing import Annotated, Literal, TypeAlias

from pydantic import Field, TypeAdapter

from .models import ContractModel


class BridgeBinding(ContractModel):
    task_id: str
    attempt_id: str
    agent_registry_id: str
    provider_agent_id: str
    input_revision: int = Field(ge=0)
    fence: int = Field(ge=0)


class BridgeSessionBinding(BridgeBinding):
    conversation_id: str


class BridgeCommandBase(ContractModel):
    schema_version: Literal["1"]
    request_id: str = Field(min_length=1, max_length=128)
    operation_id: str = Field(min_length=1, max_length=256)


class HelloCommand(BridgeCommandBase):
    command: Literal["hello"]


class AgentCreateCommand(BridgeCommandBase):
    command: Literal["agent.create"]
    owner: str = Field(min_length=1, max_length=128)
    creation_tag: str = Field(min_length=1, max_length=128)
    role: Literal["hekate", "critic"] = "hekate"
    model: str | None = None
    max_input_tokens: int | None = Field(default=None, ge=1)
    max_output_tokens: int | None = Field(default=None, ge=1)


class AgentGetCommand(BridgeCommandBase):
    command: Literal["agent.get"]
    provider_agent_id: str = Field(min_length=1, max_length=256)


class AgentListCommand(BridgeCommandBase):
    command: Literal["agent.list"]
    owner: str = Field(min_length=1, max_length=128)
    creation_tag: str | None = None


class AgentDeleteCommand(BridgeCommandBase):
    command: Literal["agent.delete"]
    provider_agent_id: str = Field(min_length=1, max_length=256)


class SessionPrepareCommand(BridgeCommandBase):
    command: Literal["session.prepare"]
    binding: BridgeBinding


class SessionTurnCommand(BridgeCommandBase):
    command: Literal["session.turn"]
    binding: BridgeSessionBinding
    message: str = Field(min_length=1, max_length=65_536)


class EventsCollectCommand(BridgeCommandBase):
    command: Literal["events.collect"]
    binding: BridgeSessionBinding
    wait_ms: int = Field(default=0, ge=0, le=5_000)


BridgeCommand: TypeAlias = Annotated[
    HelloCommand
    | AgentCreateCommand
    | AgentGetCommand
    | AgentListCommand
    | AgentDeleteCommand
    | SessionPrepareCommand
    | SessionTurnCommand
    | EventsCollectCommand,
    Field(discriminator="command"),
]


class CapabilitiesResult(ContractModel):
    kind: Literal["capabilities"]
    sdk_version: str
    backend: Literal["remote"]
    protocol_version: str
    capabilities: dict[str, bool]
    limitations: tuple[str, ...]


class AgentResult(ContractModel):
    kind: Literal["agent"]
    provider_agent_id: str
    present: bool
    owner: str | None = None
    creation_tag: str | None = None
    tools: tuple[str, ...] = ()
    model: str | None = None
    model_settings: dict[str, object] = Field(default_factory=dict)


class AgentListResult(ContractModel):
    kind: Literal["agents"]
    provider_agent_ids: tuple[str, ...]


class SessionResult(ContractModel):
    kind: Literal["session"]
    provider_agent_id: str
    conversation_id: str
    model: str | None = None
    agent_tools: tuple[str, ...] = ()
    tool_executor_calls: int = Field(default=0, ge=0)
    blocked_tool_attempts: int = Field(default=0, ge=0)
    app_server_info: dict[str, object]


class TurnResult(ContractModel):
    kind: Literal["turn"]
    state: Literal["DISPATCHED", "COMPLETED", "FAILED", "UNKNOWN"]


class BridgeUsage(ContractModel):
    completeness: Literal["UNKNOWN", "PARTIAL", "COMPLETE"]
    source: Literal["runtime_reported", "provider_reported", "runtime_estimated"] | None = None
    accounting_call_id: str | None = None
    provider_call_id: str | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    cost_usd: str | None = None


SdkErrorCode: TypeAlias = Literal[
    "approval_conflict",
    "approval_conflict_terminal",
    "protocol_error",
    "error",
    "llm_api_error",
    "max_steps",
    "interrupted",
    "stream_closed",
    "structured_output_error",
]


class BridgeEvent(ContractModel):
    schema_version: Literal["1"]
    event_id: str = Field(min_length=1)
    operation_id: str = Field(min_length=1)
    binding: BridgeSessionBinding
    event_type: str = Field(min_length=1)
    usage: BridgeUsage
    error_code: SdkErrorCode | None = None
    success: bool | None = None


class EventsResult(ContractModel):
    kind: Literal["events"]
    state: Literal["RUNNING", "COMPLETE", "FAILED", "UNKNOWN", "EMPTY"]
    events: tuple[BridgeEvent, ...] = Field(max_length=1_000)
    tool_executor_calls: int = Field(default=0, ge=0)
    blocked_tool_attempts: int = Field(default=0, ge=0)


BridgeResult: TypeAlias = Annotated[
    CapabilitiesResult | AgentResult | AgentListResult | SessionResult | TurnResult | EventsResult,
    Field(discriminator="kind"),
]


class BridgeReply(ContractModel):
    schema_version: Literal["1"]
    request_id: str = Field(min_length=1, max_length=128)
    operation_id: str = Field(min_length=1, max_length=256)
    command: str
    status: Literal["CONFIRMED", "REJECTED", "UNKNOWN"]
    result: BridgeResult | None = None
    error: str | None = None


def bridge_schema() -> dict[str, object]:
    schemas = {
        "BridgeCommand": TypeAdapter(BridgeCommand).json_schema(),
        "BridgeReply": BridgeReply.model_json_schema(),
        "BridgeEvent": BridgeEvent.model_json_schema(),
    }
    definitions: dict[str, object] = {}
    for name, schema in schemas.items():
        schema_definitions = schema.pop("$defs", {})
        for nested_name, nested_schema in schema_definitions.items():
            definitions.setdefault(nested_name, nested_schema)
        definitions[name] = schema
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "urn:hekate:bridge:v1",
        "$defs": definitions,
        "oneOf": [
            {"$ref": "#/$defs/BridgeCommand"},
            {"$ref": "#/$defs/BridgeReply"},
            {"$ref": "#/$defs/BridgeEvent"},
        ],
        "$comment": "Private JSONL bridge contract. Frames are limited to 1 MiB by the transport.",
    }


BRIDGE_COMMAND_ADAPTER = TypeAdapter(BridgeCommand)
BRIDGE_REPLY_ADAPTER = TypeAdapter(BridgeReply)
