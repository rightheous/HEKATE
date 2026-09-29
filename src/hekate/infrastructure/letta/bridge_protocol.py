from __future__ import annotations

from collections.abc import AsyncIterator

from hekate.domain.models import BridgeFrame, BridgeReply, Command, RuntimeEvent
from hekate.domain.types import Instant


def encode_command(command: Command) -> bytes:
    raise NotImplementedError


def decode_frame(frame: bytes) -> BridgeFrame:
    raise NotImplementedError


async def request(command: Command, deadline: Instant) -> BridgeReply:
    raise NotImplementedError


async def receive_events() -> AsyncIterator[RuntimeEvent]:
    raise NotImplementedError


async def reply_tool(request_id: str, result: object) -> None:
    raise NotImplementedError
