from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from uuid import uuid4

from pydantic import ValidationError

from hekate.domain.bridge_contracts import (
    BRIDGE_REPLY_ADAPTER, MAX_BRIDGE_FRAME_BYTES, BridgeCommand, BridgeReply,
    encode_bridge_command_frame,
)

MAX_FRAME_BYTES = MAX_BRIDGE_FRAME_BYTES


def _pairs_no_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def encode_command(command: BridgeCommand) -> bytes:
    return encode_bridge_command_frame(command)


def decode_frame(frame: bytes) -> BridgeReply:
    if len(frame) > MAX_FRAME_BYTES:
        raise ValueError("bridge frame exceeds 1 MiB")
    try:
        frame.decode("utf-8", errors="strict")
        json.loads(frame, object_pairs_hook=_pairs_no_duplicates)
        return BRIDGE_REPLY_ADAPTER.validate_json(frame, strict=True)
    except ValidationError as error:
        issues = error.errors(include_input=False)
        detail = "; ".join(f"{issue['loc']}: {issue['msg']}" for issue in issues[:4])
        raise ValueError(f"invalid bridge reply: {detail}") from error
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise ValueError("invalid bridge reply") from error


class BridgeClient:
    """Private JSONL subprocess client. A timed out request poisons the stream."""

    def __init__(
        self,
        node_bin: str,
        bridge_entry: Path,
        env: dict[str, str] | None = None,
        *,
        max_pending: int = 16,
    ) -> None:
        self.node_bin = node_bin
        self.bridge_entry = bridge_entry
        self.env = env
        self.max_pending = max_pending
        self.process: asyncio.subprocess.Process | None = None
        self._pending: dict[str, asyncio.Future[BridgeReply]] = {}
        self._slots = asyncio.Semaphore(max_pending)
        self._write_lock = asyncio.Lock()
        self._reader_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._stderr_tail = bytearray()

    async def start(self) -> None:
        if self.process is not None:
            if self.process.returncode is not None or (self._reader_task and self._reader_task.done()):
                raise RuntimeError("bridge process is no longer usable; create a new worker process")
            return
        environment = os.environ.copy()
        if self.env:
            environment.update(self.env)
        self.process = await asyncio.create_subprocess_exec(
            self.node_bin,
            str(self.bridge_entry),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=environment,
            limit=MAX_FRAME_BYTES + 1,
        )
        self._reader_task = asyncio.create_task(self._read_replies())
        self._stderr_task = asyncio.create_task(self._drain_stderr())

    async def _drain_stderr(self) -> None:
        assert self.process is not None and self.process.stderr is not None
        while chunk := await self.process.stderr.read(8192):
            self._stderr_tail.extend(chunk)
            del self._stderr_tail[:-32_768]

    @property
    def stderr_text(self) -> str:
        return self._stderr_tail.decode("utf-8", errors="replace")

    async def _fail_all(self, error: BaseException) -> None:
        for future in tuple(self._pending.values()):
            if not future.done():
                future.set_exception(error)
        self._pending.clear()

    async def _read_replies(self) -> None:
        assert self.process is not None and self.process.stdout is not None
        try:
            while True:
                frame = await self.process.stdout.readline()
                if not frame:
                    raise EOFError("bridge stdout closed")
                if not frame.endswith(b"\n") or len(frame) - 1 > MAX_FRAME_BYTES:
                    raise ValueError("invalid or oversized bridge frame")
                reply = decode_frame(frame[:-1])
                future = self._pending.pop(reply.request_id, None)
                if future is None:
                    raise ValueError("bridge reply has no pending request")
                future.set_result(reply)
        except BaseException as error:
            await self._fail_all(error)

    async def request(self, command: BridgeCommand, timeout: float = 15) -> BridgeReply:
        if timeout <= 0:
            raise ValueError("bridge timeout must be positive")
        frame = encode_command(command)
        await self.start()
        assert self.process is not None and self.process.stdin is not None
        await self._slots.acquire()
        if command.request_id in self._pending:
            self._slots.release()
            raise ValueError("bridge request id is already pending")
        future: asyncio.Future[BridgeReply] = asyncio.get_running_loop().create_future()
        self._pending[command.request_id] = future
        sent = False
        try:
            async with self._write_lock:
                self.process.stdin.write(frame)
                sent = True
                await self.process.stdin.drain()
            try:
                reply = await asyncio.wait_for(future, timeout)
            except TimeoutError:
                await self.close()
                raise TimeoutError("bridge request outcome is unknown; stream closed without retry")
            if (reply.operation_id, reply.command) != (command.operation_id, command.command):
                await self.close()
                raise ValueError("bridge reply correlation mismatch")
            return reply
        except asyncio.CancelledError:
            self._pending.pop(command.request_id, None)
            await self.close()
            raise
        except BaseException:
            self._pending.pop(command.request_id, None)
            if sent:
                await self.close()
            raise
        finally:
            self._slots.release()

    async def close(self, grace_seconds: float = 2) -> None:
        process, self.process = self.process, None
        if process is None:
            return
        if process.stdin and not process.stdin.is_closing():
            process.stdin.close()
        try:
            await asyncio.wait_for(process.wait(), grace_seconds)
        except TimeoutError:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), 1)
            except TimeoutError:
                process.kill()
                await process.wait()
        for task in (self._reader_task, self._stderr_task):
            if task and not task.done():
                task.cancel()
        await asyncio.gather(*(task for task in (self._reader_task, self._stderr_task) if task), return_exceptions=True)
        await self._fail_all(EOFError("bridge closed"))
        self._reader_task = self._stderr_task = None


def command_base(operation_id: str, command: str) -> dict[str, str]:
    return {
        "schema_version": "1",
        "request_id": str(uuid4()),
        "operation_id": operation_id,
        "command": command,
    }
