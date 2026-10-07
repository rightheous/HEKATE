import asyncio
from pathlib import Path
import signal
import socket
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from hekate import __main__ as cli


class _Process:
    def __init__(self, pid: int, exit_code: int | None = None) -> None:
        self.pid = pid
        self.returncode = None
        self.exit_code = exit_code
        self.terminated = False
        self.finished = asyncio.Event()

    async def wait(self) -> int:
        if self.exit_code is not None:
            self.returncode = self.exit_code
            return self.exit_code
        await self.finished.wait()
        return int(self.returncode or 0)

    def terminate(self) -> None:
        self.terminated = True
        self.returncode = -signal.SIGTERM
        self.finished.set()

    def kill(self) -> None:
        self.returncode = -signal.SIGKILL
        self.finished.set()


class PersonalRunOwnershipTests(TestCase):
    def _fixtures(self):
        with socket.socket() as stream:
            stream.bind(("127.0.0.1", 0))
            port = stream.getsockname()[1]
        settings = SimpleNamespace(
            runtime_mode="test", local={"gateway": {"host": "127.0.0.1", "port": port}},
            project_dir=Path.cwd(),
        )

        async def doctor(_settings):
            ready = {key: {"status": "READY"} for key in (
                "configuration", "profile_assets", "postgres", "authorization", "node_bridge", "letta_runtime",
            )}
            ready["provider_gateway"] = {"status": "NOT_RUNNING"}
            return {"checks": ready}

        async def connect(_host, _port):
            class Writer:
                def close(self):
                    pass

                async def wait_closed(self):
                    pass

            return object(), Writer()

        common = (
            patch.object(cli, "_settings", return_value=settings),
            patch.object(cli, "run_doctor", side_effect=doctor),
            patch("hekate.settings.validate_settings"),
            patch("asyncio.open_connection", side_effect=connect),
        )
        return common

    def test_worker_start_failure_reaps_gateway_started_by_this_run(self):
        gateway = _Process(101)
        calls = 0

        async def create(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                return gateway
            raise OSError("worker spawn failed")

        fixtures = self._fixtures()
        with fixtures[0], fixtures[1], fixtures[2], fixtures[3], \
                patch("asyncio.create_subprocess_exec", side_effect=create):
            with self.assertRaisesRegex(OSError, "worker spawn failed"):
                asyncio.run(cli._run_command())
        self.assertTrue(gateway.terminated)
        self.assertEqual(gateway.returncode, -signal.SIGTERM)

    def test_unexpected_child_exit_reaps_other_managed_child(self):
        gateway, worker = _Process(102), _Process(103, exit_code=23)
        calls = 0

        async def create(*args, **kwargs):
            nonlocal calls
            calls += 1
            return gateway if calls == 1 else worker

        fixtures = self._fixtures()
        with fixtures[0], fixtures[1], fixtures[2], fixtures[3], \
                patch("asyncio.create_subprocess_exec", side_effect=create):
            self.assertEqual(asyncio.run(cli._run_command()), 23)
        self.assertTrue(gateway.terminated)
        self.assertEqual(worker.returncode, 23)

    def test_sigint_stops_only_the_children_started_by_run(self):
        gateway, worker = _Process(104), _Process(105)
        handlers = {}
        calls = 0

        def add_signal_handler(_loop, sig, callback, *args):
            handlers[sig] = callback
            return True

        def remove_signal_handler(_loop, sig):
            handlers.pop(sig, None)
            return True

        async def create(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                asyncio.get_running_loop().call_soon(handlers[signal.SIGINT])
                return worker
            return gateway

        fixtures = self._fixtures()
        probe_loop = asyncio.new_event_loop()
        loop_type = type(probe_loop)
        probe_loop.close()
        with fixtures[0], fixtures[1], fixtures[2], fixtures[3], \
                patch.object(loop_type, "add_signal_handler", add_signal_handler), \
                patch.object(loop_type, "remove_signal_handler", remove_signal_handler), \
                patch("asyncio.create_subprocess_exec", side_effect=create):
            self.assertEqual(asyncio.run(cli._run_command()), 0)
        self.assertTrue(gateway.terminated)
        self.assertTrue(worker.terminated)
