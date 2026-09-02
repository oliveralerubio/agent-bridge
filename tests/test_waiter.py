from __future__ import annotations

import sys
import tempfile
import threading
import time
import subprocess
import unittest
from pathlib import Path

from agent_bridge.bridge import Bridge
from agent_bridge.store import Store
from agent_bridge.waiter import CompletionWaiter, WaitResult, completion_body


class CompletionWaiterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.store = Store(self.root / "bridge.sqlite3")
        self.sender = self.store.create_run(
            name="worker",
            agent="custom",
            mode="one-shot",
            command="worker",
            cwd=str(self.root),
        )
        self.receiver = self.store.create_run(
            name="orchestrator",
            agent="custom",
            mode="one-shot",
            command="orchestrator",
            cwd=str(self.root),
        )
        self.store.update_run(self.sender.id, status="running")
        self.store.update_run(self.receiver.id, status="running")

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_completion_event_wakes_waiter_without_deadline_polling(self) -> None:
        waiter = CompletionWaiter(self.store)
        result_holder: list[WaitResult] = []

        def wait_for_completion() -> None:
            result_holder.append(
                waiter.wait(
                    waiter_run_id=self.receiver.id,
                    target_run_id=self.sender.id,
                    timeout=10,
                    heartbeat_timeout=5,
                )
            )

        started = time.monotonic()
        thread = threading.Thread(target=wait_for_completion)
        thread.start()
        deadline = time.monotonic() + 3
        while not Path(self.receiver.inbox_path or "").exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertTrue(Path(self.receiver.inbox_path or "").exists())

        message = self.store.create_message(
            from_run_id=self.sender.id,
            to_run_id=self.receiver.id,
            body=completion_body(
                run_id=self.sender.id,
                status="success",
                summary="worker verified its result",
            ),
        )
        delivered = Bridge(self.store).deliver(message)
        thread.join(timeout=3)

        self.assertFalse(thread.is_alive())
        self.assertLess(time.monotonic() - started, 3)
        result = result_holder[0]
        self.assertEqual(result.status, "completed")
        self.assertEqual(result.message_id, message.id)
        self.assertEqual(self.store.get_message(message.id).status, "acknowledged")
        self.assertEqual(delivered.transport, "socket")

    def test_completion_event_triggers_next_step_without_parent_polling(self) -> None:
        marker = self.root / "next-step"
        next_step = [
            sys.executable,
            "-c",
            "from pathlib import Path; import os; Path(__import__('sys').argv[1]).write_text(os.environ['AGENT_BRIDGE_COMPLETION_STATUS'])",
            str(marker),
        ]
        result_holder: list[WaitResult] = []

        def wait_for_completion() -> None:
            result_holder.append(
                CompletionWaiter(self.store).wait(
                    waiter_run_id=self.receiver.id,
                    target_run_id=self.sender.id,
                    timeout=10,
                    heartbeat_timeout=5,
                    success_command=next_step,
                    success_timeout=2,
                )
            )

        thread = threading.Thread(target=wait_for_completion)
        thread.start()
        deadline = time.monotonic() + 3
        while not Path(self.receiver.inbox_path or "").exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        message = self.store.create_message(
            from_run_id=self.sender.id,
            to_run_id=self.receiver.id,
            body=completion_body(run_id=self.sender.id, status="success", summary="ready"),
        )
        Bridge(self.store).deliver(message)
        thread.join(timeout=3)

        self.assertFalse(thread.is_alive())
        self.assertEqual(result_holder[0].status, "completed")
        self.assertEqual(result_holder[0].trigger_status, "completed")
        self.assertEqual(marker.read_text(), "success")

    def test_dead_owned_process_is_detected_and_falls_back(self) -> None:
        process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(0.15)"], start_new_session=True)
        try:
            self.store.set_process(self.sender.id, process.pid)
            process.wait(timeout=2)
            marker = self.root / "dead-process-fallback"
            fallback = [sys.executable, "-c", "from pathlib import Path; Path(__import__('sys').argv[1]).write_text('fallback')", str(marker)]

            result = CompletionWaiter(self.store).wait(
                waiter_run_id=self.receiver.id,
                target_run_id=self.sender.id,
                timeout=5,
                heartbeat_timeout=5,
                poll_interval=0.05,
                fallback_command=fallback,
                fallback_timeout=2,
            )

            self.assertEqual(result.status, "failed")
            self.assertEqual(result.fallback_status, "completed")
            self.assertEqual(marker.read_text(), "fallback")
        finally:
            if process.poll() is None:
                process.kill()

    def test_stale_heartbeat_triggers_fallback(self) -> None:
        marker = self.root / "heartbeat-fallback"
        fallback = [
            sys.executable,
            "-c",
            "from pathlib import Path; Path(__import__('sys').argv[1]).write_text('heartbeat expired')",
            str(marker),
        ]
        result = CompletionWaiter(self.store).wait(
            waiter_run_id=self.receiver.id,
            target_run_id=self.sender.id,
            timeout=5,
            heartbeat_timeout=0.05,
            poll_interval=0.02,
            fallback_command=fallback,
            fallback_timeout=2,
        )

        self.assertEqual(result.status, "failed")
        self.assertEqual(result.fallback_status, "completed")
        self.assertIn("heartbeat", result.error or "")
        self.assertEqual(marker.read_text(), "heartbeat expired")

    def test_failed_agent_triggers_bounded_fallback_without_false_success(self) -> None:
        self.store.update_run(
            self.sender.id,
            status="failed",
            lifecycle_state="failed",
            failure_reason="provider exited",
        )
        marker = self.root / "fallback-ran"
        fallback = [
            sys.executable,
            "-c",
            "from pathlib import Path; Path(__import__('sys').argv[1]).write_text('fallback')",
            str(marker),
        ]

        result = CompletionWaiter(self.store).wait(
            waiter_run_id=self.receiver.id,
            target_run_id=self.sender.id,
            timeout=10,
            heartbeat_timeout=5,
            fallback_command=fallback,
            fallback_timeout=2,
        )

        self.assertEqual(result.status, "failed")
        self.assertEqual(result.fallback_status, "completed")
        self.assertEqual(result.error, "provider exited")
        self.assertEqual(marker.read_text(), "fallback")
        self.assertNotEqual(result.status, "completed")

    def test_deadline_and_fallback_timeout_are_both_bounded(self) -> None:
        started = time.monotonic()
        result = CompletionWaiter(self.store).wait(
            waiter_run_id=self.receiver.id,
            target_run_id=None,
            timeout=0.25,
            heartbeat_timeout=0,
            fallback_command=[sys.executable, "-c", "import time; time.sleep(2)"],
            fallback_timeout=0.1,
        )

        self.assertLess(time.monotonic() - started, 2)
        self.assertEqual(result.status, "timeout")
        self.assertEqual(result.fallback_status, "timeout")
        self.assertIn("deadline", result.error or "")

    def test_success_exit_without_completion_event_becomes_partial(self) -> None:
        self.store.update_run(self.sender.id, status="success", lifecycle_state="completed")

        result = CompletionWaiter(self.store).wait(
            waiter_run_id=self.receiver.id,
            target_run_id=self.sender.id,
            timeout=0.2,
            heartbeat_timeout=0,
        )

        self.assertEqual(result.status, "partial")
        self.assertIn("completion", result.error or "")


if __name__ == "__main__":
    unittest.main()
