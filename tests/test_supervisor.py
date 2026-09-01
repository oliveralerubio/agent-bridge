from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

from agent_bridge.store import Store
from agent_bridge.supervisor import ExecutionSupervisor, load_execution_manifest


class SupervisorContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.store = Store(self.root / "bridge.sqlite3")

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def phase(self, name: str, code: str, *, role: str = "custom", timeout: float = 5) -> dict[str, object]:
        return {
            "name": name,
            "role": role,
            "command": [sys.executable, "-c", code],
            "timeout": timeout,
        }

    def manifest(self, *phases: dict[str, object]) -> object:
        return load_execution_manifest(
            {
                "name": "contract-run",
                "cwd": str(self.root),
                "phases": list(phases),
            }
        )

    def test_phases_run_in_order_and_persist_checkpoints(self) -> None:
        manifest = self.manifest(
            self.phase(
                "scout",
                "import os; print('phase=' + os.environ['AGENT_BRIDGE_PHASE']); print('run=' + os.environ['AGENT_BRIDGE_RUN_ID']); print('AGENT_BRIDGE_AGENT_END phase=scout status=success')",
                role="scout",
            ),
            self.phase(
                "verify",
                "import os; print('phase=' + os.environ['AGENT_BRIDGE_PHASE']); print('AGENT_BRIDGE_AGENT_END phase=verify status=success')",
                role="verification",
            ),
        )

        result = ExecutionSupervisor(self.store).run(manifest)

        self.assertEqual(result.status, "done")
        self.assertEqual([phase.status for phase in result.phases], ["done", "done"])
        self.assertTrue(Path(result.checkpoint_path).is_file())
        checkpoint = json.loads(Path(result.checkpoint_path).read_text())
        self.assertEqual(checkpoint["status"], "done")
        self.assertEqual(checkpoint["completed_phases"], ["scout", "verify"])
        self.assertIn("phase=scout", result.phases[0].output)
        self.assertIn("phase=verify", result.phases[1].output)
        self.assertIsNotNone(result.phases[0].run_id)
        phase_run_id = result.phases[0].run_id
        assert phase_run_id is not None
        self.assertIn(f"run={phase_run_id}", result.phases[0].output)
        self.assertEqual(self.store.get_run(phase_run_id).status, "success")

    def test_zero_exit_without_agent_end_is_partial_not_done(self) -> None:
        manifest = self.manifest(self.phase("writer", "print('changed files but no completion proof')", role="writer"))

        result = ExecutionSupervisor(self.store).run(manifest)

        self.assertEqual(result.status, "partial")
        self.assertEqual(result.phases[0].status, "partial")
        self.assertFalse(result.phases[0].agent_end)

    def test_timeout_is_distinct_and_not_success(self) -> None:
        manifest = self.manifest(self.phase("slow", "import time; time.sleep(30)", timeout=0.2))

        result = ExecutionSupervisor(self.store).run(manifest)

        self.assertEqual(result.status, "timeout")
        self.assertEqual(result.phases[0].status, "timeout")
        self.assertNotEqual(result.status, "done")

    def test_operator_stop_terminates_running_phase(self) -> None:
        manifest = self.manifest(self.phase("slow", "import time; time.sleep(30)", timeout=30))
        supervisor = ExecutionSupervisor(self.store)
        holder: list[object] = []

        def execute() -> None:
            holder.append(supervisor.run(manifest))

        thread = threading.Thread(target=execute)
        thread.start()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                execution = self.store.get_execution("contract-run")
                phases = self.store.list_execution_phases("contract-run")
            except KeyError:
                time.sleep(0.02)
                continue
            if execution["status"] == "running" and phases[0]["run_id"]:
                phase_run = self.store.get_run(phases[0]["run_id"])
                if phase_run.process_pid:
                    break
            time.sleep(0.02)
        else:
            self.fail("supervisor did not expose a running phase process")

        stopped = supervisor.stop("contract-run")
        thread.join(timeout=5)

        self.assertFalse(thread.is_alive())
        self.assertEqual(stopped.status, "failed")
        self.assertEqual(self.store.get_execution("contract-run")["status"], "failed")

    def test_resume_reuses_execution_and_skips_completed_phase(self) -> None:
        marker = self.root / "marker"
        script = self.root / "phase.py"
        script.write_text(
            "import pathlib, sys\n"
            "marker = pathlib.Path(sys.argv[2])\n"
            "if sys.argv[1] == 'scout':\n"
            "    marker.write_text(marker.read_text() + 'scout\\n' if marker.exists() else 'scout\\n')\n"
            "    print('AGENT_BRIDGE_AGENT_END phase=scout status=success')\n"
            "else:\n"
            "    if not marker.exists() or 'scout' not in marker.read_text(): raise SystemExit(2)\n"
            "    print('AGENT_BRIDGE_AGENT_END phase=writer status=success')\n"
        )
        manifest = load_execution_manifest(
            {
                "name": "resume-run",
                "cwd": str(self.root),
                "phases": [
                    {"name": "scout", "role": "scout", "command": [sys.executable, str(script), "scout", str(marker)]},
                    {"name": "writer", "role": "writer", "command": [sys.executable, str(script), "writer", str(marker)]},
                ],
            }
        )
        first = ExecutionSupervisor(self.store).run(manifest)
        self.assertEqual(first.status, "done")
        self.assertEqual(marker.read_text().splitlines(), ["scout"])

        resumed = ExecutionSupervisor(self.store).run(manifest, resume=True)

        self.assertEqual(resumed.id, first.id)
        self.assertEqual(resumed.status, "done")
        self.assertEqual(marker.read_text().splitlines(), ["scout"])

    def test_manifest_requires_argv_and_only_one_writer(self) -> None:
        with self.assertRaises(ValueError):
            load_execution_manifest(
                {
                    "name": "shell-rejected",
                    "cwd": str(self.root),
                    "phases": [{"name": "x", "command": "sh -c dangerous"}],
                }
            )
        with self.assertRaises(ValueError):
            load_execution_manifest(
                {
                    "name": "two-writers",
                    "cwd": str(self.root),
                    "phases": [
                        self.phase("one", "print('AGENT_BRIDGE_AGENT_END phase=one status=success')", role="writer"),
                        self.phase("two", "print('AGENT_BRIDGE_AGENT_END phase=two status=success')", role="writer"),
                    ],
                }
            )

    def test_resume_rejects_changed_manifest(self) -> None:
        original = self.manifest(self.phase("one", "print('AGENT_BRIDGE_AGENT_END phase=one status=success')"))
        first = ExecutionSupervisor(self.store).run(original)
        changed = self.manifest(self.phase("one", "print('different')"))

        with self.assertRaises(ValueError):
            ExecutionSupervisor(self.store).run(changed, resume=True)

        self.assertEqual(self.store.get_execution(first.id)["status"], "done")


if __name__ == "__main__":
    unittest.main()
