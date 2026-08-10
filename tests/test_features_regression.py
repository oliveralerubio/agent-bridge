import json
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

from agent_bridge.bridge import Bridge
from agent_bridge.lifecycle import TeamLifecycle
from agent_bridge.protocol import MAX_ADAPTER_CAPABILITIES, validate_adapter_frame
from agent_bridge.reports import MAX_REPORT_BYTES, load_report_file
from agent_bridge.socket_transport import UnixSocketTransport
from agent_bridge.store import Store
from agent_bridge.teams import load_manifest


class FeatureRegressionTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.path = Path(self.tempdir.name) / "bridge.sqlite3"
        self.store = Store(self.path)
        self.sender = self.store.create_run(name="sender", agent="x", mode="interactive", command="x", cwd=self.tempdir.name)
        self.recipient = self.store.create_run(
            name="recipient", agent="x", mode="interactive", command="x", cwd=self.tempdir.name,
            readiness_required=True,
        )
        self.store.update_run(self.sender.id, status="running")
        self.store.update_run(self.recipient.id, status="running")

    def tearDown(self):
        self.tempdir.cleanup()

    def test_adapter_capabilities_and_typed_frame_limits(self):
        with self.assertRaises(ValueError):
            validate_adapter_frame({"type": "hello", "run_id": self.recipient.id})
        with self.assertRaises(ValueError):
            validate_adapter_frame({"type": "hello", "run_id": self.recipient.id, "capabilities": ["x"] * (MAX_ADAPTER_CAPABILITIES + 1)})
        with self.assertRaises(ValueError):
            validate_adapter_frame({"type": "error", "run_id": self.recipient.id, "session_id": "s", "error": "bad\x00error"})

    def test_ready_without_transport_does_not_fake_delivery(self):
        bridge = Bridge(self.store)
        bridge.handle_adapter_frame({"type": "hello", "run_id": self.recipient.id, "session_id": "s"})
        message = self.store.create_message(from_run_id=self.sender.id, to_run_id=self.recipient.id, body="must remain queued")
        result = bridge.handle_adapter_frame({"type": "ready", "run_id": self.recipient.id, "session_id": "s"})
        self.assertEqual(result.deliveries[0].transport, "queued")
        self.assertEqual(self.store.get_message(message.id).status, "queued")

    def test_real_socket_handshake_delivers_after_ready(self):
        received = []
        transport = UnixSocketTransport()
        listener = threading.Thread(
            target=transport.listen,
            kwargs={"path": self.recipient.inbox_path, "on_message": received.append, "once": True, "timeout": 5},
            daemon=True,
        )
        listener.start()
        deadline = time.monotonic() + 2
        while not Path(self.recipient.inbox_path).exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        bridge = Bridge(self.store, socket=transport)
        bridge.handle_adapter_frame({"type": "hello", "run_id": self.recipient.id, "session_id": "s", "capabilities": ["messages"]})
        message = self.store.create_message(from_run_id=self.sender.id, to_run_id=self.recipient.id, body="socket handoff")
        self.assertEqual(bridge.deliver(message).message.status, "queued")
        result = bridge.handle_adapter_frame({"type": "ready", "run_id": self.recipient.id, "session_id": "s"})
        listener.join(3)
        self.assertFalse(listener.is_alive())
        self.assertEqual(result.deliveries[0].transport, "socket")
        self.assertEqual(received[0]["body"], "socket handoff")
        self.assertEqual(self.store.get_message(message.id).status, "delivered")

    def test_team_direct_lifecycle_preserves_membership_history(self):
        manifest = load_manifest(json.dumps({
            "name": "local-team",
            "members": [{
                "name": "dummy",
                "agent": "custom",
                "command": f'{sys.executable} -c "import time; time.sleep(20)"',
                "cwd": self.tempdir.name,
                "role": "worker",
                "restart_policy": "never",
            }],
        }))
        lifecycle = TeamLifecycle(self.store)
        team = lifecycle.create_from_manifest(manifest)
        started = lifecycle.start(team.id)
        self.assertEqual(started["team"].status, "running")
        first_run_id = started["members"][0].run.id
        restarted = lifecycle.restart(team.id)
        self.assertEqual(restarted["members"][0].run.id, first_run_id)
        self.assertEqual(restarted["members"][0].run.restart_count, 1)
        self.assertEqual(len(self.store.list_team_members(team.id, active_only=False)), 1)
        stopped = lifecycle.stop(team.id)
        self.assertEqual(stopped["team"].status, "stopped")

    def test_approval_hook_fails_closed_and_is_recorded(self):
        team = self.store.create_team(name="governed")
        self.store.add_team_member(team.id, self.sender.id, role="lead", is_lead=True)
        self.store.add_team_member(team.id, self.recipient.id)
        self.store.add_hook(
            name="deny-approval", event="task.approved",
            command=[sys.executable, "-c", "import sys; print('denied' * 10000); sys.exit(1)"],
            timeout=2, max_output=128,
        )
        task = self.store.create_task(title="gated", created_by=self.sender.id, requires_approval=True)
        with self.assertRaises(ValueError):
            self.store.approve_task(task.id, self.sender.id)
        self.assertEqual(self.store.get_task(task.id).status, "awaiting_approval")
        events = self.store.list_hook_events()
        self.assertEqual(events[0].status, "denied")
        self.assertLessEqual(len(events[0].output), 128)

    def test_report_is_bounded_and_survives_reopen_on_rejected_task(self):
        task = self.store.create_task(title="reject me", created_by=self.sender.id, requires_approval=True)
        self.store.grant_operator(self.sender.id)
        self.store.reject_task(task.id, self.sender.id, "not now")
        report = {"goal": "document rejection", "verified_facts": [], "tests": [], "files_changed": [], "blockers": ["blocked"], "next_action": "retry"}
        self.store.add_task_report(task.id, self.sender.id, report)
        reopened = Store(self.path)
        self.assertEqual(reopened.get_task_reports(task.id)[0].next_action, "retry")
        oversized = Path(self.tempdir.name) / "oversized.json"
        oversized.write_bytes(b"{" + b"x" * MAX_REPORT_BYTES + b"}")
        with self.assertRaises(ValueError):
            load_report_file(oversized)


if __name__ == "__main__":
    unittest.main()
