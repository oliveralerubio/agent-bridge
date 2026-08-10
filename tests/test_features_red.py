import json
import os
import stat
import tempfile
import threading
import time
import unittest
from pathlib import Path

from agent_bridge.bridge import Bridge
from agent_bridge.protocol import validate_adapter_frame
from agent_bridge.store import Store
from agent_bridge.teams import load_manifest
from agent_bridge.reports import load_report_file


class FeatureContractTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.tempdir.name) / "bridge.sqlite3")
        self.lead = self.store.create_run(
            name="lead", agent="custom", mode="interactive", command="cat", cwd="."
        )
        self.worker = self.store.create_run(
            name="worker", agent="custom", mode="interactive", command="cat", cwd="."
        )

    def tearDown(self):
        self.tempdir.cleanup()

    def test_adapter_handshake_is_validated_and_gates_delivery(self):
        with self.assertRaises(ValueError):
            validate_adapter_frame({"type": "ready", "run_id": self.worker.id})
        hello = validate_adapter_frame(
            {
                "type": "agent-bridge.hello",
                "run_id": self.worker.id,
                "session_id": "session-1",
                "capabilities": ["messages", "heartbeat"],
            }
        )
        self.assertEqual(hello["type"], "hello")
        message = self.store.create_message(
            from_run_id=self.lead.id, to_run_id=self.worker.id, body="hello adapter"
        )
        bridge = Bridge(self.store)
        self.assertEqual(bridge.deliver(message).message.status, "queued")
        bridge.handle_adapter_frame(hello)
        self.assertEqual(bridge.deliver(message).message.status, "queued")
        deliveries = bridge.handle_adapter_frame(
            {
                "type": "ready",
                "run_id": self.worker.id,
                "session_id": "session-1",
                "capabilities": ["messages", "heartbeat"],
            }
        )
        self.assertTrue(deliveries)
        self.assertEqual(deliveries.deliveries[0].transport, "queued")
        self.assertEqual(self.store.get_message(message.id).status, "queued")
        self.assertEqual(self.store.get_run(self.worker.id).readiness, "ready")

    def test_heartbeat_expiry_and_recovery(self):
        bridge = Bridge(self.store)
        bridge.handle_adapter_frame(
            {"type": "hello", "run_id": self.worker.id, "session_id": "s"}
        )
        bridge.handle_adapter_frame(
            {"type": "ready", "run_id": self.worker.id, "session_id": "s"}
        )
        self.store.expire_adapter_heartbeats(ttl_seconds=0)
        self.assertEqual(self.store.get_run(self.worker.id).readiness, "offline")
        bridge.handle_adapter_frame(
            {"type": "heartbeat", "run_id": self.worker.id, "session_id": "s"}
        )
        bridge.handle_adapter_frame(
            {"type": "ready", "run_id": self.worker.id, "session_id": "s"}
        )
        self.assertEqual(self.store.get_run(self.worker.id).readiness, "ready")

    def test_team_membership_lead_and_governance(self):
        team = self.store.create_team(name="reviewers")
        self.store.add_team_member(team.id, self.lead.id, role="lead", is_lead=True)
        self.store.add_team_member(team.id, self.worker.id, role="member")
        with self.assertRaises(ValueError):
            self.store.add_team_member(team.id, self.worker.id, role="member", is_lead=True)
        task = self.store.create_task(
            title="gated", created_by=self.lead.id, requires_approval=True
        )
        with self.assertRaises(ValueError):
            self.store.claim_task(task.id, self.worker.id)
        approved = self.store.approve_task(task.id, self.lead.id)
        self.assertEqual(approved.status, "approved")
        self.assertEqual(self.store.claim_task(task.id, self.worker.id).status, "in_progress")

    def test_ungrouped_governance_requires_operator(self):
        task = self.store.create_task(
            title="ungrouped gated", created_by=self.lead.id, requires_approval=True
        )
        with self.assertRaises(ValueError):
            self.store.approve_task(task.id, self.lead.id)
        self.store.grant_operator(self.lead.id)
        self.assertEqual(self.store.approve_task(task.id, self.lead.id).status, "approved")

    def test_peer_text_cannot_approve_and_report_is_bounded_and_persisted(self):
        task = self.store.create_task(
            title="gated", created_by=self.lead.id, requires_approval=True
        )
        with self.assertRaises(ValueError):
            self.store.approve_task(task.id, self.worker.id)
        report = {
            "goal": "finish task",
            "verified_facts": ["fact"],
            "tests": ["unit"],
            "files_changed": ["x.py"],
            "blockers": [],
            "next_action": "none",
        }
        self.store.grant_operator(self.lead.id)
        self.store.approve_task(task.id, self.lead.id)
        self.store.claim_task(task.id, self.worker.id)
        self.store.complete_task(task.id, self.worker.id, report=report)
        self.assertEqual(self.store.get_task_reports(task.id)[0].goal, "finish task")
        with self.assertRaises(ValueError):
            self.store.complete_task(task.id, self.worker.id, report={"goal": 1})

    def test_manifest_is_bounded_json(self):
        manifest = load_manifest(
            json.dumps(
                {
                    "name": "demo",
                    "members": [
                        {
                            "name": "one",
                            "agent": "custom",
                            "command": "cat",
                            "cwd": ".",
                            "role": "worker",
                            "startup_timeout": 1,
                            "restart_policy": "never",
                        }
                    ],
                }
            ).encode()
        )
        self.assertEqual(manifest.name, "demo")
        self.assertEqual(manifest.members[0].restart_policy, "never")
        with self.assertRaises(ValueError):
            load_report_file(Path(self.tempdir.name) / "missing.json")


if __name__ == "__main__":
    unittest.main()
