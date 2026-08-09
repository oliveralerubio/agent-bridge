import tempfile
import unittest
from pathlib import Path

from agent_bridge.protocol import MAX_MESSAGE_CHARS, format_injection, validate_body
from agent_bridge.store import Store


class StoreProtocolTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.tempdir.name) / "bridge.sqlite3")
        self.sender_run = self.store.create_run(
            name="worker-a",
            agent="custom",
            mode="interactive",
            command="example-agent",
            cwd="/tmp/project",
            tmux_session="bridge-worker-a",
        )
        self.recipient = self.store.create_run(
            name="worker-b",
            agent="custom",
            mode="interactive",
            command="example-agent",
            cwd="/tmp/project",
            tmux_session="bridge-worker-b",
        )

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_run_can_be_resolved_by_id_or_name(self) -> None:
        self.assertEqual(self.store.get_run(self.sender_run.id).id, self.sender_run.id)
        self.assertEqual(self.store.get_run("worker-a").id, self.sender_run.id)

    def test_message_is_idempotent_and_can_be_acknowledged(self) -> None:
        first = self.store.create_message(
            from_run_id=self.sender_run.id,
            to_run_id=self.recipient.id,
            body="The schema field is now display_name.",
            idempotency_key="rename-1",
        )
        duplicate = self.store.create_message(
            from_run_id=self.sender_run.id,
            to_run_id=self.recipient.id,
            body="The schema field is now display_name.",
            idempotency_key="rename-1",
        )
        self.assertEqual(first.id, duplicate.id)
        self.assertEqual(len(self.store.list_messages(self.recipient.id)), 1)

        delivered = self.store.mark_delivered(first.id)
        self.assertEqual(delivered.status, "delivered")
        acknowledged = self.store.acknowledge(first.id, self.recipient.id)
        self.assertEqual(acknowledged.status, "acknowledged")
        self.assertEqual(self.store.list_messages(self.recipient.id, pending_only=True), [])

    def test_idempotency_key_cannot_change_message_meaning(self) -> None:
        self.store.create_message(
            from_run_id=self.sender_run.id,
            to_run_id=self.recipient.id,
            body="first meaning",
            idempotency_key="same-key",
        )
        with self.assertRaises(ValueError):
            self.store.create_message(
                from_run_id=self.sender_run.id,
                to_run_id=self.recipient.id,
                body="different meaning",
                idempotency_key="same-key",
            )

    def test_only_recipient_can_acknowledge(self) -> None:
        message = self.store.create_message(
            from_run_id=self.sender_run.id,
            to_run_id=self.recipient.id,
            body="verify this handoff",
        )
        self.store.mark_delivered(message.id)
        with self.assertRaises(ValueError):
            self.store.acknowledge(message.id, self.sender_run.id)

        with self.assertRaises(ValueError):
            validate_body("x" * (MAX_MESSAGE_CHARS + 1))
        with self.assertRaises(ValueError):
            validate_body("safe\x00unsafe")
        with self.assertRaises(ValueError):
            validate_body("unsafe\x1b[31m")

    def test_injection_envelope_contains_metadata_without_history(self) -> None:
        message = self.store.create_message(
            from_run_id=self.sender_run.id,
            to_run_id=self.recipient.id,
            body="Only this bounded handoff should be delivered.",
        )
        envelope = format_injection(message, self.sender_run, self.recipient)
        self.assertIn(message.id, envelope)
        self.assertIn("worker-a", envelope)
        self.assertIn("Only this bounded handoff", envelope)
        self.assertNotIn("runs.sqlite3", envelope)
        self.assertNotIn("conversation history", envelope.lower())


if __name__ == "__main__":
    unittest.main()
