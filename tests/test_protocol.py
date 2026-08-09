import sqlite3
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
        self.sender_two = self.store.create_run(
            name="worker-c",
            agent="custom",
            mode="interactive",
            command="example-agent",
            cwd="/tmp/project",
            tmux_session="bridge-worker-c",
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
        self.assertTrue(self.recipient.inbox_path)

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

    def test_idempotency_key_is_sender_scoped(self) -> None:
        first = self.store.create_message(
            from_run_id=self.sender_run.id,
            to_run_id=self.recipient.id,
            body="same handoff",
            idempotency_key="shared-key",
        )
        second = self.store.create_message(
            from_run_id=self.sender_two.id,
            to_run_id=self.recipient.id,
            body="same handoff",
            idempotency_key="shared-key",
        )
        self.assertNotEqual(first.id, second.id)
        self.assertEqual(first.from_run_id, self.sender_run.id)
        self.assertEqual(second.from_run_id, self.sender_two.id)

    def test_acknowledgement_requires_delivery(self) -> None:
        message = self.store.create_message(
            from_run_id=self.sender_run.id,
            to_run_id=self.recipient.id,
            body="not delivered yet",
        )
        with self.assertRaises(ValueError):
            self.store.acknowledge(message.id, self.recipient.id)

    def test_lifecycle_transitions_do_not_reopen_terminal_delivery(self) -> None:
        message = self.store.create_message(
            from_run_id=self.sender_run.id,
            to_run_id=self.recipient.id,
            body="deliver once",
        )
        delivered = self.store.mark_delivered(message.id)
        self.assertEqual(delivered.status, "delivered")
        with self.assertRaises(ValueError):
            self.store.hold_message(message.id, self.recipient.id)
        with self.assertRaises(ValueError):
            self.store.refuse_message(message.id, self.recipient.id)

        acknowledged = self.store.acknowledge(message.id, self.recipient.id)
        self.assertEqual(acknowledged.status, "acknowledged")
        with self.assertRaises(ValueError):
            self.store.mark_failed(message.id, "late transport failure")
        with self.assertRaises(ValueError):
            self.store.accept_message(message.id, self.recipient.id)

    def test_message_actions_require_the_recipient(self) -> None:
        message = self.store.create_message(
            from_run_id=self.sender_run.id,
            to_run_id=self.recipient.id,
            body="recipient-only control",
        )
        with self.assertRaises(ValueError):
            self.store.hold_message(message.id, self.sender_run.id)
        with self.assertRaises(ValueError):
            self.store.refuse_message(message.id, self.sender_run.id)

    def test_iter_pending_only_returns_redeliverable_messages(self) -> None:
        queued = self.store.create_message(
            from_run_id=self.sender_run.id,
            to_run_id=self.recipient.id,
            body="queued",
        )
        delivered = self.store.create_message(
            from_run_id=self.sender_run.id,
            to_run_id=self.recipient.id,
            body="delivered",
        )
        self.store.mark_delivered(delivered.id)
        held = self.store.create_message(
            from_run_id=self.sender_two.id,
            to_run_id=self.recipient.id,
            body="held",
        )
        self.store.hold_message(held.id, self.recipient.id)

        self.assertEqual([item.id for item in self.store.iter_pending(self.recipient.id)], [queued.id])

    def test_only_recipient_can_acknowledge(self) -> None:
        message = self.store.create_message(
            from_run_id=self.sender_run.id,
            to_run_id=self.recipient.id,
            body="verify this handoff",
        )
        self.store.mark_delivered(message.id)
        with self.assertRaises(ValueError):
            self.store.acknowledge(message.id, self.sender_run.id)

    def test_inbound_policy_and_queue_bound(self) -> None:
        bounded = self.store.create_run(
            name="bounded",
            agent="custom",
            mode="interactive",
            command="example-agent",
            cwd="/tmp/project",
            tmux_session="bridge-bounded",
            max_inbox=1,
        )
        first = self.store.create_message(
            from_run_id=self.sender_run.id,
            to_run_id=bounded.id,
            body="first",
        )
        with self.assertRaises(ValueError):
            self.store.create_message(
                from_run_id=self.sender_run.id,
                to_run_id=bounded.id,
                body="second",
            )

        held = self.store.hold_message(first.id, bounded.id)
        self.assertEqual(held.status, "held")
        accepted = self.store.accept_message(first.id, bounded.id)
        self.assertEqual(accepted.status, "queued")
        refused = self.store.refuse_message(first.id, bounded.id)
        self.assertEqual(refused.status, "refused")

        self.store.set_inbound_policy(bounded.id, "refuse")
        refused_on_create = self.store.create_message(
            from_run_id=self.sender_run.id,
            to_run_id=bounded.id,
            body="policy refuses this",
        )
        self.assertEqual(refused_on_create.status, "refused")

        full_refused = self.store.create_run(
            name="full-refused",
            agent="custom",
            mode="interactive",
            command="example-agent",
            cwd="/tmp/project",
            tmux_session="bridge-full-refused",
            max_inbox=1,
        )
        self.store.create_message(
            from_run_id=self.sender_run.id,
            to_run_id=full_refused.id,
            body="occupy the active inbox",
        )
        self.store.set_inbound_policy(full_refused.id, "refuse")
        self.assertEqual(
            self.store.create_message(
                from_run_id=self.sender_run.id,
                to_run_id=full_refused.id,
                body="refusal does not need inbox capacity",
            ).status,
            "refused",
        )

    def test_reply_is_direct_and_bounded(self) -> None:
        message = self.store.create_message(
            from_run_id=self.sender_run.id,
            to_run_id=self.recipient.id,
            body="please verify the migration",
        )
        reply = self.store.create_message(
            from_run_id=self.recipient.id,
            to_run_id=self.sender_run.id,
            body="verified",
            reply_to=message.id,
        )
        self.assertEqual(reply.reply_to, message.id)
        self.assertEqual(reply.to_run_id, self.sender_run.id)
        self.assertEqual(reply.hop_count, 1)
        with self.assertRaises(ValueError):
            self.store.create_message(
                from_run_id=self.sender_two.id,
                to_run_id=self.sender_run.id,
                body="spoofed reply",
                reply_to=message.id,
            )

    def test_run_missing_state_is_recoverable(self) -> None:
        missing = self.store.update_run(self.recipient.id, status="missing")
        self.assertEqual(missing.status, "missing")
        recovered = self.store.update_run(self.recipient.id, status="running")
        self.assertEqual(recovered.status, "running")

    def test_legacy_schema_migrates_without_global_idempotency_keys(self) -> None:
        legacy_path = Path(self.tempdir.name) / "legacy.sqlite3"
        connection = sqlite3.connect(legacy_path)
        connection.executescript(
            """
            PRAGMA foreign_keys = ON;
            CREATE TABLE runs (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                agent TEXT NOT NULL,
                mode TEXT NOT NULL CHECK (mode IN ('interactive', 'one-shot')),
                command TEXT NOT NULL,
                cwd TEXT NOT NULL,
                tmux_session TEXT UNIQUE,
                status TEXT NOT NULL CHECK (status IN ('starting', 'running', 'success', 'failed', 'killed', 'missing')),
                started_at TEXT NOT NULL,
                ended_at TEXT,
                log_path TEXT
            );
            CREATE TABLE messages (
                id TEXT PRIMARY KEY,
                from_run_id TEXT NOT NULL REFERENCES runs(id),
                to_run_id TEXT NOT NULL REFERENCES runs(id),
                body TEXT NOT NULL,
                body_sha256 TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('queued', 'delivered', 'acknowledged', 'failed')),
                idempotency_key TEXT UNIQUE,
                created_at TEXT NOT NULL,
                delivered_at TEXT,
                acknowledged_at TEXT,
                error TEXT
            );
            INSERT INTO runs VALUES
              ('legacy-sender', 'legacy-sender', 'custom', 'interactive', 'agent', '.', NULL, 'running', '2026-08-10T00:00:00+00:00', NULL, NULL),
              ('legacy-recipient', 'legacy-recipient', 'custom', 'interactive', 'agent', '.', NULL, 'running', '2026-08-10T00:00:00+00:00', NULL, NULL);
            INSERT INTO messages VALUES
              ('legacy-message', 'legacy-sender', 'legacy-recipient', 'old handoff', '00babc2de2d2ba3868f61147d98f316890b984aeed16d99ef6d5c881757730cd', 'queued', 'legacy-key', '2026-08-10T00:00:00+00:00', NULL, NULL, NULL);
            """,
        )
        connection.commit()
        connection.close()

        migrated = Store(legacy_path)
        old = migrated.get_message("legacy-message")
        self.assertEqual(old.body, "old handoff")
        self.assertTrue(migrated.get_run("legacy-sender").inbox_path)
        new_sender = migrated.create_run(
            name="legacy-other",
            agent="custom",
            mode="interactive",
            command="agent",
            cwd=".",
        )
        second = migrated.create_message(
            from_run_id=new_sender.id,
            to_run_id="legacy-recipient",
            body="same key, different sender",
            idempotency_key="legacy-key",
        )
        self.assertNotEqual(second.id, old.id)
        with migrated.connect() as check:
            indexes = check.execute("PRAGMA index_list(messages)").fetchall()
        self.assertTrue(any(row[1] == "idx_messages_idempotency_sender" for row in indexes))

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

        with self.assertRaises(ValueError):
            validate_body("x" * (MAX_MESSAGE_CHARS + 1))
        with self.assertRaises(ValueError):
            validate_body("safe\x00unsafe")
        with self.assertRaises(ValueError):
            validate_body("unsafe\x1b[31m")


if __name__ == "__main__":
    unittest.main()
