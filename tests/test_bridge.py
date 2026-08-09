import tempfile
import unittest
from pathlib import Path

from agent_bridge.bridge import Bridge
from agent_bridge.store import Store


class FakeTmux:
    def __init__(self) -> None:
        self.sessions: set[str] = set()
        self.injected: list[tuple[str, str]] = []

    def has_session(self, session: str) -> bool:
        return session in self.sessions

    def inject(self, *, session: str, text: str) -> None:
        self.injected.append((session, text))


class BridgeDeliveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.tempdir.name) / "bridge.sqlite3")
        self.sender = self.store.create_run(
            name="sender", agent="pi", mode="interactive", command="pi", cwd=".", tmux_session="sender"
        )
        self.recipient = self.store.create_run(
            name="recipient", agent="agy", mode="interactive", command="agy", cwd=".", tmux_session="recipient"
        )

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_unavailable_recipient_keeps_message_queued(self) -> None:
        tmux = FakeTmux()
        bridge = Bridge(self.store, tmux)
        message = self.store.create_message(
            from_run_id=self.sender.id,
            to_run_id=self.recipient.id,
            body="wait for the reviewer",
        )
        result = bridge.deliver(message)
        self.assertEqual(result.transport, "queued")
        self.assertEqual(result.message.status, "queued")
        self.assertEqual(tmux.injected, [])

    def test_active_recipient_receives_and_message_is_delivered(self) -> None:
        tmux = FakeTmux()
        tmux.sessions.add("recipient")
        bridge = Bridge(self.store, tmux)
        message = self.store.create_message(
            from_run_id=self.sender.id,
            to_run_id=self.recipient.id,
            body="review the schema change",
        )
        result = bridge.deliver(message)
        self.assertEqual(result.transport, "tmux")
        self.assertEqual(result.message.status, "delivered")
        self.assertEqual(len(tmux.injected), 1)
        self.assertIn(message.id, tmux.injected[0][1])


if __name__ == "__main__":
    unittest.main()
