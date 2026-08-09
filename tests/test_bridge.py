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


class FakeSocket:
    def __init__(self) -> None:
        self.available_paths: set[str] = set()
        self.sent: list[tuple[str, dict[str, object]]] = []

    def send(self, *, path: str, payload: dict[str, object]) -> None:
        if path not in self.available_paths:
            raise ConnectionError("socket unavailable")
        self.sent.append((path, payload))


class RejectingSocket(FakeSocket):
    def send(self, *, path: str, payload: dict[str, object]) -> None:
        from agent_bridge.socket_transport import SocketTransportError

        raise SocketTransportError("listener rejected frame")


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

    def test_pending_message_is_redelivered_after_recipient_becomes_ready(self) -> None:
        tmux = FakeTmux()
        bridge = Bridge(self.store, tmux)
        message = self.store.create_message(
            from_run_id=self.sender.id,
            to_run_id=self.recipient.id,
            body="wait for the reviewer",
        )
        self.assertEqual(bridge.deliver(message).transport, "queued")
        tmux.sessions.add("recipient")
        results = bridge.drain_pending(self.recipient.id)
        self.assertEqual([result.transport for result in results], ["tmux"])
        self.assertEqual(self.store.get_message(message.id).status, "delivered")

    def test_active_recipient_prefers_socket_over_tmux(self) -> None:
        tmux = FakeTmux()
        tmux.sessions.add("recipient")
        socket = FakeSocket()
        socket.available_paths.add(self.recipient.inbox_path)
        bridge = Bridge(self.store, tmux, socket)
        message = self.store.create_message(
            from_run_id=self.sender.id,
            to_run_id=self.recipient.id,
            body="deliver through the structured inbox",
        )
        result = bridge.deliver(message)
        self.assertEqual(result.transport, "socket")
        self.assertEqual(tmux.injected, [])
        self.assertEqual(len(socket.sent), 1)
        self.assertEqual(socket.sent[0][1]["message_id"], message.id)

    def test_socket_falls_back_to_tmux_when_no_listener_exists(self) -> None:
        tmux = FakeTmux()
        tmux.sessions.add("recipient")
        bridge = Bridge(self.store, tmux, FakeSocket())
        message = self.store.create_message(
            from_run_id=self.sender.id,
            to_run_id=self.recipient.id,
            body="fallback is explicit",
        )
        result = bridge.deliver(message)
        self.assertEqual(result.transport, "tmux")
        self.assertEqual(len(tmux.injected), 1)

    def test_drain_continues_after_socket_rejection(self) -> None:
        tmux = FakeTmux()
        tmux.sessions.add("recipient")
        bridge = Bridge(self.store, tmux, RejectingSocket())
        messages = [
            self.store.create_message(
                from_run_id=self.sender.id,
                to_run_id=self.recipient.id,
                body=f"rejected {index}",
            )
            for index in range(2)
        ]
        self.assertEqual(bridge.drain_pending(self.recipient.id), [])
        self.assertEqual([self.store.get_message(message.id).status for message in messages], ["failed", "failed"])

        import threading

        tmux = FakeTmux()
        tmux.sessions.add("recipient")
        bridge = Bridge(self.store, tmux)
        message = self.store.create_message(
            from_run_id=self.sender.id,
            to_run_id=self.recipient.id,
            body="one delivery only",
        )
        barrier = threading.Barrier(2)
        results = []

        def deliver() -> None:
            barrier.wait()
            results.append(bridge.deliver(message))

        threads = [threading.Thread(target=deliver) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(len(tmux.injected), 1)
        self.assertEqual(sum(result.transport == "tmux" for result in results), 1)


if __name__ == "__main__":
    unittest.main()
