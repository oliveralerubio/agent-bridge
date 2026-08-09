import json
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path

from agent_bridge.protocol import MAX_MESSAGE_CHARS
from agent_bridge.socket_transport import MAX_FRAME_BYTES, UnixSocketTransport


class UnixSocketTransportTests(unittest.TestCase):
    @staticmethod
    def _connect_when_ready(path: str) -> socket.socket:
        deadline = time.monotonic() + 2.0
        while True:
            client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                client.connect(path)
            except ConnectionRefusedError:
                client.close()
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.01)
            else:
                return client

    def test_listener_receives_structured_message_and_acknowledges(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            path = str(Path(tempdir) / "inbox.sock")
            transport = UnixSocketTransport()
            received: list[dict[str, object]] = []

            thread = threading.Thread(
                target=transport.listen,
                kwargs={"path": path, "on_message": received.append, "once": True, "timeout": 5.0},
                daemon=True,
            )
            thread.start()
            for _ in range(100):
                if Path(path).exists():
                    break
                time.sleep(0.01)
            transport.send(path=path, payload={"message_id": "msg-1", "body": "hello"})
            thread.join(timeout=2)

            self.assertFalse(thread.is_alive())
            self.assertEqual(received, [{"message_id": "msg-1", "body": "hello"}])

    def test_listener_accepts_maximum_utf8_body(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            path = str(Path(tempdir) / "inbox.sock")
            transport = UnixSocketTransport()
            received: list[dict[str, object]] = []
            thread = threading.Thread(
                target=transport.listen,
                kwargs={"path": path, "on_message": received.append, "once": True, "timeout": 5.0},
                daemon=True,
            )
            thread.start()
            for _ in range(100):
                if Path(path).exists():
                    break
                time.sleep(0.01)

            body = "é" * MAX_MESSAGE_CHARS
            transport.send(path=path, payload={"message_id": "msg-utf8", "body": body})
            thread.join(timeout=2)
            self.assertFalse(thread.is_alive())
            self.assertEqual(received[0]["body"], body)
            self.assertFalse(Path(path).exists())

    def test_listener_rejects_oversized_or_invalid_json(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            path = str(Path(tempdir) / "inbox.sock")
            transport = UnixSocketTransport()
            received: list[dict[str, object]] = []
            thread = threading.Thread(
                target=transport.listen,
                kwargs={"path": path, "on_message": received.append, "once": True, "timeout": 5.0},
                daemon=True,
            )
            thread.start()
            for _ in range(100):
                if Path(path).exists():
                    break
                time.sleep(0.01)

            with self._connect_when_ready(path) as client:
                client.sendall(b"not-json\n")
                response = client.recv(1024)
            thread.join(timeout=2)
            self.assertEqual(json.loads(response), {"ok": False, "error": "invalid JSON"})
            self.assertEqual(received, [])
            self.assertFalse(Path(path).exists())

    def test_listener_rejects_oversized_frames_with_a_bounded_error(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            path = str(Path(tempdir) / "inbox.sock")
            transport = UnixSocketTransport()
            thread = threading.Thread(
                target=transport.listen,
                kwargs={"path": path, "on_message": lambda payload: None, "once": True, "timeout": 5.0},
                daemon=True,
            )
            thread.start()
            for _ in range(100):
                if Path(path).exists():
                    break
                time.sleep(0.01)

            with self._connect_when_ready(path) as client:
                client.sendall(b"x" * (MAX_FRAME_BYTES + 1) + b"\n")
                response = client.recv(1024)
            thread.join(timeout=2)
            self.assertFalse(thread.is_alive())
            self.assertEqual(json.loads(response), {"ok": False, "error": "invalid or oversized frame"})
            self.assertFalse(Path(path).exists())


if __name__ == "__main__":
    unittest.main()
