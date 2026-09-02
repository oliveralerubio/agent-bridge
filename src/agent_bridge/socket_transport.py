from __future__ import annotations

import json
import os
import socket
import time
from pathlib import Path
from threading import Event
from typing import Callable

from .protocol import MAX_SOCKET_FRAME_BYTES, encode_socket_payload

MAX_FRAME_BYTES = MAX_SOCKET_FRAME_BYTES
MAX_UNIX_SOCKET_PATH_BYTES = 107


class SocketTransportError(RuntimeError):
    pass


class SocketUnavailable(ConnectionError):
    pass


class UnixSocketTransport:
    """Small authenticated-by-filesystem local inbox transport.

    The socket carries structured JSON and never submits terminal keystrokes.
    Filesystem permissions are the trust boundary: listeners are created with
    mode 0600 beside the local SQLite database.
    """

    def send(self, *, path: str, payload: dict[str, object], timeout: float = 1.0) -> None:
        if not path:
            raise SocketUnavailable("recipient has no inbox socket")
        try:
            frame = encode_socket_payload(payload)
        except (TypeError, ValueError) as exc:
            raise SocketTransportError(str(exc)) from exc
        # A listener publishes its filesystem entry before the accept loop is
        # scheduled. Retry a transient refusal so path existence is not used as
        # a false readiness signal by callers starting both processes together.
        retry_until = time.monotonic() + min(max(timeout, 0.0), 0.25)
        while True:
            try:
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                    client.settimeout(timeout)
                    client.connect(path)
                    client.sendall(frame)
                    response = self._read_line(client)
                break
            except ConnectionRefusedError as exc:
                if time.monotonic() >= retry_until:
                    raise SocketUnavailable(str(exc)) from exc
                time.sleep(0.01)
            except (FileNotFoundError, socket.timeout, OSError) as exc:
                raise SocketUnavailable(str(exc)) from exc
        try:
            decoded = json.loads(response.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SocketTransportError("inbox returned invalid acknowledgement") from exc
        if not isinstance(decoded, dict) or decoded.get("ok") is not True:
            detail = decoded.get("error", "inbox rejected message") if isinstance(decoded, dict) else "inbox rejected message"
            raise SocketTransportError(str(detail))

    def listen(
        self,
        *,
        path: str,
        on_message: Callable[[dict[str, object]], None],
        once: bool = False,
        timeout: float | None = None,
        stop_event: Event | None = None,
    ) -> None:
        if not path:
            raise SocketTransportError("inbox socket path is empty")
        socket_path = Path(path).expanduser()
        if len(os.fsencode(str(socket_path))) > MAX_UNIX_SOCKET_PATH_BYTES:
            raise SocketTransportError("inbox socket path is too long for Unix sockets")
        socket_path.parent.mkdir(parents=True, exist_ok=True)
        self._remove_stale_socket(socket_path)

        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        bound = False
        try:
            server.bind(str(socket_path))
            bound = True
            os.chmod(socket_path, 0o600)
            server.listen(16)
            if timeout is not None:
                deadline = time.monotonic() + timeout
            else:
                deadline = None
            while True:
                if stop_event is not None and stop_event.is_set():
                    break
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    server.settimeout(min(0.5, remaining))
                else:
                    server.settimeout(0.5)
                try:
                    connection, _ = server.accept()
                except socket.timeout:
                    continue
                with connection:
                    try:
                        raw = self._read_line(connection)
                        payload = json.loads(raw.decode("utf-8"))
                        if not isinstance(payload, dict):
                            raise ValueError("message must be a JSON object")
                        on_message(payload)
                    except SocketTransportError:
                        self._send_error(connection, "invalid or oversized frame")
                    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
                        self._send_error(connection, "invalid JSON")
                    except Exception as exc:  # listener reports a bounded failure to sender
                        self._send_error(connection, str(exc)[:256])
                    else:
                        self._send_response(connection, {"ok": True})
                if once:
                    break
        finally:
            server.close()
            if bound:
                try:
                    socket_path.unlink()
                except FileNotFoundError:
                    pass

    @staticmethod
    def _remove_stale_socket(socket_path: Path) -> None:
        if not socket_path.exists():
            return
        probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        probe.settimeout(0.2)
        try:
            probe.connect(str(socket_path))
        except ConnectionRefusedError:
            socket_path.unlink()
        except FileNotFoundError:
            return
        except socket.timeout as exc:
            raise SocketTransportError("inbox socket is already in use") from exc
        except OSError as exc:
            raise SocketTransportError("inbox path already exists and is not a stale socket") from exc
        else:
            raise SocketTransportError("inbox socket is already in use")
        finally:
            probe.close()

    @staticmethod
    def _send_response(connection: socket.socket, payload: dict[str, object]) -> None:
        try:
            connection.sendall(encode_socket_payload(payload))
        except OSError:
            pass

    @classmethod
    def _send_error(cls, connection: socket.socket, detail: str) -> None:
        cls._send_response(connection, {"ok": False, "error": detail[:256]})

    @staticmethod
    def _read_line(connection: socket.socket) -> bytes:
        chunks: list[bytes] = []
        size = 0
        while size <= MAX_FRAME_BYTES:
            chunk = connection.recv(min(4096, MAX_FRAME_BYTES + 1 - size))
            if not chunk:
                break
            chunks.append(chunk)
            size += len(chunk)
            if b"\n" in chunk:
                break
        raw = b"".join(chunks)
        if len(raw) > MAX_FRAME_BYTES or b"\n" not in raw:
            raise SocketTransportError("inbox frame exceeds the bounded limit")
        return raw.split(b"\n", 1)[0]
