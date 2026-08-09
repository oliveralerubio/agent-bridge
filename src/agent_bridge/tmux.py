from __future__ import annotations

import secrets
import shlex
import subprocess
from pathlib import Path
from typing import Protocol, Sequence


class MessageTransport(Protocol):
    def has_session(self, session: str) -> bool: ...

    def inject(self, *, session: str, text: str) -> None: ...


class TmuxError(RuntimeError):
    pass


class TmuxTransport:
    def __init__(self, binary: str = "tmux") -> None:
        self.binary = binary

    def available(self) -> bool:
        try:
            result = subprocess.run(
                [self.binary, "-V"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
        except OSError:
            return False
        return result.returncode == 0

    def has_session(self, session: str) -> bool:
        try:
            result = subprocess.run(
                [self.binary, "has-session", "-t", session],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
        except OSError:
            return False
        return result.returncode == 0

    def start_session(
        self,
        *,
        session: str,
        cwd: str,
        argv: Sequence[str],
        environment: dict[str, str],
        log_path: str | Path | None = None,
    ) -> None:
        if not self.available():
            raise TmuxError("tmux is not installed or not available on PATH")
        if not argv:
            raise TmuxError("agent command cannot be empty")
        exports = " ".join(
            f"export {key}={shlex.quote(value)};" for key, value in environment.items()
        )
        command = f"{exports} exec " + " ".join(shlex.quote(part) for part in argv)
        result = subprocess.run(
            [self.binary, "new-session", "-d", "-s", session, "-c", cwd, command],
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            raise TmuxError(result.stderr.strip() or "tmux could not create the session")
        if log_path is not None:
            log_command = f"cat >> {shlex.quote(str(log_path))}"
            pipe = subprocess.run(
                [self.binary, "pipe-pane", "-o", "-t", session, log_command],
                text=True,
                capture_output=True,
                check=False,
            )
            if pipe.returncode != 0:
                raise TmuxError(pipe.stderr.strip() or "tmux could not attach the log pipe")

    def inject(self, *, session: str, text: str) -> None:
        if not self.has_session(session):
            raise TmuxError(f"tmux session is not available: {session}")
        buffer_name = f"agent-bridge-{secrets.token_hex(8)}"
        loaded = subprocess.run(
            [self.binary, "load-buffer", "-b", buffer_name, "-"],
            input=text,
            text=True,
            capture_output=True,
            check=False,
        )
        if loaded.returncode != 0:
            raise TmuxError(loaded.stderr.strip() or "tmux could not load the message buffer")
        pasted = subprocess.run(
            [self.binary, "paste-buffer", "-d", "-b", buffer_name, "-t", session],
            text=True,
            capture_output=True,
            check=False,
        )
        if pasted.returncode != 0:
            raise TmuxError(pasted.stderr.strip() or "tmux could not paste the message")
        submitted = subprocess.run(
            [self.binary, "send-keys", "-t", session, "Enter"],
            text=True,
            capture_output=True,
            check=False,
        )
        if submitted.returncode != 0:
            raise TmuxError(submitted.stderr.strip() or "tmux could not submit the message")

    def stop(self, session: str) -> None:
        subprocess.run(
            [self.binary, "kill-session", "-t", session],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
