from __future__ import annotations

import json
import os
import signal
import subprocess
import threading
from dataclasses import dataclass
from typing import Sequence

MAX_HOOK_INPUT_BYTES = 16 * 1024
MAX_HOOK_ARGS = 32
MAX_HOOK_ARG_CHARS = 1_024


@dataclass(frozen=True)
class HookExecution:
    status: str
    output: str
    returncode: int | None = None


def validate_command(command: Sequence[str]) -> tuple[str, ...]:
    if not isinstance(command, (list, tuple)) or not command or len(command) > MAX_HOOK_ARGS:
        raise ValueError("hook command must be a non-empty bounded argument array")
    result = []
    for arg in command:
        if not isinstance(arg, str) or not arg or len(arg) > MAX_HOOK_ARG_CHARS:
            raise ValueError("hook arguments must be bounded text")
        if any(ord(char) < 32 and char not in "\t" for char in arg):
            raise ValueError("hook arguments contain unsupported control characters")
        result.append(arg)
    return tuple(result)


def encode_event(event: dict[str, object]) -> bytes:
    try:
        payload = json.dumps(event, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError("hook event is not valid JSON") from exc
    if len(payload) > MAX_HOOK_INPUT_BYTES:
        raise ValueError("hook event exceeds the bounded input limit")
    return payload


def run_command(command: Sequence[str], event: dict[str, object], *, timeout: float, max_output: int) -> HookExecution:
    command_tuple = validate_command(command)
    if timeout <= 0 or timeout > 60:
        raise ValueError("hook timeout must be between 0 and 60 seconds")
    if max_output <= 0 or max_output > 64 * 1024:
        raise ValueError("hook output limit is invalid")
    input_bytes = encode_event(event)
    process = subprocess.Popen(
        list(command_tuple),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=False,
        close_fds=True,
        start_new_session=True,
    )
    captured = [bytearray(), bytearray()]

    def drain(stream, target: bytearray) -> None:
        try:
            while True:
                chunk = stream.read(4096)
                if not chunk:
                    return
                remaining = max_output - len(target)
                if remaining > 0:
                    target.extend(chunk[:remaining])
        except (OSError, ValueError):
            return

    threads = [
        threading.Thread(target=drain, args=(process.stdout, captured[0]), daemon=True),
        threading.Thread(target=drain, args=(process.stderr, captured[1]), daemon=True),
    ]
    for thread in threads:
        thread.start()
    try:
        try:
            if process.stdin is not None:
                process.stdin.write(input_bytes)
                process.stdin.close()
        except (BrokenPipeError, OSError):
            pass
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except (OSError, ProcessLookupError):
            try:
                process.kill()
            except OSError:
                pass
        try:
            process.wait(timeout=1.0)
        except subprocess.TimeoutExpired:
            pass
        status = "timeout"
        returncode = None
    finally:
        # Closing the read ends prevents a grandchild that inherited them from
        # keeping hook execution alive after the bounded parent wait.
        for stream in (process.stdout, process.stderr):
            if stream is not None:
                stream.close()
        for thread in threads:
            thread.join(timeout=0.5)
    output = (bytes(captured[0]) + bytes(captured[1])).decode("utf-8", "replace")[:max_output]
    if "status" in locals() and status == "timeout":
        return HookExecution(status, "hook timed out", returncode)
    if process.returncode == 0:
        return HookExecution("allowed", output, 0)
    return HookExecution("denied", output or "hook exited non-zero", process.returncode)
