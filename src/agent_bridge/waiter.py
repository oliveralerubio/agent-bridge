from __future__ import annotations

import datetime as dt
import json
import os
import selectors
import signal
import sqlite3
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

from .bridge import Bridge
from .hooks import validate_command
from .models import Run
from .protocol import validate_body, validate_message_frame
from .socket_transport import UnixSocketTransport
from .store import Store

COMPLETION_TYPE = "agent-bridge.completion"
COMPLETION_STATUSES = {"success", "failed", "partial"}
MAX_COMPLETION_SUMMARY = 8 * 1024
MAX_ACTION_OUTPUT = 64 * 1024
MAX_WAIT_SECONDS = 86_400.0
MAX_POLL_INTERVAL = 5.0


def _text(value: object, field: str, limit: int) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > limit:
        raise ValueError(f"completion {field} is invalid")
    value = value.strip()
    if any(ord(char) < 32 and char not in "\n\t" for char in value):
        raise ValueError(f"completion {field} contains unsupported control characters")
    return value


def completion_body(
    *,
    run_id: str,
    status: str,
    summary: str,
    execution_id: str | None = None,
    phase: str | None = None,
) -> str:
    run_id = _text(run_id, "run_id", 128)
    if status not in COMPLETION_STATUSES:
        raise ValueError(f"completion status must be one of {sorted(COMPLETION_STATUSES)}")
    summary = validate_body(summary)
    if len(summary) > MAX_COMPLETION_SUMMARY:
        raise ValueError("completion summary is too large")
    payload: dict[str, object] = {
        "type": COMPLETION_TYPE,
        "run_id": run_id,
        "status": status,
        "summary": summary,
    }
    if execution_id is not None:
        payload["execution_id"] = _text(execution_id, "execution_id", 128)
    if phase is not None:
        payload["phase"] = _text(phase, "phase", 128)
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    if len(encoded.encode("utf-8")) > MAX_COMPLETION_SUMMARY:
        raise ValueError("completion message is too large")
    return encoded


def parse_completion_body(body: object, *, expected_run_id: str | None = None) -> dict[str, object] | None:
    if not isinstance(body, str):
        return None
    try:
        decoded = json.loads(validate_body(body))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(decoded, dict) or decoded.get("type") != COMPLETION_TYPE:
        return None
    allowed = {"type", "run_id", "status", "summary", "execution_id", "phase"}
    if set(decoded) - allowed:
        raise ValueError("completion message contains unknown fields")
    run_id = _text(decoded.get("run_id"), "run_id", 128)
    if expected_run_id is not None and run_id != expected_run_id:
        raise ValueError("completion message run does not match the expected agent")
    status = decoded.get("status")
    if status not in COMPLETION_STATUSES:
        raise ValueError("completion status is invalid")
    summary = validate_body(decoded.get("summary"))  # type: ignore[arg-type]
    if len(summary) > MAX_COMPLETION_SUMMARY:
        raise ValueError("completion summary is too large")
    result: dict[str, object] = {
        "type": COMPLETION_TYPE,
        "run_id": run_id,
        "status": status,
        "summary": summary,
    }
    for field in ("execution_id", "phase"):
        if field in decoded:
            result[field] = _text(decoded[field], field, 128)
    return result


@dataclass(frozen=True)
class ActionResult:
    status: str
    output: str = ""
    returncode: int | None = None


@dataclass(frozen=True)
class WaitResult:
    id: str
    status: str
    waiter_run_id: str
    target_run_id: str | None
    target_execution_id: str | None
    message_id: str | None
    completion_status: str | None
    error: str | None
    trigger_status: str | None
    fallback_status: str | None

    def as_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "status": self.status,
            "waiter_run_id": self.waiter_run_id,
            "target_run_id": self.target_run_id,
            "target_execution_id": self.target_execution_id,
            "message_id": self.message_id,
            "completion_status": self.completion_status,
            "error": self.error,
            "trigger_status": self.trigger_status,
            "fallback_status": self.fallback_status,
        }


def _run_bounded_command(command: Sequence[str], event: Mapping[str, object], timeout: float) -> ActionResult:
    command_tuple = validate_command(command)
    if timeout <= 0 or timeout > MAX_WAIT_SECONDS:
        raise ValueError("action timeout must be greater than zero and no more than 24 hours")
    try:
        input_bytes = (json.dumps(dict(event), ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError("action event is not valid JSON") from exc
    process: subprocess.Popen[bytes] | None = None
    selector: selectors.BaseSelector | None = None
    captured = bytearray()
    timed_out = False
    environment = os.environ.copy()
    for key, value in event.items():
        if isinstance(value, str):
            environment[f"AGENT_BRIDGE_{key.upper()}"] = value
    completion = event.get("completion")
    if isinstance(completion, Mapping):
        for key, value in completion.items():
            if isinstance(value, str):
                environment[f"AGENT_BRIDGE_COMPLETION_{key.upper()}"] = value
    environment["AGENT_BRIDGE_EVENT_JSON"] = input_bytes.decode("utf-8").strip()
    try:
        process = subprocess.Popen(
            list(command_tuple),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            shell=False,
            start_new_session=True,
            close_fds=True,
            env=environment,
        )
        if process.stdin is not None:
            try:
                process.stdin.write(input_bytes)
                process.stdin.close()
            except (BrokenPipeError, OSError):
                pass
        assert process.stdout is not None
        os.set_blocking(process.stdout.fileno(), False)
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ)
        deadline = time.monotonic() + timeout
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                timed_out = True
                _terminate(process)
                break
            for key, _ in selector.select(min(remaining, 0.1)):
                try:
                    chunk = os.read(key.fd, 8192)
                except BlockingIOError:
                    continue
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                if len(captured) < MAX_ACTION_OUTPUT:
                    captured.extend(chunk[: MAX_ACTION_OUTPUT - len(captured)])
            if process.poll() is not None and not selector.get_map():
                break
        if selector.get_map():
            try:
                selector.unregister(process.stdout)
            except Exception:
                pass
        try:
            returncode = process.wait(timeout=1.0)
        except subprocess.TimeoutExpired:
            return ActionResult("timeout", "action process did not exit after termination", None)
    except (OSError, ValueError) as exc:
        if process is not None:
            _terminate(process)
        return ActionResult("failed", str(exc)[:MAX_ACTION_OUTPUT], None)
    finally:
        if selector is not None:
            selector.close()
        if process is not None:
            if process.stdin is not None:
                process.stdin.close()
            if process.stdout is not None:
                process.stdout.close()
    output = bytes(captured).decode("utf-8", "replace")[:MAX_ACTION_OUTPUT]
    if timed_out:
        return ActionResult("timeout", "action timed out", None)
    return ActionResult("completed" if returncode == 0 else "failed", output, returncode)


def _terminate(process: subprocess.Popen[bytes]) -> None:
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


def _parse_timestamp(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        return dt.datetime.fromisoformat(value)
    except ValueError:
        return None


class CompletionWaiter:
    """Block on a structured completion message with bounded failure detection."""

    def __init__(self, store: Store) -> None:
        self.store = store

    def _read_run(self, run_id: str) -> Run | None:
        try:
            return self.store.get_run(run_id, timeout=0.1)
        except (KeyError, sqlite3.OperationalError):
            return None

    def _failure_reason(self, run_id: str, heartbeat_timeout: float) -> str | None:
        run = self._read_run(run_id)
        if run is None:
            return None
        if run.status in {"failed", "killed", "missing"}:
            return run.failure_reason or f"agent run is {run.status}"
        if run.process_pid is not None and not self.store.process_owned_and_running(run):
            return "managed agent process is no longer running"
        if heartbeat_timeout <= 0 or run.status not in {"starting", "running"}:
            return None
        if run.adapter_session_id:
            heartbeat = _parse_timestamp(run.adapter_heartbeat_at)
            if heartbeat is None or (dt.datetime.now(dt.timezone.utc) - heartbeat).total_seconds() > heartbeat_timeout:
                return "adapter heartbeat expired"
            return None
        if run.process_pid is not None:
            return None
        heartbeat = _parse_timestamp(run.last_heartbeat or run.started_at)
        if heartbeat is not None:
            age = (dt.datetime.now(dt.timezone.utc) - heartbeat).total_seconds()
            if age > heartbeat_timeout:
                return "agent heartbeat expired"
        return None

    def _result_from_wait(self, wait_id: str) -> WaitResult:
        row = self.store.get_wait(wait_id)
        return WaitResult(
            id=row["id"],
            status=row["status"],
            waiter_run_id=row["waiter_run_id"],
            target_run_id=row["target_run_id"],
            target_execution_id=row["target_execution_id"],
            message_id=row["message_id"],
            completion_status=row["completion_status"],
            error=row["error"],
            trigger_status=row["trigger_status"],
            fallback_status=row["fallback_status"],
        )

    def _action_event(
        self,
        wait_id: str,
        waiter_run_id: str,
        target_run_id: str | None,
        target_execution_id: str | None,
        message_id: str | None,
        completion: Mapping[str, object] | None,
        error: str | None,
    ) -> dict[str, object]:
        return {
            "type": "agent-bridge.wait",
            "wait_id": wait_id,
            "waiter_run_id": waiter_run_id,
            "target_run_id": target_run_id,
            "target_execution_id": target_execution_id,
            "message_id": message_id,
            "completion": dict(completion or {}),
            "error": error,
        }

    def _run_fallback(
        self,
        *,
        wait_id: str,
        waiter_run_id: str,
        target_run_id: str | None,
        target_execution_id: str | None,
        message_id: str | None,
        completion: Mapping[str, object] | None,
        error: str | None,
        command: Sequence[str] | None,
        timeout: float,
    ) -> str:
        if not command:
            self.store.update_wait(wait_id, fallback_status="not_configured")
            return "not_configured"
        self.store.update_wait(wait_id, status="waiting", fallback_status="running")
        result = _run_bounded_command(
            command,
            self._action_event(
                wait_id,
                waiter_run_id,
                target_run_id,
                target_execution_id,
                message_id,
                completion,
                error,
            ),
            timeout,
        )
        action_error = None if result.status == "completed" else (result.output or result.status)[:1_000]
        self.store.update_wait(wait_id, fallback_status=result.status, fallback_error=action_error)
        return result.status

    def wait(
        self,
        *,
        waiter_run_id: str,
        target_run_id: str | None = None,
        target_execution_id: str | None = None,
        timeout: float = 3_600.0,
        heartbeat_timeout: float = 120.0,
        poll_interval: float = 0.5,
        success_command: Sequence[str] | None = None,
        success_timeout: float = 300.0,
        fallback_command: Sequence[str] | None = None,
        fallback_timeout: float = 300.0,
    ) -> WaitResult:
        if timeout <= 0 or timeout > MAX_WAIT_SECONDS:
            raise ValueError("wait timeout must be greater than zero and no more than 24 hours")
        if heartbeat_timeout < 0 or heartbeat_timeout > MAX_WAIT_SECONDS:
            raise ValueError("heartbeat timeout is invalid")
        if poll_interval <= 0 or poll_interval > MAX_POLL_INTERVAL:
            raise ValueError("poll interval is invalid")
        if target_run_id is not None and target_execution_id is not None:
            raise ValueError("wait may target a run or an execution, not both")
        if target_run_id is not None:
            self.store.get_run(target_run_id)
        if target_execution_id is not None:
            self.store.get_execution(target_execution_id)
        waiter_run = self.store.get_run(waiter_run_id)
        wait_id = self.store.create_wait(
            waiter_run_id=waiter_run_id,
            target_run_id=target_run_id,
            target_execution_id=target_execution_id,
            timeout_seconds=timeout,
        )
        self.store.audit(
            "wait.started",
            waiter_run_id,
            wait_id,
            {"target_run_id": target_run_id, "target_execution_id": target_execution_id, "timeout": timeout},
        )
        stop_event = threading.Event()
        received_event = threading.Event()
        completion: dict[str, object] = {}
        message_id: list[str | None] = [None]
        listener_error: list[str] = []

        def on_message(payload: dict[str, object]) -> None:
            try:
                frame = validate_message_frame(payload, recipient_id=waiter_run_id)
                source = frame["from"]["id"]  # type: ignore[index]
                if target_run_id is not None and source != target_run_id:
                    return
                parsed = parse_completion_body(frame["body"], expected_run_id=source)  # type: ignore[arg-type]
                if parsed is None:
                    return
                if target_run_id is not None and parsed["run_id"] != target_run_id:
                    return
                if target_execution_id is not None and parsed.get("execution_id") != target_execution_id:
                    return
            except (KeyError, TypeError, ValueError):
                return
            message_id[0] = str(frame["message_id"])
            completion.update(parsed)
            received_event.set()

        def serve() -> None:
            try:
                UnixSocketTransport().listen(
                    path=waiter_run.inbox_path or "",
                    on_message=on_message,
                    timeout=None,
                    stop_event=stop_event,
                )
            except Exception as exc:
                listener_error.append(str(exc)[:1_000])
                stop_event.set()

        listener = threading.Thread(target=serve, name=f"agent-bridge-wait-{wait_id}", daemon=True)
        listener.start()
        deadline = time.monotonic() + timeout
        socket_path = Path(waiter_run.inbox_path or "")
        failure: str | None = None
        try:
            while not socket_path.exists():
                if listener_error:
                    failure = listener_error[0]
                    break
                if target_run_id is not None:
                    failure = self._failure_reason(target_run_id, heartbeat_timeout)
                    if failure:
                        break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    failure = "wait deadline expired before the inbox became ready"
                    break
                stop_event.wait(min(0.05, remaining))
            if failure is None and socket_path.exists():
                try:
                    Bridge(self.store).drain_pending(waiter_run_id)
                except Exception as exc:
                    failure = f"pending completion delivery failed: {exc}"[:1_000]
            while failure is None and not received_event.is_set():
                if listener_error:
                    failure = listener_error[0]
                    break
                if target_run_id is not None:
                    failure = self._failure_reason(target_run_id, heartbeat_timeout)
                    if failure:
                        break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    target = self._read_run(target_run_id) if target_run_id else None
                    if target is not None and target.status == "success":
                        failure = "agent finished without a completion message"
                    else:
                        failure = "wait deadline expired without a completion message"
                    break
                stop_event.wait(min(poll_interval, remaining))
        finally:
            stop_event.set()
            listener.join(timeout=2.0)

        if received_event.is_set() and message_id[0] is not None:
            acknowledged = False
            for _ in range(100):
                try:
                    message = self.store.get_message(message_id[0], timeout=0.1)
                except sqlite3.OperationalError:
                    time.sleep(0.01)
                    continue
                if message.status == "acknowledged":
                    acknowledged = True
                    break
                if message.status == "delivered":
                    try:
                        self.store.acknowledge(message_id[0], waiter_run_id, timeout=0.1)
                    except sqlite3.OperationalError:
                        time.sleep(0.01)
                        continue
                    acknowledged = True
                    break
                time.sleep(0.01)
            if not acknowledged:
                received_event.clear()
                failure = "completion message was not durably delivered for acknowledgement"

        if received_event.is_set():
            self.store.update_wait(
                wait_id,
                message_id=message_id[0],
                completion_status=str(completion.get("status")),
            )
            reported = str(completion.get("status"))
            if reported == "success":
                self.store.update_wait(wait_id, status="completed", error=None)
                trigger_status = "not_configured"
                if success_command:
                    self.store.update_wait(wait_id, trigger_status="running")
                    action = _run_bounded_command(
                        success_command,
                        self._action_event(
                            wait_id,
                            waiter_run_id,
                            target_run_id,
                            target_execution_id,
                            message_id[0],
                            completion,
                            None,
                        ),
                        success_timeout,
                    )
                    trigger_status = action.status
                    trigger_error = None if action.status == "completed" else (action.output or action.status)[:1_000]
                    self.store.update_wait(wait_id, trigger_status=trigger_status, trigger_error=trigger_error)
                    if action.status != "completed":
                        self.store.update_wait(wait_id, status="failed", error=f"success trigger {action.status}")
                else:
                    self.store.update_wait(wait_id, trigger_status=trigger_status)
                self.store.audit("wait.completed", waiter_run_id, wait_id, {"message_id": message_id[0]})
                return self._result_from_wait(wait_id)
            status = "failed" if reported == "failed" else "partial"
            failure = str(completion.get("summary") or f"agent reported {reported}")[:1_000]
            self.store.update_wait(wait_id, status=status, error=failure)
        else:
            if failure is None:
                failure = "wait ended without a completion event"
            target = self._read_run(target_run_id) if target_run_id else None
            status = "timeout" if failure.startswith("wait deadline expired") else "failed"
            if target is not None and target.status == "success":
                status = "partial"
            self.store.update_wait(wait_id, status=status, error=failure)

        fallback_status = self._run_fallback(
            wait_id=wait_id,
            waiter_run_id=waiter_run_id,
            target_run_id=target_run_id,
            target_execution_id=target_execution_id,
            message_id=message_id[0],
            completion=completion or None,
            error=failure,
            command=fallback_command,
            timeout=fallback_timeout,
        )
        self.store.update_wait(wait_id, status=status, fallback_status=fallback_status, error=failure)
        self.store.audit("wait.failed", waiter_run_id, wait_id, {"status": status, "error": failure[:256]})
        return self._result_from_wait(wait_id)
