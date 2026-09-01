from __future__ import annotations

import hashlib
import json
import os
import re
import selectors
import shlex
import signal
import subprocess
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from .store import Store, utc_now

MAX_MANIFEST_BYTES = 128 * 1024
MAX_PHASES = 32
MAX_PHASE_NAME = 128
MAX_ROLE_NAME = 64
MAX_COMMAND_ARGS = 64
MAX_COMMAND_ARG_CHARS = 4_096
MAX_OUTPUT_BYTES = 64 * 1024
MAX_CHECKPOINT_BYTES = 128 * 1024
_ALLOWED_ROLES = {"preflight", "scout", "writer", "reviewer", "verification", "custom"}
_PHASE_END_RE = re.compile(r"^AGENT_BRIDGE_AGENT_END\s+phase=(\S+)\s+status=(\S+)\s*$")


@dataclass(frozen=True)
class PhaseSpec:
    name: str
    role: str
    command: tuple[str, ...]
    cwd: str | None = None
    timeout: float = 900.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "role": self.role,
            "command": list(self.command),
            "cwd": self.cwd,
            "timeout": self.timeout,
        }


@dataclass(frozen=True)
class ExecutionManifest:
    name: str
    cwd: str
    phases: tuple[PhaseSpec, ...]
    checkpoint_dir: str | None = None
    metadata: Mapping[str, Any] | None = None
    max_output: int = MAX_OUTPUT_BYTES

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "cwd": self.cwd,
            "phases": [phase.as_dict() for phase in self.phases],
            "checkpoint_dir": self.checkpoint_dir,
            "metadata": dict(self.metadata or {}),
            "max_output": self.max_output,
        }


@dataclass(frozen=True)
class PhaseResult:
    index: int
    run_id: str | None
    name: str
    role: str
    status: str
    exit_code: int | None
    agent_end: bool
    output: str
    error: str | None
    started_at: str | None
    ended_at: str | None


@dataclass(frozen=True)
class ExecutionResult:
    id: str
    name: str
    status: str
    current_phase: int | None
    checkpoint_path: str
    phases: tuple[PhaseResult, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "status": self.status,
            "current_phase": self.current_phase,
            "checkpoint_path": self.checkpoint_path,
            "phases": [
                {
                    "index": phase.index,
                    "run_id": phase.run_id,
                    "name": phase.name,
                    "role": phase.role,
                    "status": phase.status,
                    "exit_code": phase.exit_code,
                    "agent_end": phase.agent_end,
                    "output": phase.output,
                    "error": phase.error,
                    "started_at": phase.started_at,
                    "ended_at": phase.ended_at,
                }
                for phase in self.phases
            ],
        }


def _bounded_text(value: object, field: str, limit: int) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > limit:
        raise ValueError(f"execution {field} is invalid")
    value = value.strip()
    if any(ord(char) < 32 and char not in "\t" for char in value):
        raise ValueError(f"execution {field} contains unsupported control characters")
    return value


def _bounded_metadata(value: object) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError("execution metadata must be an object")

    def validate(item: object) -> None:
        if isinstance(item, str):
            if any(ord(char) < 32 and char not in "\n\t" for char in item):
                raise ValueError("execution metadata contains unsupported control characters")
        elif isinstance(item, Mapping):
            for key, child in item.items():
                if not isinstance(key, str):
                    raise ValueError("execution metadata keys must be text")
                validate(key)
                validate(child)
        elif isinstance(item, (list, tuple)):
            for child in item:
                validate(child)

    validate(value)
    try:
        encoded = json.dumps(dict(value), ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError("execution metadata must be valid JSON") from exc
    if len(encoded) > 16 * 1024:
        raise ValueError("execution metadata is too large")
    return dict(value)


def _bounded_argv(value: object) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or not value or len(value) > MAX_COMMAND_ARGS:
        raise ValueError("execution phase command must be a bounded argv array")
    result: list[str] = []
    for arg in value:
        result.append(_bounded_text(arg, "command argument", MAX_COMMAND_ARG_CHARS))
    return tuple(result)


def _read_manifest(source: object) -> bytes:
    if isinstance(source, Mapping):
        try:
            raw = json.dumps(dict(source), ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise ValueError("execution manifest must be valid JSON") from exc
    elif isinstance(source, bytes):
        raw = source
    elif isinstance(source, Path):
        try:
            with source.expanduser().open("rb") as handle:
                raw = handle.read(MAX_MANIFEST_BYTES + 1)
        except OSError as exc:
            raise ValueError(f"could not read execution manifest: {exc}") from exc
    elif isinstance(source, str):
        path = Path(source).expanduser()
        if len(source) < 4096 and path.exists():
            return _read_manifest(path)
        raw = source.encode("utf-8")
    else:
        raise ValueError("execution manifest must be a path, JSON object, text, or bytes")
    if len(raw) > MAX_MANIFEST_BYTES:
        raise ValueError("execution manifest exceeds the bounded limit")
    return raw


def load_execution_manifest(source: object) -> ExecutionManifest:
    raw = _read_manifest(source)
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("execution manifest must be UTF-8 JSON") from exc
    if not isinstance(data, Mapping):
        raise ValueError("execution manifest must be a JSON object")

    allowed = {"name", "cwd", "phases", "checkpoint_dir", "metadata", "max_output"}
    unknown = set(data) - allowed
    if unknown:
        raise ValueError(f"unknown execution manifest fields: {', '.join(sorted(unknown))}")
    name = _bounded_text(data.get("name"), "name", 128)
    cwd = _bounded_text(data.get("cwd", "."), "cwd", 4_096)
    raw_phases = data.get("phases")
    if not isinstance(raw_phases, list) or not raw_phases or len(raw_phases) > MAX_PHASES:
        raise ValueError("execution phases must be a bounded non-empty list")

    phases: list[PhaseSpec] = []
    names: set[str] = set()
    writer_count = 0
    for raw_phase in raw_phases:
        if not isinstance(raw_phase, Mapping):
            raise ValueError("execution phase must be an object")
        phase_allowed = {"name", "role", "command", "cwd", "timeout"}
        phase_unknown = set(raw_phase) - phase_allowed
        if phase_unknown:
            raise ValueError(f"unknown execution phase fields: {', '.join(sorted(phase_unknown))}")
        phase_name = _bounded_text(raw_phase.get("name"), "phase name", MAX_PHASE_NAME)
        if phase_name in names:
            raise ValueError(f"duplicate execution phase: {phase_name}")
        names.add(phase_name)
        role = _bounded_text(raw_phase.get("role", "custom"), "phase role", MAX_ROLE_NAME)
        if role not in _ALLOWED_ROLES:
            raise ValueError(f"unsupported execution phase role: {role}")
        if role == "writer":
            writer_count += 1
        command = _bounded_argv(raw_phase.get("command"))
        phase_cwd = raw_phase.get("cwd")
        if phase_cwd is not None:
            phase_cwd = _bounded_text(phase_cwd, "phase cwd", 4_096)
        timeout = raw_phase.get("timeout", 900.0)
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0 < timeout <= 3_600:
            raise ValueError("execution phase timeout must be between 0 and 3600 seconds")
        phases.append(PhaseSpec(phase_name, role, command, phase_cwd, float(timeout)))
    if writer_count > 1:
        raise ValueError("execution manifest may contain only one writer phase")

    checkpoint_dir = data.get("checkpoint_dir")
    if checkpoint_dir is not None:
        checkpoint_dir = _bounded_text(checkpoint_dir, "checkpoint_dir", 4_096)
    max_output = data.get("max_output", MAX_OUTPUT_BYTES)
    if isinstance(max_output, bool) or not isinstance(max_output, int) or not 1 <= max_output <= MAX_OUTPUT_BYTES:
        raise ValueError(f"execution max_output must be between 1 and {MAX_OUTPUT_BYTES}")
    return ExecutionManifest(name, cwd, tuple(phases), checkpoint_dir, _bounded_metadata(data.get("metadata")), max_output)


class ExecutionSupervisor:
    """Run a bounded, sequential phase plan with durable SQLite checkpoints."""

    def __init__(self, store: Store):
        self.store = store

    @staticmethod
    def _manifest_json(manifest: ExecutionManifest) -> str:
        return json.dumps(manifest.as_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    def _checkpoint_path(self, manifest: ExecutionManifest, execution_id: str) -> Path:
        base = Path(manifest.checkpoint_dir).expanduser() if manifest.checkpoint_dir else self.store.path.parent / "checkpoints"
        base.mkdir(parents=True, exist_ok=True)
        return (base / f"{execution_id}.json").resolve()

    def _create_execution(self, manifest: ExecutionManifest, checkpoint_path: Path, execution_id: str) -> str:
        manifest_json = self._manifest_json(manifest)
        digest = hashlib.sha256(manifest_json.encode("utf-8")).hexdigest()
        now = utc_now()
        try:
            with self.store.connect() as connection:
                connection.execute(
                    """INSERT INTO executions
                    (id, name, status, manifest_sha256, manifest_json, checkpoint_path, current_phase, created_at, updated_at)
                    VALUES (?, ?, 'pending', ?, ?, ?, NULL, ?, ?)""",
                    (execution_id, manifest.name, digest, manifest_json, str(checkpoint_path), now, now),
                )
                for index, phase in enumerate(manifest.phases):
                    connection.execute(
                        """INSERT INTO execution_phases
                        (execution_id, phase_index, name, role, command_json, cwd, timeout_seconds, status)
                        VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')""",
                        (execution_id, index, phase.name, phase.role, json.dumps(list(phase.command)), phase.cwd, phase.timeout),
                    )
        except Exception as exc:
            if "UNIQUE" in str(exc).upper():
                raise ValueError(f"execution name already exists: {manifest.name}") from exc
            raise
        return execution_id

    def _get_execution_by_name(self, name: str) -> dict[str, Any] | None:
        with self.store.connect() as connection:
            row = connection.execute("SELECT * FROM executions WHERE name = ?", (name,)).fetchone()
        return dict(row) if row else None

    def _get_execution(self, reference: str) -> dict[str, Any]:
        with self.store.connect() as connection:
            row = connection.execute("SELECT * FROM executions WHERE id = ? OR name = ? LIMIT 1", (reference, reference)).fetchone()
        if row is None:
            raise KeyError(f"execution not found: {reference}")
        return dict(row)

    def _get_phases(self, execution_id: str) -> list[dict[str, Any]]:
        with self.store.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM execution_phases WHERE execution_id = ? ORDER BY phase_index", (execution_id,)
            ).fetchall()
        return [dict(row) for row in rows]

    def _update_execution(self, execution_id: str, *, status: str, current_phase: int | None, error: str | None = None) -> None:
        with self.store.connect() as connection:
            connection.execute(
                """UPDATE executions SET status = ?, current_phase = ?, error = ?, updated_at = ?,
                completed_at = CASE WHEN ? IN ('done', 'failed', 'timeout', 'partial') THEN COALESCE(completed_at, ?) ELSE NULL END
                WHERE id = ?""",
                (status, current_phase, error, utc_now(), status, utc_now() if status in {"done", "failed", "timeout", "partial"} else None, execution_id),
            )

    def _update_phase(self, execution_id: str, index: int, **fields: Any) -> None:
        allowed = {"run_id", "status", "started_at", "ended_at", "exit_code", "agent_end", "output", "error"}
        unknown = set(fields) - allowed
        if unknown:
            raise ValueError(f"unknown phase fields: {', '.join(sorted(unknown))}")
        if not fields:
            return
        assignments = ", ".join(f"{key} = ?" for key in fields)
        with self.store.connect() as connection:
            connection.execute(
                f"UPDATE execution_phases SET {assignments} WHERE execution_id = ? AND phase_index = ?",
                (*fields.values(), execution_id, index),
            )

    def _write_checkpoint(self, execution: Mapping[str, Any], phases: Iterable[Mapping[str, Any]]) -> None:
        phases = list(phases)
        path = Path(execution["checkpoint_path"])
        completed = [phase["name"] for phase in phases if phase["status"] == "done"]
        payload = {
            "execution_id": execution["id"],
            "name": execution["name"],
            "status": execution["status"],
            "current_phase": execution["current_phase"],
            "manifest_sha256": execution["manifest_sha256"],
            "completed_phases": completed,
            "phases": [
                {
                    "index": phase["phase_index"],
                    "run_id": phase["run_id"],
                    "name": phase["name"],
                    "role": phase["role"],
                    "status": phase["status"],
                    "exit_code": phase["exit_code"],
                    "agent_end": bool(phase["agent_end"]),
                    "error": phase["error"],
                }
                for phase in phases
            ],
            "updated_at": utc_now(),
        }
        encoded = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        if len(encoded) > MAX_CHECKPOINT_BYTES:
            raise ValueError("execution checkpoint exceeds the bounded limit")
        temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
        try:
            temporary.write_bytes(encoded)
            os.chmod(temporary, 0o600)
            with temporary.open("rb") as handle:
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)

    @staticmethod
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

    def _execute_phase(
        self,
        execution_id: str,
        phase_run_id: str,
        checkpoint_path: str,
        phase: PhaseSpec,
        max_output: int,
    ) -> tuple[str, int | None, bool, str, str | None]:
        execution = self._get_execution(execution_id)
        manifest_data = json.loads(execution["manifest_json"])
        cwd = Path(phase.cwd or manifest_data["cwd"]).expanduser().resolve()
        if not cwd.is_dir():
            return "failed", None, False, "", f"working directory does not exist: {cwd}"
        env = os.environ.copy()
        phase_run = self.store.get_run(phase_run_id)
        env.update(
            {
                "AGENT_BRIDGE_RUN_ID": phase_run.id,
                "AGENT_BRIDGE_RUN_NAME": phase_run.name,
                "AGENT_BRIDGE_DB": str(self.store.path),
                "AGENT_BRIDGE_INBOX": phase_run.inbox_path or "",
                "AGENT_BRIDGE_INBOUND_POLICY": phase_run.inbound_policy,
                "AGENT_BRIDGE_EXECUTION_ID": execution_id,
                "AGENT_BRIDGE_PHASE": phase.name,
                "AGENT_BRIDGE_PHASE_ROLE": phase.role,
                "AGENT_BRIDGE_CHECKPOINT": checkpoint_path,
            }
        )
        self.store.update_run(phase_run_id, status="running", lifecycle_state="running")
        process: subprocess.Popen[bytes] | None = None
        selector: selectors.BaseSelector | None = None
        captured = bytearray()
        timed_out = False
        try:
            process = subprocess.Popen(
                list(phase.command),
                cwd=str(cwd),
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                shell=False,
                start_new_session=True,
                close_fds=True,
            )
            if process.poll() is None:
                try:
                    self.store.set_process(phase_run_id, process.pid)
                except ValueError:
                    # A very short-lived process may exit before /proc can be read.
                    if process.poll() is None:
                        raise
            assert process.stdout is not None
            fd = process.stdout.fileno()
            os.set_blocking(fd, False)
            selector = selectors.DefaultSelector()
            selector.register(process.stdout, selectors.EVENT_READ)
            deadline = time.monotonic() + phase.timeout
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    timed_out = True
                    self._terminate(process)
                    break
                for key, _ in selector.select(min(remaining, 0.1)):
                    try:
                        chunk = os.read(key.fd, 8192)
                    except BlockingIOError:
                        continue
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    if len(captured) < max_output:
                        captured.extend(chunk[: max_output - len(captured)])
                if process.poll() is not None and not selector.get_map():
                    break
            if selector.get_map():
                try:
                    selector.unregister(process.stdout)
                except Exception:
                    pass
            returncode = process.wait(timeout=1.0)
        except (OSError, ValueError) as exc:
            if process is not None:
                self._terminate(process)
            return "failed", None, False, bytes(captured).decode("utf-8", "replace"), str(exc)
        finally:
            try:
                self.store.clear_process(phase_run_id)
            except KeyError:
                pass
            if selector is not None:
                selector.close()
            if process is not None and process.stdout is not None:
                process.stdout.close()
        output = bytes(captured).decode("utf-8", "replace")[:max_output]
        if timed_out:
            return "timeout", None, False, output, f"phase exceeded timeout of {phase.timeout:g} seconds"
        sentinel_status: str | None = None
        for line in output.splitlines():
            match = _PHASE_END_RE.match(line.strip())
            if match and match.group(1) == phase.name:
                sentinel_status = match.group(2)
        agent_end = sentinel_status is not None
        if returncode != 0:
            return "failed", returncode, agent_end, output, f"phase exited with code {returncode}"
        if sentinel_status != "success":
            return "partial", returncode, agent_end, output, "missing successful AGENT_BRIDGE_AGENT_END proof"
        return "done", returncode, True, output, None

    def _result(self, execution_id: str) -> ExecutionResult:
        execution = self._get_execution(execution_id)
        phase_rows = self._get_phases(execution_id)
        phases = tuple(
            PhaseResult(
                index=row["phase_index"],
                run_id=row["run_id"],
                name=row["name"],
                role=row["role"],
                status=row["status"],
                exit_code=row["exit_code"],
                agent_end=bool(row["agent_end"]),
                output=row["output"] or "",
                error=row["error"],
                started_at=row["started_at"],
                ended_at=row["ended_at"],
            )
            for row in phase_rows
        )
        return ExecutionResult(execution["id"], execution["name"], execution["status"], execution["current_phase"], execution["checkpoint_path"], phases)

    def _ensure_phase_run(self, execution_id: str, index: int, phase: PhaseSpec) -> str:
        phase_row = self._get_phases(execution_id)[index]
        if phase_row["run_id"]:
            return phase_row["run_id"]
        execution = self._get_execution(execution_id)
        manifest_data = json.loads(execution["manifest_json"])
        metadata = manifest_data.get("metadata") or {}
        agent = metadata.get("agent", "supervised") if isinstance(metadata, Mapping) else "supervised"
        if not isinstance(agent, str) or not agent.strip():
            agent = "supervised"
        cwd = Path(phase.cwd or manifest_data["cwd"]).expanduser().resolve()
        phase_run_id = f"{execution_id}-phase-{index}"
        run = self.store.create_run(
            name=f"{execution_id}/phase-{index}",
            agent=agent,
            mode="one-shot",
            command=shlex.join(list(phase.command)),
            cwd=str(cwd),
            run_id=phase_run_id,
            role=phase.role,
        )
        self._update_phase(execution_id, index, run_id=run.id)
        return run.id

    def stop(self, reference: str) -> ExecutionResult:
        """Stop the currently running phase and persist an operator failure."""
        execution = self._get_execution(reference)
        phases = self._get_phases(execution["id"])
        active = next((phase for phase in phases if phase["status"] == "running"), None)
        if active is None:
            return self._result(execution["id"])
        run_id = active["run_id"]
        if run_id:
            run = self.store.get_run(run_id)
            if self.store.process_owned_and_running(run):
                try:
                    os.killpg(run.process_pid, signal.SIGTERM)  # type: ignore[arg-type]
                except (OSError, ProcessLookupError):
                    pass
                time.sleep(0.05)
                refreshed = self.store.get_run(run_id)
                if self.store.process_owned_and_running(refreshed):
                    try:
                        os.killpg(refreshed.process_pid, signal.SIGKILL)  # type: ignore[arg-type]
                    except (OSError, ProcessLookupError):
                        pass
            self.store.update_run(run_id, status="killed", ended_at=utc_now(), lifecycle_state="stopped", failure_reason="stopped by operator", exit_code=-9)
        self._update_phase(
            execution["id"],
            active["phase_index"],
            status="failed",
            ended_at=utc_now(),
            error="stopped by operator",
            exit_code=-9,
        )
        self._update_execution(execution["id"], status="failed", current_phase=active["phase_index"], error="stopped by operator")
        execution = self._get_execution(execution["id"])
        self._write_checkpoint(execution, self._get_phases(execution["id"]))
        return self._result(execution["id"])

    def run(self, manifest: ExecutionManifest, *, resume: bool = False) -> ExecutionResult:
        manifest_json = self._manifest_json(manifest)
        digest = hashlib.sha256(manifest_json.encode("utf-8")).hexdigest()
        existing = self._get_execution_by_name(manifest.name)
        if existing is not None:
            if not resume:
                raise ValueError(f"execution name already exists: {manifest.name}; use --resume")
            if existing["manifest_sha256"] != digest:
                raise ValueError("resume manifest does not match the recorded execution")
            execution_id = existing["id"]
        else:
            execution_id = f"exec-{uuid.uuid4().hex}"
            checkpoint_path = self._checkpoint_path(manifest, execution_id)
            self._create_execution(manifest, checkpoint_path, execution_id)
            execution = self._get_execution(execution_id)
            self._write_checkpoint(execution, self._get_phases(execution_id))

        execution = self._get_execution(execution_id)
        phase_rows = self._get_phases(execution_id)
        if execution["status"] == "done":
            return self._result(execution_id)
        self._update_execution(execution_id, status="running", current_phase=execution["current_phase"])
        for index, phase in enumerate(manifest.phases):
            current = self._get_phases(execution_id)[index]
            if current["status"] == "done":
                continue
            phase_run_id = self._ensure_phase_run(execution_id, index, phase)
            self._update_execution(execution_id, status="running", current_phase=index)
            started = utc_now()
            self._update_phase(execution_id, index, status="running", started_at=started, ended_at=None, error=None, output="", agent_end=0, exit_code=None)
            status, exit_code, agent_end, output, error = self._execute_phase(
                execution_id,
                phase_run_id,
                self._get_execution(execution_id)["checkpoint_path"],
                phase,
                manifest.max_output,
            )
            self._update_phase(
                execution_id,
                index,
                status=status,
                ended_at=utc_now(),
                exit_code=exit_code,
                agent_end=int(agent_end),
                output=output[:MAX_OUTPUT_BYTES],
                error=error,
            )
            self.store.update_run(
                phase_run_id,
                status="success" if status == "done" else "failed",
                ended_at=utc_now(),
                lifecycle_state="completed" if status == "done" else "failed",
                failure_reason=error,
                exit_code=exit_code,
            )
            self._update_execution(execution_id, status=status if status != "done" else "running", current_phase=index, error=error)
            execution = self._get_execution(execution_id)
            self._write_checkpoint(execution, self._get_phases(execution_id))
            if status != "done":
                return self._result(execution_id)

        self._update_execution(execution_id, status="done", current_phase=None, error=None)
        execution = self._get_execution(execution_id)
        self._write_checkpoint(execution, self._get_phases(execution_id))
        return self._result(execution_id)
