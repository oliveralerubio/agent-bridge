from __future__ import annotations

import datetime as dt
import hashlib
import os
import json
import secrets
import sqlite3
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .hooks import run_command, validate_command
from .models import CompletionReport, Hook, HookEvent, Message, Run, Task, Team, TeamMember
from .protocol import (
    ADAPTER_HEARTBEAT_TTL_SECONDS,
    ADAPTER_PROTOCOL_VERSION,
    MAX_ADAPTER_CAPABILITIES,
    MAX_MESSAGE_CHARS,
    MAX_REPLY_HOPS,
    validate_adapter_frame,
    validate_body,
)
from .reports import validate_report

DEFAULT_MAX_INBOX = 100
MAX_MESSAGES_PER_MINUTE = 60
DELIVERY_LEASE_SECONDS = 30
MAX_HOOKS = 128

SCHEMA = """
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS teams (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status IN ('created', 'starting', 'running', 'partial', 'stopping', 'stopped', 'failed')),
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    stopped_at TEXT
);

CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    agent TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('interactive', 'one-shot')),
    command TEXT NOT NULL,
    cwd TEXT NOT NULL,
    tmux_session TEXT UNIQUE,
    inbox_path TEXT UNIQUE,
    team TEXT,
    team_id TEXT REFERENCES teams(id),
    role TEXT NOT NULL DEFAULT 'member',
    is_lead INTEGER NOT NULL DEFAULT 0 CHECK (is_lead IN (0, 1)),
    inbound_policy TEXT NOT NULL DEFAULT 'accept' CHECK (inbound_policy IN ('accept', 'hold', 'refuse')),
    max_inbox INTEGER NOT NULL DEFAULT 100 CHECK (max_inbox > 0),
    status TEXT NOT NULL CHECK (status IN ('starting', 'running', 'success', 'failed', 'killed', 'missing')),
    lifecycle_state TEXT NOT NULL DEFAULT 'registered',
    started_at TEXT NOT NULL,
    ended_at TEXT,
    last_heartbeat TEXT,
    log_path TEXT,
    readiness TEXT NOT NULL DEFAULT 'offline' CHECK (readiness IN ('offline', 'hello', 'ready', 'busy', 'idle', 'stopping', 'failed')),
    adapter_protocol INTEGER,
    adapter_session_id TEXT,
    capabilities_json TEXT NOT NULL DEFAULT '[]',
    adapter_heartbeat_at TEXT,
    adapter_ready_at TEXT,
    adapter_error TEXT,
    failure_reason TEXT,
    exit_code INTEGER,
    readiness_required INTEGER NOT NULL DEFAULT 0 CHECK (readiness_required IN (0, 1)),
    readiness_timeout REAL NOT NULL DEFAULT 0,
    restart_policy TEXT NOT NULL DEFAULT 'never' CHECK (restart_policy IN ('never', 'on-failure', 'always')),
    restart_count INTEGER NOT NULL DEFAULT 0,
    process_pid INTEGER,
    process_start_token TEXT
);

CREATE TABLE IF NOT EXISTS team_members (
    team_id TEXT NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    role TEXT NOT NULL DEFAULT 'member',
    is_lead INTEGER NOT NULL DEFAULT 0 CHECK (is_lead IN (0, 1)),
    joined_at TEXT NOT NULL,
    removed_at TEXT,
    PRIMARY KEY (team_id, run_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_team_one_active_lead ON team_members(team_id) WHERE is_lead = 1 AND removed_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_team_members_active ON team_members(team_id, removed_at, joined_at);

CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    from_run_id TEXT NOT NULL REFERENCES runs(id),
    to_run_id TEXT NOT NULL REFERENCES runs(id),
    body TEXT NOT NULL,
    body_sha256 TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('queued', 'held', 'delivered', 'acknowledged', 'failed', 'refused')),
    idempotency_key TEXT,
    reply_to TEXT REFERENCES messages(id),
    hop_count INTEGER NOT NULL DEFAULT 0 CHECK (hop_count >= 0),
    delivery_attempts INTEGER NOT NULL DEFAULT 0 CHECK (delivery_attempts >= 0),
    created_at TEXT NOT NULL,
    delivered_at TEXT,
    acknowledged_at TEXT,
    held_at TEXT,
    refused_at TEXT,
    claimed_by TEXT,
    claim_expires_at TEXT,
    error TEXT,
    adapter_accepted_at TEXT
);

CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    created_by TEXT NOT NULL REFERENCES runs(id),
    assigned_to TEXT REFERENCES runs(id),
    team_id TEXT REFERENCES teams(id),
    status TEXT NOT NULL CHECK (status IN ('proposed', 'awaiting_approval', 'approved', 'pending', 'in_progress', 'completed', 'failed', 'cancelled', 'rejected', 'blocked')),
    requires_approval INTEGER NOT NULL DEFAULT 0 CHECK (requires_approval IN (0, 1)),
    depends_on TEXT NOT NULL DEFAULT '[]',
    approved_by TEXT REFERENCES runs(id),
    approved_at TEXT,
    rejection_reason TEXT,
    blocked_reason TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS reports (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id),
    created_by TEXT NOT NULL REFERENCES runs(id),
    goal TEXT NOT NULL,
    verified_facts_json TEXT NOT NULL,
    tests_json TEXT NOT NULL,
    files_changed_json TEXT NOT NULL,
    blockers_json TEXT NOT NULL,
    next_action TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS operators (
    run_id TEXT PRIMARY KEY REFERENCES runs(id),
    granted_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hooks (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    event TEXT NOT NULL,
    command_json TEXT NOT NULL,
    timeout_seconds REAL NOT NULL DEFAULT 5,
    max_output INTEGER NOT NULL DEFAULT 8192,
    fail_closed INTEGER NOT NULL DEFAULT 0,
    enabled INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_hooks_event ON hooks(event, enabled);

CREATE TABLE IF NOT EXISTS hook_events (
    id TEXT PRIMARY KEY,
    hook_id TEXT REFERENCES hooks(id),
    event TEXT NOT NULL,
    status TEXT NOT NULL,
    output TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_hook_events_created ON hook_events(created_at);

CREATE TABLE IF NOT EXISTS audit_log (
    id TEXT PRIMARY KEY,
    event TEXT NOT NULL,
    actor_run_id TEXT,
    resource_id TEXT,
    detail_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS adapter_events (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id),
    session_id TEXT,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS executions (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'done', 'failed', 'timeout', 'partial')),
    manifest_sha256 TEXT NOT NULL,
    manifest_json TEXT NOT NULL,
    checkpoint_path TEXT NOT NULL,
    current_phase INTEGER,
    error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS execution_phases (
    execution_id TEXT NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
    run_id TEXT REFERENCES runs(id),
    phase_index INTEGER NOT NULL,
    name TEXT NOT NULL,
    role TEXT NOT NULL,
    command_json TEXT NOT NULL,
    cwd TEXT,
    timeout_seconds REAL NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'done', 'failed', 'timeout', 'partial')),
    started_at TEXT,
    ended_at TEXT,
    exit_code INTEGER,
    agent_end INTEGER NOT NULL DEFAULT 0 CHECK (agent_end IN (0, 1)),
    output TEXT NOT NULL DEFAULT '',
    error TEXT,
    PRIMARY KEY (execution_id, phase_index),
    UNIQUE (execution_id, name)
);

CREATE INDEX IF NOT EXISTS idx_executions_status ON executions(status, updated_at);
CREATE INDEX IF NOT EXISTS idx_execution_phases_status ON execution_phases(execution_id, status, phase_index);

CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_idempotency_sender
    ON messages(from_run_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_messages_recipient_status ON messages(to_run_id, status, created_at);
CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages(from_run_id, created_at);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status, updated_at);
"""

NEW_MESSAGES_TABLE = """
CREATE TABLE messages (
    id TEXT PRIMARY KEY, from_run_id TEXT NOT NULL REFERENCES runs(id), to_run_id TEXT NOT NULL REFERENCES runs(id),
    body TEXT NOT NULL, body_sha256 TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('queued', 'held', 'delivered', 'acknowledged', 'failed', 'refused')),
    idempotency_key TEXT, reply_to TEXT REFERENCES messages(id), hop_count INTEGER NOT NULL DEFAULT 0 CHECK (hop_count >= 0),
    delivery_attempts INTEGER NOT NULL DEFAULT 0 CHECK (delivery_attempts >= 0), created_at TEXT NOT NULL, delivered_at TEXT,
    acknowledged_at TEXT, held_at TEXT, refused_at TEXT, claimed_by TEXT, claim_expires_at TEXT, error TEXT,
    adapter_accepted_at TEXT
)
"""

NEW_TASKS_TABLE = """
CREATE TABLE tasks (
    id TEXT PRIMARY KEY, title TEXT NOT NULL, description TEXT NOT NULL DEFAULT '', created_by TEXT NOT NULL REFERENCES runs(id),
    assigned_to TEXT REFERENCES runs(id), team_id TEXT REFERENCES teams(id),
    status TEXT NOT NULL CHECK (status IN ('proposed', 'awaiting_approval', 'approved', 'pending', 'in_progress', 'completed', 'failed', 'cancelled', 'rejected', 'blocked')),
    requires_approval INTEGER NOT NULL DEFAULT 0 CHECK (requires_approval IN (0, 1)), depends_on TEXT NOT NULL DEFAULT '[]',
    approved_by TEXT REFERENCES runs(id), approved_at TEXT, rejection_reason TEXT, blocked_reason TEXT,
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL, completed_at TEXT
)
"""


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def _token(prefix: str) -> str:
    return f"{prefix}-{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d%H%M%S')}-{secrets.token_hex(4)}"


def _clean_name(value: str, field: str, limit: int = 128) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be text")
    value = value.strip()
    if not value:
        raise ValueError(f"{field} cannot be empty")
    if len(value) > limit or any(char in value for char in "\x00\r\n"):
        raise ValueError(f"{field} is invalid or too long")
    return value


def _reject_controls(value: object, field: str) -> None:
    if isinstance(value, str) and any(ord(char) < 32 and char not in "\n\t" for char in value):
        raise ValueError(f"{field} contains unsupported control characters")
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{field} keys must be text")
            _reject_controls(key, field)
            _reject_controls(item, field)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _reject_controls(item, field)


def _json_object(value: Mapping[str, Any], field: str, limit: int = 16 * 1024) -> str:
    _reject_controls(value, field)
    try:
        encoded = json.dumps(dict(value), ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be bounded JSON") from exc
    if len(encoded) > limit:
        raise ValueError(f"{field} is too large")
    return encoded.decode("utf-8")


class Store:
    def __init__(self, path: str | Path):
        self.path = Path(path).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        return connection

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(SCHEMA)
            self._migrate_legacy(connection)
            connection.execute("PRAGMA user_version = 5")

    def _migrate_legacy(self, connection: sqlite3.Connection) -> None:
        run_columns = {row[1] for row in connection.execute("PRAGMA table_info(runs)")}
        if run_columns:
            additions = [
                ("inbox_path", "TEXT"), ("team", "TEXT"), ("team_id", "TEXT"), ("role", "TEXT NOT NULL DEFAULT 'member'"),
                ("is_lead", "INTEGER NOT NULL DEFAULT 0"), ("inbound_policy", "TEXT NOT NULL DEFAULT 'accept'"),
                ("max_inbox", f"INTEGER NOT NULL DEFAULT {DEFAULT_MAX_INBOX}"), ("last_heartbeat", "TEXT"),
                ("lifecycle_state", "TEXT NOT NULL DEFAULT 'registered'"), ("readiness", "TEXT NOT NULL DEFAULT 'offline'"),
                ("adapter_protocol", "INTEGER"), ("adapter_session_id", "TEXT"), ("capabilities_json", "TEXT NOT NULL DEFAULT '[]'"),
                ("adapter_heartbeat_at", "TEXT"), ("adapter_ready_at", "TEXT"), ("adapter_error", "TEXT"),
                ("failure_reason", "TEXT"), ("exit_code", "INTEGER"), ("readiness_required", "INTEGER NOT NULL DEFAULT 0"),
                ("readiness_timeout", "REAL NOT NULL DEFAULT 0"), ("restart_policy", "TEXT NOT NULL DEFAULT 'never'"),
                ("restart_count", "INTEGER NOT NULL DEFAULT 0"), ("process_pid", "INTEGER"),
                ("process_start_token", "TEXT"),
            ]
            for column, definition in additions:
                if column not in run_columns:
                    connection.execute(f"ALTER TABLE runs ADD COLUMN {column} {definition}")
            connection.execute(
                "UPDATE runs SET inbox_path = ? || '/sockets/' || id || '.sock' WHERE inbox_path IS NULL",
                (str(self.path.parent),),
            )
        message_columns = {row[1] for row in connection.execute("PRAGMA table_info(messages)")}
        if message_columns and "reply_to" not in message_columns:
            connection.execute("DROP INDEX IF EXISTS idx_messages_recipient_status")
            connection.execute("DROP INDEX IF EXISTS idx_messages_sender")
            connection.execute("ALTER TABLE messages RENAME TO messages_v1")
            connection.execute(NEW_MESSAGES_TABLE)
            connection.execute(
                """INSERT INTO messages (id, from_run_id, to_run_id, body, body_sha256, status, idempotency_key,
                created_at, delivered_at, acknowledged_at, error)
                SELECT id, from_run_id, to_run_id, body, body_sha256, status, idempotency_key,
                created_at, delivered_at, acknowledged_at, error FROM messages_v1"""
            )
            connection.execute("DROP TABLE messages_v1")
        elif message_columns and "adapter_accepted_at" not in message_columns:
            connection.execute("ALTER TABLE messages ADD COLUMN adapter_accepted_at TEXT")
        task_columns = {row[1] for row in connection.execute("PRAGMA table_info(tasks)")}
        task_sql = connection.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='tasks'").fetchone()
        if task_columns and ("requires_approval" not in task_columns or "awaiting_approval" not in (task_sql[0] or "")):
            connection.execute("DROP INDEX IF EXISTS idx_tasks_status")
            connection.execute("ALTER TABLE tasks RENAME TO tasks_v1")
            connection.execute(NEW_TASKS_TABLE)
            old = {row[1] for row in connection.execute("PRAGMA table_info(tasks_v1)")}
            connection.execute(
                f"""INSERT INTO tasks (id, title, description, created_by, assigned_to, status, depends_on, created_at, updated_at, completed_at)
                SELECT id, title, description, created_by, assigned_to, status, depends_on, created_at, updated_at, completed_at FROM tasks_v1"""
            )
            connection.execute("DROP TABLE tasks_v1")
        phase_columns = {row[1] for row in connection.execute("PRAGMA table_info(execution_phases)")}
        if phase_columns and "run_id" not in phase_columns:
            connection.execute("ALTER TABLE execution_phases ADD COLUMN run_id TEXT REFERENCES runs(id)")
        connection.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_idempotency_sender ON messages(from_run_id, idempotency_key) WHERE idempotency_key IS NOT NULL")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_messages_recipient_status ON messages(to_run_id, status, created_at)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages(from_run_id, created_at)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status, updated_at)")
        # The old free-form team label remains intact; these rows only add
        # durable entities and membership for databases that already used it.
        labels = connection.execute("SELECT DISTINCT team FROM runs WHERE team IS NOT NULL AND team != ''").fetchall()
        for label_row in labels:
            label = label_row[0]
            team = connection.execute("SELECT id FROM teams WHERE name = ?", (label,)).fetchone()
            team_id = team[0] if team else _token("team")
            if team is None:
                now = utc_now()
                connection.execute("INSERT INTO teams (id, name, status, metadata_json, created_at, updated_at) VALUES (?, ?, 'created', '{}', ?, ?)", (team_id, label, now, now))
            runs = connection.execute("SELECT id FROM runs WHERE team = ?", (label,)).fetchall()
            for run in runs:
                connection.execute("UPDATE runs SET team_id = ? WHERE id = ?", (team_id, run[0]))
                connection.execute("INSERT OR IGNORE INTO team_members (team_id, run_id, role, is_lead, joined_at) VALUES (?, ?, 'member', 0, ?)", (team_id, run[0], utc_now()))

    # ---------- runs and readiness ----------
    def create_run(self, *, name: str, agent: str, mode: str, command: str, cwd: str,
                   tmux_session: str | None = None, log_path: str | None = None, run_id: str | None = None,
                   inbox_path: str | None = None, team: str | None = None, team_id: str | None = None,
                   role: str = "member", is_lead: bool = False, inbound_policy: str = "accept",
                   max_inbox: int = DEFAULT_MAX_INBOX, readiness_required: bool = False,
                   readiness_timeout: float = 0, restart_policy: str = "never") -> Run:
        name, agent = _clean_name(name, "run name"), _clean_name(agent, "agent")
        if mode not in {"interactive", "one-shot"}:
            raise ValueError("mode must be interactive or one-shot")
        if not isinstance(command, str) or not command.strip():
            raise ValueError("command cannot be empty")
        if inbound_policy not in {"accept", "hold", "refuse"}:
            raise ValueError("inbound policy must be accept, hold, or refuse")
        if max_inbox < 1 or readiness_timeout < 0 or readiness_timeout > 3600:
            raise ValueError("run limits are invalid")
        if restart_policy not in {"never", "on-failure", "always"}:
            raise ValueError("restart policy is invalid")
        run_id = run_id or _token("run")
        inbox_path = inbox_path or str(self.path.parent / "sockets" / f"{run_id}.sock")
        if len(inbox_path) >= 104:
            raise ValueError("inbox socket path is too long for Unix sockets")
        if team_id:
            team_entity = self.get_team(team_id)
            team_id = team_entity.id
            team = team_entity.name
        elif team and self._team_exists(team):
            team_entity = self.get_team(team)
            team_id = team_entity.id
            team = team_entity.name
        started_at = utc_now()
        try:
            with self.connect() as connection:
                connection.execute("""INSERT INTO runs
                    (id, name, agent, mode, command, cwd, tmux_session, inbox_path, team, team_id, role, is_lead,
                     inbound_policy, max_inbox, status, lifecycle_state, started_at, last_heartbeat, log_path,
                     readiness_required, readiness_timeout, restart_policy)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'starting', 'registered', ?, ?, ?, ?, ?, ?)""",
                    (run_id, name, agent, mode, command, cwd, tmux_session, inbox_path, team, team_id, role, int(is_lead),
                     inbound_policy, max_inbox, started_at, started_at, log_path, int(readiness_required), readiness_timeout, restart_policy))
        except sqlite3.IntegrityError as exc:
            raise ValueError(f"run name, inbox path, or tmux session already exists: {name}") from exc
        self.emit_hook("member.created", {"run_id": run_id, "name": name}, actor_run_id=run_id)
        return self.get_run(run_id)

    def get_run(self, reference: str) -> Run:
        with self.connect() as connection:
            row = connection.execute("SELECT * FROM runs WHERE id = ? LIMIT 1", (reference,)).fetchone()
            if row is None:
                row = connection.execute("SELECT * FROM runs WHERE name = ? LIMIT 1", (reference,)).fetchone()
        if row is None:
            raise KeyError(f"run not found: {reference}")
        return Run.from_row(row)

    def list_runs(self, limit: int = 50, *, team: str | None = None, active_only: bool = False) -> list[Run]:
        query, clauses, params = "SELECT * FROM runs", [], []
        if team:
            team_ref = self.get_team(team).id if self._team_exists(team) else team
            clauses.append("(team_id = ? OR team = ?)"); params.extend([team_ref, team])
        if active_only:
            clauses.append("status IN ('starting', 'running', 'missing')")
        if clauses: query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY started_at DESC LIMIT ?"; params.append(limit)
        with self.connect() as connection: rows = connection.execute(query, params).fetchall()
        return [Run.from_row(row) for row in rows]

    # ---------- supervised executions ----------
    def get_execution(self, reference: str) -> dict[str, Any]:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM executions WHERE id = ? OR name = ? LIMIT 1",
                (reference, reference),
            ).fetchone()
        if row is None:
            raise KeyError(f"execution not found: {reference}")
        return dict(row)

    def list_executions(self, limit: int = 50, *, status: str | None = None) -> list[dict[str, Any]]:
        valid = {"pending", "running", "done", "failed", "timeout", "partial"}
        if status is not None and status not in valid:
            raise ValueError(f"invalid execution status: {status}")
        query = "SELECT * FROM executions"
        params: list[Any] = []
        if status is not None:
            query += " WHERE status = ?"
            params.append(status)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        with self.connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def list_execution_phases(self, execution_reference: str) -> list[dict[str, Any]]:
        execution = self.get_execution(execution_reference)
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM execution_phases WHERE execution_id = ? ORDER BY phase_index",
                (execution["id"],),
            ).fetchall()
        return [dict(row) for row in rows]

    def update_run(self, run_id: str, *, status: str, ended_at: str | None = None,
                   lifecycle_state: str | None = None, failure_reason: str | None = None,
                   exit_code: int | None = None) -> Run:
        if status not in {"starting", "running", "success", "failed", "killed", "missing"}:
            raise ValueError(f"invalid run status: {status}")
        now = utc_now()
        with self.connect() as connection:
            connection.execute("""UPDATE runs SET status = ?, ended_at = COALESCE(?, ended_at), last_heartbeat = ?,
                lifecycle_state = COALESCE(?, lifecycle_state), failure_reason = COALESCE(?, failure_reason), exit_code = COALESCE(?, exit_code)
                WHERE id = ?""", (status, ended_at, now, lifecycle_state, failure_reason, exit_code, run_id))
        self.emit_hook("member.lifecycle", {"run_id": run_id, "status": status, "lifecycle_state": lifecycle_state}, actor_run_id=run_id)
        self.audit("member.lifecycle", run_id, run_id, {"status": status, "lifecycle_state": lifecycle_state})
        return self.get_run(run_id)

    @staticmethod
    def process_start_token(pid: int) -> str | None:
        """Return a Linux process start token, without trusting a reused PID."""
        if pid <= 0:
            return None
        try:
            raw = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
            tail = raw.rsplit(")", 1)[1].split()
            # tail[0] is state (field 3); field 22 is offset 19.
            return tail[19]
        except (OSError, ValueError, IndexError):
            return None

    def set_process(self, run_id: str, pid: int) -> Run:
        if pid <= 0:
            raise ValueError("process PID must be positive")
        token = self.process_start_token(pid)
        if token is None:
            raise ValueError("process is not available for ownership verification")
        with self.connect() as connection:
            connection.execute("UPDATE runs SET process_pid = ?, process_start_token = ? WHERE id = ?", (pid, token, run_id))
        return self.get_run(run_id)

    def clear_process(self, run_id: str) -> Run:
        with self.connect() as connection:
            connection.execute("UPDATE runs SET process_pid = NULL, process_start_token = NULL WHERE id = ?", (run_id,))
        return self.get_run(run_id)

    def process_owned_and_running(self, run: Run) -> bool:
        if run.process_pid is None or run.process_start_token is None:
            return False
        if self.process_start_token(run.process_pid) != run.process_start_token:
            return False
        try:
            os.kill(run.process_pid, 0)
        except OSError:
            return False
        try:
            state = Path(f"/proc/{run.process_pid}/stat").read_text(encoding="ascii").rsplit(")", 1)[1].split()[0]
        except (OSError, IndexError):
            state = ""
        return state != "Z"

    def heartbeat(self, run_id: str) -> Run:
        with self.connect() as connection:
            connection.execute("UPDATE runs SET last_heartbeat = ? WHERE id = ?", (utc_now(), run_id))
        return self.get_run(run_id)

    def set_inbound_policy(self, run_id: str, policy: str) -> Run:
        if policy not in {"accept", "hold", "refuse"}: raise ValueError("invalid inbound policy")
        with self.connect() as connection: connection.execute("UPDATE runs SET inbound_policy = ? WHERE id = ?", (policy, run_id))
        return self.get_run(run_id)

    def set_readiness(self, run_id: str, readiness: str, *, error: str | None = None) -> Run:
        if readiness not in {"offline", "hello", "ready", "busy", "idle", "stopping", "failed"}: raise ValueError("invalid readiness state")
        with self.connect() as connection: connection.execute("UPDATE runs SET readiness = ?, adapter_error = ? WHERE id = ?", (readiness, error, run_id))
        return self.get_run(run_id)

    # ---------- teams ----------
    def _team_exists(self, reference: str) -> bool:
        with self.connect() as connection:
            return connection.execute("SELECT 1 FROM teams WHERE id = ? OR name = ?", (reference, reference)).fetchone() is not None

    def create_team(self, *, name: str, metadata: Mapping[str, Any] | None = None, team_id: str | None = None) -> Team:
        name = _clean_name(name, "team name")
        metadata_json = _json_object(metadata or {}, "team metadata")
        team_id = team_id or _token("team"); now = utc_now()
        try:
            with self.connect() as connection:
                connection.execute("INSERT INTO teams (id, name, status, metadata_json, created_at, updated_at) VALUES (?, ?, 'created', ?, ?, ?)", (team_id, name, metadata_json, now, now))
        except sqlite3.IntegrityError as exc: raise ValueError(f"team name or ID already exists: {name}") from exc
        self.emit_hook("team.created", {"team_id": team_id, "name": name})
        return self.get_team(team_id)

    def get_team(self, reference: str) -> Team:
        with self.connect() as connection: row = connection.execute("SELECT * FROM teams WHERE id = ? OR name = ? LIMIT 1", (reference, reference)).fetchone()
        if row is None: raise KeyError(f"team not found: {reference}")
        return Team.from_row(row)

    def list_teams(self, limit: int = 50) -> list[Team]:
        with self.connect() as connection: rows = connection.execute("SELECT * FROM teams ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        return [Team.from_row(row) for row in rows]

    def update_team(self, team_id: str, status: str) -> Team:
        if status not in {"created", "starting", "running", "partial", "stopping", "stopped", "failed"}: raise ValueError("invalid team status")
        now = utc_now()
        with self.connect() as connection:
            connection.execute("UPDATE teams SET status = ?, updated_at = ?, stopped_at = CASE WHEN ? = 'stopped' THEN ? WHEN ? IN ('starting', 'running', 'partial') THEN NULL ELSE stopped_at END WHERE id = ?", (status, now, status, now, status, team_id))
        self.emit_hook("team.lifecycle", {"team_id": team_id, "status": status})
        self.audit("team.lifecycle", None, team_id, {"status": status})
        return self.get_team(team_id)

    def add_team_member(self, team_reference: str, run_reference: str, *, role: str = "member", is_lead: bool = False) -> TeamMember:
        team = self.get_team(team_reference); run = self.get_run(run_reference); role = _clean_name(role, "member role")
        now = utc_now()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            other = connection.execute("SELECT team_id FROM team_members WHERE run_id = ? AND removed_at IS NULL AND team_id != ?", (run.id, team.id)).fetchone()
            if other is not None: raise ValueError("run is already an active member of another team")
            if is_lead:
                existing = connection.execute("SELECT run_id FROM team_members WHERE team_id = ? AND is_lead = 1 AND removed_at IS NULL AND run_id != ?", (team.id, run.id)).fetchone()
                if existing is not None: raise ValueError("team already has a lead")
            existing = connection.execute("SELECT 1 FROM team_members WHERE team_id = ? AND run_id = ?", (team.id, run.id)).fetchone()
            if existing:
                connection.execute("UPDATE team_members SET role = ?, is_lead = ?, removed_at = NULL WHERE team_id = ? AND run_id = ?", (role, int(is_lead), team.id, run.id))
            else:
                connection.execute("INSERT INTO team_members (team_id, run_id, role, is_lead, joined_at) VALUES (?, ?, ?, ?, ?)", (team.id, run.id, role, int(is_lead), now))
            connection.execute("UPDATE runs SET team_id = ?, team = ?, role = ?, is_lead = ? WHERE id = ?", (team.id, team.name, role, int(is_lead), run.id))
        self.emit_hook("member.joined", {"team_id": team.id, "run_id": run.id, "role": role, "is_lead": is_lead}, actor_run_id=run.id)
        return self.get_team_member(team.id, run.id)

    def get_team_member(self, team_id: str, run_id: str) -> TeamMember:
        with self.connect() as connection:
            row = connection.execute("""SELECT tm.team_id, tm.run_id, tm.role AS member_role, tm.is_lead AS member_is_lead,
                tm.joined_at, tm.removed_at, r.* FROM team_members tm JOIN runs r ON r.id = tm.run_id WHERE tm.team_id = ? AND tm.run_id = ?""", (team_id, run_id)).fetchone()
        if row is None: raise KeyError("team member not found")
        return TeamMember.from_row(row)

    def list_team_members(self, team_reference: str, *, active_only: bool = True) -> list[TeamMember]:
        team = self.get_team(team_reference); clause = "AND tm.removed_at IS NULL" if active_only else ""
        with self.connect() as connection:
            rows = connection.execute(f"""SELECT tm.team_id, tm.run_id, tm.role AS member_role, tm.is_lead AS member_is_lead,
                tm.joined_at, tm.removed_at, r.* FROM team_members tm JOIN runs r ON r.id = tm.run_id WHERE tm.team_id = ? {clause} ORDER BY tm.joined_at""", (team.id,)).fetchall()
        return [TeamMember.from_row(row) for row in rows]

    def remove_team_member(self, team_reference: str, run_reference: str) -> TeamMember:
        team = self.get_team(team_reference); run = self.get_run(run_reference)
        with self.connect() as connection:
            updated = connection.execute("UPDATE team_members SET removed_at = COALESCE(removed_at, ?) WHERE team_id = ? AND run_id = ? AND removed_at IS NULL", (utc_now(), team.id, run.id))
            if updated.rowcount != 1: raise ValueError("run is not an active member of this team")
            connection.execute("UPDATE runs SET team_id = NULL, team = NULL, role = 'member', is_lead = 0 WHERE id = ?", (run.id,))
        self.emit_hook("member.removed", {"team_id": team.id, "run_id": run.id}, actor_run_id=run.id)
        return self.get_team_member(team.id, run.id)

    def is_team_lead(self, team_id: str | None, run_id: str) -> bool:
        if not team_id: return False
        with self.connect() as connection: return connection.execute("SELECT 1 FROM team_members WHERE team_id = ? AND run_id = ? AND is_lead = 1 AND removed_at IS NULL", (team_id, run_id)).fetchone() is not None

    # ---------- messages ----------
    def create_message(self, *, from_run_id: str, to_run_id: str, body: str, idempotency_key: str | None = None, reply_to: str | None = None) -> Message:
        body = validate_body(body)
        if from_run_id == to_run_id: raise ValueError("a run cannot send a message to itself")
        self.get_run(from_run_id); recipient = self.get_run(to_run_id)
        if idempotency_key: idempotency_key = _clean_name(idempotency_key, "idempotency key")
        hop_count = 0
        if reply_to:
            parent = self.get_message(reply_to)
            if parent.to_run_id != from_run_id or parent.from_run_id != to_run_id: raise ValueError("reply must be sent from the original recipient to the original sender")
            hop_count = parent.hop_count + 1
            if hop_count > MAX_REPLY_HOPS: raise ValueError(f"reply chain exceeds the {MAX_REPLY_HOPS}-hop limit")
        digest, now = hashlib.sha256(body.encode("utf-8")).hexdigest(), utc_now()
        minute_ago = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=1)).isoformat(timespec="seconds")
        status = {"accept": "queued", "hold": "held", "refuse": "refused"}[recipient.inbound_policy]
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if idempotency_key:
                existing = connection.execute("SELECT * FROM messages WHERE from_run_id = ? AND idempotency_key = ?", (from_run_id, idempotency_key)).fetchone()
                if existing:
                    if existing["body_sha256"] != digest or existing["to_run_id"] != to_run_id or existing["reply_to"] != reply_to: raise ValueError("idempotency key was already used for a different message")
                    return Message.from_row(existing)
            if status != "refused":
                active = connection.execute("SELECT COUNT(*) FROM messages WHERE to_run_id = ? AND status IN ('queued', 'held', 'delivered', 'failed')", (to_run_id,)).fetchone()[0]
                if active >= recipient.max_inbox: raise ValueError(f"recipient inbox is full (limit {recipient.max_inbox})")
            recent = connection.execute("SELECT COUNT(*) FROM messages WHERE from_run_id = ? AND to_run_id = ? AND created_at >= ?", (from_run_id, to_run_id, minute_ago)).fetchone()[0]
            if recent >= MAX_MESSAGES_PER_MINUTE: raise ValueError("message rate limit exceeded for this sender and recipient")
            message_id = _token("msg")
            connection.execute("""INSERT INTO messages (id, from_run_id, to_run_id, body, body_sha256, status, idempotency_key, reply_to, hop_count, created_at, held_at, refused_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (message_id, from_run_id, to_run_id, body, digest, status, idempotency_key, reply_to, hop_count, now, now if status == "held" else None, now if status == "refused" else None))
        self.emit_hook("message.received", {"message_id": message_id, "from_run_id": from_run_id, "to_run_id": to_run_id}, actor_run_id=from_run_id)
        return self.get_message(message_id)

    def get_message(self, message_id: str) -> Message:
        with self.connect() as connection: row = connection.execute("SELECT * FROM messages WHERE id = ?", (message_id,)).fetchone()
        if row is None: raise KeyError(f"message not found: {message_id}")
        return Message.from_row(row)

    def list_messages(self, to_run_id: str, *, pending_only: bool = False) -> list[Message]:
        query, params = "SELECT * FROM messages WHERE to_run_id = ?", [to_run_id]
        if pending_only: query += " AND status IN ('queued', 'held', 'delivered', 'failed')"
        query += " ORDER BY created_at ASC"
        with self.connect() as connection: rows = connection.execute(query, params).fetchall()
        return [Message.from_row(row) for row in rows]

    def claim_delivery(self, message_id: str, claim_id: str, lease_seconds: int = DELIVERY_LEASE_SECONDS) -> bool:
        if not claim_id or lease_seconds <= 0: raise ValueError("delivery claim is invalid")
        now = dt.datetime.now(dt.timezone.utc); expires = (now + dt.timedelta(seconds=lease_seconds)).isoformat(timespec="seconds")
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            result = connection.execute("""UPDATE messages SET claimed_by = ?, claim_expires_at = ?, delivery_attempts = delivery_attempts + 1
                WHERE id = ? AND status IN ('queued', 'failed') AND (claim_expires_at IS NULL OR claim_expires_at <= ?)""", (claim_id, expires, message_id, now.isoformat(timespec="seconds")))
            return result.rowcount == 1

    def release_delivery_claim(self, message_id: str, claim_id: str) -> Message:
        with self.connect() as connection: connection.execute("UPDATE messages SET claimed_by = NULL, claim_expires_at = NULL WHERE id = ? AND claimed_by = ?", (message_id, claim_id))
        return self.get_message(message_id)

    def mark_delivered(self, message_id: str, claim_id: str | None = None) -> Message:
        message = self.get_message(message_id)
        if message.status in {"delivered", "acknowledged"}: return message
        if message.status not in {"queued", "failed"}: raise ValueError("only queued or failed messages can be marked delivered")
        params: list[Any] = [utc_now(), utc_now(), message_id]
        condition = "id = ? AND status IN ('queued', 'failed')"
        if claim_id is not None: condition += " AND claimed_by = ?"; params.append(claim_id)
        with self.connect() as connection:
            updated = connection.execute(f"UPDATE messages SET status = 'delivered', delivered_at = COALESCE(delivered_at, ?), adapter_accepted_at = COALESCE(adapter_accepted_at, ?), error = NULL, claimed_by = NULL, claim_expires_at = NULL WHERE {condition}", params)
            if updated.rowcount != 1 and self.get_message(message_id).status not in {"delivered", "acknowledged"}: raise ValueError("message delivery claim is no longer valid")
        delivered = self.get_message(message_id)
        self.emit_hook("message.delivered", {"message_id": message_id, "to_run_id": delivered.to_run_id}, actor_run_id=delivered.from_run_id)
        self.audit("message.delivered", delivered.from_run_id, message_id, {"to_run_id": delivered.to_run_id})
        return delivered

    def mark_failed(self, message_id: str, error: str, claim_id: str | None = None) -> Message:
        message = self.get_message(message_id)
        if message.status not in {"queued", "failed"}: raise ValueError("only queued or failed messages can be marked failed")
        params: list[Any] = [str(error)[:1000], message_id]; condition = "id = ? AND status IN ('queued', 'failed')"
        if claim_id is not None: condition += " AND claimed_by = ?"; params.append(claim_id)
        with self.connect() as connection:
            updated = connection.execute(f"UPDATE messages SET status = 'failed', error = ?, claimed_by = NULL, claim_expires_at = NULL WHERE {condition}", params)
            if updated.rowcount != 1: raise ValueError("message delivery claim is no longer valid")
        self.emit_hook("message.delivery_failure", {"message_id": message_id, "error": str(error)[:256]}, actor_run_id=message.from_run_id)
        self.audit("message.delivery_failure", message.from_run_id, message_id, {"error": str(error)[:256]})
        return self.get_message(message_id)

    def acknowledge(self, message_id: str, recipient_run_id: str) -> Message:
        message = self.get_message(message_id)
        if message.to_run_id != recipient_run_id: raise ValueError("only the recipient run can acknowledge this message")
        if message.status == "acknowledged": return message
        if message.status != "delivered": raise ValueError("only a delivered message can be acknowledged")
        with self.connect() as connection: connection.execute("UPDATE messages SET status = 'acknowledged', acknowledged_at = ? WHERE id = ? AND status = 'delivered'", (utc_now(), message_id))
        self.emit_hook("message.acknowledged", {"message_id": message_id, "run_id": recipient_run_id}, actor_run_id=recipient_run_id)
        self.audit("message.acknowledged", recipient_run_id, message_id, {})
        return self.get_message(message_id)

    def hold_message(self, message_id: str, recipient_run_id: str) -> Message:
        return self._message_action(message_id, recipient_run_id, "held", {"queued", "failed"})

    def accept_message(self, message_id: str, recipient_run_id: str) -> Message:
        message = self.get_message(message_id)
        if message.to_run_id != recipient_run_id: raise ValueError("only the recipient run can accept this message")
        if message.status == "queued": return message
        if message.status != "held": raise ValueError("only a held message can be accepted")
        with self.connect() as connection: connection.execute("UPDATE messages SET status = 'queued', held_at = NULL, error = NULL WHERE id = ? AND status = 'held'", (message_id,))
        return self.get_message(message_id)

    def refuse_message(self, message_id: str, recipient_run_id: str) -> Message:
        return self._message_action(message_id, recipient_run_id, "refused", {"queued", "held", "failed"})

    def _message_action(self, message_id: str, recipient_run_id: str, status: str, allowed: set[str]) -> Message:
        message = self.get_message(message_id)
        if message.to_run_id != recipient_run_id: raise ValueError("only the recipient run can control this message")
        if message.status == status: return message
        if message.status not in allowed: raise ValueError("message cannot be changed in its current state")
        timestamp_field = "held_at" if status == "held" else "refused_at"
        with self.connect() as connection: connection.execute(f"UPDATE messages SET status = ?, {timestamp_field} = ?, claimed_by = NULL, claim_expires_at = NULL WHERE id = ? AND status IN ({','.join('?' for _ in allowed)})", (status, utc_now(), message_id, *allowed))
        return self.get_message(message_id)

    def message_counts(self, to_run_id: str) -> dict[str, int]:
        with self.connect() as connection: rows = connection.execute("SELECT status, COUNT(*) AS count FROM messages WHERE to_run_id = ? GROUP BY status", (to_run_id,)).fetchall()
        counts = {status: 0 for status in ("queued", "held", "delivered", "acknowledged", "failed", "refused")}; counts.update({row["status"]: row["count"] for row in rows}); return counts

    def iter_pending(self, to_run_id: str | None = None) -> Iterable[Message]:
        query, params = "SELECT * FROM messages WHERE status IN ('queued', 'failed')", []
        if to_run_id: query += " AND to_run_id = ?"; params.append(to_run_id)
        query += " ORDER BY created_at ASC"
        with self.connect() as connection: rows = connection.execute(query, params).fetchall()
        return iter(Message.from_row(row) for row in rows)

    # ---------- governance and reports ----------
    def create_task(self, *, title: str, created_by: str, description: str = "", assigned_to: str | None = None,
                    depends_on: Iterable[str] = (), requires_approval: bool = False,
                    status: str | None = None) -> Task:
        title = validate_body(title); description = description.strip()
        if len(description) > MAX_MESSAGE_CHARS or any(ord(char) < 32 and char not in "\n\t" for char in description): raise ValueError("task description is invalid or too large")
        if not isinstance(requires_approval, bool):
            raise ValueError("requires_approval must be boolean")
        creator = self.get_run(created_by); assignee = self.get_run(assigned_to) if assigned_to else None
        team_id = creator.team_id
        if assignee and team_id and assignee.team_id != team_id: raise ValueError("assigned run must be in the task creator's team")
        dependencies = tuple(dict.fromkeys(depends_on))
        if any(not dependency for dependency in dependencies): raise ValueError("task dependency IDs cannot be empty")
        if status is not None and status not in {"proposed", "awaiting_approval", "pending"}:
            raise ValueError("task status must be proposed, awaiting_approval, or pending")
        if status is None:
            status = "awaiting_approval" if requires_approval else "pending"
        if requires_approval and status == "pending":
            status = "awaiting_approval"
        now = utc_now(); task_id = _token("task")
        with self.connect() as connection:
            for dependency in dependencies:
                row = connection.execute("SELECT team_id FROM tasks WHERE id = ?", (dependency,)).fetchone()
                if row is None: raise ValueError(f"task dependency not found: {dependency}")
                if team_id and row[0] not in {None, team_id}: raise ValueError("task dependency crosses team boundary")
            connection.execute("INSERT INTO tasks (id, title, description, created_by, assigned_to, team_id, status, requires_approval, depends_on, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (task_id, title, description, creator.id, assignee.id if assignee else None, team_id, status, int(requires_approval), json.dumps(dependencies), now, now))
        self.emit_hook("task.created", {"task_id": task_id, "team_id": team_id, "requires_approval": requires_approval}, actor_run_id=creator.id)
        self.audit("task.created", creator.id, task_id, {"requires_approval": requires_approval})
        return self.get_task(task_id)

    def get_task(self, task_id: str) -> Task:
        with self.connect() as connection: row = connection.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        if row is None: raise KeyError(f"task not found: {task_id}")
        return Task.from_row(row)

    def list_tasks(self, status: str | None = None, *, team: str | None = None) -> list[Task]:
        valid = {"proposed", "awaiting_approval", "approved", "pending", "in_progress", "completed", "failed", "cancelled", "rejected", "blocked"}
        if status and status not in valid: raise ValueError("invalid task status")
        query, clauses, params = "SELECT * FROM tasks", [], []
        if status: clauses.append("status = ?"); params.append(status)
        if team: clauses.append("team_id = ?"); params.append(self.get_team(team).id)
        if clauses: query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY created_at ASC"
        with self.connect() as connection: rows = connection.execute(query, params).fetchall()
        return [Task.from_row(row) for row in rows]

    def _authorized_governance_actor(self, task: Task, actor: str) -> Run:
        run = self.get_run(actor)
        if self.is_operator(run.id) or self.is_team_lead(task.team_id, run.id):
            return run
        raise ValueError("only an authorized operator or designated team lead may govern this task")

    def submit_task(self, task_id: str, actor_run_id: str) -> Task:
        task = self.get_task(task_id); actor = self._authorized_governance_actor(task, actor_run_id)
        if task.status != "proposed":
            raise ValueError("task is not proposed")
        with self.connect() as connection:
            connection.execute("UPDATE tasks SET status = 'awaiting_approval', updated_at = ? WHERE id = ? AND status = 'proposed'", (utc_now(), task_id))
        self.audit("task.submitted", actor.id, task_id, {})
        return self.get_task(task_id)

    def approve_task(self, task_id: str, actor_run_id: str) -> Task:
        task = self.get_task(task_id); actor = self._authorized_governance_actor(task, actor_run_id)
        if task.status != "awaiting_approval": raise ValueError("task is not awaiting approval")
        self._run_hooks("task.approved", {"task_id": task_id, "actor_run_id": actor.id}, gate=True)
        now = utc_now()
        with self.connect() as connection: connection.execute("UPDATE tasks SET status = 'approved', approved_by = ?, approved_at = ?, updated_at = ? WHERE id = ? AND status = 'awaiting_approval'", (actor.id, now, now, task_id))
        self.audit("task.approved", actor.id, task_id, {}); return self.get_task(task_id)

    def reject_task(self, task_id: str, actor_run_id: str, reason: str = "") -> Task:
        task = self.get_task(task_id); actor = self._authorized_governance_actor(task, actor_run_id); reason = reason.strip()
        if len(reason) > 1000: raise ValueError("rejection reason is too long")
        if task.status != "awaiting_approval": raise ValueError("task is not awaiting approval")
        self._run_hooks("task.rejected", {"task_id": task_id, "actor_run_id": actor.id, "reason": reason}, gate=True)
        with self.connect() as connection: connection.execute("UPDATE tasks SET status = 'rejected', rejection_reason = ?, updated_at = ? WHERE id = ? AND status = 'awaiting_approval'", (reason, utc_now(), task_id))
        self.audit("task.rejected", actor.id, task_id, {"reason": reason}); return self.get_task(task_id)

    def block_task(self, task_id: str, actor_run_id: str, reason: str = "") -> Task:
        task = self.get_task(task_id); actor = self._authorized_governance_actor(task, actor_run_id); reason = reason.strip()[:1000]
        with self.connect() as connection: connection.execute("UPDATE tasks SET status = 'blocked', blocked_reason = ?, updated_at = ? WHERE id = ? AND status IN ('pending', 'approved', 'awaiting_approval')", (reason, utc_now(), task_id))
        self.audit("task.blocked", actor.id, task_id, {"reason": reason}); return self.get_task(task_id)

    def claim_task(self, task_id: str, run_id: str) -> Task:
        claimant = self.get_run(run_id)
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE"); row = connection.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
            if row is None: raise KeyError(f"task not found: {task_id}")
            if row["status"] == "awaiting_approval": raise ValueError("task requires approval before it can be claimed")
            if row["status"] in {"rejected", "blocked", "cancelled"}: raise ValueError("task is not available for claiming")
            if row["status"] not in {"pending", "approved"}: raise ValueError("task is not available for claiming")
            if row["assigned_to"] is not None and row["assigned_to"] != run_id: raise ValueError("task is assigned to another run")
            if row["team_id"] and claimant.team_id != row["team_id"]: raise ValueError("task is outside the run's team")
            dependencies = json.loads(row["depends_on"])
            if dependencies:
                placeholders = ",".join("?" for _ in dependencies); found = connection.execute(f"SELECT COUNT(*) FROM tasks WHERE id IN ({placeholders})", dependencies).fetchone()[0]
                if found != len(dependencies): raise ValueError("task dependency is missing")
                if connection.execute(f"SELECT COUNT(*) FROM tasks WHERE id IN ({placeholders}) AND status != 'completed'", dependencies).fetchone()[0]: raise ValueError("task dependencies are not complete")
            updated = connection.execute("UPDATE tasks SET status = 'in_progress', assigned_to = ?, updated_at = ? WHERE id = ? AND status IN ('pending', 'approved')", (run_id, utc_now(), task_id))
            if updated.rowcount != 1: raise ValueError("task was claimed by another run")
        self.emit_hook("task.claimed", {"task_id": task_id, "run_id": run_id}, actor_run_id=run_id)
        self.audit("task.claimed", run_id, task_id, {})
        return self.get_task(task_id)

    def complete_task(self, task_id: str, run_id: str, *, report: Mapping[str, Any] | None = None) -> Task:
        normalized = validate_report(report) if report is not None else None
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE"); row = connection.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
            if row is None: raise KeyError(f"task not found: {task_id}")
            if row["assigned_to"] != run_id: raise ValueError("only the assigned run can complete this task")
            if row["status"] != "in_progress": raise ValueError("only an in-progress task can be completed")
            now = utc_now(); connection.execute("UPDATE tasks SET status = 'completed', updated_at = ?, completed_at = ? WHERE id = ?", (now, now, task_id))
            if normalized is not None: self._insert_report(connection, task_id, run_id, normalized, now)
        self.emit_hook("task.completed", {"task_id": task_id, "run_id": run_id, "has_report": normalized is not None}, actor_run_id=run_id)
        self.audit("task.completed", run_id, task_id, {"has_report": normalized is not None})
        return self.get_task(task_id)

    def fail_task(self, task_id: str, run_id: str, *, report: Mapping[str, Any] | None = None) -> Task:
        normalized = validate_report(report) if report is not None else None
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE"); row = connection.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
            if row is None: raise KeyError(f"task not found: {task_id}")
            if row["assigned_to"] != run_id or row["status"] != "in_progress": raise ValueError("only the assigned in-progress run can fail this task")
            now = utc_now(); connection.execute("UPDATE tasks SET status = 'failed', updated_at = ? WHERE id = ?", (now, task_id))
            if normalized is not None: self._insert_report(connection, task_id, run_id, normalized, now)
        self.emit_hook("task.failed", {"task_id": task_id, "run_id": run_id, "has_report": normalized is not None}, actor_run_id=run_id)
        self.audit("task.failed", run_id, task_id, {"has_report": normalized is not None})
        return self.get_task(task_id)

    def add_task_report(self, task_id: str, run_id: str, report: Mapping[str, Any]) -> CompletionReport:
        normalized = validate_report(report); task = self.get_task(task_id); actor = self.get_run(run_id)
        if task.status not in {"completed", "rejected", "failed"}: raise ValueError("reports may only be attached to completed, rejected, or failed tasks")
        authorized = actor.id in {task.created_by, task.assigned_to} or self.is_operator(actor.id) or self.is_team_lead(task.team_id, actor.id)
        if not authorized:
            raise ValueError("actor cannot attach this report")
        now = utc_now()
        with self.connect() as connection: report_id = self._insert_report(connection, task_id, actor.id, normalized, now)
        self.emit_hook("task.reported", {"task_id": task_id, "report_id": report_id}, actor_run_id=actor.id)
        self.audit("task.reported", actor.id, task_id, {"report_id": report_id})
        return self.get_task_reports(task_id)[-1]

    def _insert_report(self, connection: sqlite3.Connection, task_id: str, run_id: str, report: Mapping[str, Any], now: str) -> str:
        report_id = _token("report")
        connection.execute("INSERT INTO reports (id, task_id, created_by, goal, verified_facts_json, tests_json, files_changed_json, blockers_json, next_action, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (report_id, task_id, run_id, report["goal"], json.dumps(report["verified_facts"], ensure_ascii=False), json.dumps(report["tests"], ensure_ascii=False), json.dumps(report["files_changed"], ensure_ascii=False), json.dumps(report["blockers"], ensure_ascii=False), report["next_action"], now))
        return report_id

    def get_task_reports(self, task_id: str) -> list[CompletionReport]:
        self.get_task(task_id)
        with self.connect() as connection: rows = connection.execute("SELECT * FROM reports WHERE task_id = ? ORDER BY created_at", (task_id,)).fetchall()
        return [CompletionReport.from_row(row) for row in rows]

    # ---------- operators and hooks ----------
    def grant_operator(self, run_id: str) -> Run:
        run = self.get_run(run_id)
        with self.connect() as connection: connection.execute("INSERT OR REPLACE INTO operators (run_id, granted_at) VALUES (?, ?)", (run.id, utc_now()))
        self.audit("operator.granted", run.id, run.id, {}); return run

    def revoke_operator(self, run_id: str) -> None:
        run = self.get_run(run_id)
        with self.connect() as connection: connection.execute("DELETE FROM operators WHERE run_id = ?", (run.id,))
        self.audit("operator.revoked", run.id, run.id, {})

    def is_operator(self, run_id: str) -> bool:
        with self.connect() as connection: return connection.execute("SELECT 1 FROM operators WHERE run_id = ?", (run_id,)).fetchone() is not None

    def add_hook(self, *, name: str, event: str, command: Sequence[str], timeout: float = 5, max_output: int = 8192, fail_closed: bool = False) -> Hook:
        name, event = _clean_name(name, "hook name"), _clean_name(event, "hook event"); command_tuple = validate_command(command)
        with self.connect() as connection:
            if connection.execute("SELECT COUNT(*) FROM hooks WHERE enabled = 1").fetchone()[0] >= MAX_HOOKS:
                raise ValueError(f"hook limit of {MAX_HOOKS} exceeded")
        if timeout <= 0 or timeout > 60 or max_output <= 0 or max_output > 64 * 1024: raise ValueError("hook limits are invalid")
        hook_id = _token("hook")
        try:
            with self.connect() as connection: connection.execute("INSERT INTO hooks (id, name, event, command_json, timeout_seconds, max_output, fail_closed) VALUES (?, ?, ?, ?, ?, ?, ?)", (hook_id, name, event, json.dumps(command_tuple), timeout, max_output, int(fail_closed)))
        except sqlite3.IntegrityError as exc: raise ValueError(f"hook name already exists: {name}") from exc
        return self.get_hook(hook_id)

    def get_hook(self, hook_id: str) -> Hook:
        with self.connect() as connection: row = connection.execute("SELECT * FROM hooks WHERE id = ? OR name = ?", (hook_id, hook_id)).fetchone()
        if row is None: raise KeyError(f"hook not found: {hook_id}")
        return Hook.from_row(row)

    def list_hooks(self, event: str | None = None) -> list[Hook]:
        query, params = "SELECT * FROM hooks WHERE enabled = 1", []
        if event: query += " AND event = ?"; params.append(event)
        query += " ORDER BY name"
        with self.connect() as connection: rows = connection.execute(query, params).fetchall()
        return [Hook.from_row(row) for row in rows]

    def list_hook_events(self, limit: int = 100) -> list[HookEvent]:
        with self.connect() as connection: rows = connection.execute("SELECT * FROM hook_events ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        return [HookEvent.from_row(row) for row in rows]

    def _run_hooks(self, event: str, payload: dict[str, object], *, gate: bool = False) -> None:
        hooks = self.list_hooks(event); event_payload = dict(payload, event=event, timestamp=utc_now())
        for hook in hooks:
            try: result = run_command(hook.command, event_payload, timeout=hook.timeout, max_output=hook.max_output)
            except Exception as exc: result_status, output = "failed", str(exc)[:hook.max_output]
            else: result_status, output = result.status, result.output
            event_id = _token("hook-event")
            with self.connect() as connection: connection.execute("INSERT INTO hook_events (id, hook_id, event, status, output, created_at) VALUES (?, ?, ?, ?, ?, ?)", (event_id, hook.id, event, result_status, output[:hook.max_output], utc_now()))
            if (gate or hook.fail_closed) and result_status != "allowed": raise ValueError(f"hook denied {event}: {output or result_status}")

    def emit_hook(self, event: str, payload: dict[str, object], *, actor_run_id: str | None = None) -> None:
        # Ordinary hooks are observational unless explicitly fail-closed.
        # _run_hooks records every result and raises only for a gate or a
        # fail-closed hook; do not swallow that security-sensitive decision.
        bounded = dict(payload)
        if actor_run_id is not None:
            bounded["actor_run_id"] = actor_run_id
        self._run_hooks(event, bounded)

    def audit(self, event: str, actor_run_id: str | None, resource_id: str | None, detail: Mapping[str, Any]) -> None:
        with self.connect() as connection: connection.execute("INSERT INTO audit_log (id, event, actor_run_id, resource_id, detail_json, created_at) VALUES (?, ?, ?, ?, ?, ?)", (_token("audit"), event, actor_run_id, resource_id, _json_object(detail, "audit detail", 8 * 1024), utc_now()))

    def list_audit(self, limit: int = 100) -> list[dict[str, Any]]:
        with self.connect() as connection: rows = connection.execute("SELECT * FROM audit_log ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            try:
                item["detail"] = json.loads(item["detail_json"])
            except (TypeError, json.JSONDecodeError):
                item["detail"] = {}
            result.append(item)
        return result

    # ---------- adapter/session state ----------
    def adapter_event(self, frame: Mapping[str, object]) -> Run:
        normalized = validate_adapter_frame(dict(frame)); kind = normalized["type"]; run = self.get_run(str(normalized["run_id"]))
        session = normalized.get("session_id")
        if kind == "hello":
            capabilities = normalized.get("capabilities", [])
            if len(capabilities) > MAX_ADAPTER_CAPABILITIES: raise ValueError("too many adapter capabilities")
            with self.connect() as connection: connection.execute("UPDATE runs SET adapter_protocol = ?, adapter_session_id = ?, capabilities_json = ?, readiness = 'hello', adapter_heartbeat_at = ?, adapter_ready_at = NULL, adapter_error = NULL, lifecycle_state = 'running', status = CASE WHEN status = 'starting' THEN 'running' ELSE status END WHERE id = ?", (normalized.get("protocol_version", ADAPTER_PROTOCOL_VERSION), session, json.dumps(capabilities), utc_now(), run.id))
        else:
            if not run.adapter_session_id or session != run.adapter_session_id: raise ValueError("adapter session is not established")
            now = utc_now(); updates: dict[str, Any] = {"adapter_heartbeat_at": now, "adapter_error": None}
            if kind == "ready": updates.update(readiness="ready", adapter_ready_at=now, capabilities_json=json.dumps(normalized.get("capabilities", list(run.capabilities))))
            elif kind == "busy": updates["readiness"] = "busy"
            elif kind == "idle": updates["readiness"] = "idle"
            elif kind == "heartbeat": pass
            elif kind == "shutdown": updates.update(readiness="stopping", lifecycle_state="stopping")
            elif kind == "error": updates.update(readiness="failed", adapter_error=str(normalized["error"]))
            elif kind == "ack":
                message = self.get_message(str(normalized["message_id"]))
                if message.to_run_id != run.id: raise ValueError("adapter cannot acknowledge another run's message")
                if message.status == "delivered": self.acknowledge(message.id, run.id)
            with self.connect() as connection:
                assignments = ", ".join(f"{key} = ?" for key in updates); connection.execute(f"UPDATE runs SET {assignments} WHERE id = ?", (*updates.values(), run.id))
        payload_json = _json_object(normalized, "adapter event", 16 * 1024)
        with self.connect() as connection: connection.execute("INSERT INTO adapter_events (id, run_id, session_id, event_type, payload_json, created_at) VALUES (?, ?, ?, ?, ?, ?)", (_token("adapter-event"), run.id, session, str(kind), payload_json, utc_now()))
        if kind in {"ready", "idle"}: self.emit_hook("adapter.ready", {"run_id": run.id, "session_id": session})
        if kind == "error": self.emit_hook("adapter.failure", {"run_id": run.id, "error": normalized.get("error", "")})
        return self.get_run(run.id)

    def expire_adapter_heartbeats(self, *, ttl_seconds: float = ADAPTER_HEARTBEAT_TTL_SECONDS) -> list[Run]:
        now = dt.datetime.now(dt.timezone.utc); expired: list[Run] = []
        for run in self.list_runs(limit=10_000):
            if not run.adapter_session_id or not run.adapter_heartbeat_at: continue
            try: heartbeat = dt.datetime.fromisoformat(run.adapter_heartbeat_at)
            except ValueError: heartbeat = dt.datetime.min.replace(tzinfo=dt.timezone.utc)
            if ttl_seconds <= 0 or (now - heartbeat).total_seconds() > ttl_seconds:
                with self.connect() as connection: connection.execute("UPDATE runs SET readiness = 'offline', adapter_error = 'heartbeat expired' WHERE id = ? AND adapter_session_id = ?", (run.id, run.adapter_session_id))
                self.emit_hook("adapter.failure", {"run_id": run.id, "error": "heartbeat expired"}); expired.append(self.get_run(run.id))
        return expired

    def adapter_ready(self, run: Run) -> bool:
        if not run.adapter_session_id and not run.readiness_required: return True
        return run.readiness in {"ready", "idle"} and bool(run.adapter_heartbeat_at)

    def list_adapter_events(self, run_reference: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        params: list[object] = []
        query = "SELECT * FROM adapter_events"
        if run_reference:
            run = self.get_run(run_reference)
            query += " WHERE run_id = ?"
            params.append(run.id)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        with self.connect() as connection:
            rows = connection.execute(query, params).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            try:
                item["payload"] = json.loads(item["payload_json"])
            except (TypeError, json.JSONDecodeError):
                item["payload"] = {}
            result.append(item)
        return result
