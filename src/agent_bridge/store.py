from __future__ import annotations

import datetime as dt
import hashlib
import json
import secrets
import sqlite3
from pathlib import Path
from typing import Iterable

from .models import Message, Run, Task
from .protocol import MAX_MESSAGE_CHARS, MAX_REPLY_HOPS, validate_body

DEFAULT_MAX_INBOX = 100
MAX_MESSAGES_PER_MINUTE = 60
DELIVERY_LEASE_SECONDS = 30

SCHEMA = """
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

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
    inbound_policy TEXT NOT NULL DEFAULT 'accept'
        CHECK (inbound_policy IN ('accept', 'hold', 'refuse')),
    max_inbox INTEGER NOT NULL DEFAULT 100 CHECK (max_inbox > 0),
    status TEXT NOT NULL CHECK (status IN ('starting', 'running', 'success', 'failed', 'killed', 'missing')),
    started_at TEXT NOT NULL,
    ended_at TEXT,
    last_heartbeat TEXT,
    log_path TEXT
);

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
    error TEXT
);

CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    created_by TEXT NOT NULL REFERENCES runs(id),
    assigned_to TEXT REFERENCES runs(id),
    status TEXT NOT NULL CHECK (status IN ('pending', 'in_progress', 'completed', 'failed', 'cancelled')),
    depends_on TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_idempotency_sender
    ON messages(from_run_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_messages_recipient_status
    ON messages(to_run_id, status, created_at);
CREATE INDEX IF NOT EXISTS idx_messages_sender
    ON messages(from_run_id, created_at);
CREATE INDEX IF NOT EXISTS idx_tasks_status
    ON tasks(status, updated_at);
"""

NEW_MESSAGES_TABLE = """
CREATE TABLE messages (
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
    error TEXT
)
"""


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def _token(prefix: str) -> str:
    return f"{prefix}-{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d%H%M%S')}-{secrets.token_hex(4)}"


def _clean_name(value: str, field: str) -> str:
    value = value.strip()
    if not value:
        raise ValueError(f"{field} cannot be empty")
    if len(value) > 128:
        raise ValueError(f"{field} cannot exceed 128 characters")
    if any(char in value for char in "\x00\r\n"):
        raise ValueError(f"{field} cannot contain control characters")
    return value


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
            connection.execute("PRAGMA user_version = 2")

    def _migrate_legacy(self, connection: sqlite3.Connection) -> None:
        run_columns = {row[1] for row in connection.execute("PRAGMA table_info(runs)")}
        if run_columns:
            additions = [
                ("inbox_path", "TEXT"),
                ("team", "TEXT"),
                ("inbound_policy", "TEXT NOT NULL DEFAULT 'accept'"),
                ("max_inbox", f"INTEGER NOT NULL DEFAULT {DEFAULT_MAX_INBOX}"),
                ("last_heartbeat", "TEXT"),
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
                """
                INSERT INTO messages
                  (id, from_run_id, to_run_id, body, body_sha256, status,
                   idempotency_key, created_at, delivered_at, acknowledged_at, error)
                SELECT id, from_run_id, to_run_id, body, body_sha256, status,
                       idempotency_key, created_at, delivered_at, acknowledged_at, error
                FROM messages_v1
                """
            )
            connection.execute("DROP TABLE messages_v1")
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_messages_recipient_status ON messages(to_run_id, status, created_at)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages(from_run_id, created_at)"
            )
        connection.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_idempotency_sender "
            "ON messages(from_run_id, idempotency_key) WHERE idempotency_key IS NOT NULL"
        )

    def create_run(
        self,
        *,
        name: str,
        agent: str,
        mode: str,
        command: str,
        cwd: str,
        tmux_session: str | None = None,
        log_path: str | None = None,
        run_id: str | None = None,
        inbox_path: str | None = None,
        team: str | None = None,
        inbound_policy: str = "accept",
        max_inbox: int = DEFAULT_MAX_INBOX,
    ) -> Run:
        name = _clean_name(name, "run name")
        agent = _clean_name(agent, "agent")
        if mode not in {"interactive", "one-shot"}:
            raise ValueError("mode must be interactive or one-shot")
        if not command.strip():
            raise ValueError("command cannot be empty")
        if inbound_policy not in {"accept", "hold", "refuse"}:
            raise ValueError("inbound policy must be accept, hold, or refuse")
        if max_inbox < 1:
            raise ValueError("max inbox must be positive")
        run_id = run_id or _token("run")
        inbox_path = inbox_path or str(self.path.parent / "sockets" / f"{run_id}.sock")
        if len(inbox_path) >= 104:
            raise ValueError("inbox socket path is too long for Unix sockets")
        started_at = utc_now()
        try:
            with self.connect() as connection:
                connection.execute(
                    """
                    INSERT INTO runs
                      (id, name, agent, mode, command, cwd, tmux_session, inbox_path,
                       team, inbound_policy, max_inbox, status, started_at, last_heartbeat, log_path)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'starting', ?, ?, ?)
                    """,
                    (
                        run_id,
                        name,
                        agent,
                        mode,
                        command,
                        cwd,
                        tmux_session,
                        inbox_path,
                        team,
                        inbound_policy,
                        max_inbox,
                        started_at,
                        started_at,
                        log_path,
                    ),
                )
        except sqlite3.IntegrityError as exc:
            raise ValueError(f"run name, inbox path, or tmux session already exists: {name}") from exc
        return self.get_run(run_id)

    def get_run(self, reference: str) -> Run:
        with self.connect() as connection:
            row = connection.execute("SELECT * FROM runs WHERE id = ? LIMIT 1", (reference,)).fetchone()
            if row is None:
                row = connection.execute("SELECT * FROM runs WHERE name = ? LIMIT 1", (reference,)).fetchone()
        if row is None:
            raise KeyError(f"run not found: {reference}")
        return Run.from_row(row)

    def list_runs(
        self, limit: int = 50, *, team: str | None = None, active_only: bool = False
    ) -> list[Run]:
        query = "SELECT * FROM runs"
        clauses: list[str] = []
        params: list[object] = []
        if team:
            clauses.append("team = ?")
            params.append(team)
        if active_only:
            clauses.append("status IN ('starting', 'running', 'missing')")
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY started_at DESC LIMIT ?"
        params.append(limit)
        with self.connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [Run.from_row(row) for row in rows]

    def update_run(self, run_id: str, *, status: str, ended_at: str | None = None) -> Run:
        allowed = {"starting", "running", "success", "failed", "killed", "missing"}
        if status not in allowed:
            raise ValueError(f"invalid run status: {status}")
        with self.connect() as connection:
            connection.execute(
                "UPDATE runs SET status = ?, ended_at = COALESCE(?, ended_at), last_heartbeat = ? WHERE id = ?",
                (status, ended_at, utc_now(), run_id),
            )
        return self.get_run(run_id)

    def heartbeat(self, run_id: str) -> Run:
        with self.connect() as connection:
            connection.execute("UPDATE runs SET last_heartbeat = ? WHERE id = ?", (utc_now(), run_id))
        return self.get_run(run_id)

    def set_inbound_policy(self, run_id: str, policy: str) -> Run:
        if policy not in {"accept", "hold", "refuse"}:
            raise ValueError("inbound policy must be accept, hold, or refuse")
        with self.connect() as connection:
            connection.execute("UPDATE runs SET inbound_policy = ? WHERE id = ?", (policy, run_id))
        return self.get_run(run_id)

    def create_message(
        self,
        *,
        from_run_id: str,
        to_run_id: str,
        body: str,
        idempotency_key: str | None = None,
        reply_to: str | None = None,
    ) -> Message:
        body = validate_body(body)
        if from_run_id == to_run_id:
            raise ValueError("a run cannot send a message to itself")
        self.get_run(from_run_id)
        recipient = self.get_run(to_run_id)
        if idempotency_key:
            idempotency_key = _clean_name(idempotency_key, "idempotency key")

        hop_count = 0
        if reply_to:
            parent = self.get_message(reply_to)
            if parent.to_run_id != from_run_id or parent.from_run_id != to_run_id:
                raise ValueError("reply must be sent from the original recipient to the original sender")
            hop_count = parent.hop_count + 1
            if hop_count > MAX_REPLY_HOPS:
                raise ValueError(f"reply chain exceeds the {MAX_REPLY_HOPS}-hop limit")

        digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
        now = utc_now()
        minute_ago = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=1)).isoformat(timespec="seconds")
        status = {
            "accept": "queued",
            "hold": "held",
            "refuse": "refused",
        }[recipient.inbound_policy]
        held_at = now if status == "held" else None
        refused_at = now if status == "refused" else None
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if idempotency_key:
                existing = connection.execute(
                    "SELECT * FROM messages WHERE from_run_id = ? AND idempotency_key = ?",
                    (from_run_id, idempotency_key),
                ).fetchone()
                if existing is not None:
                    if (
                        existing["body_sha256"] != digest
                        or existing["to_run_id"] != to_run_id
                        or existing["reply_to"] != reply_to
                    ):
                        raise ValueError("idempotency key was already used for a different message")
                    return Message.from_row(existing)

            if status != "refused":
                active_count = connection.execute(
                    """
                    SELECT COUNT(*) FROM messages
                    WHERE to_run_id = ? AND status IN ('queued', 'held', 'delivered', 'failed')
                    """,
                    (to_run_id,),
                ).fetchone()[0]
                if active_count >= recipient.max_inbox:
                    raise ValueError(f"recipient inbox is full (limit {recipient.max_inbox})")
            recent_count = connection.execute(
                """
                SELECT COUNT(*) FROM messages
                WHERE from_run_id = ? AND to_run_id = ? AND created_at >= ?
                """,
                (from_run_id, to_run_id, minute_ago),
            ).fetchone()[0]
            if recent_count >= MAX_MESSAGES_PER_MINUTE:
                raise ValueError("message rate limit exceeded for this sender and recipient")

            message_id = _token("msg")
            connection.execute(
                """
                INSERT INTO messages
                  (id, from_run_id, to_run_id, body, body_sha256, status,
                   idempotency_key, reply_to, hop_count, created_at, held_at, refused_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    message_id,
                    from_run_id,
                    to_run_id,
                    body,
                    digest,
                    status,
                    idempotency_key,
                    reply_to,
                    hop_count,
                    now,
                    held_at,
                    refused_at,
                ),
            )
        return self.get_message(message_id)

    def get_message(self, message_id: str) -> Message:
        with self.connect() as connection:
            row = connection.execute("SELECT * FROM messages WHERE id = ?", (message_id,)).fetchone()
        if row is None:
            raise KeyError(f"message not found: {message_id}")
        return Message.from_row(row)

    def list_messages(self, to_run_id: str, *, pending_only: bool = False) -> list[Message]:
        query = "SELECT * FROM messages WHERE to_run_id = ?"
        params: list[object] = [to_run_id]
        if pending_only:
            query += " AND status IN ('queued', 'held', 'delivered', 'failed')"
        query += " ORDER BY created_at ASC"
        with self.connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [Message.from_row(row) for row in rows]

    def claim_delivery(self, message_id: str, claim_id: str, lease_seconds: int = DELIVERY_LEASE_SECONDS) -> bool:
        if not claim_id:
            raise ValueError("delivery claim ID cannot be empty")
        if lease_seconds <= 0:
            raise ValueError("delivery lease must be positive")
        now = dt.datetime.now(dt.timezone.utc)
        expires = (now + dt.timedelta(seconds=lease_seconds)).isoformat(timespec="seconds")
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            result = connection.execute(
                """
                UPDATE messages
                SET claimed_by = ?, claim_expires_at = ?,
                    delivery_attempts = delivery_attempts + 1
                WHERE id = ? AND status IN ('queued', 'failed')
                  AND (claim_expires_at IS NULL OR claim_expires_at <= ?)
                """,
                (claim_id, expires, message_id, now.isoformat(timespec="seconds")),
            )
            return result.rowcount == 1

    def release_delivery_claim(self, message_id: str, claim_id: str) -> Message:
        with self.connect() as connection:
            connection.execute(
                "UPDATE messages SET claimed_by = NULL, claim_expires_at = NULL WHERE id = ? AND claimed_by = ?",
                (message_id, claim_id),
            )
        return self.get_message(message_id)

    def mark_delivered(self, message_id: str, claim_id: str | None = None) -> Message:
        message = self.get_message(message_id)
        if message.status in {"delivered", "acknowledged"}:
            return message
        if message.status not in {"queued", "failed"}:
            raise ValueError("only queued or failed messages can be marked delivered")
        with self.connect() as connection:
            if claim_id is None:
                updated = connection.execute(
                    """
                    UPDATE messages
                    SET status = 'delivered', delivered_at = COALESCE(delivered_at, ?),
                        error = NULL, claimed_by = NULL, claim_expires_at = NULL
                    WHERE id = ? AND status IN ('queued', 'failed')
                    """,
                    (utc_now(), message_id),
                )
            else:
                updated = connection.execute(
                    """
                    UPDATE messages
                    SET status = 'delivered', delivered_at = COALESCE(delivered_at, ?),
                        error = NULL, claimed_by = NULL, claim_expires_at = NULL
                    WHERE id = ? AND status IN ('queued', 'failed') AND claimed_by = ?
                    """,
                    (utc_now(), message_id, claim_id),
                )
            if updated.rowcount != 1:
                current = self.get_message(message_id)
                if current.status not in {"delivered", "acknowledged"}:
                    raise ValueError("message delivery claim is no longer valid")
        return self.get_message(message_id)

    def mark_failed(self, message_id: str, error: str, claim_id: str | None = None) -> Message:
        message = self.get_message(message_id)
        if message.status not in {"queued", "failed"}:
            raise ValueError("only queued or failed messages can be marked failed")
        with self.connect() as connection:
            if claim_id is None:
                updated = connection.execute(
                    """
                    UPDATE messages
                    SET status = 'failed', error = ?, claimed_by = NULL, claim_expires_at = NULL
                    WHERE id = ? AND status IN ('queued', 'failed')
                    """,
                    (error[:1000], message_id),
                )
            else:
                updated = connection.execute(
                    """
                    UPDATE messages SET status = 'failed', error = ?, claimed_by = NULL, claim_expires_at = NULL
                    WHERE id = ? AND status IN ('queued', 'failed') AND claimed_by = ?
                    """,
                    (error[:1000], message_id, claim_id),
                )
            if updated.rowcount != 1:
                raise ValueError("message delivery claim is no longer valid")
        return self.get_message(message_id)

    def acknowledge(self, message_id: str, recipient_run_id: str) -> Message:
        message = self.get_message(message_id)
        if message.to_run_id != recipient_run_id:
            raise ValueError("only the recipient run can acknowledge this message")
        if message.status == "acknowledged":
            return message
        if message.status != "delivered":
            raise ValueError("only a delivered message can be acknowledged")
        with self.connect() as connection:
            connection.execute(
                "UPDATE messages SET status = 'acknowledged', acknowledged_at = ? WHERE id = ? AND status = 'delivered'",
                (utc_now(), message_id),
            )
        return self.get_message(message_id)

    def hold_message(self, message_id: str, recipient_run_id: str) -> Message:
        message = self.get_message(message_id)
        if message.to_run_id != recipient_run_id:
            raise ValueError("only the recipient run can hold this message")
        if message.status == "held":
            return message
        if message.status not in {"queued", "failed"}:
            raise ValueError("message cannot be held in its current state")
        with self.connect() as connection:
            connection.execute(
                "UPDATE messages SET status = 'held', held_at = ?, claimed_by = NULL, claim_expires_at = NULL WHERE id = ? AND status IN ('queued', 'failed')",
                (utc_now(), message_id),
            )
        return self.get_message(message_id)

    def accept_message(self, message_id: str, recipient_run_id: str) -> Message:
        message = self.get_message(message_id)
        if message.to_run_id != recipient_run_id:
            raise ValueError("only the recipient run can accept this message")
        if message.status == "queued":
            return message
        if message.status != "held":
            raise ValueError("only a held message can be accepted")
        with self.connect() as connection:
            connection.execute(
                "UPDATE messages SET status = 'queued', held_at = NULL, error = NULL WHERE id = ? AND status = 'held'",
                (message_id,),
            )
        return self.get_message(message_id)

    def refuse_message(self, message_id: str, recipient_run_id: str) -> Message:
        message = self.get_message(message_id)
        if message.to_run_id != recipient_run_id:
            raise ValueError("only the recipient run can refuse this message")
        if message.status == "refused":
            return message
        if message.status not in {"queued", "held", "failed"}:
            raise ValueError("message cannot be refused in its current state")
        with self.connect() as connection:
            connection.execute(
                "UPDATE messages SET status = 'refused', refused_at = ?, claimed_by = NULL, claim_expires_at = NULL WHERE id = ? AND status IN ('queued', 'held', 'failed')",
                (utc_now(), message_id),
            )
        return self.get_message(message_id)

    def message_counts(self, to_run_id: str) -> dict[str, int]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT status, COUNT(*) AS count FROM messages WHERE to_run_id = ? GROUP BY status",
                (to_run_id,),
            ).fetchall()
        counts = {status: 0 for status in ("queued", "held", "delivered", "acknowledged", "failed", "refused")}
        counts.update({row["status"]: row["count"] for row in rows})
        return counts

    def iter_pending(self, to_run_id: str | None = None) -> Iterable[Message]:
        query = "SELECT * FROM messages WHERE status IN ('queued', 'failed')"
        params: list[object] = []
        if to_run_id:
            query += " AND to_run_id = ?"
            params.append(to_run_id)
        query += " ORDER BY created_at ASC"
        with self.connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return iter(Message.from_row(row) for row in rows)

    def create_task(
        self,
        *,
        title: str,
        created_by: str,
        description: str = "",
        assigned_to: str | None = None,
        depends_on: Iterable[str] = (),
    ) -> Task:
        title = validate_body(title)
        description = description.strip()
        if len(description) > MAX_MESSAGE_CHARS:
            raise ValueError(f"task description exceeds the {MAX_MESSAGE_CHARS}-character limit")
        if any(ord(char) < 32 and char not in "\n\t" for char in description):
            raise ValueError("task description contains unsupported control characters")
        self.get_run(created_by)
        if assigned_to:
            self.get_run(assigned_to)
        dependencies = tuple(dict.fromkeys(depends_on))
        if any(not dependency for dependency in dependencies):
            raise ValueError("task dependency IDs cannot be empty")
        with self.connect() as connection:
            for dependency in dependencies:
                if connection.execute("SELECT 1 FROM tasks WHERE id = ?", (dependency,)).fetchone() is None:
                    raise ValueError(f"task dependency not found: {dependency}")
            task_id = _token("task")
            now = utc_now()
            connection.execute(
                """
                INSERT INTO tasks
                  (id, title, description, created_by, assigned_to, status, depends_on, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?)
                """,
                (task_id, title, description, created_by, assigned_to, json.dumps(dependencies), now, now),
            )
        return self.get_task(task_id)

    def get_task(self, task_id: str) -> Task:
        with self.connect() as connection:
            row = connection.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        if row is None:
            raise KeyError(f"task not found: {task_id}")
        return Task.from_row(row)

    def list_tasks(self, status: str | None = None) -> list[Task]:
        query = "SELECT * FROM tasks"
        params: list[object] = []
        if status:
            if status not in {"pending", "in_progress", "completed", "failed", "cancelled"}:
                raise ValueError("invalid task status")
            query += " WHERE status = ?"
            params.append(status)
        query += " ORDER BY created_at ASC"
        with self.connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [Task.from_row(row) for row in rows]

    def claim_task(self, task_id: str, run_id: str) -> Task:
        self.get_run(run_id)
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
            if row is None:
                raise KeyError(f"task not found: {task_id}")
            if row["status"] != "pending":
                raise ValueError("task is not available for claiming")
            if row["assigned_to"] is not None and row["assigned_to"] != run_id:
                raise ValueError("task is assigned to another run")
            dependencies = json.loads(row["depends_on"])
            if dependencies:
                placeholders = ",".join("?" for _ in dependencies)
                incomplete = connection.execute(
                    f"""
                    SELECT COUNT(*) FROM tasks
                    WHERE id IN ({placeholders}) AND status != 'completed'
                    """,
                    dependencies,
                ).fetchone()[0]
                found = connection.execute(
                    f"SELECT COUNT(*) FROM tasks WHERE id IN ({placeholders})",
                    dependencies,
                ).fetchone()[0]
                if found != len(dependencies):
                    raise ValueError("task dependency is missing")
                if incomplete:
                    raise ValueError("task dependencies are not complete")
            updated = connection.execute(
                "UPDATE tasks SET status = 'in_progress', assigned_to = ?, updated_at = ? WHERE id = ? AND status = 'pending'",
                (run_id, utc_now(), task_id),
            )
            if updated.rowcount != 1:
                raise ValueError("task was claimed by another run")
        return self.get_task(task_id)

    def complete_task(self, task_id: str, run_id: str) -> Task:
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
            if row is None:
                raise KeyError(f"task not found: {task_id}")
            if row["assigned_to"] != run_id:
                raise ValueError("only the assigned run can complete this task")
            if row["status"] != "in_progress":
                raise ValueError("only an in-progress task can be completed")
            now = utc_now()
            connection.execute(
                "UPDATE tasks SET status = 'completed', updated_at = ?, completed_at = ? WHERE id = ?",
                (now, now, task_id),
            )
        return self.get_task(task_id)
