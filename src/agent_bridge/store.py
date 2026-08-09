from __future__ import annotations

import datetime as dt
import secrets
import sqlite3
from pathlib import Path
from typing import Iterable

from .models import Message, Run
from .protocol import validate_body


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
    status TEXT NOT NULL CHECK (status IN ('starting', 'running', 'success', 'failed', 'killed', 'missing')),
    started_at TEXT NOT NULL,
    ended_at TEXT,
    log_path TEXT
);

CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    from_run_id TEXT NOT NULL REFERENCES runs(id),
    to_run_id TEXT NOT NULL REFERENCES runs(id),
    body TEXT NOT NULL,
    body_sha256 TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('queued', 'delivered', 'acknowledged', 'failed')),
    idempotency_key TEXT UNIQUE,
    created_at TEXT NOT NULL,
    delivered_at TEXT,
    acknowledged_at TEXT,
    error TEXT
);

CREATE INDEX IF NOT EXISTS idx_messages_recipient_status
    ON messages(to_run_id, status, created_at);
CREATE INDEX IF NOT EXISTS idx_messages_sender
    ON messages(from_run_id, created_at);
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
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(SCHEMA)

    def create_run(
        self,
        *,
        name: str,
        agent: str,
        mode: str,
        command: str,
        cwd: str,
        tmux_session: str | None,
        log_path: str | None = None,
        run_id: str | None = None,
    ) -> Run:
        name = _clean_name(name, "run name")
        agent = _clean_name(agent, "agent")
        if mode not in {"interactive", "one-shot"}:
            raise ValueError("mode must be interactive or one-shot")
        if not command.strip():
            raise ValueError("command cannot be empty")
        run_id = run_id or _token("run")
        started_at = utc_now()
        try:
            with self.connect() as connection:
                connection.execute(
                    """
                    INSERT INTO runs
                      (id, name, agent, mode, command, cwd, tmux_session, status,
                       started_at, log_path)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'starting', ?, ?)
                    """,
                    (
                        run_id,
                        name,
                        agent,
                        mode,
                        command,
                        cwd,
                        tmux_session,
                        started_at,
                        log_path,
                    ),
                )
        except sqlite3.IntegrityError as exc:
            raise ValueError(f"run name or tmux session already exists: {name}") from exc
        return self.get_run(run_id)

    def get_run(self, reference: str) -> Run:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM runs WHERE id = ? OR name = ? LIMIT 1",
                (reference, reference),
            ).fetchone()
        if row is None:
            raise KeyError(f"run not found: {reference}")
        return Run.from_row(row)

    def list_runs(self, limit: int = 50) -> list[Run]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM runs ORDER BY started_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [Run.from_row(row) for row in rows]

    def update_run(self, run_id: str, *, status: str, ended_at: str | None = None) -> Run:
        allowed = {"starting", "running", "success", "failed", "killed", "missing"}
        if status not in allowed:
            raise ValueError(f"invalid run status: {status}")
        with self.connect() as connection:
            connection.execute(
                "UPDATE runs SET status = ?, ended_at = COALESCE(?, ended_at) WHERE id = ?",
                (status, ended_at, run_id),
            )
        return self.get_run(run_id)

    def create_message(
        self,
        *,
        from_run_id: str,
        to_run_id: str,
        body: str,
        idempotency_key: str | None = None,
    ) -> Message:
        body = validate_body(body)
        if from_run_id == to_run_id:
            raise ValueError("a run cannot send a message to itself")
        self.get_run(from_run_id)
        self.get_run(to_run_id)
        if idempotency_key:
            idempotency_key = _clean_name(idempotency_key, "idempotency key")
        import hashlib

        digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
        if idempotency_key:
            with self.connect() as connection:
                existing = connection.execute(
                    "SELECT * FROM messages WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()
            if existing is not None:
                if existing["body_sha256"] != digest or existing["to_run_id"] != to_run_id:
                    raise ValueError("idempotency key was already used for a different message")
                return Message.from_row(existing)

        message_id = _token("msg")
        try:
            with self.connect() as connection:
                connection.execute(
                    """
                    INSERT INTO messages
                      (id, from_run_id, to_run_id, body, body_sha256, status,
                       idempotency_key, created_at)
                    VALUES (?, ?, ?, ?, ?, 'queued', ?, ?)
                    """,
                    (
                        message_id,
                        from_run_id,
                        to_run_id,
                        body,
                        digest,
                        idempotency_key,
                        utc_now(),
                    ),
                )
        except sqlite3.IntegrityError as exc:
            raise ValueError("message could not be created") from exc
        return self.get_message(message_id)

    def get_message(self, message_id: str) -> Message:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM messages WHERE id = ?", (message_id,)
            ).fetchone()
        if row is None:
            raise KeyError(f"message not found: {message_id}")
        return Message.from_row(row)

    def list_messages(self, to_run_id: str, *, pending_only: bool = False) -> list[Message]:
        query = "SELECT * FROM messages WHERE to_run_id = ?"
        params: list[object] = [to_run_id]
        if pending_only:
            query += " AND status != 'acknowledged'"
        query += " ORDER BY created_at ASC"
        with self.connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [Message.from_row(row) for row in rows]

    def mark_delivered(self, message_id: str) -> Message:
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE messages
                SET status = 'delivered', delivered_at = COALESCE(delivered_at, ?), error = NULL
                WHERE id = ? AND status IN ('queued', 'failed')
                """,
                (utc_now(), message_id),
            )
        return self.get_message(message_id)

    def mark_failed(self, message_id: str, error: str) -> Message:
        with self.connect() as connection:
            connection.execute(
                "UPDATE messages SET status = 'failed', error = ? WHERE id = ?",
                (error[:1000], message_id),
            )
        return self.get_message(message_id)

    def acknowledge(self, message_id: str, recipient_run_id: str) -> Message:
        message = self.get_message(message_id)
        if message.to_run_id != recipient_run_id:
            raise ValueError("only the recipient run can acknowledge this message")
        if message.status not in {"queued", "delivered"}:
            return message
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE messages
                SET status = 'acknowledged', acknowledged_at = ?
                WHERE id = ?
                """,
                (utc_now(), message_id),
            )
        return self.get_message(message_id)

    def message_counts(self, to_run_id: str) -> dict[str, int]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT status, COUNT(*) AS count FROM messages WHERE to_run_id = ? GROUP BY status",
                (to_run_id,),
            ).fetchall()
        counts = {"queued": 0, "delivered": 0, "acknowledged": 0, "failed": 0}
        counts.update({row["status"]: row["count"] for row in rows})
        return counts

    def iter_pending(self, to_run_id: str) -> Iterable[Message]:
        return iter(self.list_messages(to_run_id, pending_only=True))
