from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Run:
    id: str
    name: str
    agent: str
    mode: str
    command: str
    cwd: str
    tmux_session: str | None
    inbox_path: str | None
    team: str | None
    inbound_policy: str
    max_inbox: int
    status: str
    started_at: str
    ended_at: str | None
    last_heartbeat: str | None
    log_path: str | None

    @classmethod
    def from_row(cls, row: Any) -> "Run":
        return cls(
            id=row["id"],
            name=row["name"],
            agent=row["agent"],
            mode=row["mode"],
            command=row["command"],
            cwd=row["cwd"],
            tmux_session=row["tmux_session"],
            inbox_path=row["inbox_path"],
            team=row["team"],
            inbound_policy=row["inbound_policy"],
            max_inbox=row["max_inbox"],
            status=row["status"],
            started_at=row["started_at"],
            ended_at=row["ended_at"],
            last_heartbeat=row["last_heartbeat"],
            log_path=row["log_path"],
        )


@dataclass(frozen=True)
class Message:
    id: str
    from_run_id: str
    to_run_id: str
    body: str
    body_sha256: str
    status: str
    idempotency_key: str | None
    reply_to: str | None
    hop_count: int
    delivery_attempts: int
    created_at: str
    delivered_at: str | None
    acknowledged_at: str | None
    held_at: str | None
    refused_at: str | None
    claimed_by: str | None
    claim_expires_at: str | None
    error: str | None

    @classmethod
    def from_row(cls, row: Any) -> "Message":
        return cls(
            id=row["id"],
            from_run_id=row["from_run_id"],
            to_run_id=row["to_run_id"],
            body=row["body"],
            body_sha256=row["body_sha256"],
            status=row["status"],
            idempotency_key=row["idempotency_key"],
            reply_to=row["reply_to"],
            hop_count=row["hop_count"],
            delivery_attempts=row["delivery_attempts"],
            created_at=row["created_at"],
            delivered_at=row["delivered_at"],
            acknowledged_at=row["acknowledged_at"],
            held_at=row["held_at"],
            refused_at=row["refused_at"],
            claimed_by=row["claimed_by"],
            claim_expires_at=row["claim_expires_at"],
            error=row["error"],
        )


@dataclass(frozen=True)
class Task:
    id: str
    title: str
    description: str
    created_by: str
    assigned_to: str | None
    status: str
    depends_on: tuple[str, ...]
    created_at: str
    updated_at: str
    completed_at: str | None

    @classmethod
    def from_row(cls, row: Any) -> "Task":
        try:
            dependencies = tuple(json.loads(row["depends_on"]))
        except (TypeError, json.JSONDecodeError):
            dependencies = ()
        return cls(
            id=row["id"],
            title=row["title"],
            description=row["description"],
            created_by=row["created_by"],
            assigned_to=row["assigned_to"],
            status=row["status"],
            depends_on=dependencies,
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            completed_at=row["completed_at"],
        )
