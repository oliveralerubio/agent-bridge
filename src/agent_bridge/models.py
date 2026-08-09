from __future__ import annotations

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
    status: str
    started_at: str
    ended_at: str | None
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
            status=row["status"],
            started_at=row["started_at"],
            ended_at=row["ended_at"],
            log_path=row["log_path"],
        )


@dataclass(frozen=True)
class Message:
    id: str
    from_run_id: str
    to_run_id: str
    body: str
    status: str
    idempotency_key: str | None
    created_at: str
    delivered_at: str | None
    acknowledged_at: str | None
    error: str | None

    @classmethod
    def from_row(cls, row: Any) -> "Message":
        return cls(
            id=row["id"],
            from_run_id=row["from_run_id"],
            to_run_id=row["to_run_id"],
            body=row["body"],
            status=row["status"],
            idempotency_key=row["idempotency_key"],
            created_at=row["created_at"],
            delivered_at=row["delivered_at"],
            acknowledged_at=row["acknowledged_at"],
            error=row["error"],
        )
