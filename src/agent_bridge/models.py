from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


def _json_list(value: Any) -> tuple[str, ...]:
    try:
        parsed = json.loads(value or "[]")
    except (TypeError, json.JSONDecodeError):
        return ()
    return tuple(item for item in parsed if isinstance(item, str)) if isinstance(parsed, list) else ()


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
    team_id: str | None = None
    role: str = "member"
    is_lead: bool = False
    lifecycle_state: str = "registered"
    readiness: str = "offline"
    adapter_protocol: int | None = None
    adapter_session_id: str | None = None
    capabilities: tuple[str, ...] = ()
    adapter_heartbeat_at: str | None = None
    adapter_ready_at: str | None = None
    adapter_error: str | None = None
    failure_reason: str | None = None
    exit_code: int | None = None
    readiness_required: bool = False
    readiness_timeout: float = 0.0
    restart_policy: str = "never"
    restart_count: int = 0
    process_pid: int | None = None
    process_start_token: str | None = None

    @classmethod
    def from_row(cls, row: Any) -> "Run":
        keys = set(row.keys()) if hasattr(row, "keys") else set()
        return cls(
            id=row["id"], name=row["name"], agent=row["agent"], mode=row["mode"],
            command=row["command"], cwd=row["cwd"], tmux_session=row["tmux_session"],
            inbox_path=row["inbox_path"], team=row["team"], inbound_policy=row["inbound_policy"],
            max_inbox=row["max_inbox"], status=row["status"], started_at=row["started_at"],
            ended_at=row["ended_at"], last_heartbeat=row["last_heartbeat"], log_path=row["log_path"],
            team_id=row["team_id"] if "team_id" in keys else None,
            role=row["role"] if "role" in keys and row["role"] else "member",
            is_lead=bool(row["is_lead"]) if "is_lead" in keys else False,
            lifecycle_state=row["lifecycle_state"] if "lifecycle_state" in keys and row["lifecycle_state"] else "registered",
            readiness=row["readiness"] if "readiness" in keys and row["readiness"] else "offline",
            adapter_protocol=row["adapter_protocol"] if "adapter_protocol" in keys else None,
            adapter_session_id=row["adapter_session_id"] if "adapter_session_id" in keys else None,
            capabilities=_json_list(row["capabilities_json"] if "capabilities_json" in keys else "[]"),
            adapter_heartbeat_at=row["adapter_heartbeat_at"] if "adapter_heartbeat_at" in keys else None,
            adapter_ready_at=row["adapter_ready_at"] if "adapter_ready_at" in keys else None,
            adapter_error=row["adapter_error"] if "adapter_error" in keys else None,
            failure_reason=row["failure_reason"] if "failure_reason" in keys else None,
            exit_code=row["exit_code"] if "exit_code" in keys else None,
            readiness_required=bool(row["readiness_required"]) if "readiness_required" in keys else False,
            readiness_timeout=float(row["readiness_timeout"] or 0) if "readiness_timeout" in keys else 0.0,
            restart_policy=row["restart_policy"] if "restart_policy" in keys and row["restart_policy"] else "never",
            restart_count=int(row["restart_count"] or 0) if "restart_count" in keys else 0,
            process_pid=int(row["process_pid"]) if "process_pid" in keys and row["process_pid"] is not None else None,
            process_start_token=row["process_start_token"] if "process_start_token" in keys else None,
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
    adapter_accepted_at: str | None = None

    @classmethod
    def from_row(cls, row: Any) -> "Message":
        keys = set(row.keys()) if hasattr(row, "keys") else set()
        return cls(
            id=row["id"], from_run_id=row["from_run_id"], to_run_id=row["to_run_id"], body=row["body"],
            body_sha256=row["body_sha256"], status=row["status"], idempotency_key=row["idempotency_key"],
            reply_to=row["reply_to"], hop_count=row["hop_count"], delivery_attempts=row["delivery_attempts"],
            created_at=row["created_at"], delivered_at=row["delivered_at"], acknowledged_at=row["acknowledged_at"],
            held_at=row["held_at"], refused_at=row["refused_at"], claimed_by=row["claimed_by"],
            claim_expires_at=row["claim_expires_at"], error=row["error"],
            adapter_accepted_at=row["adapter_accepted_at"] if "adapter_accepted_at" in keys else None,
        )


@dataclass(frozen=True)
class Team:
    id: str
    name: str
    status: str
    metadata: dict[str, Any]
    created_at: str
    updated_at: str
    stopped_at: str | None = None

    @classmethod
    def from_row(cls, row: Any) -> "Team":
        try:
            metadata = json.loads(row["metadata_json"] or "{}")
        except (TypeError, json.JSONDecodeError):
            metadata = {}
        return cls(row["id"], row["name"], row["status"], metadata if isinstance(metadata, dict) else {}, row["created_at"], row["updated_at"], row["stopped_at"])


@dataclass(frozen=True)
class TeamMember:
    team_id: str
    run_id: str
    role: str
    is_lead: bool
    joined_at: str
    removed_at: str | None
    run: Run | None = None
    id: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "TeamMember":
        keys = set(row.keys()) if hasattr(row, "keys") else set()
        run = Run.from_row(row) if "agent" in keys else None
        team_id = row["team_id"]
        run_id = row["run_id"]
        return cls(team_id, run_id, row["member_role"] if "member_role" in keys else row["role"], bool(row["member_is_lead"] if "member_is_lead" in keys else row["is_lead"]), row["joined_at"], row["removed_at"], run, f"member-{team_id}-{run_id}")


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
    team_id: str | None = None
    requires_approval: bool = False
    approved_by: str | None = None
    approved_at: str | None = None
    rejection_reason: str | None = None
    blocked_reason: str | None = None

    @classmethod
    def from_row(cls, row: Any) -> "Task":
        keys = set(row.keys()) if hasattr(row, "keys") else set()
        return cls(
            id=row["id"], title=row["title"], description=row["description"], created_by=row["created_by"],
            assigned_to=row["assigned_to"], status=row["status"], depends_on=_json_list(row["depends_on"]),
            created_at=row["created_at"], updated_at=row["updated_at"], completed_at=row["completed_at"],
            team_id=row["team_id"] if "team_id" in keys else None,
            requires_approval=bool(row["requires_approval"]) if "requires_approval" in keys else False,
            approved_by=row["approved_by"] if "approved_by" in keys else None,
            approved_at=row["approved_at"] if "approved_at" in keys else None,
            rejection_reason=row["rejection_reason"] if "rejection_reason" in keys else None,
            blocked_reason=row["blocked_reason"] if "blocked_reason" in keys else None,
        )


@dataclass(frozen=True)
class CompletionReport:
    id: str
    task_id: str
    created_by: str
    goal: str
    verified_facts: tuple[str, ...]
    tests: tuple[str, ...]
    files_changed: tuple[str, ...]
    blockers: tuple[str, ...]
    next_action: str
    created_at: str

    @classmethod
    def from_row(cls, row: Any) -> "CompletionReport":
        return cls(
            row["id"], row["task_id"], row["created_by"], row["goal"], _json_list(row["verified_facts_json"]),
            _json_list(row["tests_json"]), _json_list(row["files_changed_json"]), _json_list(row["blockers_json"]),
            row["next_action"], row["created_at"],
        )


@dataclass(frozen=True)
class Hook:
    id: str
    name: str
    event: str
    command: tuple[str, ...]
    timeout: float
    max_output: int
    fail_closed: bool
    enabled: bool

    @classmethod
    def from_row(cls, row: Any) -> "Hook":
        try:
            command = tuple(json.loads(row["command_json"]))
        except (TypeError, json.JSONDecodeError):
            command = ()
        return cls(row["id"], row["name"], row["event"], command, row["timeout_seconds"], row["max_output"], bool(row["fail_closed"]), bool(row["enabled"]))


@dataclass(frozen=True)
class HookEvent:
    id: str
    hook_id: str | None
    event: str
    status: str
    output: str
    created_at: str

    @classmethod
    def from_row(cls, row: Any) -> "HookEvent":
        return cls(row["id"], row["hook_id"], row["event"], row["status"], row["output"], row["created_at"])
