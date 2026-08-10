from __future__ import annotations

import json
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import Any

MAX_MANIFEST_BYTES = 128 * 1024
MAX_MEMBERS = 64


@dataclass(frozen=True)
class ManifestMember:
    name: str
    agent: str
    command: str
    cwd: str
    role: str
    lead: bool
    mode: str
    startup_timeout: float
    restart_policy: str
    readiness_required: bool
    inbox: str | None = None


@dataclass(frozen=True)
class TeamManifest:
    name: str
    metadata: dict[str, Any]
    members: tuple[ManifestMember, ...]


def _validate_metadata(value: object) -> None:
    if isinstance(value, str) and any(ord(char) < 32 and char not in "\n\t" for char in value):
        raise ValueError("manifest metadata contains unsupported control characters")
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("manifest metadata keys must be text")
            _validate_metadata(key)
            _validate_metadata(item)
    elif isinstance(value, list):
        for item in value:
            _validate_metadata(item)


def _text(value: object, field: str, limit: int = 128) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > limit:
        raise ValueError(f"manifest {field} is invalid")
    if any(ord(char) < 32 and char not in "\t" for char in value):
        raise ValueError(f"manifest {field} contains unsupported control characters")
    return value.strip()


def load_manifest(source: str | bytes | Path) -> TeamManifest:
    is_path = isinstance(source, Path)
    if isinstance(source, str) and len(source) < 4096:
        try:
            is_path = Path(source).expanduser().exists()
        except OSError:
            is_path = False
    if is_path:
        path = Path(source).expanduser()
        try:
            with path.open("rb") as handle:
                raw = handle.read(MAX_MANIFEST_BYTES + 1)
        except OSError as exc:
            raise ValueError(f"could not read team manifest: {exc}") from exc
    elif isinstance(source, str):
        raw = source.encode("utf-8")
    else:
        raw = source
    if len(raw) > MAX_MANIFEST_BYTES:
        raise ValueError("team manifest exceeds the bounded limit")
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("team manifest must be UTF-8 JSON") from exc
    if not isinstance(data, dict):
        raise ValueError("team manifest must be a JSON object")
    name = _text(data.get("name"), "name")
    metadata = data.get("metadata", {})
    if not isinstance(metadata, dict):
        raise ValueError("manifest metadata must be an object")
    _validate_metadata(metadata)
    if len(json.dumps(metadata, ensure_ascii=False, allow_nan=False).encode("utf-8")) > 16 * 1024:
        raise ValueError("manifest metadata is too large")
    raw_members = data.get("members")
    if not isinstance(raw_members, list) or not raw_members or len(raw_members) > MAX_MEMBERS:
        raise ValueError("manifest members must be a bounded non-empty list")
    members: list[ManifestMember] = []
    names: set[str] = set()
    for item in raw_members:
        if not isinstance(item, dict):
            raise ValueError("manifest member must be an object")
        member_name = _text(item.get("name"), "member name")
        if member_name in names:
            raise ValueError(f"duplicate manifest member: {member_name}")
        names.add(member_name)
        agent = _text(item.get("agent", "custom"), "member agent")
        command = _text(item.get("command"), "member command", 4_096)
        try:
            if not shlex.split(command):
                raise ValueError("empty command")
        except ValueError as exc:
            raise ValueError(f"manifest command is invalid for {member_name}") from exc
        cwd = _text(item.get("cwd", "."), "member cwd", 4_096)
        role = _text(item.get("role", "member"), "member role")
        mode = item.get("mode", "interactive")
        if mode not in {"interactive", "one-shot"}:
            raise ValueError("manifest mode must be interactive or one-shot")
        timeout = item.get("startup_timeout", item.get("readiness_timeout", 0))
        if not isinstance(timeout, (int, float)) or isinstance(timeout, bool) or not 0 <= timeout <= 3600:
            raise ValueError("manifest startup_timeout is invalid")
        restart = item.get("restart_policy", "never")
        if restart not in {"never", "on-failure", "always"}:
            raise ValueError("manifest restart_policy is invalid")
        lead = item.get("lead", False)
        if not isinstance(lead, bool):
            raise ValueError("manifest lead must be boolean")
        readiness_required = item.get("readiness_required", bool(timeout))
        if not isinstance(readiness_required, bool):
            raise ValueError("manifest readiness_required must be boolean")
        inbox = item.get("inbox")
        if inbox is not None:
            inbox = _text(inbox, "member inbox", 256)
        members.append(ManifestMember(member_name, agent, command, cwd, role, lead, mode, float(timeout), restart, readiness_required, inbox))
    if sum(member.lead for member in members) > 1:
        raise ValueError("team manifest may designate only one lead")
    return TeamManifest(name, metadata, tuple(members))
