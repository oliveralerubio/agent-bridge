from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

MAX_REPORT_BYTES = 32 * 1024
MAX_REPORT_FIELD_CHARS = 4_096
MAX_REPORT_ITEMS = 64
_REPORT_LIST_FIELDS = ("verified_facts", "tests", "files_changed", "blockers")
_REPORT_STRING_FIELDS = ("goal", "next_action")


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"report field {field} must be text")
    if len(value) > MAX_REPORT_FIELD_CHARS:
        raise ValueError(f"report field {field} is too large")
    if any(ord(char) < 32 and char not in "\n\t" for char in value):
        raise ValueError(f"report field {field} contains unsupported control characters")
    return value


def validate_report(report: object) -> dict[str, Any]:
    if not isinstance(report, Mapping):
        raise ValueError("completion report must be a JSON object")
    allowed = set(_REPORT_LIST_FIELDS) | set(_REPORT_STRING_FIELDS)
    unknown = set(report) - allowed
    if unknown:
        raise ValueError(f"unknown completion report fields: {', '.join(sorted(unknown))}")
    normalized: dict[str, Any] = {}
    for field in _REPORT_STRING_FIELDS:
        if field not in report:
            raise ValueError(f"completion report field {field} is required")
        normalized[field] = _text(report[field], field)
    for field in _REPORT_LIST_FIELDS:
        value = report.get(field, [])
        if not isinstance(value, list) or len(value) > MAX_REPORT_ITEMS:
            raise ValueError(f"report field {field} must be a bounded list")
        normalized[field] = [_text(item, f"{field} item") for item in value]
    encoded = json.dumps(normalized, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(encoded) > MAX_REPORT_BYTES:
        raise ValueError("completion report exceeds the bounded limit")
    return normalized


def parse_report_bytes(raw: bytes) -> dict[str, Any]:
    if len(raw) > MAX_REPORT_BYTES:
        raise ValueError("completion report file exceeds the bounded limit")
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("completion report is not valid UTF-8 JSON") from exc
    return validate_report(value)


def load_report_file(path: str | Path) -> dict[str, Any]:
    file_path = Path(path).expanduser()
    try:
        with file_path.open("rb") as handle:
            raw = handle.read(MAX_REPORT_BYTES + 1)
    except OSError as exc:
        raise ValueError(f"could not read completion report: {exc}") from exc
    return parse_report_bytes(raw)