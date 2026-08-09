from __future__ import annotations

import argparse
import json
import os
import shlex
import sys
import time
import uuid
from pathlib import Path
from typing import Any

from .adapters import resolve_preset
from .bridge import Bridge
from .protocol import MAX_MESSAGE_CHARS, validate_body
from .store import Store, utc_now
from .tmux import TmuxError, TmuxTransport


DEFAULT_HOME = Path(os.environ.get("AGENT_BRIDGE_HOME", "~/.agent-bridge")).expanduser()
DEFAULT_DB = DEFAULT_HOME / "bridge.sqlite3"


def _jsonable(value: Any) -> Any:
    if hasattr(value, "__dataclass_fields__"):
        return {key: _jsonable(getattr(value, key)) for key in value.__dataclass_fields__}
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    return value


def _print(value: Any, as_json: bool = False) -> None:
    if as_json:
        print(json.dumps(_jsonable(value), ensure_ascii=False, indent=2))
    elif isinstance(value, str):
        print(value)
    else:
        print(_jsonable(value))


def _store(args: argparse.Namespace) -> Store:
    path = args.db or os.environ.get("AGENT_BRIDGE_DB") or DEFAULT_DB
    return Store(path)


def _source_run(store: Store, reference: str | None) -> str:
    reference = reference or Bridge.current_run_id()
    if not reference:
        raise ValueError("provide --from or run inside an agent-bridge session")
    return store.get_run(reference).id


def _message_body(args: argparse.Namespace) -> str:
    if bool(args.message) == bool(args.message_file):
        raise ValueError("provide exactly one of --message or --message-file")
    if args.message_file:
        with Path(args.message_file).expanduser().open("r", encoding="utf-8") as handle:
            body = handle.read(MAX_MESSAGE_CHARS + 1)
        return validate_body(body)
    return validate_body(args.message)


def cmd_init(args: argparse.Namespace) -> int:
    store = _store(args)
    _print({"database": str(store.path), "status": "ready"}, args.json)
    return 0


def cmd_start(args: argparse.Namespace) -> int:
    store = _store(args)
    agent, command_text, preset_mode = resolve_preset(args.agent, args.command)
    mode = args.mode or preset_mode
    if mode not in {"interactive", "one-shot"}:
        raise ValueError("--mode must be interactive or one-shot")
    argv = shlex.split(command_text)
    if not argv:
        raise ValueError("agent command cannot be empty")
    cwd = Path(args.cwd).expanduser().resolve()
    if not cwd.is_dir():
        raise ValueError(f"working directory does not exist: {cwd}")
    safe_name = "".join(char if char.isalnum() or char in "-_" else "-" for char in args.name).strip("-") or "run"
    session = args.session or f"agent-bridge-{safe_name}-{uuid.uuid4().hex[:8]}"
    log_dir = store.path.parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    run = store.create_run(
        name=args.name,
        agent=agent,
        mode=mode,
        command=command_text,
        cwd=str(cwd),
        tmux_session=session,
        log_path=str(log_dir / f"{uuid.uuid4().hex}.log"),
    )
    tmux = TmuxTransport(args.tmux)
    environment = {
        "AGENT_BRIDGE_RUN_ID": run.id,
        "AGENT_BRIDGE_RUN_NAME": run.name,
        "AGENT_BRIDGE_DB": str(store.path),
    }
    try:
        tmux.start_session(
            session=session,
            cwd=str(cwd),
            argv=argv,
            environment=environment,
            log_path=run.log_path,
        )
        run = store.update_run(run.id, status="running")
    except (TmuxError, OSError) as exc:
        store.update_run(run.id, status="failed", ended_at=utc_now())
        raise ValueError(str(exc)) from exc

    if args.initial_message:
        if args.ready_delay:
            time.sleep(args.ready_delay)
        tmux.inject(
            session=session,
            text="[agent-bridge initial prompt]\n" + validate_body(args.initial_message),
        )

    _print(
        {
            "id": run.id,
            "name": run.name,
            "agent": run.agent,
            "mode": run.mode,
            "status": run.status,
            "tmux_session": run.tmux_session,
            "database": str(store.path),
            "log": run.log_path,
            "environment": "AGENT_BRIDGE_RUN_ID is exported inside the session",
        },
        args.json,
    )
    return 0


def cmd_send(args: argparse.Namespace) -> int:
    store = _store(args)
    from_run_id = _source_run(store, args.from_run)
    recipient = store.get_run(args.to)
    body = _message_body(args)
    message = store.create_message(
        from_run_id=from_run_id,
        to_run_id=recipient.id,
        body=body,
        idempotency_key=args.idempotency_key,
    )
    bridge = Bridge(store, TmuxTransport(args.tmux))
    if message.status in {"queued", "failed"}:
        result = bridge.deliver(message)
        message = result.message
        transport = result.transport
    else:
        transport = "already-delivered"
    _print(
        {
            "id": message.id,
            "from": from_run_id,
            "to": recipient.name,
            "status": message.status,
            "transport": transport,
        },
        args.json,
    )
    return 0


def cmd_inbox(args: argparse.Namespace) -> int:
    store = _store(args)
    run = store.get_run(args.run)
    messages = store.list_messages(run.id, pending_only=not args.all)
    if args.json:
        _print(messages, True)
        return 0
    if not messages:
        print("no messages")
        return 0
    for message in messages:
        print(f"{message.id}  {message.status:<13} {message.created_at}  from={message.from_run_id}")
        print(f"  {message.body}")
    return 0


def cmd_ack(args: argparse.Namespace) -> int:
    store = _store(args)
    run_id = _source_run(store, args.run)
    message = store.acknowledge(args.message_id, run_id)
    _print(message, args.json)
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    store = _store(args)
    run = store.get_run(args.run)
    tmux = TmuxTransport(args.tmux)
    if run.status in {"starting", "running"} and run.tmux_session and not tmux.has_session(run.tmux_session):
        run = store.update_run(run.id, status="missing", ended_at=utc_now())
    result = {"run": run, "messages": store.message_counts(run.id)}
    _print(result, args.json)
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    store = _store(args)
    runs = store.list_runs(args.limit)
    if args.json:
        _print(runs, True)
        return 0
    if not runs:
        print("no runs")
        return 0
    print(f"{'name':<24} {'agent':<10} {'status':<10} {'mode':<12} tmux session")
    for run in runs:
        print(
            f"{run.name:<24} {run.agent:<10} {run.status:<10} {run.mode:<12} {run.tmux_session or '-'}"
        )
    return 0


def cmd_log(args: argparse.Namespace) -> int:
    store = _store(args)
    run = store.get_run(args.run)
    if not run.log_path:
        print("no log configured")
        return 0
    path = Path(run.log_path)
    if not path.exists():
        print(f"log not found: {path}")
        return 0
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    print("\n".join(lines[-args.lines :]))
    return 0


def cmd_kill(args: argparse.Namespace) -> int:
    store = _store(args)
    run = store.get_run(args.run)
    if run.tmux_session:
        TmuxTransport(args.tmux).stop(run.tmux_session)
    run = store.update_run(run.id, status="killed", ended_at=utc_now())
    _print(run, args.json)
    return 0


def cmd_doctor(args: argparse.Namespace) -> int:
    store = _store(args)
    tmux = TmuxTransport(args.tmux)
    _print(
        {
            "database": str(store.path),
            "database_exists": store.path.exists(),
            "tmux_binary": args.tmux,
            "tmux_available": tmux.available(),
            "presets": ["hermes", "pi", "agy", "claude", "codex"],
        },
        args.json,
    )
    return 0


def add_message_args(parser: argparse.ArgumentParser, *, include_from: bool) -> None:
    if include_from:
        parser.add_argument("--from", dest="from_run", help="sender run name or ID; defaults to AGENT_BRIDGE_RUN_ID")
    parser.add_argument("--to", required=True, help="recipient run name or ID")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--message", help="bounded message body")
    group.add_argument("--message-file", help="read the bounded message body from a UTF-8 file")
    parser.add_argument("--idempotency-key", help="stable key for safe retries")
    parser.add_argument("--tmux", default="tmux", help="tmux executable")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agent-bridge",
        description="Local agent session registry and bounded inter-session messaging",
    )
    parser.add_argument("--db", default=None, help="SQLite database path")
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    sub = parser.add_subparsers(dest="command", required=True)

    init = sub.add_parser("init", help="initialize the local database")
    init.set_defaults(func=cmd_init)

    start = sub.add_parser("start", help="start an agent inside a tmux session")
    start.add_argument("--name", required=True, help="unique human-readable run name")
    start.add_argument("--agent", help="preset name or custom label")
    start.add_argument("--command", help="command line; required for custom agents")
    start.add_argument("--mode", choices=["interactive", "one-shot"])
    start.add_argument("--cwd", default=".")
    start.add_argument("--session", help="tmux session name")
    start.add_argument("--tmux", default="tmux")
    start.add_argument("--initial-message")
    start.add_argument("--ready-delay", type=float, default=1.0)
    start.set_defaults(func=cmd_start)

    for name, help_text in [("send", "send a bounded message"), ("tell", "send a bounded message")]:
        send = sub.add_parser(name, help=help_text)
        add_message_args(send, include_from=True)
        send.set_defaults(func=cmd_send)

    inbox = sub.add_parser("inbox", help="show messages for a run")
    inbox.add_argument("run", help="recipient run name or ID")
    inbox.add_argument("--all", action="store_true", help="include acknowledged messages")
    inbox.set_defaults(func=cmd_inbox)

    ack = sub.add_parser("ack", help="acknowledge a delivered message")
    ack.add_argument("message_id")
    ack.add_argument("--run", help="recipient run name or ID; defaults to AGENT_BRIDGE_RUN_ID")
    ack.set_defaults(func=cmd_ack)

    status = sub.add_parser("status", help="show run and message status")
    status.add_argument("run")
    status.add_argument("--tmux", default="tmux")
    status.set_defaults(func=cmd_status)

    listing = sub.add_parser("list", help="list registered runs")
    listing.add_argument("--limit", type=int, default=50)
    listing.set_defaults(func=cmd_list)

    log = sub.add_parser("log", help="show the tail of a run log")
    log.add_argument("run")
    log.add_argument("--lines", type=int, default=120)
    log.set_defaults(func=cmd_log)

    kill = sub.add_parser("kill", help="stop a tmux run")
    kill.add_argument("run")
    kill.add_argument("--tmux", default="tmux")
    kill.set_defaults(func=cmd_kill)

    doctor = sub.add_parser("doctor", help="check local prerequisites")
    doctor.add_argument("--tmux", default="tmux")
    doctor.set_defaults(func=cmd_doctor)

    for child in sub.choices.values():
        child.add_argument(
            "--db",
            default=argparse.SUPPRESS,
            help="SQLite database path",
        )
        child.add_argument(
            "--json",
            action="store_true",
            default=argparse.SUPPRESS,
            help="emit machine-readable JSON",
        )

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except (KeyError, ValueError, OSError, TmuxError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
