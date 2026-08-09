from __future__ import annotations

import argparse
import json
import os
import shlex
import stat
import sys
import time
import uuid
from pathlib import Path
from typing import Any

from .adapters import resolve_preset
from .bridge import Bridge
from .protocol import MAX_MESSAGE_CHARS, validate_body
from .socket_transport import SocketTransportError, UnixSocketTransport
from .store import Store, utc_now
from .tmux import TmuxError, TmuxTransport


DEFAULT_HOME = Path(os.environ.get("AGENT_BRIDGE_HOME", "~/.agent-bridge")).expanduser()
DEFAULT_DB = DEFAULT_HOME / "bridge.sqlite3"


def _jsonable(value: Any) -> Any:
    if hasattr(value, "__dataclass_fields__"):
        return {key: _jsonable(getattr(value, key)) for key in value.__dataclass_fields__}
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
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


def _bridge(store: Store, args: argparse.Namespace) -> Bridge:
    return Bridge(store, TmuxTransport(getattr(args, "tmux", "tmux")), UnixSocketTransport())


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


def cmd_register(args: argparse.Namespace) -> int:
    store = _store(args)
    run = store.create_run(
        name=args.name,
        agent=args.agent,
        mode=args.mode,
        command=args.command,
        cwd=str(Path(args.cwd).expanduser().resolve()),
        inbox_path=args.inbox,
        team=args.team,
        inbound_policy=args.inbound_policy,
        max_inbox=args.max_inbox,
    )
    run = store.update_run(run.id, status="running")
    _print(
        {
            "id": run.id,
            "name": run.name,
            "agent": run.agent,
            "status": run.status,
            "inbox": run.inbox_path,
            "database": str(store.path),
            "environment": {
                "AGENT_BRIDGE_RUN_ID": run.id,
                "AGENT_BRIDGE_RUN_NAME": run.name,
                "AGENT_BRIDGE_DB": str(store.path),
                "AGENT_BRIDGE_INBOX": run.inbox_path,
            },
        },
        args.json,
    )
    return 0


def cmd_heartbeat(args: argparse.Namespace) -> int:
    store = _store(args)
    run = store.heartbeat(_source_run(store, args.run))
    _print(run, args.json)
    return 0


def cmd_start(args: argparse.Namespace) -> int:
    store = _store(args)
    agent, command_text, preset_mode = resolve_preset(args.agent, args.command)
    mode = args.mode or preset_mode
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
        inbox_path=args.inbox,
        team=args.team,
        inbound_policy=args.inbound_policy,
        max_inbox=args.max_inbox,
        log_path=str(log_dir / f"{uuid.uuid4().hex}.log"),
    )
    tmux = TmuxTransport(args.tmux)
    environment = {
        "AGENT_BRIDGE_RUN_ID": run.id,
        "AGENT_BRIDGE_RUN_NAME": run.name,
        "AGENT_BRIDGE_DB": str(store.path),
        "AGENT_BRIDGE_INBOX": run.inbox_path or "",
        "AGENT_BRIDGE_INBOUND_POLICY": run.inbound_policy,
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
            "team": run.team,
            "inbound_policy": run.inbound_policy,
            "max_inbox": run.max_inbox,
            "tmux_session": run.tmux_session,
            "inbox": run.inbox_path,
            "database": str(store.path),
            "log": run.log_path,
            "environment": "AGENT_BRIDGE_RUN_ID and AGENT_BRIDGE_INBOX are exported inside the session",
        },
        args.json,
    )
    return 0


def _create_and_deliver(args: argparse.Namespace, *, reply_to: str | None = None) -> dict[str, object]:
    store = _store(args)
    from_run_id = _source_run(store, getattr(args, "from_run", None))
    recipient = store.get_run(args.to)
    message = store.create_message(
        from_run_id=from_run_id,
        to_run_id=recipient.id,
        body=_message_body(args),
        idempotency_key=getattr(args, "idempotency_key", None),
        reply_to=reply_to or getattr(args, "reply_to", None),
    )
    result = _bridge(store, args).deliver(message)
    return {
        "id": result.message.id,
        "from": from_run_id,
        "to": recipient.name,
        "reply_to": result.message.reply_to,
        "status": result.message.status,
        "transport": result.transport,
    }


def cmd_send(args: argparse.Namespace) -> int:
    _print(_create_and_deliver(args), args.json)
    return 0


def cmd_reply(args: argparse.Namespace) -> int:
    store = _store(args)
    parent = store.get_message(args.message_id)
    source = _source_run(store, args.from_run)
    if source != parent.to_run_id:
        raise ValueError("only the original recipient can use reply")
    args.to = parent.from_run_id
    _print(_create_and_deliver(args, reply_to=parent.id), args.json)
    return 0


def cmd_deliver(args: argparse.Namespace) -> int:
    store = _store(args)
    recipient = store.get_run(args.to) if args.to else None
    results = _bridge(store, args).drain_pending(recipient.id if recipient else None)
    _print(
        [{"id": result.message.id, "status": result.message.status, "transport": result.transport} for result in results],
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
        if message.reply_to:
            print(f"  in_reply_to={message.reply_to}")
        print(f"  {message.body}")
    return 0


def cmd_ack(args: argparse.Namespace) -> int:
    store = _store(args)
    run_id = _source_run(store, args.run)
    message = store.acknowledge(args.message_id, run_id)
    _print(message, args.json)
    return 0


def _message_action(args: argparse.Namespace, action: str) -> int:
    store = _store(args)
    run_id = _source_run(store, args.run)
    message = getattr(store, f"{action}_message")(args.message_id, run_id)
    if action == "accept":
        message = _bridge(store, args).deliver(message).message
    _print(message, args.json)
    return 0


def cmd_hold(args: argparse.Namespace) -> int:
    return _message_action(args, "hold")


def cmd_accept(args: argparse.Namespace) -> int:
    return _message_action(args, "accept")


def cmd_refuse(args: argparse.Namespace) -> int:
    return _message_action(args, "refuse")


def cmd_policy(args: argparse.Namespace) -> int:
    store = _store(args)
    run = store.set_inbound_policy(store.get_run(args.run).id, args.policy)
    _print(run, args.json)
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    store = _store(args)
    run = store.get_run(args.run)
    tmux = TmuxTransport(args.tmux)
    try:
        socket_available = bool(run.inbox_path and stat.S_ISSOCK(Path(run.inbox_path).stat().st_mode))
    except OSError:
        socket_available = False
    tmux_available = bool(run.tmux_session and tmux.has_session(run.tmux_session))
    result = {
        "run": run,
        "messages": store.message_counts(run.id),
        "transport_available": socket_available or tmux_available,
        "socket_available": socket_available,
        "tmux_available": tmux_available,
    }
    _print(result, args.json)
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    store = _store(args)
    runs = store.list_runs(args.limit, team=args.team, active_only=args.active)
    if args.json:
        _print(runs, True)
        return 0
    if not runs:
        print("no runs")
        return 0
    print(f"{'name':<24} {'agent':<10} {'status':<10} {'policy':<8} {'team':<16} inbox")
    for run in runs:
        print(
            f"{run.name:<24} {run.agent:<10} {run.status:<10} {run.inbound_policy:<8} "
            f"{(run.team or '-'):<16} {run.inbox_path or '-'}"
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


def cmd_listen(args: argparse.Namespace) -> int:
    store = _store(args)
    run = store.get_run(args.run)

    def on_message(payload: dict[str, object]) -> None:
        body = payload.get("body")
        target = payload.get("to")
        if payload.get("type") != "agent-bridge.message" or not isinstance(body, str):
            raise ValueError("unsupported agent-bridge message")
        if not isinstance(target, dict) or target.get("id") != run.id:
            raise ValueError("message recipient does not match this run")
        validate_body(body)
        _print(payload, args.json)
        sys.stdout.flush()

    UnixSocketTransport().listen(
        path=run.inbox_path or "",
        on_message=on_message,
        once=args.once,
        timeout=args.timeout,
    )
    return 0


def _task_created_by(store: Store, reference: str | None) -> str:
    return _source_run(store, reference)


def cmd_task_create(args: argparse.Namespace) -> int:
    store = _store(args)
    depends_on = []
    for value in args.depends_on:
        depends_on.extend(part for part in value.split(",") if part)
    task = store.create_task(
        title=args.title,
        description=args.description,
        created_by=_task_created_by(store, args.from_run),
        assigned_to=store.get_run(args.assign).id if args.assign else None,
        depends_on=depends_on,
    )
    _print(task, args.json)
    return 0


def cmd_task_list(args: argparse.Namespace) -> int:
    tasks = _store(args).list_tasks(args.status)
    _print(tasks, args.json)
    return 0


def cmd_task_show(args: argparse.Namespace) -> int:
    _print(_store(args).get_task(args.task_id), args.json)
    return 0


def cmd_task_claim(args: argparse.Namespace) -> int:
    store = _store(args)
    task = store.claim_task(args.task_id, _source_run(store, args.run))
    _print(task, args.json)
    return 0


def cmd_task_complete(args: argparse.Namespace) -> int:
    store = _store(args)
    task = store.complete_task(args.task_id, _source_run(store, args.run))
    _print(task, args.json)
    return 0


def add_message_args(parser: argparse.ArgumentParser, *, include_from: bool) -> None:
    if include_from:
        parser.add_argument("--from", dest="from_run", help="sender run name or ID; defaults to AGENT_BRIDGE_RUN_ID")
    parser.add_argument("--to", required=True, help="recipient run name or ID")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--message", help="bounded message body")
    group.add_argument("--message-file", help="read the bounded message body from a UTF-8 file")
    parser.add_argument("--idempotency-key", help="stable key for safe retries")
    parser.add_argument("--reply-to", help="parent message ID for a direct reply")
    parser.add_argument("--tmux", default="tmux", help="tmux executable fallback")


def _add_run_ref(parser: argparse.ArgumentParser, *, name: str = "--run") -> None:
    parser.add_argument(name, help="run name or ID; defaults to AGENT_BRIDGE_RUN_ID")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agent-bridge",
        description="Local agent registry, structured inboxes, bounded messaging, and shared tasks",
    )
    parser.add_argument("--db", default=None, help="SQLite database path")
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    sub = parser.add_subparsers(dest="command", required=True)

    init = sub.add_parser("init", help="initialize the local database")
    init.set_defaults(func=cmd_init)

    register = sub.add_parser("register", help="register an externally launched agent without tmux")
    register.add_argument("--name", required=True)
    register.add_argument("--agent", required=True)
    register.add_argument("--command", required=True)
    register.add_argument("--mode", choices=["interactive", "one-shot"], default="interactive")
    register.add_argument("--cwd", default=".")
    register.add_argument("--inbox")
    register.add_argument("--team")
    register.add_argument("--inbound-policy", choices=["accept", "hold", "refuse"], default="accept")
    register.add_argument("--max-inbox", type=int, default=100)
    register.set_defaults(func=cmd_register)

    heartbeat = sub.add_parser("heartbeat", help="update a run's liveness timestamp")
    _add_run_ref(heartbeat)
    heartbeat.set_defaults(func=cmd_heartbeat)

    start = sub.add_parser("start", help="start an agent inside an optional tmux display")
    start.add_argument("--name", required=True, help="unique human-readable run name")
    start.add_argument("--agent", help="preset name or custom label")
    start.add_argument("--command", help="command line; required for custom agents")
    start.add_argument("--mode", choices=["interactive", "one-shot"])
    start.add_argument("--cwd", default=".")
    start.add_argument("--session", help="tmux session name")
    start.add_argument("--inbox", help="Unix socket path; defaults beside the database")
    start.add_argument("--team", help="local team label")
    start.add_argument("--inbound-policy", choices=["accept", "hold", "refuse"], default="accept")
    start.add_argument("--max-inbox", type=int, default=100)
    start.add_argument("--tmux", default="tmux")
    start.add_argument("--initial-message")
    start.add_argument("--ready-delay", type=float, default=1.0)
    start.set_defaults(func=cmd_start)

    for name, help_text in [("send", "send a bounded message"), ("tell", "send a bounded message")]:
        send = sub.add_parser(name, help=help_text)
        add_message_args(send, include_from=True)
        send.set_defaults(func=cmd_send)

    reply = sub.add_parser("reply", help="reply directly to a received message")
    reply.add_argument("message_id")
    reply.add_argument("--from", dest="from_run", help="replying run; defaults to AGENT_BRIDGE_RUN_ID")
    group = reply.add_mutually_exclusive_group(required=True)
    group.add_argument("--message")
    group.add_argument("--message-file")
    reply.add_argument("--idempotency-key")
    reply.add_argument("--tmux", default="tmux")
    reply.set_defaults(func=cmd_reply)

    deliver = sub.add_parser("deliver", help="redeliver queued messages")
    deliver.add_argument("--to", help="limit to one recipient")
    deliver.add_argument("--tmux", default="tmux")
    deliver.set_defaults(func=cmd_deliver)

    inbox = sub.add_parser("inbox", help="show messages for a run")
    inbox.add_argument("run", help="recipient run name or ID")
    inbox.add_argument("--all", action="store_true", help="include acknowledged and refused messages")
    inbox.set_defaults(func=cmd_inbox)

    ack = sub.add_parser("ack", help="acknowledge a delivered message")
    ack.add_argument("message_id")
    _add_run_ref(ack)
    ack.set_defaults(func=cmd_ack)

    for name, func, help_text in [
        ("hold", cmd_hold, "hold a message for later review"),
        ("accept", cmd_accept, "accept a held message and attempt delivery"),
        ("refuse", cmd_refuse, "refuse a message"),
    ]:
        action = sub.add_parser(name, help=help_text)
        action.add_argument("message_id")
        _add_run_ref(action)
        action.add_argument("--tmux", default="tmux")
        action.set_defaults(func=func)

    policy = sub.add_parser("policy", help="set a run's inbound accept/hold/refuse policy")
    policy.add_argument("run")
    policy.add_argument("policy", choices=["accept", "hold", "refuse"])
    policy.set_defaults(func=cmd_policy)

    status = sub.add_parser("status", help="show run and non-mutating transport status")
    status.add_argument("run")
    status.add_argument("--tmux", default="tmux")
    status.set_defaults(func=cmd_status)

    listing = sub.add_parser("list", aliases=["peers"], help="list discoverable local runs")
    listing.add_argument("--limit", type=int, default=50)
    listing.add_argument("--team")
    listing.add_argument("--active", action="store_true")
    listing.set_defaults(func=cmd_list)

    log = sub.add_parser("log", help="show the tail of a run log")
    log.add_argument("run")
    log.add_argument("--lines", type=int, default=120)
    log.set_defaults(func=cmd_log)

    kill = sub.add_parser("kill", help="stop a tmux run")
    kill.add_argument("run")
    kill.add_argument("--tmux", default="tmux")
    kill.set_defaults(func=cmd_kill)

    listen = sub.add_parser("listen", help="serve a structured local inbox for an agent adapter")
    listen.add_argument("run", help="run name or ID")
    listen.add_argument("--once", action="store_true")
    listen.add_argument("--timeout", type=float)
    listen.set_defaults(func=cmd_listen)

    doctor = sub.add_parser("doctor", help="check local prerequisites")
    doctor.add_argument("--tmux", default="tmux")
    doctor.set_defaults(func=cmd_doctor)

    task = sub.add_parser("task", help="manage shared dependency-aware tasks")
    task_sub = task.add_subparsers(dest="task_command", required=True)
    task_create = task_sub.add_parser("create")
    task_create.add_argument("--title", required=True)
    task_create.add_argument("--description", default="")
    task_create.add_argument("--from", dest="from_run")
    task_create.add_argument("--assign")
    task_create.add_argument("--depends-on", action="append", default=[])
    task_create.set_defaults(func=cmd_task_create)
    task_list = task_sub.add_parser("list")
    task_list.add_argument("--status")
    task_list.set_defaults(func=cmd_task_list)
    task_show = task_sub.add_parser("show")
    task_show.add_argument("task_id")
    task_show.set_defaults(func=cmd_task_show)
    task_claim = task_sub.add_parser("claim")
    task_claim.add_argument("task_id")
    _add_run_ref(task_claim)
    task_claim.set_defaults(func=cmd_task_claim)
    task_complete = task_sub.add_parser("complete")
    task_complete.add_argument("task_id")
    _add_run_ref(task_complete)
    task_complete.set_defaults(func=cmd_task_complete)

    seen_parsers: set[int] = set()
    for child in sub.choices.values():
        if id(child) in seen_parsers:
            continue
        seen_parsers.add(id(child))
        child.add_argument("--db", default=argparse.SUPPRESS, help="SQLite database path")
        child.add_argument("--json", action="store_true", default=argparse.SUPPRESS, help="emit machine-readable JSON")
    seen_task_parsers: set[int] = set()
    for child in task_sub.choices.values():
        if id(child) in seen_task_parsers:
            continue
        seen_task_parsers.add(id(child))
        child.add_argument("--db", default=argparse.SUPPRESS, help="SQLite database path")
        child.add_argument("--json", action="store_true", default=argparse.SUPPRESS, help="emit machine-readable JSON")

    return parser


def cmd_doctor(args: argparse.Namespace) -> int:
    store = _store(args)
    tmux = TmuxTransport(args.tmux)
    _print(
        {
            "database": str(store.path),
            "database_exists": store.path.exists(),
            "tmux_binary": args.tmux,
            "tmux_available": tmux.available(),
            "structured_inbox": "Unix domain socket",
            "presets": ["hermes", "pi", "agy", "claude", "codex"],
        },
        args.json,
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except (KeyError, ValueError, OSError, TmuxError, SocketTransportError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
