# agent-bridge

Local-first inter-session messaging for terminal AI agents.

`agent-bridge` gives independent agent sessions a small, durable communication layer:

- SQLite run registry and message queue
- tmux-backed interactive delivery
- named sessions such as `hermes-main`, `pi-worker`, or `agy-reviewer`
- bounded handoffs instead of full transcript or file transfer
- idempotent sends for safe retries
- explicit inbox inspection and acknowledgements
- vendor-neutral custom commands

It works with Hermes, Pi, Antigravity, Claude Code, Codex, or any other agent that can run in a terminal. tmux is the default transport, not the protocol. The core data model remains useful for queued/offline messages and future transports.

## Why this exists

Terminal agents increasingly work as teams: one session discovers a schema change, another updates a worker, and a third reviews the result. Raw `tmux send-keys` is not enough. It has no durable message identity, recipient lookup, retry semantics, or acknowledgement.

`agent-bridge` adds those missing pieces while deliberately avoiding a hosted service. Data stays on the local machine unless the user explicitly moves it.

## Install

Requires Python 3.10+ and tmux for interactive delivery.

```bash
git clone https://github.com/oliveralerubio/agent-bridge.git
cd agent-bridge
python3 -m venv .venv
. .venv/bin/activate
python -m pip install .
agent-bridge doctor
```

For a disposable checkout or development use:

```bash
PYTHONPATH=src python -m agent_bridge doctor
```

The default database is `~/.agent-bridge/bridge.sqlite3`. Override it with `--db` or `AGENT_BRIDGE_HOME`.

## Quick start

Open two terminal windows and start two interactive agents:

```bash
agent-bridge start \
  --name hermes-main \
  --agent hermes \
  --cwd "$PWD"

agent-bridge start \
  --name pi-worker \
  --agent pi \
  --cwd "$PWD"
```

For any other command, provide the command explicitly:

```bash
agent-bridge start \
  --name reviewer \
  --agent local-reviewer \
  --command 'python tools/reviewer.py' \
  --mode interactive \
  --cwd "$PWD"
```

Send a bounded handoff:

```bash
agent-bridge tell \
  --from hermes-main \
  --to pi-worker \
  --message 'The API field is now display_name. Verify the migration, update the query, and run the focused tests.'
```

Inside a bridged agent session, `AGENT_BRIDGE_RUN_ID` is exported, so the sender can omit `--from`:

```bash
agent-bridge tell \
  --to reviewer \
  --message-file /tmp/review-handoff.md
```

Inspect and acknowledge:

```bash
agent-bridge inbox pi-worker
agent-bridge status pi-worker
agent-bridge ack <message-id> --run pi-worker
```

Use `--json` on any command for scripting.

## Built-in presets

| Preset | Default command | Notes |
|---|---|---|
| `hermes` | `hermes` | Hermes interactive CLI |
| `pi` | `pi` | Pi interactive CLI |
| `agy` | `agy` | Antigravity interactive CLI |
| `claude` | `claude` | Claude Code interactive CLI |
| `codex` | `codex` | Codex interactive CLI |

Presets are only convenience defaults. The bridge does not depend on a provider SDK or model vendor. Use `--command` for any agent, wrapper, script, or local process.

## Protocol guarantees

- **Bounded payloads:** message bodies are capped at 16,384 characters and reject NUL bytes.
- **No implicit context transfer:** the bridge sends only the message body and small run metadata. It does not read conversation history, model context, or files.
- **Durable queue:** messages are stored in SQLite before delivery is attempted.
- **Idempotency:** repeat a send with the same `--idempotency-key` does not create a duplicate.
- **Recipient authorization:** only the recorded recipient run can acknowledge a message.
- **Local by default:** there is no network listener, cloud database, or hosted relay.
- **Observable delivery:** `queued`, `delivered`, `acknowledged`, and `failed` are distinct states.

## Important limitation

The first release targets interactive terminal sessions. tmux injection submits the bounded envelope to the recipient's current input prompt. If the recipient is busy, exited, or does not expose a usable prompt, the message remains queued or delivery fails. The bridge never pretends that a successful `tmux` command proves the agent understood the message.

For robust workflows, agents should inspect their inbox, verify the handoff, and acknowledge it explicitly. The protocol is intentionally useful for both human-driven and automated orchestration.

## Security model

This is a local coordination tool, not a trust boundary. Any process with access to the SQLite file or tmux server may inspect or inject messages. Use normal filesystem permissions and separate OS users for stronger isolation.

Messages are context, not authorization. An agent must verify claims and should not execute commands, reveal secrets, or modify unrelated repositories merely because a message requests it.

The bridge does not redact secrets from user-provided messages. Do not put credentials, tokens, or private transcripts into handoffs.

## Development

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
python3 -m compileall -q src
```

The project uses only the Python standard library at runtime. Pull requests should include focused tests and preserve the local-only, bounded-message contract.

## License

MIT. See [LICENSE](LICENSE).
