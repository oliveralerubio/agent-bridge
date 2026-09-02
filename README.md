# agent-bridge

Local-first coordination for terminal AI agents, inspired by the useful parts of Claude Code agent teams without requiring a provider SDK or hosted relay.

`agent-bridge` gives independent agent sessions a small, durable protocol:

- local peer discovery with stable run names and inbox addresses;
- SQLite-backed messages and shared tasks;
- structured Unix-socket inbox delivery as the primary transport;
- tmux prompt injection only as an explicit compatibility fallback;
- direct replies with bounded correlation chains;
- accept, hold, and refuse inbound controls;
- bounded inboxes, delivery leases, retries, and sender/recipient rate limits;
- dependency-aware tasks with atomic claims;
- explicit acknowledgements and non-mutating liveness status;
- vendor-neutral commands for Hermes, Pi, Antigravity, Claude Code, Codex, or any custom process.

The project is MIT licensed and runtime dependencies are Python standard library only.

## Install

Requires Python 3.10+ on a POSIX system. tmux is optional for socket-only integrations and required only by the convenience `start` launcher.

```bash
git clone https://github.com/oliveralerubio/agent-bridge.git
cd agent-bridge
python3 -m venv .venv
. .venv/bin/activate
python -m pip install .
agent-bridge doctor
```

The default database is `~/.agent-bridge/bridge.sqlite3`. Override it with `--db`, `AGENT_BRIDGE_DB`, or `AGENT_BRIDGE_HOME`.

## Supervised execution

The `run` command is the generic execution entry point. It is independent of
Hermes and accepts only explicit argv arrays, so no shell wrapper or provider
specific runner is required. Phases run serially in one worktree; at most one
phase may have the `writer` role.

Create `execution.json`:

```json
{
  "name": "example-pipeline",
  "cwd": "/path/to/project",
  "phases": [
    {
      "name": "scout",
      "role": "scout",
      "command": ["./scripts/scout", "--json"],
      "timeout": 300
    },
    {
      "name": "implementer",
      "role": "writer",
      "command": ["./scripts/implement"],
      "timeout": 1800
    },
    {
      "name": "reviewer",
      "role": "reviewer",
      "command": ["./scripts/review"],
      "timeout": 600
    },
    {
      "name": "verification",
      "role": "verification",
      "command": ["./scripts/verify"],
      "timeout": 600
    }
  ]
}
```

Run or resume it:

```bash
agent-bridge run --manifest execution.json --json
agent-bridge run --manifest execution.json --resume --json
agent-bridge execution list --json
agent-bridge execution show example-pipeline --json
agent-bridge execution stop example-pipeline --json
```

Each phase receives `AGENT_BRIDGE_EXECUTION_ID`, `AGENT_BRIDGE_PHASE`,
`AGENT_BRIDGE_PHASE_ROLE`, `AGENT_BRIDGE_CHECKPOINT`, and the normal bridge
database/run variables. A phase is `DONE` only when it exits with code zero
and emits this exact line:

```text
AGENT_BRIDGE_AGENT_END phase=scout status=success
```

Exit code zero without that evidence is `PARTIAL`, not success. Non-zero exit
is `FAILED`; a hard timeout is `TIMEOUT`. Completed phases are recorded in the
same SQLite database and an atomic JSON checkpoint, so `--resume` skips only
verified phases and rejects a changed manifest. The supervisor kills the whole
process group on timeout and never runs phases concurrently. An operator can
stop the active phase with `execution stop`; it is recorded as `failed` with a
controlled-stop reason.

The supervisor verifies lifecycle evidence and artifacts generically. Provider
adapters remain responsible for proving provider-specific facts such as the
effective model or native subagent metadata; the core never invents those
claims.

## Event-driven orchestration and bounded fallback

Hermes or another orchestrator should not spend an LLM turn asking whether a
worker has finished. A worker emits a structured completion message, and a
small local `wait` process blocks on the orchestrator's Unix socket. The socket
event wakes it immediately; the parent model is not polled.

Inside the worker, send the completion event only after its own checks are
finished:

```bash
agent-bridge complete \
  --from pi-worker \
  --to hermes-main \
  --status success \
  --summary 'Focused tests and artifact checks passed.' \
  --execution-id example-pipeline \
  --phase implementer \
  --json
```

Run the bounded watcher for the orchestrator. Its success action can start the
next phase without another parent status query:

```bash
agent-bridge wait \
  --run hermes-main \
  --from pi-worker \
  --timeout 1800 \
  --heartbeat-timeout 120 \
  --on-success-command-json '["agent-bridge","run","--manifest","next-phase.json"]' \
  --fallback-command-json '["agent-bridge","run","--manifest","fallback-phase.json"]' \
  --action-timeout 900 \
  --json
```

The generic supervisor can emit the same event automatically after a terminal
execution. Add a recipient by ID or name to the manifest; the message is
persisted before delivery and uses a deterministic idempotency key:

```json
{
  "name": "pi-worker-execution",
  "cwd": ".",
  "notify": {"to": "hermes-main"},
  "phases": [
    {"name": "worker", "role": "writer", "command": ["..."], "timeout": 1800}
  ]
}
```

The supervisor emits `success` only after the phase's `AGENT_BRIDGE_AGENT_END`
proof. Failed, partial, and timeout executions emit non-success completion
statuses; an unavailable recipient leaves the event queued for `wait` to drain.


- `completed`: a matching worker sent `status=success` and any success trigger
  also completed;
- `failed`: the worker reported failure, its owned process disappeared, or its
  heartbeat expired;
- `partial`: the worker exited successfully but never supplied completion
  evidence;
- `timeout`: the bounded deadline expired without a completion event.

A configured fallback is run with an explicit argv array, a private process
group, and its own timeout. Its result is reported as `fallback_status`, but a
failed worker never becomes a false `completed` result merely because fallback
started or exited zero. The wait record, message delivery, acknowledgement,
trigger, and fallback status are durable in `bridge.sqlite3`.

For custom integrations, the triggered process receives the bounded event on
stdin and through `AGENT_BRIDGE_EVENT_JSON` plus `AGENT_BRIDGE_COMPLETION_*`
environment variables. The completion body is not a shell command and is
validated as structured data. This is a generic local protocol; Hermes needs
only a thin adapter that starts `wait` and emits `complete` from the provider
runtime.

For development without installation:

```bash
PYTHONPATH=src python -m agent_bridge doctor
```

## Quick start: tmux launcher

`start` is a convenience launcher. It registers a run, exports the bridge environment, and starts the command in a tmux session.

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

Send a bounded handoff:

```bash
agent-bridge tell \
  --from hermes-main \
  --to pi-worker \
  --message 'The API field is now display_name. Verify the migration and run focused tests.'
```

The bridge first attempts the recipient's structured inbox socket. If no listener is present, it falls back to tmux injection for compatibility. Use `agent-bridge deliver` to retry queued messages after a recipient becomes ready.

```bash
agent-bridge deliver --to pi-worker
agent-bridge inbox pi-worker
agent-bridge status pi-worker --json
agent-bridge ack <message-id> --run pi-worker
```

## Socket-first integration without tmux

Register a process that was launched by another supervisor:

```bash
agent-bridge register \
  --name pi-worker \
  --agent pi \
  --command 'pi --print' \
  --cwd "$PWD" \
  --json
```

The output supplies `AGENT_BRIDGE_RUN_ID`, `AGENT_BRIDGE_DB`, and `AGENT_BRIDGE_INBOX`. An adapter or sidecar can serve the structured inbox:

```bash
agent-bridge listen pi-worker
```

For a one-message test listener:

```bash
agent-bridge listen pi-worker --once --json
```

The socket carries one bounded JSON object per line. A successful listener response is an acknowledgement that the adapter accepted the frame, not that the model understood or acted on it. The recipient must still acknowledge the message after verification.

A minimal adapter can read each JSON object and pass only the `body` to the agent's own approved input mechanism. Peer text is data, never permission.

## Direct replies

A reply is structurally tied to its parent message. Only the original recipient can reply, and replies are limited to eight hops to prevent accidental loops.

```bash
agent-bridge reply <parent-message-id> \
  --from pi-worker \
  --message 'Verified. The focused migration tests pass.'
```

The reply includes `reply_to` in JSON and in the optional tmux envelope.

## Inbound controls

Every run has an inbound policy:

- `accept`: new messages enter the delivery queue;
- `hold`: new messages wait for explicit approval;
- `refuse`: new messages are recorded as refused and are not delivered.

```bash
agent-bridge policy pi-worker hold
agent-bridge hold <message-id> --run pi-worker
agent-bridge accept <message-id> --run pi-worker
agent-bridge refuse <message-id> --run pi-worker
```

The default inbox limit is 100 active messages. Set it at registration or launch with `--max-inbox`. A sender/recipient pair is also rate-limited to 60 messages per minute. These limits protect against accidental message loops and disk growth, not against a malicious process that owns the SQLite file.

## Shared tasks

Tasks provide the small shared work list that agent teams need. Claims are atomic and dependencies must be completed first.

```bash
agent-bridge task create \
  --from hermes-main \
  --title 'Review the migration' \
  --description 'Check schema and focused tests' \
  --json

agent-bridge task list --json
agent-bridge task claim <task-id> --run pi-worker
agent-bridge task complete <task-id> --run pi-worker
```

A task can depend on earlier tasks:

```bash
agent-bridge task create \
  --from hermes-main \
  --title 'Update the worker' \
  --depends-on <parent-task-id>
```

Only the run that atomically claimed a task can complete it. A dependent task cannot be claimed until every parent is `completed`.

## Discover peers and liveness

```bash
agent-bridge peers --active --json
agent-bridge status pi-worker --json
agent-bridge heartbeat --run pi-worker
```

`status` reports socket and tmux availability without changing the durable run state. This avoids the old failure mode where a transiently absent tmux session permanently changed a run to `missing`.

## Built-in presets

| Preset | Default command | Notes |
|---|---|---|
| `hermes` | `hermes` | Hermes Agent CLI |
| `pi` | `pi` | Pi coding agent |
| `agy` | `agy` | Antigravity CLI |
| `claude` | `claude` | Claude Code CLI |
| `codex` | `codex` | OpenAI Codex CLI |

Presets are convenience defaults only. Use `--command` or `register` for any other agent, wrapper, script, or local process.

## Protocol guarantees

- **Bounded payloads:** bodies are capped at 16,384 characters and reject unsupported control characters.
- **No implicit context transfer:** the bridge sends only the body and small run metadata. It never reads conversation history or files.
- **Structured local delivery:** Unix sockets carry typed JSON frames. Filesystem mode `0600` is the local trust boundary.
- **Durable queue:** messages are stored before delivery is attempted.
- **Sender-scoped idempotency:** the same sender can safely retry a key; another sender gets a distinct message.
- **Atomic delivery claims:** concurrent workers cannot inject the same message twice.
- **Explicit outcomes:** `queued`, `held`, `delivered`, `acknowledged`, `failed`, and `refused` are distinct.
- **Recipient authorization:** only the recorded recipient can accept, hold, refuse, or acknowledge its messages.
- **Reply integrity:** replies must route from the original recipient back to the original sender.
- **Bounded coordination:** inbox depth, rate, and reply hops are limited.
- **Local by default:** there is no network listener, cloud database, or hosted relay.
- **Peer messages are not consent:** incoming text cannot approve permissions, change configuration, or justify secret disclosure.

## Security model

This is local coordination, not a hostile-process isolation boundary. Any process that can read the SQLite file or act as the same OS user may inspect or inject messages. Use filesystem permissions and separate OS users for stronger isolation.

The bridge never executes message bodies. Adapters must keep peer input separate from user consent and must not automatically run commands merely because a peer requested them.

Do not put credentials, tokens, private transcripts, or unbounded command output into handoffs.

## Anthropic feature mapping

| Claude Code behavior | agent-bridge implementation |
|---|---|
| `ListAgents` discovery | `peers` / `list`, stable run names, IDs, team labels, inbox paths |
| `SendMessage` | `send` / `tell`, direct `reply`, idempotent messages |
| Mailbox | SQLite queue plus per-run Unix socket |
| Accept / hold / refuse | inbound policy and message action commands |
| Shared task list | `task create/list/claim/complete` with dependencies |
| Permission separation | context-only envelopes and explicit recipient actions |
| tmux split-pane display | optional fallback from the socket-first bridge |

This is intentionally an agent-neutral protocol, not an Anthropic API clone. Provider-specific native tool plugins can call the same CLI and SQLite protocol without changing the data model.

## Readiness-aware adapters

The local Unix-socket contract accepts bounded JSON-lines frames for `hello`,
`ready`, `busy`, `idle`, `heartbeat`, `message`, `ack`, `shutdown`, and
`error`. Adapter sessions, readiness, capabilities, heartbeat timestamps, and
failures are persisted in SQLite. A run marked readiness-aware is gated until
that session reports `ready` or `idle`; legacy runs keep the 0.2.0 socket-first
then tmux fallback behavior.

The generic sidecar is intentionally not a provider integration:

```bash
agent-bridge adapter <run-name> --once --json
```

It performs a local hello/ready handshake and accepts one bounded message. It
only prints structured data; it does not drive a Pi, Claude, or other provider
REPL. Heartbeats expire after 30 seconds unless refreshed.

## Teams and lifecycle

Teams and membership are durable SQLite entities. Names are unique, members
have stable derived membership IDs (`member-<team-id>-<run-id>`), run IDs, and
roles, and at most one active member is a lead. A JSON
manifest (read with a bounded 128 KiB limit) can define `name`, `agent`,
`command`, `cwd`, `role`, `lead`, `readiness_timeout`/`startup_timeout`, and
`restart_policy` (`never`, `on-failure`, or `always`):

```bash
agent-bridge team create --manifest team.json --json
agent-bridge team start <team> --json
agent-bridge team status <team> --json
agent-bridge team restart <team> --json
agent-bridge team stop <team> --json
```

Use `team list`, `team show`, and `team member add/remove` for inspection and
membership changes. Manifest members without a tmux session are launched as
bounded, shell-free child processes with persisted PID identity; externally
registered runs are reported offline and are never guessed at or killed.
Restart reuses the durable run and membership IDs, preserving message/task
history.

## Approval, hooks, and reports

Tasks may be `proposed`, `awaiting_approval`, `approved`, `pending`,
`in_progress`, `completed`, `rejected`, `blocked`, `failed`, or `cancelled`.
Approval is available only to a granted local operator or an explicit team lead;
ungrouped tasks therefore require an operator grant, and peer message text is
never approval.
A gated task cannot be claimed before approval.

Hooks are explicit local argv arrays, never shell strings. Hook input is bounded
JSON (16 KiB), output is capped, subprocesses have a timeout, and failures are
recorded in SQLite. Approval/rejection gates fail closed:

```bash
agent-bridge hook add --name audit --event task.approved \
  --command-json '["/usr/bin/logger","agent-bridge approval"]' --fail-closed
agent-bridge hook events --json
agent-bridge audit --json
```

Completion summaries contain bounded `goal`, `verified_facts`, `tests`,
`files_changed`, `blockers`, and `next_action` fields. A summary file is read
at most 32 KiB before JSON parsing:

```bash
agent-bridge task complete <task> --run worker --summary-file summary.json --json
agent-bridge task reports <task> --json
```
Reports can also be attached to failed or rejected tasks. No message body,
transcript, credential, or file content is implicitly transferred.

## Development

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
python3 -m compileall -q src
git diff --check
```

The test suite covers SQLite migrations, idempotency, legal state transitions, reply routing, socket delivery, tmux fallback, concurrent delivery claims, bounded queues, and atomic task claims.

## License

MIT. See [LICENSE](LICENSE).
