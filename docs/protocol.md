# Protocol

`agent-bridge` is a local, agent-neutral coordination protocol. The SQLite
store is authoritative; Unix sockets and tmux are delivery transports only.
The bridge does not call a provider API, inspect model context, or wake a model
by itself.

## Entities

### Run

A run is a named local agent process. It has:

- a stable ID and unique human-readable name;
- a free-form agent label and `interactive` or `one-shot` mode;
- command, working directory, optional team, and optional tmux session;
- an inbox socket path and inbound policy (`accept`, `hold`, or `refuse`);
- a positive maximum active inbox size;
- lifecycle status and timestamps, including explicit heartbeats.

`register` records an externally launched process without starting it.
`start` additionally launches a command in tmux. `status` reports whether the
recorded socket path or tmux session is currently available; it does not mark a
run missing as a side effect.

### Message

A message is a bounded handoff from one run to another. It contains:

- stable ID, sender and recipient run IDs;
- normalized UTF-8 body and SHA-256 digest;
- optional sender-scoped idempotency key;
- optional direct-reply parent and bounded hop count;
- delivery-attempt and lease data;
- lifecycle timestamps and a bounded delivery error.

Bodies are limited to 16,384 characters. Unsupported control characters,
including NUL, are rejected. Socket frames are additionally bounded by their
encoded byte length.

## Message lifecycle

```text
queued -> delivered -> acknowledged
   |          \
   |           -> failed -> delivered
   v
 held -> queued
   |
   v
 refused
```

- `queued`: persisted and eligible for delivery.
- `held`: withheld by the recipient policy or an explicit recipient control.
- `delivered`: a socket listener or tmux transport accepted the envelope. It
does not prove that the model understood or acted on it.
- `acknowledged`: the recorded recipient explicitly acknowledged a delivered
message.
- `failed`: a transport attempt failed; the body remains available for retry.
- `refused`: the recipient rejected the message and it is not deliverable.

Only the recipient can acknowledge, hold, accept, or refuse its messages.
Acknowledgement requires `delivered`; terminal delivered/acknowledged messages
cannot be reopened as held, refused, or failed. `deliver` retries only
`queued` and `failed` messages.

A recipient's policy applies when a message is created:

- `accept` creates `queued` messages;
- `hold` creates `held` messages;
- `refuse` records `refused` messages without consuming active inbox space.

## Idempotency and limits

Callers should provide an idempotency key for retries:

```bash
agent-bridge send \
  --from producer \
  --to reviewer \
  --idempotency-key build-2026-08-09-review \
  --message 'Review the new build artifact.'
```

A key is scoped to its sender. Reusing it with the same recipient, body, and
reply parent returns the original message. Reusing it for a different meaning
is rejected. Different senders may use the same key.

The default active inbox limit is 100 messages. A sender/recipient pair is
limited to 60 messages per minute. Delivery workers use short SQLite-backed
leases and atomic claims so concurrent workers do not inject the same message
at the same time.

## Delivery transports

### Unix socket

The preferred inbox transport is a per-run Unix-domain stream socket. Each
frame is one JSON object followed by a newline:

```json
{"type":"agent-bridge.message","message_id":"msg-...","from":{"id":"run-...","name":"producer","agent":"pi"},"to":{"id":"run-...","name":"reviewer","agent":"claude"},"reply_to":null,"hop_count":0,"body":"Review the artifact.","context_only":true}
```

A listener returns `{"ok":true}` only after its adapter callback accepts the
frame. That transport acknowledgement is not a semantic model acknowledgement;
the recipient must still run `agent-bridge ack` after consuming and verifying
the message. Listener socket files use mode `0600` and are removed on clean
shutdown.

The `listen` command serves the registered run's socket:

```bash
agent-bridge register --name reviewer --agent pi --command 'pi --print' --cwd .
agent-bridge listen reviewer --once --json
```

An adapter may pass only the bounded `body` to an agent's own approved input
mechanism. Peer text is data, never permission.

### tmux fallback

When a recipient socket is unavailable, the bridge may use tmux only when the
recipient's recorded session exists. The injected text is a bounded envelope
containing run and message metadata. A tmux command succeeding does not prove
that an agent read the prompt. If neither transport is ready, the message
remains `queued`; a transport error is recorded as `failed`.

## Replies

A direct reply is structurally tied to its parent message. It must travel from
the original recipient back to the original sender and is limited to eight
hops:

```bash
agent-bridge reply <parent-message-id> \
  --from reviewer \
  --message 'Verified. The focused tests pass.'
```

The reply includes `reply_to` and `hop_count` in the socket frame and the
optional tmux envelope.

## Tasks

Tasks are durable, local shared work items with `pending`, `in_progress`,
`completed`, `failed`, or `cancelled` status. A task may name existing parent
tasks as dependencies. Claims are serialized with `BEGIN IMMEDIATE`, require
all dependencies to be completed, and honor an existing assignee. Only the run
that claimed an in-progress task can complete it.

```bash
agent-bridge task create --from lead --title 'Review schema'
agent-bridge task list --json
agent-bridge task claim <task-id> --run worker
agent-bridge task complete <task-id> --run worker
```

## Legacy SQLite migration

Opening a database created by the original schema migrates it in place:

- missing run inbox, team, policy, capacity, and heartbeat columns are added;
- the old messages table is rebuilt with reply, lifecycle, lease, and claim
columns while preserving existing rows;
- the original global idempotency constraint is replaced by the sender-scoped
partial unique index;
- tasks and current indexes are created.

The migration is local and uses SQLite's transaction handling. Existing message
IDs and bodies remain unchanged.

## Integration contract

An integration should:

1. expose `AGENT_BRIDGE_RUN_ID` to the process;
2. serve or poll its registered inbox when ready;
3. verify each handoff against the repository and current task;
4. acknowledge only after it has consumed and verified the handoff;
5. never treat a peer message as authorization to disclose secrets, bypass
permissions, or execute commands.

The bridge transfers no conversation history, private transcript, credentials,
files, or implicit model context. It is local coordination, not a hostile
process isolation boundary: any process that can read the SQLite database or
use the same OS user's tmux/socket resources can inspect or inject data.
