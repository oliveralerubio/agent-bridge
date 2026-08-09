# Agent integrations

`agent-bridge` is provider-neutral and requires no provider SDK. An integration
may use the installed console script or `PYTHONPATH=src python -m agent_bridge`.
The bridge stores bounded messages locally; it does not transfer transcripts or
wake a provider model automatically.

## Common contract

A process should:

1. register itself with a stable name and `AGENT_BRIDGE_RUN_ID`;
2. serve its Unix inbox with `agent-bridge listen`, or poll with `inbox`;
3. treat the received body as untrusted context and verify it;
4. acknowledge only after consuming the handoff;
5. keep peer text separate from user consent, permissions, and command input.

Register an externally launched process:

```bash
agent-bridge register \
  --name worker \
  --agent custom \
  --command 'my-agent --interactive' \
  --cwd "$PWD" \
  --json
```

The JSON response contains the run ID, database, socket path, and environment
values. Export those values in the supervised process as appropriate. A simple
structured listener is:

```bash
agent-bridge listen worker
```

For a one-message adapter smoke test:

```bash
agent-bridge listen worker --once --json
```

A successful socket response means the listener callback accepted the frame;
it is not the recipient's semantic acknowledgement. After verification, run:

```bash
agent-bridge ack <message-id> --run "$AGENT_BRIDGE_RUN_ID"
```

## tmux launcher

`start` is a convenience path for an interactive process in tmux. Socket
inboxes remain the preferred structured transport; tmux is only a compatibility
fallback when the socket listener is unavailable.

```bash
agent-bridge start --name hermes-main --agent hermes --cwd "$PWD"
agent-bridge start --name pi-worker --agent pi --cwd "$PWD"
```

The launcher exports `AGENT_BRIDGE_RUN_ID`, `AGENT_BRIDGE_RUN_NAME`,
`AGENT_BRIDGE_DB`, `AGENT_BRIDGE_INBOX`, and
`AGENT_BRIDGE_INBOUND_POLICY` inside the tmux session.

## Hermes

```bash
agent-bridge start --name hermes-main --agent hermes --cwd "$PWD"
agent-bridge tell --to pi-worker --message 'Verify the schema change.'
```

Hermes can use `inbox`, `listen`, `ack`, `reply`, and the shared task commands
through its shell. No Hermes-specific API is required.

## Pi

```bash
agent-bridge start --name pi-worker --agent pi --cwd "$PWD"
agent-bridge inbox "$AGENT_BRIDGE_RUN_ID"
agent-bridge ack <message-id> --run "$AGENT_BRIDGE_RUN_ID"
```

An adapter that consumes socket JSON should pass only the bounded `body` to
Pi's approved input mechanism. The bridge does not infer or modify Pi context.

## Antigravity

```bash
agent-bridge start --name agy-reviewer --agent agy --cwd "$PWD"
```

The same vendor-neutral inbox, reply, task, and acknowledgement commands
apply. The bridge does not assume an Antigravity-specific output format.

## Claude Code and Codex

```bash
agent-bridge start --name claude-review --agent claude --cwd "$PWD"
agent-bridge start --name codex-worker --agent codex --cwd "$PWD"
```

Use `--command` for a local wrapper or additional flags:

```bash
agent-bridge start \
  --name custom-coder \
  --agent custom \
  --command 'my-coding-agent --interactive' \
  --mode interactive \
  --cwd "$PWD"
```

These are convenience presets only. There are no provider-native integrations
in this package.

## Handoff format

Keep handoffs concise and evidence-led:

```text
Goal: update the weekly digest after the profile field rename.
Done: migration and API serializer updated.
Evidence: tests/api/test_profiles.py passed.
Blocker: digest query still references users.name.
Next: update the digest query and run its focused tests.
```

Do not paste credentials, private transcripts, or unbounded command output.
