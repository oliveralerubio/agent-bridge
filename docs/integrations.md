# Agent integrations

`agent-bridge` does not require a provider SDK. Any process that can run in a tmux pane can participate.

## Hermes

```bash
agent-bridge start --name hermes-main --agent hermes --cwd "$PWD"
```

Inside Hermes, use the terminal tool or a shell command:

```bash
agent-bridge tell --to pi-worker --message 'The API contract changed. Re-read the schema and update your tests.'
```

For a native Hermes tool integration, a future adapter can expose `agent-bridge tell` as a first-class tool while preserving the same SQLite protocol.

## Pi

```bash
agent-bridge start --name pi-worker --agent pi --cwd "$PWD"
```

The bridge exports `AGENT_BRIDGE_RUN_ID`, `AGENT_BRIDGE_RUN_NAME`, and `AGENT_BRIDGE_DB` inside the pane. Pi can call the CLI through its shell tool:

```bash
agent-bridge inbox "$AGENT_BRIDGE_RUN_ID"
agent-bridge ack <message-id> --run "$AGENT_BRIDGE_RUN_ID"
```

## Antigravity

```bash
agent-bridge start --name agy-reviewer --agent agy --cwd "$PWD"
```

The same inbox and acknowledgement commands apply. The bridge does not assume Antigravity-specific output formats.

## Claude Code and Codex

```bash
agent-bridge start --name claude-review --agent claude --cwd "$PWD"
agent-bridge start --name codex-worker --agent codex --cwd "$PWD"
```

Use `--command` if your local wrapper or login flow needs additional flags:

```bash
agent-bridge start \
  --name custom-coder \
  --agent custom \
  --command 'my-coding-agent --interactive' \
  --mode interactive \
  --cwd "$PWD"
```

## Agent-generated handoffs

A useful handoff is concise and evidence-led:

```text
Goal: update the weekly digest after the profile field rename.
Done: migration and API serializer updated.
Evidence: tests/api/test_profiles.py passed.
Blocker: digest query still references users.name.
Next: update the digest query and run its focused tests.
```

Avoid pasting full transcripts, credentials, or unbounded command output. The receiver should verify every claim.
