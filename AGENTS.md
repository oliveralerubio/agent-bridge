# Contributor instructions

## Project contract

`agent-bridge` is a standalone, local-first, agent-agnostic coordination tool. Do not add provider-specific APIs, cloud dependencies, or implicit transcript/file transfer without an explicit design change and tests.

## Development

- Python 3.10+.
- Runtime dependencies are standard-library only.
- Use `PYTHONPATH=src python3 -m unittest discover -s tests -v`.
- Run `python3 -m compileall -q src` and `git diff --check` before handoff.
- Keep message payloads bounded and local.
- Treat tmux as an optional transport layer; the SQLite protocol must remain testable without tmux.
- Do not commit real credentials, local databases, logs, or generated virtual environments.

## Change discipline

Write a failing test before production behavior. Preserve idempotency, recipient-only acknowledgements, and the distinction between queued, delivered, acknowledged, and failed messages.
