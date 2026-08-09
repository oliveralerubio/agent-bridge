from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AgentPreset:
    name: str
    command: str
    mode: str
    description: str


PRESETS: dict[str, AgentPreset] = {
    "hermes": AgentPreset("hermes", "hermes", "interactive", "Hermes Agent CLI"),
    "pi": AgentPreset("pi", "pi", "interactive", "Pi coding agent"),
    "agy": AgentPreset("agy", "agy", "interactive", "Antigravity CLI"),
    "claude": AgentPreset("claude", "claude", "interactive", "Claude Code CLI"),
    "codex": AgentPreset("codex", "codex", "interactive", "OpenAI Codex CLI"),
}


def resolve_preset(agent: str | None, command: str | None) -> tuple[str, str, str]:
    if command:
        name = agent or command.split()[0]
        mode = PRESETS.get(name, AgentPreset(name, command, "interactive", "custom agent")).mode
        return name, command, mode
    if not agent:
        raise ValueError("provide --agent or --command")
    preset = PRESETS.get(agent)
    if preset is None:
        raise ValueError(f"unknown agent preset {agent!r}; provide --command")
    return preset.name, preset.command, preset.mode
