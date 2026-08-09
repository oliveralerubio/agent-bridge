from __future__ import annotations

import os
from dataclasses import dataclass

from .models import Message
from .protocol import format_injection
from .store import Store
from .tmux import MessageTransport, TmuxError, TmuxTransport


@dataclass(frozen=True)
class DeliveryResult:
    message: Message
    transport: str


class Bridge:
    def __init__(self, store: Store, tmux: MessageTransport | None = None) -> None:
        self.store = store
        self.tmux = tmux or TmuxTransport()

    @staticmethod
    def current_run_id() -> str | None:
        return os.environ.get("AGENT_BRIDGE_RUN_ID") or os.environ.get("AGENT_RUN_ID")

    def deliver(self, message: Message) -> DeliveryResult:
        sender = self.store.get_run(message.from_run_id)
        recipient = self.store.get_run(message.to_run_id)
        if recipient.status not in {"starting", "running"}:
            return DeliveryResult(message=message, transport="queued")
        if not recipient.tmux_session or not self.tmux.has_session(recipient.tmux_session):
            return DeliveryResult(message=message, transport="queued")
        envelope = format_injection(message, sender, recipient)
        try:
            self.tmux.inject(session=recipient.tmux_session, text=envelope)
        except TmuxError as exc:
            self.store.mark_failed(message.id, str(exc))
            raise
        delivered = self.store.mark_delivered(message.id)
        return DeliveryResult(message=delivered, transport="tmux")
