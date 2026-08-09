from __future__ import annotations

import os
import uuid
from dataclasses import dataclass

from .models import Message, Run
from .protocol import format_injection, socket_payload
from .socket_transport import SocketTransportError, SocketUnavailable, UnixSocketTransport
from .store import Store
from .tmux import MessageTransport, TmuxError, TmuxTransport


@dataclass(frozen=True)
class DeliveryResult:
    message: Message
    transport: str


class Bridge:
    def __init__(
        self,
        store: Store,
        tmux: MessageTransport | None = None,
        socket: UnixSocketTransport | None = None,
    ) -> None:
        self.store = store
        self.tmux = tmux or TmuxTransport()
        self.socket = socket or UnixSocketTransport()

    @staticmethod
    def current_run_id() -> str | None:
        return os.environ.get("AGENT_BRIDGE_RUN_ID") or os.environ.get("AGENT_RUN_ID")

    def deliver(self, message: Message) -> DeliveryResult:
        message = self.store.get_message(message.id)
        if message.status == "held":
            return DeliveryResult(message=message, transport="held")
        if message.status == "refused":
            return DeliveryResult(message=message, transport="refused")
        if message.status == "acknowledged":
            return DeliveryResult(message=message, transport="already-delivered")
        if message.status not in {"queued", "failed"}:
            return DeliveryResult(message=message, transport="in-flight")

        recipient = self.store.get_run(message.to_run_id)
        sender = self.store.get_run(message.from_run_id)
        if recipient.inbound_policy == "hold":
            held = self.store.hold_message(message.id, recipient.id)
            return DeliveryResult(message=held, transport="held")
        if recipient.inbound_policy == "refuse":
            refused = self.store.refuse_message(message.id, recipient.id)
            return DeliveryResult(message=refused, transport="refused")
        if recipient.status not in {"starting", "running", "missing"}:
            return DeliveryResult(message=message, transport="queued")

        claim_id = uuid.uuid4().hex
        if not self.store.claim_delivery(message.id, claim_id):
            current = self.store.get_message(message.id)
            transport = "already-delivered" if current.status in {"delivered", "acknowledged"} else "in-flight"
            return DeliveryResult(message=current, transport=transport)

        payload = socket_payload(message, sender, recipient)
        try:
            if recipient.inbox_path:
                try:
                    self.socket.send(path=recipient.inbox_path, payload=payload)
                except (SocketUnavailable, ConnectionError):
                    if not self._deliver_tmux_or_queue(message, sender, recipient, claim_id):
                        queued = self.store.get_message(message.id)
                        return DeliveryResult(message=queued, transport="queued")
                    delivered = self.store.mark_delivered(message.id, claim_id)
                    return DeliveryResult(message=delivered, transport="tmux")
                else:
                    delivered = self.store.mark_delivered(message.id, claim_id)
                    return DeliveryResult(message=delivered, transport="socket")
            if not self._deliver_tmux_or_queue(message, sender, recipient, claim_id):
                queued = self.store.get_message(message.id)
                return DeliveryResult(message=queued, transport="queued")
            delivered = self.store.mark_delivered(message.id, claim_id)
            return DeliveryResult(message=delivered, transport="tmux")
        except (TmuxError, SocketTransportError) as exc:
            failed = self.store.mark_failed(message.id, str(exc), claim_id)
            raise type(exc)(failed.error or str(exc)) from exc
        except Exception:
            self.store.release_delivery_claim(message.id, claim_id)
            raise

    def _deliver_tmux_or_queue(self, message: Message, sender: Run, recipient: Run, claim_id: str) -> bool:
        if not recipient.tmux_session or not self.tmux.has_session(recipient.tmux_session):
            self.store.release_delivery_claim(message.id, claim_id)
            return False
        envelope = format_injection(message, sender, recipient)
        self.tmux.inject(session=recipient.tmux_session, text=envelope)
        return True

    def drain_pending(self, to_run_id: str | None = None) -> list[DeliveryResult]:
        results: list[DeliveryResult] = []
        for message in list(self.store.iter_pending(to_run_id)):
            try:
                result = self.deliver(message)
            except (TmuxError, SocketTransportError, ConnectionError):
                continue
            results.append(result)
        return results
