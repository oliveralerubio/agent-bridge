from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from typing import Mapping

from .models import Message, Run
from .protocol import format_injection, socket_payload, validate_adapter_frame
from .socket_transport import SocketTransportError, SocketUnavailable, UnixSocketTransport
from .store import Store
from .tmux import MessageTransport, TmuxError, TmuxTransport


@dataclass(frozen=True)
class DeliveryResult:
    message: Message
    transport: str


@dataclass(frozen=True)
class AdapterEventResult:
    run: Run
    deliveries: tuple[DeliveryResult, ...] = ()


class Bridge:
    def __init__(self, store: Store, tmux: MessageTransport | None = None, socket: UnixSocketTransport | None = None) -> None:
        self.store = store
        self.tmux = tmux or TmuxTransport()
        self.socket = socket or UnixSocketTransport()

    @staticmethod
    def current_run_id() -> str | None:
        return os.environ.get("AGENT_BRIDGE_RUN_ID") or os.environ.get("AGENT_RUN_ID")

    def deliver(self, message: Message) -> DeliveryResult:
        message = self.store.get_message(message.id)
        if message.status == "held": return DeliveryResult(message, "held")
        if message.status == "refused": return DeliveryResult(message, "refused")
        if message.status == "acknowledged": return DeliveryResult(message, "already-delivered")
        if message.status not in {"queued", "failed"}: return DeliveryResult(message, "in-flight")
        recipient = self.store.get_run(message.to_run_id); sender = self.store.get_run(message.from_run_id)
        if recipient.inbound_policy == "hold": return DeliveryResult(self.store.hold_message(message.id, recipient.id), "held")
        if recipient.inbound_policy == "refuse": return DeliveryResult(self.store.refuse_message(message.id, recipient.id), "refused")
        if recipient.status not in {"starting", "running", "missing"}: return DeliveryResult(message, "queued")
        self.store.expire_adapter_heartbeats()
        recipient = self.store.get_run(recipient.id)
        # A handshake session or a manifest that requires readiness is a hard
        # gate. Legacy registered/tmux runs retain the 0.2.0 fallback.
        if not self.store.adapter_ready(recipient):
            return DeliveryResult(message, "not-ready")
        claim_id = uuid.uuid4().hex
        if not self.store.claim_delivery(message.id, claim_id):
            current = self.store.get_message(message.id)
            return DeliveryResult(current, "already-delivered" if current.status in {"delivered", "acknowledged"} else "in-flight")
        payload = socket_payload(message, sender, recipient)
        adapter_aware = bool(recipient.adapter_session_id or recipient.readiness_required)
        try:
            if recipient.inbox_path:
                try:
                    self.socket.send(path=recipient.inbox_path, payload=payload)
                except (SocketUnavailable, ConnectionError):
                    if adapter_aware:
                        self.store.release_delivery_claim(message.id, claim_id)
                        return DeliveryResult(self.store.get_message(message.id), "queued")
                    if not self._deliver_tmux_or_queue(message, sender, recipient, claim_id):
                        return DeliveryResult(self.store.get_message(message.id), "queued")
                    return DeliveryResult(self.store.mark_delivered(message.id, claim_id), "tmux")
                else:
                    return DeliveryResult(self.store.mark_delivered(message.id, claim_id), "socket")
            if adapter_aware:
                self.store.release_delivery_claim(message.id, claim_id)
                return DeliveryResult(self.store.get_message(message.id), "queued")
            if not self._deliver_tmux_or_queue(message, sender, recipient, claim_id):
                return DeliveryResult(self.store.get_message(message.id), "queued")
            return DeliveryResult(self.store.mark_delivered(message.id, claim_id), "tmux")
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
        self.tmux.inject(session=recipient.tmux_session, text=format_injection(message, sender, recipient))
        return True

    def drain_pending(self, to_run_id: str | None = None) -> list[DeliveryResult]:
        results: list[DeliveryResult] = []
        for message in list(self.store.iter_pending(to_run_id)):
            try: result = self.deliver(message)
            except (TmuxError, SocketTransportError, ConnectionError): continue
            results.append(result)
        return results

    def handle_adapter_frame(self, frame: Mapping[str, object]) -> AdapterEventResult:
        normalized = validate_adapter_frame(dict(frame))
        run = self.store.adapter_event(normalized)
        deliveries: tuple[DeliveryResult, ...] = ()
        if normalized["type"] in {"ready", "idle"}:
            # Readiness is not delivery acknowledgement.  A message becomes
            # delivered only after a real socket or tmux transport accepts it.
            deliveries = tuple(self.drain_pending(run.id))
        return AdapterEventResult(self.store.get_run(run.id), deliveries)

    def request_shutdown(self, run_id: str, reason: str = "operator requested shutdown") -> bool:
        run = self.store.get_run(run_id)
        if not run.adapter_session_id or not run.inbox_path:
            return False
        payload = {"type": "agent-bridge.shutdown", "run_id": run.id, "session_id": run.adapter_session_id, "reason": reason[:256]}
        self.socket.send(path=run.inbox_path, payload=payload)
        return True
