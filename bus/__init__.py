"""Message bus module for decoupled channel-agent communication."""

from membot.bus.events import InboundMessage, OutboundMessage
from membot.bus.queue import MessageBus

__all__ = ["MessageBus", "InboundMessage", "OutboundMessage"]
