"""Chat channels module with plugin architecture."""

from membot.channels.base import BaseChannel
from membot.channels.manager import ChannelManager

__all__ = ["BaseChannel", "ChannelManager"]
