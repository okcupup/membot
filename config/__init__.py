"""Configuration module for membot."""

from membot.config.loader import load_config, get_config_path
from membot.config.schema import Config

__all__ = ["Config", "load_config", "get_config_path"]
