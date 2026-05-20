"""
membot - A lightweight AI agent framework.

The source tree currently keeps implementation packages such as ``cli`` and
``agent`` at the repository root. Extend this package path so imports like
``membot.cli.commands`` resolve to those existing modules.
"""

from __future__ import annotations

from pathlib import Path

__version__ = "0.2.0"
__logo__ = "🐈"

_repo_root = Path(__file__).resolve().parent.parent
if str(_repo_root) not in __path__:
    __path__.append(str(_repo_root))
