"""Structured outcome returned by one complete AgentLoop execution."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class ExecutionOutcome(str, Enum):
    """Technical execution outcome, independent of business-task success."""

    FINAL = "FINAL"
    PROVIDER_ERROR = "PROVIDER_ERROR"
    TOOL_ERROR = "TOOL_ERROR"
    ITERATION_LIMIT = "ITERATION_LIMIT"
    CANCELLED = "CANCELLED"
    TIMEOUT = "TIMEOUT"
    INTERNAL_ERROR = "INTERNAL_ERROR"


@dataclass(slots=True)
class ExecutionResult:
    """Result and diagnostic transcript for one invocation."""

    outcome: ExecutionOutcome
    final_content: str | None
    messages: list[dict[str, Any]]
    tools_used: list[str] = field(default_factory=list)
    error_code: str | None = None
    error_message: str | None = None

    def __post_init__(self) -> None:
        """Keep incomplete Finals and uncoded failures out of success state."""

        if self.outcome is ExecutionOutcome.FINAL and not (
            self.final_content and self.final_content.strip()
        ):
            self.outcome = ExecutionOutcome.PROVIDER_ERROR
            if not self.error_code or self.error_code == ExecutionOutcome.FINAL.value:
                self.error_code = "MISSING_FINAL"
            self.error_message = self.error_message or "execution produced no final content"
        elif self.outcome is not ExecutionOutcome.FINAL and not self.error_code:
            self.error_code = self.outcome.value

    @property
    def technical_success(self) -> bool:
        """Whether the agent produced a normal final response."""

        return self.outcome is ExecutionOutcome.FINAL and bool(
            self.final_content and self.final_content.strip()
        )

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation for durable result storage."""

        value = asdict(self)
        value["outcome"] = self.outcome.value
        value["technical_success"] = self.technical_success
        return value

    @classmethod
    def failure(
        cls,
        outcome: ExecutionOutcome,
        *,
        final_content: str | None = None,
        messages: list[dict[str, Any]] | None = None,
        tools_used: list[str] | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> "ExecutionResult":
        """Create a non-success result without turning its error into a Final."""

        return cls(
            outcome=outcome,
            final_content=final_content,
            messages=messages or [],
            tools_used=tools_used or [],
            error_code=error_code or (
                "MISSING_FINAL" if outcome is ExecutionOutcome.FINAL else outcome.value
            ),
            error_message=error_message,
        )
