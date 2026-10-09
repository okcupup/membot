"""Versioned Case contracts; fixtures and expectations have separate owners."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

CATEGORIES = ("basic", "tool_calling", "context_concurrency", "exception_recovery")
TERMINAL = {"SUCCEEDED", "FAILED", "TIMEOUT"}


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     separators=(",", ":"), allow_nan=False).encode()).hexdigest()


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class Budget(StrictModel):
    case_seconds: float = Field(default=8.0, gt=0, le=120)
    queue_seconds: float = Field(default=5.0, gt=0, le=60)
    execution_seconds: float = Field(default=3.0, gt=0, le=60)
    llm_seconds: float = Field(default=1.0, gt=0, le=60)
    tool_seconds: float = Field(default=1.0, gt=0, le=60)
    max_iterations: int = Field(default=4, ge=1, le=16)


class Call(StrictModel):
    turn: str
    name: str
    arguments: dict[str, Any] = Field(default_factory=dict)


class ToolContract(StrictModel):
    required_tools: list[str] = Field(default_factory=list)
    allowed_tools: list[str] = Field(default_factory=list)
    forbidden_tools: list[str] = Field(default_factory=list)
    expected_calls: list[Call] = Field(default_factory=list)
    order: Literal["exact", "subsequence"] = "exact"
    evaluate: bool = True


class ResultAssertion(StrictModel):
    turn: str
    kind: Literal["equals", "contains", "not_contains", "json_equals", "history_contains",
                  "history_absent", "tool_result_contains", "error_code", "delivery_equals"]
    value: Any


class Turn(StrictModel):
    id: str
    input: str = Field(min_length=1, max_length=32768)
    session: str = "a"
    channel: str = "eval"
    chat_id: str | None = None
    expected_status: Literal["SUCCEEDED", "FAILED", "TIMEOUT"] = "SUCCEEDED"
    # This is a visible-response fixture, never the oracle used to grade it.
    provider: list[dict[str, Any]] = Field(min_length=1, max_length=16)


class Case(StrictModel):
    id: str = Field(pattern=r"^[a-z][a-z0-9_-]{2,100}$")
    category: Literal["basic", "tool_calling", "context_concurrency", "exception_recovery"]
    version: int = Field(ge=1)
    description: str
    input: str
    history: list[dict[str, Any]] = Field(default_factory=list, max_length=100)
    fixtures: dict[str, Any] = Field(default_factory=dict)
    expected_status: Literal["SUCCEEDED", "FAILED", "TIMEOUT"]
    turns: list[Turn] = Field(min_length=1, max_length=16)
    scenario: Literal["single", "serial", "parallel", "limit", "hot", "context", "message",
                      "callback", "cancel", "queue_timeout"] = "single"
    tools: ToolContract = Field(default_factory=ToolContract)
    assertions: list[ResultAssertion] = Field(default_factory=list)
    judge_rubric: str | None = None
    rubric_version: str = "m6-v1"
    budget: Budget = Field(default_factory=Budget)
    business_task: bool = True
    real_model: bool = False
    provenance: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def coherent(self):
        ids = [turn.id for turn in self.turns]
        if len(ids) != len(set(ids)) or self.input != self.turns[0].input:
            raise ValueError("unique turn IDs and matching primary input are required")
        if self.expected_status != self.turns[0].expected_status:
            raise ValueError("primary expected_status must match the first turn")
        if set(self.tools.allowed_tools) & set(self.tools.forbidden_tools):
            raise ValueError("allowed and forbidden tools overlap")
        if not set(self.tools.required_tools) <= set(self.tools.allowed_tools):
            raise ValueError("required tools must be allowed")
        if any(call.turn not in ids or call.name not in self.tools.allowed_tools
               for call in self.tools.expected_calls):
            raise ValueError("expected tool calls must reference an allowed tool and turn")
        if any(assertion.turn not in ids for assertion in self.assertions):
            raise ValueError("assertion references an unknown turn")
        if not self.assertions and not self.judge_rubric:
            raise ValueError("status alone is not a task oracle")
        if self.real_model and not self.judge_rubric and not self.assertions:
            raise ValueError("real-model cases need an oracle")
        return self

    @property
    def case_hash(self) -> str:
        return digest(self.model_dump())


def load_cases(path: Path | None = None) -> list[Case]:
    root = path or Path(__file__).parent / "cases"
    files = [root] if root.is_file() else sorted(root.rglob("*.json"))
    cases = []
    for file in files:
        if file.stat().st_size > 2_097_152:
            raise ValueError(f"Case file too large: {file.name}")
        value = json.loads(file.read_text())
        cases.extend(Case.model_validate(item) for item in (value if isinstance(value, list) else [value]))
    ids = [case.id for case in cases]
    if not cases or len(ids) != len(set(ids)):
        raise ValueError("suite must contain unique, runnable Cases")
    return cases
