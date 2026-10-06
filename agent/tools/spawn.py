"""Spawn tool for creating background subagents."""

from typing import TYPE_CHECKING, Any

from membot.agent.tools.base import Tool

if TYPE_CHECKING:
    from membot.agent.subagent import SubagentManager


class SpawnTool(Tool):
    """Tool to spawn a subagent for background task execution."""
    
    def __init__(self, manager: "SubagentManager"):
        self._manager = manager
        self._origin_channel = "cli"
        self._origin_chat_id = "direct"
        self._session_key = "cli:direct"
        self._request_id: str | None = None
        self._trace_id: str | None = None
        self._invocation_id: str | None = None
    
    def set_context(self, channel: str, chat_id: str) -> None:
        """Set the origin context for subagent announcements."""
        self._origin_channel = channel
        self._origin_chat_id = chat_id
        self._session_key = f"{channel}:{chat_id}"

    def clone_for_execution(self) -> "SpawnTool":
        """Create an invocation-owned routing wrapper around the manager."""
        return SpawnTool(manager=self._manager)

    def set_session_key(self, session_key: str) -> None:
        """Keep custom/thread-scoped sessions associated with their real owner."""
        self._session_key = session_key

    def set_correlation(
        self,
        request_id: str | None = None,
        trace_id: str | None = None,
        invocation_id: str | None = None,
    ) -> None:
        """Attach the parent invocation identity to spawned work."""
        self._request_id = request_id
        self._trace_id = trace_id
        self._invocation_id = invocation_id
    
    @property
    def name(self) -> str:
        return "spawn"
    
    @property
    def description(self) -> str:
        return (
            "Spawn a subagent to handle a task in the background. "
            "Use this for complex or time-consuming tasks that can run independently. "
            "The subagent will complete the task and report back when done."
        )
    
    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "task": {
                    "type": "string",
                    "description": "The task for the subagent to complete",
                },
                "label": {
                    "type": "string",
                    "description": "Optional short label for the task (for display)",
                },
            },
            "required": ["task"],
        }
    
    async def execute(self, task: str, label: str | None = None, **kwargs: Any) -> str:
        """Spawn a subagent to execute the given task."""
        return await self._manager.spawn(
            task=task,
            label=label,
            origin_channel=self._origin_channel,
            origin_chat_id=self._origin_chat_id,
            session_key=self._session_key,
            request_id=self._request_id,
            trace_id=self._trace_id,
            invocation_id=self._invocation_id,
        )
