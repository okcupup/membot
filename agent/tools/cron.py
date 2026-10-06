"""Cron tool for scheduling reminders and tasks.

详细说明（中文）:

- 目的: 为 agent 提供可由 LLM 调用的工具 (Tool)，用来在应用的 Cron 服务
  中添加/列出/移除定时任务（提醒、周期性消息等）。
- 适用场景: 当用户在对话中希望 "在每天早上 9 点提醒我" 或 "每小时做一次状态检查"
  时，模型可以调用本工具以编程方式创建对应的 Cron job。

实现细节:
- 本工具是对 `membot.cron.service.CronService` 的轻量封装，暴露三个动作:
  - add: 创建任务（支持每 N 秒、cron 表达式、一次性 at 时间）
  - list: 列出当前任务
  - remove: 删除指定任务
- 添加任务时会使用当前上下文 (`set_context`) 中保存的 channel/chat_id，
  以便任务触发时能够把消息投递回原来的会话目标。
- 对于一次性任务（`at`），工具会自动将任务标记为 `delete_after_run`，执行一次后删除。

参数与返回值约定:
- 所有方法均返回字符串（便于模型直接将返回内容展示给用户）。
- `execute` 是 Tool 基类约定的入口，使用 `action` 字段分发到对应的内部方法。

示例用法（伪 JSON 参数，LLM 调用）:
{
  "action": "add",
  "message": "提醒：开会",
  "cron_expr": "0 9 * * *",
  "tz": "Asia/Shanghai"
}

注意事项:
- 时间字符串 `at` 必须是 ISO 格式，可被 `datetime.fromisoformat` 解析；
- `tz` 仅在提供 `cron_expr` 时有效（本实现会验证 timezone 是否存在）；
- 本模块只负责参数验证与 CronService 的封装，实际的任务调度与持久化由
  `CronService` 管理；因此本工具不会直接抛出未捕获异常（会将错误以字符串形式返回）。
"""

from typing import Any

from membot.agent.tools.base import Tool
from membot.cron.service import CronService
from membot.cron.types import CronSchedule


class CronTool(Tool):
    """Tool to schedule reminders and recurring tasks.

    详细说明（类级别）:
    - __init__(cron_service): 保存 CronService 实例，用于增删查 job。
    - set_context(channel, chat_id): 当工具在对话中被调用时，外部框架应先调用
      本方法以设置投递目标（例如 'telegram:12345' 中的 channel='telegram'，chat_id='12345'）。
    - name / description / parameters: 描述 Tool 的元信息，供 MCP 或 tools registry 使用。
    - execute(...): 入口方法，按 action 分发到 _add_job / _list_jobs / _remove_job。
    """

    def __init__(self, cron_service: CronService):
        # CronService 实例（负责实际的任务持久化与调度）
        self._cron = cron_service
        # 在对话中调用 Tool 前需由外部设置上下文（delivery 目标）。
        self._channel = ""
        self._chat_id = ""

    def set_context(self, channel: str, chat_id: str) -> None:
        """Set the current session context for delivery.

        参数:
        - channel: 目标渠道标识（如 'telegram', 'whatsapp', 'cli' 等）
        - chat_id: 渠道内的会话 id（例如 Telegram chat id 或 WhatsApp phone）

        在创建任务时，这两个值会被传给 `CronService.add_job`，用于任务触发时
        将生成的消息投递回原始会话。
        """
        self._channel = channel
        self._chat_id = chat_id

    def clone_for_execution(self) -> "CronTool":
        """Create an invocation-owned delivery context."""
        return CronTool(self._cron)

    @property
    def name(self) -> str:
        return "cron"

    @property
    def description(self) -> str:
        return "Schedule reminders and recurring tasks. Actions: add, list, remove."

    @property
    def parameters(self) -> dict[str, Any]:
        """Return the JSON-schema-like parameters description.

        该结构用于向 MCP 或调用方描述可接受的参数。字段说明：
        - action: 必需，取值 add/list/remove
        - message: add 时的提醒文本（add 必须有 message）
        - every_seconds: 周期任务间隔（秒）
        - cron_expr: 标准 cron 表达式（与 tz 一起使用用于复杂日程）
        - tz: IANA 时区，仅对 cron_expr 有效（例如 'Asia/Shanghai'）
        - at: 一次性任务的 ISO 时间字符串（例如 '2026-02-12T10:30:00'）
        - job_id: remove 时所需的任务 id
        """
        return {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["add", "list", "remove"],
                    "description": "Action to perform"
                },
                "message": {
                    "type": "string",
                    "description": "Reminder message (for add)"
                },
                "every_seconds": {
                    "type": "integer",
                    "description": "Interval in seconds (for recurring tasks)"
                },
                "cron_expr": {
                    "type": "string",
                    "description": "Cron expression like '0 9 * * *' (for scheduled tasks)"
                },
                "tz": {
                    "type": "string",
                    "description": "IANA timezone for cron expressions (e.g. 'America/Vancouver')"
                },
                "at": {
                    "type": "string",
                    "description": "ISO datetime for one-time execution (e.g. '2026-02-12T10:30:00')"
                },
                "job_id": {
                    "type": "string",
                    "description": "Job ID (for remove)"
                }
            },
            "required": ["action"]
        }

    async def execute(
        self,
        action: str,
        message: str = "",
        every_seconds: int | None = None,
        cron_expr: str | None = None,
        tz: str | None = None,
        at: str | None = None,
        job_id: str | None = None,
        **kwargs: Any
    ) -> str:
        """Main entry point called by the agent runtime.

        根据 `action` 调度到对应的私有处理函数，并将结果作为字符串返回。
        返回值为用户/模型友好的信息，不直接抛异常。
        """
        if action == "add":
            return self._add_job(message, every_seconds, cron_expr, tz, at)
        elif action == "list":
            return self._list_jobs()
        elif action == "remove":
            return self._remove_job(job_id)
        return f"Unknown action: {action}"

    def _add_job(
        self,
        message: str,
        every_seconds: int | None,
        cron_expr: str | None,
        tz: str | None,
        at: str | None,
    ) -> str:
        """Validate parameters and create a Cron job via CronService.

        验证逻辑要点：
        - message 必须存在；
        - 必须已通过 `set_context` 设置 channel/chat_id；
        - tz 只有在 cron_expr 提供时才被接受，并额外验证该时区是否存在；
        - 支持三种 schedule 类型：every（周期）、cron（表达式）、at（一次性）。

        对于一次性任务，会自动把 `delete_after_run` 设为 True，使任务执行后移除。
        """
        if not message:
            return "Error: message is required for add"
        if not self._channel or not self._chat_id:
            return "Error: no session context (channel/chat_id)"
        if tz and not cron_expr:
            return "Error: tz can only be used with cron_expr"
        if tz:
            from zoneinfo import ZoneInfo
            try:
                ZoneInfo(tz)
            except (KeyError, Exception):
                return f"Error: unknown timezone '{tz}'"

        # Build schedule
        delete_after = False
        if every_seconds:
            schedule = CronSchedule(kind="every", every_ms=every_seconds * 1000)
        elif cron_expr:
            schedule = CronSchedule(kind="cron", expr=cron_expr, tz=tz)
        elif at:
            from datetime import datetime
            try:
                dt = datetime.fromisoformat(at)
            except Exception:
                return "Error: 'at' must be ISO datetime string"
            at_ms = int(dt.timestamp() * 1000)
            schedule = CronSchedule(kind="at", at_ms=at_ms)
            delete_after = True
        else:
            return "Error: either every_seconds, cron_expr, or at is required"

        job = self._cron.add_job(
            name=message[:30],
            schedule=schedule,
            message=message,
            deliver=True,
            channel=self._channel,
            to=self._chat_id,
            delete_after_run=delete_after,
        )
        return f"Created job '{job.name}' (id: {job.id})"

    def _list_jobs(self) -> str:
        """Return a human-readable list of scheduled jobs."""
        jobs = self._cron.list_jobs()
        if not jobs:
            return "No scheduled jobs."
        lines = [f"- {j.name} (id: {j.id}, {j.schedule.kind})" for j in jobs]
        return "Scheduled jobs:\n" + "\n".join(lines)

    def _remove_job(self, job_id: str | None) -> str:
        """Remove a job by id. Returns a user-friendly message."""
        if not job_id:
            return "Error: job_id is required for remove"
        if self._cron.remove_job(job_id):
            return f"Removed job {job_id}"
        return f"Job {job_id} not found"
