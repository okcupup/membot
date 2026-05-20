"""Cron types and small documentation.

本模块定义了 Cron 服务使用的数据结构。所有时间均以毫秒（ms）为单位
以便与 Unix 时间戳(毫秒) 交换。

类型说明：
- CronSchedule: 调度定义。
  - kind: 'at' / 'every' / 'cron'
  - at_ms: 当 kind='at' 时使用，为 UTC 毫秒时间戳（>= 0）
  - every_ms: 当 kind='every' 时为间隔毫秒数（>0）
  - expr: 当 kind='cron' 时为 cron 表达式（如 '0 9 * * *'）
  - tz: 可选的 IANA 时区，仅对 cron 表达式生效

- CronPayload: 执行时要执行的操作描述（目前支持将消息投递到 channel/to）

- CronJobState: 运行时状态（下一次运行、上次运行时间、最近状态/错误）

- CronJob: 表示一条任务，包含 schedule、payload、state、元信息与 control 字段（delete_after_run）

- CronStore: 持久化格式，包含版本与任务列表。

示例：
>>> CronSchedule(kind='cron', expr='0 9 * * *', tz='Asia/Shanghai')
>>> CronPayload(message='开会提醒', deliver=True, channel='cli', to='direct')

这些 dataclass 在 `cron/service.py` 中被序列化/反序列化为磁盘上的 JSON，对应字段名
使用驼峰或驼峰化键（例如 atMs, everyMs, expr, tz 等）在持久化 JSON 中出现。
"""

from dataclasses import dataclass, field
from typing import Literal


@dataclass
class CronSchedule:
    """Schedule definition for a cron job.

    Fields:
    - kind: One of 'at'|'every'|'cron'
    - at_ms: For 'at' schedules, timestamp in ms
    - every_ms: For 'every' schedules, interval in ms
    - expr: For 'cron' schedules, cron expression
    - tz: Optional timezone name (IANA) for cron schedules
    """
    kind: Literal["at", "every", "cron"]
    # For "at": timestamp in ms
    at_ms: int | None = None
    # For "every": interval in ms
    every_ms: int | None = None
    # For "cron": cron expression (e.g. "0 9 * * *")
    expr: str | None = None
    # Timezone for cron expressions
    tz: str | None = None


@dataclass
class CronPayload:
    """What to do when the job runs.

    - kind: 'system_event' or 'agent_turn' (default 'agent_turn')
    - message: payload message text
    - deliver: whether to deliver the response to `channel`/`to`
    - channel/to: optional delivery target
    """
    kind: Literal["system_event", "agent_turn"] = "agent_turn"
    message: str = ""
    # Deliver response to channel
    deliver: bool = False
    channel: str | None = None  # e.g. "whatsapp"
    to: str | None = None  # e.g. phone number


@dataclass
class CronJobState:
    """Runtime state of a job.

    - next_run_at_ms: next scheduled run timestamp (ms) or None
    - last_run_at_ms: last run timestamp (ms) or None
    - last_status: 'ok'|'error'|'skipped' or None
    - last_error: last error message if any
    """
    next_run_at_ms: int | None = None
    last_run_at_ms: int | None = None
    last_status: Literal["ok", "error", "skipped"] | None = None
    last_error: str | None = None


@dataclass
class CronJob:
    """A scheduled job.

    - id: short id string
    - name: human friendly name
    - enabled: whether job is active
    - schedule/payload/state: corresponding dataclasses
    - created_at_ms/updated_at_ms: metadata timestamps
    - delete_after_run: for 'at' jobs, whether to remove after executed
    """
    id: str
    name: str
    enabled: bool = True
    schedule: CronSchedule = field(default_factory=lambda: CronSchedule(kind="every"))
    payload: CronPayload = field(default_factory=CronPayload)
    state: CronJobState = field(default_factory=CronJobState)
    created_at_ms: int = 0
    updated_at_ms: int = 0
    delete_after_run: bool = False


@dataclass
class CronStore:
    """Persistent store for cron jobs.

    - version: storage schema version
    - jobs: list of CronJob
    """
    version: int = 1
    jobs: list[CronJob] = field(default_factory=list)
