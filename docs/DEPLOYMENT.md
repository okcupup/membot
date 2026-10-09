# 单机部署与演练

M5 的拓扑是 Nginx、两个 aiohttp API 进程、一个 asyncio Worker 进程、
PostgreSQL 16、Redis 7 和一次性 migration。API 不构造 AgentLoop；Worker
持有 PostgreSQL advisory lock，同一数据库拒绝第二个执行 Worker。主 Invocation
并发由该进程的 Semaphore 限制，不运行 Gunicorn、多进程 executor 或 prefork。

## 准备与启动

需要 Python 3.11 项目虚拟环境、Docker Engine 27.5.1 或兼容版本、Compose
2.32.4 或更新版本，以及 Make。镜像标签与 manifest digest 在
`deploy/images.env`；构建和运行依赖在两个 `*.lock` 文件中。更新依赖时先在
`.venv` 安装、检查，然后运行 `scripts/lock_dependencies.py`、干净 wheel 检查
和完整测试。版本锁不能替代升级时的安全审核，也没有声称为包内容的 hash 锁。

网络受限时可预下载同一锁文件的 wheel，然后设置 `MEMBOT_BUILD_OFFLINE=1`：

```bash
./.venv/bin/python -m pip download --only-binary=:all: -d deploy/wheelhouse -r deploy/build-requirements.lock -r deploy/requirements.lock
# 在 .env 中加 MEMBOT_BUILD_OFFLINE=1；基础镜像也必须预先拉取
make deploy-up
```

wheelhouse 通过 BuildKit 临时挂载，不进入运行镜像；缺包则构建明确失败。

本地确定性验收使用显式 fake Provider，并创建仅供本机信任的七天自签名证书：

```bash
make deploy-init-local ENV_FILE=.env.local
make deploy-config ENV_FILE=.env.local
make deploy-up ENV_FILE=.env.local
make deploy-health ENV_FILE=.env.local
make deploy-doctor ENV_FILE=.env.local
make deploy-smoke ENV_FILE=.env.local
```

初始化不会覆盖已有 env、私钥或 Provider key 文件。脚本以数据解析 `.env`，
不 `source` 或 shell eval；`MEMBOT_CLIENT_CA` 是本地测试客户端显式信任的
证书。smoke 校验证书和主机名，不使用 `-k`。fake Provider 只验证工程路径。

真实主机先复制 `.env.example`，设置密码、域名、TLS 和模型凭据：

```bash
cp .env.example .env
mkdir -p deploy/certs deploy/secrets
# deploy/certs/fullchain.pem 和 privkey.pem 来自自己的证书管理流程
# deploy/secrets/llm_api_key 中写入 Provider key；不加入 Git
make deploy-config
make deploy-up
make deploy-doctor
make deploy-smoke
```

生成至少 20 字符的 URL-safe PostgreSQL 密码。默认仅监听
`127.0.0.1:8088/8443`。需要外部访问时设置 `MEMBOT_BIND_IP`、HTTP/HTTPS
端口和 `MEMBOT_PUBLIC_HOST`；例如公网证书对应的域名与 `80/443`。只有
Nginx 有宿主机端口映射，数据库、Redis、API 和 Worker 没有。
`MEMBOT_NETWORK_SUBNET` 默认 172.29.55.0/24，应选择不与主机路由、VPN 或
其他 Docker 项目重叠的子网。这个显式子网也让 DNS 演练能暂占旧地址。
当前版本的
`MEMBOT_OWNER_ID` 是部署级归属约束，没有用户登录鉴权；公网主机需通过
已有访问网关或主机访问策略限定可访问的调用方。

Compose secret 是文件挂载。Worker UID/GID 为 `10001:10001`；宿主机 key
文件必须可由该 UID 读取，例如 chown 到 10001、chmod 0400。本地 fixture
key 是无效占位值。API 容器没有模型 key 挂载。TLS 私钥只挂到 Nginx。
证书续期后重新加载 Nginx：

```bash
docker compose --env-file deploy/images.env --env-file .env -f deploy/docker-compose.yml exec nginx nginx -t
docker compose --env-file deploy/images.env --env-file .env -f deploy/docker-compose.yml exec nginx nginx -s reload
```

没有提供实际公网主机、域名 DNS 和公信证书时，只能完成本地 TLS 验收；
这不等于公网 HTTPS 部署成功。

## 配置与资源边界

容器直接运行 wheel 中的 `membot-service api|worker|migrate`。migration
成功后 API/Worker 才启动，API/Worker 不隐式执行迁移。启动时检查迁移版本、
URL scheme、有限正数期限、prefetch >= concurrency、iterations/payload 上限
和 Provider 类型。错误配置会退出，不能静默 clamp 或降级到假模型。

| 设置 | 默认值 | 意义 |
| --- | --- | --- |
| `MEMBOT_MAX_UNFINISHED` | 1024 | DB 中 QUEUED + RUNNING，包括未发布 Outbox |
| `MEMBOT_WORKER_CONCURRENCY` | 4 | 主 Invocation 的进程内执行额度 |
| `MEMBOT_WORKER_PREFETCH` | 8 | Redis pending 恢复和本地任务数上限 |
| `MEMBOT_REDIS_STREAM_MAX_DEPTH` | 1024 | Redis 传输积压上限 |
| `MEMBOT_MAX_PAYLOAD_BYTES` | 1048576 | API body 上限；Nginx 固定 1MiB |
| `MEMBOT_MAX_ITERATIONS` | 40 | 允许的主循环次数，最高 100 |
| queue / execution / LLM / Tool timeout | 300 / 300 / 120 / 60 秒 | 等待与执行分别计时 |
| diagnostic payload / retention | 64KiB / 7 天 | 受控事件 payload 与事件保留期限 |

底层 entrypoint 的 timeout 环境变量接受 `none`，表示显式禁用该期限；默认
部署有全部期限。DB 操作超时 5 秒。Worker 的 consolidation、spawn、cron、
heartbeat 调度器关闭，长期记忆归档与 MCP server 配置不通过服务 env 开启。
Worker 健康心跳是独立数据库记录，和 Agent heartbeat 工具不同。内核支持
MCP 初始化关闭、Provider client 关闭和 shell 进程组取消；未来服务开启后台
能力时必须另行定义持久化和预算。

API 各限 1 CPU/768MiB/128 PID；Worker 2 CPU/1536MiB/256 PID；PostgreSQL
1 CPU/768MiB；Redis 0.5 CPU/256MiB（maxmemory 192MiB、noeviction）；Nginx
0.5 CPU/128MiB、固定两个 Nginx worker。API/Worker 非 root、只读根文件系统、
128MiB tmpfs、drop ALL capabilities、no-new-privileges。额度是部署保护值，
不是容量或性能结论。工具进程属于 Worker 容器，不能逃离容器的内存/PID 上限。

PostgreSQL、Redis AOF、Worker workspace 使用独立 named volumes。
`json-file` 日志每容器 `10m × 3`；migration 不自动重启，其余
`unless-stopped`。healthcheck 失败只改变 Docker healthy 状态，不会自动
重启容器，也不会自动更新 Nginx upstream。

## 健康、转发和客户端重试

- `/health/live` 只表明该 API 进程能响应。
- `/health/ready` 检查 PostgreSQL/schema、剩余受理容量和 drain 标记；
  Redis 或 Worker 暂停期间仍可通过 Outbox 持久受理，达到容量返回非就绪。
- `/health/doctor` 展示 DB 中积压、未发布 Outbox 和 Worker 心跳。
  `make deploy-doctor` 还独立检查 Redis ping/depth 和容器状态。
  心跳存活不探测模型，也不保证 Agent 此时能完成任务。

响应头 `X-Instance-ID` 和 JSON 日志携带 `api1/api2`，用于流量证据。
Nginx 使用 `least_conn`、upstream zone、Docker DNS `127.0.0.11` 与
`resolve valid=2s`，容器重建后的地址会更新。开源 Nginx 被动观察连接错误、
超时和 `502/503/504`，`max_fails=1/fail_timeout=3s`；没有实现主动 readiness
摘除。一次健康查询可能在故障实例返回 503 后被转发到另一实例。

连接超时 1 秒，API读写超时 15 秒，至多尝试两个 upstream，重试窗口 3 秒。
不配置 `non_idempotent`：已发送上游的 POST 不由 Nginx 重试。连接尚未发出
请求时可以切换 upstream。调用方若没收到受理响应，用相同 Idempotency-Key
和相同 payload 重试，数据库返回原 Invocation；不同 payload 返回 409。
运维脚本仅在提供稳定 key 时重试 POST。任务完成通过 GET 轮询，不延长 HTTP
到 LLM Final。Nginx 20 请求/秒、burst 40、429 拒绝；health 路径豁免。

## 停机与故障演练

API SIGTERM 先进入 draining，ready 503，拒绝新的变更请求，最多 10 秒
等待已受理短事务，随后关闭连接、pool；容器 stop_grace_period 为 40 秒。
Worker SIGTERM 停止消费/启动任务，保留未开始的 QUEUED，在持有执行所有权
和续租任务的条件下最多等 20 秒；超出期限取消执行、关闭工具/client、写入
`FAILED/WORKER_DRAIN_TIMEOUT`，清理阶段预算 10 秒；容器允许 45 秒。
任务、AgentLoop scheduler、MCP、Provider 和 shell 进程组都被收尾。
若进程被 SIGKILL 或依赖失败导致不能提交终态，下一 Worker 在取得独占锁后
将旧 RUNNING 写为 `FAILED/WORKER_LOST`，保留 Trace，不自动再执行写工具。

```bash
make deploy-api-drill ENV_FILE=.env.local
make deploy-worker-drill ENV_FILE=.env.local
make deploy-rate-check ENV_FILE=.env.local
make deploy-restart-check ENV_FILE=.env.local
```

演练要求显式 fake deployment，避免向真实模型/外部写工具自动重放。
API 演练 kill api1，记录每次状态、instance 和错误窗口，确认原任务继续、
重复提交保持原 ID；恢复时暂占旧 IP 强制新地址，确认 API1/API2 再次接到流量
且 Nginx 未重启。Worker 演练覆盖 Kill 恢复、正常 SIGTERM 完成、超时取消和
下一序号继续。重启检查使用 `down` 后 `up`，从持久卷验证状态完全一致和
下一轮读取已有历史。报告写到被 Git 忽略的 `deploy/reports/*.json`。

## 备份与恢复验证

```bash
make deploy-backup ENV_FILE=.env.local
make deploy-restore-check ENV_FILE=.env.local BACKUP=deploy/backups/membot-<timestamp>.dump
make deploy-down ENV_FILE=.env.local
```

备份使用 PostgreSQL 导出一致快照；manifest 和 custom-format pg_dump 来自
相同快照，即使 Worker 正在提交也一致。对 sessions/messages/invocations/
archives/events/outbox 保存计数和内容摘要，文件权限 0600。恢复检查校验 dump
SHA256，创建随机名称的隔离数据库，pg_restore 后比较表指纹并删除该测试库。
脚本不会覆盖 live 数据库，也不提供默认 `down -v`。

Redis 是传输；恢复丢失 Redis 后 Worker 按 PostgreSQL 未完成任务补发。
备份覆盖数据库历史、状态和诊断，不包含工具工作目录的文件。业务若使用
写文件工具，需在停止 Worker 后另备份 `worker_workspace` named volume；
工具外部系统也有独立的备份责任。恢复运行服务前，先验证数据库、工具文件
和代码/配置版本匹配，再执行迁移并启动；原 RUNNING 会按 WORKER_LOST 收尾。

## 发布验收

```bash
make wheel-check
LITELLM_LOCAL_MODEL_COST_MAP=True DATABASE_URL=... REDIS_URL=... make test
make deploy-config ENV_FILE=.env.local
make deploy-up ENV_FILE=.env.local
make deploy-smoke ENV_FILE=.env.local
make deploy-api-drill ENV_FILE=.env.local
make deploy-worker-drill ENV_FILE=.env.local
make deploy-restart-check ENV_FILE=.env.local
make deploy-backup ENV_FILE=.env.local
make deploy-restore-check ENV_FILE=.env.local BACKUP=...
```

外部发布还需在目标主机用真实 DNS、公信证书和真实 Provider 运行 smoke，
确认防火墙/访问范围和资源容量；此项必须有实际环境证据。
