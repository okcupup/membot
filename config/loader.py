"""Configuration loading utilities.

中文说明：
本模块负责加载与保存 membot 的配置（JSON 文件），并在发现旧版配置结构时
执行必要的迁移（backwards-compatible migration）。函数保持行为向后兼容，
在解析失败时会回退到默认配置以保证程序可用性。
"""

import json
from pathlib import Path

from membot.config.schema import Config


def get_config_path() -> Path:
    """Get the default configuration file path.

    中文说明：返回默认的配置文件路径 `~/.membot/config.json`。调用方可以用
    这个路径作为默认值，如果用户没有显式指定配置文件路径。
    """
    return Path.home() / ".membot" / "config.json"


def get_data_dir() -> Path:
    """Get the membot data directory.

    中文说明：代理到 `membot.utils.helpers.get_data_path()`，该函数会确保
    `~/.membot` 目录存在并返回对应的 Path 对象。
    """
    from membot.utils.helpers import get_data_path
    return get_data_path()


def load_config(config_path: Path | None = None) -> Config:
    """
    Load configuration from file or create default.

    中文说明：
    - 如果 `config_path` 为 None，使用 `get_config_path()` 的默认路径。
    - 尝试以 UTF-8 打开并解析 JSON。解析后会调用 `_migrate_config`
      对象完成向后兼容的字段迁移，然后用 pydantic（Config）校验并
      生成配置对象返回。
    - 如果 JSON 无法解析或校验异常（`JSONDecodeError` 或 `ValueError`），
      函数会打印警告并返回一个默认配置 `Config()`，以保证程序可以继续运行。

    Args:
        config_path: Optional path to config file. Uses default if not provided.

    Returns:
        Loaded `Config` 对象（pydantic model）。
    """
    path = config_path or get_config_path()

    if path.exists():
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            # 在把原始字典转换为 Config 之前，先做向后兼容的迁移
            data = _migrate_config(data)
            return Config.model_validate(data)
        except (json.JSONDecodeError, ValueError) as e:
            # 解析或校验失败时，打印警告并回退到默认配置。
            print(f"Warning: Failed to load config from {path}: {e}")
            print("Using default configuration.")

    # 配置文件不存在或解析失败 -> 返回默认配置
    return Config()


def save_config(config: Config, config_path: Path | None = None) -> None:
    """
    Save configuration to file.

    中文说明：
    - 将 pydantic 的 `Config` 对象序列化为字典（使用别名模式），然后以 JSON
      格式写入磁盘（UTF-8 编码）。
    - 如果目标目录不存在会先创建目录（`mkdir(parents=True)`）。

    Args:
        config: Configuration to save (pydantic model instance).
        config_path: Optional path to save to. Uses default if not provided.
    """
    path = config_path or get_config_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    data = config.model_dump(by_alias=True)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def _migrate_config(data: dict) -> dict:
    """Migrate old config formats to current.

    中文说明：
    该函数负责把历史版本的配置结构迁移到当前版本所期望的结构。
    当前实现包含一条兼容性迁移：将 `tools.exec.restrictToWorkspace` 字段
    提升为顶层 `tools.restrictToWorkspace` 字段；这通常用于把 exec 工具
    的限制迁移到全局工具配置位置。

    具体逻辑：
    - 读取 `data['tools']`（若不存在则使用空 dict）。
    - 读取 `tools['exec']`（若不存在则使用空 dict）。
    - 如果 `exec` 中包含 `restrictToWorkspace`，而 `tools` 顶层没有该字段，
      则把它移动到 `tools['restrictToWorkspace']` 并从 `exec` 中删除该键。

    业务原因：
    - 早期版本可能只在 exec 子配置中记录该开关，后续版本希望把工具访问
      限制提升为通用配置，迁移逻辑保证旧配置仍然有效。

    Args:
        data: 原始配置字典（从 JSON 解析得到）。

    Returns:
        已迁移/规范化的配置字典（不做深拷贝，直接在原 dict 上修改）。
    """
    # Move tools.exec.restrictToWorkspace → tools.restrictToWorkspace
    tools = data.get("tools", {})
    exec_cfg = tools.get("exec", {})
    # 如果 exec_cfg 中包含 restrictToWorkspace，而 tools 顶层没有这个字段，
    # 则把值提升到 tools 顶层（并从 exec_cfg 中移除），以兼容新结构。
    if "restrictToWorkspace" in exec_cfg and "restrictToWorkspace" not in tools:
        tools["restrictToWorkspace"] = exec_cfg.pop("restrictToWorkspace")
    return data
