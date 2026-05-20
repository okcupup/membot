"""Utility functions for membot.

中文说明：
本模块包含 membot 使用的若干小工具函数，主要用于路径/工作区管理、
文件名安全化、时间戳生成以及将内置模板同步到用户工作区。
这些函数设计为小而可复用，且对不存在的目录具有容错能力（会自动创建）。
"""

import re
from pathlib import Path
from datetime import datetime


def ensure_dir(path: Path) -> Path:
    """Ensure directory exists, return it.

    中文说明：
    确保给定路径存在（作为目录）。如果目录不存在，函数会递归创建所有父目录。
    该函数是幂等的（多次调用不会抛错），适合在程序启动或初始化时保证目录结构存在。

    Args:
        path: 要确保存在的目录路径（Path 对象）。

    Returns:
        传入的 Path 对象（便于链式调用）。
    """
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_data_path() -> Path:
    """~/.membot data directory.

    中文说明：
    返回 membot 的用户数据目录（默认位于用户主目录下的 `.membot`），并确保该目录存在。
    """
    return ensure_dir(Path.home() / ".membot")


def get_workspace_path(workspace: str | None = None) -> Path:
    """Resolve and ensure workspace path. Defaults to ~/.membot/workspace.

    中文说明：
    如果传入 `workspace` 字符串，则展开（支持 `~`），并返回对应的 Path；否则使用
    默认的 `~/.membot/workspace`。函数会确保目标目录存在。
    """
    path = Path(workspace).expanduser() if workspace else Path.home() / ".membot" / "workspace"
    return ensure_dir(path)


def timestamp() -> str:
    """Current ISO timestamp.

    中文说明：返回当前时间的 ISO 格式字符串（例如用于日志或历史记录条目中）。
    """
    return datetime.now().isoformat()


_UNSAFE_CHARS = re.compile(r'[<>:"/\\|?*]')

def safe_filename(name: str) -> str:
    """Replace unsafe path characters with underscores.

    中文说明：
    将文件名中在大多数系统或 shell 中不安全的字符（如 `<>:"/\\|?*`）替换为下划线，
    并去除首尾空白，从而生成跨平台更安全的文件名。

    该函数不会进一步规范化字符编码或长度，调用方若有更严格需求应自行处理。
    """
    return _UNSAFE_CHARS.sub("_", name).strip()


def sync_workspace_templates(workspace: Path, silent: bool = False) -> list[str]:
    """Sync bundled templates to workspace. Only creates missing files.

    中文说明：
    将包内的 `templates` 目录下的 Markdown 模板文件复制到目标 `workspace` 下。
    该函数只会创建那些当前不存在的文件，默认不会覆盖已有内容，这样可以让用户
    自行定制模板而不会被覆盖。

    返回值是一个相对路径列表，表示已经创建的文件（相对于 workspace）。

    重要细节：
    - 使用 `importlib.resources.files('membot')` 读取包内资源；在某些开发环境
      中（未安装为 package）此方法可能失败，函数会在失败时返回空列表以便优雅降级。
    - 会确保 `workspace/memory/HISTORY.md`（空文件）和 `workspace/memory/MEMORY.md`（模板）存在，
      并确保 `workspace/skills` 目录存在。
    - 当 `silent=False` 时，使用 `rich` 打印创建的文件名；注意这只是为了更漂亮的
      输出，`rich` 不是必需依赖，如果环境中没有 `rich` 调用方应当容忍导入错误。
    """
    from importlib.resources import files as pkg_files
    try:
        tpl = pkg_files("membot") / "templates"
    except Exception:
        return []
    if not tpl.is_dir():
        return []

    added: list[str] = []

    def _write(src, dest: Path):
        if dest.exists():
            return
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(src.read_text(encoding="utf-8") if src else "", encoding="utf-8")
        added.append(str(dest.relative_to(workspace)))

    for item in tpl.iterdir():
        if item.name.endswith(".md"):
            _write(item, workspace / item.name)
    _write(tpl / "memory" / "MEMORY.md", workspace / "memory" / "MEMORY.md")
    _write(None, workspace / "memory" / "HISTORY.md")
    (workspace / "skills").mkdir(exist_ok=True)

    if added and not silent:
        from rich.console import Console
        for name in added:
            Console().print(f"  [dim]Created {name}[/dim]")
    return added
