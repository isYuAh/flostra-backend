from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, List
from uuid import uuid4


@dataclass
class Secret:
    """简单的内存 Secret 模型（当前版本不持久化、不加密）"""

    id: str
    name: str
    value: str


class SecretNotFoundError(KeyError):
    """找不到指定 Secret 时抛出的异常"""


_STORE: Dict[str, Secret] = {}


def create_secret(name: str, value: str) -> Secret:
    """
    创建一个新的 Secret，仅存内存。
    注意：当前实现不会去重 name，同名可以存在多个。
    """
    secret_id = f"sec_{uuid4().hex}"
    secret = Secret(id=secret_id, name=name, value=value)
    _STORE[secret_id] = secret
    return secret


def list_secrets() -> List[Secret]:
    """列出当前进程内的所有 Secret（不包含 value）"""
    return list(_STORE.values())


def delete_secret(secret_id: str) -> None:
    """删除指定 Secret，不存在则抛 SecretNotFoundError"""
    try:
        del _STORE[secret_id]
    except KeyError as exc:
        raise SecretNotFoundError(secret_id) from exc


def resolve_secret_value(secret_id: str) -> str:
    """运行时根据 ID 取出 Secret 明文值"""
    try:
        return _STORE[secret_id].value
    except KeyError as exc:
        raise SecretNotFoundError(secret_id) from exc


def _bootstrap_defaults() -> None:
    """
    预置一些测试用 Secret，方便前端开发：
    - 始终内置两条虚拟 Secret（值是占位符，不能直接用于生产）：
      * DUMMY_API_KEY
      * DUMMY_DB_PASSWORD
    - 如果环境变量中存在 LLM_API_KEY / ASK_API_KEY，则额外注入对应 Secret。
    """
    # 固定的测试用 Secret（占位值）
    create_secret("DUMMY_API_KEY", "dummy-api-key")
    create_secret("DUMMY_DB_PASSWORD", "dummy-db-password")
    create_secret("DUMMY_LONG_LONG_LONG_LONG_LONG", "LLLLLL")

    # 可选：从环境变量注入真实 Key（如果你愿意配置的话）
    if os.getenv("LLM_API_KEY"):
        create_secret("LLM_API_KEY", os.environ["LLM_API_KEY"])
    if os.getenv("ASK_API_KEY"):
        create_secret("ASK_API_KEY", os.environ["ASK_API_KEY"])


_bootstrap_defaults()
