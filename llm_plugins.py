from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Type


@dataclass
class LLMContext:
    """
    LLM 节点运行时上下文。
    插件通过修改此对象来影响最终的 LLM 调用和结果。
    """

    # 基础元数据
    session_id: Optional[str] = None
    user_query: str = ""

    # 输入参数（可被插件修改）
    system_prompt: str = ""
    messages: List[Dict[str, Any]] = field(default_factory=list)

    # 工具相关
    tools: List[Dict[str, Any]] = field(default_factory=list)  # OpenAI tool schema 列表
    tool_functions: Dict[str, Callable[..., Any]] = field(default_factory=dict)  # name -> callable

    # 执行结果
    ai_response_text: str = ""
    ai_raw_response: Any = None

    # 额外透传/调试数据
    extra_data: Dict[str, Any] = field(default_factory=dict)


class WorkflowPlugin(ABC):
    """所有插件的基类"""

    type: str  # 唯一标识

    def __init__(self, config: Dict[str, Any]):
        self.config = config or {}

    @classmethod
    @abstractmethod
    def get_schema(cls) -> Dict[str, Any]:
        """返回插件的配置 schema（前端用）"""

    async def on_pre_process(self, context: LLMContext) -> None:
        """LLM 调用前的钩子"""
        return None

    async def on_post_process(self, context: LLMContext) -> None:
        """LLM 调用后的钩子"""
        return None


_PLUGIN_REGISTRY: Dict[str, Type[WorkflowPlugin]] = {}


def register_plugin(cls: Type[WorkflowPlugin]) -> Type[WorkflowPlugin]:
    p_type = getattr(cls, "type", None)
    if not p_type:
        raise ValueError("Plugin class must define a non-empty 'type'")
    if p_type in _PLUGIN_REGISTRY:
        raise ValueError(f"Duplicated plugin type: {p_type}")
    _PLUGIN_REGISTRY[p_type] = cls
    return cls


def get_plugin_class(plugin_type: str) -> Optional[Type[WorkflowPlugin]]:
    return _PLUGIN_REGISTRY.get(plugin_type)


def list_plugins_schema() -> List[Dict[str, Any]]:
    """给前端：返回所有插件的 schema 列表"""
    return [cls.get_schema() for cls in _PLUGIN_REGISTRY.values()]


class PluginExecutor:
    """串行执行插件生命周期"""

    def __init__(self, plugins_conf: List[Dict[str, Any]], context: LLMContext):
        self.context = context
        self.plugins: List[WorkflowPlugin] = []
        for conf in plugins_conf or []:
            p_type = conf.get("type")
            p_cls = get_plugin_class(p_type)
            if p_cls:
                self.plugins.append(p_cls(conf.get("config", {})))

    async def run_pre_process(self) -> None:
        for plugin in self.plugins:
            await plugin.on_pre_process(self.context)

    async def run_post_process(self) -> None:
        for plugin in self.plugins:
            await plugin.on_post_process(self.context)


# ---------- 示例插件 ----------


@register_plugin
class SimpleSystemAppendPlugin(WorkflowPlugin):
    """
    示例：将配置的文本附加到 system_prompt（追加模式）
    """

    type = "sys-append"

    @classmethod
    def get_schema(cls) -> Dict[str, Any]:
        return {
            "type": cls.type,
            "label": "System Prompt 追加",
            "description": "在现有 system prompt 后追加一段文本",
            "kind": "plugin",  # 前端可据此区分普通插件/工具类插件
            "parameters": [
                {
                    "name": "text",
                    "label": "追加内容",
                    "dataType": "string",
                    "widgetType": "textarea",
                    "config": {"type": "textarea", "placeholder": "例如：你是一个友好的助手。"},
                    "required": True,
                }
            ],
        }

    async def on_pre_process(self, context: LLMContext) -> None:
        text = self.config.get("text") or ""
        if text:
            if context.system_prompt:
                context.system_prompt += "\n" + text
            else:
                context.system_prompt = text


@register_plugin
class SystemPromptPlugin(WorkflowPlugin):
    """
    System Prompt 处理插件：
    - mode: append / prepend / replace
    - value: 文本内容
    """

    type = "sys-prompt"

    @classmethod
    def get_schema(cls) -> Dict[str, Any]:
        return {
            "type": cls.type,
            "label": "System Prompt 处理",
            "description": "按模式追加/前置/替换 system prompt",
            "kind": "plugin",
            "parameters": [
                {
                    "name": "mode",
                    "label": "模式",
                    "dataType": "string",
                    "widgetType": "select",
                    "config": {
                        "type": "select",
                        "options": [
                            {"label": "追加", "value": "append"},
                            {"label": "前置", "value": "prepend"},
                            {"label": "替换", "value": "replace"},
                        ],
                    },
                    "default": "append",
                    "required": True,
                },
                {
                    "name": "value",
                    "label": "内容",
                    "dataType": "string",
                    "widgetType": "textarea",
                    "config": {
                        "type": "textarea",
                        "placeholder": "例如：你只能输出好或坏",
                    },
                    "required": True,
                },
            ],
        }

    async def on_pre_process(self, context: LLMContext) -> None:
        mode = (self.config.get("mode") or "append").lower()
        value = self.config.get("value") or ""
        if not value:
            return

        current = context.system_prompt or ""

        if mode == "replace":
            context.system_prompt = value
        elif mode == "prepend":
            context.system_prompt = value + ("\n" + current if current else "")
        else:  # append or fallback
            context.system_prompt = (current + "\n" if current else "") + value


@register_plugin
class DummyToolPlugin(WorkflowPlugin):
    """
    示例：注册一个简单的加法工具，演示 tool_call 流程
    """

    type = "dummy-tool-add"

    @classmethod
    def get_schema(cls) -> Dict[str, Any]:
        return {
            "type": cls.type,
            "label": "示例工具：加法",
            "description": "向模型暴露一个加法工具 (add)",
            "kind": "tool",  # 标记这是一个注册工具的插件
            "parameters": [],
        }

    async def on_pre_process(self, context: LLMContext) -> None:
        tool_schema = {
            "type": "function",
            "function": {
                "name": "add",
                "description": "计算两数之和",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "a": {"type": "number"},
                        "b": {"type": "number"},
                    },
                    "required": ["a", "b"],
                },
            },
        }
        context.tools.append(tool_schema)

        def add(a: float, b: float) -> float:
            return a + b

        context.tool_functions["add"] = add
