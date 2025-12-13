from __future__ import annotations

import json
import re
from typing import Any

from .base import JsonDict, WorkflowNode, register_node
from .utils import json_resolve_path


@register_node
class TemplateStringNode(WorkflowNode):
    """
    模板字符串节点：使用 Mustache 风格占位符 {{path}} 渲染文本
    """

    type = "string-template"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "模板字符串",
            "description": "使用 {{path}} 占位符从输入 JSON 中取值并渲染为字符串",
            "category": "逻辑",
            "icon": "json",
            "inputs": [
                {
                    "name": "context",
                    "label": "上下文",
                    "type": ["object", "array", "string"],
                    "required": False,
                    "desc": "模板变量来源：对象或 JSON 字符串；若为非对象，将作为变量 value 提供",
                }
            ],
            "outputs": [{"name": "text", "label": "结果字符串", "type": "string", "required": True}],
            "parameters": [
                {
                    "name": "template",
                    "label": "模板",
                    "type": "string",
                    "widget": "textarea",
                    "extra": {"placeholder": "例如：你好，{{user.name}}，今天是 {{date}}。"},
                    "required": True,
                    "desc": "使用 {{path}} 作为占位符，path 语法同 JSON 提取节点",
                }
            ],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict) -> JsonDict:
        template = params.get("template") or ""
        if not isinstance(template, str):
            template = str(template)

        raw_context = inputs.get("context")

        if isinstance(raw_context, (dict, list)):
            ctx: Any = raw_context
        elif isinstance(raw_context, str):
            try:
                parsed = json.loads(raw_context)
            except json.JSONDecodeError:
                parsed = raw_context
            if isinstance(parsed, (dict, list)):
                ctx = parsed
            else:
                ctx = {"value": parsed}
        else:
            ctx = {"value": raw_context}

        text = _render_template_string(template, ctx)
        return {"text": text}


def _render_template_string(template: str, context: Any) -> str:
    if not template:
        return ""

    pattern = re.compile(r"{{\s*([a-zA-Z_][\w\.\[\]0-9]*|)\s*}}")

    def repl(match: "re.Match[str]") -> str:
        path = match.group(1).strip()
        if path == "":
            value = context
        else:
            value = json_resolve_path(context, path, default="")
        if value is None:
            return ""
        return str(value)

    return pattern.sub(repl, template)
