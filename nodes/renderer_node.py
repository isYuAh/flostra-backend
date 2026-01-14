from __future__ import annotations

from .base import JsonDict, WorkflowNode, register_node


@register_node
class RendererNode(WorkflowNode):
    """展示节点：接收文本，在前端用 GenericNode 渲染"""

    type = "renderer"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "文本渲染",
            "description": "接收上游文本，在前端以自定义样式展示",
            "category": "IO",
            "icon": "renderer",
            "inputs": [
                {
                    "name": "text",
                    "label": "文本输入",
                    "type": ["string", "object", "array"],
                    "required": True,
                    "desc": "要展示的文本，可为字符串/对象/数组，由前端决定展示方式",
                }
            ],
            "outputs": [],
            "parameters": [
                {
                    "name": "title",
                    "label": "显示标题",
                    "type": "string",
                    "widget": "input",
                    "extra": {"placeholder": "例如：LLM 输出预览"},
                    "default": "渲染结果",
                    "required": False,
                    "uiPlacement": "node-body",
                }
            ],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict, context: JsonDict = None) -> JsonDict:
        text = inputs.get("text", "")
        return {"text": text, "title": params.get("title", "渲染结果")}
