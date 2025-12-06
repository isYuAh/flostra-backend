from __future__ import annotations

import asyncio
import json
import os
import re
from abc import ABC, abstractmethod
import smtplib
import ssl
from typing import Any, Dict, List, Optional, Type

from email.message import EmailMessage
from email.utils import make_msgid

import httpx
from openai import OpenAI

from llm_plugins import (
    LLMContext,
    PluginExecutor,
    list_plugins_schema,
)

JsonDict = Dict[str, Any]


class WorkflowNode(ABC):
    """工作流节点基类：负责 schema 定义 + 执行逻辑"""

    # 每个子类必须定义唯一的 type
    type: str

    @classmethod
    @abstractmethod
    def get_schema(cls) -> JsonDict:
        """返回节点的 JSON 蓝图定义，前端根据这个渲染 UI"""

    @classmethod
    @abstractmethod
    async def run(cls, inputs: JsonDict, params: JsonDict) -> JsonDict:
        """执行节点逻辑，所有 I/O 都在后端完成"""


_NODE_REGISTRY: Dict[str, Type[WorkflowNode]] = {}


def register_node(node_cls: Type[WorkflowNode]) -> Type[WorkflowNode]:
    """将节点类注册到内存表中，提供给路由查询使用"""
    node_type = getattr(node_cls, "type", None)
    if not node_type:
        raise ValueError("Node class must define non-empty `type` attribute")
    if node_type in _NODE_REGISTRY:
        raise ValueError(f"Duplicate node type registered: {node_type}")
    _NODE_REGISTRY[node_type] = node_cls
    return node_cls


def get_all_node_schemas() -> List[JsonDict]:
    """给前端：返回所有节点的 schema 列表"""
    return [cls.get_schema() for cls in _NODE_REGISTRY.values()]


def get_node_cls(node_type: str) -> Type[WorkflowNode]:
    """根据 type 查找节点类"""
    try:
        return _NODE_REGISTRY[node_type]
    except KeyError as exc:
        raise KeyError(f"Unknown node type: {node_type!r}") from exc


# ---------- Workflow 类节点（Start / End） ----------


@register_node
class ManualStartNode(WorkflowNode):
    """调试用 Start 节点：通过端口常量或 overrides 注入 payload，向下游透传"""

    type = "trigger_manual"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "调试 Start",
            "description": "调试运行入口：payload 通过端口常量或 overrides 注入，作为子图入口数据",
            "category": "Workflow",
            "icon": "material:play_arrow",
            "inputs": [
                {
                    "id": "payload",
                    "label": "调试输入",
                    "type": "object",
                    "required": False,
                    "desc": "运行时通过端口常量或 overrides 注入的数据",
                }
            ],
            "outputs": [
                {
                    "id": "payload",
                    "label": "输出 Payload",
                    "type": "object",
                    "required": False,
                }
            ],
            "parameters": [],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict) -> JsonDict:
        # 调试 Start 的语义：
        # - payload 完全由端口常量 / overrides / 上游决定，节点本身只做透传
        return {"payload": inputs.get("payload")}


@register_node
class HttpStartNode(WorkflowNode):
    """HTTP Start 节点：用于发布成 HTTP 端点的入口"""

    type = "trigger_http"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "HTTP Start",
            "description": "HTTP 请求入口节点，用于从外部 HTTP 请求触发工作流",
            "category": "Workflow",
            "icon": "material:http",
            "inputs": [],
            "outputs": [
                {
                    "id": "method",
                    "label": "HTTP 方法",
                    "type": "string",
                    "required": False,
                },
                {
                    "id": "data",
                    "label": "合并数据 (body 覆盖 query)",
                    "type": "object",
                    "required": False,
                },
                {
                    "id": "request",
                    "label": "原始请求对象",
                    "type": "object",
                    "required": False,
                },
            ],
            "parameters": [
                {
                    "name": "method",
                    "label": "HTTP 方法",
                    "dataType": "string",
                    "widgetType": "select",
                    "config": {
                        "type": "select",
                        "options": [
                            {"label": "GET", "value": "GET"},
                            {"label": "POST", "value": "POST"},
                            {"label": "PUT", "value": "PUT"},
                            {"label": "DELETE", "value": "DELETE"},
                        ],
                    },
                    "default": "POST",
                    "required": True,
                },
                {
                    "name": "path",
                    "label": "路径",
                    "dataType": "string",
                    "widgetType": "input",
                    "config": {
                        "type": "input",
                        "placeholder": "/chat",
                    },
                    "default": "/chat",
                    "required": True,
                    "desc": "用于发布为 HTTP Endpoint 的路径（同一工作流内应唯一）",
                },
            ],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict) -> JsonDict:
        # 从运行上下文获取请求（若有）
        context = inputs.get("__context") or {}
        ctx_request = context.get("request") if isinstance(context, dict) else None

        def _to_dict(val: Any) -> Dict[str, Any]:
            if val is None:
                return {}
            if isinstance(val, dict):
                return val
            if isinstance(val, str):
                try:
                    parsed = json.loads(val)
                    return parsed if isinstance(parsed, dict) else {}
                except Exception:
                    return {}
            return {}

        query = _to_dict(inputs.get("query")) or _to_dict(ctx_request.get("query") if ctx_request else None)
        body_input = inputs.get("body")
        payload = inputs.get("payload")
        body_source = body_input if body_input is not None else payload
        body = _to_dict(body_source) or _to_dict(ctx_request.get("body") if ctx_request else None)

        # 合并数据：body 优先覆盖 query
        data: Dict[str, Any] = {}
        data.update(query)
        data.update(body)

        method = inputs.get("method") or (ctx_request.get("method") if ctx_request else params.get("method")) or ""
        path = inputs.get("path") or (ctx_request.get("path") if ctx_request else params.get("path")) or ""

        # 构造返回的 request 对象，降级为最小字段
        request_obj = ctx_request if isinstance(ctx_request, dict) else {}
        if not request_obj:
            request_obj = {
                "method": method,
                "path": path,
                "headers": _to_dict(inputs.get("headers")),
                "query": query,
                "body": body,
                "rawBody": inputs.get("rawBody"),
            }

        return {
            "method": method,
            "data": data,
            "request": request_obj,
        }


@register_node
class EndNode(WorkflowNode):
    """End 节点：标记工作流的结果输出位置（支持动态端口）"""

    type = "end"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "End",
            "description": "工作流结束节点，作为默认的结果输出节点；支持配置多个结果端口",
            "category": "Workflow",
            "icon": "material:stop_circle",
            # 输入/输出端口使用 dynamicInputs/dynamicOutputs，由前端根据参数 ports 生成
            "inputs": [],
            "outputs": [],
            "parameters": [
                {
                    "name": "ports",
                    "label": "结果端口配置",
                    "dataType": "array",
                    "widgetType": "ports-list",
                    "config": {
                        "type": "dynamic-list",
                        "role": "end-ports",
                        "template": {
                            "id": "",
                            "label": "",
                            "required": False,
                        },
                    },
                    "effects": ["cal_input_ports"],
                    "required": False,
                    # 默认提供一个名为 value 的端口，方便快速上手
                    "default": [
                        {
                            "id": "value",
                            "label": "结果值",
                            "required": False,
                        }
                    ],
                    "desc": (
                        "配置 End 节点的结果端口；id 作为端口 ID，label 为展示名称。"
                        "所有端口类型均为 any，执行时会将对应输入端口的值原样透传到结果中。"
                    ),
                }
            ],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict) -> JsonDict:
        """
        End 节点运行逻辑：
        - 读取 params['ports'] 中配置的所有端口 ID
        - 对于每个端口 ID p，如果 inputs 中存在同名 key，则透传到 outputs[p]
        - 如果 params['ports'] 不存在，则回退到旧行为：仅透传 'value' 端口
        """
        ports = params.get("ports")
        outputs: Dict[str, Any] = {}

        # 回退兼容：未配置 ports 时，视为只有一个 'value' 端口
        if not ports:
            if "value" in inputs:
                outputs["value"] = inputs.get("value")
            return outputs

        for port_cfg in ports:
            if not isinstance(port_cfg, dict):
                continue
            pid = port_cfg.get("id")
            if not pid:
                continue
            if pid in inputs:
                outputs[pid] = inputs.get(pid)

        return outputs


# ---------- JSON 提取节点 ----------


@register_node
class JsonExtractNode(WorkflowNode):
    """
    JSON 提取节点：
    - 固定一个输入端口 input（object/any）
    - 输出端口通过参数 ports（ports_list）配置，前端根据该列表生成 dynamicOutputs
    - 运行时根据每个端口配置中的 path，从 input JSON 中提取对应的值
    """

    type = "json-extract"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "JSON 提取",
            "description": "根据配置的路径从输入 JSON 中提取多个字段作为输出端口",
            "category": "Tools",
            "icon": "material:data_object",
            "inputs": [
                {
                    "id": "input",
                    "label": "JSON 输入",
                    "type": ["object", "string"],
                    "required": True,
                    "desc": "上游节点输出的 JSON 对象，或 JSON 字符串",
                }
            ],
            # 输出端口使用 dynamicOutputs，由前端根据参数 ports（ports_list）生成
            "outputs": [],
            "parameters": [
                {
                    "name": "ports",
                    "label": "输出端口配置",
                    "dataType": "array",
                    "widgetType": "ports-list",
                    "config": {
                        "type": "dynamic-list",
                        "role": "json-extract-ports",
                        "template": {
                            "id": "",
                            "label": "",
                            "path": "",
                            "default": None,
                            "required": False,
                        },
                    },
                    "effects": ["cal_output_ports"],
                    "required": False,
                    "desc": (
                        "为每个输出端口配置一个 JSON 路径："
                        "id = 输出端口 ID；label = 显示名称；path = JSON 路径，如 user.name 或 items[0].price"
                    ),
                }
            ],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict) -> JsonDict:
        """
        运行时逻辑：
        - 从 inputs['input'] 读取 JSON 对象
        - 遍历 params['ports'] （若不存在则视为空数组）
        - 对每个端口配置：
          * 使用 path（如 'a.b[0].c'）从 JSON 中提取值
          * 将提取到的值放入 outputs[port.id]
        """
        raw = inputs.get("input")

        # 如果传的是字符串，尝试解析为 JSON；失败则原样使用
        data: Any
        if isinstance(raw, str):
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                data = raw
        else:
            data = raw

        ports = params.get("ports") or []
        outputs: Dict[str, Any] = {}

        for port_cfg in ports:
            if not isinstance(port_cfg, dict):
                continue
            out_id = port_cfg.get("id")
            path = port_cfg.get("path")
            default = port_cfg.get("default")
            required = bool(port_cfg.get("required", False))

            if not out_id or not path:
                # 配置不完整，跳过
                continue

            value = _json_resolve_path(data, path, default=default)
            if value is None and required:
                raise ValueError(f"JSON 提取节点端口 {out_id!r} 在路径 {path!r} 上未找到值")

            outputs[out_id] = value

        return outputs


def _json_resolve_path(data: Any, path: str, default: Any = None) -> Any:
    """
    简单的 JSON 路径解析器，支持：
    - 点号访问：a.b.c -> data['a']['b']['c']
    - 中括号索引：items[0].price -> data['items'][0]['price']

    不支持复杂的 JSONPath 语法，仅用于本项目的轻量级提取需求。
    """
    if path is None or path == "":
        return data

    current = data
    # 先按 '.' 拆分顶层路径片段
    segments = path.split(".")

    for seg in segments:
        if current is None:
            return default

        # 解析单个片段中的中括号（如 "items[0][1]"）
        # 核心思路：先取出 key，再依次应用所有索引
        key = ""
        indexes: List[int] = []
        buf = ""
        in_bracket = False
        for ch in seg:
            if ch == "[":
                if not in_bracket:
                    in_bracket = True
                    key = buf
                    buf = ""
                else:
                    # 不支持嵌套 '[['，直接失败
                    return default
            elif ch == "]":
                if not in_bracket:
                    return default
                in_bracket = False
                idx_str = buf.strip()
                buf = ""
                if not idx_str.isdigit():
                    return default
                indexes.append(int(idx_str))
            else:
                buf += ch

        if in_bracket:
            # 中括号未闭合
            return default

        # 如果没有中括号，则整个 seg 就是 key
        if key == "":
            key = buf

        # 先按 key 取值（如果 key 不是空）
        if key:
            if isinstance(current, dict):
                if key not in current:
                    return default
                current = current.get(key)
            else:
                # 当前不是 dict，无法按 key 访问
                return default

        # 再按顺序应用索引
        for idx in indexes:
            if isinstance(current, list):
                if idx < 0 or idx >= len(current):
                    return default
                current = current[idx]
            else:
                return default

    return current


# ---------- JSON 构造节点 ----------


@register_node
class JsonBuildNode(WorkflowNode):
    """
    JSON 构造节点：
    - 所有输入端口通过参数 fields 配置（dynamicInputs）
    - 运行时将各输入端口的值聚合为一个 JSON 对象输出
    """

    type = "json-build"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "JSON 构造",
            "description": "将多个输入端口的值聚合为一个 JSON 对象输出",
            "category": "Tools",
            "icon": "material:data_array",
            # 输入端口由前端根据 fields 参数生成 dynamicInputs
            "inputs": [],
            "outputs": [
                {
                    "id": "output",
                    "label": "JSON 输出",
                    "type": "object",
                    "required": False,
                    "desc": "由各输入端口聚合而成的 JSON 对象",
                }
            ],
            "parameters": [
                {
                    "name": "fields",
                    "label": "字段配置",
                    "dataType": "array",
                    "widgetType": "ports-list",
                    "config": {
                        "type": "dynamic-list",
                        "role": "json-build-inputs",
                        "template": {
                            "id": "",
                            "label": "",
                            "required": False,
                        },
                    },
                    "effects": ["cal_input_ports"],
                    "required": False,
                    "desc": (
                        "配置要聚合的输入端口：id 作为端口 ID，也是 JSON 对象中的 key；"
                        "label 为展示名称。"
                    ),
                }
            ],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict) -> JsonDict:
        """
        运行逻辑：
        - 遍历 params['fields'] 中配置的每个端口 ID
        - 将 inputs[端口ID] 收集到一个字典中作为 JSON 输出
        """
        fields = params.get("fields") or []
        obj: Dict[str, Any] = {}

        if not fields:
            # 未配置字段时，回退为直接聚合全部输入端口
            for key, value in inputs.items():
                obj[key] = value
            return {"output": obj}

        for field_cfg in fields:
            if not isinstance(field_cfg, dict):
                continue
            fid = field_cfg.get("id")
            required = bool(field_cfg.get("required", False))
            if not fid:
                continue

            if fid in inputs:
                obj[fid] = inputs.get(fid)
            elif required:
                raise ValueError(f"JSON 构造节点字段 {fid!r} 为必需，但未提供对应输入")

        return {"output": obj}


# ---------- 代码节点 ----------


@register_node
class CodeNode(WorkflowNode):
    """
    代码节点：
    - 通过 inputPorts / outputPorts 配置动态输入/输出端口
    - 用户在参数 code 中编写 Python 代码，并实现 run(inputs, params) 函数
    - 执行时调用 run(inputs, params) 获取结果，并按输出端口规则映射
    """

    type = "code"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "代码",
            "description": "执行自定义 Python 代码，对输入做任意处理并输出结果",
            "category": "Logic",
            "icon": "material:code",
            "inputs": [],
            "outputs": [],
            "parameters": [
                {
                    "name": "inputPorts",
                    "label": "输入端口",
                    "dataType": "array",
                    "widgetType": "ports-list",
                    "config": {
                        "type": "dynamic-list",
                        "role": "code-input-ports",
                        "template": {
                            "id": "",
                            "label": "",
                            "required": False,
                        },
                    },
                    "effects": ["cal_input_ports"],
                    "required": False,
                    "default": [
                        {
                            "id": "input",
                            "label": "输入",
                            "required": False,
                        }
                    ],
                    "desc": "配置代码节点的输入端口，id 将作为 inputs 字典中的 key。",
                },
                {
                    "name": "outputPorts",
                    "label": "输出端口",
                    "dataType": "array",
                    "widgetType": "ports-list",
                    "config": {
                        "type": "dynamic-list",
                        "role": "code-output-ports",
                        "template": {
                            "id": "",
                            "label": "",
                            "required": False,
                        },
                    },
                    "effects": ["cal_output_ports"],
                    "required": False,
                    "default": [
                        {
                            "id": "output",
                            "label": "输出",
                            "required": False,
                        }
                    ],
                    "desc": "配置代码节点的输出端口，id 将作为结果字典中的 key。",
                },
                {
                    "name": "lang",
                    "label": "语言",
                    "dataType": "string",
                    "widgetType": "select",
                    "config": {
                        "type": "select",
                        "options": [
                            {"label": "Python", "value": "python"},
                        ],
                    },
                    "default": "python",
                    "required": True,
                },
                {
                    "name": "code",
                    "label": "代码",
                    "dataType": "string",
                    "widgetType": "textarea",
                    "config": {
                        "type": "code-editor",
                        "language": "python",
                    },
                    "required": True,
                    "desc": (
                        "在此编写 Python 代码，并实现函数 run(inputs: dict, params: dict) -> Any：\n"
                        "- 单输出端口：可以返回任意类型，直接作为该端口的值；\n"
                        "- 多输出端口：必须返回 dict，以端口 id 为 key。"
                    ),
                },
            ],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict) -> JsonDict:
        lang = (params.get("lang") or "python").lower()
        if lang != "python":
            raise ValueError(f"暂不支持语言 {lang!r}，当前仅支持 'python'")

        code_str = params.get("code")
        if not isinstance(code_str, str) or not code_str.strip():
            raise ValueError("代码节点缺少必需的参数 code")

        output_ports = params.get("outputPorts") or []

        # 准备执行环境
        global_env: Dict[str, Any] = {"__name__": "__code_node__"}
        local_env: Dict[str, Any] = {}

        try:
            exec(code_str, global_env, local_env)
        except Exception as exc:  # noqa: BLE001
            raise ValueError(f"代码节点执行代码时出错：{exc}") from exc

        func = local_env.get("run") or global_env.get("run")
        if not callable(func):
            raise ValueError("代码节点必须在代码中定义函数 run(inputs, params)")

        loop = asyncio.get_event_loop()

        def _call_user() -> Any:
            return func(inputs, params)

        try:
            result = await loop.run_in_executor(None, _call_user)
        except Exception as exc:  # noqa: BLE001
            raise ValueError(f"代码节点运行 run(inputs, params) 时出错：{exc}") from exc

        # 将用户返回值映射到输出端口
        outputs: Dict[str, Any] = {}
        num_ports = len(output_ports)

        if num_ports == 0:
            # 无输出端口：忽略返回值，仅作为副作用节点
            return outputs

        if num_ports == 1:
            # 单端口：直接将返回值作为该端口的值
            port_id = output_ports[0].get("id") or "output"
            outputs[port_id] = result
            return outputs

        # 多端口：要求返回 dict，以端口 id 为 key
        if not isinstance(result, dict):
            raise ValueError("代码节点配置了多个输出端口，但 run() 返回的结果不是 dict")

        for port_cfg in output_ports:
            if not isinstance(port_cfg, dict):
                continue
            pid = port_cfg.get("id")
            if not pid:
                continue
            outputs[pid] = result.get(pid)

        return outputs


# ---------- HTTP 请求节点 ----------


@register_node
class HttpRequestNode(WorkflowNode):
    """
    HTTP 请求节点：
    - 使用 HTTP 调用外部 API
    - 支持在参数中配置 method / url / headers / query，body 可通过输入端口传入
    """

    type = "http-request"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "HTTP 请求",
            "description": "调用外部 HTTP 接口，返回状态码、响应头和响应体",
            "category": "Tools",
            "icon": "material:cloud",
            "inputs": [
                {
                    "id": "body",
                    "label": "请求 Body",
                    "type": "any",
                    "required": False,
                    "desc": "请求体内容，若为 object/array 将作为 JSON 发送",
                },
                {
                    "id": "url",
                    "label": "URL（覆盖参数）",
                    "type": "string",
                    "required": False,
                    "desc": "如果提供，将覆盖参数中的 URL",
                },
            ],
            "outputs": [
                {
                    "id": "status",
                    "label": "状态码",
                    "type": "number",
                    "required": True,
                },
                {
                    "id": "headers",
                    "label": "响应头",
                    "type": "object",
                    "required": False,
                },
                {
                    "id": "body",
                    "label": "响应体",
                    "type": "any",
                    "required": False,
                },
            ],
            "parameters": [
                {
                    "name": "method",
                    "label": "HTTP 方法",
                    "dataType": "string",
                    "widgetType": "select",
                    "config": {
                        "type": "select",
                        "options": [
                            {"label": "GET", "value": "GET"},
                            {"label": "POST", "value": "POST"},
                            {"label": "PUT", "value": "PUT"},
                            {"label": "DELETE", "value": "DELETE"},
                            {"label": "PATCH", "value": "PATCH"},
                        ],
                    },
                    "default": "GET",
                    "required": True,
                },
                {
                    "name": "url",
                    "label": "URL",
                    "dataType": "string",
                    "widgetType": "input",
                    "config": {
                        "type": "input",
                        "placeholder": "https://api.example.com/path",
                    },
                    "required": True,
                },
                {
                    "name": "headers",
                    "label": "Headers（JSON）",
                    "dataType": "object",
                    "widgetType": "json-editor",
                    "config": {
                        "type": "json-editor",
                    },
                    "required": False,
                    "desc": "请求头对象，例如 {\"Authorization\": \"Bearer ...\"}",
                },
                {
                    "name": "query",
                    "label": "Query（JSON）",
                    "dataType": "object",
                    "widgetType": "json-editor",
                    "config": {
                        "type": "json-editor",
                    },
                    "required": False,
                    "desc": "查询参数对象，将序列化到 URL 上",
                },
                {
                    "name": "timeout",
                    "label": "超时时间（秒）",
                    "dataType": "number",
                    "widgetType": "input",
                    "config": {
                        "type": "input",
                        "placeholder": "默认 10",
                    },
                    "required": False,
                },
            ],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict) -> JsonDict:
        method = (params.get("method") or "GET").upper()
        url = inputs.get("url") or params.get("url")
        if not url:
            raise ValueError("HTTP 请求节点缺少 URL（可在参数 url 中配置，或通过输入端口 url 覆盖）")

        headers = params.get("headers") or {}
        query = params.get("query") or None
        body = inputs.get("body")

        timeout_val = params.get("timeout")
        try:
            timeout = float(timeout_val) if timeout_val is not None else 10.0
        except (TypeError, ValueError):
            timeout = 10.0

        # 根据 body 类型决定使用 JSON 还是普通文本
        json_body: Any = None
        data_body: Any = None
        if isinstance(body, (dict, list)):
            json_body = body
        elif body is not None:
            data_body = str(body)

        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.request(
                    method=method,
                    url=url,
                    headers=headers,
                    params=query,
                    json=json_body,
                    data=data_body,
                )
        except httpx.RequestError as exc:
            raise ValueError(f"HTTP 请求失败：{exc}") from exc

        status = resp.status_code
        resp_headers = dict(resp.headers)

        try:
            resp_body: Any = resp.json()
        except ValueError:
            resp_body = resp.text

        return {
            "status": status,
            "headers": resp_headers,
            "body": resp_body,
        }


# ---------- 邮件发送节点 ----------


@register_node
class EmailSendNode(WorkflowNode):
    """
    邮件发送节点（仅 SMTP/SMTPS）：
    - 参数配置服务器、账户、TLS 方式
    - 输入端口提供收件人、标题、正文
    """

    type = "email-send"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "邮件发送",
            "description": "通过 SMTP/SMTPS 发送邮件，支持 STARTTLS",
            "category": "IO",
            "icon": "material:send",
            "inputs": [
                {
                    "id": "to",
                    "label": "收件人",
                    "type": ["string", "array"],
                    "required": True,
                    "desc": "收件人地址，逗号分隔字符串或字符串数组",
                },
                {
                    "id": "subject",
                    "label": "主题",
                    "type": "string",
                    "required": False,
                },
                {
                    "id": "body",
                    "label": "正文",
                    "type": ["string", "object"],
                    "required": False,
                    "desc": "邮件文本内容，非字符串会转为字符串",
                },
            ],
            "outputs": [
                {"id": "ok", "label": "是否成功", "type": "boolean", "required": True},
                {"id": "message_id", "label": "Message-ID", "type": "string", "required": False},
                {"id": "log", "label": "日志", "type": "string", "required": False},
            ],
            "parameters": [
                {
                    "name": "server",
                    "label": "SMTP 服务器",
                    "dataType": "string",
                    "widgetType": "input",
                    "config": {"type": "input", "placeholder": "smtp.example.com"},
                    "required": True,
                },
                {
                    "name": "port",
                    "label": "端口",
                    "dataType": "number",
                    "widgetType": "input",
                    "config": {"type": "input", "placeholder": "587 或 465"},
                    "required": False,
                    "default": 587,
                },
                {
                    "name": "username",
                    "label": "账号",
                    "dataType": "string",
                    "widgetType": "input",
                    "config": {"type": "input", "placeholder": "name@example.com"},
                    "required": False,
                },
                {
                    "name": "password",
                    "label": "密码/授权码",
                    "dataType": "string",
                    "widgetType": "secret",
                    "config": {"type": "input", "placeholder": "建议使用 SecretRef"},
                    "required": False,
                },
                {
                    "name": "from",
                    "label": "发件人",
                    "dataType": "string",
                    "widgetType": "input",
                    "config": {"type": "input", "placeholder": "缺省用账号"},
                    "required": False,
                },
                {
                    "name": "use_ssl",
                    "label": "使用 SMTPS (465)",
                    "dataType": "boolean",
                    "widgetType": "switch",
                    "config": {"type": "switch"},
                    "default": False,
                    "required": False,
                },
                {
                    "name": "use_starttls",
                    "label": "启用 STARTTLS",
                    "dataType": "boolean",
                    "widgetType": "switch",
                    "config": {"type": "switch"},
                    "default": True,
                    "required": False,
                },
                {
                    "name": "timeout",
                    "label": "超时（秒）",
                    "dataType": "number",
                    "widgetType": "input",
                    "config": {"type": "input", "placeholder": "默认 10"},
                    "required": False,
                },
            ],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict) -> JsonDict:
        server = params.get("server")
        if not server:
            raise ValueError("邮件发送节点缺少 server 参数")

        raw_to = inputs.get("to")
        recipients = cls._normalize_recipients(raw_to)
        if not recipients:
            raise ValueError("邮件发送节点缺少收件人 to")

        subject = inputs.get("subject") or ""
        body = inputs.get("body")
        body_text = body if isinstance(body, str) else json.dumps(body, ensure_ascii=False) if body is not None else ""

        port = int(params.get("port") or 587)
        use_ssl = bool(params.get("use_ssl", False))
        use_starttls = bool(params.get("use_starttls", True))
        timeout_val = params.get("timeout")
        try:
            timeout = float(timeout_val) if timeout_val is not None else 10.0
        except (TypeError, ValueError):
            timeout = 10.0

        username = params.get("username") or None
        password = params.get("password") or None
        from_addr = params.get("from") or username or ""

        if use_ssl and use_starttls:
            # SMTPS 不需要再 STARTTLS
            use_starttls = False

        msg = EmailMessage()
        msg["Subject"] = str(subject)
        msg["From"] = from_addr
        msg["To"] = ", ".join(recipients)
        msg["Message-ID"] = make_msgid()
        msg.set_content(str(body_text))

        loop = asyncio.get_event_loop()

        def _send_email() -> str:
            def _safe_close(smtp: smtplib.SMTP) -> None:
                """确保退出时调用 quit；失败则兜底 close，避免半开连接。"""
                try:
                    smtp.quit()
                except Exception:  # noqa: BLE001
                    try:
                        smtp.close()
                    except Exception:
                        pass

            try:
                if use_ssl:
                    context = ssl.create_default_context()
                    smtp = smtplib.SMTP_SSL(server, port, timeout=timeout, context=context)
                    try:
                        cls._maybe_login(smtp, username, password)
                        smtp.send_message(msg)
                    finally:
                        _safe_close(smtp)
                else:
                    smtp = smtplib.SMTP(server, port, timeout=timeout)
                    try:
                        smtp.ehlo()
                        if use_starttls:
                            context = ssl.create_default_context()
                            smtp.starttls(context=context)
                            smtp.ehlo()
                        cls._maybe_login(smtp, username, password)
                        smtp.send_message(msg)
                    finally:
                        _safe_close(smtp)
                return msg["Message-ID"] or ""
            except Exception as exc:  # noqa: BLE001
                # 避免把密码暴露在错误信息中
                raise ValueError(f"邮件发送失败：{exc}") from None

        message_id = await loop.run_in_executor(None, _send_email)

        return {
            "ok": True,
            "message_id": message_id,
            "log": f"sent to {len(recipients)} recipient(s)",
        }

    @staticmethod
    def _normalize_recipients(raw: Any) -> List[str]:
        if raw is None:
            return []
        if isinstance(raw, str):
            parts = [p.strip() for p in raw.split(",") if p.strip()]
            return list(dict.fromkeys(parts))  # 去重但保持顺序
        if isinstance(raw, list):
            parts: List[str] = []
            for item in raw:
                if not isinstance(item, str):
                    continue
                addr = item.strip()
                if addr:
                    parts.append(addr)
            return list(dict.fromkeys(parts))
        return []

    @staticmethod
    def _maybe_login(smtp: smtplib.SMTP, username: Optional[str], password: Optional[str]) -> None:
        if username and password:
            smtp.login(username, password)


# ---------- 模板字符串节点 ----------


@register_node
class TemplateStringNode(WorkflowNode):
    """
    模板字符串节点：
    - 使用 Mustache 风格占位符 {{path}} 渲染文本
    - 输入 context 可为对象或 JSON 字符串，内部使用 JSON 路径解析占位符
    """

    type = "string-template"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "模板字符串",
            "description": "使用 {{path}} 占位符从输入 JSON 中取值并渲染为字符串",
            "category": "Logic",
            "icon": "material:format_quote",
            "inputs": [
                {
                    "id": "context",
                    "label": "上下文",
                    "type": ["object", "string"],
                    "required": False,
                    "desc": "模板变量来源：对象或 JSON 字符串；若为非对象，将作为变量 value 提供",
                }
            ],
            "outputs": [
                {
                    "id": "text",
                    "label": "结果字符串",
                    "type": "string",
                    "required": True,
                }
            ],
            "parameters": [
                {
                    "name": "template",
                    "label": "模板",
                    "dataType": "string",
                    "widgetType": "textarea",
                    "config": {
                        "type": "textarea",
                        "placeholder": "例如：你好，{{user.name}}，今天是 {{date}}。",
                    },
                    "required": True,
                    "desc": (
                        "使用 {{path}} 作为占位符，其中 path 使用与 JSON 提取节点相同的路径语法："
                        "如 user.name 或 items[0].price。"
                    ),
                }
            ],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict) -> JsonDict:
        template = params.get("template") or ""
        if not isinstance(template, str):
            template = str(template)

        raw_context = inputs.get("context")

        # 将上下文转换为对象形式，便于路径解析
        ctx: Any
        if isinstance(raw_context, dict):
            ctx = raw_context
        elif isinstance(raw_context, str):
            try:
                parsed = json.loads(raw_context)
            except json.JSONDecodeError:
                parsed = raw_context
            # 如果解析结果是对象，则直接使用，否则包一层 value
            if isinstance(parsed, dict):
                ctx = parsed
            else:
                ctx = {"value": parsed}
        else:
            ctx = {"value": raw_context}

        text = _render_template_string(template, ctx)
        return {"text": text}


def _render_template_string(template: str, context: Any) -> str:
    """
    使用 Mustache 风格 {{path}} 渲染模板：
    - path 支持 a.b[0].c 形式，内部复用 _json_resolve_path
    - 找不到时替换为空字符串
    """
    if not template:
        return ""

    # 只支持字母/数字/下划线和点号的路径
    pattern = re.compile(r"{{\s*([a-zA-Z_][\w\.]*)\s*}}")

    def repl(match: "re.Match[str]") -> str:
        path = match.group(1).strip()
        value = _json_resolve_path(context, path, default="")
        if value is None:
            return ""
        return str(value)

    return pattern.sub(repl, template)


# ---------- Text 节点 ----------
# ---------- Text 节点 ----------


@register_node
class TextNode(WorkflowNode):
    """简单透传节点：输出等于输入，用于文本流转"""

    type = "text"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "文本",
            "description": "文本透传节点（输出 = 输入），常量由端口配置决定",
            "category": "IO",
            "icon": "material:text_fields",
            "inputs": [
                {
                    "id": "text",
                    "label": "文本输入",
                    "type": "string",
                    "required": False,
                }
            ],
            "outputs": [
                {
                    "id": "text",
                    "label": "文本输出",
                    "type": "string",
                    "required": True,
                }
            ],
            "parameters": [],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict) -> JsonDict:
        # 简单透传：输出 = 输入；未连线时输出空字符串，由前端/调度器用端口常量补充
        text = inputs.get("text", "")
        return {"text": text}


# ---------- Renderer 节点 ----------


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
            "icon": "material:preview",
            "inputs": [
                {
                    "id": "text",
                    "label": "文本输入",
                    "type": ["string", "object"],
                    "required": True,
                    "desc": "要展示的文本，一般来自 LLM/文本节点；也可为对象，由前端决定展示方式",
                }
            ],
            "outputs": [],
            "parameters": [
                {
                    "name": "title",
                    "label": "显示标题",
                    "dataType": "string",
                    "widgetType": "input",
                    "config": {
                        "type": "input",
                        "placeholder": "例如：LLM 输出预览",
                    },
                    "default": "渲染结果",
                    "required": False,
                    "uiPlacement": "node-body",
                }
            ],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict) -> JsonDict:
        # 渲染逻辑主要在前端，这里做透传，便于调试/回放
        text = inputs.get("text", "")
        return {
            "text": text,
            "title": params.get("title", "渲染结果"),
        }


# ---------- LLM 节点 ----------


def _create_openai_client(base_url: Optional[str] = None) -> OpenAI:
    """
    创建 OpenAI 客户端：
    - API Key 从环境变量 LLM_API_KEY 读取（避免明文写死）
    - base_url: 优先使用节点参数，其次使用环境变量 LLM_BASE_URL，再次默认 siliconflow
    """
    api_key = os.getenv("LLM_API_KEY")
    if not api_key:
        raise RuntimeError("缺少环境变量 LLM_API_KEY，无法调用大模型")

    return OpenAI(
        api_key=api_key,
        base_url=base_url or os.getenv("LLM_BASE_URL") or "https://api.siliconflow.cn/v1",
    )


@register_node
class LLMNode(WorkflowNode):
    """
    LLM 调用节点：
    - 参数：base_url / model / temperature / system_prompt
    - 输入：message（string）
    - 输出：text（处理后的纯文本）+ raw（完整 JSON）
    """

    type = "llm"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "LLM 调用",
            "description": "调用兼容 OpenAI 的 LLM，输出文本和原始 JSON",
            "category": "Model",
            "icon": "material:smart_toy",
            "inputs": [
                {
                    "id": "message",
                    "label": "消息",
                    "type": "string",
                    "required": True,
                    "desc": "用户要发送给模型的消息，一般来自 Text 节点",
                }
            ],
            "outputs": [
                {
                    "id": "text",
                    "label": "文本输出",
                    "type": "string",
                    "required": True,
                    "desc": "模型回复的纯文本（去掉思考过程）",
                },
                {
                    "id": "raw",
                    "label": "原始响应",
                    "type": "object",
                    "required": False,
                    "desc": "完整的模型响应 JSON，用于调试或二次处理",
                },
            ],
            "parameters": [
                {
                    "name": "base_url",
                    "label": "Base URL",
                    "dataType": "string",
                    "widgetType": "input",
                    "config": {
                        "type": "input",
                        "placeholder": "默认使用后端配置的 LLM_BASE_URL",
                    },
                    "required": False,
                },
                {
                    "name": "model",
                    "label": "模型标识",
                    "dataType": "string",
                    "widgetType": "input",
                    "config": {
                        "type": "input",
                        "placeholder": "如 deepseek-ai/DeepSeek-R1-Distill-Qwen-7B",
                    },
                    "default": "deepseek-ai/DeepSeek-R1-Distill-Qwen-7B",
                    "required": True,
                },
                {
                    "name": "temperature",
                    "label": "温度",
                    "dataType": "number",
                    "widgetType": "slider",
                    "config": {
                        "type": "slider",
                        "min": 0.0,
                        "max": 1.0,
                        "step": 0.1,
                    },
                    "default": 0.7,
                    "required": False,
                },
                {
                    "name": "system_prompt",
                    "label": "系统提示词（可选）",
                    "dataType": "string",
                    "widgetType": "textarea",
                    "config": {
                        "type": "textarea",
                        "rows": 3,
                        "placeholder": "例如：你是一个乐于助人的助手。",
                    },
                    "required": False,
                },
            ],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict) -> JsonDict:
        message = inputs.get("message")
        if message is None:
            raise ValueError("LLM 节点缺少必需的输入 message")

        base_url = params.get("base_url") or None
        model = params.get("model") or "deepseek-ai/DeepSeek-R1-Distill-Qwen-7B"
        temperature = float(params.get("temperature") or 0.7)
        system_prompt = params.get("system_prompt") or ""

        client = _create_openai_client(base_url=base_url)

        def _call_llm() -> tuple[str, Any]:
            msgs = []
            if system_prompt:
                msgs.append({"role": "system", "content": system_prompt})
            msgs.append({"role": "user", "content": str(message)})

            resp = client.chat.completions.create(
                model=model,
                messages=msgs,
                temperature=temperature,
                stream=False,
            )
            content = resp.choices[0].message.content or ""

            # 兼容 DeepSeek R1 的 </think> 包裹格式
            text = content
            if "</think>" in content:
                try:
                    _, rest = content.split("</think>", 1)
                    text = rest.strip()
                except Exception:
                    # 防御性处理：解析失败就直接用原文
                    text = content

            # 尽量拿到可 JSON 化的原始输出
            dump_fn = getattr(resp, "model_dump", None)
            if callable(dump_fn):
                raw: Any = dump_fn()
            else:
                raw = getattr(resp, "to_dict", lambda: resp)()
            return text, raw

        loop = asyncio.get_event_loop()
        text, raw = await loop.run_in_executor(None, _call_llm)
        return {"text": text, "raw": raw}


# ---------- Super LLM（插件化 + tool_call） ----------


@register_node
class SuperLLMNode(WorkflowNode):
    """
    增强版 LLM 节点：
    - 支持插件链（pre/post hooks）
    - 支持 tool_call：如果模型返回 tool_calls，则自动执行并将结果反馈给模型，直到无 tool_calls
    """

    type = "llm-super"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "LLM（增强版）",
            "description": "支持插件、tools 的增强版 LLM 节点",
            "category": "Model",
            "icon": "material:smart_toy",
            "inputs": [
                {
                    "id": "message",
                    "label": "消息",
                    "type": "string",
                    "required": True,
                },
                {
                    "id": "session_id",
                    "label": "会话 ID",
                    "type": "string",
                    "required": False,
                },
            ],
            "outputs": [
                {
                    "id": "text",
                    "label": "文本输出",
                    "type": "string",
                    "required": True,
                },
                {
                    "id": "raw",
                    "label": "原始响应",
                    "type": "object",
                    "required": False,
                },
            ],
            "parameters": [
                {
                    "name": "base_url",
                    "label": "Base URL",
                    "dataType": "string",
                    "widgetType": "input",
                    "config": {"type": "input", "placeholder": "默认使用 LLM_BASE_URL"},
                    "required": False,
                },
                {
                    "name": "model",
                    "label": "模型标识",
                    "dataType": "string",
                    "widgetType": "input",
                    "config": {"type": "input", "placeholder": "如 deepseek-ai/DeepSeek-R1-Distill-Qwen-7B"},
                    "default": "deepseek-ai/DeepSeek-R1-Distill-Qwen-7B",
                    "required": True,
                },
                {
                    "name": "temperature",
                    "label": "温度",
                    "dataType": "number",
                    "widgetType": "slider",
                    "config": {"type": "slider", "min": 0.0, "max": 1.0, "step": 0.1},
                    "default": 0.7,
                    "required": False,
                },
                {
                    "name": "system_prompt",
                    "label": "系统提示词（可选）",
                    "dataType": "string",
                    "widgetType": "textarea",
                    "config": {"type": "textarea", "rows": 3, "placeholder": "例如：你是一个乐于助人的助手。"},
                    "required": False,
                },
                {
                    "name": "plugins",
                    "label": "插件列表",
                    "dataType": "array",
                    "widgetType": "plugins",
                    "config": {
                        "type": "plugin-list",
                        # 前端自行从 /plugins 获取可用插件 schema
                    },
                    "required": False,
                    "desc": "按顺序执行的插件列表，每项：{ type, config }",
                },
            ],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict) -> JsonDict:
        message = inputs.get("message")
        if message is None:
            raise ValueError("Super LLM 节点缺少必需的输入 message")

        base_url = params.get("base_url") or None
        model = params.get("model") or "deepseek-ai/DeepSeek-R1-Distill-Qwen-7B"
        temperature = float(params.get("temperature") or 0.7)
        system_prompt = params.get("system_prompt") or ""

        # 初始化上下文
        context = LLMContext(
            session_id=inputs.get("session_id"),
            user_query=message,
            messages=[{"role": "user", "content": str(message)}],
            system_prompt=system_prompt,
        )

        # 运行插件 pre hooks
        executor = PluginExecutor(params.get("plugins") or [], context)
        await executor.run_pre_process()

        # 构造 client
        client = _create_openai_client(base_url=base_url)

        async def call_llm_with_context(ctx: LLMContext) -> Any:
            msgs = list(ctx.messages)
            if ctx.system_prompt:
                msgs = [{"role": "system", "content": ctx.system_prompt}] + msgs
            return await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: client.chat.completions.create(
                    model=model,
                    messages=msgs,
                    temperature=temperature,
                    tools=ctx.tools if ctx.tools else None,
                    tool_choice="auto" if ctx.tools else None,
                ),
            )

        def process_response(resp: Any) -> tuple[str, Any, List[Dict[str, Any]]]:
            raw = getattr(resp, "model_dump", lambda: resp)()
            choices = raw.get("choices", [])
            first = choices[0] if choices else {}
            msg = first.get("message", {}) or {}
            content = msg.get("content") or ""
            tool_calls = msg.get("tool_calls") or []
            if "</think>" in content:
                try:
                    _, rest = content.split("</think>", 1)
                    content = rest.strip()
                except Exception:
                    pass
            return content, raw, tool_calls

        # 主循环：执行模型 + tool_call
        current_resp = await call_llm_with_context(context)
        text, raw, tool_calls = process_response(current_resp)

        while tool_calls:
            tool_messages = []
            for tc in tool_calls:
                name = tc.get("function", {}).get("name")
                args_str = tc.get("function", {}).get("arguments", "{}")
                func = context.tool_functions.get(name)
                if not func:
                    tool_messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tc.get("id"),
                            "name": name or "",
                            "content": f"Tool {name!r} not implemented.",
                        }
                    )
                    continue
                try:
                    parsed_args = json.loads(args_str) if args_str else {}
                except json.JSONDecodeError:
                    parsed_args = {}
                try:
                    result = func(**parsed_args) if isinstance(parsed_args, dict) else func(parsed_args)
                except Exception as exc:  # noqa: BLE001
                    result = f"Tool execution error: {exc}"

                tool_messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tc.get("id"),
                        "name": name or "",
                        "content": json.dumps(result, ensure_ascii=False),
                    }
                )

            context.messages.extend(tool_messages)
            current_resp = await call_llm_with_context(context)
            text, raw, tool_calls = process_response(current_resp)

        context.ai_response_text = text
        context.ai_raw_response = raw

        # 运行插件 post hooks
        await executor.run_post_process()

        return {"text": context.ai_response_text, "raw": context.ai_raw_response}
