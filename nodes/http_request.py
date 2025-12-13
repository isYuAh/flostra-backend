from __future__ import annotations

import asyncio
import base64
import json
from typing import Any, Dict, Optional

import httpx

from .base import JsonDict, WorkflowNode, register_node


@register_node
class HttpRequestNode(WorkflowNode):
    """
    HTTP 请求节点：
    - 使用 HTTP 调用外部 API
    - URL / METHOD / BODY 均可由输入端口提供
    - extras 承载 headers/query/auth 等信息
    """

    type = "http-request"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "HTTP 请求",
            "description": "调用外部 HTTP 接口，返回状态码、响应头和响应体",
            "category": "IO",
            "icon": "http",
            "inputs": [
                {"name": "url", "label": "URL", "type": "string", "required": True},
                {"name": "method", "label": "HTTP 方法", "type": "string", "required": False},
                {"name": "body", "label": "请求 Body", "type": "any", "required": False},
                {"name": "extras", "label": "附加参数", "type": ["object", "string"], "required": False},
            ],
            "outputs": [
                {"name": "status", "label": "状态码", "type": "number", "required": True},
                {"name": "headers", "label": "响应头", "type": "object", "required": False},
                {"name": "body", "label": "响应体", "type": "any", "required": False},
            ],
            "parameters": [
                {"name": "timeout", "label": "超时时间（秒）", "type": "number", "widget": "input", "required": False},
                {"name": "retry", "label": "重试次数", "type": "number", "widget": "input", "required": False},
            ],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict) -> JsonDict:
        method = (inputs.get("method") or "GET").upper()
        url = inputs.get("url")
        if not url:
            raise ValueError("HTTP 请求节点缺少 URL（请在输入端口 url 提供）")

        extras_raw = inputs.get("extras")
        headers, query, auth_header = cls._normalize_extras(extras_raw)
        body = inputs.get("body")

        timeout = cls._parse_timeout(params.get("timeout"))
        retry = cls._parse_retry(params.get("retry"))
        resp: Optional[httpx.Response] = None

        json_body: Any = None
        data_body: Any = None
        if isinstance(body, (dict, list)):
            json_body = body
        elif body is not None:
            data_body = str(body)

        if auth_header and "Authorization" not in headers:
            headers["Authorization"] = auth_header

        attempt = 0
        last_err: Exception | None = None
        while attempt <= retry:
            try:
                async with httpx.AsyncClient(timeout=timeout) as client:
                    resp = await client.request(
                        method=method,
                        url=url,
                        headers=headers,
                        params=query or None,
                        json=json_body,
                        data=data_body,
                    )
                status = resp.status_code
                if status in cls._retriable_statuses() and attempt < retry:
                    attempt += 1
                    await asyncio.sleep(0.5)
                    continue
                break
            except httpx.RequestError as exc:
                last_err = exc
                if attempt < retry:
                    attempt += 1
                    await asyncio.sleep(0.5)
                    continue
                raise ValueError(f"HTTP 请求失败：{exc}") from exc

        if last_err is not None and resp is None:
            raise ValueError(f"HTTP 请求失败：{last_err}") from last_err

        status = resp.status_code
        resp_headers = dict(resp.headers)

        try:
            resp_body: Any = resp.json()
        except ValueError:
            resp_body = resp.text

        return {"status": status, "headers": resp_headers, "body": resp_body}

    @staticmethod
    def _parse_timeout(val: Any) -> float:
        try:
            timeout = float(val) if val is not None else 10.0
            if timeout <= 0:
                return 10.0
            return timeout
        except (TypeError, ValueError):
            return 10.0

    @staticmethod
    def _parse_retry(val: Any) -> int:
        try:
            retry = int(val) if val is not None else 0
            return max(retry, 0)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _normalize_extras(extras: Any) -> tuple[Dict[str, Any], Dict[str, Any], Optional[str]]:
        if extras is None:
            return {}, {}, None
        if isinstance(extras, str):
            try:
                extras = json.loads(extras)
            except json.JSONDecodeError as exc:
                raise ValueError(f"extras 需为对象或可解析为 JSON 的字符串：{exc}") from exc
        if not isinstance(extras, dict):
            raise ValueError("extras 需为对象或可解析为 JSON 的字符串")

        headers = extras.get("headers") or {}
        if not isinstance(headers, dict):
            raise ValueError("extras.headers 必须是对象")

        query = extras.get("query") or {}
        if not isinstance(query, dict):
            raise ValueError("extras.query 必须是对象")

        auth_header: Optional[str] = None
        auth_cfg = extras.get("auth")
        if isinstance(auth_cfg, dict):
            if "bearer" in auth_cfg and auth_cfg.get("bearer"):
                auth_header = f"Bearer {auth_cfg.get('bearer')}"
            elif "basic" in auth_cfg and isinstance(auth_cfg.get("basic"), dict):
                basic = auth_cfg["basic"]
                username = basic.get("username")
                password = basic.get("password")
                if username is not None and password is not None:
                    token = base64.b64encode(f"{username}:{password}".encode()).decode()
                    auth_header = f"Basic {token}"
        elif isinstance(auth_cfg, str) and auth_cfg:
            auth_header = f"Bearer {auth_cfg}"

        return headers, query, auth_header

    @staticmethod
    def _retriable_statuses() -> set[int]:
        return {429, 500, 501, 502, 503, 504, 505, 506, 507, 508, 509, 510, 511}
