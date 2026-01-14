from __future__ import annotations

import asyncio
import base64
import json
from typing import Any, Dict, Optional, List, Union

import httpx

from .base import JsonDict, WorkflowNode, register_node
from io_utils import FileIoService
from runtime_files import is_file_ref


@register_node
class HttpRequestNode(WorkflowNode):
    """
    HTTP 请求节点：
    - 使用 HTTP 调用外部 API
    - URL / METHOD / BODY 均可由输入端口提供
    - extras 承载 headers/query/auth 等信息
    - 支持文件上传 (Multipart)
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
                {"name": "files", "label": "文件 (支持单个或列表)", "type": ["object", "array"], "required": False},
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
    async def run(cls, inputs: JsonDict, params: JsonDict, context: JsonDict = None) -> JsonDict:
        method = (inputs.get("method") or "GET").upper()
        url = inputs.get("url")
        if not url:
            raise ValueError("HTTP 请求节点缺少 URL（请在输入端口 url 提供）")
        
        if not context:
            raise ValueError("HTTP节点需要运行时上下文")

        extras_raw = inputs.get("extras")
        headers, query, auth_header = cls._normalize_extras(extras_raw)
        body = inputs.get("body")
        files_input = inputs.get("files")

        timeout = cls._parse_timeout(params.get("timeout"))
        retry = cls._parse_retry(params.get("retry"))
        resp: Optional[httpx.Response] = None

        json_body: Any = None
        data_body: Any = None
        multipart_files: Any = None

        # 处理文件上传
        if files_input:
            multipart_files = []
            files_to_process = []
            if isinstance(files_input, list):
                files_to_process = files_input
            elif isinstance(files_input, dict):
                files_to_process = [files_input]
            
            for f in files_to_process:
                if is_file_ref(f):
                    content, filename, mime = await FileIoService.get_file_bytes(context, f)
                    # httpx files format: (field_name, (filename, content, mime_type))
                    # Default field name to 'file' if generic list, or use name from ref if possible?
                    # For now use 'file' as key. If user needs specific keys, they might need a more complex input structure.
                    multipart_files.append(("file", (filename, content, mime)))
            
            if not multipart_files:
                multipart_files = None # No valid files found

        # 如果有文件，Body通常作为 form data
        if multipart_files:
            data_body = {}
            if isinstance(body, dict):
                 # Flatten body for data fields in multipart
                 for k, v in body.items():
                     if isinstance(v, (str, int, float, bool)):
                         data_body[k] = str(v)
                     else:
                         data_body[k] = json.dumps(v)
            elif body is not None:
                # If body is string, ignore or try to use? 
                pass 
            
            # Remove Content-Type if set, let httpx set boundary
            if "Content-Type" in headers:
                headers.pop("Content-Type")
                
        else:
            # Normal Body Processing
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
                        files=multipart_files,
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