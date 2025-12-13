import asyncio
import copy
import json
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from workflow_engine import (
    OverrideDTO,
    RunWorkflowSpec,
    WorkflowEdgeDTO,
    WorkflowEvent,
    WorkflowNodeDTO,
    run_workflow,
)
from nodes import get_all_node_schemas
from db import get_session, Workflow

HEARTBEAT_INTERVAL_SECONDS = 10.0

router = APIRouter()


class WorkflowNodeModel(BaseModel):
    """前端提交的节点结构（与 React Flow 节点对应）"""

    id: str
    type: str
    params: Dict[str, Any] = Field(default_factory=dict)
    portConstants: Dict[str, Any] = Field(default_factory=dict)


class WorkflowEdgeModel(BaseModel):
    """前端提交的边结构（与 React Flow edge 对应）"""

    id: str
    sourceNodeId: str
    sourcePortId: str
    targetNodeId: str
    targetPortId: str
    kind: str = Field(default="data", pattern="^(data|control)$")


class OverrideModel(BaseModel):
    """本次运行对某个端口的覆盖值"""

    nodeId: str
    portId: str
    value: Any


class RunWorkflowRequestModel(BaseModel):
    """启动一次工作流运行的请求体"""

    workflowId: Optional[str] = None
    nodes: List[WorkflowNodeModel] = Field(default_factory=list)
    edges: List[WorkflowEdgeModel] = Field(default_factory=list)
    entryNodes: Optional[List[str]] = None
    targets: Optional[List[str]] = None
    overrides: List[OverrideModel] = Field(default_factory=list)


class RunStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    ERROR = "error"


@dataclass
class RunState:
    """运行实例的状态（仅存于内存，用于 SSE 推送）"""

    id: str
    status: RunStatus
    queue: "asyncio.Queue[WorkflowEvent]"
    task: asyncio.Task


RUNS: Dict[str, RunState] = {}
LAST_SPEC: Optional[RunWorkflowSpec] = None  # 仅供临时测试使用，内存缓存


@router.post("/workflow/run")
async def start_workflow_run(payload: RunWorkflowRequestModel, session: AsyncSession = Depends(get_session)):
    """
    启动一次工作流运行：
    - 立即返回 runId
    - 实际执行放在后台任务中，通过 SSE 推送进度
    """

    # 若指定 workflowId 且未携带图定义，则从数据库加载 definition
    if payload.workflowId and not payload.nodes and not payload.edges:
        wf = (
            await session.execute(select(Workflow).where(Workflow.id == payload.workflowId))
        ).scalar_one_or_none()
        if not wf:
            raise HTTPException(status_code=404, detail="workflow not found")
        definition = wf.definition or {}
        payload.nodes = [WorkflowNodeModel(**n) for n in definition.get("nodes", [])]  # type: ignore[arg-type]
        payload.edges = [WorkflowEdgeModel(**e) for e in definition.get("edges", [])]  # type: ignore[arg-type]
        payload.entryNodes = payload.entryNodes or definition.get("entryNodes")
        payload.targets = payload.targets or definition.get("targets")
    run_id = str(uuid4())
    queue: "asyncio.Queue[WorkflowEvent]" = asyncio.Queue()

    # 先占位创建 RunState，方便 emitter 更新状态
    state = RunState(
        id=run_id,
        status=RunStatus.QUEUED,
        queue=queue,
        task=None,  # 稍后填充
    )
    RUNS[run_id] = state

    async def emit(event: WorkflowEvent) -> None:
        # 根据事件更新 RunState 的整体状态
        if event.event == "workflow_completed":
            status = event.data.get("status")
            if status == "success":
                state.status = RunStatus.SUCCESS
            elif status == "error":
                state.status = RunStatus.ERROR
        elif event.event == "workflow_started":
            state.status = RunStatus.RUNNING

        await queue.put(event)

    # 将 Pydantic 模型转换为内部 DTO
    spec = RunWorkflowSpec(
        nodes=[
            WorkflowNodeDTO(
                id=n.id,
                type=n.type,
                params=n.params,
                port_constants=n.portConstants or None,
            )
            for n in payload.nodes
        ],
        edges=[
            WorkflowEdgeDTO(
                id=e.id,
                source_node_id=e.sourceNodeId,
                source_port_id=e.sourcePortId,
                target_node_id=e.targetNodeId,
                target_port_id=e.targetPortId,
                kind=e.kind or "data",
            )
            for e in payload.edges
        ],
        entry_nodes=payload.entryNodes,
        targets=payload.targets,
        overrides=[
            OverrideDTO(node_id=o.nodeId, port_id=o.portId, value=o.value)
            for o in payload.overrides
        ],
    )

    async def runner() -> None:
        try:
            await run_workflow(run_id=run_id, spec=spec, emit=emit, context={})
        finally:
            # 通知 SSE 结束
            await queue.put(
                WorkflowEvent(
                    event="_internal_done",
                    data={"runId": run_id},
                )
            )

    state.task = asyncio.create_task(runner())

    # 缓存最近一次提交的完整图（仅临时调试用）
    global LAST_SPEC
    LAST_SPEC = copy.deepcopy(spec)

    return {"runId": run_id, "status": state.status.value}


@router.api_route(
    "/workflow/run/last/http",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
)
async def run_last_workflow_via_http(request: Request):
    """
    临时调试入口：使用最近一次提交的工作流图，按 HTTP Start 配置匹配 method+path 后执行。
    - 需至少包含一个 trigger.http 节点。
    - 请求 path 通过 query 参数 `path`（若未提供则用当前路由路径）与节点 params.path 匹配。
    - 仅匹配到的 HTTP Start 会被作为入口。
    """

    if LAST_SPEC is None:
        raise HTTPException(status_code=404, detail="no cached workflow")

    # 构造 request 上下文
    headers: Dict[str, Any] = dict(request.headers)
    query: Dict[str, Any] = dict(request.query_params)
    raw_body_bytes = await request.body()
    raw_body: Optional[str] = None
    try:
        raw_body = raw_body_bytes.decode("utf-8") if raw_body_bytes else None
    except Exception:
        raw_body = None

    body: Any = None
    if raw_body:
        try:
            body = json.loads(raw_body)
        except Exception:
            body = raw_body

    method = request.method.upper()

    # 计算用于匹配的路径：优先 query，若无则用去掉当前路由前缀后的路径
    path_for_match = query.get("path")
    if not path_for_match:
        current_path = str(request.url.path)
        route_path = getattr(request.scope.get("route"), "path", None)
        if route_path and current_path.startswith(route_path):
            trimmed = current_path[len(route_path) :] or "/"
            path_for_match = trimmed
        else:
            path_for_match = "/"

    context_request = {
        "method": method,
        "path": path_for_match,
        "headers": headers,
        "query": query,
        "body": body,
        "rawBody": raw_body,
        "client": request.client.host if request.client else None,
    }

    http_starts = [n for n in LAST_SPEC.nodes if n.type == "trigger.http"]
    if not http_starts:
        raise HTTPException(status_code=400, detail="cached workflow has no HTTP Start node")

    matched = None
    for n in http_starts:
        cfg_method = (n.params.get("method") or "").upper()
        cfg_path = n.params.get("path") or ""
        if (not cfg_method or cfg_method == method) and (not cfg_path or cfg_path == path_for_match):
            matched = n
            break

    if matched is None:
        raise HTTPException(status_code=400, detail="no HTTP Start matched method/path")

    spec = copy.deepcopy(LAST_SPEC)
    spec.entry_nodes = [matched.id]

    run_id = str(uuid4())
    events: List[WorkflowEvent] = []
    final_event: Optional[WorkflowEvent] = None
    last_error: Optional[Dict[str, Any]] = None

    async def emit(event: WorkflowEvent) -> None:
        nonlocal final_event, last_error
        events.append(event)
        if event.event == "node_completed" and event.data.get("status") == "error":
            last_error = {
                "nodeId": event.data.get("nodeId"),
                "message": event.data.get("errorMessage"),
            }
        if event.event == "workflow_completed":
            final_event = event

    await run_workflow(run_id=run_id, spec=spec, emit=emit, context={"request": context_request})

    if final_event is None:
        raise HTTPException(status_code=500, detail="workflow did not return completion event")

    return {
        "runId": run_id,
        "status": final_event.data.get("status"),
        "results": final_event.data.get("results"),
        "error": last_error,
    }


@router.get("/workflow/run/{run_id}/events")
async def stream_workflow_events(run_id: str):
    """
    SSE 通道：前端通过 EventSource 订阅某次运行的所有事件
    """
    state = RUNS.get(run_id)
    if state is None:
        raise HTTPException(status_code=404, detail="Run not found")

    queue = state.queue

    async def event_generator():
        try:
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=HEARTBEAT_INTERVAL_SECONDS)
                except asyncio.TimeoutError:
                    heartbeat = {"runId": run_id, "ts": time.time()}
                    yield "event: heartbeat\n"
                    yield f"data: {json.dumps(heartbeat, ensure_ascii=False)}\n\n"
                    continue

                if event.event == "_internal_done":
                    break

                # 标准 SSE 格式：event + data
                yield f"event: {event.event}\n"
                yield f"data: {json.dumps(event.data, ensure_ascii=False)}\n\n"

        except asyncio.CancelledError:  # 连接被客户端关闭
            return

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@router.get("/workflows/{workflow_id}/graph")
async def get_workflow_graph(workflow_id: str, includeDefinitions: bool = False):
    """
    查询工作流的图结构。
    现阶段尚未接入持久化，先返回占位结构，便于前端对接。
    """
    resp = {"workflowId": workflow_id, "nodes": [], "edges": []}
    if includeDefinitions:
        resp["definitions"] = get_all_node_schemas()
    return resp
