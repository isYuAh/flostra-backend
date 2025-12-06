from __future__ import annotations

import asyncio
import collections
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional

from nodes import get_node_cls
from secrets_store import resolve_secret_value

JsonDict = Dict[str, Any]


@dataclass
class WorkflowNodeDTO:
    """前端传入的节点定义（运行时视角）"""

    id: str
    type: str
    params: JsonDict
    port_constants: Optional[JsonDict] = None


@dataclass
class WorkflowEdgeDTO:
    """前端传入的边定义（运行时视角）"""

    id: str
    source_node_id: str
    source_port_id: str
    target_node_id: str
    target_port_id: str


@dataclass
class OverrideDTO:
    """单次运行的端口覆盖值"""

    node_id: str
    port_id: str
    value: Any


@dataclass
class RunWorkflowSpec:
    """一次运行需要的完整图结构"""

    nodes: List[WorkflowNodeDTO]
    edges: List[WorkflowEdgeDTO]
    entry_nodes: Optional[List[str]] = None
    targets: Optional[List[str]] = None
    overrides: List[OverrideDTO] = None

    def __post_init__(self) -> None:
        if self.overrides is None:
            self.overrides = []


@dataclass
class WorkflowEvent:
    """发给 SSE 层的事件"""

    event: str
    data: JsonDict


EventEmitter = Callable[[WorkflowEvent], Awaitable[None]]


def _build_graph(
    spec: RunWorkflowSpec,
) -> Dict[str, Any]:
    """根据节点和边构建拓扑所需的辅助结构"""
    node_map: Dict[str, WorkflowNodeDTO] = {n.id: n for n in spec.nodes}

    # 出边 / 入边表
    out_edges: Dict[str, List[WorkflowEdgeDTO]] = {n.id: [] for n in spec.nodes}
    in_edges: Dict[str, List[WorkflowEdgeDTO]] = {n.id: [] for n in spec.nodes}
    indegree: Dict[str, int] = {n.id: 0 for n in spec.nodes}

    for e in spec.edges:
        if e.source_node_id not in node_map or e.target_node_id not in node_map:
            raise ValueError(f"Edge {e.id!r} references unknown node(s)")
        out_edges[e.source_node_id].append(e)
        in_edges[e.target_node_id].append(e)
        indegree[e.target_node_id] += 1

    # Kahn 拓扑排序，要求是 DAG
    queue: "collections.deque[str]" = collections.deque(
        [nid for nid, deg in indegree.items() if deg == 0]
    )
    topo_order: List[str] = []

    while queue:
        nid = queue.popleft()
        topo_order.append(nid)
        for e in out_edges.get(nid, []):
            indegree[e.target_node_id] -= 1
            if indegree[e.target_node_id] == 0:
                queue.append(e.target_node_id)

    if len(topo_order) != len(spec.nodes):
        raise ValueError("Workflow graph contains cycles, DAG is required")

    # 出度为 0 的节点（sink）
    sink_nodes = [nid for nid, outs in out_edges.items() if not outs]

    # 入度为 0 的节点（source）
    source_nodes = [nid for nid, deg in indegree.items() if deg == 0]

    # End 节点：约定 type == "end"
    end_nodes = [nid for nid, n in node_map.items() if n.type == "end"]

    return {
        "node_map": node_map,
        "out_edges": out_edges,
        "in_edges": in_edges,
        "topo_order": topo_order,
        "sink_nodes": sink_nodes,
        "source_nodes": source_nodes,
        "end_nodes": end_nodes,
    }


def _apply_overrides(
    node_id: str,
    base_inputs: JsonDict,
    overrides_by_node: Dict[str, List[OverrideDTO]],
) -> JsonDict:
    """将 overrides 应用到某个节点的输入上，后写覆盖先写"""
    result = dict(base_inputs)
    for ov in overrides_by_node.get(node_id, []):
        result[ov.port_id] = ov.value
    return result


def _merge_inputs_for_node(
    node_id: str,
    node: WorkflowNodeDTO,
    in_edges: Dict[str, List[WorkflowEdgeDTO]],
    context_outputs: Dict[str, JsonDict],
) -> JsonDict:
    """
    汇总某个节点的输入：
    - 先应用 port_constants（仅作为“未连线时的默认值”）
    - 再叠加来自所有入边的上游输出（上游优先级更高）
    """
    inputs: JsonDict = {}

    # 1) 先用常量端口作为默认值
    if node.port_constants:
        inputs.update(node.port_constants)

    # 2) 再用连线覆盖：有上游时，以上游为准
    for edge in in_edges.get(node_id, []):
        source_outputs = context_outputs.get(edge.source_node_id, {})
        if edge.source_port_id in source_outputs:
            inputs[edge.target_port_id] = source_outputs[edge.source_port_id]

    return inputs


def _resolve_secrets(obj: Any) -> Any:
    """
    预留的 Secret 解析入口：
    - 识别 {"__kind": "secretRef", "id": "..."} 结构并替换为真实值
    - 其它值保持不变，支持嵌套 dict / list
    """
    if isinstance(obj, dict):
        # SecretRef 的约定结构
        if obj.get("__kind") == "secretRef":
            secret_id = obj.get("id")
            if not secret_id:
                raise ValueError("secretRef 缺少 id 字段")
            # 运行时从内存存储中解析真实值
            return resolve_secret_value(secret_id)

        # 普通 dict，递归解析内部值
        return {k: _resolve_secrets(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [_resolve_secrets(v) for v in obj]

    return obj


async def run_workflow(
    run_id: str,
    spec: RunWorkflowSpec,
    emit: EventEmitter,
    context: Optional[Dict[str, Any]] = None,
) -> None:
    """
    执行整张工作流：
    - 先拓扑排序
    - 按顺序执行每个节点
    - 通过 emit 回调把事件抛给外层（通常是 SSE）
    """
    graph = _build_graph(spec)
    node_map: Dict[str, WorkflowNodeDTO] = graph["node_map"]
    out_edges: Dict[str, List[WorkflowEdgeDTO]] = graph["out_edges"]
    in_edges: Dict[str, List[WorkflowEdgeDTO]] = graph["in_edges"]
    topo_order: List[str] = graph["topo_order"]
    sink_nodes: List[str] = graph["sink_nodes"]
    end_nodes: List[str] = graph["end_nodes"]

    # 预处理 overrides：按 node 分组
    overrides_by_node: Dict[str, List[OverrideDTO]] = {}
    for ov in spec.overrides:
        overrides_by_node.setdefault(ov.node_id, []).append(ov)

    context_outputs: Dict[str, JsonDict] = {}

    # 计算可达子图（REACHABLE_SET）
    all_nodes = set(node_map.keys())
    reachable: set[str]

    if not spec.entry_nodes:
        # 默认入口：若无 HTTP 请求上下文，则跳过 trigger_http 作为起点
        default_entries = [nid for nid in graph["source_nodes"]]
        if not (context or {}).get("request"):
            default_entries = [
                nid for nid in default_entries if node_map[nid].type != "trigger_http"
            ]
        # 如果全被过滤掉且没有 HTTP 上下文，则不回退整图，保持空入口（视为不执行任何节点）
        if default_entries:
            spec.entry_nodes = default_entries
    
    if spec.entry_nodes is not None and len(spec.entry_nodes) == 0:
        # 显式空入口：不执行任何节点
        reachable = set()
    elif spec.entry_nodes:
        # 显式（或刚刚推导出的）入口：只执行从这些入口可达的子图
        for nid in spec.entry_nodes:
            if nid not in node_map:
                raise ValueError(f"entry node {nid!r} not found in workflow nodes")

        reachable = set()
        queue: "collections.deque[str]" = collections.deque(spec.entry_nodes)
        while queue:
            nid = queue.popleft()
            if nid in reachable:
                continue
            reachable.add(nid)
            for edge in out_edges.get(nid, []):
                queue.append(edge.target_node_id)
    else:
        # 仍然没有入口，则整图执行
        reachable = all_nodes

    # 在全局拓扑序基础上过滤得到本次实际执行顺序
    effective_order: List[str] = [nid for nid in topo_order if nid in reachable]

    await emit(WorkflowEvent(event="workflow_started", data={"runId": run_id}))

    for node_id in effective_order:
        node = node_map[node_id]

        await emit(
            WorkflowEvent(
                event="node_started",
                data={"runId": run_id, "nodeId": node_id},
            )
        )

        # 1) 汇总输入
        base_inputs = _merge_inputs_for_node(
            node_id=node_id,
            node=node,
            in_edges=in_edges,
            context_outputs=context_outputs,
        )
        inputs = _apply_overrides(
            node_id=node_id,
            base_inputs=base_inputs,
            overrides_by_node=overrides_by_node,
        )

        # 2) 解析 Secret：
        safe_inputs = _resolve_secrets(inputs)
        safe_params = _resolve_secrets(node.params)

        # 2.5) 注入运行上下文（只读），避免被 overrides 覆盖
        if context is not None:
            safe_inputs["__context"] = context

        # 3) 执行节点
        try:
            node_cls = get_node_cls(node.type)
            outputs = await node_cls.run(safe_inputs, safe_params)
            context_outputs[node_id] = outputs

            await emit(
                WorkflowEvent(
                    event="node_completed",
                    data={
                        "runId": run_id,
                        "nodeId": node_id,
                        "status": "success",
                        "inputValues": inputs,
                        "outputValues": outputs,
                    },
                )
            )
        except Exception as exc:  # noqa: BLE001
            # 节点失败时，整个 workflow 直接失败并结束
            await emit(
                WorkflowEvent(
                    event="node_completed",
                    data={
                        "runId": run_id,
                        "nodeId": node_id,
                        "status": "error",
                        "errorMessage": str(exc),
                    },
                )
            )
            await emit(
                WorkflowEvent(
                    event="workflow_completed",
                    data={"runId": run_id, "status": "error"},
                )
            )
            return

    # 计算最终返回结果：只返回 targets
    # 优先级：
    # 1) 显式指定的 targets ∩ reachable
    # 2) 可达子图中的 End 节点
    # 3) 可达子图中的 sink 节点
    targets: List[str]
    if spec.targets:
        targets = [nid for nid in spec.targets if nid in reachable]
    else:
        reachable_ends = [nid for nid in end_nodes if nid in reachable]
        if reachable_ends:
            targets = reachable_ends
        else:
            reachable_sinks = [nid for nid in sink_nodes if nid in reachable]
            targets = reachable_sinks

    results_dict: Dict[str, Any] = {
        nid: context_outputs.get(nid) for nid in targets if nid in context_outputs
    }

    # 如果只有单个目标节点，直接返回其值；多个则返回按节点 ID 的字典
    if len(results_dict) == 1:
        results: Any = next(iter(results_dict.values()))
    else:
        results = results_dict

    await emit(
        WorkflowEvent(
            event="workflow_completed",
            data={
                "runId": run_id,
                "status": "success",
                "results": results,
            },
        )
    )
