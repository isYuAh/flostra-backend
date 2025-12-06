from typing import Any, Dict

from fastapi import APIRouter
from pydantic import BaseModel, Field

from nodes import get_all_node_schemas, get_node_cls

router = APIRouter()


class NodeExecuteRequest(BaseModel):
    type: str
    inputs: Dict[str, Any] = Field(default_factory=dict)
    params: Dict[str, Any] = Field(default_factory=dict)


@router.get("/nodes")
async def list_nodes():
    """前端获取所有节点 schema"""
    return {"nodes": get_all_node_schemas()}


@router.post("/nodes/execute")
async def execute_node(payload: NodeExecuteRequest):
    """执行单个节点"""
    node_cls = get_node_cls(payload.type)
    outputs = await node_cls.run(payload.inputs, payload.params)
    return {"type": payload.type, "outputs": outputs}

