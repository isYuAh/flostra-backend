from __future__ import annotations

from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import select, update, delete
from sqlalchemy.ext.asyncio import AsyncSession

from db import Workflow, get_session
from nodes import get_all_node_schemas

router = APIRouter()


class WorkflowCreate(BaseModel):
    name: str
    description: Optional[str] = None
    definition: Dict[str, Any] = Field(default_factory=dict)


class WorkflowUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    definition: Optional[Dict[str, Any]] = None


@router.post("/workspaces/{workspace_id}/workflows")
async def create_workflow(workspace_id: UUID, payload: WorkflowCreate, session: AsyncSession = Depends(get_session)):
    wf = Workflow(
        workspace_id=workspace_id,
        name=payload.name,
        description=payload.description,
        definition=payload.definition,
    )
    session.add(wf)
    await session.commit()
    await session.refresh(wf)
    return {"id": str(wf.id), "workspaceId": str(wf.workspace_id)}


@router.put("/workspaces/{workspace_id}/workflows/{workflow_id}")
async def update_workflow(
    workspace_id: UUID,
    workflow_id: UUID,
    payload: WorkflowUpdate,
    session: AsyncSession = Depends(get_session),
):
    stmt = (
        update(Workflow)
        .where(Workflow.id == workflow_id, Workflow.workspace_id == workspace_id)
        .values(
            **{
                k: v
                for k, v in {
                    "name": payload.name,
                    "description": payload.description,
                    "definition": payload.definition,
                }.items()
                if v is not None
            }
        )
        .returning(Workflow.id)
    )
    res = await session.execute(stmt)
    row = res.first()
    if row is None:
        raise HTTPException(status_code=404, detail="workflow not found")
    await session.commit()
    return {"id": str(row.id)}


@router.get("/workspaces/{workspace_id}/workflows")
async def list_workflows(workspace_id: UUID, session: AsyncSession = Depends(get_session)):
    rows = (
        await session.execute(
            select(
                Workflow.id,
                Workflow.name,
                Workflow.description,
                Workflow.updated_at,
            ).where(Workflow.workspace_id == workspace_id)
        )
    ).all()
    return {
        "items": [
            {
                "id": str(r.id),
                "name": r.name,
                "description": r.description,
                "updatedAt": r.updated_at,
            }
            for r in rows
        ]
    }


@router.get("/workspaces/{workspace_id}/workflows/{workflow_id}/graph")
async def get_workflow_graph(
    workspace_id: UUID,
    workflow_id: UUID,
    includeDefinitions: bool = False,
    session: AsyncSession = Depends(get_session),
):
    wf = (
        await session.execute(
            select(Workflow).where(Workflow.id == workflow_id, Workflow.workspace_id == workspace_id)
        )
    ).scalar_one_or_none()
    if not wf:
        raise HTTPException(status_code=404, detail="workflow not found")
    definition = wf.definition or {}
    resp: Dict[str, Any] = {
        "workflowId": str(workflow_id),
        "nodes": definition.get("nodes", []),
        "edges": definition.get("edges", []),
        "entryNodes": definition.get("entryNodes"),
        "targets": definition.get("targets"),
    }
    if includeDefinitions:
        resp["definitions"] = get_all_node_schemas()
    return resp


@router.delete("/workspaces/{workspace_id}/workflows/{workflow_id}")
async def delete_workflow(workspace_id: UUID, workflow_id: UUID, session: AsyncSession = Depends(get_session)):
    res = await session.execute(
        delete(Workflow).where(Workflow.id == workflow_id, Workflow.workspace_id == workspace_id)
    )
    if res.rowcount == 0:
        raise HTTPException(status_code=404, detail="workflow not found")
    await session.commit()
    return {"deleted": True}
