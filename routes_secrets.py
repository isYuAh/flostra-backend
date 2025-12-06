from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from secrets_store import (
    SecretNotFoundError,
    create_secret,
    delete_secret,
    list_secrets,
)

router = APIRouter()


class SecretCreateRequest(BaseModel):
    name: str
    value: str


class SecretResponse(BaseModel):
    id: str
    name: str


@router.post("/secrets", response_model=SecretResponse)
async def create_secret_route(payload: SecretCreateRequest):
    """
    创建一个新的 Secret：
    - 请求体中包含明文 value
    - 返回时只回传 id 和 name，不回传 value
    """
    secret = create_secret(payload.name, payload.value)
    return SecretResponse(id=secret.id, name=secret.name)


@router.get("/secrets", response_model=List[SecretResponse])
async def list_secrets_route():
    """列出当前进程内所有可用的 Secret（仅 id + name）"""
    return [SecretResponse(id=s.id, name=s.name) for s in list_secrets()]


@router.delete("/secrets/{secret_id}", status_code=204)
async def delete_secret_route(secret_id: str):
    """删除指定 Secret"""
    try:
        delete_secret(secret_id)
    except SecretNotFoundError:
        raise HTTPException(status_code=404, detail="Secret not found")

    return None

