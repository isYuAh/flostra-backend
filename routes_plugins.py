from fastapi import APIRouter

from llm_plugins import list_plugins_schema

router = APIRouter()


@router.get("/plugins")
async def list_plugins():
    """列出当前后端可用的 LLM 插件 schema 列表"""
    return {"plugins": list_plugins_schema()}

