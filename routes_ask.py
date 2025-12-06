import asyncio
import os
from typing import Optional

from fastapi import APIRouter
from openai import OpenAI

router = APIRouter()


def process_thinking(resp: str):
    think, output = resp.split("</think>", 1)
    return output.strip(), think.lstrip("</think>").strip()


_ai_client: Optional[OpenAI] = None


def _get_ai_client() -> OpenAI:
    """
    懒加载 OpenAI 客户端：
    - 优先使用 ASK_API_KEY / ASK_BASE_URL
    - 其次回退到 LLM_API_KEY / LLM_BASE_URL
    """
    global _ai_client
    if _ai_client is not None:
        return _ai_client

    api_key = os.getenv("ASK_API_KEY") or os.getenv("LLM_API_KEY")
    if not api_key:
        raise RuntimeError("缺少环境变量 ASK_API_KEY 或 LLM_API_KEY，无法调用 /ask 接口使用的大模型")

    base_url = (
        os.getenv("ASK_BASE_URL")
        or os.getenv("LLM_BASE_URL")
        or "https://api.siliconflow.cn/v1"
    )

    _ai_client = OpenAI(api_key=api_key, base_url=base_url)
    return _ai_client


async def resp(q: str):
    loop = asyncio.get_event_loop()

    def call_openai():
        client = _get_ai_client()
        r = client.chat.completions.create(
            model="deepseek-ai/DeepSeek-R1-Distill-Qwen-7B",
            messages=[{"role": "user", "content": f"{q}"}],
            stream=False,
        )
        content = r.choices[0].message.content
        if content is None:
            return ""
        if "</think>" in content:
            return process_thinking(content)[1]
        return content

    return await loop.run_in_executor(None, call_openai)


@router.get("/ask")
async def ask(question: str):
    answer = await resp(question)
    return {"answer": answer}

