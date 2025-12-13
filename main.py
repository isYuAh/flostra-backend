import os
from pathlib import Path

import fastapi
from fastapi.middleware.cors import CORSMiddleware
from db import init_db


def load_env_from_dotenv() -> None:
    """
    从当前目录的 .env 文件加载环境变量：
    - 忽略不存在的文件
    - 忽略注释行和空行
    - 不覆盖已有的环境变量
    """
    base_dir = Path(__file__).resolve().parent
    env_path = base_dir / ".env"
    if not env_path.is_file():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        if key in os.environ:
            # 已有环境变量优先，避免覆盖外部注入的配置
            continue
        value = value.strip().strip('"').strip("'")
        os.environ[key] = value


# 在导入依赖环境变量的模块前预先加载 .env
load_env_from_dotenv()

from routes_ask import router as ask_router
from routes_nodes import router as nodes_router
from routes_secrets import router as secrets_router
from routes_plugins import router as plugins_router
from routes_workflow import router as workflow_router
from routes_workflow_store import router as workflow_store_router

app = fastapi.FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 模块化路由挂载
app.include_router(nodes_router)
app.include_router(ask_router)
app.include_router(workflow_router)
app.include_router(workflow_store_router)
app.include_router(secrets_router)
app.include_router(plugins_router)


@app.on_event("startup")
async def _startup() -> None:
    # 启动时检查/创建必要的数据表
    await init_db()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8441, reload=True)
