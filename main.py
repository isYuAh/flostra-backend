import os
import logging
from pathlib import Path

import fastapi
from fastapi.middleware.cors import CORSMiddleware
from db import init_db

if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s:%(lineno)d %(message)s",
    )

logger = logging.getLogger(__name__)


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
    # HTTP 进程仅负责 API；worker 由独立入口启动

    # 启动时导出静态 definitions（给 Java/SpringBoot 消费）；失败不影响服务启动
    try:
        from definitions_exporter import export_definitions_if_enabled
        logger.info("Startup: begin export definitions")
        exported = export_definitions_if_enabled(base_dir=Path(__file__).resolve().parent)
        if exported:
            logging.getLogger(__name__).info(
                "Exported definitions: %s",
                {k: str(v) for k, v in exported.items()},
            )
        logger.info("Startup: export definitions done")
    except Exception as exc:  # noqa: BLE001
        logging.getLogger(__name__).warning("Export definitions failed: %s", exc)

    # 启动时检查/创建必要的数据表
    logger.info("Startup: init_db begin")
    await init_db()
    logger.info("Startup: init_db done")


@app.on_event("shutdown")
async def _shutdown() -> None:
    logger.info("Shutdown: complete")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8441, reload=True)
