import asyncio
import logging
import os
import signal
from pathlib import Path

from mq_worker import WorkflowMQWorker


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
            continue
        value = value.strip().strip('"').strip("'")
        os.environ[key] = value


def setup_logging() -> None:
    level_name = os.getenv("WORKER_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=level,
            format="%(asctime)s %(levelname)s %(name)s:%(lineno)d %(message)s",
        )
    else:
        logging.getLogger().setLevel(level)


async def _run_worker() -> None:
    load_env_from_dotenv()
    setup_logging()
    logger = logging.getLogger(__name__)

    worker = WorkflowMQWorker()
    stop_event = asyncio.Event()

    def _signal_handler(sig: signal.Signals) -> None:
        logger.info("Received signal %s, stopping worker...", sig.name)
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_running_loop().add_signal_handler(sig, _signal_handler, sig)
        except NotImplementedError:
            # Windows/limited environments可能不支持 add_signal_handler
            pass

    logger.info("Worker service starting...")
    try:
        await worker.start()
        logger.info("Worker started; waiting for messages")
        await stop_event.wait()
    except Exception as exc:  # noqa: BLE001
        logger.exception("Worker crashed: %s", exc)
        raise
    finally:
        try:
            await worker.stop()
        finally:
            logger.info("Worker service stopped")


def main() -> None:
    try:
        asyncio.run(_run_worker())
    except KeyboardInterrupt:
        # 已在信号里处理，这里确保退出码干净
        pass


if __name__ == "__main__":
    main()
