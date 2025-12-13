from __future__ import annotations

import os
from typing import AsyncGenerator, Optional
from sqlalchemy.engine import make_url

import sqlalchemy as sa
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.exc import OperationalError

Base = declarative_base()


class Workspace(Base):
    __tablename__ = "workspace"

    id = sa.Column(UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()"))
    name = sa.Column(sa.Text, nullable=False)
    owner_id = sa.Column(sa.Text, nullable=False)
    created_at = sa.Column(sa.DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = sa.Column(sa.DateTime(timezone=True), server_default=func.now(), nullable=False)


class Workflow(Base):
    __tablename__ = "workflow"

    id = sa.Column(UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()"))
    workspace_id = sa.Column(UUID(as_uuid=True), sa.ForeignKey("workspace.id", ondelete="CASCADE"), nullable=False)
    name = sa.Column(sa.Text, nullable=False)
    description = sa.Column(sa.Text, nullable=True)
    definition = sa.Column(JSONB, nullable=False)
    created_at = sa.Column(sa.DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = sa.Column(sa.DateTime(timezone=True), server_default=func.now(), nullable=False)
    __table_args__ = (sa.Index("idx_workflow_workspace", "workspace_id"),)


class WorkspaceMember(Base):
    __tablename__ = "workspace_member"

    workspace_id = sa.Column(UUID(as_uuid=True), sa.ForeignKey("workspace.id", ondelete="CASCADE"), primary_key=True)
    user_id = sa.Column(sa.Text, primary_key=True)
    role = sa.Column(sa.Text, nullable=False)  # owner/admin/member/viewer
    created_at = sa.Column(sa.DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = sa.Column(sa.DateTime(timezone=True), server_default=func.now(), nullable=False)
    __table_args__ = (
        sa.Index("idx_workspace_member_user", "user_id"),
    )


_engine: Optional[AsyncEngine] = None
SessionLocal: Optional[async_sessionmaker[AsyncSession]] = None


def _build_db_url(db_name: Optional[str] = None) -> str:
    dsn = os.getenv("PG_DSN")
    if dsn:
        # 允许显式 DSN（需使用 asyncpg 驱动前缀）
        return dsn
    host = os.getenv("PG_HOST", "localhost")
    port = os.getenv("PG_PORT", "5432")
    user = os.getenv("PG_USER", "postgres")
    password = os.getenv("PG_PASSWORD", "")
    db = db_name or os.getenv("PG_DB", "postgres")
    return f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{db}"


def _fallback_db_name() -> Optional[str]:
    return os.getenv("PG_DB_FALLBACK", "postgres")


async def _create_database_with_urls(target_url: str, target_db: str) -> None:
    """
    兼容显式 PG_DSN：尝试切换到 fallback 库创建目标库。
    """
    fb_name = _fallback_db_name()
    if not fb_name:
        raise RuntimeError("目标数据库不存在，且未配置 PG_DB_FALLBACK，无法自动创建")

    url_obj = make_url(target_url)
    if url_obj.database == fb_name:
        raise RuntimeError("目标数据库不存在，且当前连接已指向 fallback 数据库，无法创建目标库")

    fallback_url = url_obj.set(database=fb_name)
    fb_engine = create_async_engine(str(fallback_url), isolation_level="AUTOCOMMIT", future=True)
    async with fb_engine.begin() as conn:
        try:
            await conn.execute(sa.text(f'CREATE DATABASE "{target_db}"'))
        except Exception as exc:  # noqa: BLE001
            # 如果库已存在或无权限，抛出友好错误
            raise RuntimeError(f"尝试创建数据库 {target_db} 失败：{exc}") from exc
    await fb_engine.dispose()


async def init_db() -> None:
    """
    启动时初始化数据库：
    - 若目标数据库不存在且允许 fallback，则自动创建
    - 确保 workspace/workflow 表存在
    """
    global _engine, SessionLocal

    target_db = os.getenv("PG_DB", "postgres")
    target_url = _build_db_url()
    target_host = os.getenv("PG_HOST", "localhost")
    target_port = os.getenv("PG_PORT", "5432")
    target_user = os.getenv("PG_USER", "postgres")

    def _make_engine(url: str, **kwargs) -> AsyncEngine:
        return create_async_engine(url, echo=False, future=True, **kwargs)

    def _is_missing_db(exc: Exception) -> bool:
        orig = getattr(exc, "orig", None)
        code = getattr(orig, "sqlstate", None) or getattr(orig, "pgcode", None)
        msg = str(exc)
        return (
            code == "3D000"
            or "InvalidCatalogName" in msg
            or ("does not exist" in msg and "database" in msg)
        )

    try:
        engine = _make_engine(target_url)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    except Exception as exc:  # noqa: BLE001
        if _is_missing_db(exc):
            await _create_database_with_urls(target_url, target_db)
            engine = _make_engine(target_url)
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
        else:
            raise RuntimeError(
                f"连接或初始化数据库失败，请检查 PG 配置（特别是账号密码/权限）：{exc}; "
                f"host={target_host} port={target_port} user={target_user} db={target_db}"
            ) from exc

    _engine = engine
    SessionLocal = async_sessionmaker(engine, expire_on_commit=False)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    if SessionLocal is None:
        raise RuntimeError("数据库尚未初始化，请先调用 init_db()")
    async with SessionLocal() as session:
        yield session
