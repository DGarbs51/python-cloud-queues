"""Load-test database access. Create tables explicitly with ``python -m db init``."""

from __future__ import annotations

import asyncio
import os
import ssl
import sys
from datetime import datetime
from functools import lru_cache

import certifi
from sqlalchemy import BigInteger, Integer, String, create_engine, delete, select, text
from sqlalchemy.dialects.mysql import DATETIME
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker


class Base(DeclarativeBase):
    pass


class LoadRow(Base):
    __tablename__ = "load_rows"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run: Mapped[str] = mapped_column(String(32), index=True)
    n: Mapped[int] = mapped_column(Integer)
    payload: Mapped[str] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(DATETIME(fsp=6), default=datetime.utcnow)


def database_url(driver: str) -> URL:
    raw = os.environ.get("DATABASE_URL")
    url = make_url(raw) if raw else URL.create(
        "mysql", username=os.environ.get("DB_USERNAME", "root"),
        password=os.environ.get("DB_PASSWORD", ""),
        host=os.environ.get("DB_HOST", "127.0.0.1"),
        port=int(os.environ.get("DB_PORT", "3306")),
        database=os.environ.get("DB_DATABASE", "python_cloud_queues"),
    )
    if url.get_backend_name() != "mysql":
        raise ValueError("DATABASE_URL must use MySQL")
    return url.set(drivername=f"mysql+{driver}")


def engine_options(url: URL, *, asynchronous: bool = False) -> dict:
    connect_args = {"connect_timeout": 5}
    local = url.host in {"localhost", "127.0.0.1", "::1"}
    if not local or os.environ.get("DB_SSL", "0") != "0":
        connect_args["ssl"] = (ssl.create_default_context(cafile=certifi.where())
                               if asynchronous else {"ca": certifi.where()})
    return dict(pool_size=1, max_overflow=0, pool_pre_ping=True, pool_recycle=280,
                connect_args=connect_args)


@lru_cache(maxsize=1)
def sync_engine():
    url = database_url("pymysql")
    return create_engine(url, **engine_options(url))


def sync_session():
    return sessionmaker(sync_engine(), expire_on_commit=False)()


def async_engine():
    # Each job owns its engine, so connections cannot escape to another event loop.
    url = database_url("aiomysql")
    return create_async_engine(url, **engine_options(url, asynchronous=True))


def ping() -> None:
    with sync_engine().connect() as connection:
        connection.execute(text("SELECT 1"))


def write_rows(run: str, rows: int) -> None:
    with sync_session() as session, session.begin():
        session.add_all(LoadRow(run=run, n=n, payload="x" * 255) for n in range(rows))


def read_rows(rows: int) -> None:
    with sync_session() as session:
        session.execute(select(LoadRow).limit(rows)).all()


def query_rows(rows: int) -> None:
    with sync_session() as session:
        for n in range(rows):
            session.execute(select(LoadRow.id).where(LoadRow.n == n).limit(1)).all()


async def query_rows_async(rows: int) -> None:
    engine = async_engine()
    try:
        async def query(n: int) -> None:
            async with AsyncSession(engine) as session:
                await session.execute(select(LoadRow.id).where(LoadRow.n == n).limit(1))

        # The pool intentionally permits one connection per worker; bound task allocation too.
        for start in range(0, rows, 10):
            results = await asyncio.gather(
                *(query(n) for n in range(start, min(start + 10, rows))), return_exceptions=True,
            )
            for result in results:
                if isinstance(result, BaseException):
                    raise result
    finally:
        await engine.dispose()


def cleanup(run: str) -> None:
    with sync_session() as session, session.begin():
        session.execute(delete(LoadRow).where(LoadRow.run == run))


if __name__ == "__main__":
    if sys.argv[1:] != ["init"]:
        raise SystemExit("usage: python -m db init")
    Base.metadata.create_all(sync_engine())
    print("Database schema ready")
