import os
import asyncio
import uuid
import random
from datetime import datetime, timedelta
from urllib.parse import urlparse
from zoneinfo import ZoneInfo
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def local_now():
    tz = os.environ.get("TZ")
    if tz:
        try:
            return datetime.now(ZoneInfo(tz))
        except Exception:
            return datetime.now().astimezone()
    return datetime.now().astimezone()


def ensure_database(dsn: str):
    u = urlparse(dsn)
    db = (u.path or "/postgres").lstrip("/")
    base = f"postgresql://{u.username}:{u.password}@{u.hostname}:{u.port}/postgres"
    with psycopg2.connect(base) as conn:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db,))
            if cur.fetchone() is None:
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db)))


def setup_schema(dsn: str):
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE IF NOT EXISTS vllm_log (id uuid PRIMARY KEY, time time NOT NULL, value double precision NOT NULL)"
            )
            cur.execute(
                "ALTER TABLE vllm_log ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT NOW()"
            )


def _write_once_blocking(dsn: str):
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO vllm_log (id, time, value, created_at) VALUES (%s, %s, %s, %s)",
                (str(uuid.uuid4()), local_now().time(), random.random(), local_now()),
            )
            conn.commit()


async def writer_loop(dsn: str):
    await asyncio.to_thread(setup_schema, dsn)
    while True:
        try:
            await asyncio.to_thread(_write_once_blocking, dsn)
            await asyncio.sleep(60)
        except Exception as e:
            print(f"retrying: {e}")
            await asyncio.sleep(5)


def _cleanup_once_blocking(dsn: str):
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM vllm_log WHERE created_at < NOW() - INTERVAL '60 days'"
            )
            conn.commit()


async def cleanup_loop(dsn: str):
    while True:
        try:
            await asyncio.to_thread(_cleanup_once_blocking, dsn)
            await asyncio.sleep(3600)
        except Exception as e:
            print(f"cleanup retrying: {e}")
            await asyncio.sleep(10)


async def main():
    user = os.environ.get("POSTGRES_USER", "postgres")
    password = os.environ.get("POSTGRES_PASSWORD", "")
    db = os.environ.get("POSTGRES_DB", "postgres")
    host = "postgredb"
    port = os.environ.get("POSTGRES_PORT", "5432")
    dsn = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    try:
        await asyncio.to_thread(ensure_database, dsn)
    except Exception as e:
        print(f"ensure_database failed: {e}")
    await asyncio.gather(
        cleanup_loop(dsn),
        writer_loop(dsn),
    )


if __name__ == "__main__":
    asyncio.run(main())
