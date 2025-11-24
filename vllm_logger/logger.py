import os
import time
import uuid
import random
from datetime import datetime
from urllib.parse import urlparse
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def ensure_database(dsn: str):
    u = urlparse(dsn)
    db = (u.path or "/postgres").lstrip("/")
    base = f"postgresql://{u.username}:{u.password}@{u.hostname}:{u.port}/postgres"
    with psycopg2.connect(base) as conn:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db,))
            if cur.fetchone() is None:
                cur.execute(sql.SQL("CREATE DATABASE {}" ).format(sql.Identifier(db)))

def run(dsn: str):
    while True:
        try:
            with psycopg2.connect(dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "CREATE TABLE IF NOT EXISTS vllm_log (id uuid PRIMARY KEY, time time NOT NULL, value double precision NOT NULL)"
                    )
                while True:
                    with conn.cursor() as cur:
                        cur.execute(
                            "INSERT INTO vllm_log (id, time, value) VALUES (%s, %s, %s)",
                            (str(uuid.uuid4()), datetime.now().time(), random.random()),
                        )
                        conn.commit()
                    time.sleep(60)
        except Exception as e:
            print(f"retrying: {e}")
            time.sleep(5)

if __name__ == "__main__":
    user = os.environ.get("POSTGRES_USER", "postgres")
    password = os.environ.get("POSTGRES_PASSWORD", "")
    db = os.environ.get("POSTGRES_DB", "postgres")
    host = "postgredb"
    port = os.environ.get("POSTGRES_PORT", "5432")
    dsn = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    try:
        ensure_database(dsn)
    except Exception as e:
        print(f"ensure_database failed: {e}")
    run(dsn)
