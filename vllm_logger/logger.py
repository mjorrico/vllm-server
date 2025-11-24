import os
import time
import uuid
import random
from datetime import datetime
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def connect():
    user = os.environ.get("POSTGRES_USER", "postgres")
    password = os.environ.get("POSTGRES_PASSWORD", "")
    db = os.environ.get("POSTGRES_DB", "postgres")
    host = os.environ.get("POSTGRES_HOST", "postgredb")
    port = os.environ.get("POSTGRES_PORT", "5432")
    dsn = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    conn = psycopg2.connect(dsn)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    return conn

def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS vllm_log (id uuid PRIMARY KEY, time time NOT NULL, value double precision NOT NULL)"
        )
        cur.execute(
            "ALTER TABLE vllm_log ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT NOW()"
        )

def insert_random(conn):
    with conn.cursor() as cur:
        now = datetime.now().time()
        value = random.random()
        cur.execute(
            "INSERT INTO vllm_log (id, time, value) VALUES (%s, %s, %s)",
            (uuid.uuid4(), now, value),
        )

def run():
    conn = connect()
    ensure_table(conn)
    while True:
        try:
            insert_random(conn)
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            conn = connect()
            ensure_table(conn)
        time.sleep(30)

if __name__ == "__main__":
    run()
