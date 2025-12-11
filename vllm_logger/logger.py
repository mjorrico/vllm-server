import os
import sys
import time
import uuid
import random
from datetime import datetime
import psycopg2
import psycopg2.extras
import pynvml

# Register UUID adapter for psycopg2
psycopg2.extras.register_uuid()


def connect():
    dsn = os.getenv("VLLM_LOGGER_DB_URI")
    # print(f"[INFO] Connecting to database: {dsn}", flush=True)
    conn = psycopg2.connect(dsn)
    conn.autocommit = False  # Explicit transaction control
    print("[INFO] Database connection successful", flush=True)
    return conn


def ensure_table(conn):
    print("[INFO] Ensuring table exists...", flush=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """CREATE TABLE IF NOT EXISTS vllm_log (
                    id uuid PRIMARY KEY,
                    gpu_index integer NOT NULL,
                    gpu_utilization double precision NOT NULL,
                    memory_used bigint NOT NULL,
                    memory_total bigint NOT NULL,
                    temperature double precision,
                    created_at timestamptz NOT NULL DEFAULT NOW()
                )"""
            )
        conn.commit()  # CRITICAL: Commit the schema changes
        print("[INFO] Table ensured successfully", flush=True)
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Failed to ensure table: {e}", flush=True)
        raise


def insert_gpu_metrics(conn):
    try:
        pynvml.nvmlInit()
        device_count = pynvml.nvmlDeviceGetCount()

        with conn.cursor() as cur:
            for i in range(device_count):
                handle = pynvml.nvmlDeviceGetHandleByIndex(i)

                # Get GPU utilization
                utilization = pynvml.nvmlDeviceGetUtilizationRates(handle)
                gpu_util = utilization.gpu

                # Get memory info
                mem_info = pynvml.nvmlDeviceGetMemoryInfo(handle)
                memory_used = mem_info.used
                memory_total = mem_info.total

                # Get temperature (may not be available on all GPUs)
                try:
                    temperature = pynvml.nvmlDeviceGetTemperature(
                        handle, pynvml.NVML_TEMPERATURE_GPU
                    )
                except pynvml.NVMLError:
                    temperature = None

                cur.execute(
                    """INSERT INTO vllm_log (id, gpu_index, gpu_utilization, memory_used, memory_total, temperature) 
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    (
                        uuid.uuid4(),
                        i,
                        gpu_util,
                        memory_used,
                        memory_total,
                        temperature,
                    ),
                )

                # Convert bytes to MiB for easier comparison with nvidia-smi
                memory_used_mib = memory_used / (1024 * 1024)
                memory_total_mib = memory_total / (1024 * 1024)

                print(
                    f"[INFO] Inserted GPU {i}: util={gpu_util}%, "
                    f"mem={memory_used_mib:.0f}MiB/{memory_total_mib:.0f}MiB "
                    f"({memory_used}/{memory_total} bytes), temp={temperature}°C",
                    flush=True,
                )

        conn.commit()  # CRITICAL: Commit the insert
        pynvml.nvmlShutdown()
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Failed to insert GPU metrics: {e}", flush=True)
        raise


def run():
    print("[START] vllm_logger starting up...", flush=True)

    # Wait for database to be ready
    max_retries = 30
    for attempt in range(max_retries):
        try:
            conn = connect()
            break
        except Exception as e:
            print(
                f"[WARN] Database not ready (attempt {attempt+1}/{max_retries}): {e}",
                flush=True,
            )
            if attempt == max_retries - 1:
                print(
                    "[FATAL] Could not connect to database after maximum retries",
                    flush=True,
                )
                sys.exit(1)
            time.sleep(2)

    ensure_table(conn)
    INSERTION_FREQUENCY = 30  # seconds
    print(
        f"[INFO] Entering main loop - will insert data every {INSERTION_FREQUENCY} seconds",
        flush=True,
    )

    while True:
        try:
            insert_gpu_metrics(conn)
        except Exception as e:
            print(f"[ERROR] Exception in main loop: {e}", flush=True)
            print("[INFO] Reconnecting to database...", flush=True)
            try:
                conn.close()
            except Exception:
                pass

            # Attempt reconnection with retries
            for attempt in range(5):
                try:
                    conn = connect()
                    ensure_table(conn)
                    print("[INFO] Reconnection successful", flush=True)
                    break
                except Exception as reconnect_error:
                    print(
                        f"[WARN] Reconnection attempt {attempt+1} failed: {reconnect_error}",
                        flush=True,
                    )
                    if attempt == 4:
                        print("[FATAL] Could not reconnect to database", flush=True)
                        sys.exit(1)
                    time.sleep(5)

        time.sleep(INSERTION_FREQUENCY)


if __name__ == "__main__":
    run()
