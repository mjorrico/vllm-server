import asyncio
import os
import sys
import pynvml
import random
from datetime import datetime, timedelta
from urllib.parse import urlparse

from ClickhouseDB import ClickhouseDBClient

# Configuration
USER = os.getenv("CLICKHOUSE_USER", "default")
PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
DB_NAME = os.getenv("CLICKHOUSE_DB_VLLM_LOGGER", "default")
LOG_INTERVAL = 1


def get_db_client():
    return ClickhouseDBClient(
        "clickhouse",
        8123,
        user=USER,
        password=PASSWORD,
        database=DB_NAME,
    )


async def task_logger(db):
    """Runs every 1 second to log GPU stats."""
    print(f"[TASK] Logger task started (Interval: {LOG_INTERVAL}s)", flush=True)

    while True:
        try:
            # 1. Get GPU Data (Synchronous but fast)
            device_count = pynvml.nvmlDeviceGetCount()
            data_batch = []

            # ClickHouse formatting for DateTime64(3): 'YYYY-MM-DD HH:MM:SS.mmm'
            current_time = datetime.utcnow()
            # Format time explicitly to ensure compatibility
            time_str = current_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            for i in range(device_count):
                handle = pynvml.nvmlDeviceGetHandleByIndex(i)
                util = pynvml.nvmlDeviceGetUtilizationRates(handle).gpu
                mem = pynvml.nvmlDeviceGetMemoryInfo(handle)
                try:
                    temp = pynvml.nvmlDeviceGetTemperature(
                        handle, pynvml.NVML_TEMPERATURE_GPU
                    )
                except pynvml.NVMLError:
                    temp = "NULL"  # ClickHouse Nullable handling

                # Prepare value string: (created_at, gpu_index, gpu_utilization, memory_used, memory_total, temperature)
                temp_val = str(temp) if temp != "NULL" else "NULL"
                data_batch.append(
                    f"('{time_str}', {i}, {float(util)}, {mem.used}, {mem.total}, {temp_val})"
                )

            if not data_batch:
                await asyncio.sleep(LOG_INTERVAL)
                continue

            # 2. Insert Data (Async via ClickhouseDB driver)
            values_str = ",".join(data_batch)
            query = f"""
                INSERT INTO vllm_logger.vllm_log 
                (created_at, gpu_index, gpu_utilization, memory_used, memory_total, temperature) 
                VALUES {values_str}
            """

            response = await db.run_query(query)

            # Check for error string in response (since run_query returns error string or dict/None)
            if isinstance(response, str) and response.startswith("ClickHouse Error:"):
                print(f"[ERROR] Insert failed: {response}", flush=True)

            # Optional: Verbose logging
            # print(f"[LOG] Inserted metrics for {device_count} GPUs", flush=True)

        except Exception as e:
            print(f"[ERROR] Logging failed: {e}", flush=True)
            # If DB is down, wait a bit longer before retrying to avoid log spam
            await asyncio.sleep(5)

        # Sleep for LOG_INTERVAL (non-blocking)
        await asyncio.sleep(LOG_INTERVAL)


async def insert_dummy_data(db):
    """Inserts 31 days of dummy metrics for 4 GPUs (1-second intervals)."""
    start_time = datetime.utcnow() - timedelta(days=31)
    batch, total = [], 0
    RETENTION_SECONDS = 31 * 24 * 60 * 60
    TOTAL_RECORDS = RETENTION_SECONDS * 4

    print(f"Inserting records: 0/{TOTAL_RECORDS} (0%)", flush=True)

    try:
        for sec in range(RETENTION_SECONDS):
            ts = (start_time + timedelta(seconds=sec)).strftime("%Y-%m-%d %H:%M:%S.%f")[
                :-3
            ]
            for gpu in range(4):
                batch.append(
                    f"('{ts}', {gpu}, {round(random.uniform(20, 95), 2)}, "
                    f"{random.randint(2_000_000_000, 22_000_000_000)}, 24000000000, {random.randint(45, 85)})"
                )

            if len(batch) >= 10000 or sec == RETENTION_SECONDS - 1:
                query = f"INSERT INTO vllm_logger.vllm_log (created_at, gpu_index, gpu_utilization, memory_used, memory_total, temperature) VALUES {','.join(batch)}"

                # Retry mechanism
                for attempt in range(3):
                    resp = await db.run_query(query)

                    if isinstance(resp, str) and resp.startswith("ClickHouse Error:"):
                        print(
                            f"\n[WARN] Insert failed (Attempt {attempt+1}/3): {resp}",
                            flush=True,
                        )
                        await asyncio.sleep(1)  # Wait before retry
                    else:
                        # Success
                        total += len(batch)
                        percentage = (total / TOTAL_RECORDS) * 100
                        print(
                            f"Inserting records: {total}/{TOTAL_RECORDS} ({percentage:.2f}%)",
                            flush=True,
                        )
                        break
                else:
                    print(
                        f"\n[ERROR] Failed to insert batch of {len(batch)} records after 3 attempts.",
                        flush=True,
                    )

                batch = []

        if total == TOTAL_RECORDS:
            print(f"[SUCCESS] Inserted all {total:,} records", flush=True)
        else:
            print(
                f"[WARNING] Finished with {total:,}/{TOTAL_RECORDS} records ({TOTAL_RECORDS - total} missing)",
                flush=True,
            )

        await asyncio.sleep(31 * 24 * 60 * 60)
    except Exception as e:
        print(f"[ERROR] Logic failed: {e} (inserted {total:,})", flush=True)


async def main():
    print("[START] Starting Async VLLM Logger (ClickHouse Driver)...", flush=True)

    # 1. Init NVML
    try:
        pynvml.nvmlInit()
    except pynvml.NVMLError as e:
        print(f"[FATAL] NVML Init failed: {e}")
        # sys.exit(1) # Commented out for dev environment if no GPU

    # 2. Init DB Client
    db = get_db_client()

    # 3. Run Logger Task
    try:
        # await task_logger(db)
        await insert_dummy_data(db)
    except asyncio.CancelledError:
        print("[STOP] Tasks cancelled.")
    finally:
        try:
            pynvml.nvmlShutdown()
        except:
            pass
        print("[STOP] Shutdown complete.")


if __name__ == "__main__":
    try:
        # standard python asyncio entry point
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
