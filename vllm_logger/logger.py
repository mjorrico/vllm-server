import asyncio
import os
import sys
import uuid
import asyncpg
import pynvml
from datetime import datetime

# Configuration
DSN = os.getenv("VLLM_LOGGER_DB_URI")
LOG_INTERVAL = 5
CLEANUP_INTERVAL = 86400  # 24 Hours
RETENTION_DAYS = 31


async def ensure_schema(pool):
    """Initializes the database schema."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS vllm_log (
                id uuid PRIMARY KEY,
                gpu_index integer NOT NULL,
                gpu_utilization double precision NOT NULL,
                memory_used bigint NOT NULL,
                memory_total bigint NOT NULL,
                temperature double precision,
                created_at timestamptz NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_vllm_log_created_at 
            ON vllm_log(created_at);
        """
        )
        print("[INFO] Schema ensured.", flush=True)


async def task_cleanup(pool):
    """Runs once every 24 hours to delete old logs."""
    print(f"[TASK] Cleanup task started (Interval: {CLEANUP_INTERVAL}s)", flush=True)
    while True:
        try:
            # asyncpg uses $1, $2 syntax, not %s
            result = await pool.execute(
                f"DELETE FROM vllm_log WHERE created_at < NOW() - INTERVAL '{RETENTION_DAYS} days'"
            )
            # result string looks like "DELETE 150"
            print(
                f"[CLEANUP] {result} (Records older than {RETENTION_DAYS} days)",
                flush=True,
            )
        except Exception as e:
            print(f"[ERROR] Cleanup failed: {e}", flush=True)

        # Sleep for 24 hours (non-blocking)
        await asyncio.sleep(CLEANUP_INTERVAL)


async def task_logger(pool):
    """Runs every 5 seconds to log GPU stats."""
    print(f"[TASK] Logger task started (Interval: {LOG_INTERVAL}s)", flush=True)
    while True:
        try:
            # 1. Get GPU Data (Synchronous but fast)
            # If NVML is slow, we could wrap this in run_in_executor,
            # but usually it's sub-millisecond.
            device_count = pynvml.nvmlDeviceGetCount()
            data_batch = []

            for i in range(device_count):
                handle = pynvml.nvmlDeviceGetHandleByIndex(i)
                util = pynvml.nvmlDeviceGetUtilizationRates(handle).gpu
                mem = pynvml.nvmlDeviceGetMemoryInfo(handle)
                try:
                    temp = pynvml.nvmlDeviceGetTemperature(
                        handle, pynvml.NVML_TEMPERATURE_GPU
                    )
                except pynvml.NVMLError:
                    temp = None

                # Prepare tuple for bulk insert
                data_batch.append(
                    (uuid.uuid4(), i, float(util), mem.used, mem.total, temp)
                )

            # 2. Insert Data (Async)
            async with pool.acquire() as conn:
                await conn.executemany(
                    """
                    INSERT INTO vllm_log 
                    (id, gpu_index, gpu_utilization, memory_used, memory_total, temperature) 
                    VALUES ($1, $2, $3, $4, $5, $6)
                """,
                    data_batch,
                )

            # Optional: Verbose logging
            # print(f"[LOG] Inserted metrics for {device_count} GPUs", flush=True)

        except Exception as e:
            print(f"[ERROR] Logging failed: {e}", flush=True)
            # If DB is down, wait a bit longer before retrying to avoid log spam
            await asyncio.sleep(5)

        # Sleep for 5 seconds (non-blocking)
        await asyncio.sleep(LOG_INTERVAL)


async def main():
    print("[START] Starting Async VLLM Logger...", flush=True)

    # 1. Init NVML
    try:
        pynvml.nvmlInit()
    except pynvml.NVMLError as e:
        print(f"[FATAL] NVML Init failed: {e}")
        sys.exit(1)

    # 2. Init DB Connection Pool
    # We use a pool so multiple tasks can talk to DB if needed
    try:
        pool = await asyncpg.create_pool(DSN)
        await ensure_schema(pool)
    except Exception as e:
        print(f"[FATAL] DB Connection failed: {e}")
        sys.exit(1)

    # 3. Run Tasks Concurrently
    try:
        await asyncio.gather(task_logger(pool), task_cleanup(pool))
    except asyncio.CancelledError:
        print("[STOP] Tasks cancelled.")
    finally:
        await pool.close()
        pynvml.nvmlShutdown()
        print("[STOP] Shutdown complete.")


if __name__ == "__main__":
    try:
        # standard python asyncio entry point
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
