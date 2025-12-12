import asyncio
import os
import sys
import uuid
import asyncpg
import pynvml
from datetime import datetime

# Configuration
DSN = os.getenv("VLLM_LOGGER_DB_URI")
LOG_INTERVAL = 1
CLEANUP_INTERVAL = 60 * 60 * 24  # 24 Hours
RETENTION_SECONDS = 31 * 24 * 60 * 60


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
                f"DELETE FROM vllm_log WHERE created_at < NOW() - INTERVAL '{RETENTION_SECONDS} seconds'"
            )
            # result string looks like "DELETE 150"
            print(
                f"[CLEANUP] {result} (Records older than {RETENTION_SECONDS} seconds)",
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


async def populate_test_data_30_days(pool):
    """
    Testing function to insert logs into vllm_log table with 1 second delta for 30 days.
    This is for debugging purposes to quickly populate the table.
    """
    import random
    from datetime import timedelta

    print("[TEST] Starting to populate 30 days of test data...", flush=True)

    # Configuration
    NUM_GPUS = 4
    DAYS = 30
    TOTAL_SECONDS = DAYS * 24 * 60 * 60  # 2,592,000 seconds
    BATCH_SIZE = 10000  # Insert in batches for better performance

    # Calculate end time (now) and start time (30 days ago)
    end_time = datetime.now()
    start_time = end_time - timedelta(days=DAYS)

    print(f"[TEST] Generating data from {start_time} to {end_time}", flush=True)
    print(f"[TEST] Total records to insert: {TOTAL_SECONDS * NUM_GPUS:,}", flush=True)

    batch = []
    records_inserted = 0

    try:
        async with pool.acquire() as conn:
            for second_offset in range(TOTAL_SECONDS):
                current_time = start_time + timedelta(seconds=second_offset)

                # Generate data for each GPU
                for gpu_index in range(NUM_GPUS):
                    # Generate realistic GPU metrics
                    gpu_utilization = random.uniform(20.0, 95.0)
                    memory_total = 24 * 1024 * 1024 * 1024  # 24 GB
                    memory_used = int(memory_total * random.uniform(0.3, 0.9))
                    temperature = random.uniform(45.0, 82.0)

                    batch.append(
                        (
                            uuid.uuid4(),
                            gpu_index,
                            gpu_utilization,
                            memory_used,
                            memory_total,
                            temperature,
                            current_time,
                        )
                    )

                # Insert batch when it reaches BATCH_SIZE
                if len(batch) >= BATCH_SIZE:
                    await conn.executemany(
                        """
                        INSERT INTO vllm_log 
                        (id, gpu_index, gpu_utilization, memory_used, memory_total, temperature, created_at) 
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        """,
                        batch,
                    )
                    records_inserted += len(batch)
                    progress = (second_offset / TOTAL_SECONDS) * 100
                    print(
                        f"[TEST] Progress: {progress:.1f}% ({records_inserted:,} records)",
                        flush=True,
                    )
                    batch = []

            # Insert remaining records
            if batch:
                await conn.executemany(
                    """
                    INSERT INTO vllm_log 
                    (id, gpu_index, gpu_utilization, memory_used, memory_total, temperature, created_at) 
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    """,
                    batch,
                )
                records_inserted += len(batch)

        print(f"[TEST] Successfully inserted {records_inserted:,} records!", flush=True)

        await asyncio.sleep(50000)

    except Exception as e:
        print(f"[ERROR] Test data population failed: {e}", flush=True)
        raise


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
        # await asyncio.gather(task_logger(pool), task_cleanup(pool))
        await populate_test_data_30_days(pool)
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
