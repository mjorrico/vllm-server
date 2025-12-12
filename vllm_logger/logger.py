import asyncio
import os
import sys
import asyncpg
import pynvml
from datetime import datetime

# Configuration
DSN = os.getenv("VLLM_LOGGER_DB_URI")
LOG_INTERVAL = 5
CLEANUP_INTERVAL = 60 * 60 * 24  # 24 Hours
RETENTION_SECONDS = 31 * 24 * 60 * 60

CLEANUP_INTERVAL = 16
RETENTION_SECONDS = 60


def get_partition_info(dt):
    """Generate partition name and bounds for a given datetime (minute granularity)."""
    from datetime import timedelta

    # Truncate to minute
    dt_minute = dt.replace(second=0, microsecond=0)
    partition_name = f"vllm_log_{dt_minute.strftime('%Y_%m_%d_%H_%M')}"

    # Partition covers [dt_minute, dt_minute + 1 minute)
    from_time = dt_minute
    to_time = dt_minute + timedelta(minutes=1)

    return partition_name, from_time, to_time


async def ensure_partition_exists(conn, dt):
    """Create a partition if it doesn't exist for the given datetime."""
    partition_name, from_time, to_time = get_partition_info(dt)

    try:
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {partition_name}
            PARTITION OF vllm_log
            FOR VALUES FROM ('{from_time.isoformat()}') TO ('{to_time.isoformat()}');
            """
        )
        print(f"[INFO] Ensured partition: {partition_name}", flush=True)
    except Exception as e:
        print(f"[ERROR] Failed to create partition {partition_name}: {e}", flush=True)
        raise


async def ensure_schema(pool):
    """Initializes the database schema and creates initial partitions."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS vllm_log (
                gpu_index integer NOT NULL,
                gpu_utilization double precision NOT NULL,
                memory_used bigint NOT NULL,
                memory_total bigint NOT NULL,
                temperature double precision,
                created_at timestamptz NOT NULL DEFAULT NOW(),
                PRIMARY KEY (gpu_index, created_at)
            ) PARTITION BY RANGE (created_at);
        """
        )
        print("[INFO] Schema ensured.", flush=True)

        # Create 3 partitions in advance (current + 2 future minutes)
        now = datetime.now()
        for i in range(3):
            from datetime import timedelta

            target_time = now + timedelta(minutes=i)
            await ensure_partition_exists(conn, target_time)

        print("[INFO] Initial partitions created.", flush=True)


async def task_cleanup(pool):
    """Manages partitions: creates 3 in advance and drops old ones."""
    print(f"[TASK] Cleanup task started (Interval: {CLEANUP_INTERVAL}s)", flush=True)
    while True:
        try:
            async with pool.acquire() as conn:
                # 1. Create 3 partitions in advance
                now = datetime.now()
                from datetime import timedelta

                for i in range(3):
                    target_time = now + timedelta(minutes=i)
                    await ensure_partition_exists(conn, target_time)

                # 2. Drop partitions older than retention period
                retention_threshold = now - timedelta(seconds=RETENTION_SECONDS)

                # Query for old partitions
                old_partitions = await conn.fetch(
                    """
                    SELECT tablename FROM pg_tables
                    WHERE schemaname = 'public'
                    AND tablename LIKE 'vllm_log_%'
                    AND tablename != 'vllm_log'
                    """
                )

                dropped_count = 0
                for partition_record in old_partitions:
                    partition_name = partition_record["tablename"]
                    # Parse partition name: vllm_log_YYYY_MM_DD_HH_MI
                    try:
                        parts = partition_name.split("_")
                        if len(parts) == 7:
                            year, month, day, hour, minute = (
                                int(parts[2]),
                                int(parts[3]),
                                int(parts[4]),
                                int(parts[5]),
                                int(parts[6]),
                            )
                            partition_time = datetime(year, month, day, hour, minute)

                            if partition_time < retention_threshold:
                                await conn.execute(
                                    f"DROP TABLE IF EXISTS {partition_name}"
                                )
                                print(
                                    f"[CLEANUP] Dropped partition: {partition_name}",
                                    flush=True,
                                )
                                dropped_count += 1
                    except (ValueError, IndexError) as e:
                        print(
                            f"[WARNING] Could not parse partition name {partition_name}: {e}",
                            flush=True,
                        )

                if dropped_count > 0:
                    print(
                        f"[CLEANUP] Dropped {dropped_count} old partitions (older than {RETENTION_SECONDS} seconds)",
                        flush=True,
                    )
                else:
                    print(f"[CLEANUP] No old partitions to drop", flush=True)

        except Exception as e:
            print(f"[ERROR] Cleanup failed: {e}", flush=True)

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
                data_batch.append((i, float(util), mem.used, mem.total, temp))

            # 2. Insert Data (Async)
            async with pool.acquire() as conn:
                # Ensure partition exists for current time
                await ensure_partition_exists(conn, datetime.now())

                await conn.executemany(
                    """
                    INSERT INTO vllm_log 
                    (gpu_index, gpu_utilization, memory_used, memory_total, temperature) 
                    VALUES ($1, $2, $3, $4, $5)
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
