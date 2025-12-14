import asyncio
import os
import sys
import httpx
import pynvml
from datetime import datetime

# Configuration
DSN = os.getenv("VLLM_LOGGER_DB_URI")  # Format: http://host:port
USER = os.getenv("CLICKHOUSE_USER", "default")
PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
LOG_INTERVAL = 1


async def task_logger(client):
    """Runs every 1 second to log GPU stats."""
    print(f"[TASK] Logger task started (Interval: {LOG_INTERVAL}s)", flush=True)
    
    # Headers for auth
    headers = {
        "X-ClickHouse-User": USER,
        "X-ClickHouse-Key": PASSWORD,
    }

    while True:
        try:
            # 1. Get GPU Data (Synchronous but fast)
            device_count = pynvml.nvmlDeviceGetCount()
            data_batch = []
            
            # ClickHouse formatting for DateTime64(3): 'YYYY-MM-DD HH:MM:SS.mmm'
            # But standard ISO format usually works if parsed correctly. 
            # Safest is to use 'YYYY-MM-DD HH:MM:SS.mmm' string or let clickhouse parse standard string.
            current_time = datetime.now()
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
                # Note: ClickHouse VALUES format expects (val1, val2, ...), (val1, val2, ...)
                # Strings must be quoted.
                temp_val = str(temp) if temp != "NULL" else "NULL"
                data_batch.append(
                    f"('{time_str}', {i}, {float(util)}, {mem.used}, {mem.total}, {temp_val})"
                )

            if not data_batch:
                await asyncio.sleep(LOG_INTERVAL)
                continue

            # 2. Insert Data (Async HTTP)
            # Query: INSERT INTO vllm_logger.vllm_log (cols) VALUES (vals)
            values_str = ",".join(data_batch)
            query = f"""
                INSERT INTO vllm_logger.vllm_log 
                (created_at, gpu_index, gpu_utilization, memory_used, memory_total, temperature) 
                VALUES {values_str}
            """

            response = await client.post(
                DSN,
                data=query.encode('utf-8'),
                headers=headers
            )
            
            if response.status_code != 200:
                print(f"[ERROR] Insert failed {response.status_code}: {response.text}", flush=True)
            
            # Optional: Verbose logging
            # print(f"[LOG] Inserted metrics for {device_count} GPUs", flush=True)

        except Exception as e:
            print(f"[ERROR] Logging failed: {e}", flush=True)
            # If DB is down, wait a bit longer before retrying to avoid log spam
            await asyncio.sleep(5)

        # Sleep for LOG_INTERVAL (non-blocking)
        await asyncio.sleep(LOG_INTERVAL)


async def main():
    print("[START] Starting Async VLLM Logger (ClickHouse REST)...", flush=True)

    # 1. Init NVML
    try:
        pynvml.nvmlInit()
    except pynvml.NVMLError as e:
        print(f"[FATAL] NVML Init failed: {e}")
        sys.exit(1)

    # 2. Init HTTP Client
    # We use a persistent client for connection pooling
    async with httpx.AsyncClient() as client:
        # 3. Run Logger Task
        try:
            await task_logger(client)
        except asyncio.CancelledError:
            print("[STOP] Tasks cancelled.")
        finally:
            pynvml.nvmlShutdown()
            print("[STOP] Shutdown complete.")


if __name__ == "__main__":
    try:
        # standard python asyncio entry point
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
