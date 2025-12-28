from fastapi import APIRouter
from app.modules.ClickhouseDB.main import ClickhouseDBClient
import os
from datetime import datetime
import httpx

CLICKHOUSE_DB_VLLM_LOGGER = os.getenv("CLICKHOUSE_DB_VLLM_LOGGER")
router = APIRouter(prefix="/metrics", tags=["metrics"])


@router.get("/gpu")
async def get_metrics():
    db = ClickhouseDBClient(
        host="clickhouse",
        port=8123,
    )
    print(f"Querying from database: {CLICKHOUSE_DB_VLLM_LOGGER}", flush=True)
    # queries to be run
    query = f"""SELECT 
    toDate(created_at) AS day, 
    round(avg(gpu_utilization), 2) AS avg_gpu_util_percent, 
    round(avg(memory_used / memory_total) * 100, 2) AS avg_mem_util_percent
FROM {CLICKHOUSE_DB_VLLM_LOGGER}.vllm_log
GROUP BY day
ORDER BY day DESC"""
    result_historical = await db.run_query(query)

    # Query the latest entry for status checking
    query_latest = f"""SELECT 
    created_at,
    gpu_utilization, 
    memory_used, 
    memory_total, 
    temperature
FROM {CLICKHOUSE_DB_VLLM_LOGGER}.vllm_log
ORDER BY created_at DESC 
LIMIT 1"""

    result_latest = await db.run_query(query_latest)

    latest_data = {}
    is_online = False

    if result_latest and "data" in result_latest and result_latest["data"]:
        latest_entry = result_latest["data"][0]

        created_at_str = latest_entry.get("created_at")
        if created_at_str:
            try:
                # The logger inserts "%Y-%m-%d %H:%M:%S.%f" (truncated to 3 decimal places)
                created_at = datetime.strptime(created_at_str, "%Y-%m-%d %H:%M:%S.%f")
            except ValueError:
                # Fallback for seconds precision if microseconds are missing
                created_at = datetime.strptime(created_at_str, "%Y-%m-%d %H:%M:%S")

            # Check if within last 1 minute
            if (datetime.utcnow() - created_at).total_seconds() < 60:
                is_online = True

        latest_data = latest_entry

    model_id = None
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get("http://vllm:9009/v1/models")
            resp.raise_for_status()
            model_id = resp.json()["data"][0]["id"]
    except Exception as e:
        print(f"Failed to fetch model ID: {e}", flush=True)

    metrics_information = {
        "gpu": "NVIDIA RTX 2060",
        "llm": model_id,
        "historical": result_historical["data"],
        "gpu_load": latest_data.get("gpu_utilization", 0),
        "memory_usage": round(latest_data.get("memory_used", 0) / 1024**3, 2),
        "memory_total": round(latest_data.get("memory_total", 0) / 1024**3, 2),
        "temperature": latest_data.get("temperature", 0),
        "status": "online" if is_online else "offline",
    }
    return metrics_information
