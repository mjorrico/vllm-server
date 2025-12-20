from fastapi import APIRouter
from app.modules.ClickhouseDB.main import ClickhouseDBClient
import os
import httpx

CLICKHOUSE_DB_VLLM_LOGGER = os.getenv("CLICKHOUSE_DB_VLLM_LOGGER")
router = APIRouter()


@router.get("/metrics")
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

    model_id = "Unknown"
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
    }
    return metrics_information
