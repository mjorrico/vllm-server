from fastapi import APIRouter
from app.modules.ClickhouseDB.main import ClickhouseDBClient
import os

CLICKHOUSE_DB_VLLM_LOGGER = os.getenv("CLICKHOUSE_DB_VLLM_LOGGER")
router = APIRouter()


@router.get("/metrics")
async def get_metrics():
    db = ClickhouseDBClient(
        host="clickhouse",
        port=8123,
    )
    # queries to be run
    query = f"""SELECT 
    toDate(created_at) AS day, 
    round(avg(gpu_utilization), 2) AS avg_gpu_util_percent, 
    round(avg(memory_used / memory_total) * 100, 2) AS avg_mem_util_percent
FROM {CLICKHOUSE_DB_VLLM_LOGGER}.vllm_log
GROUP BY day
ORDER BY day DESC"""
    result = await db.run_query(query)
    return result
