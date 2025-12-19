from fastapi import APIRouter
from app.modules.ClickhouseDB.main import ClickhouseDBClient

router = APIRouter()


@router.get("/metrics")
async def get_metrics():
    db = ClickhouseDBClient()
    # queries to be run
    query = """
        SELECT 
            toDate(created_at) AS day, 
            round(avg(gpu_utilization), 2) AS avg_gpu_util_percent, 
            round(avg(memory_used / memory_total) * 100, 2) AS avg_mem_util_percent
        FROM vllm_log
        GROUP BY day
        ORDER BY day DESC;
    """
    result = await db.run_query(query)
    return result
