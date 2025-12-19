from fastapi import FastAPI
from .routers import (
    test_router,
    metrics,
)


app = FastAPI()

app.include_router(test_router.router, prefix="/api/v1")
app.include_router(metrics.router, prefix="/api/v1")


@app.get("/health")
async def health_check():
    return {"status": "healthy"}
