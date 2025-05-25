from fastapi import APIRouter
from fastapi.responses import Response
from prometheus_client import generate_latest, REGISTRY

router = APIRouter()

@router.get("/metrics", tags=["metrics"])
async def get_metrics():
    """
    Get Prometheus metrics
    """
    metrics = generate_latest(REGISTRY)
    return Response(content=metrics, media_type="text/plain")
