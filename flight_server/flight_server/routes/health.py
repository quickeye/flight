from fastapi import APIRouter, HTTPException
from typing import Dict
import psutil
import platform
import datetime

router = APIRouter()

@router.get("/health", tags=["health"])
async def health_check() -> Dict[str, str]:
    """
    Get server health status
    """
    try:
        # Get system information
        system_info = {
            "status": "healthy",
            "timestamp": datetime.datetime.now().isoformat(),
            "system": platform.system(),
            "python_version": platform.python_version(),
            "memory_usage": f"{psutil.Process().memory_info().rss / (1024 * 1024):.2f} MB",
            "cpu_count": str(psutil.cpu_count()),
            "load_avg": ", ".join(map(str, psutil.getloadavg()))
        }
        return system_info
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))
