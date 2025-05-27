from .health import router as health_router
from .query import router as query_router
from .metrics import router as metrics_router
from .files import router as files_router

__all__ = ['health_router', 'query_router', 'metrics_router', 'files_router']
