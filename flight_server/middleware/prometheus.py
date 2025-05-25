from prometheus_client import Counter, Histogram, Gauge
import psutil
import time
from starlette.requests import Request
from starlette.middleware.base import BaseHTTPMiddleware

# Prometheus metrics
REQUEST_COUNT = Counter(
    "http_requests_total", "Total HTTP requests",
    ["method", "endpoint", "http_status"]
)

REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds", "Request latency",
    ["method", "endpoint"]
)

REQUEST_SIZE = Histogram(
    "http_request_size_bytes", "Request size in bytes",
    ["method", "endpoint"]
)

RESPONSE_SIZE = Histogram(
    "http_response_size_bytes", "Response size in bytes",
    ["method", "endpoint"]
)

HTTP_STATUS_ERRORS = Counter(
    "http_status_errors_total", "Count of 4xx and 5xx errors",
    ["status_code"]
)

MEMORY_USAGE = Gauge(
    "flight_memory_usage_bytes", "Resident memory usage"
)

QUEUE_DEPTH = Gauge(
    "flight_queue_depth", "Pending executor queue size"
)

ACTIVE_THREADS = Gauge(
    "flight_active_threads", "Active executor threads"
)

class PrometheusMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        method = request.method
        endpoint = request.url.path
        content_length = request.headers.get("content-length")
        size = int(content_length) if content_length and content_length.isdigit() else 0
        REQUEST_SIZE.labels(method, endpoint).observe(size)

        response = await call_next(request)
        duration = time.time() - start_time
        status_code = response.status_code

        REQUEST_COUNT.labels(method, endpoint, str(status_code)).inc()
        REQUEST_LATENCY.labels(method, endpoint).observe(duration)
        
        # For streaming responses, we can't measure body size directly
        if hasattr(response, 'body'):
            RESPONSE_SIZE.labels(method, endpoint).observe(len(response.body or b""))
        else:
            RESPONSE_SIZE.labels(method, endpoint).observe(0)  # Set to 0 for streaming responses
        
        if 400 <= status_code < 600:
            HTTP_STATUS_ERRORS.labels(str(status_code)).inc()

        # Get executor from app state
        executor = request.app.state.executor
        
        MEMORY_USAGE.set(psutil.Process().memory_info().rss)
        if executor:
            QUEUE_DEPTH.set(executor._work_queue.qsize())
            ACTIVE_THREADS.set(len(executor._threads))
        else:
            QUEUE_DEPTH.set(0)
            ACTIVE_THREADS.set(0)

        return response
