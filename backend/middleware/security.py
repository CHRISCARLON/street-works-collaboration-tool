import re
import time
from collections import defaultdict, deque
from loguru import logger
from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.routing import Match
from urllib.parse import unquote

# TODO: Middleware needs improving this is just a basic implementation for now

client_requests = defaultdict(deque)
rate_limit_requests = 2
rate_limit_window = 30

sql_injection_patterns = [
    re.compile(
        r"(\b(union|select|insert|update|delete|drop|create|alter|exec|execute)\b)",
        re.IGNORECASE,
    ),
    re.compile(r"(--|#|\/\*|\*\/)", re.IGNORECASE),
    re.compile(
        r"^(create|select|insert|update|delete|drop|alter|union|exec|execute)$",
        re.IGNORECASE,
    ),
]

safe_patterns = {
    "project_id": re.compile(r"^PROJ_[A-Z0-9_]+$"),
    "atco_code": re.compile(r"^[0-9]{3,}[A-Za-z0-9]*$"),
    "general": re.compile(r"^[A-Za-z0-9_\-\s\.]+$"),
}


def validate_parameter(param_name: str, param_value: str) -> bool:
    """Validate parameters"""
    if not param_value or not isinstance(param_value, str):
        return False

    if len(param_value) > 100:
        return False

    for pattern in sql_injection_patterns:
        if pattern.search(param_value):
            logger.warning(f"Invalid parameter detected: {param_value}")
            return False

    if param_name in ["project_id"]:
        return safe_patterns["project_id"].match(param_value) is not None
    elif param_name in ["atco_code"]:
        return safe_patterns["atco_code"].match(param_value) is not None
    else:
        return safe_patterns["general"].match(param_value) is not None


async def security_middleware(request: Request, call_next):
    """Security middleware with rate limiting and SQL injection protection"""

    excluded_paths = ["/docs", "/redoc", "/openapi.json", "/health"]
    path = str(request.url.path)

    if path == "/" or any(path.startswith(excluded) for excluded in excluded_paths):
        return await call_next(request)

    client_ip = request.client.host if request.client else "unknown"
    now = time.time()
    client_queue = client_requests[client_ip]

    while client_queue and client_queue[0] <= now - rate_limit_window:
        client_queue.popleft()

    if len(client_queue) >= rate_limit_requests:
        logger.warning(f"Rate limited: {client_ip}")
        return JSONResponse(status_code=429, content={"error": "Rate limit exceeded"})

    client_queue.append(now)

    try:
        app = request.scope["app"]

        for route in app.router.routes:
            match, _ = route.matches(
                {"type": "http", "path": request.url.path, "method": request.method}
            )
            if match == Match.FULL:
                path_params = route.param_convertors
                path_values = route.path_regex.match(request.url.path)

                if path_values and path_params:
                    params = {}
                    for i, (param_name, convertor) in enumerate(path_params.items(), 1):
                        param_value = unquote(path_values.group(i))
                        params[param_name] = param_value

                    for param_name, param_value in params.items():
                        if not validate_parameter(param_name, str(param_value)):
                            return JSONResponse(
                                status_code=400,
                                content={
                                    "error": "Invalid parameter format",
                                    "message": f"Parameter '{param_name}' contains invalid characters or patterns",
                                },
                            )
                break
    except Exception as e:
        logger.error(f"Error parsing path params: {e}")
        pass

    response = await call_next(request)
    return response
