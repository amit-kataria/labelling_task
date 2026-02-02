from __future__ import annotations

from typing import Any

from labelling_task.utils.time_utils import now_ms


def success(data: Any, message: str = "request processed successfully") -> dict[str, Any]:
    return {"status": "success", "message": message, "data": data, "timestamp": now_ms()}


def failure(message: str) -> dict[str, Any]:
    return {"status": "failure", "message": message, "timestamp": now_ms()}
