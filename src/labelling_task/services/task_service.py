from __future__ import annotations

import json
from datetime import datetime, timezone
from math import ceil
from typing import Any

import redis.asyncio as redis

from labelling_task.auth.models import Principal
from labelling_task.domain.entities.task import FilterClause, TaskCreateRequest, TaskDetailRequest, TaskListRequest
from labelling_task.errors import ForbiddenError
from labelling_task.configs.settings import get_settings
from labelling_task.repositories.task_repository import TaskRepository, dt_to_iso, oid_to_str
from labelling_task.configs.logging_config import get_logger
log = get_logger(__name__)

def _mongo_op(clause: FilterClause) -> dict[str, Any]:
    op = clause.operator
    val = clause.value
    if op == "eq":
        return val
    if op == "gte":
        return {"$gte": val}
    if op == "lte":
        return {"$lte": val}
    if op == "in":
        return {"$in": val if isinstance(val, list) else [val]}
    if op == "regex":
        return {"$regex": val}
    return val


def _parse_datetime(val: Any) -> Any:
    # API examples send "2024-12-31" (date) or ISO timestamp.
    if not isinstance(val, str):
        return val
    try:
        if len(val) == 10:
            dt = datetime.fromisoformat(val).replace(tzinfo=timezone.utc)
        else:
            dt = datetime.fromisoformat(val)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return val


def build_query(filters: dict[str, FilterClause]) -> dict[str, Any]:
    q: dict[str, Any] = {}
    for field, clause in filters.items():
        value = clause.value
        if field in {"created_on", "created_at"}:
            field = "created_at"
            value = _parse_datetime(value)
        if field in {"updated_on", "updated_at"}:
            field = "updated_at"
            value = _parse_datetime(value)
        q[field] = _mongo_op(FilterClause(operator=clause.operator, value=value))
    return q


def build_projection(fields: list[str] | None) -> dict[str, int] | None:
    if not fields:
        return None
    proj = {f: 1 for f in fields}
    # always keep external_id so UI can navigate
    proj["external_id"] = 1
    proj["tenant_id"] = 1
    proj["org"] = 1
    proj["status"] = 1
    proj["created_at"] = 1
    proj["updated_at"] = 1
    proj["allocated_to"] = 1
    proj["owner"] = 1
    proj["task_details"] = 1
    proj["created_by"] = 1
    proj["updated_by"] = 1
    return proj


def build_sort(sort_spec: list[dict[str, str]]) -> list[tuple[str, int]]:
    out: list[tuple[str, int]] = []
    for s in sort_spec or []:
        field = s.get("field")
        direction = s.get("direction", "asc").lower()
        if field in {"created_on", "created_at"}:
            field = "created_at"
        if field in {"updated_on", "updated_at"}:
            field = "updated_at"
        out.append((field, 1 if direction == "asc" else -1))
    if not out:
        out = [("created_at", -1)]
    return out


class TaskService:
    def __init__(self, repo: TaskRepository, redis_client: redis.Redis):
        self._repo = repo
        self._redis = redis_client

    async def create_task(self, principal: Principal, req: TaskCreateRequest) -> dict[str, Any]:
        if principal.role not in {"Admin", "Super Admin", "SuperAdmin"}:
            raise ForbiddenError("only admin can create tasks")

        now = datetime.now(timezone.utc)
        doc: dict[str, Any] = {
            "external_id": req.external_id,
            "tenant_id": principal.tenant_id,
            "org": req.org,
            "status": req.status,
            "owner": principal.user_id,
            "allocated_to": None,
            "task_details": req.task_details.model_dump(),
            "created_by": principal.user_id,
            "updated_by": principal.user_id,
            "created_at": now,
            "updated_at": now,
            "deleted_at": None,
        }

        _id = await self._repo.insert(doc)

        # Enqueue allocation event (Redis Streams).
        settings = get_settings()
        await self._redis.xadd(
            settings.redis_stream_tasks,
            {
                "event": "TASK_CREATED",
                "tenant_id": principal.tenant_id,
                "external_id": req.external_id,
                "org": req.org,
                "assignment": req.task_details.task_assignment_type,
                "workflow": req.task_details.workflow_type,
                "data_type": req.task_details.data_type,
                "created_by": principal.user_id,
            },
        )

        # Cache static-ish details (instructions/labels) for quick UI access.
        cache_key = f"lt:taskmeta:{principal.tenant_id}:{req.external_id}"
        await self._redis.setex(
            cache_key,
            3600,
            json.dumps(
                {
                    "external_id": req.external_id,
                    "instructions": req.task_details.instructions,
                    "labels": [l.model_dump() for l in req.task_details.labels],
                }
            ),
        )

        return {
            "id": _id,
            "external_id": req.external_id,
            "org": req.org,
            "status": req.status,
            "updated_by": principal.user_id,
            "created_by": principal.user_id,
            "created_on": dt_to_iso(now),
            "updated_on": dt_to_iso(now),
            "task_details": req.task_details.model_dump(),
        }

    async def list_tasks(self, principal: Principal, req: TaskListRequest) -> dict[str, Any]:
        query = build_query(req.filters)

        # Non-admins can only see their own tasks.
        if principal.role not in {"Admin", "Super Admin", "SuperAdmin"}:
            query["allocated_to"] = principal.user_id

        projection = build_projection(req.fields)
        sort = build_sort(req.sort)
        skip = max(req.page, 0) * max(req.size, 1)
        limit = max(min(req.size, 100), 1)

        items, total = await self._repo.list(
            tenant_id=principal.tenant_id,
            query=query,
            projection=projection,
            skip=skip,
            limit=limit,
            sort=sort,
        )

        tasks_out: list[dict[str, Any]] = []
        for it in items:
            it = oid_to_str(it)
            it["created_on"] = dt_to_iso(it.get("created_at"))
            it["updated_on"] = dt_to_iso(it.get("updated_at"))
            it.pop("created_at", None)
            it.pop("updated_at", None)
            it.pop("tenant_id", None)  # never leak tenant ids in payloads
            tasks_out.append(it)

        total_pages = ceil(total / limit) if limit else 0
        return {
            "tasks": tasks_out,
            "totalElements": total,
            "totalPages": total_pages,
            "currentPage": req.page,
        }

    async def get_task_detail(self, principal: Principal, req: TaskDetailRequest) -> dict[str, Any]:
        doc = await self._repo.get_by_external_id(tenant_id=principal.tenant_id, external_id=req.external_id)

        # Non-admins: only tasks allocated to them
        if principal.role not in {"Admin", "Super Admin", "SuperAdmin"} and doc.get("allocated_to") != principal.user_id:
            raise ForbiddenError("forbidden")

        doc = oid_to_str(doc)
        doc["created_on"] = dt_to_iso(doc.get("created_at"))
        doc["updated_on"] = dt_to_iso(doc.get("updated_at"))
        doc.pop("created_at", None)
        doc.pop("updated_at", None)
        doc.pop("tenant_id", None)
        return doc
