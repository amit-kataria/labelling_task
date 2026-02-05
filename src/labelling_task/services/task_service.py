import json
from datetime import datetime, timezone
from math import ceil
from typing import Any, List, Dict, Optional, Tuple, Union
import uuid
import asyncio
import redis.asyncio as redis

from labelling_task.domain.entities.task import (
    FilterClause,
    TaskCreateRequest,
    TaskDetailRequest,
    TaskListRequest,
    SortSpec,
    TaskActionRequest,
    TaskUpdateRequest,
    AnnotationItem,
    CommentItem,
)
from labelling_task.domain.entities.allocation import AllocationRequest
from labelling_task.errors import ForbiddenError
from labelling_task.services.allocation_service import AllocationService
from labelling_task.configs.settings import get_settings
from labelling_task.repositories.task_repository import TaskRepository, dt_to_iso, oid_to_str
from labelling_task.configs.logging_config import get_logger

log = get_logger(__name__)


def _mongo_op(clause: FilterClause) -> dict[str, Any]:
    op = clause.operator
    val = clause.value
    if op == "eq":
        return val
    if op == "ne":
        return {"$ne": val}
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
            field = "created_on"
            value = _parse_datetime(value)
        if field in {"updated_on", "updated_at"}:
            field = "updated_on"
            value = _parse_datetime(value)
        q[field] = _mongo_op(FilterClause(operator=clause.operator, value=value))
        log.debug(f"built_query: {q}")
    return q


def build_projection(fields: Optional[List[str]]) -> Optional[Dict[str, int]]:
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


def build_sort(sort_spec: Union[List[SortSpec], List[Dict[str, str]]]) -> List[Tuple[str, int]]:
    out: list[tuple[str, int]] = []
    for s in sort_spec or []:
        if isinstance(s, SortSpec):
            field = s.field
            direction = s.direction.lower()
        else:
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


def merge_annotations(
    existing_details: list[AnnotationItem], new_details: list[AnnotationItem]
) -> list[AnnotationItem]:
    """
    Merge annotations from new_details into existing_details.

    Rules:
    1. If new annotation has _id=None, it's a new element - add it
    2. If new annotation has _id but doesn't exist in existing, it's deleted - skip it
    3. If new annotation has _id and exists in existing, update it
    """
    annotation_map: dict[str, AnnotationItem] = {}

    # Build map of existing annotations
    for a in existing_details or []:
        if a._id:
            annotation_map[a._id] = a

    existing_ids = set(annotation_map.keys())
    final_annotations = []

    for a in new_details:
        if a._id is None:
            # New annotation - generate ID and add
            a._id = str(uuid.uuid4())
            final_annotations.append(a)
            log.info(
                f"New annotation added: {a._id} (label={a.label}, start={a.start}, end={a.end})"
            )
        elif a._id in existing_ids:
            # Existing annotation - update
            final_annotations.append(a)
        else:
            # Has ID but not in existing - it was deleted, skip
            log.info(f"Annotation deleted: {a._id} (label={a.label})")

    return final_annotations


def merge_comments(
    existing_comments: List[CommentItem],
    new_comments: List[CommentItem],
    user_id: Optional[str] = None,
) -> List[CommentItem]:
    """
    Merge comments from new_comments into existing_comments.

    Rules:
    1. If new comment has _id=None, it's a new comment - add with generated ID and timestamp
    2. If existing comment is not in new_comments, it's deleted - exclude from final list
    3. If new comment has _id matching existing, it's an update

    Returns:
        List of merged CommentItem objects
    """
    # Build map of existing comments by ID
    existing_map: dict[str, CommentItem] = {}
    for comment in existing_comments or []:
        if comment._id:
            existing_map[comment._id] = comment

    # Track which existing IDs are still present
    existing_ids_in_new = set()
    final_comments: List[CommentItem] = []
    now = datetime.now(timezone.utc)

    for comment in new_comments or []:
        if comment._id is None:
            # New comment - generate ID and set defaults
            comment._id = str(uuid.uuid4())
            if not comment.author:
                comment.author = user_id
            if not comment.timestamp:
                comment.timestamp = now

            log.info(
                f"New comment added: id={comment._id}, author={comment.author}, "
                f"page={comment.pageNumber}, text='{comment.text[:50]}...'"
            )
            final_comments.append(comment)
        else:
            # Has _id - check if it exists in existing comments
            if comment._id in existing_map:
                # Update existing comment - preserve original values if not provided
                existing_comment = existing_map[comment._id]
                if not comment.author:
                    comment.author = existing_comment.author or user_id
                if not comment.timestamp:
                    comment.timestamp = existing_comment.timestamp or now

                existing_ids_in_new.add(comment._id)
                final_comments.append(comment)
            else:
                # Has ID but not in existing - should not happen, treat as deleted
                log.warning(
                    f"Comment with ID {comment._id} not found in existing comments - skipping"
                )

    # Log deleted comments (existing but not in new)
    deleted_ids = set(existing_map.keys()) - existing_ids_in_new
    for deleted_id in deleted_ids:
        deleted_comment = existing_map[deleted_id]
        log.info(
            f"Comment deleted: id={deleted_id}, author={deleted_comment.author}, "
            f"page={deleted_comment.pageNumber}, text='{deleted_comment.text[:50]}...'"
        )

    return final_comments


class TaskService:
    def __init__(
        self, repo: TaskRepository, redis_client: redis.Redis, allocation_service: AllocationService
    ):
        self._repo = repo
        self._redis = redis_client
        self._allocation_service = allocation_service

    async def create_task(
        self, tenant_id: str, user_id: str, role: str, req: TaskCreateRequest
    ) -> dict[str, Any]:
        log.info(
            "svc.task.create start request_id=%s tenant_id=%s user_id=%s external_id=%s",
            req.request_id,
            tenant_id,
            user_id,
            req.external_id,
        )
        if role not in {"Admin", "Super Admin", "SuperAdmin"}:
            raise ForbiddenError("only admin can create tasks")

        now = datetime.now(timezone.utc)
        doc: dict[str, Any] = {
            "external_id": req.external_id,
            "tenant_id": tenant_id,
            "org": req.org,
            "status": req.status,
            "owner": user_id,
            "allocated_to": None,
            "task_details": req.task_details.model_dump(),
            "created_by": user_id,
            "updated_by": user_id,
            "created_at": now,
            "updated_at": now,
            "deleted_at": None,
        }

        _id = await self._repo.insert(doc)
        log.info(
            "svc.task.create inserted request_id=%s tenant_id=%s external_id=%s id=%s",
            req.request_id,
            tenant_id,
            req.external_id,
            _id,
        )

        # Enqueue allocation event (Redis Streams).
        settings = get_settings()
        log.info(
            "svc.task.create enqueue stream=%s request_id=%s tenant_id=%s external_id=%s",
            settings.redis_stream_tasks,
            req.request_id,
            tenant_id,
            req.external_id,
        )
        await self._redis.xadd(
            settings.redis_stream_tasks,
            {
                "event": "TASK_CREATED",
                "tenant_id": tenant_id,
                "external_id": req.external_id,
                "org": req.org,
                "assignment": req.task_details.task_assignment_type,
                "workflow": req.task_details.workflow_type,
                "data_type": req.task_details.data_type,
                "created_by": user_id,
            },
        )

        # Cache static-ish details (instructions/labels) for quick UI access.
        cache_key = f"lt:taskmeta:{tenant_id}:{req.external_id}"
        log.info(
            "svc.task.create cache_set key=%s request_id=%s tenant_id=%s external_id=%s",
            cache_key,
            req.request_id,
            tenant_id,
            req.external_id,
        )
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

        asyncio.create_task(
            self._allocation_service.allocate(
                AllocationRequest(
                    tenant_id=tenant_id,
                    role="Role_Annotator",
                    task_id=req.external_id,
                    assignment=req.task_details.task_assignment_type,
                    workflow=req.task_details.workflow_type,
                    data_type=req.task_details.data_type,
                )
            )
        )

        out = {
            "id": _id,
            "external_id": req.external_id,
            "org": req.org,
            "status": req.status,
            "updated_by": user_id,
            "created_by": user_id,
            "created_on": dt_to_iso(now),
            "updated_on": dt_to_iso(now),
            "task_details": req.task_details.model_dump(),
        }
        log.info(
            "svc.task.create done request_id=%s tenant_id=%s external_id=%s",
            req.request_id,
            tenant_id,
            req.external_id,
        )
        return out

    async def list_tasks(
        self, tenant_id: str, user_id: str, role: str, req: TaskListRequest
    ) -> dict[str, Any]:
        log.info(
            "svc.task.list start request_id=%s tenant_id=%s user_id=%s role=%s",
            req.request_id,
            tenant_id,
            user_id,
            role,
        )
        query = build_query(req.filters)

        # Non-admins can only see their own tasks.
        if role not in {"Admin", "Super Admin", "SuperAdmin"}:
            query["allocated_to"] = user_id

        projection = build_projection(req.fields)
        sort = build_sort(req.sort)
        skip = max(req.page, 0) * max(req.size, 1)
        limit = max(min(req.size, 100), 1)

        log.info(
            "svc.task.list query request_id=%s tenant_id=%s skip=%s limit=%s sort=%s query_keys=%s",
            req.request_id,
            tenant_id,
            skip,
            limit,
            sort,
            sorted(list(query.keys())),
        )
        items, total = await self._repo.list(
            tenant_id=tenant_id,
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
        out = {
            "tasks": tasks_out,
            "totalElements": total,
            "totalPages": total_pages,
            "currentPage": req.page,
        }
        log.info(
            "svc.task.list done request_id=%s tenant_id=%s returned=%s total=%s",
            req.request_id,
            tenant_id,
            len(tasks_out),
            total,
        )
        return out

    async def get_task_detail(
        self, tenant_id: str, user_id: str, role: str, req: TaskDetailRequest
    ) -> dict[str, Any]:
        log.info(
            "svc.task.detail start request_id=%s tenant_id=%s user_id=%s external_id=%s",
            req.request_id,
            tenant_id,
            user_id,
            req.external_id,
        )
        doc = await self._repo.get_by_external_id(tenant_id=tenant_id, external_id=req.external_id)

        # Non-admins: only tasks allocated to them
        if (
            role not in {"Admin", "Super Admin", "SuperAdmin"}
            and doc.get("allocated_to") != user_id
        ):
            raise ForbiddenError("forbidden")

        doc = oid_to_str(doc)
        doc["created_on"] = dt_to_iso(doc.get("created_at"))
        doc["updated_on"] = dt_to_iso(doc.get("updated_at"))
        doc.pop("created_at", None)
        doc.pop("updated_at", None)
        doc.pop("tenant_id", None)
        log.info(
            "svc.task.detail done request_id=%s tenant_id=%s external_id=%s",
            req.request_id,
            tenant_id,
            req.external_id,
        )
        return doc

    async def update_task(
        self,
        tenant_id: str,
        user_id: str,
        req: TaskUpdateRequest,
    ) -> dict[str, Any]:
        log.info(
            "svc.task.update start request_id=%s tenant_id=%s user_id=%s external_id=%s",
            req.request_id,
            tenant_id,
            user_id,
            req.external_id,
        )

        # 1. Fetch existing task
        task = await self._repo.get_task_by_external_id(
            tenant_id=tenant_id, external_id=req.external_id
        )

        # 2. Merge annotations and comments
        # We follow the logic from SaveAnnotationAction.java

        existing_details = task.task_details
        new_details = req.task_details

        # Merge Annotations (by ID)
        annotation_map = merge_annotations(existing_details.annotations, new_details.annotations)

        # Merge Comments (by ID)
        comment_map = merge_comments(existing_details.comments, new_details.comments, user_id)

        # Prepare final task_details
        merged_task_details = existing_details.model_copy()
        merged_task_details.annotations = annotation_map
        merged_task_details.comments = comment_map

        # Also update basic fields if sent in request
        for key in ["project_name", "project_desc", "instructions", "labels"]:
            if new_details.get(key):
                merged_task_details[key] = new_details[key]

        # 3. Save to repository
        updates = {
            "task_details": merged_task_details,
            "updated_by": user_id,
            "updated_at": datetime.now(timezone.utc),
            "status": "ANNOTATIONS_SAVE",  # As per Java code
        }

        updated_doc = await self._repo.update(
            tenant_id=tenant_id, external_id=req.external_id, updates=updates
        )

        log.info(
            "svc.task.update success request_id=%s tenant_id=%s user_id=%s external_id=%s",
            req.request_id,
            tenant_id,
            user_id,
            req.external_id,
        )

        # 4. Enqueue event
        settings = get_settings()
        await self._redis.xadd(
            settings.redis_stream_tasks,
            {
                "event": "TASK_UPDATED",
                "tenant_id": tenant_id,
                "external_id": req.external_id,
                "updated_by": user_id,
                "request_id": req.request_id or "",
            },
        )

        # Return formatted doc
        updated_doc = oid_to_str(updated_doc)
        updated_doc["created_on"] = dt_to_iso(updated_doc.get("created_at"))
        updated_doc["updated_on"] = dt_to_iso(updated_doc.get("updated_at"))
        updated_doc.pop("created_at", None)
        updated_doc.pop("updated_at", None)
        updated_doc.pop("tenant_id", None)
        return updated_doc

    async def update_task_status(
        self,
        tenant_id: str,
        user_id: str,
        req: TaskActionRequest,
        new_status: str,
    ) -> dict[str, Any]:
        log.info(
            "svc.task.update_status start request_id=%s tenant_id=%s user_id=%s external_id=%s status=%s",
            req.request_id,
            tenant_id,
            user_id,
            req.external_id,
            new_status,
        )

        doc = await self._repo.update_status(
            tenant_id=tenant_id,
            external_id=req.external_id,
            status=new_status,
            updated_by=user_id,
        )

        log.info(
            "svc.task.update_status success request_id=%s tenant_id=%s external_id=%s status=%s",
            req.request_id,
            tenant_id,
            req.external_id,
            new_status,
        )
        # Enqueue event (Redis Streams).
        settings = get_settings()
        await self._redis.xadd(
            settings.redis_stream_tasks,
            {
                "event": "TASK_STATUS_UPDATED",
                "tenant_id": tenant_id,
                "external_id": req.external_id,
                "status": new_status,
                "updated_by": user_id,
                "request_id": req.request_id or "",
            },
        )

        doc = oid_to_str(doc)
        doc["created_on"] = dt_to_iso(doc.get("created_at"))
        doc["updated_on"] = dt_to_iso(doc.get("updated_at"))
        doc.pop("created_at", None)
        doc.pop("updated_at", None)
        doc.pop("tenant_id", None)
        return doc
