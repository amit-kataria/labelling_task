from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase

from labelling_task.configs.settings import Settings
from labelling_task.errors import NotFoundError
from labelling_task.configs.logging_config import get_logger
log = get_logger(__name__)

class TaskRepository:
    def __init__(self, db: AsyncIOMotorDatabase, settings: Settings):
        self._db = db
        self._settings = settings
        self._col = db["annotation_tasks"]

    async def ensure_indexes(self) -> None:
        log.info("repo.task.ensure_indexes start")
        # Multi-tenant uniqueness: external_id unique within tenant.
        await self._col.create_index([("tenant_id", 1), ("external_id", 1)], unique=True)

        # Common list filters / sorts
        await self._col.create_index([("tenant_id", 1), ("status", 1), ("created_at", -1)])
        await self._col.create_index([("tenant_id", 1), ("allocated_to", 1), ("status", 1)])
        await self._col.create_index([("tenant_id", 1), ("org", 1), ("created_at", -1)])

        # Retention for deleted tasks (TTL on deleted_at).
        ttl_seconds = int(timedelta(days=self._settings.deleted_retention_days).total_seconds())
        await self._col.create_index(
            [("deleted_at", 1)],
            expireAfterSeconds=ttl_seconds,
            name="ttl_deleted_at",
        )
        log.info("repo.task.ensure_indexes done")

    async def insert(self, doc: dict[str, Any]) -> str:
        log.info(
            "repo.task.insert tenant_id=%s external_id=%s status=%s",
            doc.get("tenant_id"),
            doc.get("external_id"),
            doc.get("status"),
        )
        res = await self._col.insert_one(doc)
        return str(res.inserted_id)

    async def get_by_external_id(self, *, tenant_id: str, external_id: str) -> dict[str, Any]:
        log.info("repo.task.get_by_external_id tenant_id=%s external_id=%s", tenant_id, external_id)
        doc = await self._col.find_one({"tenant_id": tenant_id, "external_id": external_id, "deleted_at": None})
        if not doc:
            log.info("repo.task.get_by_external_id not_found tenant_id=%s external_id=%s", tenant_id, external_id)
            raise NotFoundError("task not found")
        return doc

    async def list(
        self,
        *,
        tenant_id: str,
        query: dict[str, Any],
        projection: dict[str, int] | None,
        skip: int,
        limit: int,
        sort: list[tuple[str, int]],
    ) -> tuple[list[dict[str, Any]], int]:
        q = {"tenant_id": tenant_id, "deleted_at": None, **query}
        log.info(
            "repo.task.list tenant_id=%s skip=%s limit=%s sort=%s query_keys=%s",
            tenant_id,
            skip,
            limit,
            sort,
            sorted(list(query.keys())),
        )
        log.debug(f"executing query {q}")
        cursor = self._col.find(q, projection=projection).sort(sort).skip(skip).limit(limit)
        items = await cursor.to_list(length=limit)
        total = await self._col.count_documents(q)
        return items, total


def oid_to_str(doc: dict[str, Any]) -> dict[str, Any]:
    if "_id" in doc and isinstance(doc["_id"], ObjectId):
        doc["id"] = str(doc["_id"])
        del doc["_id"]
    return doc


def dt_to_iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    # Match sample style (no timezone suffix) while keeping UTC.
    return dt.astimezone(timezone.utc).replace(tzinfo=None).isoformat(timespec="milliseconds")
