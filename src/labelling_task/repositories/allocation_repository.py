from motor.motor_asyncio import AsyncIOMotorDatabase
from labelling_task.configs.settings import Settings
from datetime import datetime
from typing import Optional


class AllocationRepository:
    def __init__(self, db: AsyncIOMotorDatabase, settings: Settings):
        self._db = db
        self._settings = settings
        self._col = db["labelling_task_allocation_stats"]

    async def upsert_users(self, tenant_id, role, users):
        for u in users:
            await self._col.update_one(
                {"tenant_id": tenant_id, "user_id": u, "role": role},
                {
                    "$setOnInsert": {
                        "is_active": True,
                        "active_task_count": 0,
                        "last_assigned_at": None,
                        "last_task_id": None,
                    }
                },
                upsert=True,
            )

    async def allocate_rr(self, tenant_id, role, task_id):
        return await self._col.find_one_and_update(
            {"tenant_id": tenant_id, "role": role, "is_active": True},
            {
                "$set": {"last_assigned_at": datetime.utcnow(), "last_task_id": task_id},
                "$inc": {"active_task_count": 1},
            },
            sort=[("last_assigned_at", 1)],
            return_document=True,
        )

    async def allocate_ll(self, tenant_id, role, task_id):
        return await self._col.find_one_and_update(
            {"tenant_id": tenant_id, "role": role, "is_active": True},
            {
                "$set": {"last_assigned_at": datetime.utcnow(), "last_task_id": task_id},
                "$inc": {"active_task_count": 1},
            },
            sort=[("active_task_count", 1), ("last_assigned_at", 1)],
            return_document=True,
        )

    async def allocate_la(self, tenant_id, role, task_id):
        return await self._col.find_one_and_update(
            {"tenant_id": tenant_id, "role": role, "last_task_id": task_id, "is_active": True},
            {"$set": {"last_assigned_at": datetime.utcnow()}, "$inc": {"active_task_count": 1}},
            return_document=True,
        )
