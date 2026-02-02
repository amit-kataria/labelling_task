from __future__ import annotations

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from labelling_task.configs.settings import Settings


def get_mongo_client(settings: Settings) -> AsyncIOMotorClient:
    return AsyncIOMotorClient(settings.mongo_uri)


def get_mongo_db(client: AsyncIOMotorClient, settings: Settings) -> AsyncIOMotorDatabase:
    return client[settings.mongo_db]
