from __future__ import annotations

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from labelling_task.configs.settings import Settings
from labelling_task.configs.logging_config import get_logger
log = get_logger(__name__)

def get_mongo_client(settings: Settings) -> AsyncIOMotorClient:
    log.info("mongo.client.create uri=%s", settings.mongo_uri)
    return AsyncIOMotorClient(settings.mongo_uri)


def get_mongo_db(client: AsyncIOMotorClient, settings: Settings) -> AsyncIOMotorDatabase:
    log.info("mongo.db.select db=%s", settings.mongo_db)
    return client[settings.mongo_db]
