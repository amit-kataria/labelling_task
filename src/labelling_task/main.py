from __future__ import annotations

import logging

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse

from labelling_task.auth.dependencies import get_principal
from labelling_task.configs.settings import Settings, get_settings
from labelling_task.errors import AppError
from labelling_task.repositories.mongo import get_mongo_client, get_mongo_db
from labelling_task.repositories.redis_client import get_redis
from labelling_task.repositories.task_repository import TaskRepository
from labelling_task.routers.health_router import router as health_router
from labelling_task.routers.task_router import router as task_router
from labelling_task.utils.logging import configure_logging
from labelling_task.utils.response import failure

log = logging.getLogger(__name__)


def create_app() -> FastAPI:
    app = FastAPI(title="labelling_task", version="0.1.0")

    app.include_router(health_router)
    app.include_router(task_router, dependencies=[Depends(get_principal)])

    @app.exception_handler(AppError)
    async def app_error_handler(_: Request, exc: AppError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=failure(exc.message))

    @app.exception_handler(Exception)
    async def unhandled_error_handler(_: Request, exc: Exception) -> JSONResponse:
        log.exception("Unhandled error: %s", str(exc))
        return JSONResponse(status_code=500, content=failure("internal server error"))

    @app.on_event("startup")
    async def startup() -> None:
        configure_logging()
        settings: Settings = get_settings()

        mongo_client = get_mongo_client(settings)
        mongo_db = get_mongo_db(mongo_client, settings)
        redis_client = get_redis(settings)

        app.state.settings = settings
        app.state.mongo_client = mongo_client
        app.state.mongo_db = mongo_db
        app.state.redis = redis_client

        repo = TaskRepository(mongo_db, settings)
        await repo.ensure_indexes()
        app.state.task_repo = repo

    @app.on_event("shutdown")
    async def shutdown() -> None:
        redis_client = getattr(app.state, "redis", None)
        if redis_client is not None:
            await redis_client.close()

        mongo_client = getattr(app.state, "mongo_client", None)
        if mongo_client is not None:
            mongo_client.close()

    return app


app = create_app()

if __name__ == "__main__":
    import uvicorn

    log.info(f"Starting Uvicorn server...")
    try:
        uvicorn.run(
            "labelling_task.main:app", host="0.0.0.0", port=5050, reload=False, log_level="info"
        )
    except Exception as e:
        log.error(f"Failed to start server: {e}", exc_info=True)
        sys.exit(1)
