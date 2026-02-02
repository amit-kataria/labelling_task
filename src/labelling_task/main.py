from __future__ import annotations

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse

from labelling_task.auth.dependencies import get_principal
from labelling_task.configs.settings import Settings, get_settings
from labelling_task.errors import AppError
from labelling_task.repositories.mongo import get_mongo_client, get_mongo_db
from labelling_task.repositories.redis_client import redis_client
from labelling_task.repositories.task_repository import TaskRepository
from labelling_task.routers.health_router import router as health_router
from labelling_task.routers.task_router import router as task_router
from labelling_task.utils.response import failure
from fastapi.middleware.cors import CORSMiddleware
from labelling_task.configs.logging_config import get_logger, setup_logging
import time
log = get_logger(__name__)


def create_app() -> FastAPI:
    app = FastAPI(title="labelling_task", version="0.1.0")
    settings: Settings = get_settings()
    # Normalize CORS origins from settings (.env can provide a comma-separated string)
    raw_origins = settings.CORS_ORIGINS
    if isinstance(raw_origins, str):
        origins = [o.strip() for o in raw_origins.split(",") if o.strip()]
    elif isinstance(raw_origins, (list, tuple, set)):
        origins = list(raw_origins)
    else:
        origins = []
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    @app.middleware("http")
    async def request_logging_middleware(request: Request, call_next):
        start = time.perf_counter()
        method = request.method
        path = request.url.path
        request_id = request.headers.get("x-request-id") or request.headers.get("x-correlation-id")
        tenant_id = request.headers.get("x-tenant-id")  # optional; canonical is JWT claim
        
        log.info(
            "request.start method=%s path=%s request_id=%s tenant_hint=%s",
            method,
            path,
            request_id,
            tenant_id,
        )
        try:
            response = await call_next(request)
        finally:
            elapsed_ms = int((time.perf_counter() - start) * 1000)
            status_code = getattr(locals().get("response", None), "status_code", "unknown")
            log.info(
                "request.end method=%s path=%s status=%s request_id=%s elapsed_ms=%s",
                method,
                path,
                status_code,
                request_id,
                elapsed_ms,
            )
        return response
    app.include_router(health_router)
    app.include_router(task_router)

    @app.exception_handler(AppError)
    async def app_error_handler(_: Request, exc: AppError) -> JSONResponse:
        log.info("request.error type=app_error status=%s message=%s", exc.http_status, exc.message)
        return JSONResponse(status_code=exc.http_status, content=failure(exc.message))

    @app.exception_handler(Exception)
    async def unhandled_error_handler(_: Request, exc: Exception) -> JSONResponse:
        log.exception("Unhandled error: %s", str(exc))
        return JSONResponse(status_code=500, content=failure("internal server error"))

    @app.on_event("startup")
    async def startup() -> None:
        setup_logging()
        settings: Settings = get_settings()

        mongo_client = get_mongo_client(settings)
        mongo_db = get_mongo_db(mongo_client, settings)
        await redis_client.connect()

        app.state.settings = settings
        app.state.mongo_client = mongo_client
        app.state.mongo_db = mongo_db
        app.state.redis = redis_client.client
        
        repo = TaskRepository(mongo_db, settings)
        log.info("startup.ensure_indexes begin")
        # await repo.ensure_indexes()
        log.info("startup.ensure_indexes skipped")
        app.state.task_repo = repo

    @app.on_event("shutdown")
    async def shutdown() -> None:
        log.info("shutdown.begin")
        await redis_client.close()
        mongo_client = getattr(app.state, "mongo_client", None)
        if mongo_client is not None:
            mongo_client.close()
        log.info("shutdown.done")

    return app


app = create_app()