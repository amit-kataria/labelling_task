from labelling_task.services.allocation_service import AllocationService

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse

from labelling_task.configs.settings import Settings, get_settings
from labelling_task.errors import AppError
from labelling_task.repositories.mongo import get_mongo_client, get_mongo_db
from labelling_task.repositories.redis_client import redis_client
from labelling_task.repositories.task_repository import TaskRepository
from labelling_task.repositories.allocation_repository import AllocationRepository
from labelling_task.routers.health_router import router as health_router
from labelling_task.routers.task_router import router as task_router
from labelling_task.utils.response import failure
from fastapi.middleware.cors import CORSMiddleware
from labelling_task.configs.logging_config import get_logger, setup_logging
from labelling_task.services.zip_processing_service import ZipProcessingService
import asyncio
import time
import httpx
from labelling_task.webclient.OAuth2TokenProvider import OAuth2TokenProvider
from labelling_task.webclient.OAuth2HttpClient import OAuth2HttpClient

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

        # Set up HTTP client and ZIP processing worker
        token_provider = OAuth2TokenProvider(
            token_url=settings.oauth2_token_url,
            client_id=settings.oauth2_client_id,
            client_secret=settings.oauth2_client_secret,
            scope=settings.oauth2_scope,
        )
        # We can pass an existing httpx client if we want to share it/config it
        httpx_client = httpx.AsyncClient(timeout=60.0)
        http_client = OAuth2HttpClient(token_provider=token_provider, client=httpx_client)
        app.state.http_client = http_client

        repo = TaskRepository(mongo_db, settings)
        log.info("startup.ensure_indexes begin")
        # await repo.ensure_indexes()
        log.info("startup.ensure_indexes skipped")
        app.state.task_repo = repo
        allocation_repo = AllocationRepository(mongo_db, settings)
        # await allocation_repo.ensure_indexes()
        app.state.allocation_repo = allocation_repo

        allocation_service = AllocationService(
            allocation_repo=allocation_repo,
            task_repo=repo,
            user_client=http_client,
        )
        app.state.allocation_service = allocation_service

        zip_service = ZipProcessingService(
            repo=repo,
            redis_client=redis_client.client,
            settings=settings,
            http_client=http_client,
        )
        app.state.zip_service = zip_service

        async def zip_worker() -> None:
            stream = settings.redis_stream_zip_jobs
            group = settings.zip_consumer_group
            consumer = settings.zip_consumer_name

            # Ensure consumer group exists
            try:
                await redis_client.client.xgroup_create(stream, group, id="0", mkstream=True)
                log.info("zip_worker.group_created stream=%s group=%s", stream, group)
            except Exception as exc:
                if "BUSYGROUP" not in str(exc):
                    log.error("zip_worker.group_create_failed %s", str(exc))

            log.info("zip_worker.start stream=%s group=%s consumer=%s", stream, group, consumer)
            while True:
                try:
                    resp = await redis_client.client.xreadgroup(
                        groupname=group,
                        consumername=consumer,
                        streams={stream: ">"},
                        count=1,
                        block=5000,
                    )
                    if not resp:
                        continue

                    for _, messages in resp:
                        for message_id, fields in messages:
                            data = fields
                            document_id = data.get("document_id") or data.get("file_id")
                            project_external_id = data.get("project_external_id")
                            tenant_id = data.get("tenant_id")
                            request_id = data.get("request_id")

                            if not document_id or not project_external_id or not tenant_id:
                                log.warning(
                                    "zip_worker.skip message_id=%s missing_required_fields=%s",
                                    message_id,
                                    data,
                                )
                                await redis_client.client.xack(stream, group, message_id)
                                continue

                            try:
                                await zip_service.process_zip_job(
                                    tenant_id=str(tenant_id),
                                    document_id=str(document_id),
                                    project_external_id=str(project_external_id),
                                    request_id=str(request_id) if request_id else None,
                                )
                                await redis_client.client.xack(stream, group, message_id)
                            except Exception as exc:
                                log.error(
                                    "zip_worker.processing_failed message_id=%s error=%s",
                                    message_id,
                                    str(exc),
                                    exc_info=True,
                                )
                except Exception as loop_exc:
                    log.error("zip_worker.loop_error %s", str(loop_exc), exc_info=True)
                    await asyncio.sleep(5)

        app.state.zip_worker_task = asyncio.create_task(zip_worker())

    @app.on_event("shutdown")
    async def shutdown() -> None:
        log.info("shutdown.begin")
        await redis_client.close()
        # Cancel zip worker
        worker = getattr(app.state, "zip_worker_task", None)
        if worker:
            worker.cancel()
        http_client = getattr(app.state, "http_client", None)
        if http_client is not None:
            await http_client.session.aclose()
        mongo_client = getattr(app.state, "mongo_client", None)
        if mongo_client is not None:
            mongo_client.close()
        log.info("shutdown.done")

    return app


app = create_app()
