from __future__ import annotations

from fastapi import APIRouter, Depends, Request

from labelling_task.auth.dependencies import get_principal
from labelling_task.auth.models import Principal
from labelling_task.domain.entities.task import TaskCreateRequest, TaskDetailRequest, TaskListRequest
from labelling_task.services.task_service import TaskService
from labelling_task.utils.response import success
from labelling_task.configs.logging_config import get_logger
log = get_logger(__name__)

router = APIRouter(prefix="/task", tags=["task"])


def _service(request: Request) -> TaskService:
    repo = request.app.state.task_repo
    redis_client = request.app.state.redis
    return TaskService(repo=repo, redis_client=redis_client)


@router.post("/create")
async def create_task(
    request: Request,
    body: TaskCreateRequest,
    principal: Principal = Depends(get_principal),
) -> dict:
    svc = _service(request)
    data = await svc.create_task(principal, body)
    return success(data)


@router.post("/list")
async def list_tasks(
    request: Request,
    body: TaskListRequest,
    principal: Principal = Depends(get_principal),
) -> dict:
    svc = _service(request)
    data = await svc.list_tasks(principal, body)
    return success(data, message="Request successful")


@router.post("/detail")
async def task_detail(
    request: Request,
    body: TaskDetailRequest,
    principal: Principal = Depends(get_principal),
) -> dict:
    svc = _service(request)
    data = await svc.get_task_detail(principal, body)
    return success(data, message="Request successful")
