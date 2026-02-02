from fastapi import APIRouter, Depends, Request

from labelling_task.auth.dependencies import get_principal
from labelling_task.auth.models import Principal
from labelling_task.domain.entities.task import TaskCreateRequest, TaskDetailRequest, TaskListRequest
from labelling_task.services.task_service import TaskService
from labelling_task.utils.response import success
from labelling_task.repositories.redis_client import redis_client
from labelling_task.auth.security import get_current_user, require_role
from labelling_task.configs.logging_config import get_logger
log = get_logger(__name__)

router = APIRouter(prefix="/ext/task", tags=["task"])

def get_current_sub(token_data=Depends(get_current_user)):
    # Extract sub or useful ID from token
    return token_data.get("sub", "unknown")


def _service(request: Request) -> TaskService:
    repo = request.app.state.task_repo
    return TaskService(repo=repo, redis_client=redis_client.client)


@router.post("/create")
async def create_task(
    request: Request,
    body: TaskCreateRequest,
    principal: Principal = Depends(get_principal),
) -> dict:
    log.info(
        "task.create.start request_id=%s tenant_id=%s user_id=%s external_id=%s org=%s",
        body.request_id,
        principal.tenant_id,
        principal.user_id,
        body.external_id,
        body.org,
    )
    svc = _service(request)
    data = await svc.create_task(principal, body)
    log.info(
        "task.create.done request_id=%s tenant_id=%s user_id=%s external_id=%s task_id=%s",
        body.request_id,
        principal.tenant_id,
        principal.user_id,
        body.external_id,
        data.get("id"),
    )
    return success(data)


@router.post("/v2/list")
@router.post("/list")
async def list_tasks(
    request: Request,
    body: TaskListRequest,
    token_data = Depends(get_current_user),
) -> dict:
    log.info(
        "task.list.start request_id=%s tenant_id=%s user_id=%s page=%s size=%s",
        body.request_id,
        token_data.get("tenantId", ""),
        token_data.get("sub", "unknown"),
        body.page,
        body.size,
    )
    svc = _service(request)
    tenant_id = token_data.get("tenantId", "")
    user_id = token_data.get("sub", "unknown")
    role = token_data.get("role", "unknown")
    
    data = await svc.list_tasks(tenant_id, user_id, role, body)
    log.info(
        "task.list.done request_id=%s tenant_id=%s user_id=%s returned=%s total=%s",
        body.request_id,
        token_data.get("tenantId", ""),
        token_data.get("sub", "unknown"),
        len(data.get("tasks") or []),
        data.get("totalElements"),
    )
    return success(data, message="Request successful")


@router.post("/detail")
async def task_detail(
    request: Request,
    body: TaskDetailRequest,
    principal: Principal = Depends(get_principal),
) -> dict:
    log.info(
        "task.detail.start request_id=%s tenant_id=%s user_id=%s external_id=%s",
        body.request_id,
        principal.tenant_id,
        principal.user_id,
        body.external_id,
    )
    svc = _service(request)
    data = await svc.get_task_detail(principal, body)
    log.info(
        "task.detail.done request_id=%s tenant_id=%s user_id=%s external_id=%s",
        body.request_id,
        principal.tenant_id,
        principal.user_id,
        body.external_id,
    )
    return success(data, message="Request successful")
