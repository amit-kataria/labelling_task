from typing import Union
from fastapi import APIRouter, Depends, Request, HTTPException

from labelling_task.domain.entities.task import (
    TaskCreateRequest,
    TaskDetailRequest,
    TaskListRequest,
    TaskListRequest2,
    TaskActionRequest,
    TaskUpdateRequest,
)
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
    allocation_service = request.app.state.allocation_service
    return TaskService(
        repo=repo, redis_client=redis_client.client, allocation_service=allocation_service
    )


@router.post("/create")
async def create_task(
    request: Request,
    body: TaskCreateRequest,
    token_data=Depends(require_role("Role_Admin")),
) -> dict:
    log.info(
        "task.create.start request_id=%s tenant_id=%s user_id=%s external_id=%s org=%s",
        body.request_id,
        token_data.get("tenantId", ""),
        token_data.get("sub", "unknown"),
        body.external_id,
        body.org,
    )
    svc = _service(request)
    data = await svc.create_task(
        tenant_id=token_data.get("tenantId", ""),
        user_id=token_data.get("sub", "unknown"),
        roles=token_data.get("roles", "unknown"),
        req=body,
    )
    log.info(
        "task.create.done request_id=%s tenant_id=%s user_id=%s external_id=%s task_id=%s",
        body.request_id,
        token_data.get("tenantId", ""),
        token_data.get("sub", "unknown"),
        body.external_id,
        data.get("id"),
    )
    return success(data)


@router.put("/perform/save_annotations")
async def save_annotations(
    request: Request,
    body: TaskUpdateRequest,
    token_data=Depends(get_current_user),
) -> dict:
    log.info(
        "task.save_annotations.start request_id=%s tenant_id=%s user_id=%s external_id=%s",
        body.request_id,
        token_data.get("tenantId", ""),
        token_data.get("sub", "unknown"),
        body.external_id,
    )
    tenant_id = token_data.get("tenantId", "")
    user_id = token_data.get("sub", "unknown")
    svc = _service(request)
    data = await svc.update_task(
        tenant_id=tenant_id,
        user_id=user_id,
        role=token_data.get("role", "unknown"),
        body=body,
    )
    log.info(
        "task.save_annotations.done request_id=%s tenant_id=%s user_id=%s external_id=%s",
        body.request_id,
        tenant_id,
        user_id,
        body.external_id,
    )
    return success(data, message="Task updated successfully")


@router.post("/v2/list")
@router.post("/list")
async def list_tasks(
    request: Request,
    body: TaskListRequest2,
    token_data=Depends(get_current_user),
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
    role = token_data.get("roles", "unknown")

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
async def get_task_detail(
    request: Request,
    body: TaskDetailRequest,
    token_data=Depends(get_current_user),
) -> dict:
    log.info(
        "task.detail.start tenant_id=%s user_id=%s external_id=%s",
        token_data.get("tenantId", ""),
        token_data.get("sub", "unknown"),
        body.external_id,
    )
    tenant_id = token_data.get("tenantId", "")
    user_id = token_data.get("sub", "unknown")
    role = token_data.get("role", "unknown")
    svc = _service(request)
    data = await svc.get_task_detail(tenant_id, user_id, role, body)
    log.info(
        "task.detail.done request_id=%s tenant_id=%s user_id=%s external_id=%s",
        body.request_id,
        token_data.get("tenantId", ""),
        token_data.get("sub", "unknown"),
        body.external_id,
    )
    return success(data, message="Request successful")


@router.put("/perform/park_task")
async def park_task(
    request: Request,
    body: TaskActionRequest,
    token_data=Depends(require_role("Role_Annotator")),
) -> dict:
    log.info(
        "task.park.start request_id=%s tenant_id=%s user_id=%s external_id=%s",
        body.request_id,
        token_data.get("tenantId", ""),
        token_data.get("sub", "unknown"),
        body.external_id,
    )
    tenant_id = token_data.get("tenantId", "")
    user_id = token_data.get("sub", "unknown")
    svc = _service(request)

    data = await svc.update_task_status(tenant_id, user_id, body, "PARKED")
    log.info("task.park.done external_id=%s", body.external_id)
    return success(data, message="Task parked successfully")


@router.put("/perform/unpark_task")
async def unpark_task(
    request: Request,
    body: TaskActionRequest,
    token_data=Depends(require_role("Role_Annotator")),
) -> dict:
    log.info(
        "task.unpark.start request_id=%s tenant_id=%s user_id=%s external_id=%s",
        body.request_id,
        token_data.get("tenantId", ""),
        token_data.get("sub", "unknown"),
        body.external_id,
    )
    tenant_id = token_data.get("tenantId", "")
    user_id = token_data.get("sub", "unknown")
    svc = _service(request)

    data = await svc.update_task_status(tenant_id, user_id, body, "ANNOTATIONS_SAVE")
    log.info("task.unpark.done external_id=%s", body.external_id)
    return success(data, message="Task unparked successfully")


@router.put("/perform/assign_task_annotator")
async def reject_task(
    request: Request,
    body: TaskActionRequest,
    token_data=Depends(require_role("Role_Reviewer")),
) -> dict:
    log.info(
        "task.reject.start request_id=%s tenant_id=%s user_id=%s external_id=%s",
        body.request_id,
        token_data.get("tenantId", ""),
        token_data.get("sub", "unknown"),
        body.external_id,
    )
    tenant_id = token_data.get("tenantId", "")
    user_id = token_data.get("sub", "unknown")
    svc = _service(request)

    data = await svc.update_task_status(tenant_id, user_id, body, "assign_task_annotator")
    log.info("task.reject.done external_id=%s", body.external_id)
    return success(data, message="Task rejected and reassigned to annotator")


@router.put("/perform/assign_task_reviewer")
async def submit_or_accept_task(
    request: Request,
    body: TaskActionRequest,
    token_data=Depends(get_current_user),
) -> dict:
    log.info(
        "task.assign_reviewer.start request_id=%s tenant_id=%s user_id=%s external_id=%s",
        body.request_id,
        token_data.get("tenantId", ""),
        token_data.get("sub", "unknown"),
        body.external_id,
    )
    tenant_id = token_data.get("tenantId", "")
    user_id = token_data.get("sub", "unknown")
    roles = token_data.get("roles", [])
    svc = _service(request)

    target_status = None
    if "Role_Annotator" in roles:
        target_status = "assign_task_reviewer"
    elif "Role_Reviewer" in roles:
        target_status = "accept_annotation"

    if not target_status:
        log.warning("task.assign_reviewer.forbidden user_id=%s roles=%s", user_id, roles)
        raise HTTPException(status_code=403, detail="Insufficient privileges")

    data = await svc.update_task_status(tenant_id, user_id, body, target_status)
    log.info(
        "task.assign_reviewer.done external_id=%s target_status=%s",
        body.external_id,
        target_status,
    )
    return success(data, message=f"Task moved to {target_status}")
