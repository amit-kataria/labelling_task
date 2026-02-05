from labelling_task.webclient.OAuth2HttpClient import OAuth2HttpClient
from labelling_task.repositories.task_repository import TaskRepository
from labelling_task.repositories.allocation_repository import AllocationRepository
from labelling_task.allocation.strategy_factory import StrategyFactory
from labelling_task.domain.entities.allocation import AllocationRequest
from labelling_task.configs.logging_config import get_logger

log = get_logger(__name__)


class AllocationService:
    def __init__(
        self,
        allocation_repo: AllocationRepository,
        task_repo: TaskRepository,
        user_client: OAuth2HttpClient,
    ):
        self._factory = StrategyFactory(allocation_repo)
        self._task_repo = task_repo
        self._allocation_repo = allocation_repo
        self._user_client = user_client

    async def allocate(self, req: AllocationRequest):
        strategy = self._factory.get(req.assignment)

        doc = await strategy.allocate(req)
        if doc:
            log.info(
                "alloc.success tenant=%s user=%s task=%s", req.tenant_id, doc.user_id, req.task_id
            )
            await self._task_repo.set_allocated_to(
                tenant_id=req.tenant_id,
                external_id=req.task_id,
                user_id=doc.user_id,
            )
            return doc

        # bootstrap
        users = await self._user_client.get_users_by_role(req.tenant_id, req.role)

        await self._allocation_repo.upsert_users(req.tenant_id, req.role, users)

        doc = await strategy.allocate(req)
        if doc:
            log.info("alloc.success_after_bootstrap user=%s task=%s", doc.user_id, req.task_id)
            await self._task_repo.set_allocated_to(
                tenant_id=req.tenant_id,
                external_id=req.task_id,
                user_id=doc.user_id,
            )
        else:
            log.error("alloc.failed_after_bootstrap tenant=%s task=%s", req.tenant_id, req.task_id)

        return doc
