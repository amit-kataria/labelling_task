from labelling_task.allocation.allocation_strategy import AllocationStrategy
from labelling_task.configs.logging_config import get_logger
from labelling_task.repositories.allocation_repository import AllocationRepository
from labelling_task.domain.entities.allocation import AllocationRequest

log = get_logger(__name__)


class RoundRobinStrategy(AllocationStrategy):
    def __init__(self, repo: AllocationRepository):
        super().__init__(repo)

    def name(self):
        return "RR"

    async def _allocate_core(self, req: AllocationRequest):
        log.info("alloc.rr.start tenant=%s task=%s", req.tenant_id, req.task_id)
        return await self._repo.allocate_rr(req.tenant_id, req.role, req.task_id)


class LeastLoadedStrategy(AllocationStrategy):
    def __init__(self, repo: AllocationRepository):
        super().__init__(repo)

    def name(self):
        return "LL"

    async def _allocate_core(self, req: AllocationRequest):
        log.info("alloc.ll.start tenant=%s task=%s", req.tenant_id, req.task_id)
        return await self._repo.allocate_ll(req.tenant_id, req.role, req.task_id)


class LastAssignedStrategy(AllocationStrategy):
    def __init__(self, repo: AllocationRepository, fallback: AllocationStrategy):
        super().__init__(repo)
        self._fallback = fallback

    def name(self):
        return "LA"

    async def _allocate_core(self, req: AllocationRequest):
        doc = await self._repo.allocate_la(req.tenant_id, req.role, req.task_id)
        if doc:
            return doc

        # fallback to LL
        return await self._fallback._allocate_core(req)
