from labelling_task.repositories.allocation_repository import AllocationRepository
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from datetime import datetime

from labelling_task.allocation.errors import NoEligibleUsersError
from labelling_task.configs.logging_config import get_logger
from labelling_task.domain.entities.allocation import AllocationRequest

log = get_logger(__name__)


class AllocationStrategy(ABC):
    """
    Template-method base class.
    Concrete strategies override only _allocate_core().
    """

    def __init__(self, repo: AllocationRepository):
        self._repo = repo

    # ----------------------------
    # Public API
    # ----------------------------

    async def allocate(self, req: AllocationRequest) -> Dict[str, Any]:
        """
        Orchestrates full lifecycle:
        validate -> try allocate -> bootstrap -> retry
        """

        self._validate(req)

        log.info(
            "alloc.start strategy=%s tenant=%s role=%s task=%s",
            self.name(),
            req.tenant_id,
            req.role,
            req.task_id,
        )

        doc = await self._allocate_core(req)
        if doc:
            self._log_success(doc, req)
            return doc

        log.warning(
            "alloc.no_candidate strategy=%s tenant=%s role=%s", self.name(), req.tenant_id, req.role
        )

        raise NoEligibleUsersError(f"No eligible users for role={req.role}")

    # ----------------------------
    # Hooks
    # ----------------------------

    def _validate(self, req: AllocationRequest):
        if not req.tenant_id:
            raise ValueError("tenant_id missing")
        if not req.role:
            raise ValueError("role missing")
        if not req.task_id:
            raise ValueError("task_id missing")

    def _log_success(self, doc: Dict[str, Any], req: AllocationRequest):
        log.info(
            "alloc.success strategy=%s user=%s tenant=%s task=%s count=%s",
            self.name(),
            doc["user_id"],
            req.tenant_id,
            req.task_id,
            doc.get("active_task_count"),
        )

    # ----------------------------
    # Mandatory override
    # ----------------------------

    @abstractmethod
    async def _allocate_core(self, req: AllocationRequest) -> Optional[Dict[str, Any]]:
        """
        Must perform ONE atomic Mongo findOneAndUpdate.
        """
        pass

    @abstractmethod
    def name(self) -> str:
        pass
