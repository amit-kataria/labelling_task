from labelling_task.allocation.allocation_strategies import (
    RoundRobinStrategy,
    LeastLoadedStrategy,
    LastAssignedStrategy,
)
from labelling_task.repositories.allocation_repository import AllocationRepository


class StrategyFactory:
    def __init__(self, allocation_repo: AllocationRepository):
        self._strategies = {
            "RR": RoundRobinStrategy(allocation_repo),
            "LL": LeastLoadedStrategy(allocation_repo),
            "LA": LastAssignedStrategy(allocation_repo, fallback=LeastLoadedStrategy(allocation_repo)),
        }

    def get(self, name: str):
        if name not in self._strategies:
            raise ValueError(f"Unknown strategy: {name}")
        return self._strategies[name]
