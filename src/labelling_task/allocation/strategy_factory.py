from labelling_task.allocation.allocation_strategies import (
    RoundRobinStrategy,
    LeastLoadedStrategy,
    LastAssignedStrategy,
)


class StrategyFactory:
    def __init__(self, repo):
        self._strategies = {
            "RR": RoundRobinStrategy(repo),
            "LL": LeastLoadedStrategy(repo),
            "LA": LastAssignedStrategy(repo, fallback=self._strategies["LL"]),
        }

    def get(self, name):
        if name not in self._strategies:
            raise ValueError(f"Unknown strategy: {name}")
        return self._strategies[name]
