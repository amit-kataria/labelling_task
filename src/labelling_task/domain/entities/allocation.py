from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class AllocationRequest:
    tenant_id: str
    role: str
    task_id: str
    assignment: str
    workflow: str
    data_type: str
