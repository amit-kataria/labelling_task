from __future__ import annotations

from labelling_task.domain.entities.task import FilterClause
from labelling_task.services.task_service import build_query, build_sort


def test_build_query_maps_created_on_to_created_at_and_parses_date() -> None:
    q = build_query(
        {
            "status": FilterClause(operator="eq", value="TASKS_ASSIGN_ANNOTATE"),
            "created_on": FilterClause(operator="gte", value="2024-12-31"),
        }
    )
    assert q["status"] == "TASKS_ASSIGN_ANNOTATE"
    assert "created_at" in q
    assert "$gte" in q["created_at"]


def test_build_sort_maps_created_on() -> None:
    s = build_sort([{"field": "created_on", "direction": "desc"}])
    assert s[0][0] == "created_at"
    assert s[0][1] == -1
