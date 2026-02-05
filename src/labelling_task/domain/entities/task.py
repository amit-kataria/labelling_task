from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, List, Optional

from pydantic import BaseModel, Field, field_validator


class AnnotationItem(BaseModel):
    _id: str | None = None
    start: int
    end: int
    section: str
    label: str
    pageNumber: int
    paragraphNo: int
    value: str
    description: str | None = None
    color: str | None = None


class CommentItem(BaseModel):
    _id: str | None = None
    text: str
    author: str | None = None
    timestamp: datetime | None = None
    pageNumber: str


class TaskDetails(BaseModel):
    project_name: str | None = None
    project_desc: str | None = None
    data_type: Literal["pdf", "text", "image", "audio", "video"] | str
    task_assignment_type: Literal["RoundRobin", "LeastLoaded", "Manual"] | str = "RoundRobin"
    workflow_type: Literal["Single Pass", "With Review", "Consencus"] | str = "Single Pass"

    instructions: str | None = None
    # Per-file details / output
    file_name: str | None = None
    annotations: list[AnnotationItem] = Field(default_factory=list)
    comments: list[CommentItem] = Field(default_factory=list)


class Task(BaseModel):
    """
    Mongo document model for `tasks` collection.
    """

    id: str | None = None  # stringified ObjectId in API responses
    external_id: str

    tenant_id: str
    org: str

    status: str
    owner: str | None = None  # usually admin who uploaded zip / owns project
    allocated_to: str | None = None  # annotation user / reviewer id

    task_details: TaskDetails

    # Auditing fields (required)
    created_by: str = "anonymousUser"
    updated_by: str = "anonymousUser"
    created_at: datetime
    updated_at: datetime
    deleted_at: datetime | None = None
    parent_external_id: str | None = None


class Envelope(BaseModel):
    request_id: str | None = None


class TaskCreateRequest(Envelope):
    external_id: str
    org: str
    status: str = "new"
    task_details: TaskDetails


class FilterClause(BaseModel):
    operator: Literal["eq", "ne", "gt", "gte", "lt", "lte", "in", "nin", "regex"]
    value: Any


class FilterCondition(BaseModel):
    logic: Optional[Literal["AND", "OR"]] = None
    field: Optional[str] = None
    operator: Optional[Literal["eq", "ne", "gt", "gte", "lt", "lte", "in", "nin", "regex"]] = None
    value: Optional[Any] = None
    conditions: Optional[List[FilterCondition]] = None

    @field_validator("logic", mode="before")
    @classmethod
    def normalize_logic(cls, v):
        if isinstance(v, str):
            return v.upper()
        return v


class SortCriterion(BaseModel):
    field: str
    direction: Literal["asc", "desc"] = "asc"

    @field_validator("direction", mode="before")
    @classmethod
    def normalize_direction(cls, v):
        if isinstance(v, str):
            return v.lower()
        return v


class TaskListRequest(Envelope):
    """Deprecated: using TaskListRequest2 instead"""

    filters: dict[str, FilterClause] = Field(default_factory=dict)
    fields: list[str] | None = None
    page: int = 0
    size: int = 10
    sort: list[SortCriterion] = Field(default_factory=list)


class TaskListRequest2(Envelope):
    filters: Optional[FilterCondition] = None
    sort: Optional[List[SortCriterion]] = None
    page: Optional[int] = 0
    size: Optional[int] = 10
    fields: Optional[List[str]] = None

    @field_validator("filters", mode="before")
    @classmethod
    def transform_filters(cls, v):
        if isinstance(v, dict) and "logic" not in v and "field" not in v:
            # It's likely the old map format: { "field": { "operator": "...", "value": "..." } }
            conditions = []
            for field, clause in v.items():
                if isinstance(clause, dict) and "operator" in clause:
                    conditions.append(
                        FilterCondition(
                            field=field,
                            operator=clause.get("operator"),
                            value=clause.get("value"),
                        )
                    )
            if conditions:
                return FilterCondition(logic="AND", conditions=conditions)
        return v


class TaskDetailRequest(Envelope):
    external_id: str


class TaskActionRequest(Envelope):
    external_id: str


class TaskUpdateRequest(Envelope):
    external_id: str
    task_details: TaskDetails
