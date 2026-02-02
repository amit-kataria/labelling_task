from __future__ import annotations

from fastapi import APIRouter

from labelling_task.utils.response import success

router = APIRouter()


@router.get("/health")
async def health() -> dict:
    return success({"ok": True}, message="healthy")
