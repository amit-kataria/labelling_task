from __future__ import annotations

import asyncio
import json
import logging
import mimetypes
import tempfile
import zipfile
from pathlib import Path
from typing import Any

import httpx
import redis.asyncio as redis

from labelling_task.configs.settings import Settings
from labelling_task.repositories.task_repository import TaskRepository, dt_to_iso

log = logging.getLogger(__name__)


class ZipProcessingService:
    """
    Python equivalent of the Java ZipProcessingService.

    Responsibilities:
    - Download ZIP from upload-service (streaming) using document_id.
    - Extract entries to a temp directory.
    - For each file:
        - Upload to upload-service as multipart/form-data with metadata.
        - Create a child task linked to the project task (parent).
    - Update project task with child_task_count.
    - Optionally emit allocation events via Redis Streams.
    """

    def __init__(
        self,
        repo: TaskRepository,
        redis_client: redis.Redis,
        settings: Settings,
        http_client: httpx.AsyncClient,
    ) -> None:
        self._repo = repo
        self._redis = redis_client
        self._settings = settings
        self._http = http_client

    async def process_zip_job(
        self,
        *,
        tenant_id: str,
        document_id: str,
        project_external_id: str,
        request_id: str | None = None,
    ) -> None:
        """
        Entry point invoked by Redis stream consumer.
        """
        log.info(
            "zip.process.start tenant_id=%s document_id=%s project_external_id=%s request_id=%s",
            tenant_id,
            document_id,
            project_external_id,
            request_id,
        )

        # Load project (parent) task
        project = await self._repo.get_by_external_id(
            tenant_id=tenant_id, external_id=project_external_id
        )

        temp_dir_path: Path | None = None
        try:
            with tempfile.TemporaryDirectory(prefix="zip-work-") as tmp:
                temp_dir_path = Path(tmp)
                zip_path = temp_dir_path / "input.zip"

                await self._download_zip_to_file(document_id=document_id, target=zip_path)
                created = await self._extract_and_process(zip_path, temp_dir_path, project)
                await self._update_project_count(project, created)

        except Exception as exc:  # pragma: no cover - defensive log
            log.error(
                "zip.process.failed tenant_id=%s document_id=%s project_external_id=%s error=%s",
                tenant_id,
                document_id,
                project_external_id,
                str(exc),
                exc_info=True,
            )

        log.info(
            "zip.process.done tenant_id=%s document_id=%s project_external_id=%s",
            tenant_id,
            document_id,
            project_external_id,
        )

    async def _download_zip_to_file(self, *, document_id: str, target: Path) -> None:
        """
        Stream-download the ZIP from upload-service into a local temp file.
        """
        url = f"{self._settings.upload_service_base_url}/int/media/file/download/id/{document_id}"
        log.info("zip.download.start url=%s target=%s", url, target)

        async with self._http.stream("GET", url) as resp:
            resp.raise_for_status()
            with target.open("wb") as f:
                async for chunk in resp.aiter_bytes():
                    f.write(chunk)

        log.info("zip.download.done bytes=%s", target.stat().st_size)

    async def _extract_and_process(
        self,
        zip_path: Path,
        working_dir: Path,
        project: dict[str, Any],
    ) -> int:
        """
        Stream ZIP extraction and per-entry upload + child task creation.
        """
        created = 0
        log.info("zip.extract.start zip=%s working_dir=%s", zip_path, working_dir)

        # Use standard library ZipFile with buffered reads; file itself was streamed to disk.
        with zipfile.ZipFile(zip_path, "r") as zf:
            for entry in zf.infolist():
                if entry.is_dir():
                    continue

                out_file = working_dir / entry.filename
                out_file.parent.mkdir(parents=True, exist_ok=True)

                # Extract this entry to disk
                with zf.open(entry, "r") as src, out_file.open("wb") as dst:
                    for chunk in iter(lambda: src.read(1024 * 64), b""):
                        dst.write(chunk)

                media_type, _ = mimetypes.guess_type(str(out_file))
                new_file_id = await self._upload_file(out_file, project, media_type)
                if new_file_id:
                    await self._create_child_task(project, new_file_id, out_file.name)
                    created += 1

        log.info("zip.extract.done created_child_tasks=%s", created)
        return created

    async def _upload_file(
        self,
        file_path: Path,
        project: dict[str, Any],
        media_type: str | None,
    ) -> str | None:
        """
        Stream-upload a single file as multipart to upload-service, with metadata.
        """
        url = f"{self._settings.upload_service_base_url}/ext/media/file/upload"
        tenant_id = project.get("tenant_id")

        metadata: dict[str, Any] = {
            "tenant_id": tenant_id,
            "task_created": True,
            "external_id": project.get("external_id"),
            "media_name": file_path.name,
            "parent_id": project.get("external_id"),
            "owner": project.get("owner"),
            "created_by": project.get("created_by"),
        }

        log.info("zip.upload.start file=%s url=%s tenant_id=%s", file_path, url, tenant_id)

        content_type = media_type or "application/octet-stream"
        try:
            with file_path.open("rb") as f:
                files = {
                    "file": (file_path.name, f, content_type),
                    "metadata": (None, json.dumps(metadata), "application/json"),
                }
                resp = await self._http.post(url, files=files)
                resp.raise_for_status()
                body = resp.json()
        except Exception as exc:  # pragma: no cover - defensive
            log.error("zip.upload.failed file=%s error=%s", file_path, str(exc), exc_info=True)
            return None

        data = body.get("data") or {}
        file_id = data.get("id")
        log.info("zip.upload.done file=%s new_file_id=%s", file_path, file_id)
        return file_id

    async def _create_child_task(
        self,
        project: dict[str, Any],
        file_id: str,
        file_name: str,
    ) -> None:
        """
        Create a child task mirroring the Java createChildTask semantics.
        """
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)

        tenant_id = project.get("tenant_id")
        owner = project.get("owner")
        created_by = project.get("created_by") or owner or "system"

        # Copy task_details and add filename
        task_details = dict(project.get("task_details") or {})
        task_details["file_name"] = file_name

        child_doc: dict[str, Any] = {
            "external_id": file_id,
            "tenant_id": tenant_id,
            "org": project.get("org"),
            "status": "TASKS_ASSIGN_ANNOTATE",
            "owner": owner,
            "allocated_to": None,
            "task_details": task_details,
            "created_by": created_by,
            "updated_by": created_by,
            "created_at": now,
            "updated_at": now,
            "deleted_at": None,
            "parent_external_id": project.get("external_id"),
        }

        inserted_id = await self._repo.insert(child_doc)
        log.info(
            "zip.child_task.created tenant_id=%s parent_external_id=%s child_external_id=%s child_id=%s",
            tenant_id,
            project.get("external_id"),
            file_id,
            inserted_id,
        )

        # Emit allocation event to same stream used for normal task creation.
        settings = self._settings
        await self._redis.xadd(
            settings.redis_stream_tasks,
            {
                "event": "TASK_CREATED",
                "tenant_id": tenant_id,
                "external_id": file_id,
                "org": project.get("org"),
                "assignment": (task_details.get("task_assignment_type") or project.get("task_details", {}).get("task_assignment_type")),
                "workflow": (task_details.get("workflow_type") or project.get("task_details", {}).get("workflow_type")),
                "data_type": task_details.get("data_type") or "",
                "created_by": created_by,
            },
        )

    async def _update_project_count(self, project: dict[str, Any], created: int) -> None:
        """
        Increment child_task_count on the parent/project task.
        """
        if created <= 0:
            return

        task_details = dict(project.get("task_details") or {})
        current = int(task_details.get("child_task_count") or 0)
        new_count = current + created
        task_details["child_task_count"] = new_count

        # Simple update by external_id+tenant_id
        tenant_id = project.get("tenant_id")
        external_id = project.get("external_id")

        await self._repo._col.update_one(
            {"tenant_id": tenant_id, "external_id": external_id},
            {"$set": {"task_details": task_details, "updated_at": datetime.now(timezone.utc)}},
        )

        log.info(
            "zip.project.updated tenant_id=%s external_id=%s child_task_count=%s",
            tenant_id,
            external_id,
            new_count,
        )

