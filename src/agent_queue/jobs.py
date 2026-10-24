from __future__ import annotations

import base64
import json
import threading
import time
from pathlib import Path
from typing import Any

from .paths import Paths
from .util import atomic_json


class JobStore:
    """Human-readable job history. Ante remains the delivery source of truth."""

    def __init__(self, paths: Paths) -> None:
        self.paths = paths
        self._lock = threading.RLock()

    def _path(self, job_id: str) -> Path:
        return self.paths.jobs / f"{job_id}.json"

    def create(self, job_id: str, **fields: Any) -> dict:
        with self._lock:
            if self._path(job_id).exists():
                return self.get(job_id)
            record = {
                "job_id": job_id,
                "status": "running",
                "accepted_at": time.time(),
                "attempts": [],
                **fields,
            }
            atomic_json(self._path(job_id), record)
            return record

    def update(self, job_id: str, **fields: Any) -> dict:
        with self._lock:
            record = self.get(job_id)
            record.update(fields)
            atomic_json(self._path(job_id), record)
            return record

    def start_attempt(
        self, job_id: str, attempt: int, *, workspace: str, revision: str
    ) -> dict:
        with self._lock:
            record = self.get(job_id)
            record["status"] = "running"
            record["started_at"] = record.get("started_at") or time.time()
            record["workspace"] = workspace
            record["revision"] = revision
            record["attempts"].append(
                {"attempt": attempt, "started_at": time.time(), "status": "running"}
            )
            atomic_json(self._path(job_id), record)
            return record

    def finish_attempt(
        self, job_id: str, attempt: int, status: str, **fields: Any
    ) -> dict:
        with self._lock:
            record = self.get(job_id)
            for entry in reversed(record.get("attempts", [])):
                if entry.get("attempt") == attempt:
                    entry.update(status=status, finished_at=time.time(), **fields)
                    break
            record["status"] = status
            record["updated_at"] = time.time()
            record.update(
                {key: value for key, value in fields.items() if key != "error"}
            )
            if fields.get("error"):
                record["last_error"] = fields["error"]
            if status in {"succeeded", "failed"}:
                record["finished_at"] = time.time()
            atomic_json(self._path(job_id), record)
            return record

    def schedule_retry(
        self, job_id: str, attempt: int, error: str, next_attempt_at: float
    ) -> dict:
        return self.finish_attempt(
            job_id,
            attempt,
            "retry_wait",
            error=error,
            next_attempt_at=next_attempt_at,
        )

    def store_failure_payload(self, job_id: str, payload: bytes) -> None:
        self.update(job_id, payload_base64=base64.b64encode(payload).decode())

    def mark_retried(self, job_id: str, new_job_id: str) -> None:
        self.update(job_id, retried_as=new_job_id, retried_at=time.time())

    def get(self, job_id: str) -> dict:
        path = self._path(job_id)
        try:
            return json.loads(path.read_text())
        except FileNotFoundError as error:
            raise KeyError(job_id) from error
        except (OSError, json.JSONDecodeError) as error:
            raise RuntimeError(f"cannot read job {path}: {error}") from error

    def list(self, limit: int = 50) -> list[dict]:
        records = []
        for path in self.paths.jobs.glob("*.json"):
            try:
                records.append(json.loads(path.read_text()))
            except (OSError, json.JSONDecodeError):
                continue
        records.sort(key=lambda item: item.get("accepted_at", 0), reverse=True)
        return records[:limit]
