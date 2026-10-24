from __future__ import annotations

import json
import logging
import logging.handlers
import sys
from datetime import datetime, timezone
from pathlib import Path

from .config import LoggingConfig
from .paths import Paths

RESERVED = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
    "taskName",
    "message",
    "asctime",
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname.lower(),
            "logger": record.name,
        }
        event = getattr(record, "event", None)
        if event:
            payload["event"] = event
        for key, value in record.__dict__.items():
            if key not in RESERVED and key != "event" and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["error"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


class TextFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        event = getattr(record, "event", "")
        fields = {
            key: value
            for key, value in record.__dict__.items()
            if key not in RESERVED and key != "event" and not key.startswith("_")
        }
        suffix = (
            " " + " ".join(f"{key}={value}" for key, value in fields.items())
            if fields
            else ""
        )
        stamp = datetime.fromtimestamp(record.created, tz=timezone.utc).strftime(
            "%H:%M:%S"
        )
        return f"{stamp} {record.levelname:<7} {event or record.name}{suffix}{self._exception(record)}"

    def _exception(self, record: logging.LogRecord) -> str:
        if not record.exc_info:
            return ""
        return "\n" + self.formatException(record.exc_info)


def configure_logging(config: LoggingConfig, paths: Paths) -> None:
    root = logging.getLogger()
    root.setLevel(config.level.upper())
    for handler in list(root.handlers):
        root.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass
    if config.destination == "file":
        path = Path(config.path or "")
        if not path.is_absolute():
            path = paths.home / path
        path.parent.mkdir(parents=True, exist_ok=True)
        handler = logging.handlers.RotatingFileHandler(
            path, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
        )
    else:
        handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(
        JsonFormatter() if config.format == "json" else TextFormatter()
    )
    root.addHandler(handler)
    # Third-party clients can be extremely chatty and may log request bodies.
    for name in (
        "httpx",
        "httpcore",
        "openai",
        "agentscope",
        "urllib3",
        "asyncio",
        "pika",
    ):
        logging.getLogger(name).setLevel(logging.WARNING)


def event(logger: logging.Logger, level: int, name: str, **fields) -> None:
    logger.log(level, name, extra={"event": name, **fields})
