from __future__ import annotations

import fcntl
import json
import os
import re
import secrets
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .errors import CliError

SECRET_KEYS = (
    "KEY",
    "TOKEN",
    "SECRET",
    "PASSWORD",
    "CREDENTIAL",
    "AUTHORIZATION",
    "COOKIE",
)
REDACTED = "<redacted>"


def atomic_write(path: Path, data: bytes, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


def atomic_json(path: Path, value: Any) -> None:
    atomic_write(
        path,
        (
            json.dumps(value, ensure_ascii=False, indent=2, sort_keys=False) + "\n"
        ).encode(),
    )


def _fsync_directory(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


@contextmanager
def file_lock(path: Path, *, blocking: bool = False) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+")
    try:
        flags = fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB)
        try:
            fcntl.flock(handle.fileno(), flags)
        except BlockingIOError as error:
            raise CliError(75, f"another AgentQueue process holds {path}") from error
        yield
    finally:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


def daemon_is_locked(path: Path) -> bool:
    if not path.exists():
        return False
    with path.open("a+") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        return False


def new_id(prefix: str) -> str:
    return f"{prefix}_{int(__import__('time').time() * 1000):x}_{secrets.token_hex(4)}"


def looks_secret(name: str) -> bool:
    upper = name.upper()
    return any(marker in upper for marker in SECRET_KEYS)


def redact_env(env: dict[str, str]) -> dict[str, str]:
    return {
        key: (REDACTED if looks_secret(key) and value else value)
        for key, value in sorted(env.items())
    }


def normalize_topic(topic: str) -> str:
    if not topic:
        raise ValueError("topic must not be empty")
    if len(topic.encode()) > 255:
        raise ValueError("topic must be at most 255 UTF-8 bytes")
    if any(ord(character) < 32 or ord(character) == 127 for character in topic):
        raise ValueError("topic must not contain control characters")
    if "*" in topic or "#" in topic:
        raise ValueError("wildcard topics are not supported; use an exact routing key")
    return topic


NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


def normalize_skill_name(name: str) -> str:
    if not NAME.fullmatch(name) or name in {".", ".."} or ".." in name:
        raise ValueError(
            "skill name must start with a letter or digit and contain only "
            "letters, digits, '.', '_', '-' (max 64 characters)"
        )
    return name
