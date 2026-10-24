"""Filesystem contract for a single AgentQueue home."""

from __future__ import annotations

import hashlib
import os
import stat
from dataclasses import dataclass
from pathlib import Path

from .errors import CliError

HOME_ENV = "AGENTQUEUE_HOME"


def resolve_home(explicit: str | os.PathLike[str] | None = None) -> Path:
    raw = explicit or os.environ.get(HOME_ENV) or (Path.home() / ".agentqueue")
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    return path.resolve()


@dataclass(frozen=True)
class Paths:
    home: Path

    @property
    def config(self) -> Path:
        return self.home / "config.yaml"

    @property
    def env(self) -> Path:
        return self.home / ".env"

    @property
    def env_example(self) -> Path:
        return self.home / ".env.example"

    @property
    def skills(self) -> Path:
        return self.home / "skills"

    @property
    def workspaces(self) -> Path:
        return self.home / "workspaces"

    @property
    def state(self) -> Path:
        return self.home / "state"

    @property
    def jobs(self) -> Path:
        return self.state / "jobs"

    @property
    def artifacts(self) -> Path:
        return self.home / "artifacts"

    @property
    def run(self) -> Path:
        return self.home / "run"

    @property
    def socket(self) -> Path:
        path = self.run / "control.sock"
        if len(str(path).encode()) <= 100:
            return path
        runtime = os.environ.get("XDG_RUNTIME_DIR")
        base = Path(runtime) if runtime else Path("/tmp")
        digest = hashlib.sha256(str(self.home).encode()).hexdigest()[:10]
        return base / f"agentqueue-{os.getuid()}-{digest}" / "control.sock"

    @property
    def lock(self) -> Path:
        return self.state / "daemon.lock"

    @property
    def config_lock(self) -> Path:
        return self.state / "config.lock"

    @property
    def database(self) -> Path:
        return self.state / "queue.redb"

    def ensure(self) -> None:
        self.home.mkdir(parents=True, exist_ok=True)
        _private_dir(self.home)
        for path in (
            self.skills,
            self.workspaces,
            self.state,
            self.jobs,
            self.artifacts,
            self.run,
        ):
            path.mkdir(parents=True, exist_ok=True)
            _private_dir(path)


def _private_dir(path: Path) -> None:
    try:
        mode = stat.S_IMODE(path.stat().st_mode)
        if mode & 0o077:
            path.chmod(0o700)
    except OSError as error:
        raise CliError(73, f"cannot secure directory {path}: {error}") from error
