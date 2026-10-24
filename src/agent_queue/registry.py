"""Immutable, content-addressed skill revisions under the AgentQueue home."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import time
from dataclasses import dataclass
from pathlib import Path

from .errors import CliError
from .paths import Paths
from .util import atomic_json, new_id, normalize_skill_name

MAX_FILES = 1000
MAX_TOTAL_BYTES = 64 * 1024 * 1024
MAX_FILE_BYTES = 16 * 1024 * 1024


@dataclass(frozen=True)
class SkillPackage:
    name: str
    revision: str
    root: Path
    source: str

    @property
    def skill_md(self) -> Path:
        return self.root / "SKILL.md"

    @property
    def init_md(self) -> Path | None:
        path = self.root / "INIT.md"
        return path if path.is_file() else None

    @property
    def extras(self) -> Path | None:
        path = self.root / "extras"
        return path if path.is_dir() else None

    @property
    def scripts(self) -> Path | None:
        path = self.root / "scripts"
        return path if path.is_dir() else None

    @property
    def has_init(self) -> bool:
        return self.init_md is not None

    def read_skill(self) -> str:
        text = self.skill_md.read_text()
        if not text.strip():
            raise CliError(65, f"{self.skill_md} is empty")
        return text

    def read_init(self) -> str:
        if self.init_md is None:
            return ""
        return self.init_md.read_text()


def _manifest_path(paths: Paths, name: str) -> Path:
    return paths.skills / name / "manifest.json"


def _read_manifest(paths: Paths, name: str) -> dict:
    path = _manifest_path(paths, name)
    try:
        return json.loads(path.read_text())
    except FileNotFoundError as error:
        raise CliError(65, f"skill {name!r} is not installed") from error
    except (OSError, json.JSONDecodeError) as error:
        raise CliError(70, f"cannot read skill manifest {path}: {error}") from error


def _hash_tree(root: Path) -> tuple[str, int, int]:
    digest = hashlib.sha256()
    files = 0
    total = 0
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        relative = path.relative_to(root).as_posix()
        data = path.read_bytes()
        files += 1
        total += len(data)
        if files > MAX_FILES:
            raise CliError(65, f"skill package exceeds {MAX_FILES} files")
        if len(data) > MAX_FILE_BYTES:
            raise CliError(65, f"{relative} exceeds {MAX_FILE_BYTES} bytes")
        if total > MAX_TOTAL_BYTES:
            raise CliError(65, f"skill package exceeds {MAX_TOTAL_BYTES} bytes")
        digest.update(relative.encode())
        digest.update(b"\0")
        digest.update(len(data).to_bytes(8, "big"))
        digest.update(data)
    return digest.hexdigest(), files, total


def _copy_file(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target, follow_symlinks=False)
    executable = bool(source.stat().st_mode & 0o111)
    target.chmod(0o700 if executable else 0o600)


def _copy_tree(source: Path, target: Path) -> None:
    for path in source.rglob("*"):
        if path.is_symlink():
            raise CliError(65, f"symlinks are not allowed in skill packages: {path}")
        mode = path.lstat().st_mode
        if stat.S_ISDIR(mode):
            continue
        if not stat.S_ISREG(mode):
            raise CliError(
                65, f"special files are not allowed in skill packages: {path}"
            )
        if path.name.startswith("."):
            continue
        _copy_file(path, target / path.relative_to(source))


def _collect(source: Path, staging: Path) -> None:
    if source.is_symlink():
        raise CliError(65, f"symlinks are not allowed as skill sources: {source}")
    if not source.is_file():
        raise CliError(65, f"skill source is not a file: {source}")
    if source.name == "SKILL.md":
        root = source.parent
        _copy_file(source, staging / "SKILL.md")
        init = root / "INIT.md"
        if init.exists():
            if init.is_symlink() or not init.is_file():
                raise CliError(65, f"INIT.md must be a regular file: {init}")
            _copy_file(init, staging / "INIT.md")
        for name in ("extras", "scripts"):
            directory = root / name
            if directory.exists():
                if directory.is_symlink() or not directory.is_dir():
                    raise CliError(
                        65, f"{name} must be a regular directory: {directory}"
                    )
                _copy_tree(directory, staging / name)
    else:
        _copy_file(source, staging / "SKILL.md")


def import_skill(
    paths: Paths, source: str | os.PathLike[str], name: str | None = None
) -> SkillPackage:
    source_path = Path(source).expanduser()
    if source_path.is_symlink():
        raise CliError(65, f"symlinks are not allowed as skill sources: {source_path}")
    source_path = source_path.resolve()
    if not source_path.is_file():
        raise CliError(65, f"skill file not found: {source_path}")
    inferred = name or (
        source_path.parent.name if source_path.name == "SKILL.md" else source_path.stem
    )
    try:
        skill_name = normalize_skill_name(inferred)
    except ValueError as error:
        raise CliError(65, str(error)) from error

    paths.skills.mkdir(parents=True, exist_ok=True)
    staging = paths.skills / f".staging-{new_id('s')}"
    try:
        staging.mkdir(mode=0o700)
        _collect(source_path, staging)
        revision, files, total = _hash_tree(staging)
        revisions = paths.skills / skill_name / "revisions"
        revisions.mkdir(parents=True, exist_ok=True)
        target = revisions / revision
        if target.exists():
            shutil.rmtree(staging)
        else:
            os.replace(staging, target)
        manifest_path = _manifest_path(paths, skill_name)
        manifest = (
            json.loads(manifest_path.read_text())
            if manifest_path.exists()
            else {"name": skill_name, "current": None, "revisions": {}}
        )
        manifest["revisions"][revision] = {
            "imported_at": time.time(),
            "files": files,
            "bytes": total,
            "has_init": (target / "INIT.md").is_file(),
            "source": str(source_path),
        }
        manifest["current"] = revision
        manifest["updated_at"] = time.time()
        atomic_json(manifest_path, manifest)
        return SkillPackage(skill_name, revision, target, str(source_path))
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def load_skill(paths: Paths, name: str, revision: str | None = None) -> SkillPackage:
    manifest = _read_manifest(paths, name)
    selected = revision or manifest.get("current")
    if not selected or selected not in manifest.get("revisions", {}):
        raise CliError(65, f"revision {selected!r} is not installed for skill {name!r}")
    root = paths.skills / name / "revisions" / selected
    if not (root / "SKILL.md").is_file():
        raise CliError(65, f"skill revision is incomplete: {root}")
    return SkillPackage(
        name, selected, root, manifest["revisions"][selected].get("source", "")
    )


def list_skills(paths: Paths) -> list[dict]:
    if not paths.skills.is_dir():
        return []
    result = []
    for entry in sorted(paths.skills.iterdir()):
        if not entry.is_dir() or entry.name.startswith("."):
            continue
        manifest_path = entry / "manifest.json"
        if not manifest_path.is_file():
            continue
        try:
            manifest = json.loads(manifest_path.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        result.append(manifest)
    return result


def delete_skill(paths: Paths, name: str, *, delete_workspace: bool = False) -> None:
    directory = paths.skills / name
    if not directory.exists():
        raise CliError(65, f"skill {name!r} is not installed")
    shutil.rmtree(directory)
    if delete_workspace:
        shutil.rmtree(paths.workspaces / name, ignore_errors=True)


def prune_unreferenced_revisions(paths: Paths, name: str, keep: set[str]) -> None:
    """Remove old revisions only when no job/route references them."""
    revisions = paths.skills / name / "revisions"
    if not revisions.is_dir():
        return
    for entry in revisions.iterdir():
        if entry.is_dir() and entry.name not in keep:
            shutil.rmtree(entry, ignore_errors=True)
