from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

from .errors import CliError
from .paths import Paths

UNIT = "agentqueue.service"


def unit_path() -> Path:
    base = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
    return base / "systemd" / "user" / UNIT


def _quote(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def render_unit(paths: Paths, grace: float = 30.0) -> str:
    python = sys.executable
    stop_timeout = max(40, int(grace) + 10)
    return f"""[Unit]
Description=AgentQueue automation daemon
Documentation=https://github.com/rizkyandriawan/agent-queue
StartLimitIntervalSec=60
StartLimitBurst=5

[Service]
Type=simple
ExecStart={_quote(python)} -m agent_queue --home {_quote(str(paths.home))} serve
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
RestartSec=3
RestartPreventExitStatus=64 78
TimeoutStartSec=30
TimeoutStopSec={stop_timeout}
KillMode=mixed
UMask=0077
Environment=PYTHONUNBUFFERED=1
StandardOutput=journal
StandardError=journal
SyslogIdentifier=agentqueue

[Install]
WantedBy=default.target
"""


def _systemctl(*arguments: str, check: bool = True) -> subprocess.CompletedProcess:
    if shutil.which("systemctl") is None:
        raise CliError(69, "systemctl is not available on this system")
    result = subprocess.run(
        ["systemctl", "--user", *arguments], text=True, capture_output=True
    )
    if check and result.returncode != 0:
        message = result.stderr.strip() or result.stdout.strip() or "systemctl failed"
        raise CliError(69, message)
    return result


def install(paths: Paths, grace: float = 30.0) -> Path:
    path = unit_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    content = render_unit(paths, grace)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(content)
    temporary.chmod(0o644)
    temporary.replace(path)
    _systemctl("daemon-reload")
    return path


def uninstall() -> None:
    path = unit_path()
    _systemctl("disable", "--now", UNIT, check=False)
    path.unlink(missing_ok=True)
    _systemctl("daemon-reload")


def enable() -> None:
    _systemctl("enable", UNIT)


def disable() -> None:
    _systemctl("disable", UNIT)


def start() -> None:
    _systemctl("start", UNIT)


def stop() -> None:
    _systemctl("stop", UNIT)


def restart() -> None:
    _systemctl("restart", UNIT)


def active() -> bool:
    result = _systemctl("is-active", UNIT, check=False)
    return result.stdout.strip() == "active"


def logs(arguments: list[str]) -> int:
    if shutil.which("journalctl") is None:
        raise CliError(
            69, "journalctl is not available; use `agentqueue serve` to see logs"
        )
    command = ["journalctl", "--user", "-u", UNIT, *arguments]
    return subprocess.call(command)


def lingering_hint() -> str:
    return (
        "To keep the service running after logout, enable user lingering:\n"
        f"  loginctl enable-linger {os.environ.get('USER', '<user>')}"
    )
