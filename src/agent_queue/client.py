from __future__ import annotations

import json
import socket
import time
from typing import Any

from .errors import CliError, DaemonUnavailable
from .paths import Paths

MAX_RESPONSE = 4 * 1024 * 1024


def request(
    paths: Paths, command: str, arguments: dict | None = None, timeout: float = 30.0
) -> dict[str, Any]:
    payload = json.dumps(
        {"version": 1, "command": command, "arguments": arguments or {}}
    )
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.settimeout(timeout)
    try:
        client.connect(str(paths.socket))
        client.sendall((payload + "\n").encode())
        chunks = bytearray()
        while b"\n" not in chunks and len(chunks) <= MAX_RESPONSE:
            chunk = client.recv(65536)
            if not chunk:
                break
            chunks.extend(chunk)
    except FileNotFoundError as error:
        raise DaemonUnavailable(
            f"AgentQueue daemon is not running for {paths.home} (no control socket)"
        ) from error
    except ConnectionRefusedError as error:
        raise DaemonUnavailable(
            f"AgentQueue daemon is not running for {paths.home}"
        ) from error
    except socket.timeout as error:
        raise CliError(75, "control request timed out") from error
    except OSError as error:
        raise DaemonUnavailable(f"cannot reach AgentQueue daemon: {error}") from error
    finally:
        client.close()
    try:
        response = json.loads(bytes(chunks).split(b"\n", 1)[0] or b"{}")
    except json.JSONDecodeError as error:
        raise CliError(70, "invalid response from daemon") from error
    if not response.get("ok"):
        error = response.get("error") or {}
        raise CliError(75, error.get("message", "daemon rejected the request"))
    return response.get("data", {})


def daemon_online(paths: Paths) -> bool:
    try:
        request(paths, "ping", timeout=1)
        return True
    except (CliError, DaemonUnavailable):
        return False


def wait_operation(paths: Paths, operation_id: str, timeout: float) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        operation = request(paths, "op.status", {"id": operation_id})
        if operation.get("status") in {"succeeded", "failed"}:
            if operation["status"] == "failed":
                raise CliError(65, operation.get("error", "preparation failed"))
            return operation
        time.sleep(0.2)
    raise CliError(75, f"operation {operation_id} did not finish within {timeout:.0f}s")


def wait_route(paths: Paths, topic: str, timeout: float) -> dict:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        status = request(paths, "status")
        for route in status.get("routes", []):
            if route["topic"] == topic:
                last = route
                if route["state"] == "ready":
                    return route
                if route["state"] == "init_failed":
                    raise CliError(65, f"initialization failed: {route.get('error')}")
        time.sleep(0.2)
    raise CliError(
        75, f"topic {topic} is not ready within {timeout:.0f}s (last: {last})"
    )
