from __future__ import annotations

import threading
import time
from pathlib import Path

import pika

from agent_queue.config import default_config, read_raw, write_raw
from agent_queue.paths import Paths
from agent_queue.registry import import_skill


def home(tmp: Path) -> Paths:
    paths = Paths(tmp / "home")
    paths.ensure()
    config = default_config()
    config["amqp"]["port"] = 0
    config["logging"]["level"] = "error"
    config["runtime"]["max_parallel_jobs"] = 4
    config["runtime"]["shutdown_grace_seconds"] = 5
    config["defaults"]["retry"] = {
        "initial_seconds": 0.05,
        "maximum_seconds": 0.1,
        "jitter": False,
    }
    write_raw(paths.config, config)
    paths.env.write_text("AGENT_MODEL=test-model\nAGENT_API_KEY=test-key\n")
    return paths


def make_skill(
    tmp: Path,
    name: str = "demo",
    *,
    init: bool = True,
    skill: str = "Process the message.",
) -> Path:
    root = tmp / f"src-{name}"
    root.mkdir(parents=True)
    (root / "SKILL.md").write_text(f"# {name}\n\n{skill}\n")
    if init:
        (root / "INIT.md").write_text("# Setup\n\nPrepare the workspace.\n")
    (root / "extras").mkdir()
    (root / "extras" / "api.md").write_text("API documentation\n")
    return root


def install(paths: Paths, tmp: Path, name: str = "demo", **kwargs):
    return import_skill(paths, make_skill(tmp, name, **kwargs) / "SKILL.md", name)


def add_route(
    paths: Paths,
    topic: str,
    skill,
    *,
    concurrency: int = 1,
    max_attempts: int | None = None,
) -> None:
    raw = read_raw(paths.config)
    raw.setdefault("routes", {})[topic] = {
        "skill": skill.name,
        "revision": skill.revision,
        "concurrency": concurrency,
        "enabled": True,
        **({"max_attempts": max_attempts} if max_attempts else {}),
    }
    write_raw(paths.config, raw)


def publish(
    host: str, port: int, topic: str, body: bytes, message_id: str | None = None
) -> None:
    connection = pika.BlockingConnection(pika.ConnectionParameters(host, port))
    try:
        channel = connection.channel()
        channel.confirm_delivery()
        channel.basic_publish(
            "",
            topic,
            body,
            properties=pika.BasicProperties(
                message_id=message_id,
                content_type="application/octet-stream",
                delivery_mode=2,
            ),
        )
    finally:
        connection.close()


def wait_for(predicate, timeout: float = 5.0, interval: float = 0.02):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    raise AssertionError("condition was not met in time")


class FakeRunner:
    def __init__(self) -> None:
        self.init_calls = []
        self.messages = []
        self.fail_init = False
        self.failures = 0
        self.delay = 0.0
        self.barrier: threading.Barrier | None = None
        self.lock = threading.Lock()
        self.active = 0
        self.peak = 0

    def run_init(self, skill, workspace, artifact_dir, env, *, timeout, max_iterations):
        self.init_calls.append(skill.revision)
        if self.fail_init:
            raise RuntimeError("setup failed")
        (workspace / "init.txt").write_text("ready")
        return "initialized"

    def run_message(
        self,
        skill,
        workspace,
        artifact_dir,
        env,
        raw,
        *,
        timeout,
        max_iterations,
        metadata,
    ):
        with self.lock:
            self.messages.append((skill.revision, raw, metadata))
            self.active += 1
            self.peak = max(self.peak, self.active)
            fail = self.failures > 0
            if fail:
                self.failures -= 1
        try:
            if self.barrier is not None:
                self.barrier.wait(timeout=3)
            if self.delay:
                time.sleep(self.delay)
            if fail:
                raise RuntimeError("transient failure")
            (artifact_dir / "message.txt").write_text(raw.decode())
            return "done"
        finally:
            with self.lock:
                self.active -= 1
