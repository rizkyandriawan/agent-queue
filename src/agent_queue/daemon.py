"""Foreground AgentQueue daemon: AMQP ingress, scheduling, init, control."""

from __future__ import annotations

import base64
import hashlib
import heapq
import json
import logging
import os
import random
import signal
import socket
import struct
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import Any


from ante import Ante, AnteError

from . import __version__
from .config import Config, Route, load_config
from .errors import CliError, ConfigError, Conflict
from .jobs import JobStore
from .logging_setup import configure_logging, event
from .paths import Paths
from .registry import SkillPackage, load_skill
from .runner import AgentScopeRunner
from .util import new_id

INIT_CONTRACT_VERSION = "1"
MAX_REQUEST_BYTES = 1024 * 1024
log = logging.getLogger("agentqueue")


def _revision_of(config: Config, routes: dict[str, Route]) -> str:
    payload = json.dumps(
        {
            "config": config.to_dict(),
            "routes": {
                k: v.skill + ":" + (v.revision or "") for k, v in routes.items()
            },
        },
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:12]


class RouteRuntime:
    def __init__(self, daemon: Daemon, route: Route, skill: SkillPackage) -> None:
        self.daemon = daemon
        self.route = route
        self.skill = skill
        self.state = "initializing"
        self.error: str | None = None
        self.workspace: Path | None = None
        self.generation: str | None = None
        self.identity: str | None = None
        self.paused = not route.enabled
        self.started_at = time.time()
        self.last_success_at: float | None = None
        self.last_error: str | None = None
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None
        self._init_lock = threading.Lock()
        self._init_future: Future | None = None

    @property
    def topic(self) -> str:
        return self.route.topic

    @property
    def ready(self) -> bool:
        return self.state == "ready" and not self.paused

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(5)

    def wake(self) -> None:
        self._wake.set()

    def prepare(self, force: bool = False) -> dict:
        with self._init_lock:
            self.state = "initializing"
            self.error = None
            event(
                log,
                logging.INFO,
                "init.started",
                topic=self.topic,
                skill=self.skill.name,
                revision=self.skill.revision[:12],
                force=force,
            )
            try:
                result = self._prepare_locked(force)
                self.state = "ready"
                self.error = None
                event(
                    log,
                    logging.INFO,
                    "init.completed",
                    topic=self.topic,
                    skill=self.skill.name,
                    generation=self.generation,
                )
                return result
            except BaseException as error:
                self.state = "init_failed"
                self.error = f"{type(error).__name__}: {error}"
                self.last_error = self.error
                event(
                    log,
                    logging.ERROR,
                    "init.failed",
                    topic=self.topic,
                    skill=self.skill.name,
                    error=self.error,
                )
                raise

    def _prepare_locked(self, force: bool) -> dict:
        identity = hashlib.sha256(
            "\0".join(
                [
                    self.skill.revision,
                    INIT_CONTRACT_VERSION,
                    self.daemon.config.init.revision,
                ]
            ).encode()
        ).hexdigest()
        base = self.daemon.paths.workspaces / self.skill.name
        base.mkdir(parents=True, exist_ok=True)
        active_path = base / "active.json"
        if not force and active_path.is_file():
            try:
                active = json.loads(active_path.read_text())
            except (OSError, json.JSONDecodeError):
                active = {}
            workspace = base / str(active.get("generation", ""))
            if active.get("identity") == identity and workspace.is_dir():
                self.identity = identity
                self.generation = active["generation"]
                self.workspace = workspace
                return {
                    "identity": identity,
                    "generation": self.generation,
                    "reused": True,
                }

        candidate = base / f".candidate-{identity[:16]}-{os.getpid()}"
        if candidate.exists():
            import shutil

            shutil.rmtree(candidate)
        candidate.mkdir(parents=True, mode=0o700)
        self._copy_package(candidate)
        self.daemon._write_event_file(
            self.daemon.paths.artifacts / "init" / self.skill.name,
            {
                "topic": self.topic,
                "skill": self.skill.name,
                "revision": self.skill.revision,
                "identity": identity,
                "generation": candidate.name,
                "phase": "init",
            },
        )
        try:
            if self.skill.has_init:
                self.daemon.runner.run_init(
                    self.skill,
                    candidate,
                    self.daemon.paths.artifacts / "init" / self.skill.name,
                    self.daemon.env,
                    timeout=self.route.timeout_seconds,
                    max_iterations=self.route.max_iterations,
                )
            final = base / f"gen-{identity[:16]}"
            if final.exists():
                final = base / f"gen-{identity[:16]}-{int(time.time())}"
            os.replace(candidate, final)
            generation = final.name
            active = {
                "identity": identity,
                "generation": generation,
                "skill": self.skill.name,
                "revision": self.skill.revision,
                "completed_at": time.time(),
            }
            from .util import atomic_json

            atomic_json(active_path, active)
        except BaseException:
            import shutil

            shutil.rmtree(candidate, ignore_errors=True)
            raise
        self.identity = identity
        self.generation = generation
        self.workspace = final
        return {"identity": identity, "generation": generation, "reused": False}

    def _copy_package(self, destination: Path) -> None:
        import shutil

        if self.skill.extras:
            shutil.copytree(self.skill.extras, destination / "extras")
        if self.skill.scripts:
            shutil.copytree(self.skill.scripts, destination / "scripts")
        (destination / "jobs").mkdir(exist_ok=True)

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._dispatch, name=f"route-{self.topic}", daemon=True
        )
        self._thread.start()

    def _dispatch(self) -> None:
        while not self._stop.is_set():
            if not self.ready or self.workspace is None:
                self._wake.wait(0.2)
                self._wake.clear()
                continue
            capacity = self.daemon._capacity(self)
            if capacity <= 0:
                self._wake.wait(0.2)
                self._wake.clear()
                continue
            lease = int(
                (
                    self.route.timeout_seconds
                    + self.daemon.config.defaults.retry.maximum_seconds
                    + 60
                )
                * 1000
            )
            try:
                deliveries = self.daemon.ante.take(
                    self.topic,
                    max=capacity,
                    lease_ms=lease,
                    wait_ms=200,
                    parse=lambda raw, _kind: raw,
                )
            except Exception as error:  # storage, decoding, or queue replacement
                event(
                    log,
                    logging.ERROR,
                    "ingress.error",
                    topic=self.topic,
                    error=str(error),
                )
                try:
                    self.daemon.ante.declare(self.topic)
                except AnteError:
                    pass
                self._stop.wait(1)
                continue
            if not deliveries:
                continue
            allowed = self.daemon._admit(self, len(deliveries))
            for delivery in deliveries[:allowed]:
                try:
                    self.daemon.pool.submit(self._execute, delivery)
                except RuntimeError:
                    self.daemon._release(self.topic, 1)
                    try:
                        delivery.requeue()
                    except AnteError:
                        pass
            for delivery in deliveries[allowed:]:
                try:
                    delivery.requeue()
                except AnteError:
                    pass

    def _execute(self, delivery) -> None:
        attempt = delivery.attempts
        job_id = f"j_{delivery.message_id}"
        started = time.time()
        bound_skill = self.skill
        bound_workspace = self.workspace
        bound_attempts = self.route.max_attempts
        bound_timeout = self.route.timeout_seconds
        bound_iterations = self.route.max_iterations
        try:
            if self.daemon.stopping.is_set():
                delivery.requeue()
                return
            try:
                existing = self.daemon.jobs.get(job_id)
            except KeyError:
                existing = None
            if existing:
                try:
                    bound_skill = load_skill(
                        self.daemon.paths,
                        existing.get("skill", self.skill.name),
                        existing.get("revision") or self.skill.revision,
                    )
                except CliError as error:
                    event(
                        log,
                        logging.ERROR,
                        "job.revision_missing",
                        job_id=job_id,
                        error=str(error),
                    )
                candidate = (
                    self.daemon.paths.workspaces
                    / existing.get("skill", self.skill.name)
                    / str(existing.get("workspace", ""))
                )
                if candidate.is_dir():
                    bound_workspace = candidate
                bound_attempts = int(existing.get("max_attempts", bound_attempts))
                bound_timeout = float(existing.get("timeout_seconds", bound_timeout))
                bound_iterations = int(existing.get("max_iterations", bound_iterations))
            self.daemon.jobs.create(
                job_id,
                topic=self.topic,
                skill=bound_skill.name,
                revision=bound_skill.revision,
                workspace=bound_workspace.name if bound_workspace else None,
                ante_message_id=delivery.message_id,
                application_message_id=delivery.application_message_id,
                correlation_id=delivery.correlation_id,
                content_type=delivery.content_type,
                payload_bytes=len(delivery.raw),
                max_attempts=bound_attempts,
                timeout_seconds=bound_timeout,
                max_iterations=bound_iterations,
            )
            self.daemon.jobs.start_attempt(
                job_id,
                attempt,
                workspace=bound_workspace.name if bound_workspace else "",
                revision=bound_skill.revision,
            )
            event(
                log,
                logging.INFO,
                "job.started",
                topic=self.topic,
                job_id=job_id,
                attempt=attempt,
                skill=bound_skill.name,
                revision=bound_skill.revision[:12],
            )
            artifacts = self.daemon.paths.artifacts / job_id
            artifacts.mkdir(parents=True, exist_ok=True)
            if bound_workspace is None:
                raise RuntimeError("job has no prepared workspace")
            outcome = self.daemon.runner.run_message(
                bound_skill,
                bound_workspace,
                artifacts,
                self.daemon.env,
                delivery.raw,
                timeout=bound_timeout,
                max_iterations=bound_iterations,
                metadata={
                    "message_id": delivery.message_id,
                    "application_message_id": delivery.application_message_id,
                    "correlation_id": delivery.correlation_id,
                    "content_type": delivery.content_type,
                    "attempt": attempt,
                },
            )
            result = {
                "job_id": job_id,
                "topic": self.topic,
                "skill": bound_skill.name,
                "revision": bound_skill.revision,
                "attempt": attempt,
                "status": "succeeded",
                "outcome": outcome,
                "message_id": delivery.message_id,
                "application_message_id": delivery.application_message_id,
                "correlation_id": delivery.correlation_id,
                "finished_at": time.time(),
            }
            self.daemon.ante.publish(
                f"{self.topic}.results",
                result,
                content_type="application/json",
                correlation_id=delivery.correlation_id,
            )
            self.daemon.jobs.finish_attempt(
                job_id,
                attempt,
                "succeeded",
                outcome=outcome,
                skill=bound_skill.name,
                revision=bound_skill.revision,
            )
            self.last_success_at = time.time()
            self.last_error = None
            self.daemon._write_event_file(
                self.daemon.paths.artifacts / job_id,
                {"phase": "message", "status": "succeeded", **result},
            )
            delivery.ack()
            event(
                log,
                logging.INFO,
                "job.completed",
                topic=self.topic,
                job_id=job_id,
                attempt=attempt,
                duration_ms=int((time.time() - started) * 1000),
            )
        except BaseException as error:
            self._handle_failure(
                delivery,
                job_id,
                attempt,
                error,
                bound_skill,
                bound_workspace,
                bound_attempts,
            )
        finally:
            self.daemon._release(self.topic, 1)
            self._wake.set()

    def _handle_failure(
        self,
        delivery,
        job_id: str,
        attempt: int,
        error: BaseException,
        skill: SkillPackage,
        workspace: Path | None,
        max_attempts: int,
    ) -> None:
        try:
            self._record_failure(
                delivery, job_id, attempt, error, skill, workspace, max_attempts
            )
        except Exception as record_error:
            event(
                log,
                logging.ERROR,
                "job.failure_record_error",
                job_id=job_id,
                error=f"{type(record_error).__name__}: {record_error}",
            )
            try:
                delivery.requeue()
            except AnteError:
                pass

    def _record_failure(
        self,
        delivery,
        job_id: str,
        attempt: int,
        error: BaseException,
        skill: SkillPackage,
        workspace: Path | None,
        max_attempts: int,
    ) -> None:
        message = f"{type(error).__name__}: {error}"[:2000]
        self.last_error = message
        event(
            log,
            logging.WARNING,
            "job.failed_attempt",
            topic=self.topic,
            job_id=job_id,
            attempt=attempt,
            error=message,
        )
        if attempt >= max_attempts:
            failure = {
                "job_id": job_id,
                "topic": self.topic,
                "skill": skill.name,
                "revision": skill.revision,
                "attempt": attempt,
                "status": "failed",
                "error": message,
                "message_id": delivery.message_id,
                "application_message_id": delivery.application_message_id,
                "correlation_id": delivery.correlation_id,
                "finished_at": time.time(),
            }
            try:
                self.daemon.jobs.store_failure_payload(job_id, delivery.raw)
                self.daemon.ante.publish(
                    f"{self.topic}.failed",
                    failure,
                    content_type="application/json",
                    correlation_id=delivery.correlation_id,
                )
            except (AnteError, OSError) as publish_error:
                event(
                    log,
                    logging.ERROR,
                    "job.failure_record_error",
                    job_id=job_id,
                    error=str(publish_error),
                )
            self.daemon.jobs.finish_attempt(job_id, attempt, "failed", error=message)
            self.daemon._write_event_file(
                self.daemon.paths.artifacts / job_id,
                {"phase": "message", **failure},
            )
            try:
                delivery.ack()
            except AnteError as ack_error:
                event(
                    log,
                    logging.ERROR,
                    "job.ack_failed",
                    job_id=job_id,
                    error=str(ack_error),
                )
            return
        delay = min(
            self.daemon.config.defaults.retry.initial_seconds * (2 ** (attempt - 1)),
            self.daemon.config.defaults.retry.maximum_seconds,
        )
        if self.daemon.config.defaults.retry.jitter:
            delay *= random.uniform(0.8, 1.2)
        due = time.time() + delay
        self.daemon.jobs.schedule_retry(job_id, attempt, message, due)
        try:
            self.daemon.retries.add(delivery, due, self.topic)
            event(
                log,
                logging.INFO,
                "job.retry_scheduled",
                topic=self.topic,
                job_id=job_id,
                attempt=attempt,
                delay=round(delay, 2),
            )
        except Exception:
            try:
                delivery.requeue()
            except AnteError:
                pass

    def status(self) -> dict:
        info: dict[str, Any] = {}
        try:
            info = self.daemon.ante.info(self.topic)
        except AnteError:
            pass
        return {
            "topic": self.topic,
            "skill": self.skill.name,
            "revision": self.skill.revision,
            "state": "paused" if self.paused and self.state == "ready" else self.state,
            "error": self.error,
            "paused": self.paused,
            "concurrency": self.route.concurrency,
            "active": self.daemon._active.get(self.topic, 0),
            "workspace": self.generation,
            "queue": {
                "ready": info.get("ready", 0),
                "inflight": info.get("inflight", 0),
                "total": info.get("total", 0),
            },
            "last_success_at": self.last_success_at,
            "last_error": self.last_error,
        }


class RetryScheduler:
    def __init__(self, daemon: Daemon) -> None:
        self.daemon = daemon
        self._heap: list[tuple[float, int, Any, str]] = []
        self._counter = 0
        self._lock = threading.Condition()
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run, name="retry-scheduler", daemon=True
        )

    def start(self) -> None:
        self._thread.start()

    def add(self, delivery, due: float, topic: str) -> None:
        with self._lock:
            self._counter += 1
            heapq.heappush(self._heap, (due, self._counter, delivery, topic))
            self._lock.notify()

    def _run(self) -> None:
        while not self._stop.is_set():
            due_item = None
            with self._lock:
                if not self._heap:
                    self._lock.wait(0.5)
                if self._heap:
                    due, _, delivery, topic = self._heap[0]
                    now = time.time()
                    if due <= now:
                        due_item = heapq.heappop(self._heap)
                    else:
                        self._lock.wait(min(due - now, 0.5))
            if due_item:
                _, _, delivery, topic = due_item
                try:
                    delivery.requeue()
                    self.daemon._wake_route(topic)
                except AnteError as error:
                    event(
                        log,
                        logging.WARNING,
                        "retry.requeue_failed",
                        topic=topic,
                        error=str(error),
                    )

    def drain(self) -> None:
        self._stop.set()
        with self._lock:
            self._lock.notify_all()
            held = list(self._heap)
            self._heap.clear()
        for _, _, delivery, _topic in held:
            try:
                delivery.requeue()
            except AnteError:
                pass


class Daemon:
    def __init__(
        self,
        paths: Paths,
        config: Config | None = None,
        env: dict[str, str] | None = None,
        runner=None,
    ) -> None:
        self.paths = paths
        self.config = config
        self.env = env or {}
        self.runner = runner
        self.ante: Ante | None = None
        self.jobs = JobStore(paths)
        self.routes: dict[str, RouteRuntime] = {}
        self.pool: ThreadPoolExecutor | None = None
        self.init_pool: ThreadPoolExecutor | None = None
        self.retries = RetryScheduler(self)
        self.stopping = threading.Event()
        self._reload_requested = threading.Event()
        self._lock_handle = None
        self._control: socket.socket | None = None
        self._control_thread: threading.Thread | None = None
        self._active: dict[str, int] = {}
        self._global_active = 0
        self._admission = threading.Condition()
        self._operations: dict[str, dict] = {}
        self._reload_lock = threading.Lock()
        self._state = "stopped"
        self.started_at: float | None = None
        self.amqp_address: tuple[str, int] | None = None
        self.desired_revision = "0" * 12
        self.active_revision = "0" * 12
        self.last_reload_error: str | None = None
        self._pid = os.getpid()

    # ---------- lifecycle ----------

    def start(self) -> None:
        self.paths.ensure()
        self._lock_handle = self._acquire_lock()
        self._state = "starting"
        try:
            self.config, self.env = load_config(self.paths)
            self.config.model_settings(self.env)
            configure_logging(self.config.logging, self.paths)
            self.runner = self.runner or AgentScopeRunner(self.config)
            self.pool = ThreadPoolExecutor(
                max_workers=self.config.runtime.max_parallel_jobs,
                thread_name_prefix="agentqueue-job",
            )
            self.init_pool = ThreadPoolExecutor(
                max_workers=self.config.runtime.max_parallel_init,
                thread_name_prefix="agentqueue-init",
            )
            self.ante = Ante.open(self.paths.database)
            event(
                log,
                logging.INFO,
                "daemon.starting",
                version=__version__,
                pid=self._pid,
                home=str(self.paths.home),
            )
            self.desired_revision = _revision_of(self.config, self.config.routes)
            self._control = self._open_control()
            self._control_thread = threading.Thread(
                target=self._control_loop, name="control", daemon=True
            )
            self._control_thread.start()
            security_path = None
            if self.config.amqp.security:
                candidate = Path(self.config.amqp.security).expanduser()
                if not candidate.is_absolute():
                    candidate = self.paths.home / candidate
                security_path = str(candidate)
            self.amqp_address = self.ante.serve_amqp(
                self.config.amqp.host,
                self.config.amqp.port,
                security_path=security_path,
            )
            self._build_routes(self.config.routes, initial=True)
            if self.retries._thread.ident is None:
                self.retries.start()
            self.active_revision = self.desired_revision
            self.started_at = time.time()
            self._state = "running"
            event(
                log,
                logging.INFO,
                "daemon.ready",
                amqp=f"{self.amqp_address[0]}:{self.amqp_address[1]}",
                tls=bool(self.config.amqp.security),
                routes=len(self.routes),
                revision=self.active_revision,
            )
        except BaseException:
            self.stop()
            raise

    def run(self) -> int:
        self.start()
        previous = {}
        if threading.current_thread() is threading.main_thread():
            for sig in (signal.SIGTERM, signal.SIGINT):
                previous[sig] = signal.signal(sig, lambda *_: self.stopping.set())
            if hasattr(signal, "SIGHUP"):
                previous[signal.SIGHUP] = signal.signal(
                    signal.SIGHUP, lambda *_: self._reload_requested.set()
                )
        try:
            while not self.stopping.is_set():
                if self._reload_requested.is_set():
                    self._reload_requested.clear()
                    try:
                        self.reload()
                    except Exception as error:
                        event(log, logging.ERROR, "config.rejected", error=str(error))
                time.sleep(0.1)
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)
            clean = self.stop()
        return 0 if clean else 1

    def _acquire_lock(self):
        self.paths.state.mkdir(parents=True, exist_ok=True)
        handle = self.paths.lock.open("a+")
        import fcntl

        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            handle.close()
            raise CliError(
                75, f"another AgentQueue daemon owns {self.paths.home}"
            ) from error
        handle.seek(0)
        handle.truncate()
        handle.write(str(os.getpid()))
        handle.flush()
        os.fsync(handle.fileno())
        return handle

    def stop(self) -> bool:
        if self._state in {"stopped", "stopping"}:
            return True
        self._state = "stopping"
        self.stopping.set()
        event(log, logging.INFO, "daemon.draining")
        if self.ante is not None:
            try:
                self.ante.stop_amqp()
            except Exception:
                pass
        for route in list(self.routes.values()):
            route.stop()
        deadline = time.time() + (
            self.config.runtime.shutdown_grace_seconds if self.config else 30
        )
        with self._admission:
            while self._global_active > 0 and time.time() < deadline:
                self._admission.wait(min(0.2, max(0, deadline - time.time())))
            clean = self._global_active == 0
        self.retries.drain()
        if self.pool is not None:
            self.pool.shutdown(wait=clean, cancel_futures=not clean)
        if self.init_pool is not None:
            self.init_pool.shutdown(wait=False, cancel_futures=True)
        init_running = any(
            "future" in record and not record["future"].done()
            for record in self._operations.values()
        )
        clean = clean and not init_running
        if self._control is not None:
            try:
                self._control.close()
            except OSError:
                pass
        try:
            self.paths.socket.unlink(missing_ok=True)
        except OSError:
            pass
        if self.ante is not None:
            try:
                self.ante.close()
            except Exception:
                pass
            self.ante = None
        if self._lock_handle is not None:
            try:
                import fcntl

                fcntl.flock(self._lock_handle.fileno(), fcntl.LOCK_UN)
            finally:
                self._lock_handle.close()
                self._lock_handle = None
        self._state = "stopped"
        event(log, logging.INFO, "daemon.stopped", clean=clean)
        return clean

    # ---------- routing ----------

    def _build_routes(
        self, routes: dict[str, Route], initial: bool = False
    ) -> list[RouteRuntime]:
        assert self.ante is not None
        reused: list[RouteRuntime] = []
        for topic, raw_route in routes.items():
            route = self.config.effective(raw_route)
            skill = load_skill(self.paths, route.skill, route.revision)
            runtime = self.routes.get(topic)
            if (
                runtime
                and runtime.skill.revision == skill.revision
                and runtime.route.skill == route.skill
            ):
                runtime.route = route
                runtime.paused = runtime.paused or not route.enabled
                runtime.wake()
                reused.append(runtime)
                continue
            if runtime:
                runtime.stop()
                self.routes.pop(topic, None)
            self.ante.declare(topic)
            self.ante.declare(f"{topic}.results")
            self.ante.declare(f"{topic}.failed")
            runtime = RouteRuntime(self, route, skill)
            self.routes[topic] = runtime
            runtime.start()
            self._schedule_prepare(runtime, force=False, auto=True)
        for topic in list(self.routes):
            if topic not in routes:
                self.routes.pop(topic).stop()
        return reused

    def _schedule_prepare(self, route: RouteRuntime, *, force: bool, auto: bool) -> str:
        operation_id = new_id("op")
        self._operations[operation_id] = {
            "id": operation_id,
            "topic": route.topic,
            "status": "running",
            "force": force,
            "started_at": time.time(),
        }
        future = self.init_pool.submit(route.prepare, force)
        self._operations[operation_id]["future"] = future

        def done(completed: Future) -> None:
            record = self._operations[operation_id]
            try:
                record["result"] = completed.result()
                record["status"] = "succeeded"
            except BaseException as error:
                record["status"] = "failed"
                record["error"] = f"{type(error).__name__}: {error}"
            record["finished_at"] = time.time()
            record.pop("future", None)
            if auto:
                event(
                    log,
                    logging.INFO,
                    "route.prepared",
                    topic=route.topic,
                    operation=operation_id,
                    status=record["status"],
                )

        future.add_done_callback(done)
        return operation_id

    def _capacity(self, route: RouteRuntime) -> int:
        with self._admission:
            local = self._active.get(route.topic, 0)
            return max(
                0,
                min(
                    route.route.concurrency - local,
                    self.config.runtime.max_parallel_jobs - self._global_active,
                ),
            )

    def _admit(self, route: RouteRuntime, count: int) -> int:
        with self._admission:
            local = self._active.get(route.topic, 0)
            allowed = min(
                count,
                max(0, route.route.concurrency - local),
                max(0, self.config.runtime.max_parallel_jobs - self._global_active),
            )
            self._active[route.topic] = local + allowed
            self._global_active += allowed
            return allowed

    def _release(self, topic: str, count: int = 1) -> None:
        with self._admission:
            self._active[topic] = max(0, self._active.get(topic, 0) - count)
            self._global_active = max(0, self._global_active - count)
            self._admission.notify_all()

    def _wake_route(self, topic: str) -> None:
        runtime = self.routes.get(topic)
        if runtime:
            runtime.wake()

    def _write_event_file(self, directory: Path, value: dict) -> None:
        try:
            from .util import atomic_json

            directory.mkdir(parents=True, exist_ok=True)
            atomic_json(directory / "event.json", value)
        except OSError:
            pass

    # ---------- reload ----------

    def reload(self) -> dict:
        with self._reload_lock:
            return self._reload_locked()

    def _reload_locked(self) -> dict:
        try:
            config, env = load_config(self.paths)
            config.model_settings(env)
        except (ConfigError, CliError) as error:
            self.last_reload_error = str(error)
            event(log, logging.ERROR, "config.rejected", error=str(error))
            raise
        for route in config.routes.values():
            load_skill(self.paths, route.skill, route.revision)
        warnings = []
        if (
            config.amqp.host,
            config.amqp.port,
            config.amqp.security,
        ) != (
            self.config.amqp.host,
            self.config.amqp.port,
            self.config.amqp.security,
        ):
            warnings.append(
                "AMQP bind address or security configuration changed; restart is required"
            )
        desired = _revision_of(config, config.routes)
        old_runner = self.runner
        old_init_revision = self.config.init.revision
        self.config = config
        self.env = env
        if isinstance(old_runner, AgentScopeRunner):
            old_runner.config = config
        reused = self._build_routes(config.routes)
        if old_init_revision != config.init.revision:
            for runtime in reused:
                self._schedule_prepare(runtime, force=False, auto=True)
        self.active_revision = desired
        self.desired_revision = desired
        self.last_reload_error = None
        event(log, logging.INFO, "config.applied", revision=desired, warnings=warnings)
        return {"revision": desired, "warnings": warnings}

    # ---------- control ----------

    def _open_control(self) -> socket.socket:
        self.paths.socket.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.paths.socket.parent, 0o700)
        try:
            self.paths.socket.unlink()
        except FileNotFoundError:
            pass
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            server.bind(str(self.paths.socket))
            os.chmod(self.paths.socket, 0o600)
            server.listen(16)
            server.settimeout(0.5)
        except OSError as error:
            server.close()
            raise CliError(
                69, f"cannot bind control socket {self.paths.socket}: {error}"
            ) from error
        return server

    def _control_loop(self) -> None:
        assert self._control is not None
        while not self.stopping.is_set():
            try:
                connection, _ = self._control.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(
                target=self._handle_control, args=(connection,), daemon=True
            ).start()

    def _handle_control(self, connection: socket.socket) -> None:
        try:
            connection.settimeout(10)
            if not self._peer_allowed(connection):
                connection.sendall(
                    b'{"ok":false,"error":{"code":"PERMISSION","message":"peer uid mismatch"}}\n'
                )
                return
            chunks = bytearray()
            while b"\n" not in chunks and len(chunks) <= MAX_REQUEST_BYTES:
                chunk = connection.recv(65536)
                if not chunk:
                    break
                chunks.extend(chunk)
            if len(chunks) > MAX_REQUEST_BYTES:
                raise CliError(65, "control request too large")
            line = bytes(chunks).split(b"\n", 1)[0]
            request = json.loads(line.decode() or "{}")
            if not isinstance(request, dict):
                raise CliError(65, "control request must be an object")
            response = self._dispatch_control(request)
        except (CliError, ConfigError) as error:
            response = {
                "ok": False,
                "error": {"code": "REQUEST", "message": str(error)},
            }
        except Exception as error:
            event(
                log,
                logging.ERROR,
                "control.error",
                error=f"{type(error).__name__}: {error}",
            )
            response = {
                "ok": False,
                "error": {"code": "INTERNAL", "message": str(error)},
            }
        try:
            connection.sendall((json.dumps(response) + "\n").encode())
        except OSError:
            pass
        finally:
            connection.close()

    def _peer_allowed(self, connection: socket.socket) -> bool:
        if not hasattr(socket, "SO_PEERCRED"):
            return True
        try:
            credentials = connection.getsockopt(
                socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i")
            )
            _, uid, _ = struct.unpack("3i", credentials)
            return uid == os.getuid()
        except OSError:
            return True

    def _dispatch_control(self, request: dict) -> dict:
        command = request.get("command")
        arguments = request.get("arguments") or {}
        if not isinstance(arguments, dict):
            raise CliError(65, "arguments must be an object")
        if command == "ping":
            return {"ok": True, "data": {"pong": True, "version": __version__}}
        if command == "status":
            return {"ok": True, "data": self.status()}
        if command == "reload":
            return {"ok": True, "data": self.reload()}
        if command == "shutdown":
            self.stopping.set()
            return {"ok": True, "data": {"stopping": True}}
        if command in {"route.pause", "route.resume"}:
            topic = arguments.get("topic")
            runtime = self.routes.get(topic)
            if runtime is None:
                raise CliError(65, f"unknown topic: {topic}")
            runtime.paused = command == "route.pause" or not runtime.route.enabled
            runtime.wake()
            event(
                log,
                logging.INFO,
                "route.paused" if runtime.paused else "route.resumed",
                topic=topic,
            )
            return {"ok": True, "data": {"topic": topic, "paused": runtime.paused}}
        if command == "prepare":
            topic = arguments.get("topic")
            runtime = self.routes.get(topic)
            if runtime is None:
                raise CliError(65, f"unknown topic: {topic}")
            operation = self._schedule_prepare(
                runtime, force=bool(arguments.get("force")), auto=False
            )
            return {"ok": True, "data": {"operation": operation}}
        if command == "op.status":
            operation = self._operations.get(arguments.get("id"))
            if operation is None:
                raise CliError(65, f"unknown operation: {arguments.get('id')}")
            return {
                "ok": True,
                "data": {
                    key: value for key, value in operation.items() if key != "future"
                },
            }
        if command == "jobs.list":
            return {"ok": True, "data": self.jobs.list(int(arguments.get("limit", 50)))}
        if command == "jobs.get":
            try:
                return {"ok": True, "data": self.jobs.get(str(arguments.get("job_id")))}
            except KeyError as error:
                raise CliError(65, f"unknown job: {arguments.get('job_id')}") from error
        if command == "jobs.retry":
            return {"ok": True, "data": self._retry_job(str(arguments.get("job_id")))}
        raise CliError(65, f"unknown control command: {command}")

    def _retry_job(self, job_id: str) -> dict:
        try:
            job = self.jobs.get(job_id)
        except KeyError as error:
            raise CliError(65, f"unknown job: {job_id}") from error
        if job.get("status") != "failed" or not job.get("payload_base64"):
            raise Conflict("only failed jobs with a stored payload can be retried")
        runtime = self.routes.get(job["topic"])
        if runtime is None:
            raise CliError(65, f"topic is no longer configured: {job['topic']}")
        payload = base64.b64decode(job["payload_base64"])
        message_id = self.ante.publish(
            job["topic"],
            payload,
            content_type=job.get("content_type") or "application/octet-stream",
            message_id=job.get("application_message_id") or "",
            correlation_id=job.get("correlation_id") or "",
        )
        new_job = f"j_{message_id}"
        self.jobs.mark_retried(job_id, new_job)
        runtime.wake()
        event(
            log,
            logging.INFO,
            "job.operator_retry",
            job_id=job_id,
            new_job=new_job,
            topic=job["topic"],
        )
        return {"job_id": job_id, "new_job_id": new_job, "message_id": message_id}

    def status(self) -> dict:
        routes = []
        for runtime in self.routes.values():
            routes.append(runtime.status())
        return {
            "daemon": {
                "state": self._state,
                "pid": self._pid,
                "version": __version__,
                "uptime_seconds": int(time.time() - self.started_at)
                if self.started_at
                else 0,
                "home": str(self.paths.home),
                "amqp": (
                    {
                        "host": self.amqp_address[0],
                        "port": self.amqp_address[1],
                        "tls": bool(self.config and self.config.amqp.security),
                    }
                    if self.amqp_address
                    else None
                ),
                "desired_revision": self.desired_revision,
                "active_revision": self.active_revision,
                "last_reload_error": self.last_reload_error,
                "active_jobs": self._global_active,
                "max_parallel_jobs": self.config.runtime.max_parallel_jobs
                if self.config
                else 0,
            },
            "routes": routes,
            "env": sorted(self.env),
        }
