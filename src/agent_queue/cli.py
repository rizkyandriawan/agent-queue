from __future__ import annotations

import argparse
import json
import os
import shutil
import ssl
import sys
import tarfile
import time
from pathlib import Path
from typing import Any

from . import __version__
from .client import daemon_online, request, wait_operation, wait_route
from .config import (
    Config,
    default_config,
    load_config,
    read_raw,
    update_config,
    write_raw,
)
from .daemon import Daemon
from .errors import AgentQueueError, CliError, Conflict, DaemonUnavailable
from .jobs import JobStore
from .paths import Paths, resolve_home
from .registry import delete_skill, import_skill, list_skills, load_skill
from .service import (
    active as service_active,
    disable as service_disable,
    enable as service_enable,
    install as service_install,
    lingering_hint,
    logs as service_logs,
    restart as service_restart,
    start as service_start,
    stop as service_stop,
    unit_path,
    uninstall as service_uninstall,
)
from .util import daemon_is_locked, normalize_topic, redact_env


def _common(name: str, help_text: str, **kwargs) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=f"agentqueue {name}", description=help_text, add_help=False, **kwargs
    )
    parser.add_argument(
        "--home",
        metavar="PATH",
        default=argparse.SUPPRESS,
        help="AgentQueue home (default ~/.agentqueue)",
    )
    parser.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        default=argparse.SUPPRESS,
        help="emit JSON",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=argparse.SUPPRESS,
        help="request/wait timeout in seconds",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        default=argparse.SUPPRESS,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        default=argparse.SUPPRESS,
        help="suppress nonessential output",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=argparse.SUPPRESS,
        help="increase diagnostics",
    )
    return parser


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agentqueue",
        description="Run Markdown-defined automations from AMQP messages.",
        epilog="Use `agentqueue <command> --help` for command details.",
    )
    parser.add_argument(
        "--home",
        metavar="PATH",
        default=argparse.SUPPRESS,
        help="AgentQueue home (default ~/.agentqueue)",
    )
    parser.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        default=argparse.SUPPRESS,
        help="emit JSON",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=argparse.SUPPRESS,
        help="request/wait timeout in seconds",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        default=argparse.SUPPRESS,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        default=argparse.SUPPRESS,
        help="suppress nonessential output",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=argparse.SUPPRESS,
        help="increase diagnostics",
    )
    parser.add_argument(
        "--version", action="version", version=f"agentqueue {__version__}"
    )
    parser.set_defaults(
        home=None,
        json_output=False,
        timeout=30.0,
        quiet=False,
        verbose=0,
        no_color=False,
    )
    sub = parser.add_subparsers(dest="command", metavar="<command>")

    def add(name: str, help_text: str) -> argparse.ArgumentParser:
        return sub.add_parser(
            name,
            help=help_text,
            description=help_text,
            parents=[_common(name, help_text)],
        )

    add("init", "initialize AgentQueue home and sample configuration")
    add("serve", "run the daemon in the foreground")
    add("start", "start the systemd user service")
    add("stop", "stop the daemon gracefully")
    add("restart", "restart the systemd user service")
    status = add("status", "show daemon and route status")
    status.add_argument("--check", choices=("ready",), help="exit nonzero unless ready")
    add("reload", "validate and reconcile configuration changes")
    logs = add("logs", "read or follow daemon logs")
    logs.add_argument("-f", "--follow", action="store_true")
    logs.add_argument("-n", "--lines", type=int, default=100)
    logs.add_argument("--since", default=None)
    logs.add_argument("--level", choices=("debug", "info", "warning", "error"))
    logs.add_argument("--topic")
    logs.add_argument("--job")

    addcmd = add("add", "import a skill and register a topic")
    addcmd.add_argument(
        "--file", required=True, help="SKILL.md, or a standalone .md file"
    )
    addcmd.add_argument("--topic", required=True, help="exact AMQP routing key")
    addcmd.add_argument(
        "--name", help="skill identifier (default from the file/directory)"
    )
    addcmd.add_argument("--concurrency", type=int, help="parallel jobs for this topic")
    addcmd.add_argument(
        "--wait", action="store_true", help="wait until the topic is ready"
    )

    update = add("update", "import a new revision of an existing skill")
    update.add_argument("--file", required=True)
    update.add_argument("--topic", required=True)
    update.add_argument("--name")
    update.add_argument("--concurrency", type=int)
    update.add_argument("--wait", action="store_true")

    remove = add("remove", "disable and unregister a topic")
    remove.add_argument("--topic", required=True)
    remove.add_argument(
        "--delete-skill", action="store_true", help="delete installed skill files"
    )
    remove.add_argument(
        "--delete-data", action="store_true", help="delete the skill workspace"
    )
    remove.add_argument(
        "--yes", action="store_true", help="confirm destructive deletion"
    )

    lst = add("list", "list registered topics and skills")
    lst.add_argument("--limit", type=int, default=0, help=argparse.SUPPRESS)

    show = add("show", "inspect one topic or skill")
    show.add_argument("topic", help="topic name")

    prepare = add("prepare", "run or retry a skill's initialization")
    prepare.add_argument("topic")
    prepare.add_argument("--force", action="store_true")
    prepare.add_argument("--no-wait", action="store_true")

    for name in ("pause", "resume"):
        command = add(name, f"{name} dispatching jobs for a topic")
        command.add_argument("topic")

    jobs = add("jobs", "list and inspect jobs")
    jobs_sub = jobs.add_subparsers(dest="jobs_command")
    jobs_list = jobs_sub.add_parser("list", parents=[_common("jobs list", "list jobs")])
    jobs_list.add_argument("--limit", type=int, default=50)
    jobs_show = jobs_sub.add_parser(
        "show", parents=[_common("jobs show", "show one job")]
    )
    jobs_show.add_argument("job_id")
    jobs_retry = jobs_sub.add_parser(
        "retry", parents=[_common("jobs retry", "retry a failed job")]
    )
    jobs_retry.add_argument("job_id")
    jobs_retry.add_argument("--wait", action="store_true")

    send = add("send", "publish a message through AMQP")
    send.add_argument("--topic", required=True)
    send.add_argument("--message")
    send.add_argument("--file", help="file path, or - for stdin")
    send.add_argument("--content-type", default="text/plain")
    send.add_argument("--message-id")
    send.add_argument("--correlation-id")
    send.add_argument("--host")
    send.add_argument("--port", type=int)
    send.add_argument("--tls-ca", help="trust this PEM CA and use TLS")
    send.add_argument("--tls-server-name", help="certificate name (default: host)")
    send.add_argument("--user", help="AMQP username (or AGENTQUEUE_AMQP_USER)")
    send.add_argument(
        "--password-file",
        help="read the AMQP secret from a file (or AGENTQUEUE_AMQP_PASSWORD)",
    )

    config = add("config", "validate or inspect effective configuration")
    config_sub = config.add_subparsers(dest="config_command")
    config_sub.add_parser(
        "validate", parents=[_common("config validate", "validate config")]
    )
    config_sub.add_parser(
        "show", parents=[_common("config show", "show effective config")]
    )

    add("doctor", "check the local installation and runtime")
    backup = add("backup", "create an offline state backup")
    backup.add_argument("--output", default=None)
    backup.add_argument("--with-workspaces", action="store_true")
    backup.add_argument("--with-env", action="store_true")

    service = add("service", "install or manage the systemd user service")
    service_sub = service.add_subparsers(dest="service_command")
    service_install_cmd = service_sub.add_parser(
        "install", parents=[_common("service install", "install")]
    )
    service_install_cmd.add_argument("--enable", action="store_true")
    service_sub.add_parser("enable", parents=[_common("service enable", "enable")])
    service_sub.add_parser("disable", parents=[_common("service disable", "disable")])
    service_sub.add_parser(
        "uninstall", parents=[_common("service uninstall", "uninstall")]
    )
    service_sub.add_parser("status", parents=[_common("service status", "status")])
    return parser


def main(argv: list[str] | None = None) -> int:
    raw = list(sys.argv[1:] if argv is None else argv)
    raw = ["add" if argument == "-add" else argument for argument in raw]
    parser = build_parser()
    try:
        args = parser.parse_args(raw)
    except SystemExit as error:
        return 64 if int(error.code or 0) == 2 else int(error.code or 0)
    if not args.command:
        parser.print_help()
        return 0
    paths = Paths(resolve_home(args.home))
    try:
        return COMMANDS[args.command](args, paths)
    except KeyboardInterrupt:
        return 130
    except AgentQueueError as error:
        _error(args, error.exit_code, str(error))
        return error.exit_code
    except BrokenPipeError:
        return 0
    except Exception as error:  # noqa: BLE001 - CLI boundary
        if args.verbose:
            import traceback

            traceback.print_exc()
        _error(args, 70, f"{type(error).__name__}: {error}")
        return 70


def _error(args, code: int, message: str) -> None:
    if getattr(args, "json_output", False):
        print(json.dumps({"ok": False, "error": {"code": code, "message": message}}))
    else:
        print(f"agentqueue: {message}", file=sys.stderr)


def _emit(args, data: Any, human: str | None = None) -> None:
    if getattr(args, "json_output", False):
        print(json.dumps({"ok": True, "data": data}, ensure_ascii=False, default=str))
    elif not getattr(args, "quiet", False) and human is not None:
        print(human)


def _load(paths: Paths) -> tuple[Config, dict[str, str]]:
    return load_config(paths)


def _request(args, paths: Paths, command: str, arguments: dict | None = None) -> dict:
    return request(paths, command, arguments, timeout=args.timeout)


def _command_add(args, paths: Paths) -> int:
    _load(paths)
    topic = _topic(args.topic)
    if args.concurrency is not None and args.concurrency < 1:
        raise CliError(64, "--concurrency must be positive")
    skill = import_skill(paths, args.file, args.name)

    def mutate(raw: dict) -> None:
        routes = raw.setdefault("routes", {})
        if topic in routes:
            raise CliError(
                65, f"topic {topic!r} already exists; use `agentqueue update`"
            )
        routes[topic] = {
            "skill": skill.name,
            "revision": skill.revision,
            "concurrency": args.concurrency
            or raw.get("defaults", {}).get("concurrency", 1),
            "enabled": True,
        }

    update_config(paths, mutate)
    online = daemon_online(paths)
    if args.wait and not online:
        raise DaemonUnavailable("daemon is offline; start it before using --wait")
    if online:
        _request(args, paths, "reload")
    data = {
        "topic": topic,
        "skill": skill.name,
        "revision": skill.revision,
        "state": "initializing" if online else "pending",
        "daemon": "online" if online else "offline",
    }
    if args.wait:
        data["route"] = wait_route(paths, topic, args.timeout)
    _emit(
        args,
        data,
        f"Imported skill: {skill.name}\nRegistered topic: {topic}\n"
        f"Revision: {skill.revision[:12]}\nState: {data['state']}"
        + ("\nWaiting: ready" if args.wait else ""),
    )
    return 0


def _command_update(args, paths: Paths) -> int:
    _load(paths)
    topic = _topic(args.topic)
    raw = read_raw(paths.config)
    existing = raw.get("routes", {}).get(topic)
    if existing is None:
        raise CliError(65, f"topic {topic!r} does not exist; use `agentqueue add`")
    if args.name and args.name != existing.get("skill"):
        raise CliError(65, "renaming a skill is not supported; remove and add it again")
    skill = import_skill(paths, args.file, existing.get("skill"))

    def mutate(raw: dict) -> None:
        route = raw.get("routes", {}).get(topic)
        if route is None:
            raise CliError(75, f"topic {topic!r} disappeared during update")
        route["revision"] = skill.revision
        if args.concurrency is not None:
            if args.concurrency < 1:
                raise CliError(64, "--concurrency must be positive")
            route["concurrency"] = args.concurrency

    update_config(paths, mutate)
    online = daemon_online(paths)
    if args.wait and not online:
        raise DaemonUnavailable("daemon is offline; start it before using --wait")
    if online:
        _request(args, paths, "reload")
    data = {
        "topic": topic,
        "skill": skill.name,
        "revision": skill.revision,
        "daemon": "online" if online else "offline",
    }
    if args.wait:
        data["route"] = wait_route(paths, topic, args.timeout)
    _emit(
        args,
        data,
        f"Updated topic {topic} to revision {skill.revision[:12]}"
        + ("\nWaiting: ready" if args.wait else ""),
    )
    return 0


def _command_remove(args, paths: Paths) -> int:
    _load(paths)
    topic = _topic(args.topic)
    raw = read_raw(paths.config)
    route = raw.get("routes", {}).get(topic)
    if route is None:
        raise CliError(65, f"topic {topic!r} does not exist")
    skill_name = route.get("skill")
    referenced = any(
        key != topic and value.get("skill") == skill_name
        for key, value in raw.get("routes", {}).items()
    )
    if args.delete_skill and referenced:
        raise CliError(65, f"skill {skill_name!r} is still used by another topic")
    if (args.delete_skill or args.delete_data) and not args.yes:
        raise CliError(64, "--delete-skill/--delete-data require --yes")
    online = daemon_online(paths)
    if online:
        status = _request(args, paths, "status")
        live = next(
            (item for item in status.get("routes", []) if item["topic"] == topic),
            {},
        )
        if (args.delete_skill or args.delete_data) and live.get("active", 0):
            raise Conflict(
                "topic still has active jobs; wait for them to finish before "
                "deleting skill data"
            )

    def mutate(raw: dict) -> None:
        raw.get("routes", {}).pop(topic, None)

    update_config(paths, mutate)
    if online:
        _request(args, paths, "reload")
    if args.delete_data:
        import shutil

        shutil.rmtree(paths.workspaces / str(skill_name), ignore_errors=True)
    if args.delete_skill:
        delete_skill(paths, str(skill_name), delete_workspace=args.delete_data)
    _emit(args, {"topic": topic, "removed": True}, f"Removed topic {topic}")
    return 0


def _command_list(args, paths: Paths) -> int:
    config, _ = _load(paths)
    skills = {item["name"]: item for item in list_skills(paths)}
    online = daemon_online(paths)
    status = _request(args, paths, "status") if online else None
    live = {route["topic"]: route for route in (status or {}).get("routes", [])}
    rows = []
    for topic, route in config.routes.items():
        row = {
            "topic": topic,
            "skill": route.skill,
            "revision": (
                live.get(topic, {}).get("revision")
                or route.revision
                or skills.get(route.skill, {}).get("current")
            ),
            "state": live.get(topic, {}).get(
                "state", "pending" if online else "offline"
            ),
            "concurrency": (
                live.get(topic, {}).get("concurrency")
                or route.concurrency
                or config.defaults.concurrency
            ),
            "queued": live.get(topic, {}).get("queue", {}).get("total", "-"),
        }
        rows.append(row)
    if args.json_output:
        _emit(args, {"routes": rows, "daemon": "online" if online else "offline"})
        return 0
    if not rows:
        _emit(args, {"routes": []}, "No topics registered.")
        return 0
    header = f"{'TOPIC':<28} {'SKILL':<22} {'STATE':<14} {'CONC':>4} {'QUEUED':>7}"
    lines = [header, "-" * len(header)]
    for row in rows:
        lines.append(
            f"{row['topic']:<28} {row['skill']:<22} {row['state']:<14} "
            f"{row['concurrency']:>4} {str(row['queued']):>7}"
        )
    _emit(args, {"routes": rows}, "\n".join(lines))
    return 0


def _command_show(args, paths: Paths) -> int:
    config, _ = _load(paths)
    topic = _topic(args.topic)
    route = config.routes.get(topic)
    if route is None:
        raise CliError(65, f"topic {topic!r} does not exist")
    try:
        skill = load_skill(paths, route.skill, route.revision)
        skill_data = {
            "name": skill.name,
            "revision": skill.revision,
            "has_init": skill.has_init,
            "root": str(skill.root),
        }
    except CliError as error:
        skill_data = {"error": str(error)}
    data = {"route": config.effective(route).__dict__, "skill": skill_data}
    if daemon_online(paths):
        status = _request(args, paths, "status")
        data["live"] = next(
            (item for item in status.get("routes", []) if item["topic"] == topic), None
        )
    _emit(args, data, json.dumps(data, indent=2, default=str))
    return 0


def _command_prepare(args, paths: Paths) -> int:
    _load(paths)
    operation = _request(
        args, paths, "prepare", {"topic": _topic(args.topic), "force": args.force}
    )
    data = {"operation": operation["operation"]}
    if not args.no_wait:
        data.update(wait_operation(paths, operation["operation"], args.timeout))
    _emit(args, data, f"Preparation {data.get('status', 'running')} for {args.topic}")
    return 0


def _command_pause_resume(args, paths: Paths) -> int:
    _load(paths)
    data = _request(args, paths, f"route.{args.command}", {"topic": _topic(args.topic)})
    _emit(args, data, f"{args.topic}: {'paused' if data['paused'] else 'running'}")
    return 0


def _command_status(args, paths: Paths) -> int:
    try:
        data = _request(args, paths, "status")
        online = True
    except (DaemonUnavailable, CliError):
        data = {"daemon": {"state": "stopped", "home": str(paths.home)}, "routes": []}
        online = False
    if args.check == "ready":
        bad = data["daemon"].get("state") != "running" or any(
            route["state"] not in {"ready", "paused"}
            for route in data.get("routes", [])
        )
        if bad:
            _emit(args, data, None)
            return 75
    if args.json_output:
        _emit(args, data)
        return 0
    daemon = data["daemon"]
    lines = [
        f"Daemon: {daemon.get('state')}  pid={daemon.get('pid', '-')}  "
        f"version={daemon.get('version', '-')}  home={daemon.get('home')}"
    ]
    if daemon.get("amqp"):
        lines.append(
            f"AMQP: {daemon['amqp']['host']}:{daemon['amqp']['port']}  "
            f"tls={daemon['amqp'].get('tls', False)}  "
            f"active={daemon.get('active_jobs')}/{daemon.get('max_parallel_jobs')}  "
            f"revision={daemon.get('active_revision')}"
        )
    for warning in (daemon.get("last_reload_error"),):
        if warning:
            lines.append(f"Warning: {warning}")
    routes = data.get("routes", [])
    if routes:
        lines.append("")
        header = f"{'TOPIC':<26} {'STATE':<14} {'ACTIVE':>6} {'READY':>6} {'INFLIGHT':>8}  SKILL"
        lines.extend([header, "-" * len(header)])
        for route in routes:
            lines.append(
                f"{route['topic']:<26} {route['state']:<14} {route['active']:>6} "
                f"{route['queue']['ready']:>6} {route['queue']['inflight']:>8}  {route['skill']}"
            )
    else:
        lines.append("No topics registered.")
    if not online:
        lines.append("Daemon is offline.")
    _emit(args, data, "\n".join(lines))
    return 0 if online or args.check is None else 69


def _command_reload(args, paths: Paths) -> int:
    data = _request(args, paths, "reload")
    _emit(
        args,
        data,
        f"Configuration applied: revision {data['revision']}"
        + ("\n" + "\n".join(data["warnings"]) if data.get("warnings") else ""),
    )
    return 0


def _command_logs(args, paths: Paths) -> int:
    arguments = ["-n", str(args.lines)]
    if args.follow:
        arguments.append("-f")
    if args.since:
        arguments.extend(["--since", args.since])
    if args.level:
        arguments.extend(["--grep", f'"level": "{args.level}"'])
    if args.topic:
        arguments.extend(["--grep", args.topic])
    if args.job:
        arguments.extend(["--grep", args.job])
    return service_logs(arguments)


def _command_jobs(args, paths: Paths) -> int:
    command = args.jobs_command or "list"
    if command == "list":
        limit = getattr(args, "limit", 50)
        if daemon_online(paths):
            data = _request(args, paths, "jobs.list", {"limit": limit})
        else:
            data = JobStore(paths).list(limit)
        if args.json_output:
            _emit(args, data)
            return 0
        header = f"{'JOB':<22} {'TOPIC':<24} {'STATUS':<12} {'ATT':>3}  SKILL"
        lines = [header, "-" * len(header)]
        for job in data:
            lines.append(
                f"{job['job_id']:<22} {job.get('topic', '-'):<24} {job.get('status', '-'):<12} "
                f"{len(job.get('attempts', [])):>3}  {job.get('skill', '-')}"
            )
        _emit(args, data, "\n".join(lines) if data else "No jobs.")
        return 0
    if command == "show":
        if daemon_online(paths):
            data = _request(args, paths, "jobs.get", {"job_id": args.job_id})
        else:
            try:
                data = JobStore(paths).get(args.job_id)
            except KeyError as error:
                raise CliError(65, f"unknown job: {args.job_id}") from error
        _emit(args, data, json.dumps(data, indent=2, default=str))
        return 0
    if command == "retry":
        data = _request(args, paths, "jobs.retry", {"job_id": args.job_id})
        if args.wait:
            deadline = time.monotonic() + args.timeout
            while time.monotonic() < deadline:
                try:
                    job = _request(
                        args, paths, "jobs.get", {"job_id": data["new_job_id"]}
                    )
                except CliError:
                    # The worker may not have persisted the job record yet.
                    time.sleep(0.1)
                    continue
                if job.get("status") in {"succeeded", "failed"}:
                    data["job"] = job
                    break
                time.sleep(0.25)
            else:
                raise CliError(75, "retried job did not finish in time")
        _emit(args, data, f"Retried {args.job_id} as {data['new_job_id']}")
        return 0
    raise CliError(64, f"unknown jobs command: {command}")


def _command_send(args, paths: Paths) -> int:
    topic = _topic(args.topic)
    if args.message is not None and args.file is not None:
        raise CliError(64, "use either --message or --file, not both")
    if args.message is not None:
        body = args.message.encode()
    elif args.file == "-":
        body = sys.stdin.buffer.read()
    elif args.file is not None:
        body = Path(args.file).read_bytes()
    else:
        raise CliError(64, "provide --message, --file, or --file=- for stdin")
    config, _ = _load(paths)
    host, port = args.host, args.port
    explicit = host is not None or port is not None
    secured = False
    if host is None or port is None:
        if daemon_online(paths):
            status = _request(args, paths, "status")
            address = status["daemon"].get("amqp") or {}
            host = host or address.get("host", config.amqp.host)
            port = port or address.get("port", config.amqp.port)
            secured = bool(address.get("tls"))
        else:
            host = host or config.amqp.host
            port = port or config.amqp.port
            secured = bool(config.amqp.security) and not explicit

    ca = args.tls_ca or os.environ.get("AGENTQUEUE_AMQP_CA")
    server_name = (
        args.tls_server_name or os.environ.get("AGENTQUEUE_AMQP_SERVER_NAME") or host
    )
    user = args.user or os.environ.get("AGENTQUEUE_AMQP_USER")
    password = None
    if args.password_file:
        try:
            password = Path(args.password_file).read_text().rstrip("\r\n")
        except OSError as error:
            raise CliError(
                66, f"read password file {args.password_file}: {error}"
            ) from error
    else:
        password = os.environ.get("AGENTQUEUE_AMQP_PASSWORD")
    if secured and not ca:
        raise CliError(
            64,
            "the AMQP ingress is secured; pass --tls-ca (or set AGENTQUEUE_AMQP_CA)",
        )
    if secured and not user:
        raise CliError(
            64,
            "the AMQP ingress requires credentials; pass --user "
            "(or set AGENTQUEUE_AMQP_USER)",
        )
    if secured and password is None:
        raise CliError(
            64,
            "the AMQP ingress requires a secret; pass --password-file "
            "(or set AGENTQUEUE_AMQP_PASSWORD)",
        )
    try:
        import pika
    except ImportError as error:
        raise CliError(
            69, "pika is required for `agentqueue send`; pip install pika"
        ) from error
    parameters = {"host": host, "port": port}
    if ca:
        try:
            context = ssl.create_default_context(cafile=ca)
        except (OSError, ssl.SSLError) as error:
            raise CliError(66, f"load CA file {ca}: {error}") from error
        parameters["ssl_options"] = pika.SSLOptions(context, server_name)
    if user:
        parameters["credentials"] = pika.PlainCredentials(user, password or "")
    connection = None
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(**parameters))
        channel = connection.channel()
        channel.confirm_delivery()
        properties = pika.BasicProperties(
            content_type=args.content_type,
            message_id=args.message_id,
            correlation_id=args.correlation_id,
            delivery_mode=2,
        )
        channel.basic_publish("", topic, body, properties=properties)
    except Exception as error:  # pika exposes several transport error types
        raise CliError(69, f"AMQP publish failed: {error}") from error
    finally:
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass
    _emit(
        args,
        {
            "topic": topic,
            "bytes": len(body),
            "host": host,
            "port": port,
            "tls": bool(ca),
            "user": user,
        },
        f"Published {len(body)} bytes to {topic} at {host}:{port}"
        + (" over TLS" if ca else ""),
    )
    return 0


def _command_config(args, paths: Paths) -> int:
    command = args.config_command or "validate"
    config, env = _load(paths)
    if command == "validate":
        missing = []
        for name in (config.model.name_env, config.model.api_key_env):
            if not env.get(name):
                missing.append(name)
        data = {"valid": True, "missing_env": missing}
        _emit(
            args,
            data,
            "Configuration is valid."
            + (f" Missing env: {', '.join(missing)}" if missing else ""),
        )
        return 0
    data = {"config": config.to_dict(), "env": redact_env(env)}
    _emit(args, data, json.dumps(data, indent=2))
    return 0


def _command_doctor(args, paths: Paths) -> int:
    checks = []

    def check(name: str, ok: bool, detail: str) -> None:
        checks.append({"check": name, "ok": ok, "detail": detail})

    check("python", sys.version_info >= (3, 11), sys.version.split()[0])
    try:
        from importlib.metadata import version

        import ante  # noqa: F401

        check("ante", True, f"ante-embedded {version('ante-embedded')}")
    except Exception as error:  # noqa: BLE001
        check("ante", False, str(error))
    try:
        from importlib.metadata import version

        check("agentscope", version("agentscope") == "2.0.10", version("agentscope"))
    except Exception as error:  # noqa: BLE001
        check("agentscope", False, str(error))
    check("home", paths.home.is_dir(), str(paths.home))
    if paths.config.exists():
        try:
            config, env = _load(paths)
            check("config", True, f"{len(config.routes)} route(s)")
            missing = [
                n
                for n in (config.model.name_env, config.model.api_key_env)
                if not env.get(n)
            ]
            check("model-env", not missing, ", ".join(missing) or "set")
        except AgentQueueError as error:
            check("config", False, str(error))
    else:
        check("config", False, "run `agentqueue init`")
    try:
        daemon = _request(args, paths, "status")
        check(
            "daemon",
            daemon["daemon"]["state"] == "running",
            f"{daemon['daemon']['state']} pid {daemon['daemon']['pid']}",
        )
        check(
            "amqp",
            bool(daemon["daemon"].get("amqp")),
            str(daemon["daemon"].get("amqp")),
        )
    except AgentQueueError as error:
        check("daemon", False, str(error))
    check("systemd-unit", unit_path().exists(), str(unit_path()))
    try:
        free = shutil.disk_usage(
            paths.home if paths.home.exists() else Path.home()
        ).free
        check("disk", free > 10 * 1024 * 1024, f"{free // (1024 * 1024)} MiB free")
    except OSError as error:
        check("disk", False, str(error))
    if args.json_output:
        _emit(args, checks)
        return 0 if all(item["ok"] for item in checks) else 75
    lines = [
        f"{'OK' if item['ok'] else 'FAIL':<5} {item['check']:<14} {item['detail']}"
        for item in checks
    ]
    _emit(args, checks, "\n".join(lines))
    return 0 if all(item["ok"] for item in checks) else 75


def _command_backup(args, paths: Paths) -> int:
    if daemon_is_locked(paths.lock):
        raise CliError(75, "stop the daemon before creating an offline backup")
    if not paths.home.exists():
        raise CliError(65, f"AgentQueue home does not exist: {paths.home}")
    output = Path(
        args.output or f"agentqueue-backup-{time.strftime('%Y%m%dT%H%M%SZ')}.tar.gz"
    )
    with tarfile.open(output, "w:gz") as archive:
        for name in ("config.yaml", ".env.example"):
            path = paths.home / name
            if path.exists():
                archive.add(path, arcname=name)
        if args.with_env and paths.env.exists():
            archive.add(paths.env, arcname=".env")
        for name in ("skills", "state"):
            path = paths.home / name
            if path.exists():
                archive.add(path, arcname=name)
        if args.with_workspaces and paths.workspaces.exists():
            archive.add(paths.workspaces, arcname="workspaces")
    output.chmod(0o600)
    _emit(
        args,
        {"output": str(output), "bytes": output.stat().st_size},
        f"Backup written to {output}",
    )
    return 0


def _command_service(args, paths: Paths) -> int:
    command = args.service_command or "status"
    if command == "install":
        config = None
        if paths.config.exists():
            config, _ = _load(paths)
        grace = config.runtime.shutdown_grace_seconds if config else 30
        path = service_install(paths, grace)
        if args.enable:
            service_enable()
        _emit(
            args,
            {"unit": str(path), "enabled": bool(args.enable)},
            f"Installed {path}\nNext: `agentqueue start`\n{lingering_hint()}",
        )
        return 0
    if command == "enable":
        service_enable()
        _emit(args, {"enabled": True}, "Service enabled at login.")
        return 0
    if command == "disable":
        service_disable()
        _emit(args, {"enabled": False}, "Service disabled.")
        return 0
    if command == "uninstall":
        service_uninstall()
        _emit(args, {"uninstalled": True}, "Service uninstalled; data was kept.")
        return 0
    if command == "status":
        running = service_active()
        _emit(args, {"active": running}, "active" if running else "inactive")
        return 0 if running else 75
    raise CliError(64, f"unknown service command: {command}")


def _command_start(args, paths: Paths) -> int:
    if not unit_path().exists():
        raise CliError(69, "service is not installed; run `agentqueue service install`")
    service_start()
    _emit(args, {"started": True}, "Service started.")
    return 0


def _command_stop(args, paths: Paths) -> int:
    if unit_path().exists():
        service_stop()
    else:
        try:
            _request(args, paths, "shutdown")
        except DaemonUnavailable as error:
            raise CliError(69, "no running daemon found") from error
    _emit(args, {"stopped": True}, "Service stopped.")
    return 0


def _command_restart(args, paths: Paths) -> int:
    if not unit_path().exists():
        raise CliError(69, "service is not installed; run `agentqueue service install`")
    service_restart()
    _emit(args, {"restarted": True}, "Service restarted.")
    return 0


def _command_serve(args, paths: Paths) -> int:
    daemon = Daemon(paths)
    code = daemon.run()
    if code:
        # Non-daemon worker threads may still be alive after a dirty drain.
        os._exit(code)
    return code


def _topic(value: str) -> str:
    try:
        return normalize_topic(value)
    except ValueError as error:
        raise CliError(64, str(error)) from error


_ENV_EXAMPLE = """# Any OpenAI Chat Completions-compatible provider with tool calling.
AGENT_MODEL=
AGENT_API_KEY=
# AGENT_BASE_URL=https://provider.example/v1

# Your automation's credentials/configuration, inherited by generated scripts.
# SERVICE_API_URL=https://service.example
# SERVICE_API_TOKEN=
"""


def _command_init(args, paths: Paths) -> int:
    paths.ensure()
    created = []
    if not paths.config.exists():
        write_raw(paths.config, default_config())
        created.append(str(paths.config))
    if not paths.env_example.exists():
        paths.env_example.write_text(_ENV_EXAMPLE)
        paths.env_example.chmod(0o644)
        created.append(str(paths.env_example))
    if not paths.env.exists():
        paths.env.write_text(_ENV_EXAMPLE)
        paths.env.chmod(0o600)
        created.append(str(paths.env))
    _emit(
        args,
        {"home": str(paths.home), "created": created},
        f"AgentQueue home: {paths.home}"
        + ("\nCreated: " + ", ".join(created) if created else "")
        + "\nNext: edit .env, then `agentqueue -add --file=SKILL.md --topic=my.topic`\n"
        "Then: `agentqueue service install && agentqueue start`",
    )
    return 0


COMMANDS = {
    "init": _command_init,
    "serve": _command_serve,
    "start": _command_start,
    "stop": _command_stop,
    "restart": _command_restart,
    "status": _command_status,
    "reload": _command_reload,
    "logs": _command_logs,
    "add": _command_add,
    "update": _command_update,
    "remove": _command_remove,
    "list": _command_list,
    "show": _command_show,
    "prepare": _command_prepare,
    "pause": _command_pause_resume,
    "resume": _command_pause_resume,
    "jobs": _command_jobs,
    "send": _command_send,
    "config": _command_config,
    "doctor": _command_doctor,
    "backup": _command_backup,
    "service": _command_service,
}


if __name__ == "__main__":
    raise SystemExit(main())
