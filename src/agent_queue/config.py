"""Strict configuration schema and environment snapshot handling."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from dotenv import dotenv_values

from .errors import ConfigError
from .paths import Paths
from .util import atomic_write, file_lock, normalize_skill_name, normalize_topic

SUPPORTED_VERSION = 1
ENV_NAME = __import__("re").compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
LOG_LEVELS = {"debug", "info", "warning", "error"}
LOG_FORMATS = {"json", "text"}


@dataclass(frozen=True)
class AmqpConfig:
    host: str = "127.0.0.1"
    port: int = 5672


@dataclass(frozen=True)
class ModelConfig:
    name_env: str = "AGENT_MODEL"
    api_key_env: str = "AGENT_API_KEY"
    base_url_env: str = "AGENT_BASE_URL"


@dataclass(frozen=True)
class RetryConfig:
    initial_seconds: float = 2.0
    maximum_seconds: float = 60.0
    jitter: bool = True


@dataclass(frozen=True)
class Defaults:
    concurrency: int = 1
    timeout_seconds: float = 300.0
    max_attempts: int = 3
    max_iterations: int = 30
    retry: RetryConfig = field(default_factory=RetryConfig)


@dataclass(frozen=True)
class RuntimeConfig:
    max_parallel_jobs: int = 8
    max_parallel_init: int = 1
    shutdown_grace_seconds: float = 30.0


@dataclass(frozen=True)
class LoggingConfig:
    level: str = "info"
    format: str = "json"
    destination: str = "stderr"
    path: str | None = None


@dataclass(frozen=True)
class InitConfig:
    revision: str = ""


@dataclass(frozen=True)
class Route:
    topic: str
    skill: str
    revision: str | None = None
    concurrency: int | None = None
    enabled: bool = True
    timeout_seconds: float | None = None
    max_attempts: int | None = None
    max_iterations: int | None = None


@dataclass(frozen=True)
class Config:
    amqp: AmqpConfig
    model: ModelConfig
    runtime: RuntimeConfig
    defaults: Defaults
    logging: LoggingConfig
    init: InitConfig
    routes: dict[str, Route]
    version: int = SUPPORTED_VERSION

    def effective(self, route: Route) -> Route:
        return Route(
            topic=route.topic,
            skill=route.skill,
            revision=route.revision,
            concurrency=route.concurrency or self.defaults.concurrency,
            enabled=route.enabled,
            timeout_seconds=route.timeout_seconds or self.defaults.timeout_seconds,
            max_attempts=route.max_attempts or self.defaults.max_attempts,
            max_iterations=route.max_iterations or self.defaults.max_iterations,
        )

    def model_settings(self, env: dict[str, str]) -> tuple[str, str, str | None]:
        name = env.get(self.model.name_env, "").strip()
        key = env.get(self.model.api_key_env, "").strip()
        base_url = env.get(self.model.base_url_env, "").strip() or None
        if not name:
            raise ConfigError(
                f"{self.model.name_env} is not set in the environment or .env"
            )
        if not key:
            raise ConfigError(
                f"{self.model.api_key_env} is not set in the environment or .env"
            )
        return name, key, base_url

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "amqp": {"host": self.amqp.host, "port": self.amqp.port},
            "model": {
                "name_env": self.model.name_env,
                "api_key_env": self.model.api_key_env,
                "base_url_env": self.model.base_url_env,
            },
            "runtime": {
                "max_parallel_jobs": self.runtime.max_parallel_jobs,
                "max_parallel_init": self.runtime.max_parallel_init,
                "shutdown_grace_seconds": self.runtime.shutdown_grace_seconds,
            },
            "defaults": {
                "concurrency": self.defaults.concurrency,
                "timeout_seconds": self.defaults.timeout_seconds,
                "max_attempts": self.defaults.max_attempts,
                "max_iterations": self.defaults.max_iterations,
                "retry": {
                    "initial_seconds": self.defaults.retry.initial_seconds,
                    "maximum_seconds": self.defaults.retry.maximum_seconds,
                    "jitter": self.defaults.retry.jitter,
                },
            },
            "logging": {
                "level": self.logging.level,
                "format": self.logging.format,
                "destination": self.logging.destination,
                **({"path": self.logging.path} if self.logging.path else {}),
            },
            "init": {"revision": self.init.revision},
            "routes": {
                topic: {
                    "skill": route.skill,
                    **({"revision": route.revision} if route.revision else {}),
                    "concurrency": route.concurrency,
                    "enabled": route.enabled,
                    **(
                        {"timeout_seconds": route.timeout_seconds}
                        if route.timeout_seconds
                        else {}
                    ),
                    **(
                        {"max_attempts": route.max_attempts}
                        if route.max_attempts
                        else {}
                    ),
                    **(
                        {"max_iterations": route.max_iterations}
                        if route.max_iterations
                        else {}
                    ),
                }
                for topic, route in self.routes.items()
            },
        }


class _Loader(yaml.SafeLoader):
    pass


def _construct_mapping(loader: _Loader, node: yaml.MappingNode) -> dict[Any, Any]:
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=True)
        if key in mapping:
            raise ConfigError(f"duplicate YAML key: {key!r}")
        mapping[key] = loader.construct_object(value_node, deep=True)
    return mapping


_Loader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_mapping
)


def read_raw(path: Path) -> dict[str, Any]:
    try:
        text = path.read_text()
    except FileNotFoundError as error:
        raise ConfigError(
            f"configuration not found: {path} (run `agentqueue init`)"
        ) from error
    except OSError as error:
        raise ConfigError(f"cannot read {path}: {error}") from error
    try:
        data = yaml.load(text, Loader=_Loader)
    except ConfigError:
        raise
    except yaml.YAMLError as error:
        raise ConfigError(f"invalid YAML in {path}: {error}") from error
    if not isinstance(data, dict):
        raise ConfigError(f"{path} must contain a YAML mapping")
    return data


def write_raw(path: Path, data: dict[str, Any]) -> None:
    text = yaml.safe_dump(data, sort_keys=False, allow_unicode=True, width=100)
    atomic_write(path, text.encode(), mode=0o600)


def _mapping(data: Any, name: str, allowed: set[str]) -> dict[str, Any]:
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ConfigError(f"{name} must be a mapping")
    unknown = set(data) - allowed
    if unknown:
        raise ConfigError(
            f"unknown {name} key(s): {', '.join(sorted(map(str, unknown)))}"
        )
    return data


def _bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise ConfigError(f"{name} must be true or false")
    return value


def _int(value: Any, name: str, minimum: int = 1, maximum: int = 1_000_000) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{name} must be an integer")
    if not minimum <= value <= maximum:
        raise ConfigError(f"{name} must be between {minimum} and {maximum}")
    return value


def _float(value: Any, name: str, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(f"{name} must be a number")
    number = float(value)
    if not minimum <= number <= maximum:
        raise ConfigError(f"{name} must be between {minimum} and {maximum}")
    return number


def _string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{name} must be a non-empty string")
    return value.strip()


def _env_name(value: Any, name: str) -> str:
    text = _string(value, name)
    if not ENV_NAME.fullmatch(text):
        raise ConfigError(f"{name} must be an environment variable name")
    return text


def _revision(value: Any, name: str) -> str:
    text = _string(value, name)
    if text.startswith("sha256:"):
        text = text[7:]
    if len(text) != 64 or any(
        character not in "0123456789abcdef" for character in text
    ):
        raise ConfigError(
            f"{name} must be a SHA-256 hex digest, optionally prefixed with sha256:"
        )
    return text


def parse_config(data: dict[str, Any]) -> Config:
    _mapping(
        data,
        "top-level",
        {
            "version",
            "amqp",
            "model",
            "runtime",
            "defaults",
            "logging",
            "init",
            "routes",
        },
    )
    version = data.get("version", SUPPORTED_VERSION)
    if version != SUPPORTED_VERSION:
        raise ConfigError(
            f"unsupported config version {version!r}; this build supports {SUPPORTED_VERSION}"
        )

    amqp_data = _mapping(data.get("amqp"), "amqp", {"host", "port"})
    amqp = AmqpConfig(
        host=_string(amqp_data.get("host", "127.0.0.1"), "amqp.host"),
        port=_int(amqp_data.get("port", 5672), "amqp.port", 0, 65535),
    )

    model_data = _mapping(
        data.get("model"), "model", {"name_env", "api_key_env", "base_url_env"}
    )
    model = ModelConfig(
        name_env=_env_name(model_data.get("name_env", "AGENT_MODEL"), "model.name_env"),
        api_key_env=_env_name(
            model_data.get("api_key_env", "AGENT_API_KEY"), "model.api_key_env"
        ),
        base_url_env=_env_name(
            model_data.get("base_url_env", "AGENT_BASE_URL"), "model.base_url_env"
        ),
    )

    runtime_data = _mapping(
        data.get("runtime"),
        "runtime",
        {"max_parallel_jobs", "max_parallel_init", "shutdown_grace_seconds"},
    )
    runtime = RuntimeConfig(
        max_parallel_jobs=_int(
            runtime_data.get("max_parallel_jobs", 8),
            "runtime.max_parallel_jobs",
            1,
            1024,
        ),
        max_parallel_init=_int(
            runtime_data.get("max_parallel_init", 1), "runtime.max_parallel_init", 1, 32
        ),
        shutdown_grace_seconds=_float(
            runtime_data.get("shutdown_grace_seconds", 30),
            "runtime.shutdown_grace_seconds",
            0.1,
            3600,
        ),
    )

    defaults_data = _mapping(
        data.get("defaults"),
        "defaults",
        {"concurrency", "timeout_seconds", "max_attempts", "max_iterations", "retry"},
    )
    retry_data = _mapping(
        defaults_data.get("retry"),
        "defaults.retry",
        {"initial_seconds", "maximum_seconds", "jitter"},
    )
    initial = _float(
        retry_data.get("initial_seconds", 2),
        "defaults.retry.initial_seconds",
        0.01,
        3600,
    )
    maximum = _float(
        retry_data.get("maximum_seconds", 60),
        "defaults.retry.maximum_seconds",
        0.01,
        86400,
    )
    if maximum < initial:
        raise ConfigError("defaults.retry.maximum_seconds must be >= initial_seconds")
    defaults = Defaults(
        concurrency=_int(
            defaults_data.get("concurrency", 1), "defaults.concurrency", 1, 1024
        ),
        timeout_seconds=_float(
            defaults_data.get("timeout_seconds", 300),
            "defaults.timeout_seconds",
            0.1,
            86400,
        ),
        max_attempts=_int(
            defaults_data.get("max_attempts", 3), "defaults.max_attempts", 1, 1000
        ),
        max_iterations=_int(
            defaults_data.get("max_iterations", 30), "defaults.max_iterations", 1, 10000
        ),
        retry=RetryConfig(
            initial,
            maximum,
            _bool(retry_data.get("jitter", True), "defaults.retry.jitter"),
        ),
    )

    logging_data = _mapping(
        data.get("logging"), "logging", {"level", "format", "destination", "path"}
    )
    level = _string(logging_data.get("level", "info"), "logging.level").lower()
    fmt = _string(logging_data.get("format", "json"), "logging.format").lower()
    destination = _string(
        logging_data.get("destination", "stderr"), "logging.destination"
    ).lower()
    if level not in LOG_LEVELS:
        raise ConfigError(
            f"logging.level must be one of {', '.join(sorted(LOG_LEVELS))}"
        )
    if fmt not in LOG_FORMATS:
        raise ConfigError(
            f"logging.format must be one of {', '.join(sorted(LOG_FORMATS))}"
        )
    if destination not in {"stderr", "file"}:
        raise ConfigError("logging.destination must be stderr or file")
    log_path = logging_data.get("path")
    if destination == "file":
        log_path = _string(log_path, "logging.path")
    elif log_path is not None:
        raise ConfigError("logging.path is only valid when logging.destination=file")
    logging_config = LoggingConfig(level, fmt, destination, log_path)

    init_data = _mapping(data.get("init"), "init", {"revision"})
    init_revision = init_data.get("revision", "")
    if not isinstance(init_revision, str):
        raise ConfigError("init.revision must be a string")
    init = InitConfig(init_revision.strip())

    routes_data = data.get("routes", {})
    if not isinstance(routes_data, dict):
        raise ConfigError("routes must be a mapping of topic to route")
    routes: dict[str, Route] = {}
    for raw_topic, raw_route in routes_data.items():
        try:
            topic = normalize_topic(str(raw_topic))
        except ValueError as error:
            raise ConfigError(f"routes.{raw_topic}: {error}") from error
        route_data = _mapping(
            raw_route,
            f"routes.{topic}",
            {
                "skill",
                "revision",
                "concurrency",
                "enabled",
                "timeout_seconds",
                "max_attempts",
                "max_iterations",
            },
        )
        try:
            skill = normalize_skill_name(
                _string(route_data.get("skill"), f"routes.{topic}.skill")
            )
        except ValueError as error:
            raise ConfigError(f"routes.{topic}.skill: {error}") from error
        revision = (
            _revision(route_data["revision"], f"routes.{topic}.revision")
            if "revision" in route_data
            else None
        )
        concurrency = (
            _int(route_data["concurrency"], f"routes.{topic}.concurrency", 1, 1024)
            if "concurrency" in route_data
            else None
        )
        timeout = (
            _float(
                route_data["timeout_seconds"],
                f"routes.{topic}.timeout_seconds",
                0.1,
                86400,
            )
            if "timeout_seconds" in route_data
            else None
        )
        attempts = (
            _int(route_data["max_attempts"], f"routes.{topic}.max_attempts", 1, 1000)
            if "max_attempts" in route_data
            else None
        )
        iterations = (
            _int(
                route_data["max_iterations"], f"routes.{topic}.max_iterations", 1, 10000
            )
            if "max_iterations" in route_data
            else None
        )
        routes[topic] = Route(
            topic=topic,
            skill=skill,
            revision=revision,
            concurrency=concurrency,
            enabled=_bool(route_data.get("enabled", True), f"routes.{topic}.enabled"),
            timeout_seconds=timeout,
            max_attempts=attempts,
            max_iterations=iterations,
        )
    return Config(amqp, model, runtime, defaults, logging_config, init, routes, version)


def load_env(path: Path) -> dict[str, str]:
    try:
        values = dotenv_values(path)
    except OSError as error:
        raise ConfigError(f"cannot read {path}: {error}") from error
    file_values = {key: value or "" for key, value in values.items()}
    # Process environment wins over .env, as documented.
    return {**file_values, **{k: v for k, v in os.environ.items()}}


def load_config(paths: Paths) -> tuple[Config, dict[str, str]]:
    config = parse_config(read_raw(paths.config))
    return config, load_env(paths.env)


def update_config(paths: Paths, mutate) -> Config:
    """Serialize CLI config edits; validate before replacing the file."""
    with file_lock(paths.config_lock, blocking=True):
        raw = read_raw(paths.config)
        mutate(raw)
        config = parse_config(raw)
        write_raw(paths.config, raw)
    return config


def default_config() -> dict[str, Any]:
    return {
        "version": 1,
        "amqp": {"host": "127.0.0.1", "port": 5672},
        "model": {
            "name_env": "AGENT_MODEL",
            "api_key_env": "AGENT_API_KEY",
            "base_url_env": "AGENT_BASE_URL",
        },
        "runtime": {
            "max_parallel_jobs": 8,
            "max_parallel_init": 1,
            "shutdown_grace_seconds": 30,
        },
        "defaults": {
            "concurrency": 1,
            "timeout_seconds": 300,
            "max_attempts": 3,
            "max_iterations": 30,
            "retry": {"initial_seconds": 2, "maximum_seconds": 60, "jitter": True},
        },
        "logging": {"level": "info", "format": "json", "destination": "stderr"},
        "init": {"revision": ""},
        "routes": {},
    }
