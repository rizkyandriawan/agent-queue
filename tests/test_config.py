import tempfile
import unittest
from pathlib import Path

from agent_queue.config import (
    ConfigError,
    default_config,
    load_env,
    parse_config,
    read_raw,
    write_raw,
)
from agent_queue.util import normalize_topic, redact_env


class ConfigTests(unittest.TestCase):
    def test_defaults_and_effective_route(self):
        raw = default_config()
        raw["routes"] = {"ticket.create": {"skill": "demo", "revision": None}}
        del raw["routes"]["ticket.create"]["revision"]
        config = parse_config(raw)
        self.assertEqual(config.amqp.port, 5672)
        self.assertEqual(config.routes["ticket.create"].skill, "demo")
        effective = config.effective(config.routes["ticket.create"])
        self.assertEqual(effective.concurrency, 1)
        self.assertEqual(effective.timeout_seconds, 300.0)
        self.assertEqual(effective.max_attempts, 3)

    def test_unknown_keys_and_bad_types(self):
        raw = default_config()
        raw["surprise"] = 1
        with self.assertRaises(ConfigError):
            parse_config(raw)
        raw = default_config()
        raw["amqp"]["port"] = "5672"
        with self.assertRaises(ConfigError):
            parse_config(raw)
        raw = default_config()
        raw["routes"] = {"wild.*": {"skill": "demo"}}
        with self.assertRaises(ConfigError):
            parse_config(raw)
        raw = default_config()
        raw["routes"] = {"ticket": {"skill": "../escape"}}
        with self.assertRaises(ConfigError):
            parse_config(raw)
        raw = default_config()
        raw["routes"] = {"ticket": {"skill": "demo", "revision": "sha256:abc"}}
        with self.assertRaises(ConfigError):
            parse_config(raw)

    def test_duplicate_yaml_keys_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.yaml"
            path.write_text("version: 1\nroutes: {}\nroutes: {}\n")
            with self.assertRaises(ConfigError):
                read_raw(path)

    def test_route_overrides_and_retry_bounds(self):
        raw = default_config()
        raw["routes"] = {
            "x": {
                "skill": "demo",
                "concurrency": 4,
                "timeout_seconds": 20,
                "max_attempts": 2,
                "max_iterations": 5,
                "enabled": False,
            }
        }
        config = parse_config(raw)
        route = config.effective(config.routes["x"])
        self.assertEqual((route.concurrency, route.timeout_seconds), (4, 20))
        self.assertEqual((route.max_attempts, route.max_iterations), (2, 5))
        self.assertFalse(route.enabled)
        raw["defaults"]["retry"]["maximum_seconds"] = 1
        with self.assertRaises(ConfigError):
            parse_config(raw)

    def test_env_precedence_and_redaction(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env"
            path.write_text(
                "AGENT_MODEL=file\nAGENT_API_KEY=secret\nBUSINESS_TOKEN=abc\n"
            )
            import os

            old = os.environ.get("AGENT_MODEL")
            os.environ["AGENT_MODEL"] = "process"
            try:
                env = load_env(path)
            finally:
                if old is None:
                    os.environ.pop("AGENT_MODEL", None)
                else:
                    os.environ["AGENT_MODEL"] = old
            self.assertEqual(env["AGENT_MODEL"], "process")
            self.assertEqual(env["AGENT_API_KEY"], "secret")
            redacted = redact_env(env)
            self.assertEqual(redacted["AGENT_API_KEY"], "<redacted>")
            self.assertEqual(redacted["BUSINESS_TOKEN"], "<redacted>")
            self.assertEqual(redacted["AGENT_MODEL"], "process")

    def test_round_trip(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.yaml"
            write_raw(path, default_config())
            self.assertEqual(read_raw(path)["version"], 1)
            self.assertIn("routes", read_raw(path))

    def test_topic_validation(self):
        self.assertEqual(normalize_topic("ticket.create"), "ticket.create")
        for bad in ("", "a\nb", "a.*", "a.#", "x" * 256):
            with self.assertRaises(ValueError):
                normalize_topic(bad)


if __name__ == "__main__":
    unittest.main()
