import json
import tempfile
import unittest
from pathlib import Path

from agent_queue.errors import CliError
from agent_queue.util import (
    atomic_json,
    atomic_write,
    daemon_is_locked,
    file_lock,
    looks_secret,
    new_id,
    normalize_skill_name,
    normalize_topic,
    redact_env,
)


class UtilTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)

    def tearDown(self):
        self.directory.cleanup()

    def test_atomic_write_and_json(self):
        path = self.root / "nested" / "state.json"
        atomic_json(path, {"a": 1})
        self.assertEqual(json.loads(path.read_text()), {"a": 1})
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        atomic_write(path, b"raw", mode=0o640)
        self.assertEqual(path.read_bytes(), b"raw")
        self.assertEqual(path.stat().st_mode & 0o777, 0o640)
        self.assertEqual(list(path.parent.glob(".state.json.*")), [])

    def test_file_lock_and_daemon_lock(self):
        path = self.root / "daemon.lock"
        with file_lock(path):
            self.assertTrue(daemon_is_locked(path))
            with self.assertRaises(CliError) as error:
                with file_lock(path):
                    pass
            self.assertEqual(error.exception.exit_code, 75)
        self.assertFalse(daemon_is_locked(path))

    def test_normalization_and_ids(self):
        self.assertEqual(normalize_topic("order.created"), "order.created")
        for bad in ("", "a" * 256, "bad\x00topic", "a.*", "a.#"):
            with self.assertRaises(ValueError):
                normalize_topic(bad)
        self.assertEqual(normalize_skill_name("demo.v2"), "demo.v2")
        for bad in ("", ".", "..", "../x", "-x", "a" * 65, "a b"):
            with self.assertRaises(ValueError):
                normalize_skill_name(bad)
        self.assertNotEqual(new_id("job"), new_id("job"))

    def test_redaction(self):
        self.assertTrue(looks_secret("SERVICE_API_TOKEN"))
        self.assertTrue(looks_secret("aws_secret_access_key"))
        self.assertFalse(looks_secret("SERVICE_API_URL"))
        self.assertEqual(
            redact_env(
                {
                    "SERVICE_API_TOKEN": "secret",
                    "SERVICE_API_URL": "http://x",
                    "EMPTY": "",
                }
            ),
            {
                "EMPTY": "",
                "SERVICE_API_TOKEN": "<redacted>",
                "SERVICE_API_URL": "http://x",
            },
        )


if __name__ == "__main__":
    unittest.main()
