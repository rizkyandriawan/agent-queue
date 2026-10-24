import io
import json
import logging
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path

from agent_queue.config import LoggingConfig
from agent_queue.logging_setup import configure_logging, event
from agent_queue.paths import Paths


class LoggingTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.paths = Paths(Path(self.directory.name) / "home")
        self.paths.ensure()

    def tearDown(self):
        for handler in logging.getLogger().handlers:
            handler.close()
        logging.getLogger().handlers.clear()
        self.directory.cleanup()

    def test_json_to_stderr(self):
        stream = io.StringIO()
        with redirect_stderr(stream):
            configure_logging(LoggingConfig(format="json"), self.paths)
            event(logging.getLogger("test"), logging.INFO, "unit.event", topic="x")
        payload = json.loads(stream.getvalue())
        self.assertEqual(payload["event"], "unit.event")
        self.assertEqual(payload["topic"], "x")
        self.assertEqual(payload["level"], "info")

    def test_text_to_stderr_and_file(self):
        stream = io.StringIO()
        with redirect_stderr(stream):
            configure_logging(LoggingConfig(format="text", level="debug"), self.paths)
            event(logging.getLogger("test"), logging.WARNING, "warned", job="j_1")
        self.assertIn("WARNING warned job=j_1", stream.getvalue())

        configure_logging(
            LoggingConfig(destination="file", path="logs/daemon.log"), self.paths
        )
        log = logging.getLogger("test")
        event(log, logging.INFO, "to.file", value=1)
        for handler in logging.getLogger().handlers:
            handler.flush()
        text = (self.paths.home / "logs" / "daemon.log").read_text()
        self.assertIn("to.file", text)
        self.assertIn('"value": 1', text)


if __name__ == "__main__":
    unittest.main()
