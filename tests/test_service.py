import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from agent_queue import service
from agent_queue.paths import Paths


class ServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.paths = Paths(self.root / "home")
        self.environment = patch.dict(
            os.environ, {"XDG_CONFIG_HOME": str(self.root / "config")}
        )
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def tearDown(self):
        self.directory.cleanup()

    def test_render_unit_escapes_and_covers_lifecycle(self):
        text = service.render_unit(self.paths, grace=60)
        self.assertIn("Type=simple", text)
        self.assertIn("TimeoutStopSec=70", text)
        self.assertIn("UMask=0077", text)
        self.assertIn("ExecReload=/bin/kill -HUP $MAINPID", text)
        self.assertIn("RestartPreventExitStatus=64 78", text)
        self.assertIn(str(self.paths.home), text)
        self.assertIn("-m agent_queue --home", text)

    def test_install_and_uninstall(self):
        calls = []
        with patch.object(
            service,
            "_systemctl",
            side_effect=lambda *args, **kwargs: calls.append(args),
        ):
            path = service.install(self.paths, grace=10)
            self.assertEqual(path, service.unit_path())
            self.assertEqual(path.stat().st_mode & 0o777, 0o644)
            self.assertIn("TimeoutStopSec=40", path.read_text())
            self.assertEqual(calls, [("daemon-reload",)])
            service.uninstall()
        self.assertFalse(path.exists())
        self.assertEqual(calls[-1], ("daemon-reload",))

    def test_wrappers_reject_missing_systemctl(self):
        with patch.object(service.shutil, "which", return_value=None):
            with self.assertRaises(service.CliError):
                service.enable()
        with patch.object(service.shutil, "which", return_value=None):
            with self.assertRaises(service.CliError):
                service.logs(["-n", "5"])

    def test_wrappers_and_active(self):
        results = [
            subprocess.CompletedProcess(["systemctl"], 0, "active\n", ""),
            subprocess.CompletedProcess(["systemctl"], 1, "inactive\n", "bad\n"),
        ]
        calls = []

        def fake_run(arguments, text, capture_output):
            calls.append(arguments)
            return results.pop(0)

        with patch.object(service.shutil, "which", return_value="/bin/systemctl"):
            with patch.object(service.subprocess, "run", side_effect=fake_run):
                self.assertTrue(service.active())
                self.assertFalse(service.active())
        with patch.object(service.shutil, "which", return_value="/bin/systemctl"):
            with patch.object(
                service.subprocess,
                "run",
                return_value=subprocess.CompletedProcess(["systemctl"], 1, "", "boom"),
            ):
                with self.assertRaises(service.CliError):
                    service.start()

    def test_logs_invokes_journalctl(self):
        with patch.object(service.shutil, "which", return_value="/bin/journalctl"):
            with patch.object(service.subprocess, "call", return_value=7) as call:
                self.assertEqual(service.logs(["-n", "3"]), 7)
        call.assert_called_once_with(
            ["journalctl", "--user", "-u", service.UNIT, "-n", "3"]
        )
        self.assertIn("loginctl enable-linger", service.lingering_hint())


if __name__ == "__main__":
    unittest.main()
