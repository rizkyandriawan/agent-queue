import contextlib
import io
import json
import os
import tarfile
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from agent_queue.cli import main
from agent_queue.client import request
from agent_queue.daemon import Daemon

from helpers import FakeRunner, add_route, home, install, wait_for


class CliLiveTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.paths = home(self.root)
        self.source = install(self.paths, self.root)
        add_route(self.paths, "ticket.create", self.source)
        self.runner = FakeRunner()
        self.daemon = Daemon(self.paths, runner=self.runner)
        self.thread = threading.Thread(target=self.daemon.run, daemon=True)
        self.thread.start()
        wait_for(
            lambda: (
                (self.daemon.status().get("routes") or [{}])[0].get("state") == "ready"
            ),
            5,
        )

    def tearDown(self):
        self.daemon.stopping.set()
        self.thread.join(5)
        self.directory.cleanup()

    def cli(self, *arguments):
        output = io.StringIO()
        errors = io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
            code = main(["--home", str(self.paths.home), *arguments])
        return code, output.getvalue(), errors.getvalue()

    def send(self, body=b'{"id": 7}', topic="ticket.create", queue=None):
        code, _, errors = self.cli("send", "--topic", topic, "--message", body.decode())
        self.assertEqual(code, 0, errors)
        return self.take(queue or f"{topic}.results")

    def take(self, queue, timeout=5):
        def attempt():
            deliveries = self.daemon.ante.take(queue, max=1, wait_ms=50)
            if not deliveries:
                return None
            payload = deliveries[0].payload
            deliveries[0].ack()
            return payload

        return wait_for(attempt, timeout)

    def test_list_show_status_pause_resume_prepare(self):
        code, output, _ = self.cli("list", "--json")
        self.assertEqual(code, 0)
        data = json.loads(output)["data"]
        self.assertEqual(data["daemon"], "online")
        self.assertEqual(data["routes"][0]["state"], "ready")
        code, output, _ = self.cli("list")
        self.assertIn("ticket.create", output)

        code, output, _ = self.cli("show", "ticket.create", "--json")
        self.assertEqual(code, 0)
        shown = json.loads(output)["data"]
        self.assertEqual(shown["live"]["state"], "ready")
        self.assertTrue(shown["skill"]["has_init"])

        code, output, _ = self.cli("status", "--json")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output)["data"]["daemon"]["state"], "running")
        self.assertEqual(self.cli("status", "--check", "ready")[0], 0)
        self.assertEqual(self.cli("status")[0], 0)

        self.assertEqual(self.cli("pause", "ticket.create")[0], 0)
        self.assertEqual(self.cli("resume", "ticket.create")[0], 0)
        before = len(self.runner.init_calls)
        code, output, errors = self.cli("prepare", "ticket.create", "--force")
        self.assertEqual(code, 0, errors)
        self.assertIn("succeeded", output)
        self.assertEqual(len(self.runner.init_calls), before + 1)
        wait_for(lambda: self.daemon.status()["routes"][0]["state"] == "ready")

    def test_add_update_remove_online_and_send(self):
        code, output, errors = self.cli(
            "add",
            f"--file={self.source.root / 'SKILL.md'}",
            "--topic=ticket.update",
            "--wait",
        )
        self.assertEqual(code, 0, errors)
        self.assertIn("ready", output)
        self.assertIn(
            "ticket.update",
            [route["topic"] for route in self.daemon.status()["routes"]],
        )
        code, output, errors = self.cli(
            "update",
            f"--file={self.source.root / 'SKILL.md'}",
            "--topic=ticket.update",
            "--wait",
            "--concurrency",
            "2",
        )
        self.assertEqual(code, 0, errors)
        wait_for(
            lambda: (
                next(
                    route["state"]
                    for route in self.daemon.status()["routes"]
                    if route["topic"] == "ticket.update"
                )
                == "ready"
            )
        )

        result = self.send(topic="ticket.update")
        self.assertEqual(result["status"], "succeeded")
        code, output, _ = self.cli("jobs", "list", "--json")
        self.assertEqual(code, 0)
        jobs = json.loads(output)["data"]
        self.assertTrue(jobs)
        job_id = jobs[0]["job_id"]
        code, output, _ = self.cli("jobs", "show", job_id, "--json")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output)["data"]["job_id"], job_id)

        self.assertEqual(self.cli("remove", "--topic=ticket.update")[0], 0)
        self.assertEqual(self.cli("remove", "--topic=ticket.update")[0], 65)
        self.assertEqual(
            self.cli("remove", "--topic=ticket.create", "--delete-data")[0], 64
        )

    def test_failure_jobs_retry_and_backup(self):
        raw = self.paths.config.read_text()
        self.paths.config.write_text(
            raw.replace("max_attempts: 3", "max_attempts: 1", 1)
        )
        request(self.paths, "reload")
        wait_for(lambda: self.daemon.status()["routes"][0]["state"] == "ready")
        self.runner.failures = 1
        failure = self.send(queue="ticket.create.failed")
        self.assertEqual(failure["status"], "failed")
        wait_for(lambda: self.daemon.jobs.get("j_1")["status"] == "failed")
        self.runner.failures = 0
        code, output, errors = self.cli("jobs", "retry", "j_1", "--wait")
        self.assertEqual(code, 0, errors)
        self.assertIn("Retried", output)
        self.take("ticket.create.results")
        code, output, _ = self.cli("jobs", "show", "j_1")
        self.assertIn('"retried_as"', output)

        code, _, errors = self.cli("backup", "--output", str(self.root / "live.tar.gz"))
        self.assertEqual(code, 75)
        self.daemon.stop()
        output_path = self.root / "backup.tar.gz"
        code, _, errors = self.cli(
            "backup",
            "--output",
            str(output_path),
            "--with-workspaces",
            "--with-env",
        )
        self.assertEqual(code, 0, errors)
        with tarfile.open(output_path) as archive:
            names = archive.getnames()
        self.assertIn("config.yaml", names)
        self.assertIn(".env", names)
        self.assertTrue(any(name.startswith("workspaces/") for name in names))
        self.assertEqual(output_path.stat().st_mode & 0o777, 0o600)

    def test_stop_via_control_socket(self):
        code, _, errors = self.cli("stop")
        self.assertEqual(code, 0, errors)
        wait_for(lambda: self.daemon._state == "stopped")
        code, output, _ = self.cli("status")
        self.assertIn("offline", output)

    def test_service_start_restart_logs_and_serve(self):
        unit = self.root / "agentqueue.service"
        unit.write_text("[Unit]\n")
        with (
            patch("agent_queue.cli.unit_path", return_value=unit),
            patch("agent_queue.cli.service_start") as start,
            patch("agent_queue.cli.service_restart") as restart,
            patch("agent_queue.cli.service_stop") as stop,
            patch("agent_queue.cli.service_enable") as enable,
            patch("agent_queue.cli.service_disable") as disable,
            patch("agent_queue.cli.service_uninstall") as uninstall,
            patch("agent_queue.cli.service_active", return_value=True),
            patch("agent_queue.cli.service_install", return_value=unit) as install,
            patch("agent_queue.cli.service_logs", return_value=0) as logs,
        ):
            self.assertEqual(self.cli("start")[0], 0)
            self.assertEqual(self.cli("restart")[0], 0)
            self.assertEqual(self.cli("stop")[0], 0)
            self.assertEqual(self.cli("service", "install", "--enable")[0], 0)
            self.assertEqual(self.cli("service", "enable")[0], 0)
            self.assertEqual(self.cli("service", "disable")[0], 0)
            self.assertEqual(self.cli("service", "uninstall")[0], 0)
            self.assertEqual(self.cli("service", "status")[0], 0)
            self.assertEqual(self.cli("logs", "-n", "3", "--level", "error")[0], 0)
        start.assert_called_once()
        restart.assert_called_once()
        stop.assert_called_once()
        self.assertEqual(enable.call_count, 2)
        disable.assert_called_once()
        uninstall.assert_called_once()
        install.assert_called_once()
        logs.assert_called_once()

        class StubDaemon:
            def __init__(self, paths):
                self.paths = paths

            def run(self):
                return 0

        with patch("agent_queue.cli.Daemon", StubDaemon):
            self.assertEqual(self.cli("serve")[0], 0)

    def test_doctor_and_config(self):
        with patch.dict(os.environ, {"XDG_CONFIG_HOME": str(self.root / "xdg")}):
            code, output, _ = self.cli("doctor", "--json")
            self.assertEqual(code, 75)
            checks = json.loads(output)["data"]
            self.assertTrue(any(item["check"] == "daemon" for item in checks))
            code, output, _ = self.cli("config", "show", "--json")
            self.assertEqual(code, 0)
            self.assertEqual(json.loads(output)["data"]["config"]["version"], 1)


if __name__ == "__main__":
    unittest.main()
