import stat
import tempfile
import time
import unittest
from pathlib import Path

from agent_queue.client import request
from agent_queue.config import read_raw, write_raw
from agent_queue.daemon import Daemon
from agent_queue.registry import import_skill

from helpers import FakeRunner, add_route, home, install, publish, wait_for


class DaemonTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.paths = home(self.root)
        self.skill = install(self.paths, self.root)
        add_route(self.paths, "ticket.create", self.skill, concurrency=1)
        self.runner = FakeRunner()
        self.daemon = Daemon(self.paths, runner=self.runner)
        self.daemon.start()
        wait_for(lambda: self.route()["state"] == "ready", 5)

    def tearDown(self):
        self.daemon.stop()
        self.directory.cleanup()

    def route(self):
        routes = self.daemon.status()["routes"]
        return routes[0]

    def job(self, job_id):
        try:
            return self.daemon.jobs.get(job_id)
        except KeyError:
            return {}

    def result(self, queue="ticket.create.results", timeout=5):
        return wait_for(lambda: self._take(queue), timeout)

    def _take(self, queue):
        deliveries = self.daemon.ante.take(queue, max=1, wait_ms=50)
        if not deliveries:
            return None
        delivery = deliveries[0]
        payload = delivery.payload
        delivery.ack()
        return payload

    def test_init_control_socket_and_successful_job(self):
        self.assertTrue(
            (
                self.paths.workspaces / "demo" / self.route()["workspace"] / "init.txt"
            ).is_file()
        )
        mode = stat.S_IMODE(self.paths.socket.stat().st_mode)
        self.assertEqual(mode, 0o600)
        status = request(self.paths, "status")
        self.assertEqual(status["daemon"]["state"], "running")
        self.assertEqual(status["routes"][0]["state"], "ready")

        host, port = self.daemon.amqp_address
        publish(host, port, "ticket.create", b'{"id": 1}', message_id="app-1")
        wait_for(lambda: self.daemon.ante.info("ticket.create")["total"] == 0)
        result = self.result()
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["application_message_id"], "app-1")
        job = self.daemon.jobs.get("j_1")
        self.assertEqual(job["status"], "succeeded")
        self.assertEqual(job["attempts"][0]["status"], "succeeded")
        self.assertEqual(self.runner.messages[0][1], b'{"id": 1}')

    def test_pause_resume_and_reload(self):
        request(self.paths, "route.pause", {"topic": "ticket.create"})
        self.assertTrue(self.route()["paused"])
        host, port = self.daemon.amqp_address
        publish(host, port, "ticket.create", b"paused")
        time.sleep(0.3)
        self.assertEqual(self.daemon.ante.info("ticket.create")["total"], 1)
        request(self.paths, "route.resume", {"topic": "ticket.create"})
        self.assertFalse(self.route()["paused"])
        self.assertEqual(self.result()["status"], "succeeded")

        (self.root / "src-demo" / "SKILL.md").write_text("# v2\nNew behavior.\n")
        new_skill = import_skill(
            self.paths, self.root / "src-demo" / "SKILL.md", "demo"
        )
        raw = read_raw(self.paths.config)
        raw["routes"]["ticket.create"]["revision"] = new_skill.revision
        write_raw(self.paths.config, raw)
        data = self.daemon.reload()
        self.assertNotEqual(data["revision"], "0" * 12)
        wait_for(lambda: self.route()["state"] == "ready")
        publish(host, port, "ticket.create", b"after reload")
        self.assertEqual(self.result()["status"], "succeeded")
        self.assertEqual(self.runner.messages[-1][0], new_skill.revision)

    def test_retry_then_success(self):
        self.runner.failures = 1
        host, port = self.daemon.amqp_address
        publish(host, port, "ticket.create", b"retry me")
        result = self.result(timeout=6)
        self.assertEqual(result["status"], "succeeded")
        wait_for(lambda: self.job("j_1").get("status") == "succeeded")
        job = self.daemon.jobs.get("j_1")
        self.assertEqual(
            [item["status"] for item in job["attempts"]], ["retry_wait", "succeeded"]
        )
        self.assertEqual(job["attempts"][1]["attempt"], 2)

    def test_terminal_failure_and_manual_retry(self):
        raw = read_raw(self.paths.config)
        raw["routes"]["ticket.create"]["max_attempts"] = 1
        write_raw(self.paths.config, raw)
        self.daemon.reload()
        wait_for(lambda: self.route()["state"] == "ready")
        self.runner.failures = 1
        host, port = self.daemon.amqp_address
        publish(host, port, "ticket.create", b"will fail")
        failure = self.result("ticket.create.failed", timeout=6)
        self.assertEqual(failure["status"], "failed")
        job = self.daemon.jobs.get("j_1")
        self.assertEqual(job["status"], "failed")
        self.assertEqual(job["payload_base64"], "d2lsbCBmYWls")
        data = request(self.paths, "jobs.retry", {"job_id": "j_1"})
        result = self.result(timeout=6)
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(data["new_job_id"], f"j_{data['message_id']}")
        self.assertEqual(self.daemon.jobs.get("j_1")["retried_as"], data["new_job_id"])
        self.assertEqual(
            self.daemon.jobs.get(data["new_job_id"])["status"], "succeeded"
        )

    def test_retry_keeps_the_bound_revision_after_reload(self):
        raw = read_raw(self.paths.config)
        raw["defaults"]["retry"]["initial_seconds"] = 1.0
        raw["defaults"]["retry"]["maximum_seconds"] = 2.0
        raw["routes"]["ticket.create"]["max_attempts"] = 3
        write_raw(self.paths.config, raw)
        self.daemon.reload()
        wait_for(lambda: self.route()["state"] == "ready")
        old_revision = self.route()["revision"]
        self.runner.failures = 1
        host, port = self.daemon.amqp_address
        publish(host, port, "ticket.create", b"bound")
        wait_for(lambda: self.job("j_1").get("status") == "retry_wait")

        (self.root / "src-demo" / "SKILL.md").write_text("# v2\nBound test.\n")
        new_skill = import_skill(
            self.paths, self.root / "src-demo" / "SKILL.md", "demo"
        )
        raw = read_raw(self.paths.config)
        raw["routes"]["ticket.create"]["revision"] = new_skill.revision
        write_raw(self.paths.config, raw)
        self.daemon.reload()
        wait_for(lambda: self.route()["state"] == "ready")

        self.assertEqual(self.result(timeout=6)["status"], "succeeded")
        wait_for(lambda: self.job("j_1").get("status") == "succeeded")
        job = self.job("j_1")
        self.assertEqual(job["revision"], old_revision)
        self.assertEqual(
            [message[0] for message in self.runner.messages[:2]],
            [old_revision, old_revision],
        )
        publish(host, port, "ticket.create", b"new revision")
        self.assertEqual(self.result(timeout=6)["status"], "succeeded")
        self.assertEqual(self.runner.messages[-1][0], new_skill.revision)

    def test_changing_init_revision_reruns_init_in_place(self):
        before = self.route()["workspace"]
        calls = len(self.runner.init_calls)
        raw = read_raw(self.paths.config)
        raw["init"]["revision"] = "refresh-1"
        write_raw(self.paths.config, raw)
        self.daemon.reload()
        wait_for(lambda: len(self.runner.init_calls) == calls + 1)
        wait_for(lambda: self.route()["state"] == "ready")
        self.assertNotEqual(self.route()["workspace"], before)

    def test_parallel_capacity(self):
        raw = read_raw(self.paths.config)
        raw["routes"]["ticket.create"]["concurrency"] = 2
        write_raw(self.paths.config, raw)
        self.daemon.reload()
        wait_for(lambda: self.route()["state"] == "ready")
        self.runner.barrier = __import__("threading").Barrier(2, timeout=3)
        self.runner.delay = 0.2
        host, port = self.daemon.amqp_address
        for index in range(4):
            publish(host, port, "ticket.create", f"job {index}".encode())
        for _ in range(4):
            self.result(timeout=8)
        self.assertEqual(self.runner.peak, 2)


class InitFailureTests(unittest.TestCase):
    def test_failed_init_blocks_dispatch_and_reports_state(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = home(root)
            skill = install(paths, root)
            add_route(paths, "topic", skill)
            runner = FakeRunner()
            runner.fail_init = True
            daemon = Daemon(paths, runner=runner)
            daemon.start()
            try:
                wait_for(lambda: daemon.status()["routes"][0]["state"] == "init_failed")
                host, port = daemon.amqp_address
                publish(host, port, "topic", b"waiting")
                time.sleep(0.3)
                self.assertEqual(daemon.ante.info("topic")["total"], 1)
                self.assertEqual(runner.messages, [])
                status = request(paths, "status")
                self.assertEqual(status["routes"][0]["state"], "init_failed")
                self.assertIn("setup failed", status["routes"][0]["error"])
            finally:
                daemon.stop()


if __name__ == "__main__":
    unittest.main()
