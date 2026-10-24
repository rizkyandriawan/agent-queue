import json
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path

from agent_queue import client
from agent_queue.errors import CliError, DaemonUnavailable
from agent_queue.paths import Paths


class FakeControl:
    def __init__(self, path, responses):
        self.responses = list(responses)
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.socket.bind(str(path))
        self.socket.listen()
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def run(self):
        while True:
            try:
                connection, _ = self.socket.accept()
            except OSError:
                return
            with connection:
                connection.recv(65536)
                if not self.responses:
                    continue
                delay, payload = self.responses.pop(0)
                if delay:
                    time.sleep(delay)
                try:
                    connection.sendall(payload)
                except OSError:
                    pass

    def close(self):
        self.socket.close()


class ClientTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.paths = Paths(Path(self.directory.name) / "home")
        self.paths.run.mkdir(parents=True)

    def tearDown(self):
        self.directory.cleanup()

    def serve(self, responses):
        server = FakeControl(self.paths.socket, responses)
        self.addCleanup(server.close)
        return server

    def test_missing_socket_is_unavailable(self):
        with self.assertRaises(DaemonUnavailable):
            client.request(self.paths, "ping")
        self.assertFalse(client.daemon_online(self.paths))

    def test_ok_error_malformed_and_unterminated(self):
        self.serve(
            [
                (0, json.dumps({"ok": True, "data": {"pong": 1}}).encode() + b"\n"),
                (
                    0,
                    json.dumps({"ok": False, "error": {"message": "no"}}).encode()
                    + b"\n",
                ),
                (0, b"not-json\n"),
                (0, json.dumps({"ok": True, "data": {"raw": True}}).encode()),
            ]
        )
        self.assertEqual(client.request(self.paths, "ping"), {"pong": 1})
        with self.assertRaises(CliError) as error:
            client.request(self.paths, "ping")
        self.assertEqual(error.exception.exit_code, 75)
        with self.assertRaises(CliError) as error:
            client.request(self.paths, "ping")
        self.assertEqual(error.exception.exit_code, 70)
        self.assertEqual(client.request(self.paths, "ping"), {"raw": True})

    def test_timeout(self):
        self.serve([(1.0, b'{"ok": true, "data": {}}\n')])
        with self.assertRaises(CliError) as error:
            client.request(self.paths, "ping", timeout=0.1)
        self.assertEqual(error.exception.exit_code, 75)

    def test_wait_operation(self):
        self.serve(
            [
                (0, b'{"ok": true, "data": {"status": "running"}}\n'),
                (0, b'{"ok": true, "data": {"status": "succeeded", "id": "op"}}\n'),
            ]
        )
        result = client.wait_operation(self.paths, "op", 2)
        self.assertEqual(result["status"], "succeeded")

    def test_wait_operation_failure(self):
        self.serve(
            [(0, b'{"ok": true, "data": {"status": "failed", "error": "boom"}}\n')]
        )
        with self.assertRaises(CliError) as error:
            client.wait_operation(self.paths, "op", 2)
        self.assertEqual(error.exception.exit_code, 65)

    def test_wait_operation_timeout(self):
        self.serve([(0, b'{"ok": true, "data": {"status": "running"}}\n')])
        with self.assertRaises(CliError) as error:
            client.wait_operation(self.paths, "op", 0.3)
        self.assertEqual(error.exception.exit_code, 75)

    def test_wait_route_ready_failed_and_timeout(self):
        self.serve(
            [
                (0, b'{"ok": true, "data": {"routes": []}}\n'),
                (
                    0,
                    b'{"ok": true, "data": {"routes": [{"topic": "x", '
                    b'"state": "initializing"}]}}\n',
                ),
                (
                    0,
                    b'{"ok": true, "data": {"routes": [{"topic": "x", '
                    b'"state": "ready", "workspace": "g"}]}}\n',
                ),
                (
                    0,
                    b'{"ok": true, "data": {"routes": [{"topic": "y", '
                    b'"state": "init_failed", "error": "bad"}]}}\n',
                ),
                (0, b'{"ok": true, "data": {"routes": []}}\n'),
            ]
        )
        route = client.wait_route(self.paths, "x", 2)
        self.assertEqual(route["workspace"], "g")
        with self.assertRaises(CliError) as error:
            client.wait_route(self.paths, "y", 2)
        self.assertIn("bad", str(error.exception))
        with self.assertRaises(CliError):
            client.wait_route(self.paths, "z", 0.3)

    def test_daemon_online_connection_refused(self):
        self.paths.socket.touch()
        self.assertFalse(client.daemon_online(self.paths))


if __name__ == "__main__":
    unittest.main()
