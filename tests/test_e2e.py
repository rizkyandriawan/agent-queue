"""Real AMQP + real AgentScope + local OpenAI-compatible HTTP fixture."""

import json
import os
import re
import shlex
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

from agent_queue.config import read_raw, write_raw
from agent_queue.daemon import Daemon
from agent_queue.registry import import_skill

from helpers import add_route, home, publish, wait_for

SCRIPT = (
    "import json, os, sys, urllib.request\n"
    "payload = json.loads(sys.argv[1])\n"
    "request = urllib.request.Request(os.environ['AUTOMATION_API_URL'], "
    "data=json.dumps(payload).encode(), headers={'Content-Type':'application/json'})\n"
    "print(urllib.request.urlopen(request).read().decode())\n"
)


class OpenAIHandler(BaseHTTPRequestHandler):
    calls = []
    business = []

    def log_message(self, *_args):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if self.path == "/business":
            self.business.append(body)
            response = {"ok": True, "processed": body}
        else:
            self.calls.append(body)
            response = self.reply(body)
        encoded = json.dumps(response).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def reply(self, body):
        messages = body["messages"]
        system = str(messages[0]["content"])
        previous = [
            call["function"]["name"]
            for message in messages
            for call in message.get("tool_calls", [])
        ]
        if "init phase" in system:
            if "read_file" not in previous:
                name, arguments = "read_file", {"path": "extras/api.md"}
            elif "write_file" not in previous:
                name, arguments = (
                    "write_file",
                    {
                        "path": "scripts/call_api.py",
                        "content": SCRIPT,
                    },
                )
            elif "run_shell" not in previous:
                name, arguments = (
                    "run_shell",
                    {"command": "python -m py_compile scripts/call_api.py"},
                )
            else:
                name, arguments = (
                    "GenerateStructuredOutput",
                    {
                        "success": True,
                        "summary": "environment ready",
                    },
                )
        else:
            user = next(
                message for message in messages if message.get("name") == "agentqueue"
            )
            content = user["content"]
            if isinstance(content, list):
                content = "\n".join(block.get("text", "") for block in content)
            payload = re.search(r"(\{.*\})", content, re.S).group(1)
            if "run_shell" not in previous:
                name, arguments = (
                    "run_shell",
                    {"command": "python scripts/call_api.py " + shlex.quote(payload)},
                )
            else:
                name, arguments = (
                    "GenerateStructuredOutput",
                    {
                        "success": True,
                        "summary": "order processed",
                    },
                )
        return {
            "id": "chat-test",
            "object": "chat.completion",
            "created": 1,
            "model": "test-model",
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": None,
                        "tool_calls": [
                            {
                                "id": f"tool-{len(previous)}",
                                "type": "function",
                                "function": {
                                    "name": name,
                                    "arguments": json.dumps(arguments),
                                },
                            }
                        ],
                    },
                    "finish_reason": "tool_calls",
                }
            ],
            "usage": {"prompt_tokens": 50, "completion_tokens": 10, "total_tokens": 60},
        }


class EndToEndTests(unittest.TestCase):
    def test_init_then_message_and_restart_skips_init(self):
        OpenAIHandler.calls = []
        OpenAIHandler.business = []
        server = ThreadingHTTPServer(("127.0.0.1", 0), OpenAIHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        endpoint = f"http://127.0.0.1:{server.server_port}"

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = home(root)
            source = root / "agent-skill"
            (source / "extras").mkdir(parents=True)
            (source / "SKILL.md").write_text(
                "Read USAGE.md and run the prepared API script with the message body.\n"
            )
            (source / "INIT.md").write_text(
                "Read extras/api.md, write scripts/call_api.py, and syntax-check it.\n"
            )
            (source / "extras" / "api.md").write_text(
                "POST JSON to AUTOMATION_API_URL.\n"
            )
            skill = import_skill(paths, source / "SKILL.md", "agent-skill")
            add_route(paths, "order.create", skill)
            raw = read_raw(paths.config)
            raw["defaults"]["timeout_seconds"] = 10
            raw["defaults"]["max_attempts"] = 1
            write_raw(paths.config, raw)
            paths.env.write_text(
                "AGENT_MODEL=test-model\n"
                "AGENT_API_KEY=fake-local-key\n"
                f"AGENT_BASE_URL={endpoint}/v1\n"
                f"AUTOMATION_API_URL={endpoint}/business\n"
            )

            with patch.dict(os.environ, {"AUTOMATION_API_URL": endpoint + "/business"}):
                daemon = Daemon(paths)
                daemon.start()
                try:
                    wait_for(
                        lambda: daemon.status()["routes"][0]["state"] == "ready", 10
                    )
                    host, port = daemon.amqp_address
                    publish(
                        host,
                        port,
                        "order.create",
                        b'{"order_id": 42}',
                        message_id="order-42",
                    )
                    result = wait_for(
                        lambda: self.take(daemon, "order.create.results"), 10
                    )
                    self.assertEqual(result["status"], "succeeded")
                    self.assertEqual(result["application_message_id"], "order-42")
                    self.assertEqual(OpenAIHandler.business, [{"order_id": 42}])
                    generation = daemon.status()["routes"][0]["workspace"]
                    self.assertTrue(
                        (
                            paths.workspaces
                            / "agent-skill"
                            / generation
                            / "scripts"
                            / "call_api.py"
                        ).is_file()
                    )
                finally:
                    daemon.stop()
                init_count = sum(
                    "init phase" in str(call["messages"][0]["content"])
                    for call in OpenAIHandler.calls
                )
                restarted = Daemon(paths)
                restarted.start()
                try:
                    wait_for(
                        lambda: restarted.status()["routes"][0]["state"] == "ready", 10
                    )
                    self.assertEqual(
                        sum(
                            "init phase" in str(call["messages"][0]["content"])
                            for call in OpenAIHandler.calls
                        ),
                        init_count,
                    )
                finally:
                    restarted.stop()

    @staticmethod
    def take(daemon, queue):
        deliveries = daemon.ante.take(queue, max=1, wait_ms=100)
        if not deliveries:
            return None
        delivery = deliveries[0]
        payload = delivery.payload
        delivery.ack()
        return payload


if __name__ == "__main__":
    unittest.main()
