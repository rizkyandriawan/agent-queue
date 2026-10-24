import asyncio
import base64
import tempfile
import unittest
from pathlib import Path

from agent_queue.config import default_config
from agent_queue.registry import import_skill
from agent_queue.runner import MAX_TOOL_OUTPUT, AgentScopeRunner, _drain, _terminate
from agent_queue.paths import Paths


class RunnerUnitTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.paths = Paths(self.root / "home")
        self.paths.ensure()
        source = self.root / "skill"
        source.mkdir()
        (source / "SKILL.md").write_text("# Skill\n")
        (source / "INIT.md").write_text("# Init\n")
        self.skill = import_skill(self.paths, source / "SKILL.md", "skill")
        config = default_config()
        self.runner = AgentScopeRunner(config)

    def tearDown(self):
        self.directory.cleanup()

    def test_run_message_passes_metadata_and_binary_base64(self):
        captured = {}

        def fake_run(phase, skill, workspace, artifacts, env, task, timeout, iters):
            captured.update(phase=phase, task=task, timeout=timeout)
            return "ok"

        self.runner._run = fake_run
        raw = b"\xff\x00binary"
        result = self.runner.run_message(
            self.skill,
            self.root / "ws",
            self.root / "art",
            {"A": "b"},
            raw,
            timeout=12,
            max_iterations=3,
            metadata={"message_id": 7, "correlation_id": "c"},
        )
        self.assertEqual(result, "ok")
        self.assertEqual(captured["phase"], "message")
        self.assertIn("message_id: 7", captured["task"])
        self.assertIn(base64.b64encode(raw).decode(), captured["task"])
        self.assertEqual(captured["timeout"], 12)

    def test_run_init_includes_instructions(self):
        captured = {}

        def fake_run(phase, skill, workspace, artifacts, env, task, timeout, iters):
            captured.update(phase=phase, task=task)
            return "ready"

        self.runner._run = fake_run
        self.assertEqual(
            self.runner.run_init(
                self.skill,
                self.root / "ws",
                self.root / "art",
                {},
                timeout=5,
                max_iterations=2,
            ),
            "ready",
        )
        self.assertEqual(captured["phase"], "init")
        self.assertIn("# Init", captured["task"])

    def test_drain_caps_output(self):
        async def scenario():
            reader = asyncio.StreamReader()
            reader.feed_data(b"x" * (MAX_TOOL_OUTPUT + 100))
            reader.feed_eof()
            return await _drain(reader)

        output = asyncio.run(scenario())
        self.assertIn("[output truncated]", output)
        self.assertEqual(len(output), MAX_TOOL_OUTPUT + len("\n[output truncated]"))

    def test_terminate_kills_process_group(self):
        async def scenario():
            process = await asyncio.create_subprocess_exec(
                "/bin/bash",
                "-c",
                "sleep 30",
                start_new_session=True,
            )
            await _terminate(process)
            return process.returncode

        self.assertIsNotNone(asyncio.run(scenario()))


if __name__ == "__main__":
    unittest.main()
