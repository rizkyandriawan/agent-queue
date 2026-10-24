import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from agent_queue.cli import main
from agent_queue.config import read_raw


class CliTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.home = self.root / "home"
        self.skill = self.root / "skill"
        (self.skill / "extras").mkdir(parents=True)
        (self.skill / "SKILL.md").write_text("# Skill\nDo work.\n")
        (self.skill / "INIT.md").write_text("# Setup\nPrepare.\n")
        (self.skill / "extras" / "api.md").write_text("docs\n")

    def tearDown(self):
        self.directory.cleanup()

    def run_cli(self, *arguments):
        output = io.StringIO()
        errors = io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
            code = main(["--home", str(self.home), *arguments])
        return code, output.getvalue(), errors.getvalue()

    def test_help_and_version(self):
        code, output, _ = self.run_cli("--help")
        self.assertEqual(code, 0)
        for command in ("add", "serve", "status", "service", "jobs", "send"):
            self.assertIn(command, output)
        code, output, _ = self.run_cli("--version")
        self.assertEqual(code, 0)
        self.assertIn("agentqueue", output)
        code, output, _ = self.run_cli("add", "--help")
        self.assertEqual(code, 0)
        self.assertIn("--concurrency", output)

    def test_init_add_list_show_update_remove(self):
        code, output, _ = self.run_cli("init")
        self.assertEqual(code, 0)
        self.assertTrue((self.home / "config.yaml").is_file())
        self.assertEqual((self.home / ".env").stat().st_mode & 0o077, 0)

        # The documented `-add` spelling is an alias.
        code, output, errors = self.run_cli(
            "-add", f"--file={self.skill / 'SKILL.md'}", "--topic=ticket.create"
        )
        self.assertEqual(code, 0, errors)
        routes = read_raw(self.home / "config.yaml")["routes"]
        self.assertIn("ticket.create", routes)
        self.assertEqual(routes["ticket.create"]["concurrency"], 1)

        code, output, _ = self.run_cli("list")
        self.assertEqual(code, 0)
        self.assertIn("ticket.create", output)

        code, output, _ = self.run_cli("show", "ticket.create", "--json")
        self.assertEqual(code, 0)
        data = json.loads(output)["data"]
        self.assertEqual(data["route"]["skill"], "skill")

        (self.skill / "SKILL.md").write_text("# Skill v2\n")
        code, _, errors = self.run_cli(
            "update", f"--file={self.skill / 'SKILL.md'}", "--topic=ticket.create"
        )
        self.assertEqual(code, 0, errors)
        self.assertNotEqual(
            read_raw(self.home / "config.yaml")["routes"]["ticket.create"]["revision"],
            routes["ticket.create"]["revision"],
        )

        code, _, _ = self.run_cli("remove", "--topic=ticket.create")
        self.assertEqual(code, 0)
        self.assertEqual(read_raw(self.home / "config.yaml")["routes"], {})

    def test_add_conflict_and_update_missing(self):
        self.run_cli("init")
        self.run_cli("-add", f"--file={self.skill / 'SKILL.md'}", "--topic=x")
        code, _, errors = self.run_cli(
            "-add", f"--file={self.skill / 'SKILL.md'}", "--topic=x"
        )
        self.assertEqual(code, 65)
        self.assertIn("already exists", errors)
        code, _, errors = self.run_cli(
            "update", f"--file={self.skill / 'SKILL.md'}", "--topic=missing"
        )
        self.assertEqual(code, 65)

    def test_status_offline_config_and_backup(self):
        self.run_cli("init")
        code, output, _ = self.run_cli("status")
        self.assertEqual(code, 0)
        self.assertIn("offline", output)
        code, output, _ = self.run_cli("config", "validate", "--json")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output)["data"]["valid"], True)
        output_path = self.root / "backup.tar.gz"
        code, _, errors = self.run_cli("backup", "--output", str(output_path))
        self.assertEqual(code, 0, errors)
        self.assertTrue(output_path.is_file())

    def test_remove_requires_confirmation_for_data(self):
        self.run_cli("init")
        self.run_cli("-add", f"--file={self.skill / 'SKILL.md'}", "--topic=x")
        code, _, errors = self.run_cli("remove", "--topic=x", "--delete-data")
        self.assertEqual(code, 64)
        self.assertIn("--yes", errors)


if __name__ == "__main__":
    unittest.main()
