import stat
import tempfile
import unittest
from pathlib import Path

from agent_queue.errors import CliError
from agent_queue.paths import Paths
from agent_queue.registry import (
    import_skill,
    list_skills,
    load_skill,
    delete_skill,
)


class RegistryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.paths = Paths(self.root / "home")
        self.paths.ensure()
        self.source = self.root / "create-ticket"
        (self.source / "extras").mkdir(parents=True)
        (self.source / "scripts").mkdir()
        (self.source / "SKILL.md").write_text("# Skill\nDo work.\n")
        (self.source / "INIT.md").write_text("# Init\nPrepare.\n")
        (self.source / "extras" / "api.md").write_text("API\n")
        script = self.source / "scripts" / "run.sh"
        script.write_text("#!/bin/sh\ntrue\n")
        script.chmod(0o755)
        (self.source / ".git").mkdir()
        (self.source / ".git" / "config").write_text("hidden")

    def tearDown(self):
        self.directory.cleanup()

    def test_import_is_content_addressed_and_reuses_revision(self):
        first = import_skill(self.paths, self.source / "SKILL.md")
        second = import_skill(self.paths, self.source / "SKILL.md")
        self.assertEqual(first.revision, second.revision)
        self.assertEqual(first.name, "create-ticket")
        self.assertTrue((first.root / "extras" / "api.md").is_file())
        self.assertTrue((first.root / "INIT.md").is_file())
        self.assertFalse((first.root / ".git").exists())
        script = first.root / "scripts" / "run.sh"
        self.assertTrue(script.stat().st_mode & stat.S_IXUSR)
        self.assertEqual(stat.S_IMODE(script.stat().st_mode), 0o700)
        # Hash is based only on package contents.
        (self.source / "extras" / "api.md").write_text("API v2\n")
        third = import_skill(self.paths, self.source / "SKILL.md")
        self.assertNotEqual(first.revision, third.revision)
        manifest = list_skills(self.paths)[0]
        self.assertEqual(manifest["current"], third.revision)
        self.assertEqual(len(manifest["revisions"]), 2)

    def test_load_old_revision_and_standalone_file(self):
        first = import_skill(self.paths, self.source / "SKILL.md")
        (self.source / "SKILL.md").write_text("# Skill v2\n")
        second = import_skill(self.paths, self.source / "SKILL.md")
        self.assertNotEqual(first.revision, second.revision)
        self.assertEqual(
            load_skill(self.paths, first.name, first.revision).read_skill(),
            "# Skill\nDo work.\n",
        )
        standalone = self.root / "summarize.md"
        standalone.write_text("Summarize it.")
        package = import_skill(self.paths, standalone)
        self.assertEqual(package.name, "summarize")
        self.assertEqual(package.read_skill(), "Summarize it.")
        self.assertFalse(package.has_init)

    def test_rejects_traversal_names_and_symlinks(self):
        with self.assertRaises(CliError):
            import_skill(self.paths, self.source / "SKILL.md", "../bad")
        link = self.root / "linked.md"
        link.symlink_to(self.source / "SKILL.md")
        with self.assertRaises(CliError):
            import_skill(self.paths, link)
        (self.source / "extras" / "link").symlink_to(self.source / "SKILL.md")
        with self.assertRaises(CliError):
            import_skill(self.paths, self.source / "SKILL.md")

    def test_delete(self):
        package = import_skill(self.paths, self.source / "SKILL.md")
        delete_skill(self.paths, package.name, delete_workspace=True)
        self.assertEqual(list_skills(self.paths), [])
        with self.assertRaises(CliError):
            load_skill(self.paths, package.name)


if __name__ == "__main__":
    unittest.main()
