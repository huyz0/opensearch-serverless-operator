"""Exercise the plan verifier against broken task graphs."""

import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class PlanVerifierTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        shutil.copytree(ROOT / "docs" / "implementation", self.root / "docs" / "implementation")
        (self.root / "tools").mkdir()
        shutil.copy(ROOT / "tools" / "verify_plan.py", self.root / "tools")
        self.addCleanup(self.directory.cleanup)

    def run_verifier(self):
        return subprocess.run(
            [sys.executable, str(self.root / "tools" / "verify_plan.py")],
            capture_output=True,
            text=True,
            check=False,
        )

    def replace_foundation(self, old, new):
        path = self.root / "docs" / "implementation" / "foundation.md"
        content = path.read_text(encoding="utf-8")
        self.assertIn(old, content)
        path.write_text(content.replace(old, new, 1), encoding="utf-8")

    def test_existing_plan_passes(self):
        self.assertEqual(self.run_verifier().returncode, 0)

    def test_unknown_dependency_fails(self):
        self.replace_foundation("| M00.01 | — |", "| M00.01 | M99.99 |")
        result = self.run_verifier()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unknown dependency M99.99", result.stderr)

    def test_cycle_fails(self):
        self.replace_foundation("| M00.01 | — |", "| M00.01 | M00.02 |")
        result = self.run_verifier()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("dependency cycle", result.stderr)

    def test_unmapped_task_fails(self):
        coverage = self.root / "docs" / "implementation" / "coverage.md"
        content = coverage.read_text(encoding="utf-8")
        coverage.write_text(content.replace("M00.01", ""), encoding="utf-8")
        result = self.run_verifier()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("task M00.01 has no coverage mapping", result.stderr)


if __name__ == "__main__":
    unittest.main()
