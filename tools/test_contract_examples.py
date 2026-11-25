"""Check that malformed example YAML fails the offline renderer."""

import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class ContractRendererTests(unittest.TestCase):
    def test_malformed_fixture_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            shutil.copytree(ROOT / "config" / "contracts", root / "config" / "contracts")
            (root / "tools").mkdir()
            shutil.copy(ROOT / "tools" / "check_contract_examples.py", root / "tools")
            fixture = root / "config" / "contracts" / "fixtures" / "instance.yaml"
            fixture.write_text("apiVersion: [unclosed\n", encoding="utf-8")
            result = subprocess.run(
                [sys.executable, str(root / "tools" / "check_contract_examples.py")],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertTrue(result.stderr.strip())


if __name__ == "__main__":
    unittest.main()
