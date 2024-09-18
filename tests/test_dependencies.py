import subprocess
import unittest
from typing import Any

class TestDependencies(unittest.TestCase):
    """
    Unit test class to ensure that all installed dependencies are compatible.
    """

    def test_pip_check(self) -> None:
        """
        Test to ensure that there are no dependency conflicts in the installed packages.
        
        Uses `pip check` to verify that all installed dependencies are compatible.
        If conflicts are found, the test will fail and display the conflict details.
        """
        result: subprocess.CompletedProcess[str] = subprocess.run(
            ['pip', 'check'], capture_output=True, text=True
        )
        
        if result.returncode != 0:
            self.fail(f"Dependency conflicts found:\n{result.stdout}")

if __name__ == "__main__":
    unittest.main()
