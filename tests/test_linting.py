import subprocess
import unittest
from typing import NoReturn


class TestCodeStyle(unittest.TestCase):
    """
    A test case for ensuring that the code conforms to PEP8 standards
    using the flake8 tool.
    """

    def test_flake8_conformance(self) -> NoReturn:
        """
        Ensure the code is PEP8 compliant using flake8.

        This test runs the flake8 tool to check for any PEP8 style violations.
        If any issues are found, the test will fail and output the errors.
        """
        # Run the flake8 subprocess with specific configurations
        result = subprocess.run(
            ['flake8', '--max-line-length=88', '--exclude=env,.git,test,utils,venv'],
            capture_output=True,
            text=True
        )

        # If flake8 returns a non-zero exit code, it means there are style errors
        if result.returncode != 0:
            self.fail(f"Flake8 found style errors:\n{result.stdout}")


if __name__ == "__main__":
    unittest.main()
