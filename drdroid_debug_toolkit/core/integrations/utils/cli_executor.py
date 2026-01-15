"""
CLI Execution Utility for subprocess-based command execution.
Provides consistent patterns for Azure CLI and other CLI tools.
"""
import json
import logging
import subprocess
from typing import List, Any, Optional, Tuple

from core.settings import EXTERNAL_CALL_TIMEOUT

logger = logging.getLogger(__name__)


class CLIExecutionError(Exception):
    """Raised when CLI command fails."""
    def __init__(self, command: List[str], return_code: int, stderr: str):
        self.command = command
        self.return_code = return_code
        self.stderr = stderr
        super().__init__(f"Command failed with code {return_code}: {stderr}")


class CLITimeoutError(Exception):
    """Raised when CLI command times out."""
    def __init__(self, command: List[str], timeout: int):
        self.command = command
        self.timeout = timeout
        super().__init__(f"Command timed out after {timeout}s")


class CLINotFoundError(Exception):
    """Raised when CLI tool is not found."""
    def __init__(self, cli_name: str):
        self.cli_name = cli_name
        super().__init__(f"CLI tool not found: {cli_name}. Ensure it is installed and in PATH.")


class CLIExecutor:
    """Base class for CLI command execution with consistent error handling."""

    def __init__(self, timeout: int = EXTERNAL_CALL_TIMEOUT):
        self.timeout = timeout

    def execute_command(
        self,
        command: List[str],
        parse_json: bool = True,
        timeout: Optional[int] = None
    ) -> Tuple[Any, str]:
        """
        Execute a CLI command and return parsed output.

        Args:
            command: List of command arguments (prevents shell injection)
            parse_json: Whether to parse output as JSON
            timeout: Optional timeout override

        Returns:
            Tuple of (parsed_output, raw_stdout)

        Raises:
            CLIExecutionError: If command fails
            CLITimeoutError: If command times out
            CLINotFoundError: If CLI tool is not installed
        """
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=timeout or self.timeout,
                check=False
            )

            if result.returncode != 0:
                raise CLIExecutionError(
                    command=command,
                    return_code=result.returncode,
                    stderr=result.stderr
                )

            stdout = result.stdout.strip()
            if parse_json and stdout:
                try:
                    return json.loads(stdout), stdout
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse JSON output: {e}")
                    return stdout, stdout

            return stdout, stdout

        except subprocess.TimeoutExpired:
            raise CLITimeoutError(command=command, timeout=timeout or self.timeout)
        except FileNotFoundError:
            raise CLINotFoundError(command[0] if command else "unknown")


class AzureCLIExecutor(CLIExecutor):
    """Azure CLI executor with authentication support."""

    def __init__(
        self,
        subscription_id: str,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        timeout: int = EXTERNAL_CALL_TIMEOUT
    ):
        super().__init__(timeout)
        self._subscription_id = subscription_id
        self._tenant_id = tenant_id
        self._client_id = client_id
        self._client_secret = client_secret
        self._authenticated = False

    def authenticate(self) -> bool:
        """Login using service principal credentials."""
        if self._authenticated:
            return True

        try:
            # Login with service principal
            login_command = [
                "az", "login",
                "--service-principal",
                "-u", self._client_id,
                "-p", self._client_secret,
                "--tenant", self._tenant_id,
                "--output", "json"
            ]
            self.execute_command(login_command)

            # Set subscription
            set_sub_command = [
                "az", "account", "set",
                "--subscription", self._subscription_id
            ]
            self.execute_command(set_sub_command, parse_json=False)

            self._authenticated = True
            logger.info(f"Azure CLI authenticated for subscription {self._subscription_id}")
            return True

        except CLIExecutionError as e:
            logger.error(f"Azure CLI authentication failed: {e.stderr}")
            raise
        except CLINotFoundError:
            logger.error("Azure CLI (az) not found. Please install Azure CLI.")
            raise

    def execute_az_command(
        self,
        args: List[str],
        parse_json: bool = True,
        timeout: Optional[int] = None
    ) -> Any:
        """Execute an az CLI command with authentication."""
        self.authenticate()
        command = ["az"] + args
        if parse_json and "--output" not in args and "-o" not in args:
            command.extend(["--output", "json"])
        result, _ = self.execute_command(command, parse_json=parse_json, timeout=timeout)
        return result