import base64
import json
import logging
import re
import subprocess
import tempfile

from django.conf import settings

from core.integrations.processor import Processor
from core.settings import EXTERNAL_CALL_TIMEOUT

logger = logging.getLogger(__name__)

# Read-only kubectl subcommands that do not require approval
SAFE_READONLY_COMMANDS = {
    'get', 'describe', 'logs', 'top', 'api-resources', 'api-versions',
    'explain', 'version', 'cluster-info', 'config', 'auth', 'diff'
}

# Mutating verbs that require approval
MUTATING_VERBS = {
    'apply', 'create', 'delete', 'patch', 'replace', 'scale',
    'annotate', 'label', 'taint', 'drain', 'cordon', 'uncordon', 'edit',
    'set', 'autoscale', 'expose', 'run', 'attach', 'exec', 'port-forward',
    'proxy', 'cp', 'wait'
}

# Safe pipe commands that don't require approval
SAFE_PIPE_COMMANDS = {'head', 'tail', 'grep', 'awk', 'sed', 'cut', 'sort', 'uniq', 'wc', 'less', 'more'}

# Dangerous shell operators that should be blocked
DANGEROUS_OPERATORS = [';', '&&', '||', '`', '$(', '>', '>>', '<', '&']


class KubectlApiProcessor(Processor):
    client = None

    def __init__(self, api_server=None, token=None, ssl_ca_cert=None, ssl_ca_cert_path=None):
        self.__api_server = api_server
        self.__token = token
        self.__ca_cert = None
        self.native_connection_mode = getattr(settings, 'NATIVE_KUBERNETES_API_MODE', None)
        if not self.native_connection_mode and (not api_server or not token):
            raise ValueError("Kubernetes API server and token are required for KubectlApiProcessor")
        if ssl_ca_cert_path:
            self.__ca_cert = ssl_ca_cert_path
        elif ssl_ca_cert:
            fp = tempfile.NamedTemporaryFile(delete=False)
            ca_filename = fp.name
            cert_bs = base64.urlsafe_b64decode(ssl_ca_cert.encode('utf-8'))
            fp.write(cert_bs)
            fp.close()
            self.__ca_cert = ca_filename

    @staticmethod
    def strip_kubectl_prefix(command):
        """
        Strip only the leading 'kubectl' token from a command, not all occurrences.
        This preserves subcommands like 'kubectl api-resources' correctly.

        Args:
            command: Command string potentially starting with 'kubectl'

        Returns:
            Command string with leading 'kubectl' removed
        """
        # Strip leading/trailing whitespace
        command = command.strip()
        # Remove only leading 'kubectl' followed by whitespace using regex
        command = re.sub(r'^\s*kubectl\s+', '', command)
        return command

    @staticmethod
    def check_dangerous_operators(command):
        """
        Check if command contains dangerous shell operators.

        Args:
            command: Command string to check

        Returns:
            True if dangerous operators found, False otherwise
        """
        for op in DANGEROUS_OPERATORS:
            if op in command:
                return True
        return False

    @staticmethod
    def is_safe_pipe_command(pipe_cmd):
        """
        Check if a piped command is in the safe list.

        Args:
            pipe_cmd: Piped command string (e.g., "head -n 5")

        Returns:
            True if the command is safe, False otherwise
        """
        cmd_parts = pipe_cmd.strip().split()
        if not cmd_parts:
            return False
        base_cmd = cmd_parts[0]
        return base_cmd in SAFE_PIPE_COMMANDS

    @staticmethod
    def requires_approval(command):
        """
        Determine if a kubectl command requires approval based on whether it's mutating.
        Read-only commands do not require approval.

        Args:
            command: Kubectl command string (without 'kubectl' prefix)

        Returns:
            True if approval is required, False otherwise
        """
        # Strip the command and split into parts
        cmd_parts = command.strip().split()
        if not cmd_parts:
            return False

        # Get the first verb/subcommand
        verb = cmd_parts[0].lower()

        # Check if it's a mutating verb
        if verb in MUTATING_VERBS:
            return True

        # Special handling for 'rollout' - check the subcommand
        if verb == 'rollout' and len(cmd_parts) > 1:
            rollout_action = cmd_parts[1].lower()
            # 'rollout restart' is mutating, but 'rollout status', 'rollout history' are read-only
            if rollout_action in ['restart', 'undo', 'pause', 'resume']:
                return True

        # Special handling for 'annotate' and 'label' - check for --overwrite flag
        if verb in ['annotate', 'label']:
            if '--overwrite' in command:
                return True

        # All other commands are considered read-only
        return False

    def test_connection(self):
        command = "kubectl version --output=json"
        command = self.strip_kubectl_prefix(command)
        if self.native_connection_mode:
            kubectl_command = ["kubectl"] + command.split()
        elif self.__ca_cert:
            kubectl_command = [
                                  "kubectl",
                                  f"--server={self.__api_server}",
                                  f"--token={self.__token}",
                                  f"--certificate-authority={self.__ca_cert}"
                              ] + command.split()
        else:
            kubectl_command = [
                                  "kubectl",
                                  f"--server={self.__api_server}",
                                  f"--token={self.__token}",
                                  f"--insecure-skip-tls-verify=true"
                              ] + command.split()
        try:
            process = subprocess.Popen(kubectl_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            stdout, stderr = process.communicate()
            if process.returncode == 0:
                kube_version = json.loads(stdout)
                if 'serverVersion' in kube_version:
                    return True
                elif stderr:
                    raise Exception(f"Failed to connect with kubernetes cluster. Error: {stderr}")
                else:
                    raise Exception("Failed to connect with kubernetes cluster. No server version information found in "
                                    "command: kubectl version --output=json")
            else:
                raise Exception(f"Failed to connect with kubernetes cluster. Error: {stderr}")
        except Exception as e:
            logger.error(f"Exception occurred while executing kubectl command with error: {e}")
            raise

    def execute_command(self, command, require_approval_check=True):
        """
        Execute a kubectl command with optional pipe support.

        Args:
            command: The kubectl command to execute
            require_approval_check: If True, check if command requires approval (default: True)

        Returns:
            Command output or error message

        Raises:
            ValueError: If command contains dangerous operators or requires approval
            Exception: If command execution fails
        """
        command = command.strip()

        # Check for dangerous shell operators (except pipe which we handle specially)
        cmd_without_pipes = command.split('|')[0]
        if self.check_dangerous_operators(cmd_without_pipes):
            raise ValueError(f"Command contains dangerous shell operators and cannot be executed: {command}")

        # Strip the kubectl prefix
        command = self.strip_kubectl_prefix(command)

        # Parse pipes
        if '|' in command:
            commands = [cmd.strip() for cmd in command.split('|')]
            kubectl_cmd = commands[0]
            pipe_cmds = commands[1:]

            # Validate pipe commands are safe
            for pipe_cmd in pipe_cmds:
                if not self.is_safe_pipe_command(pipe_cmd):
                    raise ValueError(f"Unsafe pipe command detected: {pipe_cmd}. Only safe commands like head, tail, grep are allowed.")
        else:
            kubectl_cmd = command
            pipe_cmds = []

        # Check if approval is required for the kubectl command
        if require_approval_check and self.requires_approval(kubectl_cmd):
            raise ValueError(f"Command requires approval: kubectl {kubectl_cmd}. This is a mutating operation.")

        # Build the kubectl command
        if self.native_connection_mode:
            kubectl_command = ["kubectl"] + kubectl_cmd.split()
        elif self.__ca_cert:
            kubectl_command = [
                                  "kubectl",
                                  f"--server={self.__api_server}",
                                  f"--token={self.__token}",
                                  f"--certificate-authority={self.__ca_cert}"
                              ] + kubectl_cmd.split()
        else:
            kubectl_command = [
                                  "kubectl",
                                  f"--server={self.__api_server}",
                                  f"--token={self.__token}",
                                  f"--insecure-skip-tls-verify=true"
                              ] + kubectl_cmd.split()

        try:
            # Execute the kubectl command
            process = subprocess.Popen(kubectl_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            stdout, stderr = process.communicate()

            # Execute pipe commands if present
            if pipe_cmds:
                for cmd in pipe_cmds:
                    process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                               stderr=subprocess.PIPE, text=True, shell=True)
                    stdout, stderr = process.communicate(input=stdout)

            if process.returncode == 0:
                print("Command Output:", stdout)
                return stdout
            else:
                raise Exception(f"kubectl command failed (exit code {process.returncode}): {stderr}")
        except Exception as e:
            logger.error(f"Exception occurred while executing kubectl command with error: {e}")
            raise

    def execute_non_kubectl_command(self, command_args):
        """
        Execute a non-kubectl command directly using subprocess.
        This is useful for tools that need to run in the same environment as kubectl
        but are not kubectl commands themselves (e.g., otterize network-mapper).
        
        Args:
            command_args: List of command arguments (e.g., ['otterize', 'network-mapper', 'export', '--format', 'json'])
            
        Returns:
            Command output as string if successful, None if failed
            
        Raises:
            Exception: If command execution fails
        """
        try:
            # Only allow this in native connection mode for security
            if not self.native_connection_mode:
                logger.warning("Non-kubectl commands are only supported in native connection mode")
                return None
                
            logger.info(f"Executing non-kubectl command: {' '.join(command_args)}")
            
            process = subprocess.Popen(
                command_args,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            stdout, stderr = process.communicate()

            if process.returncode == 0:
                logger.debug(f"Command executed successfully: {' '.join(command_args)}")
                return stdout
            else:
                logger.error(f"Command failed with return code {process.returncode}: {' '.join(command_args)}")
                if stderr:
                    logger.error(f"Command stderr: {stderr}")
                return None
                
        except FileNotFoundError:
            logger.error(f"Command not found: {command_args[0]}. Make sure it's installed and in PATH.")
            return None
        except Exception as e:
            logger.error(f"Exception occurred while executing non-kubectl command {' '.join(command_args)}: {e}")
            raise e
