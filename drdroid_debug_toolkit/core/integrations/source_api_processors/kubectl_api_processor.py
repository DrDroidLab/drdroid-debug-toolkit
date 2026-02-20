import base64
import json
import logging
import subprocess
import tempfile

from django.conf import settings

from core.integrations.processor import Processor
from core.settings import EXTERNAL_CALL_TIMEOUT

logger = logging.getLogger(__name__)


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

    def test_connection(self):
        command = "kubectl version --output=json"
        if 'kubectl' in command:
            command = command.replace('kubectl', '')
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

    def execute_command(self, command):
        command = command.strip()
        if 'kubectl' in command:
            command = command.replace('kubectl', '')
        if '|' in command:
            commands = [cmd.strip() for cmd in command.split('|')]
        else:
            commands = [command]
        if self.native_connection_mode:
            kubectl_command = ["kubectl"] + commands[0].split()
        elif self.__ca_cert:
            kubectl_command = [
                                  "kubectl",
                                  f"--server={self.__api_server}",
                                  f"--token={self.__token}",
                                  f"--certificate-authority={self.__ca_cert}"
                              ] + commands[0].split()
        else:
            kubectl_command = [
                                  "kubectl",
                                  f"--server={self.__api_server}",
                                  f"--token={self.__token}",
                                  f"--insecure-skip-tls-verify=true"
                              ] + commands[0].split()
        try:
            process = subprocess.Popen(kubectl_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            stdout, stderr = process.communicate()
            if len(commands) > 1:
                for cmd in commands[1:]:
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
