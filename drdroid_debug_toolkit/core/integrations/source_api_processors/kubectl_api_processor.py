import base64
import json
import logging
import shlex
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

    @staticmethod
    def _strip_leading_kubectl(command):
        """Strip only a *leading* 'kubectl' token. The old command.replace('kubectl', '')
        removed the word anywhere in the string — e.g. inside a grep/jq pattern like
        `grep 'kubectl'` — silently corrupting the command."""
        s = command.strip()
        if s == "kubectl":
            return ""
        if s.startswith("kubectl") and s[len("kubectl")] in (" ", "\t"):
            return s[len("kubectl"):].strip()
        return s

    @staticmethod
    def _split_pipeline(command):
        """Split a shell pipeline on top-level '|' only. Pipes inside single/double quotes (or
        backslash-escaped) stay with their segment, so `grep -E 'a|b'`, `grep "x\\|y"` and
        `jq '... | select(...) | ...'` are NOT shattered. The old command.split('|') broke these
        into invalid fragments that then failed shell parsing with
        'Unterminated quoted string' / '")" unexpected'."""
        segments, buf = [], []
        quote = None
        i, n = 0, len(command)
        while i < n:
            ch = command[i]
            if quote == "'":                       # single quotes: everything literal until closing '
                buf.append(ch)
                if ch == "'":
                    quote = None
            elif quote == '"':                     # double quotes: backslash escapes next char
                buf.append(ch)
                if ch == '\\' and i + 1 < n:
                    buf.append(command[i + 1]); i += 2; continue
                if ch == '"':
                    quote = None
            else:                                  # unquoted
                if ch in ("'", '"'):
                    quote = ch; buf.append(ch)
                elif ch == '\\' and i + 1 < n:     # escaped char outside quotes (e.g. \|)
                    buf.append(ch); buf.append(command[i + 1]); i += 2; continue
                elif ch == '|':
                    segments.append(''.join(buf).strip()); buf = []
                else:
                    buf.append(ch)
            i += 1
        segments.append(''.join(buf).strip())
        return [s for s in segments if s]

    def execute_command(self, command):
        command = self._strip_leading_kubectl(command.strip())
        segments = self._split_pipeline(command)
        if not segments:
            raise Exception("kubectl command failed: empty command")

        # Segment 0 is the kubectl invocation. Parse it quote-aware (shlex) so quoted args stay
        # intact; the old whitespace split also broke args like -o jsonpath='{...}'.
        try:
            first_args = shlex.split(segments[0])
        except ValueError as e:
            raise Exception(f"kubectl command failed: could not parse '{segments[0]}': {e}")

        # Support a trailing '2>&1' (agents add it so a downstream grep can see kubectl's error
        # output). Map it to Popen stderr=STDOUT instead of passing it to kubectl as a bogus arg.
        merge_stderr = '2>&1' in first_args
        first_args = [a for a in first_args if a != '2>&1']

        if self.native_connection_mode:
            kubectl_command = ["kubectl"] + first_args
        elif self.__ca_cert:
            kubectl_command = [
                                  "kubectl",
                                  f"--server={self.__api_server}",
                                  f"--token={self.__token}",
                                  f"--certificate-authority={self.__ca_cert}"
                              ] + first_args
        else:
            kubectl_command = [
                                  "kubectl",
                                  f"--server={self.__api_server}",
                                  f"--token={self.__token}",
                                  f"--insecure-skip-tls-verify=true"
                              ] + first_args
        try:
            process = subprocess.Popen(
                kubectl_command, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT if merge_stderr else subprocess.PIPE, text=True)
            stdout, stderr = process.communicate()
            # Remember kubectl's OWN outcome before the pipe loop overwrites `process`/`stderr`.
            # A real cluster/server error (NotFound, Forbidden, unknown flag) surfaces here; a
            # failing kubectl feeds empty input to a downstream grep, which then exits 1 and used
            # to look like the failure — masking the actual kubectl error.
            kubectl_returncode, kubectl_stderr = process.returncode, stderr
            # Pipe kubectl's output through the remaining shell segments (grep/jq/awk/...).
            for cmd in segments[1:]:
                process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE, text=True, shell=True)
                stdout, stderr = process.communicate(input=stdout)
            # Surface a genuine kubectl failure regardless of what a downstream filter returned.
            if kubectl_returncode not in (0, None) and not merge_stderr:
                raise Exception(f"kubectl command failed (exit code {kubectl_returncode}): {kubectl_stderr}")
            if process.returncode == 0:
                print("Command Output:", stdout)
                return stdout
            # A downstream filter (grep/jq/awk) exiting 1 with NO stderr means "no lines matched" —
            # a valid empty result, not a failure. Agents pipe `... | grep <x>` constantly, and a
            # no-match (grep exit 1) was being reported as an error (~420 false failures on account
            # 39992 alone). A real filter error (grep exit 2, jq parse error) writes to stderr, so
            # that path still raises below.
            if len(segments) > 1 and process.returncode == 1 and not (stderr or "").strip():
                print("Command Output (no matches):", stdout)
                return stdout
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
