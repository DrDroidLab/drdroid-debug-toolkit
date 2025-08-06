import io
import logging
import os
import subprocess
import tempfile
import time

import paramiko

from core.integrations.processor import Processor

logger = logging.getLogger(__name__)


class BashProcessor(Processor):
    client = None

    def __init__(self, remote_host=None, remote_password=None, remote_pem=None, port=None, remote_user=None):
        self.remote_user = remote_user if remote_user else None
        if remote_host and '@' in remote_host:
            self.remote_user = remote_host.split("@")[0] if remote_host else None
            self.remote_host = remote_host.split("@")[1] if remote_host else None
        else:
            self.remote_host = remote_host if remote_host else None
        self.remote_password = remote_password if remote_password else None
        self.remote_pem = remote_pem.strip() if remote_pem else None
        self.port = port if port else 22

    def get_connection(self):
        try:
            client = paramiko.SSHClient()
            error_string = ''
            client_inputs = {}
            if self.remote_pem:
                try:
                    if self.remote_password:
                        key = paramiko.RSAKey.from_private_key(io.StringIO(self.remote_pem),
                                                               password=self.remote_password)
                    else:
                        key = paramiko.RSAKey.from_private_key(io.StringIO(self.remote_pem))
                except Exception as e:
                    error_string += f"Error with RSA key type: {str(e)}\n"
                    try:
                        if self.remote_password:
                            key = paramiko.Ed25519Key.from_private_key(io.StringIO(self.remote_pem),
                                                                       password=self.remote_password)
                        else:
                            key = paramiko.Ed25519Key.from_private_key(io.StringIO(self.remote_pem))
                    except Exception as e:
                        error_string += f"Error with Ed25519 key type: {str(e)}\n"
                        try:
                            if self.remote_password:
                                key = paramiko.ECDSAKey.from_private_key(
                                    io.StringIO(self.remote_pem), password=self.remote_password)
                            else:
                                key = paramiko.ECDSAKey.from_private_key(io.StringIO(self.remote_pem))
                        except Exception as e:
                            error_string += f"Error with ECDSA key type: {str(e)}\n"
                            try:
                                if self.remote_password:
                                    key = paramiko.DSSKey.from_private_key(
                                        io.StringIO(self.remote_pem), password=self.remote_password)
                                else:
                                    key = paramiko.DSSKey.from_private_key(io.StringIO(self.remote_pem))
                            except Exception as e:
                                error_string += f"Error with DSS key type: {str(e)}\n"
                                logger.error(f"BashProcessor.get_connection:: Exception occurred while creating remote "
                                             f"connection with all types of supported key types: {error_string}")
                                raise Exception(f"Error with all types of supported key types: {error_string}")
                client_inputs['pkey'] = key
            if self.remote_host:
                client_inputs['hostname'] = self.remote_host
            if self.remote_user:
                client_inputs['username'] = self.remote_user
            if self.remote_password:
                client_inputs['password'] = self.remote_password
            if client_inputs:
                if self.port:
                    client_inputs['port'] = self.port
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                client.connect(**client_inputs)
            else:
                client = None
            return client
        except Exception as e:
            logger.error(f"BashProcessor.get_connection:: Exception occurred while creating remote connection with "
                         f"error: {e}")
            raise e

    def test_connection(self):
        try:
            command = 'echo "Connection successful"'
            with tempfile.NamedTemporaryFile(delete=False, suffix=".sh") as script_file, \
                    tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as output_file:
                script_file.write(command.encode())  # Write script content
                script_file_path = script_file.name  # Local temp script path
                output_file_path = output_file.name  # Local temp output file path

            remote_script_path = f"/tmp/{os.path.basename(script_file_path)}"
            remote_output_path = f"/tmp/{os.path.basename(output_file_path)}"
            exec_command = f"bash {remote_script_path} > {remote_output_path} 2>&1"
            client = self.get_connection()
            if client:
                try:
                    sftp = client.open_sftp()
                    sftp.put(script_file_path, remote_script_path)  # Upload script
                    sftp.chmod(remote_script_path, 0o755)  # Make script executable
                    stdin, stdout, stderr = client.exec_command(exec_command)
                    # Wait for execution
                    time.sleep(2)
                    try:
                        with sftp.open(remote_output_path, "r") as file:
                            output = file.read().decode()
                        sftp.remove(remote_script_path)
                        sftp.remove(remote_output_path)
                        sftp.close()
                    except FileNotFoundError:
                        logger.error("BashProcessor.test_connection:: Error: Output file not found. The script may "
                                     "have failed.")
                        raise Exception("BashProcessor.test_connection:: Error: Output file not found. The script may "
                                        "have failed.")
                    if output.strip() == "Connection successful":
                        return True
                    else:
                        raise Exception("Connection failed")
                except paramiko.AuthenticationException as e:
                    logger.error(f"BashProcessor.test_connection:: Authentication error: {str(e)}")
                    raise e
                except paramiko.SSHException as e:
                    logger.error(f"BashProcessor.test_connection:: SSH connection error: {str(e)}")
                    raise e
                except Exception as e:
                    logger.error(f"BashProcessor.test_connection:: Error: {str(e)}")
                    raise e
                finally:
                    client.close()
            else:
                try:
                    os.chmod(script_file_path, 0o755)
                    with open(output_file_path, "w") as output_f:
                        subprocess.run(["bash", script_file_path], stdout=output_f, stderr=output_f, check=True)
                    with open(output_file_path, "r") as file:
                        output = file.read()
                    return output.strip() == "Connection successful"
                except subprocess.CalledProcessError as e:
                    logger.error(f"BashProcessor.test_connection:: Error executing command{command}: {e}")
                    raise e
        except Exception as e:
            logger.error(f"BashProcessor.test_connection:: Exception occurred while creating remote connection with "
                         f"error: {e}")
            raise e

    def execute_command(self, command):
        try:
            try:
                self.test_connection()
            except Exception as e:
                logger.error(f"BashProcessor.execute_command:: Exception occurred while testing connection: {str(e)}")
                raise Exception("Remote Server not reachable")
            with tempfile.NamedTemporaryFile(delete=False, suffix=".sh") as script_file, \
                    tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as output_file:
                script_file.write(command.encode())  # Write script content
                script_file_path = script_file.name  # Local temp script path
                output_file_path = output_file.name  # Local temp output file path

            remote_script_path = f"/tmp/{os.path.basename(script_file_path)}"
            remote_output_path = f"/tmp/{os.path.basename(output_file_path)}"
            exec_command = f"bash {remote_script_path} > {remote_output_path} 2>&1"
            client = self.get_connection()
            if client:
                try:
                    sftp = client.open_sftp()
                    sftp.put(script_file_path, remote_script_path)  # Upload script
                    sftp.chmod(remote_script_path, 0o755)  # Make script executable
                    stdin, stdout, stderr = client.exec_command(exec_command)
                    # Wait for execution with timeout
                    timeout = 100  # seconds
                    start_time = time.time()
                    channel = stdout.channel

                    while not channel.exit_status_ready():
                        if time.time() - start_time > timeout:
                            logger.error(
                                "BashProcessor.execute_command:: Error: Command execution timed out after 100s.")
                            raise TimeoutError(
                                "BashProcessor.execute_command:: Error: Command execution timed out after 100s.")
                        time.sleep(0.5)  # Avoid busy waiting
                    exit_status = channel.recv_exit_status()
                    # Check if the script executed successfully
                    if exit_status != 0:
                        error_output = stderr.read().decode().strip()
                        logger.error(f"BashProcessor.execute_command:: Script failed with exit status {exit_status}. "
                                     f"Error output:\n{error_output}")
                        raise Exception(
                            f"BashProcessor.execute_command:: Script failed with exit status {exit_status}. "
                            f"Error output:\n{error_output}")
                    try:
                        with sftp.open(remote_output_path, "r") as file:
                            output = file.read().decode()
                        sftp.remove(remote_script_path)
                        sftp.remove(remote_output_path)
                        sftp.close()
                    except FileNotFoundError:
                        logger.error("BashProcessor.execute_command:: Error: Output file not found. The script may "
                                     "have failed.")
                        raise Exception("BashProcessor.execute_command:: Error: Output file not found. The script may "
                                        "have failed.")
                    return output.strip()
                except paramiko.AuthenticationException as e:
                    logger.error(f"BashProcessor.execute_command:: Authentication error: {str(e)}")
                    raise e
                except paramiko.SSHException as e:
                    logger.error(f"BashProcessor.execute_command:: SSH connection error: {str(e)}")
                    raise e
                except Exception as e:
                    logger.error(f"BashProcessor.execute_command:: Error: {str(e)}")
                    raise e
                finally:
                    client.close()
                    # Cleanup local temp files
                    os.unlink(script_file_path)
                    os.unlink(output_file_path)
                    client.close()
            else:
                try:
                    os.chmod(script_file_path, 0o755)
                    with open(output_file_path, "w") as output_f:
                        subprocess.run(["bash", script_file_path], stdout=output_f, stderr=output_f, check=True)
                    with open(output_file_path, "r") as file:
                        output = file.read()
                    return output.strip()
                except subprocess.CalledProcessError as e:
                    logger.error(f"BashProcessor.execute_command:: Error executing command{command}: {e}")
                    raise e
        except Exception as e:
            logger.error(f"BashProcessor.execute_command:: Exception occurred while executing remote command with "
                         f"error: {e}")
            raise e
