from google.protobuf.wrappers_pb2 import StringValue

from core.integrations.source_api_processors.bash_processor import BashProcessor
from core.integrations.source_manager import SourceManager
from core.protos.base_pb2 import TimeRange, Source, SourceModelType, SourceKeyType
from core.protos.connectors.connector_pb2 import Connector as ConnectorProto
from core.protos.literal_pb2 import LiteralType, Literal
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResult, BashCommandOutputResult, PlaybookTaskResultType
from core.protos.playbooks.source_task_definitions.bash_task_pb2 import Bash
from core.protos.ui_definition_pb2 import FormField, FormFieldType
from core.utils.credentilal_utils import generate_credentials_dict, get_connector_key_type_string, DISPLAY_NAME, CATEGORY, CUSTOM


key_type_to_form_field_map = {
    SourceKeyType.REMOTE_SERVER_HOST: FormField(
        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.REMOTE_SERVER_HOST)),
        display_name=StringValue(value="Remote Server Host"),
        description=StringValue(value='e.g. server.example.com'),
        helper_text=StringValue(value="Enter the hostname or IP address of the remote server. You can optionally specify user as 'user@host'"),
        data_type=LiteralType.STRING,
        form_field_type=FormFieldType.TEXT_FT,
        is_optional=False
    ),
    SourceKeyType.REMOTE_SERVER_USER: FormField(
        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.REMOTE_SERVER_USER)),
        display_name=StringValue(value="Remote Server User"),
        description=StringValue(value='e.g. "admin", "ubuntu", "ec2-user"'),
        helper_text=StringValue(value="Enter the username for SSH connection (Optional if specified in host string or PEM key)"),
        data_type=LiteralType.STRING,
        form_field_type=FormFieldType.TEXT_FT,
        is_optional=True
    ),
    SourceKeyType.REMOTE_SERVER_PORT: FormField(
        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.REMOTE_SERVER_PORT)),
        display_name=StringValue(value="Remote Server Port"),
        description=StringValue(value='e.g. 22'),
        helper_text=StringValue(value="Enter the SSH port of the remote server"),
        data_type=LiteralType.STRING,
        form_field_type=FormFieldType.TEXT_FT,
        is_optional=True,
        default_value=Literal(type=LiteralType.STRING, string=StringValue(value="22"))
    ),
    SourceKeyType.REMOTE_SERVER_PEM: FormField(
        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.REMOTE_SERVER_PEM)),
        display_name=StringValue(value="Remote Server PEM Key"),
        description=StringValue(value='e.g. "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEA..."'),
        helper_text=StringValue(value="Paste the content of your PEM private key for SSH authentication"),
        data_type=LiteralType.STRING,
        form_field_type=FormFieldType.MULTILINE_FT,
        is_optional=False,
        is_sensitive=True
    ),
    SourceKeyType.REMOTE_SERVER_PASSWORD: FormField(
        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.REMOTE_SERVER_PASSWORD)),
        display_name=StringValue(value="Remote Server Password"),
        description=StringValue(value='e.g. "MySecureP@ssw0rd!"'),
        helper_text=StringValue(value="Enter the password for SSH authentication"),
        data_type=LiteralType.STRING,
        form_field_type=FormFieldType.TEXT_FT,
        is_optional=False,
        is_sensitive=True
    )
}


class BashSourceManager(SourceManager):

    def __init__(self):
        self.source = Source.BASH
        self.task_proto = Bash
        self.task_type_callable_map = {
            Bash.TaskType.COMMAND: {
                'executor': self.execute_command,
                'model_types': [SourceModelType.SSH_SERVER],
                'result_type': PlaybookTaskResultType.BASH_COMMAND_OUTPUT,
                'display_name': 'Execute a BASH Command',
                'category': 'Actions',
                'form_fields': [
                    FormField(key_name=StringValue(value="remote_server"),
                              display_name=StringValue(value="Remote Server"),
                              description=StringValue(value='Select Remote Server. If not selected, '
                                                            'command will be executed on local machine.'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="command"),
                              display_name=StringValue(value="Command"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.MULTILINE_FT),
                ]
            },
        }
        self.connector_form_configs = [
            {
                "name": StringValue(value="SSH Connection (UHKWT)"),
                "description": StringValue(value="Connect to a remote server using SSH with Username, Host, PEM Key, Password, and Port."),
                "form_fields": {
                    SourceKeyType.REMOTE_SERVER_USER: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_USER],
                    SourceKeyType.REMOTE_SERVER_HOST: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_HOST],
                    SourceKeyType.REMOTE_SERVER_PEM: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_PEM],
                    SourceKeyType.REMOTE_SERVER_PASSWORD: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_PASSWORD],
                    SourceKeyType.REMOTE_SERVER_PORT: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_PORT],
                }
            },
            {
                "name": StringValue(value="SSH Connection (HKWT)"),
                "description": StringValue(value="Connect to a remote server using SSH with Host, PEM Key, Password, and Port."),
                "form_fields": {
                    SourceKeyType.REMOTE_SERVER_HOST: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_HOST],
                    SourceKeyType.REMOTE_SERVER_PEM: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_PEM],
                    SourceKeyType.REMOTE_SERVER_PASSWORD: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_PASSWORD],
                    SourceKeyType.REMOTE_SERVER_PORT: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_PORT],
                }
            },
            {
                "name": StringValue(value="SSH Connection (UHKW)"),
                "description": StringValue(value="Connect to a remote server using SSH with Username, Host, PEM Key, and Password."),
                "form_fields": {
                    SourceKeyType.REMOTE_SERVER_USER: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_USER],
                    SourceKeyType.REMOTE_SERVER_HOST: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_HOST],
                    SourceKeyType.REMOTE_SERVER_PEM: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_PEM],
                    SourceKeyType.REMOTE_SERVER_PASSWORD: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_PASSWORD],
                }
            },
            {
                "name": StringValue(value="SSH Connection (HKW)"),
                "description": StringValue(value="Connect to a remote server using SSH with Host, PEM Key, and Password."),
                "form_fields": {
                    SourceKeyType.REMOTE_SERVER_HOST: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_HOST],
                    SourceKeyType.REMOTE_SERVER_PEM: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_PEM],
                    SourceKeyType.REMOTE_SERVER_PASSWORD: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_PASSWORD],
                }
            },
            {
                "name": StringValue(value="SSH Connection (UHKT)"),
                "description": StringValue(value="Connect to a remote server using SSH with Username, Host, PEM Key, and Port."),
                "form_fields": {
                    SourceKeyType.REMOTE_SERVER_USER: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_USER],
                    SourceKeyType.REMOTE_SERVER_HOST: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_HOST],
                    SourceKeyType.REMOTE_SERVER_PEM: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_PEM],
                    SourceKeyType.REMOTE_SERVER_PORT: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_PORT],
                }
            },
            {
                "name": StringValue(value="SSH Connection (HKT)"),
                "description": StringValue(value="Connect to a remote server using SSH with Host, PEM Key, and Port."),
                "form_fields": {
                    SourceKeyType.REMOTE_SERVER_HOST: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_HOST],
                    SourceKeyType.REMOTE_SERVER_PEM: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_PEM],
                    SourceKeyType.REMOTE_SERVER_PORT: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_PORT],
                }
            },
            {
                "name": StringValue(value="SSH Connection (UHK)"),
                "description": StringValue(value="Connect to a remote server using SSH with Username, Host, and PEM Key."),
                "form_fields": {
                    SourceKeyType.REMOTE_SERVER_USER: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_USER],
                    SourceKeyType.REMOTE_SERVER_HOST: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_HOST],
                    SourceKeyType.REMOTE_SERVER_PEM: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_PEM],
                }
            },
            {
                "name": StringValue(value="SSH Connection (HK)"),
                "description": StringValue(value="Connect to a remote server using SSH with Host and PEM Key."),
                "form_fields": {
                    SourceKeyType.REMOTE_SERVER_HOST: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_HOST],
                    SourceKeyType.REMOTE_SERVER_PEM: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_PEM],
                }
            },
            {
                "name": StringValue(value="SSH Connection (UHWT)"),
                "description": StringValue(value="Connect to a remote server using SSH with Username, Host, Password, and Port."),
                "form_fields": {
                    SourceKeyType.REMOTE_SERVER_USER: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_USER],
                    SourceKeyType.REMOTE_SERVER_HOST: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_HOST],
                    SourceKeyType.REMOTE_SERVER_PASSWORD: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_PASSWORD],
                    SourceKeyType.REMOTE_SERVER_PORT: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_PORT],
                }
            },
            {
                "name": StringValue(value="SSH Connection (HWT)"),
                "description": StringValue(value="Connect to a remote server using SSH with Host, Password, and Port."),
                "form_fields": {
                    SourceKeyType.REMOTE_SERVER_HOST: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_HOST],
                    SourceKeyType.REMOTE_SERVER_PASSWORD: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_PASSWORD],
                    SourceKeyType.REMOTE_SERVER_PORT: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_PORT],
                }
            },
            {
                "name": StringValue(value="SSH Connection (UHW)"),
                "description": StringValue(value="Connect to a remote server using SSH with Username, Host, and Password."),
                "form_fields": {
                    SourceKeyType.REMOTE_SERVER_USER: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_USER],
                    SourceKeyType.REMOTE_SERVER_HOST: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_HOST],
                    SourceKeyType.REMOTE_SERVER_PASSWORD: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_PASSWORD],
                }
            },
            {
                "name": StringValue(value="SSH Connection (HW)"),
                "description": StringValue(value="Connect to a remote server using SSH with Host and Password."),
                "form_fields": {
                    SourceKeyType.REMOTE_SERVER_HOST: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_HOST],
                    SourceKeyType.REMOTE_SERVER_PASSWORD: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_PASSWORD],
                }
            },
            {
                "name": StringValue(value="SSH Connection (UHT)"),
                "description": StringValue(value="Connect to a remote server using SSH with Username, Host, and Port."),
                "form_fields": {
                    SourceKeyType.REMOTE_SERVER_USER: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_USER],
                    SourceKeyType.REMOTE_SERVER_HOST: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_HOST],
                    SourceKeyType.REMOTE_SERVER_PORT: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_PORT],
                }
            },
            {
                "name": StringValue(value="SSH Connection (HT)"),
                "description": StringValue(value="Connect to a remote server using SSH with Host and Port."),
                "form_fields": {
                    SourceKeyType.REMOTE_SERVER_HOST: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_HOST],
                    SourceKeyType.REMOTE_SERVER_PORT: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_PORT],
                }
            },
            {
                "name": StringValue(value="SSH Connection (UH)"),
                "description": StringValue(value="Connect to a remote server using SSH with Username and Host."),
                "form_fields": {
                    SourceKeyType.REMOTE_SERVER_USER: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_USER],
                    SourceKeyType.REMOTE_SERVER_HOST: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_HOST],
                }
            },
            {
                "name": StringValue(value="SSH Connection (H)"),
                "description": StringValue(value="Connect to a remote server using SSH with Host."),
                "form_fields": {
                    SourceKeyType.REMOTE_SERVER_HOST: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_HOST],
                }
            },
            {
                "name": StringValue(value="SSH Connection (T)"),
                "description": StringValue(value="Connect to a remote server using SSH with Port."),
                "form_fields": {
                    SourceKeyType.REMOTE_SERVER_PORT: key_type_to_form_field_map[SourceKeyType.REMOTE_SERVER_PORT],
                }
            },
            {
                "name": StringValue(value="Local Execution"),
                "description": StringValue(value="Execute BASH command on the local machine where the agent is running."),
                "form_fields": {}
            },
        ]
        self.connector_type_details = {
            DISPLAY_NAME: "BASH",
            CATEGORY: CUSTOM,
        }

    def get_connector_processor(self, bash_connector, **kwargs):
        generated_credentials = {}
        if bash_connector:
            generated_credentials = generate_credentials_dict(bash_connector.type, bash_connector.keys)
        if kwargs.get('remote_server_str', None):
            remote_server_str = kwargs.get('remote_server_str')
            generated_remote_server_str = generated_credentials['remote_host']
            if '@' in generated_remote_server_str:
                if '@'  not in remote_server_str:
                    remote_server_str = f"{generated_remote_server_str.split('@')[0]}@{remote_server_str}"
            generated_credentials['remote_host'] = remote_server_str
        return BashProcessor(**generated_credentials)

    def execute_command(self, time_range: TimeRange, bash_task: Bash,
                        remote_server_connector: ConnectorProto):
        try:
            bash_command: Bash.Command = bash_task.command
            remote_server_str = bash_command.remote_server.value if bash_command.remote_server else None
            if remote_server_str and not remote_server_connector:
                ssh_server_asset = get_connector_metadata_models(model_type=SourceModelType.SSH_SERVER,
                                                                 model_uid=remote_server_str)
                if not ssh_server_asset:
                    raise Exception("No remote servers assets found")
                ssh_server_asset = ssh_server_asset.first()

                db_remote_server_connector = ssh_server_asset.connector
                remote_server_connector = db_remote_server_connector.unmasked_proto

            command = bash_command.command.value
            try:
                outputs = {}
                ssh_client = self.get_connector_processor(remote_server_connector, remote_server_str=remote_server_str)
                output = ssh_client.execute_command(command)
                outputs[command] = output
                command_output_protos = []
                bash_command_result = BashCommandOutputResult.CommandOutput(command=StringValue(value=command),
                                                                            output=StringValue(value=output))
                command_output_protos.append(bash_command_result)
                return PlaybookTaskResult(source=self.source, type=PlaybookTaskResultType.BASH_COMMAND_OUTPUT,
                                          bash_command_output=BashCommandOutputResult(
                                              command_outputs=command_output_protos))
            except Exception as e:
                raise Exception(f"Error while executing Bash task: {e}")
        except Exception as e:
            raise Exception(f"Error while executing Bash task: {e}")
