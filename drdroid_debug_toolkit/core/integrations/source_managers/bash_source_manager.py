from google.protobuf.wrappers_pb2 import StringValue

from core.integrations.source_api_processors.bash_processor import BashProcessor
from core.integrations.source_manager import SourceManager
from core.protos.base_pb2 import TimeRange, Source, SourceModelType
from core.protos.connectors.connector_pb2 import Connector as ConnectorProto
from core.protos.literal_pb2 import LiteralType
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResult, BashCommandOutputResult, PlaybookTaskResultType
from core.protos.playbooks.source_task_definitions.bash_task_pb2 import Bash
from core.protos.ui_definition_pb2 import FormField, FormFieldType
from core.utils.credentilal_utils import generate_credentials_dict


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

    def get_connector_processor(self, bash_connector, **kwargs):
        generated_credentials = {}
        if bash_connector:
            generated_credentials = generate_credentials_dict(bash_connector.type, bash_connector.keys)
        if kwargs.get('remote_server_str', None):
            remote_server_str = kwargs.get('remote_server_str')
            generated_remote_server_str = generated_credentials['remote_host']
            if '@' in generated_remote_server_str:
                if '@' not in remote_server_str:
                    remote_server_str = f"{generated_remote_server_str.split('@')[0]}@{remote_server_str}"
            generated_credentials['remote_host'] = remote_server_str
        return BashProcessor(**generated_credentials)

    def execute_command(self, time_range: TimeRange, bash_task: Bash,
                        remote_server_connector: ConnectorProto):
        try:
            bash_command: Bash.Command = bash_task.command
            remote_server_str = bash_command.remote_server.value if bash_command.remote_server else None
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
