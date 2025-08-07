from google.protobuf.wrappers_pb2 import StringValue

from core.integrations.source_api_processors.kubectl_api_processor import KubectlApiProcessor
from core.integrations.source_manager import SourceManager
from core.protos.base_pb2 import Source, TimeRange
from core.protos.connectors.connector_pb2 import Connector as ConnectorProto
from core.protos.literal_pb2 import LiteralType
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResult, PlaybookTaskResultType, BashCommandOutputResult
from core.protos.playbooks.source_task_definitions.kubectl_task_pb2 import Kubectl
from core.protos.ui_definition_pb2 import FormField, FormFieldType
from core.utils.credentilal_utils import generate_credentials_dict


class KubernetesSourceManager(SourceManager):

    def __init__(self):
        self.source = Source.KUBERNETES
        self.task_proto = Kubectl
        self.task_type_callable_map = {
            Kubectl.TaskType.COMMAND: {
                'executor': self.execute_command,
                'model_types': [],
                'result_type': PlaybookTaskResultType.BASH_COMMAND_OUTPUT,
                'display_name': 'Execute a Kubectl Command',
                'category': 'Actions',
                'form_fields': [
                    FormField(key_name=StringValue(value="command"),
                              display_name=StringValue(value="Kubectl Command"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.MULTILINE_FT),
                ]
            },
            Kubectl.TaskType.K8S_WRITE_COMMAND: {
                'executor': self.execute_write_command,
                'model_types': [],
                'result_type': PlaybookTaskResultType.BASH_COMMAND_OUTPUT,
                'display_name': 'Execute a Kubectl Write Command',
                'category': 'Actions',
                'form_fields': [
                    FormField(key_name=StringValue(value="command"),
                              display_name=StringValue(value="Kubectl Write Command"),
                              description=StringValue(value='e.g. "kubectl apply -f deployment.yaml", "kubectl scale deployment nginx --replicas=3", "kubectl delete pod my-pod -n default"'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.MULTILINE_FT),
                ]
            },
        }

    def get_connector_processor(self, kubernetes_connector, **kwargs):
        generated_credentials = generate_credentials_dict(kubernetes_connector.type, kubernetes_connector.keys)
        return KubectlApiProcessor(**generated_credentials)
    
    def execute_write_command(self, time_range: TimeRange, kubernetes_task: Kubectl,
                    kubernetes_connector: ConnectorProto):
        try:
            command_str = kubernetes_task.k8s_write_command.command.value
            commands = command_str.split('\n')

            try:
                outputs = {}
                kubectl_client = self.get_connector_processor(kubernetes_connector)
                for command in commands:
                    output = kubectl_client.execute_command(command)
                    outputs[command] = output
                command_output_protos = []
                for command, output in outputs.items():
                    bash_command_result = BashCommandOutputResult.CommandOutput(command=StringValue(value=command),
                                                                                output=StringValue(value=output))
                    command_output_protos.append(bash_command_result)
                return PlaybookTaskResult(
                    source=self.source,
                    type=PlaybookTaskResultType.BASH_COMMAND_OUTPUT,
                    bash_command_output=BashCommandOutputResult(
                        command_outputs=command_output_protos
                    )
                )
            except Exception as e:
                raise Exception(f"Error while executing Kubernetes task: {e}")
        except Exception as e:
            raise Exception(f"Error while executing Kubernetes task: {e}")

    def execute_command(self, time_range: TimeRange, kubernetes_task: Kubectl,
                        kubernetes_connector: ConnectorProto):
        try:
            command_str = kubernetes_task.command.command.value
            commands = command_str.split('\n')

            try:
                outputs = {}
                kubectl_client = self.get_connector_processor(kubernetes_connector)
                for command in commands:
                    command_to_execute = command
                    output = kubectl_client.execute_command(command_to_execute)
                    outputs[command] = output

                command_output_protos = []
                for command, output in outputs.items():
                    bash_command_result = BashCommandOutputResult.CommandOutput(
                        command=StringValue(value=command),
                        output=StringValue(value=output)
                    )
                    command_output_protos.append(bash_command_result)

                return PlaybookTaskResult(
                    source=self.source,
                    type=PlaybookTaskResultType.BASH_COMMAND_OUTPUT,
                    bash_command_output=BashCommandOutputResult(
                        command_outputs=command_output_protos
                    )
                )
            except Exception as e:
                raise Exception(f"Error while executing Kubernetes task: {e}")
        except Exception as e:
            raise Exception(f"Error while executing Kubernetes task: {e}")
