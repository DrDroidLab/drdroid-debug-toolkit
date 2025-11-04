import logging

from google.protobuf.wrappers_pb2 import StringValue

from core.integrations.source_api_processors.rootly_api_processor import RootlyApiProcessor
from core.integrations.source_manager import SourceManager
from core.protos.base_pb2 import Source, TimeRange, SourceModelType, SourceKeyType
from core.protos.connectors.connector_pb2 import Connector as ConnectorProto
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResult
from core.protos.literal_pb2 import LiteralType
from core.protos.ui_definition_pb2 import FormField, FormFieldType
from core.protos.playbooks.source_task_definitions.rootly_task_pb2 import Rootly
from core.utils.credentilal_utils import generate_credentials_dict, get_connector_key_type_string, DISPLAY_NAME, CATEGORY, ALERTING

logger = logging.getLogger(__name__)


class RootlySourceManager(SourceManager):

    def __init__(self):
        self.source = Source.ROOTLY
        self.task_proto = Rootly
        self.task_type_callable_map = {
            Rootly.TaskType.SEND_TIMELINE_EVENT: {
                'task_type': 'SEND_TIMELINE_EVENT',
                'executor': self.execute_send_timeline_event,
                'model_types': [SourceModelType.ROOTLY_INCIDENT],
                'display_name': 'Send a timeline event to a Rootly incident',
                'category': 'Actions'
            }
        }

        self.connector_form_configs = [
            {
                "name": StringValue(value="Rootly API Key Connection"),
                "description": StringValue(value="Connect to Rootly using an API Key."),
                "form_fields": {
                    SourceKeyType.ROOTLY_API_KEY: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.ROOTLY_API_KEY)),
                        display_name=StringValue(value="API Key"),
                        helper_text=StringValue(value="Enter your Rootly API Key."),
                        description=StringValue(value='e.g. "1234567890abcdefghijklmnopqrstuvwxyz"'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                        is_sensitive=True
                    )
                }
            }
        ]
        self.connector_type_details = {
            DISPLAY_NAME: "ROOTLY",
            CATEGORY: ALERTING,
        }

    def get_connector_processor(self, rootly_connector, **kwargs):
        generated_credentials = generate_credentials_dict(rootly_connector.type, rootly_connector.keys)
        return RootlyApiProcessor(**generated_credentials)

    def execute_send_timeline_event(self, time_range: TimeRange, rootly_task: Rootly,
                                    rootly_connector: ConnectorProto):
        try:
            if not rootly_connector:
                raise Exception("Task execution Failed:: No Rootly source found")

            send_timeline_event_task: Rootly.SendTimelineEvent = rootly_task.send_timeline_event
            print(send_timeline_event_task)
            incident_id = send_timeline_event_task.incident_id.value
            content = send_timeline_event_task.content.value
            if not incident_id:
                raise Exception("Task execution Failed:: No Rootly incident found")

            rootly_api_processor = self.get_connector_processor(rootly_connector)
            print("Playbook Task Downstream Request: Type -> {}, Account -> {}, Incident ID -> {}".format("Rootly",
                                                                                                          rootly_connector.account_id.value,
                                                                                                          incident_id))

            return rootly_api_processor.create_timeline_event(incident_id, content)
        except Exception as e:
            logger.error(f"Error in executing send note task: {str(e)}")
            return PlaybookTaskResult(source=self.source)
