import logging
from google.protobuf.wrappers_pb2 import StringValue

from core.integrations.source_api_processors.pd_api_processor import PdApiProcessor
from core.integrations.source_manager import SourceManager
from core.protos.base_pb2 import Source, TimeRange, SourceModelType, SourceKeyType
from core.protos.connectors.connector_pb2 import Connector as ConnectorProto
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResult
from core.protos.ui_definition_pb2 import FormField, FormFieldType
from core.protos.literal_pb2 import LiteralType
from core.protos.playbooks.source_task_definitions.pager_duty_task_pb2 import PagerDuty
from core.utils.credentilal_utils import generate_credentials_dict, get_connector_key_type_string, DISPLAY_NAME, CATEGORY, ALERTING

logger = logging.getLogger(__name__)


class PagerDutySourceManager(SourceManager):

    def __init__(self):
        self.source = Source.PAGER_DUTY
        self.task_proto = PagerDuty
        self.task_type_callable_map = {
            PagerDuty.TaskType.SEND_NOTE: {
                'task_type': 'SEND_NOTE',
                'executor': self.execute_send_note,
                'model_types': [SourceModelType.PAGERDUTY_INCIDENT],
                'display_name': 'Send a note to a PagerDuty incident',
                'category': 'Actions',
                'form_fields': [
                    FormField(key_name=StringValue(value="incident_id"),
                              display_name=StringValue(value="Incident ID"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="note"),
                              display_name=StringValue(value="Note Content"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.MULTILINE_FT),
                ]
            }
        }
        self.connector_form_configs = [
            {
                "name": StringValue(value="PagerDuty API Key Connection"),
                "description": StringValue(value="Connect to PagerDuty using an API Key and optionally a configured email."),
                "form_fields": {
                    SourceKeyType.PAGER_DUTY_API_KEY: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.PAGER_DUTY_API_KEY)),
                        display_name=StringValue(value="API Key"),
                        helper_text=StringValue(value="Enter your PagerDuty API Key."),
                        description=StringValue(value='e.g. "1234567890abcdefghijklmnopqrstuvwxyz"'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                        is_sensitive=True
                    ),
                    SourceKeyType.PAGER_DUTY_CONFIGURED_EMAIL: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.PAGER_DUTY_CONFIGURED_EMAIL)),
                        display_name=StringValue(value="Configured Email"),
                        helper_text=StringValue(value="(Optional) Enter the email address associated with the PagerDuty user/API key."),
                        description=StringValue(value='e.g. "user@example.com"'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True
                    )
                }
            }
        ]
        self.connector_type_details = {
            DISPLAY_NAME: "PAGERDUTY",
            CATEGORY: ALERTING,
        }

    def get_connector_processor(self, pagerduty_connector, **kwargs):
        generated_credentials = generate_credentials_dict(pagerduty_connector.type, pagerduty_connector.keys)
        return PdApiProcessor(**generated_credentials)

    def execute_send_note(self, time_range: TimeRange, pd_task: PagerDuty,
                          pagerduty_connector: ConnectorProto):
        try:
            if not pagerduty_connector:
                raise Exception("Task execution Failed:: No PagerDuty source found")

            send_note_task: PagerDuty.SendNote = pd_task.send_note
            print(send_note_task)
            incident_id = send_note_task.incident_id.value
            note = send_note_task.note.value
            if not incident_id:
                raise Exception("Task execution Failed:: No PagerDuty incident found")

            pd_api_processor = self.get_connector_processor(pagerduty_connector)
            print("Playbook Task Downstream Request: Type -> {}, Account -> {}, Incident ID -> {}".format("PagerDuty",
                                                                                                          pagerduty_connector.account_id.value,
                                                                                                          incident_id))

            return pd_api_processor.create_note(incident_id, note)
        except Exception as e:
            logger.error(f"Error in executing send note task: {str(e)}")
            return PlaybookTaskResult(source=self.source)
