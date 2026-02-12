import logging

from google.protobuf.struct_pb2 import Struct
from google.protobuf.wrappers_pb2 import StringValue

from core.integrations.source_api_processors.metabase_api_processor import MetabaseApiProcessor
from core.integrations.source_manager import SourceManager
from core.protos.base_pb2 import Source, SourceKeyType, SourceModelType, TimeRange
from core.protos.connectors.connector_pb2 import Connector as ConnectorProto
from core.protos.literal_pb2 import LiteralType, Literal
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResult, PlaybookTaskResultType, ApiResponseResult, \
    TextResult
from core.protos.playbooks.source_task_definitions.metabase_task_pb2 import Metabase
from core.protos.ui_definition_pb2 import FormField, FormFieldType
from core.utils.credentilal_utils import generate_credentials_dict, get_connector_key_type_string, DISPLAY_NAME, \
    CATEGORY, ANALYTICS
from core.utils.proto_utils import dict_to_proto, proto_to_dict

logger = logging.getLogger(__name__)


class MetabaseSourceManager(SourceManager):

    def __init__(self):
        self.source = Source.METABASE
        self.task_proto = Metabase
        self.task_type_callable_map = {
            Metabase.TaskType.LIST_ALERTS: {
                'executor': self.list_alerts,
                'model_types': [SourceModelType.METABASE_SUBSCRIPTION],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'List Metabase Alerts',
                'category': 'Alerts',
                'form_fields': []
            },
            Metabase.TaskType.GET_ALERT: {
                'executor': self.get_alert,
                'model_types': [SourceModelType.METABASE_SUBSCRIPTION],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Get Metabase Alert',
                'category': 'Alerts',
                'form_fields': [
                    FormField(
                        key_name=StringValue(value="alert_id"),
                        display_name=StringValue(value="Alert ID"),
                        description=StringValue(value='e.g. 1, 42'),
                        helper_text=StringValue(value='Enter the Metabase alert ID'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT
                    ),
                ]
            },
            Metabase.TaskType.CREATE_ALERT: {
                'executor': self.create_alert,
                'model_types': [SourceModelType.METABASE_CARD],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Create Metabase Alert',
                'category': 'Alerts',
                'form_fields': [
                    FormField(
                        key_name=StringValue(value="card_id"),
                        display_name=StringValue(value="Card ID"),
                        description=StringValue(value='e.g. 1, 42'),
                        helper_text=StringValue(value='Enter the card/question ID to alert on'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT
                    ),
                    FormField(
                        key_name=StringValue(value="alert_condition"),
                        display_name=StringValue(value="Alert Condition"),
                        description=StringValue(value='When to trigger the alert'),
                        helper_text=StringValue(value='Select alert condition'),
                        data_type=LiteralType.STRING,
                        valid_values=[
                            Literal(type=LiteralType.STRING, string=StringValue(value="rows")),
                            Literal(type=LiteralType.STRING, string=StringValue(value="goal")),
                        ],
                        form_field_type=FormFieldType.DROPDOWN_FT
                    ),
                    FormField(
                        key_name=StringValue(value="alert_above_goal"),
                        display_name=StringValue(value="Alert Above Goal"),
                        description=StringValue(value='For goal alerts: alert when above goal (true) or below (false)'),
                        helper_text=StringValue(value='true or false'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True
                    ),
                    FormField(
                        key_name=StringValue(value="alert_first_only"),
                        display_name=StringValue(value="Alert First Only"),
                        description=StringValue(value='Only alert on the first trigger (true/false)'),
                        helper_text=StringValue(value='true or false'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True
                    ),
                    FormField(
                        key_name=StringValue(value="channels"),
                        display_name=StringValue(value="Channels (JSON)"),
                        description=StringValue(value='JSON object with "channels" array, e.g. {"channels": [{"channel_type": "email", "enabled": true, "recipients": [{"email": "user@example.com"}]}]}'),
                        helper_text=StringValue(value='Enter channels configuration as JSON'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.MULTILINE_FT
                    ),
                ]
            },
            Metabase.TaskType.UPDATE_ALERT: {
                'executor': self.update_alert,
                'model_types': [SourceModelType.METABASE_SUBSCRIPTION],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Update Metabase Alert',
                'category': 'Alerts',
                'form_fields': [
                    FormField(
                        key_name=StringValue(value="alert_id"),
                        display_name=StringValue(value="Alert ID"),
                        description=StringValue(value='e.g. 1, 42'),
                        helper_text=StringValue(value='Enter the Metabase alert ID'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT
                    ),
                    FormField(
                        key_name=StringValue(value="updates"),
                        display_name=StringValue(value="Updates (JSON)"),
                        description=StringValue(value='JSON object with fields to update, e.g. {"alert_condition": "goal", "alert_above_goal": true}'),
                        helper_text=StringValue(value='Enter updates as JSON'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.MULTILINE_FT
                    ),
                ]
            },
            Metabase.TaskType.DELETE_ALERT: {
                'executor': self.delete_alert,
                'model_types': [SourceModelType.METABASE_SUBSCRIPTION],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Delete Metabase Alert',
                'category': 'Alerts',
                'form_fields': [
                    FormField(
                        key_name=StringValue(value="alert_id"),
                        display_name=StringValue(value="Alert ID"),
                        description=StringValue(value='e.g. 1, 42'),
                        helper_text=StringValue(value='Enter the Metabase alert ID to delete'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT
                    ),
                ]
            },
            Metabase.TaskType.LIST_PULSES: {
                'executor': self.list_pulses,
                'model_types': [SourceModelType.METABASE_SUBSCRIPTION],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'List Metabase Pulses',
                'category': 'Pulses',
                'form_fields': []
            },
            Metabase.TaskType.GET_PULSE: {
                'executor': self.get_pulse,
                'model_types': [SourceModelType.METABASE_SUBSCRIPTION],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Get Metabase Pulse',
                'category': 'Pulses',
                'form_fields': [
                    FormField(
                        key_name=StringValue(value="pulse_id"),
                        display_name=StringValue(value="Pulse ID"),
                        description=StringValue(value='e.g. 1, 42'),
                        helper_text=StringValue(value='Enter the Metabase pulse ID'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT
                    ),
                ]
            },
            Metabase.TaskType.CREATE_PULSE: {
                'executor': self.create_pulse,
                'model_types': [SourceModelType.METABASE_DASHBOARD, SourceModelType.METABASE_CARD],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Create Metabase Pulse',
                'category': 'Pulses',
                'form_fields': [
                    FormField(
                        key_name=StringValue(value="name"),
                        display_name=StringValue(value="Name"),
                        description=StringValue(value='e.g. "Weekly Sales Report"'),
                        helper_text=StringValue(value='Enter pulse name'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT
                    ),
                    FormField(
                        key_name=StringValue(value="dashboard_id"),
                        display_name=StringValue(value="Dashboard ID"),
                        description=StringValue(value='e.g. 1, 42'),
                        helper_text=StringValue(value='Enter dashboard ID (optional)'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True
                    ),
                    FormField(
                        key_name=StringValue(value="collection_id"),
                        display_name=StringValue(value="Collection ID"),
                        description=StringValue(value='e.g. 1, 42'),
                        helper_text=StringValue(value='Enter collection ID (optional)'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True
                    ),
                    FormField(
                        key_name=StringValue(value="cards"),
                        display_name=StringValue(value="Cards (JSON)"),
                        description=StringValue(value='JSON object with "cards" array, e.g. {"cards": [{"id": 1, "include_csv": false, "include_xls": false}]}'),
                        helper_text=StringValue(value='Enter cards configuration as JSON'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.MULTILINE_FT
                    ),
                    FormField(
                        key_name=StringValue(value="channels"),
                        display_name=StringValue(value="Channels (JSON)"),
                        description=StringValue(value='JSON object with "channels" array, e.g. {"channels": [{"channel_type": "email", "enabled": true, "recipients": [{"email": "user@example.com"}], "schedule_type": "daily", "schedule_hour": 8}]}'),
                        helper_text=StringValue(value='Enter channels configuration as JSON'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.MULTILINE_FT
                    ),
                    FormField(
                        key_name=StringValue(value="skip_if_empty"),
                        display_name=StringValue(value="Skip If Empty"),
                        description=StringValue(value='Skip sending if results are empty (true/false)'),
                        helper_text=StringValue(value='true or false'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True
                    ),
                ]
            },
            Metabase.TaskType.UPDATE_PULSE: {
                'executor': self.update_pulse,
                'model_types': [SourceModelType.METABASE_SUBSCRIPTION],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Update Metabase Pulse',
                'category': 'Pulses',
                'form_fields': [
                    FormField(
                        key_name=StringValue(value="pulse_id"),
                        display_name=StringValue(value="Pulse ID"),
                        description=StringValue(value='e.g. 1, 42'),
                        helper_text=StringValue(value='Enter the Metabase pulse ID'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT
                    ),
                    FormField(
                        key_name=StringValue(value="updates"),
                        display_name=StringValue(value="Updates (JSON)"),
                        description=StringValue(value='JSON object with fields to update, e.g. {"name": "New Name", "skip_if_empty": true}'),
                        helper_text=StringValue(value='Enter updates as JSON'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.MULTILINE_FT
                    ),
                ]
            },
            Metabase.TaskType.DELETE_PULSE: {
                'executor': self.delete_pulse,
                'model_types': [SourceModelType.METABASE_SUBSCRIPTION],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Delete Metabase Pulse',
                'category': 'Pulses',
                'form_fields': [
                    FormField(
                        key_name=StringValue(value="pulse_id"),
                        display_name=StringValue(value="Pulse ID"),
                        description=StringValue(value='e.g. 1, 42'),
                        helper_text=StringValue(value='Enter the Metabase pulse ID to delete'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT
                    ),
                ]
            },
        }

        self.connector_form_configs = [
            {
                "name": StringValue(value="Metabase Connection"),
                "description": StringValue(value="Connect to Metabase using your instance URL and API Key."),
                "form_fields": {
                    SourceKeyType.METABASE_URL: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.METABASE_URL)),
                        display_name=StringValue(value="Metabase URL"),
                        helper_text=StringValue(value="Enter your Metabase instance URL"),
                        description=StringValue(value='e.g. "https://your-metabase-instance.com"'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False
                    ),
                    SourceKeyType.METABASE_API_KEY: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.METABASE_API_KEY)),
                        display_name=StringValue(value="API Key"),
                        helper_text=StringValue(value="Enter your Metabase API Key"),
                        description=StringValue(value='Generate an API key from Admin Settings > Authentication > API Keys'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                        is_sensitive=True
                    ),
                }
            }
        ]
        self.connector_type_details = {
            DISPLAY_NAME: "METABASE",
            CATEGORY: ANALYTICS,
        }

    def get_connector_processor(self, metabase_connector, **kwargs):
        generated_credentials = generate_credentials_dict(metabase_connector.type, metabase_connector.keys)
        return MetabaseApiProcessor(**generated_credentials)

    def test_connector_processor(self, connector, **kwargs):
        try:
            processor = self.get_connector_processor(connector, **kwargs)
            if processor.test_connection():
                return True, "Metabase connection successful."
            return False, "Metabase connection test failed."
        except Exception as e:
            logger.error(f"Error testing Metabase connection: {e}")
            return False, str(e)

    # Alert executors

    def list_alerts(self, time_range: TimeRange, metabase_task: Metabase,
                    metabase_connector: ConnectorProto):
        try:
            if not metabase_connector:
                raise ValueError("No Metabase source found")

            processor = self.get_connector_processor(metabase_connector)
            result = processor.list_alerts()

            response_struct = dict_to_proto({'alerts': result}, Struct)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=ApiResponseResult(response_body=response_struct)
            )
        except Exception as e:
            error_msg = f"Error listing Metabase alerts: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=error_msg)),
                source=self.source
            )

    def get_alert(self, time_range: TimeRange, metabase_task: Metabase,
                  metabase_connector: ConnectorProto):
        try:
            if not metabase_connector:
                raise ValueError("No Metabase source found")

            task = metabase_task.get_alert
            alert_id = task.alert_id.value
            if not alert_id:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value="Missing required field: alert_id")),
                    source=self.source
                )

            processor = self.get_connector_processor(metabase_connector)
            result = processor.get_alert(alert_id)

            response_struct = dict_to_proto({'alert': result}, Struct)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=ApiResponseResult(response_body=response_struct)
            )
        except Exception as e:
            error_msg = f"Error getting Metabase alert: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=error_msg)),
                source=self.source
            )

    def create_alert(self, time_range: TimeRange, metabase_task: Metabase,
                     metabase_connector: ConnectorProto):
        try:
            if not metabase_connector:
                raise ValueError("No Metabase source found")

            task = metabase_task.create_alert
            card_id = task.card_id.value
            alert_condition = task.alert_condition.value

            if not card_id or not alert_condition:
                missing = []
                if not card_id: missing.append("card_id")
                if not alert_condition: missing.append("alert_condition")
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"Missing required fields: {', '.join(missing)}")),
                    source=self.source
                )

            payload = {
                "card": {"id": int(card_id)},
                "alert_condition": alert_condition,
            }

            if task.HasField('alert_above_goal'):
                payload["alert_above_goal"] = task.alert_above_goal.value
            if task.HasField('alert_first_only'):
                payload["alert_first_only"] = task.alert_first_only.value

            if task.HasField('channels'):
                channels_dict = proto_to_dict(task.channels)
                if 'channels' in channels_dict:
                    payload["channels"] = channels_dict["channels"]

            processor = self.get_connector_processor(metabase_connector)
            result = processor.create_alert(payload)

            response_struct = dict_to_proto({'alert': result, 'status': 'Created'}, Struct)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=ApiResponseResult(response_body=response_struct)
            )
        except Exception as e:
            error_msg = f"Error creating Metabase alert: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=error_msg)),
                source=self.source
            )

    def update_alert(self, time_range: TimeRange, metabase_task: Metabase,
                     metabase_connector: ConnectorProto):
        try:
            if not metabase_connector:
                raise ValueError("No Metabase source found")

            task = metabase_task.update_alert
            alert_id = task.alert_id.value
            if not alert_id:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value="Missing required field: alert_id")),
                    source=self.source
                )

            updates = proto_to_dict(task.updates) if task.HasField('updates') else {}

            processor = self.get_connector_processor(metabase_connector)
            result = processor.update_alert(alert_id, updates)

            response_struct = dict_to_proto({'alert': result, 'status': 'Updated'}, Struct)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=ApiResponseResult(response_body=response_struct)
            )
        except Exception as e:
            error_msg = f"Error updating Metabase alert: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=error_msg)),
                source=self.source
            )

    def delete_alert(self, time_range: TimeRange, metabase_task: Metabase,
                     metabase_connector: ConnectorProto):
        try:
            if not metabase_connector:
                raise ValueError("No Metabase source found")

            task = metabase_task.delete_alert
            alert_id = task.alert_id.value
            if not alert_id:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value="Missing required field: alert_id")),
                    source=self.source
                )

            processor = self.get_connector_processor(metabase_connector)
            processor.delete_alert(alert_id)

            response_struct = dict_to_proto({'alert_id': alert_id, 'status': 'Deleted'}, Struct)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=ApiResponseResult(response_body=response_struct)
            )
        except Exception as e:
            error_msg = f"Error deleting Metabase alert: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=error_msg)),
                source=self.source
            )

    # Pulse executors

    def list_pulses(self, time_range: TimeRange, metabase_task: Metabase,
                    metabase_connector: ConnectorProto):
        try:
            if not metabase_connector:
                raise ValueError("No Metabase source found")

            processor = self.get_connector_processor(metabase_connector)
            result = processor.list_pulses()

            response_struct = dict_to_proto({'pulses': result}, Struct)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=ApiResponseResult(response_body=response_struct)
            )
        except Exception as e:
            error_msg = f"Error listing Metabase pulses: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=error_msg)),
                source=self.source
            )

    def get_pulse(self, time_range: TimeRange, metabase_task: Metabase,
                  metabase_connector: ConnectorProto):
        try:
            if not metabase_connector:
                raise ValueError("No Metabase source found")

            task = metabase_task.get_pulse
            pulse_id = task.pulse_id.value
            if not pulse_id:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value="Missing required field: pulse_id")),
                    source=self.source
                )

            processor = self.get_connector_processor(metabase_connector)
            result = processor.get_pulse(pulse_id)

            response_struct = dict_to_proto({'pulse': result}, Struct)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=ApiResponseResult(response_body=response_struct)
            )
        except Exception as e:
            error_msg = f"Error getting Metabase pulse: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=error_msg)),
                source=self.source
            )

    def create_pulse(self, time_range: TimeRange, metabase_task: Metabase,
                     metabase_connector: ConnectorProto):
        try:
            if not metabase_connector:
                raise ValueError("No Metabase source found")

            task = metabase_task.create_pulse
            name = task.name.value

            if not name:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value="Missing required field: name")),
                    source=self.source
                )

            payload = {"name": name}

            if task.HasField('dashboard_id'):
                payload["dashboard_id"] = int(task.dashboard_id.value)
            if task.HasField('collection_id'):
                payload["collection_id"] = int(task.collection_id.value)
            if task.HasField('skip_if_empty'):
                payload["skip_if_empty"] = task.skip_if_empty.value

            if task.HasField('cards'):
                cards_dict = proto_to_dict(task.cards)
                if 'cards' in cards_dict:
                    payload["cards"] = cards_dict["cards"]

            if task.HasField('channels'):
                channels_dict = proto_to_dict(task.channels)
                if 'channels' in channels_dict:
                    payload["channels"] = channels_dict["channels"]

            processor = self.get_connector_processor(metabase_connector)
            result = processor.create_pulse(payload)

            response_struct = dict_to_proto({'pulse': result, 'status': 'Created'}, Struct)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=ApiResponseResult(response_body=response_struct)
            )
        except Exception as e:
            error_msg = f"Error creating Metabase pulse: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=error_msg)),
                source=self.source
            )

    def update_pulse(self, time_range: TimeRange, metabase_task: Metabase,
                     metabase_connector: ConnectorProto):
        try:
            if not metabase_connector:
                raise ValueError("No Metabase source found")

            task = metabase_task.update_pulse
            pulse_id = task.pulse_id.value
            if not pulse_id:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value="Missing required field: pulse_id")),
                    source=self.source
                )

            updates = proto_to_dict(task.updates) if task.HasField('updates') else {}

            processor = self.get_connector_processor(metabase_connector)
            result = processor.update_pulse(pulse_id, updates)

            response_struct = dict_to_proto({'pulse': result, 'status': 'Updated'}, Struct)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=ApiResponseResult(response_body=response_struct)
            )
        except Exception as e:
            error_msg = f"Error updating Metabase pulse: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=error_msg)),
                source=self.source
            )

    def delete_pulse(self, time_range: TimeRange, metabase_task: Metabase,
                     metabase_connector: ConnectorProto):
        try:
            if not metabase_connector:
                raise ValueError("No Metabase source found")

            task = metabase_task.delete_pulse
            pulse_id = task.pulse_id.value
            if not pulse_id:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value="Missing required field: pulse_id")),
                    source=self.source
                )

            processor = self.get_connector_processor(metabase_connector)
            processor.delete_pulse(pulse_id)

            response_struct = dict_to_proto({'pulse_id': pulse_id, 'status': 'Deleted'}, Struct)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=ApiResponseResult(response_body=response_struct)
            )
        except Exception as e:
            error_msg = f"Error deleting Metabase pulse: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=error_msg)),
                source=self.source
            )
