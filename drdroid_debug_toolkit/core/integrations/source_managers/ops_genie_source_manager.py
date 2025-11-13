from traceback import print_tb
from google.protobuf.wrappers_pb2 import StringValue, UInt64Value, BoolValue, Int64Value
from google.protobuf.struct_pb2 import Struct


from integrations.source_api_processors.ops_genie_api_processor import OpsGenieApiProcessor
from core.integrations.source_manager import SourceManager
from drdroid_debug_toolkit.core.protos.base_pb2 import Source
from drdroid_debug_toolkit.core.protos.base_pb2 import SourceKeyType
from drdroid_debug_toolkit.core.protos.literal_pb2 import LiteralType, Literal
from core.protos.playbooks.source_task_definitions.api_task_pb2 import Api
from core.protos.playbooks.source_task_definitions.opsgenie_task_pb2 import OpsGenie
from drdroid_debug_toolkit.core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResult, PlaybookTaskResultType, ApiResponseResult, TableResult
from drdroid_debug_toolkit.core.protos.base_pb2 import TimeRange
from drdroid_debug_toolkit.core.protos.ui_definition_pb2 import FormField, FormFieldType
from core.utils.credentilal_utils import DISPLAY_NAME, CATEGORY, ALERTING, get_connector_key_type_string, generate_credentials_dict

method_proto_string_mapping = {
    Api.HttpRequest.Method.GET: "GET",
    Api.HttpRequest.Method.POST: "POST",
    Api.HttpRequest.Method.PUT: "PUT",
    Api.HttpRequest.Method.PATCH: "PATCH",
    Api.HttpRequest.Method.DELETE: "DELETE",
}


class OpsGenieSourceManager(SourceManager):

    def __init__(self):
        self.source = Source.OPS_GENIE
        self.task_proto = OpsGenie
        self.task_type_callable_map = {
            OpsGenie.TaskType.CREATE_ALERT: {
                'executor': self.create_alert,
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Create Alert',
                'model_types': [],
                'category': 'Alerting',
                'form_fields': [
                    FormField(key_name=StringValue(value="message"),
                              display_name=StringValue(value="Alert Message"),
                              description=StringValue(value='Enter the alert message'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="alias"),
                              display_name=StringValue(value="Alert Alias"),
                              description=StringValue(value='Optional alias for the alert'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="description"),
                              display_name=StringValue(value="Description"),
                              description=StringValue(value='Optional description for the alert'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="priority"),
                              display_name=StringValue(value="Priority"),
                              description=StringValue(value='Alert priority (P1, P2, P3, P4, P5)'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.DROPDOWN_FT,
                              is_optional=True,
                              valid_values=[
                                  Literal(type=LiteralType.STRING, string=StringValue(value="P1")),
                                  Literal(type=LiteralType.STRING, string=StringValue(value="P2")),
                                  Literal(type=LiteralType.STRING, string=StringValue(value="P3")),
                                  Literal(type=LiteralType.STRING, string=StringValue(value="P4")),
                                  Literal(type=LiteralType.STRING, string=StringValue(value="P5"))
                              ]),
                    FormField(key_name=StringValue(value="entity"),
                              display_name=StringValue(value="Entity"),
                              description=StringValue(value='Optional entity name'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="source"),
                              display_name=StringValue(value="Source"),
                              description=StringValue(value='Optional source of the alert'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="user"),
                              display_name=StringValue(value="User"),
                              description=StringValue(value='Optional user creating the alert'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="note"),
                              display_name=StringValue(value="Note"),
                              description=StringValue(value='Optional note for the alert'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                ]
            },
            OpsGenie.TaskType.UPDATE_ALERT: {
                'executor': self.update_alert,
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Update Alert',
                'model_types': [],
                'category': 'Alerting',
                'form_fields': [
                    FormField(key_name=StringValue(value="alert_id"),
                              display_name=StringValue(value="Alert ID"),
                              description=StringValue(value='Enter the alert ID or alias'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="message"),
                              display_name=StringValue(value="New Message"),
                              description=StringValue(value='New alert message'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="description"),
                              display_name=StringValue(value="New Description"),
                              description=StringValue(value='New alert description'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="priority"),
                              display_name=StringValue(value="New Priority"),
                              description=StringValue(value='New alert priority (P1, P2, P3, P4, P5)'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.DROPDOWN_FT,
                              is_optional=True,
                              valid_values=[
                                  Literal(type=LiteralType.STRING, string=StringValue(value="P1")),
                                  Literal(type=LiteralType.STRING, string=StringValue(value="P2")),
                                  Literal(type=LiteralType.STRING, string=StringValue(value="P3")),
                                  Literal(type=LiteralType.STRING, string=StringValue(value="P4")),
                                  Literal(type=LiteralType.STRING, string=StringValue(value="P5"))
                              ])
                ]
            },
            OpsGenie.TaskType.GET_ALERT: {
                'executor': self.get_alert,
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Get Alert',
                'model_types': [],
                'category': 'Alerting',
                'form_fields': [
                    FormField(key_name=StringValue(value="alert_id"),
                              display_name=StringValue(value="Alert ID"),
                              description=StringValue(value='Enter the alert ID or alias'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                ]
            },
            OpsGenie.TaskType.LIST_ALERTS: {
                'executor': self.list_alerts,
                'result_type': PlaybookTaskResultType.TABLE,
                'display_name': 'List Alerts',
                'model_types': [],
                'category': 'Alerting',
                'form_fields': [
                    FormField(key_name=StringValue(value="query"),
                              display_name=StringValue(value="Query"),
                              description=StringValue(value='Optional query to filter alerts (e.g., "status:open")'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="limit"),
                              display_name=StringValue(value="Limit"),
                              description=StringValue(value='Maximum number of alerts to return'),
                              data_type=LiteralType.LONG,
                              default_value=Literal(type=LiteralType.LONG, long=Int64Value(value=100)),
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="offset"),
                              display_name=StringValue(value="Offset"),
                              description=StringValue(value='Number of alerts to skip'),
                              data_type=LiteralType.LONG,
                              default_value=Literal(type=LiteralType.LONG, long=Int64Value(value=0)),
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                ]
            },
            OpsGenie.TaskType.FETCH_TEAMS: {
                'executor': self.fetch_teams,
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Fetch Teams',
                'model_types': [],
                'category': 'Alerting',
                'form_fields': []
            },
            OpsGenie.TaskType.FETCH_ESCALATION_POLICIES: {
                'executor': self.fetch_escalation_policies,
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Fetch Escalation Policies',
                'model_types': [],
                'category': 'Alerting',
                'form_fields': []
            },
            OpsGenie.TaskType.CREATE_INCIDENT: {
                'executor': self.create_incident,
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Create Incident',
                'model_types': [],
                'category': 'Incident Management',
                'form_fields': [
                    FormField(key_name=StringValue(value="message"),
                              display_name=StringValue(value="Incident Message"),
                              description=StringValue(value='Enter the incident message (required)'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="description"),
                              display_name=StringValue(value="Description"),
                              description=StringValue(value='Optional description for the incident (max 15000 characters)'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="responders"),
                              display_name=StringValue(value="Responders"),
                              description=StringValue(value='Teams/users that the incident is routed to (format: "team:team-name" or "user:user-email")'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="priority"),
                              display_name=StringValue(value="Priority"),
                              description=StringValue(value='Incident priority (P1, P2, P3, P4, P5). Default is P3'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.DROPDOWN_FT,
                              is_optional=True,
                              valid_values=[
                                  Literal(type=LiteralType.STRING, string=StringValue(value="P1")),
                                  Literal(type=LiteralType.STRING, string=StringValue(value="P2")),
                                  Literal(type=LiteralType.STRING, string=StringValue(value="P3")),
                                  Literal(type=LiteralType.STRING, string=StringValue(value="P4")),
                                  Literal(type=LiteralType.STRING, string=StringValue(value="P5"))
                              ]),
                    FormField(key_name=StringValue(value="tags"),
                              display_name=StringValue(value="Tags"),
                              description=StringValue(value='Tags for the incident (comma-separated, max 20 tags)'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="note"),
                              display_name=StringValue(value="Note"),
                              description=StringValue(value='Optional note for the incident (max 25000 characters)'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="notify_stakeholders"),
                              display_name=StringValue(value="Notify Stakeholders"),
                              description=StringValue(value='Whether stakeholders are notified (default: false)'),
                              data_type=LiteralType.BOOLEAN,
                              form_field_type=FormFieldType.CHECKBOX_FT,
                              is_optional=True),
                ]
            },
            OpsGenie.TaskType.GET_INCIDENT: {
                'executor': self.get_incident,
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Get Incident',
                'model_types': [],
                'category': 'Incident Management',
                'form_fields': [
                    FormField(key_name=StringValue(value="incident_id"),
                              display_name=StringValue(value="Incident ID"),
                              description=StringValue(value='Enter the incident ID'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                ]
            },
            OpsGenie.TaskType.LIST_INCIDENTS: {
                'executor': self.list_incidents,
                'result_type': PlaybookTaskResultType.TABLE,
                'display_name': 'List Incidents',
                'model_types': [],
                'category': 'Incident Management',
                'form_fields': [
                    FormField(key_name=StringValue(value="query"),
                              display_name=StringValue(value="Query"),
                              description=StringValue(value='Optional query to filter incidents (e.g., "status:open")'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="limit"),
                              display_name=StringValue(value="Limit"),
                              description=StringValue(value='Maximum number of incidents to return'),
                              data_type=LiteralType.LONG,
                              default_value=Literal(type=LiteralType.LONG, long=Int64Value(value=100)),
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="offset"),
                              display_name=StringValue(value="Offset"),
                              description=StringValue(value='Number of incidents to skip'),
                              data_type=LiteralType.LONG,
                              default_value=Literal(type=LiteralType.LONG, long=Int64Value(value=0)),
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                ]
            },
            OpsGenie.TaskType.CLOSE_INCIDENT: {
                'executor': self.close_incident,
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Close Incident',
                'model_types': [],
                'category': 'Incident Management',
                'form_fields': [
                    FormField(key_name=StringValue(value="incident_id"),
                              display_name=StringValue(value="Incident ID"),
                              description=StringValue(value='Enter the incident ID to close'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="note"),
                              display_name=StringValue(value="Note"),
                              description=StringValue(value='Optional note for closing the incident'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                ]
            },
            OpsGenie.TaskType.GET_REQUEST_STATUS: {
                'executor': self.get_request_status,
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Get Request Status',
                'model_types': [],
                'category': 'Incident Management',
                'form_fields': [
                    FormField(key_name=StringValue(value="request_id"),
                              display_name=StringValue(value="Request ID"),
                              description=StringValue(value='Enter the request ID to check status'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                ]
            },
            OpsGenie.TaskType.UPDATE_INCIDENT: {
                'executor': self.update_incident,
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Update Incident',
                'model_types': [],
                'category': 'Incident Management',
                'form_fields': [
                    FormField(key_name=StringValue(value="incident_id"),
                              display_name=StringValue(value="Incident ID"),
                              description=StringValue(value='Enter the incident ID to update'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="message"),
                              display_name=StringValue(value="New Message"),
                              description=StringValue(value='New incident message'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="description"),
                              display_name=StringValue(value="New Description"),
                              description=StringValue(value='New incident description'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="priority"),
                              display_name=StringValue(value="New Priority"),
                              description=StringValue(value='New incident priority (P1, P2, P3, P4, P5)'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.DROPDOWN_FT,
                              is_optional=True,
                              valid_values=[
                                  Literal(type=LiteralType.STRING, string=StringValue(value="P1")),
                                  Literal(type=LiteralType.STRING, string=StringValue(value="P2")),
                                  Literal(type=LiteralType.STRING, string=StringValue(value="P3")),
                                  Literal(type=LiteralType.STRING, string=StringValue(value="P4")),
                                  Literal(type=LiteralType.STRING, string=StringValue(value="P5"))
                              ])
                ]
            },
            OpsGenie.TaskType.RESOLVE_INCIDENT: {
                'executor': self.resolve_incident,
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Resolve Incident',
                'model_types': [],
                'category': 'Incident Management',
                'form_fields': [
                    FormField(key_name=StringValue(value="incident_id"),
                              display_name=StringValue(value="Incident ID"),
                              description=StringValue(value='Enter the incident ID to resolve'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="note"),
                              display_name=StringValue(value="Note"),
                              description=StringValue(value='Optional note for resolving the incident'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                ]
            },
            OpsGenie.TaskType.REOPEN_INCIDENT: {
                'executor': self.reopen_incident,
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Reopen Incident',
                'model_types': [],
                'category': 'Incident Management',
                'form_fields': [
                    FormField(key_name=StringValue(value="incident_id"),
                              display_name=StringValue(value="Incident ID"),
                              description=StringValue(value='Enter the incident ID to reopen'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="note"),
                              display_name=StringValue(value="Note"),
                              description=StringValue(value='Optional note for reopening the incident'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                ]
            },
            OpsGenie.TaskType.ADD_INCIDENT_NOTE: {
                'executor': self.add_incident_note,
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Add Incident Note',
                'model_types': [],
                'category': 'Incident Management',
                'form_fields': [
                    FormField(key_name=StringValue(value="incident_id"),
                              display_name=StringValue(value="Incident ID"),
                              description=StringValue(value='Enter the incident ID to add a note to'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="note"),
                              display_name=StringValue(value="Note"),
                              description=StringValue(value='Note to add to the incident'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                ]
            },
            OpsGenie.TaskType.LIST_INCIDENT_NOTES: {
                'executor': self.list_incident_notes,
                'result_type': PlaybookTaskResultType.TABLE,
                'display_name': 'List Incident Notes',
                'model_types': [],
                'category': 'Incident Management',
                'form_fields': [
                    FormField(key_name=StringValue(value="incident_id"),
                              display_name=StringValue(value="Incident ID"),
                              description=StringValue(value='Enter the incident ID to list notes for'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="offset"),
                              display_name=StringValue(value="Offset"),
                              description=StringValue(value='Optional offset for pagination'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="direction"),
                              display_name=StringValue(value="Direction"),
                              description=StringValue(value='Direction for pagination (prev, next)'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.DROPDOWN_FT,
                              is_optional=True,
                              valid_values=[
                                  Literal(type=LiteralType.STRING, string=StringValue(value="prev")),
                                  Literal(type=LiteralType.STRING, string=StringValue(value="next"))
                              ]),
                    FormField(key_name=StringValue(value="limit"),
                              display_name=StringValue(value="Limit"),
                              description=StringValue(value='Maximum number of notes to return'),
                              data_type=LiteralType.LONG,
                              default_value=Literal(type=LiteralType.LONG, long=Int64Value(value=10)),
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="order"),
                              display_name=StringValue(value="Order"),
                              description=StringValue(value='Order of notes (asc, desc)'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.DROPDOWN_FT,
                              is_optional=True,
                              valid_values=[
                                  Literal(type=LiteralType.STRING, string=StringValue(value="asc")),
                                  Literal(type=LiteralType.STRING, string=StringValue(value="desc"))
                              ]),
                ]
            },
        }
        self.connector_form_configs = [
            {
                "name": StringValue(value="OpsGenie API Key Connection"),
                "description": StringValue(value="Connect to OpsGenie using an API Key."),
                "form_fields": {
                    SourceKeyType.OPS_GENIE_API_KEY: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.OPS_GENIE_API_KEY)),
                        display_name=StringValue(value= "API Key"),
                        helper_text=StringValue(value="Enter your OpsGenie API Key."),
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
            DISPLAY_NAME: "OPS GENIE",
            CATEGORY: ALERTING,
        }

    def get_connector_processor(self, ops_genie_connector, **kwargs):
        generated_credentials = generate_credentials_dict(ops_genie_connector.type, ops_genie_connector.keys)
        return OpsGenieApiProcessor(**generated_credentials)

    def _serialize_datetime_objects(self, data):
        """Convert datetime objects to strings for protobuf compatibility"""
        if not data:
            return data
            
        if isinstance(data, dict):
            serialized = {}
            for key, value in data.items():
                if hasattr(value, 'isoformat'):  # datetime object
                    serialized[key] = value.isoformat()
                elif isinstance(value, list):
                    serialized[key] = self._serialize_datetime_objects(value)
                elif isinstance(value, dict):
                    serialized[key] = self._serialize_datetime_objects(value)
                else:
                    serialized[key] = value
            return serialized
        elif isinstance(data, list):
            serialized = []
            for item in data:
                if hasattr(item, 'isoformat'):  # datetime object
                    serialized.append(item.isoformat())
                elif isinstance(item, (dict, list)):
                    serialized.append(self._serialize_datetime_objects(item))
                else:
                    serialized.append(item)
            return serialized
        else:
            return data

    def _parse_comma_separated_string(self, value):
        """Parse comma-separated string into list, handling empty values"""
        if not value:
            return []
        return [item.strip() for item in str(value).split(',') if item.strip()]

    def create_alert(self, time_range, opsgenie_task, opsgenie_connector):
        """Create a new alert in OpsGenie"""
        try:
            create_alert_task = opsgenie_task.create_alert
            
            # Extract parameters from the task
            message = create_alert_task.message.value if create_alert_task.message.value else ""
            alias = create_alert_task.alias.value if create_alert_task.alias.value else None
            description = create_alert_task.description.value if create_alert_task.description.value else None
            responders = list(create_alert_task.responders) if create_alert_task.responders else None
            visible_to = list(create_alert_task.visible_to) if create_alert_task.visible_to else None
            actions = list(create_alert_task.actions) if create_alert_task.actions else None
            tags = list(create_alert_task.tags) if create_alert_task.tags else None
            details = dict(create_alert_task.details) if create_alert_task.details else None
            entity = create_alert_task.entity.value if create_alert_task.entity.value else None
            priority = create_alert_task.priority.value if create_alert_task.priority.value else None
            source = create_alert_task.source.value if create_alert_task.source.value else None
            user = create_alert_task.user.value if create_alert_task.user.value else None
            note = create_alert_task.note.value if create_alert_task.note.value else None
            
            processor = self.get_connector_processor(opsgenie_connector)
            result = processor.create_alert(
                message=message,
                alias=alias,
                description=description,
                responders=responders,
                visible_to=visible_to,
                actions=actions,
                tags=tags,
                details=details,
                entity=entity,
                priority=priority,
                source=source,
                user=user,
                note=note
            )
            
            # Convert datetime objects to strings for protobuf compatibility
            serialized_result = self._serialize_datetime_objects(result)
            
            response_obj = {
                "message": f"Successfully created alert: {message}",
                "alert_data": serialized_result
            }
            response_body = Struct()
            response_body.update(response_obj)
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=ApiResponseResult(
                    response_body=response_body
                )
            )
        except Exception as e:
            raise Exception(f"Error while executing OpsGenie create_alert task: {e}")

    def update_alert(self, time_range, opsgenie_task, opsgenie_connector):
        """Update an existing alert in OpsGenie"""
        try:
            update_alert_task = opsgenie_task.update_alert
            
            # Extract parameters from the task
            alert_id = update_alert_task.alert_id.value if update_alert_task.alert_id.value else ""
            message = update_alert_task.message.value if update_alert_task.message.value else None
            description = update_alert_task.description.value if update_alert_task.description.value else None
            priority = update_alert_task.priority.value if update_alert_task.priority.value else None
            source = update_alert_task.source.value if update_alert_task.source.value else None
            
            processor = self.get_connector_processor(opsgenie_connector)
            result = processor.update_alert(
                alert_id=alert_id,
                message=message,
                description=description,
                priority=priority,
            )
            
            # Convert datetime objects to strings for protobuf compatibility
            serialized_result = self._serialize_datetime_objects(result)
            
            response_obj = {
                "message": f"Successfully updated alert: {alert_id}",
                "alert_data": serialized_result
            }
            response_body = Struct()
            response_body.update(response_obj)
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=ApiResponseResult(
                    response_body=response_body
                )
            )
        except Exception as e:
            raise Exception(f"Error while executing OpsGenie update_alert task: {e}")

    def get_alert(self, time_range, opsgenie_task, opsgenie_connector):
        """Get a specific alert from OpsGenie"""
        try:
            get_alert_task = opsgenie_task.get_alert
            
            alert_id = get_alert_task.alert_id.value if get_alert_task.alert_id.value else ""
            processor = self.get_connector_processor(opsgenie_connector)
            result = processor.get_alert(alert_id)
            
            # Convert datetime objects to strings for protobuf compatibility
            serialized_result = self._serialize_datetime_objects(result)
            
            response_obj = {
                "message": f"Retrieved alert: {alert_id}",
                "alert_data": serialized_result
            }
            response_body = Struct()
            response_body.update(response_obj)
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=ApiResponseResult(
                    response_body=response_body
                )
            )
        except Exception as e:
            raise Exception(f"Error while executing OpsGenie get_alert task: {e}")

    def list_alerts(self, time_range, opsgenie_task, opsgenie_connector):
        """List alerts from OpsGenie"""
        try:
            list_alerts_task = opsgenie_task.list_alerts
            
            query = list_alerts_task.query.value if list_alerts_task.query.value else ""
            limit = list_alerts_task.limit.value if list_alerts_task.limit.value else 100
            offset = list_alerts_task.offset.value if list_alerts_task.offset.value else 0
            
            processor = self.get_connector_processor(opsgenie_connector)
            alerts_data = processor.fetch_alerts(query=query, limit=limit, offset=offset)
            
            if not alerts_data:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TABLE,
                    source=self.source,
                    table=TableResult(
                        raw_query=StringValue(value="No alerts found"),
                        total_count=UInt64Value(value=0),
                        rows=[]
                    )
                )
            
            # Create table rows for each alert
            rows = []
            for alert in alerts_data:
                columns = [
                    TableResult.TableColumn(
                        name=StringValue(value='Alert ID'),
                        value=StringValue(value=alert.get('id', ''))
                    ),
                    TableResult.TableColumn(
                        name=StringValue(value='Message'),
                        value=StringValue(value=alert.get('message', ''))
                    ),
                    TableResult.TableColumn(
                        name=StringValue(value='Status'),
                        value=StringValue(value=alert.get('status', ''))
                    ),
                    TableResult.TableColumn(
                        name=StringValue(value='Priority'),
                        value=StringValue(value=alert.get('priority', ''))
                    ),
                    TableResult.TableColumn(
                        name=StringValue(value='Created At'),
                        value=StringValue(value=str(alert.get('created_at', '')))
                    ),
                    TableResult.TableColumn(
                        name=StringValue(value='Last Occurred At'),
                        value=StringValue(value=str(alert.get('last_occurred_at', '')))
                    ),
                    TableResult.TableColumn(
                        name=StringValue(value='Count'),
                        value=StringValue(value=str(alert.get('count', '')))
                    ),
                ]
                
                row = TableResult.TableRow(columns=columns)
                rows.append(row)
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TABLE,
                source=self.source,
                table=TableResult(
                    raw_query=StringValue(value=f"Found {len(rows)} alerts"),
                    total_count=UInt64Value(value=len(rows)),
                    rows=rows,
                    searchable=BoolValue(value=True)
                )
            )
        except Exception as e:
            raise Exception(f"Error while executing OpsGenie list_alerts task: {e}")

    def close_alert(self, time_range, opsgenie_task, opsgenie_connector):
        """Close an alert in OpsGenie"""
        try:
            close_alert_task = opsgenie_task.close_alert
            
            alert_id = close_alert_task.alert_id.value if close_alert_task.alert_id.value else ""
            user = close_alert_task.user.value if close_alert_task.user.value else None
            note = close_alert_task.note.value if close_alert_task.note.value else None
            source = close_alert_task.source.value if close_alert_task.source.value else None
            
            processor = self.get_connector_processor(opsgenie_connector)
            result = processor.close_alert(
                alert_id=alert_id,
                user=user,
                note=note,
                source=source
            )
            
            # Convert datetime objects to strings for protobuf compatibility
            serialized_result = self._serialize_datetime_objects(result)
            
            response_obj = {
                "message": f"Successfully closed alert: {alert_id}",
                "close_data": serialized_result
            }
            response_body = Struct()
            response_body.update(response_obj)
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=ApiResponseResult(
                    response_body=response_body
                )
            )
        except Exception as e:
            raise Exception(f"Error while executing OpsGenie close_alert task: {e}")

    def fetch_teams(self, time_range, opsgenie_task, opsgenie_connector):
        """Fetch teams from OpsGenie"""
        try:
            processor = self.get_connector_processor(opsgenie_connector)
            teams_data = processor.fetch_teams()
            
            response_obj = {
                "message": f"Successfully fetched {len(teams_data) if teams_data else 0} teams",
                "teams_data": teams_data
            }
            response_body = Struct()
            response_body.update(response_obj)
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=ApiResponseResult(
                    response_body=response_body
                )
            )
        except Exception as e:
            raise Exception(f"Error while executing OpsGenie fetch_teams task: {e}")

    def fetch_escalation_policies(self, time_range, opsgenie_task, opsgenie_connector):
        """Fetch escalation policies from OpsGenie"""
        try:
            processor = self.get_connector_processor(opsgenie_connector)
            policies_data = processor.fetch_escalation_policies()
            
            response_obj = {
                "message": f"Successfully fetched {len(policies_data) if policies_data else 0} escalation policies",
                "policies_data": policies_data
            }
            response_body = Struct()
            response_body.update(response_obj)
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=ApiResponseResult(
                    response_body=response_body
                )
            )
        except Exception as e:
            raise Exception(f"Error while executing OpsGenie fetch_escalation_policies task: {e}")

    def create_incident(self, time_range, opsgenie_task, opsgenie_connector):
        """Create a new incident in OpsGenie"""
        try:
            create_incident_task = opsgenie_task.create_incident
            
            # Extract parameters from the task
            message = create_incident_task.message.value if create_incident_task.HasField('message') and create_incident_task.message.value else ""
            description = create_incident_task.description.value if create_incident_task.HasField('description') and create_incident_task.description.value else None
            
            # Handle responders - convert comma-separated string to list if needed
            responders = create_incident_task.responders.value if create_incident_task.responders.value else None
            if responders:
                # If it's a single string with commas, split it
                responders = self._parse_comma_separated_string(responders)
            
            priority = create_incident_task.priority.value if create_incident_task.HasField('priority') and create_incident_task.priority.value else None
            
            # Handle tags - convert comma-separated string to list if needed
            tags = create_incident_task.tags.value if create_incident_task.tags.value else None
            if tags:
                # If it's a single string with commas, split it
                tags = self._parse_comma_separated_string(tags)
            
            details = dict(create_incident_task.details) if create_incident_task.details else None
            note = create_incident_task.note.value if create_incident_task.HasField('note') and create_incident_task.note.value else None
            status_page_entry = dict(create_incident_task.status_page_entry) if create_incident_task.status_page_entry else None
            notify_stakeholders = create_incident_task.notify_stakeholders.value if create_incident_task.HasField('notify_stakeholders') else None
            
            processor = self.get_connector_processor(opsgenie_connector)
            responders = [{"name": responder, "type": "team"} for responder in responders]
            result = processor.create_incident(
                message=message,
                description=description,
                responders=responders,
                priority=priority,
                tags=tags,
                details=details,
                note=note,
                status_page_entry=status_page_entry,
                notify_stakeholders=notify_stakeholders
            )
            
            # Convert datetime objects to strings for protobuf compatibility
            serialized_result = self._serialize_datetime_objects(result)
            
            response_obj = {
                "message": f"Successfully created incident: {message}",
                "incident_data": serialized_result
            }
            response_body = Struct()
            response_body.update(response_obj)
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=ApiResponseResult(
                    response_body=response_body
                )
            )
        except Exception as e:
            raise Exception(f"Error while executing OpsGenie create_incident task: {e}")

    def get_incident(self, time_range, opsgenie_task, opsgenie_connector):
        """Get a specific incident from OpsGenie"""
        try:
            get_incident_task = opsgenie_task.get_incident
            
            incident_id = get_incident_task.incident_id.value if get_incident_task.incident_id.value else ""
            processor = self.get_connector_processor(opsgenie_connector)
            result = processor.get_incident(incident_id)
            
            # Convert datetime objects to strings for protobuf compatibility
            serialized_result = self._serialize_datetime_objects(result)
            
            response_obj = {
                "message": f"Retrieved incident: {incident_id}",
                "incident_data": serialized_result
            }
            response_body = Struct()
            response_body.update(response_obj)
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=ApiResponseResult(
                    response_body=response_body
                )
            )
        except Exception as e:
            raise Exception(f"Error while executing OpsGenie get_incident task: {e}")

    def list_incidents(self, time_range, opsgenie_task, opsgenie_connector):
        """List incidents from OpsGenie"""
        try:
            list_incidents_task = opsgenie_task.list_incidents
            
            query = list_incidents_task.query.value if list_incidents_task.query.value else ""
            limit = list_incidents_task.limit.value if list_incidents_task.limit.value else 100
            offset = list_incidents_task.offset.value if list_incidents_task.offset.value else 0
            
            processor = self.get_connector_processor(opsgenie_connector)
            incidents_data = processor.list_incidents(query=query, limit=limit, offset=offset)
            
            if not incidents_data:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TABLE,
                    source=self.source,
                    table=TableResult(
                        raw_query=StringValue(value="No incidents found"),
                        total_count=UInt64Value(value=0),
                        rows=[]
                    )
                )
            
            # Create table rows for each incident
            rows = []
            for incident in incidents_data:
                columns = [
                    TableResult.TableColumn(
                        name=StringValue(value='Incident ID'),
                        value=StringValue(value=incident.get('id', ''))
                    ),
                    TableResult.TableColumn(
                        name=StringValue(value='Message'),
                        value=StringValue(value=incident.get('message', ''))
                    ),
                    TableResult.TableColumn(
                        name=StringValue(value='Status'),
                        value=StringValue(value=incident.get('status', ''))
                    ),
                    TableResult.TableColumn(
                        name=StringValue(value='Priority'),
                        value=StringValue(value=incident.get('priority', ''))
                    ),
                    TableResult.TableColumn(
                        name=StringValue(value='Service ID'),
                        value=StringValue(value=incident.get('service_id', ''))
                    ),
                    TableResult.TableColumn(
                        name=StringValue(value='Assignee'),
                        value=StringValue(value=incident.get('assignee', ''))
                    ),
                    TableResult.TableColumn(
                        name=StringValue(value='Created At'),
                        value=StringValue(value=str(incident.get('created_at', '')))
                    ),
                    TableResult.TableColumn(
                        name=StringValue(value='Updated At'),
                        value=StringValue(value=str(incident.get('updated_at', '')))
                    ),
                ]
                
                row = TableResult.TableRow(columns=columns)
                rows.append(row)
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TABLE,
                source=self.source,
                table=TableResult(
                    raw_query=StringValue(value=f"Found {len(rows)} incidents"),
                    total_count=UInt64Value(value=len(rows)),
                    rows=rows,
                    searchable=BoolValue(value=True)
                )
            )
        except Exception as e:
            raise Exception(f"Error while executing OpsGenie list_incidents task: {e}")

    def close_incident(self, time_range, opsgenie_task, opsgenie_connector):
        """Close an incident in OpsGenie"""
        try:
            close_incident_task = opsgenie_task.close_incident
            
            incident_id = close_incident_task.incident_id.value if close_incident_task.incident_id.value else ""
            note = close_incident_task.note.value if close_incident_task.note.value else None
            
            processor = self.get_connector_processor(opsgenie_connector)
            result = processor.close_incident(
                incident_id=incident_id,
                note=note
            )
            
            # Convert datetime objects to strings for protobuf compatibility
            serialized_result = self._serialize_datetime_objects(result)
            
            response_obj = {
                "message": f"Successfully closed incident: {incident_id}",
                "close_data": serialized_result
            }
            response_body = Struct()
            response_body.update(response_obj)
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=ApiResponseResult(
                    response_body=response_body
                )
            )
        except Exception as e:
            raise Exception(f"Error while executing OpsGenie close_incident task: {e}")

    def get_request_status(self, time_range, opsgenie_task, opsgenie_connector):
        """Get the status of an incident request"""
        try:
            get_request_status_task = opsgenie_task.get_request_status
            
            request_id = get_request_status_task.request_id.value if get_request_status_task.request_id.value else ""
            processor = self.get_connector_processor(opsgenie_connector)
            result = processor.get_request_status(request_id)
            
            # Convert datetime objects to strings for protobuf compatibility
            serialized_result = self._serialize_datetime_objects(result)
            
            response_obj = {
                "message": f"Retrieved request status: {request_id}",
                "request_status": serialized_result
            }
            response_body = Struct()
            response_body.update(response_obj)
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=ApiResponseResult(
                    response_body=response_body
                )
            )
        except Exception as e:
            raise Exception(f"Error while executing OpsGenie get_request_status task: {e}")

    def update_incident(self, time_range, opsgenie_task, opsgenie_connector):
        """Update an incident in OpsGenie"""
        try:
            update_incident_task = opsgenie_task.update_incident
            
            # Extract parameters from the task
            incident_id = update_incident_task.incident_id.value if update_incident_task.HasField('incident_id') and update_incident_task.incident_id.value else ""
            message = update_incident_task.message.value if update_incident_task.HasField('message') and update_incident_task.message.value else None
            description = update_incident_task.description.value if update_incident_task.HasField('description') and update_incident_task.description.value else None
            priority = update_incident_task.priority.value if update_incident_task.HasField('priority') and update_incident_task.priority.value else None
            
            processor = self.get_connector_processor(opsgenie_connector)
            result = processor.update_incident(
                incident_id=incident_id,
                message=message,
                description=description,
                priority=priority
            )
            
            # Convert datetime objects to strings for protobuf compatibility
            serialized_result = self._serialize_datetime_objects(result)
            
            # Build update summary message
            updates = []
            if message is not None:
                updates.append("message")
            if description is not None:
                updates.append("description")
            if priority is not None:
                updates.append("priority")
            
            update_summary = ", ".join(updates) if updates else "no fields"
            
            response_obj = {
                "message": f"Successfully updated incident {incident_id} ({update_summary})",
                "update_results": serialized_result
            }
            response_body = Struct()
            response_body.update(response_obj)
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=ApiResponseResult(
                    response_body=response_body
                )
            )
        except Exception as e:
            raise Exception(f"Error while executing OpsGenie update_incident task: {e}")

    def resolve_incident(self, time_range, opsgenie_task, opsgenie_connector):
        """Resolve an incident in OpsGenie"""
        try:
            resolve_incident_task = opsgenie_task.resolve_incident
            
            incident_id = resolve_incident_task.incident_id.value if resolve_incident_task.incident_id.value else ""
            note = resolve_incident_task.note.value if resolve_incident_task.note.value else None
            
            processor = self.get_connector_processor(opsgenie_connector)
            result = processor.resolve_incident(
                incident_id=incident_id,
                note=note
            )
            
            # Convert datetime objects to strings for protobuf compatibility
            serialized_result = self._serialize_datetime_objects(result)
            
            response_obj = {
                "message": f"Successfully resolved incident: {incident_id}",
                "resolve_data": serialized_result
            }
            response_body = Struct()
            response_body.update(response_obj)
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=ApiResponseResult(
                    response_body=response_body
                )
            )
        except Exception as e:
            raise Exception(f"Error while executing OpsGenie resolve_incident task: {e}")

    def reopen_incident(self, time_range, opsgenie_task, opsgenie_connector):
        """Reopen an incident in OpsGenie"""
        try:
            reopen_incident_task = opsgenie_task.reopen_incident
            
            incident_id = reopen_incident_task.incident_id.value if reopen_incident_task.incident_id.value else ""
            note = reopen_incident_task.note.value if reopen_incident_task.note.value else None
            
            processor = self.get_connector_processor(opsgenie_connector)
            result = processor.reopen_incident(
                incident_id=incident_id,
                note=note
            )
            
            # Convert datetime objects to strings for protobuf compatibility
            serialized_result = self._serialize_datetime_objects(result)
            
            response_obj = {
                "message": f"Successfully reopened incident: {incident_id}",
                "reopen_data": serialized_result
            }
            response_body = Struct()
            response_body.update(response_obj)
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=ApiResponseResult(
                    response_body=response_body
                )
            )
        except Exception as e:
            raise Exception(f"Error while executing OpsGenie reopen_incident task: {e}")

    def add_incident_note(self, time_range, opsgenie_task, opsgenie_connector):
        """Add a note to an incident in OpsGenie"""
        try:
            add_incident_note_task = opsgenie_task.add_incident_note
            
            incident_id = add_incident_note_task.incident_id.value if add_incident_note_task.incident_id.value else ""
            note = add_incident_note_task.note.value if add_incident_note_task.note.value else ""
            
            processor = self.get_connector_processor(opsgenie_connector)
            result = processor.add_incident_note(
                incident_id=incident_id,
                note=note
            )
            
            # Convert datetime objects to strings for protobuf compatibility
            serialized_result = self._serialize_datetime_objects(result)
            
            response_obj = {
                "message": f"Successfully added note to incident: {incident_id}",
                "note_data": serialized_result
            }
            response_body = Struct()
            response_body.update(response_obj)
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=ApiResponseResult(
                    response_body=response_body
                )
            )
        except Exception as e:
            raise Exception(f"Error while executing OpsGenie add_incident_note task: {e}")

    def list_incident_notes(self, time_range, opsgenie_task, opsgenie_connector):
        """List notes for an incident in OpsGenie"""
        try:
            list_incident_notes_task = opsgenie_task.list_incident_notes
            
            incident_id = list_incident_notes_task.incident_id.value if list_incident_notes_task.incident_id.value else ""
            offset = list_incident_notes_task.offset.value if list_incident_notes_task.offset.value else None
            direction = list_incident_notes_task.direction.value if list_incident_notes_task.direction.value else None
            limit = list_incident_notes_task.limit.value if list_incident_notes_task.limit.value else 10
            order = list_incident_notes_task.order.value if list_incident_notes_task.order.value else None
            
            processor = self.get_connector_processor(opsgenie_connector)
            notes_data = processor.list_incident_notes(
                incident_id=incident_id,
                offset=offset,
                direction=direction,
                limit=limit,
                order=order
            )
            
            if not notes_data:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TABLE,
                    source=self.source,
                    table=TableResult(
                        raw_query=StringValue(value="No notes found for incident"),
                        total_count=UInt64Value(value=0),
                        rows=[]
                    )
                )
            
            # Create table rows for each note
            rows = []
            for note in notes_data:
                columns = [
                    TableResult.TableColumn(
                        name=StringValue(value='Note ID'),
                        value=StringValue(value=note.get('id', ''))
                    ),
                    TableResult.TableColumn(
                        name=StringValue(value='Note'),
                        value=StringValue(value=note.get('note', ''))
                    ),
                    TableResult.TableColumn(
                        name=StringValue(value='Owner'),
                        value=StringValue(value=note.get('owner', ''))
                    ),
                    TableResult.TableColumn(
                        name=StringValue(value='Created At'),
                        value=StringValue(value=str(note.get('created_at', '')))
                    ),
                    TableResult.TableColumn(
                        name=StringValue(value='Updated At'),
                        value=StringValue(value=str(note.get('updated_at', '')))
                    ),
                ]
                
                row = TableResult.TableRow(columns=columns)
                rows.append(row)
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TABLE,
                source=self.source,
                table=TableResult(
                    raw_query=StringValue(value=f"Found {len(rows)} notes for incident {incident_id}"),
                    total_count=UInt64Value(value=len(rows)),
                    rows=rows,
                    searchable=BoolValue(value=True)
                )
            )
        except Exception as e:
            raise Exception(f"Error while executing OpsGenie list_incident_notes task: {e}")
