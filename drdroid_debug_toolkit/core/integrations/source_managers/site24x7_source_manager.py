import logging
from datetime import datetime, timezone

from google.protobuf.struct_pb2 import Struct
from google.protobuf.wrappers_pb2 import StringValue, UInt64Value

from core.integrations.source_api_processors.site24x7_api_processor import Site24x7ApiProcessor
from core.integrations.source_manager import SourceManager
from core.protos.base_pb2 import Source, TimeRange, SourceKeyType
from core.protos.connectors.connector_pb2 import Connector as ConnectorProto
from core.protos.literal_pb2 import LiteralType, Literal
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResult, PlaybookTaskResultType, ApiResponseResult
from core.protos.playbooks.source_task_definitions.site24x7_task_pb2 import Site24x7
from core.protos.ui_definition_pb2 import FormField, FormFieldType
from core.utils.credentilal_utils import (
    generate_credentials_dict,
    get_connector_key_type_string,
    DISPLAY_NAME,
    CATEGORY,
    APPLICATION_MONITORING,
)

logger = logging.getLogger(__name__)

# Site24x7 monitor status codes that represent an active problem.
# 0=Down, 2=Trouble, 3=Critical (1=Up, 5=Suspended, 7=Maintenance, ...).
DOWN_STATUS_CODES = {0, 2, 3}


class Site24x7SourceManager(SourceManager):

    def __init__(self):
        self.source = Source.SITE24X7
        self.task_proto = Site24x7

        self.task_type_callable_map = {
            Site24x7.TaskType.GET_CURRENT_STATUS: {
                "executor": self.execute_get_current_status,
                "model_types": [],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "Get Current Status (All Monitors)",
                "category": "Status",
                "form_fields": [],
            },
            Site24x7.TaskType.GET_DOWN_MONITORS: {
                "executor": self.execute_get_down_monitors,
                "model_types": [],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "Get Down & Trouble Monitors",
                "category": "Status",
                "form_fields": [],
            },
            Site24x7.TaskType.LIST_MONITORS: {
                "executor": self.execute_list_monitors,
                "model_types": [],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "List All Monitors",
                "category": "Monitors",
                "form_fields": [],
            },
            Site24x7.TaskType.GET_MONITOR_DETAILS: {
                "executor": self.execute_get_monitor_details,
                "model_types": [],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "Get Monitor Details",
                "category": "Monitors",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="monitor_id"),
                        display_name=StringValue(value="Monitor ID"),
                        description=StringValue(value="ID of the Site24x7 monitor"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                    ),
                ],
            },
            Site24x7.TaskType.GET_AVAILABILITY_REPORT: {
                "executor": self.execute_get_availability_report,
                "model_types": [],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "Get Availability Report",
                "category": "Reports",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="monitor_id"),
                        display_name=StringValue(value="Monitor ID"),
                        description=StringValue(value="ID of the Site24x7 monitor"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                    ),
                    FormField(
                        key_name=StringValue(value="period"),
                        display_name=StringValue(value="Period"),
                        description=StringValue(
                            value="Site24x7 period constant (used only when no task time range is set). "
                            "e.g. 0=today, 1=last day, 7=last week, 30=last month. Defaults to 7."
                        ),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                    ),
                ],
            },
            Site24x7.TaskType.GET_PERFORMANCE_REPORT: {
                "executor": self.execute_get_performance_report,
                "model_types": [],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "Get Performance Report",
                "category": "Reports",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="monitor_id"),
                        display_name=StringValue(value="Monitor ID"),
                        description=StringValue(value="ID of the Site24x7 monitor"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                    ),
                    FormField(
                        key_name=StringValue(value="period"),
                        display_name=StringValue(value="Period"),
                        description=StringValue(
                            value="Site24x7 period constant (used only when no task time range is set). "
                            "e.g. 0=today, 1=last day, 7=last week, 30=last month. Defaults to 7."
                        ),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                    ),
                ],
            },
        }

        self.connector_form_configs = [
            {
                "name": StringValue(value="Site24x7 OAuth Connection"),
                "description": StringValue(value="Connect to Site24x7 using Zoho OAuth (refresh token) credentials."),
                "form_fields": {
                    SourceKeyType.SITE24X7_CLIENT_ID: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.SITE24X7_CLIENT_ID)),
                        display_name=StringValue(value="Client ID"),
                        helper_text=StringValue(value="Zoho API Console → your app → Client ID"),
                        description=StringValue(value='e.g. "1000.XXXXXXXXXXXXXXXXXXXX"'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                        is_sensitive=False,
                    ),
                    SourceKeyType.SITE24X7_CLIENT_SECRET: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.SITE24X7_CLIENT_SECRET)),
                        display_name=StringValue(value="Client Secret"),
                        helper_text=StringValue(value="Zoho API Console → your app → Client Secret"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                        is_sensitive=True,
                    ),
                    SourceKeyType.SITE24X7_REFRESH_TOKEN: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.SITE24X7_REFRESH_TOKEN)),
                        display_name=StringValue(value="Refresh Token"),
                        helper_text=StringValue(
                            value="Permanent Zoho OAuth refresh token generated with Site24x7 read scopes"
                        ),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                        is_sensitive=True,
                    ),
                    SourceKeyType.SITE24X7_DATA_CENTER: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.SITE24X7_DATA_CENTER)),
                        display_name=StringValue(value="Data Center"),
                        helper_text=StringValue(value="Region where your Site24x7 account is hosted"),
                        description=StringValue(value="Select the matching Zoho data center"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.DROPDOWN_FT,
                        valid_values=[
                            Literal(type=LiteralType.STRING, string=StringValue(value="US")),
                            Literal(type=LiteralType.STRING, string=StringValue(value="EU")),
                            Literal(type=LiteralType.STRING, string=StringValue(value="IN")),
                            Literal(type=LiteralType.STRING, string=StringValue(value="AU")),
                            Literal(type=LiteralType.STRING, string=StringValue(value="JP")),
                            Literal(type=LiteralType.STRING, string=StringValue(value="CA")),
                            Literal(type=LiteralType.STRING, string=StringValue(value="UK")),
                            Literal(type=LiteralType.STRING, string=StringValue(value="CN")),
                        ],
                        default_value=Literal(type=LiteralType.STRING, string=StringValue(value="US")),
                        is_optional=False,
                        is_sensitive=False,
                    ),
                },
            }
        ]

        self.connector_type_details = {
            DISPLAY_NAME: "Site24x7",
            CATEGORY: APPLICATION_MONITORING,
        }

    def get_connector_processor(self, site24x7_connector: ConnectorProto, **kwargs) -> Site24x7ApiProcessor:
        generated_credentials = generate_credentials_dict(site24x7_connector.type, site24x7_connector.keys)
        return Site24x7ApiProcessor(**generated_credentials)

    def _build_api_response(self, result, url_path: str) -> PlaybookTaskResult:
        result_struct = Struct()
        if isinstance(result, list):
            result_struct.update({"results": result, "count": len(result)})
        elif isinstance(result, dict):
            result_struct.update(result)
        else:
            result_struct.update({"data": str(result)})

        return PlaybookTaskResult(
            source=self.source,
            type=PlaybookTaskResultType.API_RESPONSE,
            api_response=ApiResponseResult(
                request_method=StringValue(value="GET"),
                request_url=StringValue(value=url_path),
                response_status=UInt64Value(value=200),
                response_body=result_struct,
            ),
        )

    def _report_params(self, task_data, time_range: TimeRange) -> dict:
        """Build report query params. Prefer the task time range (as start_date/end_date,
        Site24x7's documented custom-range format YYYY-MM-DD); otherwise fall back to the
        `period` constant, defaulting to last week."""
        if time_range and time_range.time_geq and time_range.time_lt:
            start = datetime.fromtimestamp(int(time_range.time_geq), tz=timezone.utc).strftime("%Y-%m-%d")
            end = datetime.fromtimestamp(int(time_range.time_lt), tz=timezone.utc).strftime("%Y-%m-%d")
            return {"start_date": start, "end_date": end}
        period = task_data.period.value if task_data.period.value else "7"
        return {"period": period}

    @staticmethod
    def _extract_down_monitors(current_status: dict) -> list:
        """Walk the current_status payload and collect monitor entries whose numeric
        `status` is Down/Trouble/Critical. Robust to the response's group nesting."""
        down = []

        def walk(node):
            if isinstance(node, dict):
                status = node.get("status")
                if status is not None and (node.get("name") or node.get("monitor_id")):
                    try:
                        if int(status) in DOWN_STATUS_CODES:
                            down.append(node)
                    except (TypeError, ValueError):
                        pass
                for value in node.values():
                    walk(value)
            elif isinstance(node, list):
                for item in node:
                    walk(item)

        walk(current_status.get("data", current_status) if isinstance(current_status, dict) else current_status)
        return down

    def execute_get_current_status(self, time_range: TimeRange, site24x7_task: Site24x7, connector: ConnectorProto):
        try:
            processor = self.get_connector_processor(connector)
            result = processor.get_current_status()
            return self._build_api_response(result, "/current_status")
        except Exception as e:
            logger.error(f"Site24x7SourceManager.execute_get_current_status:: Error: {e}")
            return PlaybookTaskResult(source=self.source, error=StringValue(value=str(e)))

    def execute_get_down_monitors(self, time_range: TimeRange, site24x7_task: Site24x7, connector: ConnectorProto):
        try:
            processor = self.get_connector_processor(connector)
            result = processor.get_current_status()
            down = self._extract_down_monitors(result)
            return self._build_api_response({"down_monitors": down, "count": len(down)}, "/current_status")
        except Exception as e:
            logger.error(f"Site24x7SourceManager.execute_get_down_monitors:: Error: {e}")
            return PlaybookTaskResult(source=self.source, error=StringValue(value=str(e)))

    def execute_list_monitors(self, time_range: TimeRange, site24x7_task: Site24x7, connector: ConnectorProto):
        try:
            processor = self.get_connector_processor(connector)
            result = processor.list_monitors()
            return self._build_api_response(result, "/monitors")
        except Exception as e:
            logger.error(f"Site24x7SourceManager.execute_list_monitors:: Error: {e}")
            return PlaybookTaskResult(source=self.source, error=StringValue(value=str(e)))

    def execute_get_monitor_details(self, time_range: TimeRange, site24x7_task: Site24x7, connector: ConnectorProto):
        try:
            task_data = site24x7_task.get_monitor_details
            monitor_id = task_data.monitor_id.value
            if not monitor_id:
                raise Exception("monitor_id is required")

            processor = self.get_connector_processor(connector)
            result = processor.get_monitor_details(monitor_id)
            return self._build_api_response(result, f"/monitors/{monitor_id}")
        except Exception as e:
            logger.error(f"Site24x7SourceManager.execute_get_monitor_details:: Error: {e}")
            return PlaybookTaskResult(source=self.source, error=StringValue(value=str(e)))

    def execute_get_availability_report(self, time_range: TimeRange, site24x7_task: Site24x7, connector: ConnectorProto):
        try:
            task_data = site24x7_task.get_availability_report
            monitor_id = task_data.monitor_id.value
            if not monitor_id:
                raise Exception("monitor_id is required")

            params = self._report_params(task_data, time_range)
            processor = self.get_connector_processor(connector)
            result = processor.get_availability_report(monitor_id, params=params)
            return self._build_api_response(result, f"/reports/availability/{monitor_id}")
        except Exception as e:
            logger.error(f"Site24x7SourceManager.execute_get_availability_report:: Error: {e}")
            return PlaybookTaskResult(source=self.source, error=StringValue(value=str(e)))

    def execute_get_performance_report(self, time_range: TimeRange, site24x7_task: Site24x7, connector: ConnectorProto):
        try:
            task_data = site24x7_task.get_performance_report
            monitor_id = task_data.monitor_id.value
            if not monitor_id:
                raise Exception("monitor_id is required")

            params = self._report_params(task_data, time_range)
            processor = self.get_connector_processor(connector)
            result = processor.get_performance_report(monitor_id, params=params)
            return self._build_api_response(result, f"/reports/performance/{monitor_id}")
        except Exception as e:
            logger.error(f"Site24x7SourceManager.execute_get_performance_report:: Error: {e}")
            return PlaybookTaskResult(source=self.source, error=StringValue(value=str(e)))
