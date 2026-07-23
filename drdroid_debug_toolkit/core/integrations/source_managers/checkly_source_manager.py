import logging

from google.protobuf.struct_pb2 import Struct
from google.protobuf.wrappers_pb2 import StringValue, UInt64Value

from core.integrations.source_api_processors.checkly_api_processor import ChecklyApiProcessor
from core.integrations.source_manager import SourceManager
from core.protos.base_pb2 import Source, TimeRange, SourceKeyType
from core.protos.connectors.connector_pb2 import Connector as ConnectorProto
from core.protos.literal_pb2 import LiteralType
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResult, PlaybookTaskResultType, ApiResponseResult
from core.protos.playbooks.source_task_definitions.checkly_task_pb2 import Checkly
from core.protos.ui_definition_pb2 import FormField, FormFieldType
from core.utils.credentilal_utils import (
    generate_credentials_dict,
    get_connector_key_type_string,
    DISPLAY_NAME,
    CATEGORY,
    APPLICATION_MONITORING,
)

logger = logging.getLogger(__name__)


class ChecklySourceManager(SourceManager):

    def __init__(self):
        self.source = Source.CHECKLY
        self.task_proto = Checkly

        self.task_type_callable_map = {
            Checkly.TaskType.LIST_CHECKS: {
                "executor": self.execute_list_checks,
                "model_types": [],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "List All Checks",
                "category": "Checks",
                "form_fields": [],
            },
            Checkly.TaskType.GET_CHECK_STATUSES: {
                "executor": self.execute_get_check_statuses,
                "model_types": [],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "Get Check Statuses",
                "category": "Checks",
                "form_fields": [],
            },
            Checkly.TaskType.GET_CHECK_RESULTS: {
                "executor": self.execute_get_check_results,
                "model_types": [],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "Get Check Results",
                "category": "Checks",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="check_id"),
                        display_name=StringValue(value="Check ID"),
                        description=StringValue(value="UUID of the Checkly check"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                    ),
                    FormField(
                        key_name=StringValue(value="limit"),
                        display_name=StringValue(value="Limit"),
                        description=StringValue(value="Max number of results to return (default 10)"),
                        data_type=LiteralType.LONG,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                    ),
                ],
            },
            Checkly.TaskType.GET_CHECK_ANALYTICS: {
                "executor": self.execute_get_check_analytics,
                "model_types": [],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "Get Check Analytics",
                "category": "Analytics",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="check_id"),
                        display_name=StringValue(value="Check ID"),
                        description=StringValue(value="UUID of the Checkly check"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                    ),
                    FormField(
                        key_name=StringValue(value="check_type"),
                        display_name=StringValue(value="Check Type"),
                        description=StringValue(value='Type of check: "api", "browser", "multistep", or "tcp"'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                    ),
                ],
            },
            Checkly.TaskType.GET_CHECK_ALERTS: {
                "executor": self.execute_get_check_alerts,
                "model_types": [],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "Get Check Alerts",
                "category": "Alerts",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="check_id"),
                        display_name=StringValue(value="Check ID"),
                        description=StringValue(value="UUID of the check (leave empty for all account alerts)"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                    ),
                ],
            },
            Checkly.TaskType.GET_CHECK_GROUPS: {
                "executor": self.execute_get_check_groups,
                "model_types": [],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "Get Check Groups",
                "category": "Checks",
                "form_fields": [],
            },
            Checkly.TaskType.GET_ERROR_GROUPS: {
                "executor": self.execute_get_error_groups,
                "model_types": [],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "Get Error Groups",
                "category": "Errors",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="check_id"),
                        display_name=StringValue(value="Check ID"),
                        description=StringValue(value="UUID of the check (leave empty for all account error groups)"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                    ),
                    FormField(
                        key_name=StringValue(value="limit"),
                        display_name=StringValue(value="Limit"),
                        description=StringValue(value="Max number of error groups to return"),
                        data_type=LiteralType.LONG,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                    ),
                ],
            },
            Checkly.TaskType.GET_REPORTING: {
                "executor": self.execute_get_reporting,
                "model_types": [],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "Get Reporting",
                "category": "Analytics",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="quick_range"),
                        display_name=StringValue(value="Quick Range"),
                        description=StringValue(
                            value=(
                                "Used when no time range is set: last24Hrs, last7Days, "
                                "last30Days, thisWeek, thisMonth, lastWeek, lastMonth"
                            )
                        ),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                    ),
                    FormField(
                        key_name=StringValue(value="granularity"),
                        display_name=StringValue(value="Granularity"),
                        description=StringValue(value='One of "day", "week", or "month"'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                    ),
                ],
            },
            Checkly.TaskType.GET_CHECK_RESULT_DETAIL: {
                "executor": self.execute_get_check_result_detail,
                "model_types": [],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "Get Check Result Detail",
                "category": "Checks",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="check_id"),
                        display_name=StringValue(value="Check ID"),
                        description=StringValue(value="UUID of the Checkly check"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                    ),
                    FormField(
                        key_name=StringValue(value="check_result_id"),
                        display_name=StringValue(value="Check Result ID"),
                        description=StringValue(value="UUID of the specific check result"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                    ),
                    FormField(
                        key_name=StringValue(value="include_assets"),
                        display_name=StringValue(value="Include Assets"),
                        description=StringValue(value="Also fetch assets (screenshots, traces, logs) for this result"),
                        data_type=LiteralType.BOOLEAN,
                        form_field_type=FormFieldType.CHECKBOX_FT,
                        is_optional=True,
                    ),
                ],
            },
            Checkly.TaskType.GET_ALERT_CHANNELS: {
                "executor": self.execute_get_alert_channels,
                "model_types": [],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "Get Alert Channels",
                "category": "Alerts",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="limit"),
                        display_name=StringValue(value="Limit"),
                        description=StringValue(value="Max number of alert channels to return"),
                        data_type=LiteralType.LONG,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                    ),
                ],
            },
        }

        self.connector_form_configs = [
            {
                "name": StringValue(value="Checkly API Key Connection"),
                "description": StringValue(value="Connect to Checkly using an API Key and Account ID."),
                "form_fields": {
                    SourceKeyType.CHECKLY_API_KEY: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.CHECKLY_API_KEY)),
                        display_name=StringValue(value="API Key"),
                        helper_text=StringValue(value="Found in Checkly Dashboard → Settings → API Keys"),
                        description=StringValue(value='e.g. "cu_live_xxxxx..."'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                        is_sensitive=True,
                    ),
                    SourceKeyType.CHECKLY_ACCOUNT_ID: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.CHECKLY_ACCOUNT_ID)),
                        display_name=StringValue(value="Account ID"),
                        helper_text=StringValue(value="Found in Checkly Dashboard → Settings → General"),
                        description=StringValue(value='e.g. "12345678-xxxx-xxxx-xxxx-xxxxxxxxxxxx"'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                        is_sensitive=False,
                    ),
                },
            }
        ]

        self.connector_type_details = {
            DISPLAY_NAME: "Checkly",
            CATEGORY: APPLICATION_MONITORING,
        }

    def get_connector_processor(self, checkly_connector: ConnectorProto, **kwargs) -> ChecklyApiProcessor:
        generated_credentials = generate_credentials_dict(checkly_connector.type, checkly_connector.keys)
        return ChecklyApiProcessor(**generated_credentials)

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

    def execute_list_checks(self, time_range: TimeRange, checkly_task: Checkly, connector: ConnectorProto):
        try:
            processor = self.get_connector_processor(connector)
            result = processor.list_checks()
            return self._build_api_response(result, "/v1/checks")
        except Exception as e:
            logger.error(f"ChecklySourceManager.execute_list_checks:: Error: {e}")
            return PlaybookTaskResult(source=self.source, error=StringValue(value=str(e)))

    def execute_get_check_statuses(self, time_range: TimeRange, checkly_task: Checkly, connector: ConnectorProto):
        try:
            processor = self.get_connector_processor(connector)
            result = processor.get_check_statuses()
            return self._build_api_response(result, "/v1/check-statuses")
        except Exception as e:
            logger.error(f"ChecklySourceManager.execute_get_check_statuses:: Error: {e}")
            return PlaybookTaskResult(source=self.source, error=StringValue(value=str(e)))

    def execute_get_check_results(self, time_range: TimeRange, checkly_task: Checkly, connector: ConnectorProto):
        try:
            task_data = checkly_task.get_check_results
            check_id = task_data.check_id.value
            if not check_id:
                raise Exception("check_id is required")
            limit = task_data.limit.value if task_data.HasField("limit") else 10

            processor = self.get_connector_processor(connector)
            result = processor.get_check_results(check_id, limit=limit)
            return self._build_api_response(result, f"/v1/check-results/{check_id}")
        except Exception as e:
            logger.error(f"ChecklySourceManager.execute_get_check_results:: Error: {e}")
            return PlaybookTaskResult(source=self.source, error=StringValue(value=str(e)))

    def execute_get_check_analytics(self, time_range: TimeRange, checkly_task: Checkly, connector: ConnectorProto):
        try:
            task_data = checkly_task.get_check_analytics
            check_id = task_data.check_id.value
            if not check_id:
                raise Exception("check_id is required")
            check_type = task_data.check_type.value if task_data.check_type.value else "api"

            from_ts = int(time_range.time_geq) if time_range and time_range.time_geq else None
            to_ts = int(time_range.time_lt) if time_range and time_range.time_lt else None

            processor = self.get_connector_processor(connector)
            result = processor.get_check_analytics(check_id, check_type, from_ts=from_ts, to_ts=to_ts)
            return self._build_api_response(result, f"/v1/analytics/{check_type}-checks/{check_id}")
        except Exception as e:
            logger.error(f"ChecklySourceManager.execute_get_check_analytics:: Error: {e}")
            return PlaybookTaskResult(source=self.source, error=StringValue(value=str(e)))

    def execute_get_check_alerts(self, time_range: TimeRange, checkly_task: Checkly, connector: ConnectorProto):
        try:
            task_data = checkly_task.get_check_alerts
            check_id = task_data.check_id.value if task_data.check_id.value else None

            processor = self.get_connector_processor(connector)
            result = processor.get_check_alerts(check_id=check_id)
            url_path = f"/v1/check-alerts/{check_id}" if check_id else "/v1/check-alerts"
            return self._build_api_response(result, url_path)
        except Exception as e:
            logger.error(f"ChecklySourceManager.execute_get_check_alerts:: Error: {e}")
            return PlaybookTaskResult(source=self.source, error=StringValue(value=str(e)))

    def execute_get_check_groups(self, time_range: TimeRange, checkly_task: Checkly, connector: ConnectorProto):
        try:
            processor = self.get_connector_processor(connector)
            result = processor.get_check_groups()
            return self._build_api_response(result, "/v1/check-groups")
        except Exception as e:
            logger.error(f"ChecklySourceManager.execute_get_check_groups:: Error: {e}")
            return PlaybookTaskResult(source=self.source, error=StringValue(value=str(e)))

    def execute_get_error_groups(self, time_range: TimeRange, checkly_task: Checkly, connector: ConnectorProto):
        try:
            task_data = checkly_task.get_error_groups
            check_id = task_data.check_id.value if task_data.check_id.value else None
            limit = task_data.limit.value if task_data.HasField("limit") else None
            page = task_data.page.value if task_data.HasField("page") else None

            processor = self.get_connector_processor(connector)
            result = processor.get_error_groups(check_id=check_id, limit=limit, page=page)
            url_path = f"/v1/error-groups/checks/{check_id}" if check_id else "/v1/error-groups"
            return self._build_api_response(result, url_path)
        except Exception as e:
            logger.error(f"ChecklySourceManager.execute_get_error_groups:: Error: {e}")
            return PlaybookTaskResult(source=self.source, error=StringValue(value=str(e)))

    def execute_get_reporting(self, time_range: TimeRange, checkly_task: Checkly, connector: ConnectorProto):
        try:
            task_data = checkly_task.get_reporting
            quick_range = task_data.quick_range.value if task_data.quick_range.value else None
            granularity = task_data.granularity.value if task_data.granularity.value else None

            from_ts = self._epoch_to_iso(time_range.time_geq) if time_range and time_range.time_geq else None
            to_ts = self._epoch_to_iso(time_range.time_lt) if time_range and time_range.time_lt else None
            if not (from_ts and to_ts) and not quick_range:
                quick_range = "last24Hrs"

            processor = self.get_connector_processor(connector)
            result = processor.get_reporting(from_ts=from_ts, to_ts=to_ts, quick_range=quick_range, granularity=granularity)
            return self._build_api_response(result, "/v1/reporting")
        except Exception as e:
            logger.error(f"ChecklySourceManager.execute_get_reporting:: Error: {e}")
            return PlaybookTaskResult(source=self.source, error=StringValue(value=str(e)))

    def execute_get_check_result_detail(self, time_range: TimeRange, checkly_task: Checkly, connector: ConnectorProto):
        try:
            task_data = checkly_task.get_check_result_detail
            check_id = task_data.check_id.value
            check_result_id = task_data.check_result_id.value
            if not check_id or not check_result_id:
                raise Exception("check_id and check_result_id are required")
            include_assets = task_data.include_assets.value if task_data.HasField("include_assets") else False

            processor = self.get_connector_processor(connector)
            result = processor.get_check_result_detail(check_id, check_result_id)
            if include_assets and isinstance(result, dict):
                result = {**result, "assets": processor.get_check_result_assets(check_id, check_result_id)}
            return self._build_api_response(result, f"/v1/check-results/{check_id}/{check_result_id}")
        except Exception as e:
            logger.error(f"ChecklySourceManager.execute_get_check_result_detail:: Error: {e}")
            return PlaybookTaskResult(source=self.source, error=StringValue(value=str(e)))

    def execute_get_alert_channels(self, time_range: TimeRange, checkly_task: Checkly, connector: ConnectorProto):
        try:
            task_data = checkly_task.get_alert_channels
            limit = task_data.limit.value if task_data.HasField("limit") else None
            page = task_data.page.value if task_data.HasField("page") else None

            processor = self.get_connector_processor(connector)
            result = processor.get_alert_channels(limit=limit, page=page)
            return self._build_api_response(result, "/v1/alert-channels")
        except Exception as e:
            logger.error(f"ChecklySourceManager.execute_get_alert_channels:: Error: {e}")
            return PlaybookTaskResult(source=self.source, error=StringValue(value=str(e)))

    @staticmethod
    def _epoch_to_iso(epoch_seconds) -> str:
        from datetime import datetime, timezone

        return datetime.fromtimestamp(int(epoch_seconds), tz=timezone.utc).isoformat()
