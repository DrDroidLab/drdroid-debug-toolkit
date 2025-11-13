import json
import logging

from datetime import datetime

from google.protobuf.struct_pb2 import Struct
from google.protobuf.wrappers_pb2 import DoubleValue, StringValue, UInt64Value, Int64Value

from core.integrations.source_api_processors.datadog_api_processor import DatadogApiProcessor
from core.integrations.source_manager import SourceManager
from core.protos.base_pb2 import TimeRange, Source, SourceModelType, SourceKeyType
from core.protos.connectors.connector_pb2 import Connector as ConnectorProto
from core.protos.literal_pb2 import LiteralType, Literal
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResult, TimeseriesResult, LabelValuePair, \
    PlaybookTaskResultType, TableResult, TextResult, ApiResponseResult
from core.protos.playbooks.source_task_definitions.datadog_task_pb2 import Datadog
from core.protos.assets.asset_pb2 import AccountConnectorAssets
from core.protos.assets.datadog_asset_pb2 import DatadogServiceAssetModel, DatadogDashboardModel
from core.protos.ui_definition_pb2 import FormField, FormFieldType

from core.utils.credentilal_utils import generate_credentials_dict, get_connector_key_type_string, DISPLAY_NAME, CATEGORY, APPLICATION_MONITORING
from core.utils.proto_utils import proto_to_dict, dict_to_proto
from core.utils.string_utils import is_partial_match
from core.utils.playbooks_client import PrototypeClient

logger = logging.getLogger(__name__)


class DatadogSourceManager(SourceManager):

    def __init__(self):
        self.source = Source.DATADOG
        self.task_proto = Datadog
        self.task_type_callable_map = {
            Datadog.TaskType.SERVICE_METRIC_EXECUTION: {
                'executor': self.execute_service_metric_execution,
                'model_types': [SourceModelType.DATADOG_SERVICE],
                'result_type': PlaybookTaskResultType.TIMESERIES,
                'display_name': 'Fetch a Datadog Metric by service',
                'category': 'Metrics',
                'form_fields': [
                    FormField(key_name=StringValue(value="service_name"),
                              display_name=StringValue(value="Service"),
                              description=StringValue(value='e.g. web-api, auth-service, payment-processor'),
                              helper_text=StringValue(value='Select Service'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT),
                    FormField(key_name=StringValue(value="environment_name"),
                              display_name=StringValue(value="Environment"),
                              description=StringValue(value='e.g. prod, staging, dev'),
                              helper_text=StringValue(value='Select Environment'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT),
                    FormField(key_name=StringValue(value="metric_family"),
                              display_name=StringValue(value="Metric Family"),
                              description=StringValue(value='e.g. system.cpu, system.memory, http.requests'),
                              helper_text=StringValue(value='Select Metric Family'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT),
                    FormField(key_name=StringValue(value="metric"),
                              display_name=StringValue(value="Metric"),
                              description=StringValue(value='e.g. system.cpu.user, system.memory.used, http.requests.total'),
                              helper_text=StringValue(value='Select Metric'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT),
                    FormField(key_name=StringValue(value="interval"),
                              display_name=StringValue(value="Interval(Seconds)"),
                              description=StringValue(value='e.g. 60, 300, 900'),
                              helper_text=StringValue(value='(Optional) Enter Interval'),
                              data_type=LiteralType.LONG,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                ]
            },
            Datadog.TaskType.LOG_QUERY_EXECUTION: {
                'executor': self.execute_log_query_execution,
                'model_types': [],
                'result_type': PlaybookTaskResultType.LOGS,
                'display_name': 'Fetch a Datadog log',
                'category': 'Logs',
                'form_fields': [
                    FormField(key_name=StringValue(value="query"),
                              display_name=StringValue(value="Query"),
                              description=StringValue(value='e.g. "service:web-api status:error", "env:prod source:nginx"'),
                              helper_text=StringValue(value='Enter Query'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="limit"),
                              display_name=StringValue(value="Limit"),
                              description=StringValue(value='e.g. 100, 200, 300'),
                              helper_text=StringValue(value='(Optional) Enter Limit'),
                              default_value=Literal(type=LiteralType.LONG, long=Int64Value(value=100)),
                              data_type=LiteralType.LONG,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                ]
            },
            Datadog.TaskType.DASHBOARD_MULTIPLE_WIDGETS: {
                'executor': self.execute_dashboard_multiple_widgets_task,
                'model_types': [SourceModelType.DATADOG_DASHBOARD],
                'result_type': PlaybookTaskResultType.TIMESERIES,
                'display_name': 'Fetch multiple widgets by dashboard name and widget ID',
                'category': 'Metrics',
                'form_fields': [
                    FormField(key_name=StringValue(value="dashboard_name"),
                              display_name=StringValue(value="Dashboard Name"),
                              description=StringValue(value='e.g. "System Overview", "API Performance"'),
                              helper_text=StringValue(value="Enter Dashboard Name"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT),
                    FormField(key_name=StringValue(value="widget_id"),
                              display_name=StringValue(value="Widget ID"),
                              description=StringValue(value='e.g. "cpu_usage_widget", "error_rate_chart"'),
                              helper_text=StringValue(value="Enter Widget ID"),
                              default_value=Literal(type=LiteralType.STRING, string=StringValue(value="")),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="interval"),
                              display_name=StringValue(value="Interval(Seconds)"),
                              description=StringValue(value='e.g. 60, 300, 900'),
                              helper_text=StringValue(value='(Optional)Enter Interval'),
                              default_value=Literal(type=LiteralType.LONG, long=Int64Value(value=300)),
                              data_type=LiteralType.LONG,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="template_variables"),
                              display_name=StringValue(value="Template Variables"),
                              description=StringValue(value='e.g. {"env": "prod", "service": "web-api"}'),
                              helper_text=StringValue(value="(Optional) Enter Template Variables"),
                              default_value=Literal(type=LiteralType.STRING, string=StringValue(value="{}")),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.CODE_EDITOR_FT,
                              is_optional=True),
                ]
            },
            Datadog.TaskType.APM_QUERY: {
                'executor': self.execute_apm_queries,
                'model_types': [SourceModelType.DATADOG_APM],
                'result_type': PlaybookTaskResultType.TIMESERIES,
                'display_name': 'Fetch APM Queries',
                'category': 'Metrics',
                'form_fields': [
                    FormField(key_name=StringValue(value="service_name"),
                              display_name=StringValue(value="Service Name"),
                              description=StringValue(value='e.g. "web-api", "auth-service"'),
                              helper_text=StringValue(value="Enter Service Name"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="metric_families"),
                              display_name=StringValue(value="Metric Families"),
                              description=StringValue(value='e.g. "trace.postgres.query,trace.redis.query,trace.http.client.request"'),
                              helper_text=StringValue(value='(Optional) Enter comma-separated Metric Families to query (leave empty for all)'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="interval"),
                              display_name=StringValue(value="Interval(Seconds)"),
                              description=StringValue(value='e.g. 60, 300, 900'),
                              helper_text=StringValue(value='(Optional)Enter Interval'),
                              data_type=LiteralType.LONG,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="environments"),
                              display_name=StringValue(value="Environments"),
                              description=StringValue(value='e.g. "production,staging,dev"'),
                              helper_text=StringValue(value='(Optional) Enter comma-separated environments to match (leave empty to use default behavior)'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                ]
            },
            Datadog.TaskType.SPAN_SEARCH_EXECUTION: {
                'executor': self.execute_span_search_execution,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Fetch Datadog spans',
                'category': 'APM',
                'form_fields': [
                    FormField(key_name=StringValue(value="query"),
                              display_name=StringValue(value="Query"),
                              description=StringValue(value='Datadog span query, e.g. "service:web-api env:prod"'),
                              helper_text=StringValue(value='Defaults to "*" when left blank'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              default_value=Literal(type=LiteralType.STRING, string=StringValue(value="*")),
                              is_optional=True),
                    FormField(key_name=StringValue(value="limit"),
                              display_name=StringValue(value="Limit"),
                              description=StringValue(value='Number of spans to fetch (max 1000)'),
                              helper_text=StringValue(value='Defaults to 100'),
                              default_value=Literal(type=LiteralType.LONG, long=Int64Value(value=100)),
                              data_type=LiteralType.LONG,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="cursor"),
                              display_name=StringValue(value="Cursor"),
                              description=StringValue(value='Pagination cursor from previous response'),
                              helper_text=StringValue(value='Use to paginate through large result sets'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                ]
            },
            Datadog.TaskType.GENERIC_QUERY: {
                'executor': self.execute_generic_query,
                'model_types': [],
                'result_type': PlaybookTaskResultType.TIMESERIES,
                'display_name': 'Execute Generic Datadog Query',
                'category': 'Metrics',
                'form_fields': [
                    FormField(key_name=StringValue(value="query"),
                              display_name=StringValue(value="Query"),
                              description=StringValue(value='e.g. "avg:system.cpu.user{*}"'),
                              helper_text=StringValue(value="Enter Datadog Query"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="interval"),
                              display_name=StringValue(value="Interval(Seconds)"),
                              description=StringValue(value='e.g. 60, 300, 900'),
                              helper_text=StringValue(value='(Optional)Enter Interval'),
                              data_type=LiteralType.LONG,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                ]
            },
            Datadog.TaskType.GET_DASHBOARD_CONFIG_DETAILS: {
                'executor': self.execute_get_dashboard_config_details,
                'model_types': [SourceModelType.DATADOG_DASHBOARD],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Get Dashboard Config Details',
                'category': 'Dashboards',
                'form_fields': [
                    FormField(key_name=StringValue(value="dashboard_id"),
                              display_name=StringValue(value="Dashboard ID"),
                              description=StringValue(value='e.g. "abc-123-def"'),
                              helper_text=StringValue(value="Enter Dashboard ID"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                ]
            },
            Datadog.TaskType.GET_DASHBOARD_VARIABLE_VALUES: {
                'executor': self.execute_get_dashboard_variable_values,
                'model_types': [SourceModelType.DATADOG_DASHBOARD],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Get Dashboard Variable Values',
                'category': 'Dashboards',
                'form_fields': [
                    FormField(key_name=StringValue(value="dashboard_id"),
                              display_name=StringValue(value="Dashboard ID"),
                              description=StringValue(value='e.g. "abc-123-def"'),
                              helper_text=StringValue(value="Enter Dashboard ID"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="variable_name"),
                              display_name=StringValue(value="Variable Name"),
                              description=StringValue(value='e.g. "env", "service"'),
                              helper_text=StringValue(value="(Optional) Enter Variable Name"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                ]
            },
            Datadog.TaskType.FETCH_DASHBOARDS: {
                'executor': self.execute_fetch_dashboards,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Fetch Dashboards',
                'category': 'Dashboards',
                'form_fields': [],
            },
        }
        self.connector_form_configs = [
            {
                "name": StringValue(value="Datadog API and Application Keys"),
                "description": StringValue(value="Connect to Datadog using your API Key and Application Key. Optionally, specify the API domain."),
                "form_fields": {
                    SourceKeyType.DATADOG_API_KEY: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.DATADOG_API_KEY)),
                        display_name=StringValue(value="API Key"),
                        description=StringValue(value='e.g. "1234abcd5678efgh9012ijkl3456mnop"'),
                        helper_text=StringValue(value="Enter your Datadog API Key found in Organization Settings > API Keys"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                        is_sensitive=True
                    ),
                    SourceKeyType.DATADOG_APP_KEY: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.DATADOG_APP_KEY)),
                        display_name=StringValue(value="Application Key"),
                        description=StringValue(value='e.g. "abcd1234efgh5678ijkl9012mnop3456qrst7890uvwx"'),
                        helper_text=StringValue(value="Enter your Datadog Application Key found in Organization Settings > Application Keys"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                        is_sensitive=True
                    ),
                    SourceKeyType.DATADOG_API_DOMAIN: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.DATADOG_API_DOMAIN)),
                        display_name=StringValue(value="API Domain"),
                        description=StringValue(value='e.g. "datadoghq.com" (US), "datadoghq.eu" (EU), "us3.datadoghq.com" (US3)'),
                        helper_text=StringValue(value="Enter your Datadog site's domain (defaults to datadoghq.com if empty)"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                        default_value=Literal(type=LiteralType.STRING, string=StringValue(value="datadoghq.com"))
                    )
                }
            }
        ]
        self.connector_type_details = {
            DISPLAY_NAME: "DATADOG",
            CATEGORY: APPLICATION_MONITORING,
        }
    def get_connector_processor(self, datadog_connector, **kwargs):
        generated_credentials = generate_credentials_dict(datadog_connector.type, datadog_connector.keys)
        if 'dd_api_domain' not in generated_credentials:
            generated_credentials['dd_api_domain'] = 'datadoghq.com'
        return DatadogApiProcessor(**generated_credentials)

    def _extract_api_domain_from_connector(self, datadog_connector: ConnectorProto) -> str:
        """Extract the Datadog API domain from the connector."""
        if not datadog_connector or not datadog_connector.keys:
            return "datadoghq.com"
        
        for key in datadog_connector.keys:
            if key.key_type == SourceKeyType.DATADOG_API_DOMAIN and key.key.value:
                return key.key.value
        
        return "datadoghq.com"

    def _build_datadog_url(self, api_domain: str, task_type: str, params: dict = None) -> str:
        """
        Build Datadog URLs for different task types.
        
        Args:
            api_domain: Datadog API domain from connector (e.g., datadoghq.com, datadoghq.eu)
            task_type: Type of task (service_metric_execution, apm_query, generic_query, etc.)
            params: Dictionary containing task-specific parameters
        
        Returns:
            Complete Datadog URL for the specific task type
        """
        if not api_domain:
            api_domain = "datadoghq.com"
        
        # Ensure we have the correct domain format
        if not api_domain.startswith('app.'):
            base_url = f"https://app.{api_domain}"
        else:
            base_url = f"https://{api_domain}"
        
        if params is None:
            params = {}
        
        if task_type == "service_metric_execution":
            service_name = params.get('service_name', '')
            environment_name = params.get('environment_name', '')
            if service_name and environment_name:
                # URL for service metrics in APM
                return f"{base_url}/apm/services/{service_name}?env={environment_name}"
            elif service_name:
                return f"{base_url}/apm/services/{service_name}"
            else:
                return f"{base_url}/apm/services"
        
        elif task_type == "apm_query":
            service_name = params.get('service_name', '')
            environment_name = params.get('environment_name', 'prod')
            if service_name:
                # URL for APM service overview
                return f"{base_url}/software?env=%2A&fromUser=true&selectedEnv={environment_name}&selectedService={service_name}"
            else:
                return f"{base_url}/software?env=%2A&fromUser=true"
        
        elif task_type == "generic_query":
            # URL for metric explorer
            return f"{base_url}/metric/explorer"
        
        elif task_type == "log_query_execution":
            # URL for log explorer
            return f"{base_url}/logs"
        
        elif task_type == "dashboard_multiple_widgets":
            dashboard_id = params.get('dashboard_id', '')
            if dashboard_id:
                return f"{base_url}/dashboard/{dashboard_id}"
            else:
                return f"{base_url}/dashboard/lists"
        
        elif task_type == "get_dashboard_config_details":
            dashboard_id = params.get('dashboard_id', '')
            if dashboard_id:
                return f"{base_url}/dashboard/{dashboard_id}"
            else:
                return f"{base_url}/dashboard/lists"
        
        elif task_type == "get_dashboard_variable_values":
            dashboard_id = params.get('dashboard_id', '')
            if dashboard_id:
                return f"{base_url}/dashboard/{dashboard_id}"
            else:
                return f"{base_url}/dashboard/lists"
        
        elif task_type == "fetch_dashboards":
            return f"{base_url}/dashboard/lists"
        
        else:
            # Default fallback to dashboard lists
            return f"{base_url}/dashboard/lists"

    def _create_metadata_with_datadog_url(self, api_domain: str, task_type: str, params: dict = None) -> Struct:
        """Create metadata struct with Datadog URL."""
        datadog_url = self._build_datadog_url(api_domain, task_type, params)
        metadata_dict = {
            "link": datadog_url
        }
        return dict_to_proto(metadata_dict, Struct)

    def execute_service_metric_execution(self, time_range: TimeRange, dd_task: Datadog,
                                         datadog_connector: ConnectorProto):
        try:
            if not datadog_connector:
                raise Exception("Task execution Failed:: No Datadog source found")

            task = dd_task.service_metric_execution
            service_name = task.service_name.value
            env_name = task.environment_name.value
            metric = task.metric.value
            interval = task.interval.value
            if not interval:
                interval = 300

            timeseries_offsets = task.timeseries_offsets
            query_tags = f"service:{service_name},env:{env_name}"
            metric_query = f'avg:{metric}{{{query_tags}}}'
            specific_metric = {"queries": [
                {
                    "name": "query1",
                    "query": metric_query
                }
            ]}

            dd_api_processor = self.get_connector_processor(datadog_connector)

            labeled_metric_timeseries: [TimeseriesResult.LabeledMetricTimeseries] = []

            # Get current time values
            current_results = dd_api_processor.fetch_metric_timeseries(time_range, specific_metric,
                                                                       interval=interval * 1000)
            if not current_results:
                metric_str = ''
                for query_item in specific_metric['queries']:
                    metric_str += query_item['query']
                    metric_str += ', '
                # Extract Datadog API domain and create metadata
                api_domain = self._extract_api_domain_from_connector(datadog_connector)
                task_params = {
                    'service_name': service_name,
                    'environment_name': env_name
                }
                metadata = self._create_metadata_with_datadog_url(api_domain, "service_metric_execution", task_params)
                
                return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT, text=TextResult(output=StringValue(
                    value=f"No data returned from Datadog for service metric: {metric_str}")), source=self.source, metadata=metadata)

            for itr, item in enumerate(current_results.series.value):
                group_tags = item.group_tags.value
                metric_labels: [LabelValuePair] = []
                if item.unit:
                    unit = item.unit[0].name
                else:
                    unit = ''
                for gt in group_tags:
                    metric_labels.append(
                        LabelValuePair(name=StringValue(value='resource_name'), value=StringValue(value=gt)))

                metric_labels.append(
                    LabelValuePair(name=StringValue(value='offset_seconds'), value=StringValue(value='0'))
                )

                times = current_results.times.value
                values = current_results.values.value[itr].value
                datapoints: [TimeseriesResult.LabeledMetricTimeseries.Datapoint] = []
                for it, val in enumerate(values):
                    datapoints.append(TimeseriesResult.LabeledMetricTimeseries.Datapoint(timestamp=int(times[it]),
                                                                                         value=DoubleValue(
                                                                                             value=val)))

                labeled_metric_timeseries.append(
                    TimeseriesResult.LabeledMetricTimeseries(metric_label_values=metric_labels,
                                                             unit=StringValue(value=unit), datapoints=datapoints))

            # Get offset values if specified
            if timeseries_offsets:
                offsets = [offset for offset in timeseries_offsets]
                for offset in offsets:
                    adjusted_start_time = TimeRange(
                        time_geq=time_range.time_geq - offset,
                        time_lt=time_range.time_lt - offset
                    )
                    offset_results = dd_api_processor.fetch_metric_timeseries(adjusted_start_time, specific_metric,
                                                                              interval=interval * 1000)
                    if not offset_results:
                        print(f"No data returned from Datadog for offset {offset} seconds")
                        continue

                    for itr, item in enumerate(offset_results.series.value):
                        group_tags = item.group_tags.value
                        metric_labels: [LabelValuePair] = []
                        if item.unit:
                            unit = item.unit[0].name
                        else:
                            unit = ''
                        for gt in group_tags:
                            metric_labels.append(
                                LabelValuePair(name=StringValue(value='resource_name'), value=StringValue(value=gt)))

                        metric_labels.append(
                            LabelValuePair(name=StringValue(value='offset_seconds'),
                                           value=StringValue(value=str(offset)))
                        )

                        times = offset_results.times.value
                        values = offset_results.values.value[itr].value
                        datapoints: [TimeseriesResult.LabeledMetricTimeseries.Datapoint] = []
                        for it, val in enumerate(values):
                            datapoints.append(
                                TimeseriesResult.LabeledMetricTimeseries.Datapoint(timestamp=int(times[it]),
                                                                                   value=DoubleValue(
                                                                                       value=val)))

                        labeled_metric_timeseries.append(
                            TimeseriesResult.LabeledMetricTimeseries(metric_label_values=metric_labels,
                                                                     unit=StringValue(value=unit),
                                                                     datapoints=datapoints))

            timeseries_result = TimeseriesResult(metric_expression=StringValue(value=metric),
                                                 metric_name=StringValue(value=service_name),
                                                 labeled_metric_timeseries=labeled_metric_timeseries)

            # Extract Datadog API domain and create metadata
            api_domain = self._extract_api_domain_from_connector(datadog_connector)
            task_params = {
                'service_name': service_name,
                'environment_name': env_name
            }
            metadata = self._create_metadata_with_datadog_url(api_domain, "service_metric_execution", task_params)

            task_result = PlaybookTaskResult(
                type=PlaybookTaskResultType.TIMESERIES,
                timeseries=timeseries_result,
                source=self.source,
                metadata=metadata)
            return task_result
        except Exception as e:
            raise Exception(f"Error while executing Datadog task: {e}")

    
    def execute_log_query_execution(self, time_range: TimeRange, dd_task: Datadog,
                                    datadog_connector: ConnectorProto):
        try:
            if not datadog_connector:
                raise Exception("Task execution Failed:: No Datadog source found")
            task = dd_task.log_query_execution
            query = task.query.value
            limit = task.limit.value
            dd_api_processor = self.get_connector_processor(datadog_connector)
            print(
                "Playbook Task Downstream Request: Type -> {}, Account -> {}, Time Range -> {}, Query -> "
                "{}".format("Datadog", datadog_connector.account_id.value, time_range, query), flush=True)
            current_results = dd_api_processor.fetch_logs(query, time_range, limit)
            if not current_results:
                # Extract Datadog API domain and create metadata
                api_domain = self._extract_api_domain_from_connector(datadog_connector)
                metadata = self._create_metadata_with_datadog_url(api_domain, "log_query_execution")
                
                return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT, text=TextResult(output=StringValue(
                    value=f"No logs returned from Datadog for query: {query}")), source=self.source, metadata=metadata)
            table_rows: [TableResult.TableRow] = []
            for item in current_results:
                # Create a list to hold the columns for the current row
                table_columns: [TableResult.TableColumn] = []

                # Extracting basic fields with default values if not present
                table_columns.append(TableResult.TableColumn(name=StringValue(value='id'),
                                                             value=StringValue(value=item['id'])))
                timestamp_value = item['attributes'].get('timestamp')
                if isinstance(timestamp_value, datetime):
                    timestamp_value = str(timestamp_value.timestamp())
                else:
                    timestamp_value = ''
                table_columns.append(TableResult.TableColumn(name=StringValue(value='timestamp'),
                                                             value=StringValue(
                                                                 value=timestamp_value)))
                table_columns.append(TableResult.TableColumn(name=StringValue(value='service'),
                                                             value=StringValue(
                                                                 value=item['attributes'].get('service', ''))))
                table_columns.append(TableResult.TableColumn(name=StringValue(value='host'),
                                                             value=StringValue(
                                                                 value=item['attributes'].get('host', ''))))
                table_columns.append(TableResult.TableColumn(name=StringValue(value='status'),
                                                             value=StringValue(
                                                                 value=item['attributes'].get('status', ''))))
                table_columns.append(TableResult.TableColumn(name=StringValue(value='message'),
                                                             value=StringValue(
                                                                 value=item['attributes'].get('message', ''))))

                # Stringify tags as a JSON array
                tags = item['attributes'].get('tags', [])
                tags_string = json.dumps(tags) if tags else json.dumps([])  # Stringify the list
                table_columns.append(TableResult.TableColumn(name=StringValue(value='tags'),
                                                             value=StringValue(value=tags_string)))

                # Stringify attributes as a JSON object
                attributes = item['attributes'].get('attributes', {})
                for key, value in attributes.items():
                    if isinstance(value, datetime):
                        attributes[key] = str(value.timestamp())
                attributes_string = json.dumps(attributes) if attributes else json.dumps({})  # Stringify the dict
                table_columns.append(TableResult.TableColumn(name=StringValue(value='attributes'),
                                                             value=StringValue(value=attributes_string)))

                # Create a new TableRow with the populated columns
                table_row = TableResult.TableRow(columns=table_columns)
                table_rows.append(table_row)

            result = TableResult(
                raw_query=StringValue(value=f"Execute ```{query}```"),
                rows=table_rows,
                total_count=UInt64Value(value=len(table_rows)),
            )

            # Extract Datadog API domain and create metadata
            api_domain = self._extract_api_domain_from_connector(datadog_connector)
            metadata = self._create_metadata_with_datadog_url(api_domain, "log_query_execution")

            task_result = PlaybookTaskResult(type=PlaybookTaskResultType.LOGS, logs=result, source=self.source, metadata=metadata)
            return task_result
        except Exception as e:
            raise Exception(f"Error while executing Datadog Log task: {e}")

    def execute_dashboard_multiple_widgets(self, 
                                           time_range: TimeRange, 
                                           dd_task: Datadog,
                                           datadog_connector: ConnectorProto,
                                           widget_id: str,
                                           dashboard_entity=None) -> PlaybookTaskResult:
        """
        Execute a task to fetch metrics from multiple widgets in a Datadog dashboard.
        
        Args:
            time_range: The time range to fetch metrics for
            dd_task: The Datadog task containing dashboard and widget information
            datadog_connector: The Datadog connector to use
            widget_id: The ID of the widget to fetch
            dashboard_entity: Optional pre-fetched dashboard entity to avoid duplicate asset lookup
            
        Returns:
            A PlaybookTaskResult containing timeseries data
        """
        try:
            if not datadog_connector:
                raise Exception("Task execution Failed:: No Datadog source found")

            task = dd_task.dashboard_multiple_widgets
            dashboard_name = task.dashboard_name.value
            template_variables = task.template_variables.value
            if task.unit and task.unit.value:
                unit = task.unit.value
            else:
                unit = ''
                
            interval = task.interval.value if task.interval else 300

            # Get the Datadog API processor
            dd_api_processor = self.get_connector_processor(datadog_connector)
            
            # Find the dashboard with the specified name
            dashboard_id = None
            widget_definition = None
            widget_title = None
            template_variables_map = {}  # Map to store template variable names to default values
            
            # If dashboard_entity is not provided, fetch it from assets
            if dashboard_entity is None:
                # Get all dashboard entities
                client = PrototypeClient()
                assets = client.get_connector_assets(
                    connector_type=Source.Name(datadog_connector.type),
                    connector_id=str(datadog_connector.id.value),
                    asset_type=SourceModelType.DATADOG_DASHBOARD,
                )
                
                if not assets or not assets.HasField('datadog') or not assets.datadog.assets:
                    return [PlaybookTaskResult(type=PlaybookTaskResultType.TEXT,
                                             text=TextResult(output=StringValue(
                                             value=f"No dashboard assets found for the account")),
                                             source=self.source)]
                
                # The datadog assets are at assets.datadog.assets
                dd_assets: [DatadogDashboardModel] = assets.datadog.assets
                all_dashboard_asset: [DatadogDashboardModel] = [dd_asset.datadog_dashboard for dd_asset in dd_assets if
                                                               dd_asset.type == SourceModelType.DATADOG_DASHBOARD]
                for dashboard_entity_item in all_dashboard_asset:
                    if is_partial_match(dashboard_entity_item.title.value, [dashboard_name]):
                        dashboard_entity = dashboard_entity_item
                        break
            
            # Process the dashboard entity if found
            if dashboard_entity:
                dashboard_id = dashboard_entity.id.value
                
                # Parse template_variables JSON string into a dictionary
                user_template_variables = {}
                if template_variables:
                    try:
                        user_template_variables = json.loads(template_variables)
                    except json.JSONDecodeError:
                        logger.warning(f"Failed to parse template_variables JSON: {template_variables}")

                # Extract template variables and create a mapping of variable names to their properties (value and prefix)
                if dashboard_entity.template_variables:
                    for template_var_struct in dashboard_entity.template_variables:
                        template_var = proto_to_dict(template_var_struct)
                        if 'name' in template_var:
                            var_name = template_var['name']
                            var_info = {
                                'prefix': template_var.get('prefix', ''),  # Get prefix or empty string if not present
                                'value': ''
                            }
                            
                            # Set the value - either from user input or default
                            if var_name in user_template_variables:
                                var_info['value'] = user_template_variables[var_name]
                            elif 'default' in template_var:
                                var_info['value'] = template_var['default']
                            
                            template_variables_map[var_name] = var_info
                
                # Search through all panels and widgets to find the specified widget ID
                for panel in dashboard_entity.panels:
                    for widget in panel.widgets:
                        if str(widget.id.value) == widget_id:
                            # Extract the full widget definition including the nested 'definition' field
                            widget_definition = proto_to_dict(widget)
                            widget_title = widget_definition.get('title', '') or f"Widget {widget_id}"
                            break
            
            if not dashboard_id:
                # Extract Datadog API domain and create metadata
                api_domain = self._extract_api_domain_from_connector(datadog_connector)
                metadata = self._create_metadata_with_datadog_url(api_domain, "dashboard_multiple_widgets")
                
                return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT,
                                         text=TextResult(output=StringValue(
                                         value=f"Dashboard with name '{dashboard_name}' not found")),
                                         source=self.source,
                                         metadata=metadata)
            
            if not widget_definition:
                # Extract Datadog API domain and create metadata
                api_domain = self._extract_api_domain_from_connector(datadog_connector)
                task_params = {
                    'dashboard_id': dashboard_id
                }
                metadata = self._create_metadata_with_datadog_url(api_domain, "dashboard_multiple_widgets", task_params)
                
                return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT,
                                         text=TextResult(output=StringValue(
                                         value=f"Widget with ID '{widget_id}' not found in dashboard '{dashboard_name}'")),
                                         source=self.source,
                                         metadata=metadata)
            
            # Get the widget type
            widget_type = widget_definition.get('widget_type', 'timeseries')
            resource_type = widget_definition.get('response_type', 'timeseries')
            queries = widget_definition.get('queries', [])
            formulas = widget_definition.get('formulas', [])
                            
            # Process queries to replace template variables with their default values
            if queries:
                for query in queries:
                    if "query" in query:
                            # Handle all template variables
                            for var_name, var_info in template_variables_map.items():
                                if f"${var_name}" in query["query"]:
                                    if var_name in user_template_variables:
                                        query["query"] = query["query"].replace(f"${var_name}", f"{var_info['prefix']}:{var_info['value']}")
                                    else:
                                        query["query"] = query["query"].replace(f"${var_name}", f"{var_info['value']}")
                        
            # For timeseries metrics, always use the specified interval (default 5 minutes)
            interval_ms = interval * 1000
            
            # For non-timeseries data, never pass the interval parameter
            # For timeseries data, only pass it if explicitly provided by the user
            if resource_type == 'timeseries' and interval != 300:  # If it's not the default value
                response = dd_api_processor.widget_query_timeseries_points_api(
                    time_range, 
                    queries, 
                    formulas, 
                    resource_type=resource_type,
                    interval_ms=interval_ms
                )
            elif resource_type == 'event_list':
                query_string = queries[0].get('query', '')
                response = dd_api_processor.widget_query_logs_stream_api(
                    time_range, 
                    query_string
                )
            else:
                # Let the Datadog API determine the appropriate interval based on the time range
                response = dd_api_processor.widget_query_timeseries_points_api(
                    time_range,
                    queries, 
                    formulas, 
                    resource_type=resource_type
                )

            if not response:
                # Extract Datadog API domain and create metadata
                api_domain = self._extract_api_domain_from_connector(datadog_connector)
                task_params = {
                    'dashboard_id': dashboard_id
                }
                metadata = self._create_metadata_with_datadog_url(api_domain, "dashboard_multiple_widgets", task_params)
                
                return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT,
                                        text=TextResult(output=StringValue(
                                        value=f"No data returned from Datadog for widget ID '{widget_id}' in dashboard '{dashboard_name}'")),
                                        widget_id=StringValue(value=widget_id),
                                        source=self.source,
                                        metadata=metadata)

            # Process the response based on resource_type
            # For event_list resource type (log streams), return logs in a table format
            if resource_type == 'event_list':
                if 'data' in response:
                    # Extract log entries from the response
                    log_entries = response.get('data', [])
                    
                    if not log_entries or not isinstance(log_entries, list):
                        # Extract Datadog API domain and create metadata
                        api_domain = self._extract_api_domain_from_connector(datadog_connector)
                        task_params = {
                            'dashboard_id': dashboard_id
                        }
                        metadata = self._create_metadata_with_datadog_url(api_domain, "dashboard_multiple_widgets", task_params)
                        
                        return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT,
                                                text=TextResult(output=StringValue(
                                                value=f"No log entries found in response for widget ID '{widget_id}' in dashboard '{dashboard_name}'")),
                                                widget_id=StringValue(value=widget_id),
                                                source=self.source,
                                                metadata=metadata)
                    
                    # Create table rows for each log entry
                    table_rows: [TableResult.TableRow] = []
                    
                    for log_entry in log_entries:
                        # Create a list to hold the columns for the current row
                        table_columns: [TableResult.TableColumn] = []
                        
                        # Extract log ID
                        log_id = log_entry.get('id', '')
                        table_columns.append(TableResult.TableColumn(
                            name=StringValue(value='id'),
                            value=StringValue(value=log_id)
                        ))
                        
                        # Extract log type
                        log_type = log_entry.get('type', '')
                        table_columns.append(TableResult.TableColumn(
                            name=StringValue(value='type'),
                            value=StringValue(value=log_type)
                        ))
                        
                        # Extract attributes
                        attributes = log_entry.get('attributes', {})
                        
                        # Extract common fields from attributes
                        service = attributes.get('service', '')
                        table_columns.append(TableResult.TableColumn(
                            name=StringValue(value='service'),
                            value=StringValue(value=service)
                        ))
                        
                        host = attributes.get('host', '')
                        table_columns.append(TableResult.TableColumn(
                            name=StringValue(value='host'),
                            value=StringValue(value=host)
                        ))
                        
                        message = attributes.get('message', '')
                        table_columns.append(TableResult.TableColumn(
                            name=StringValue(value='message'),
                            value=StringValue(value=message)
                        ))
                        
                        status = attributes.get('status', '')
                        table_columns.append(TableResult.TableColumn(
                            name=StringValue(value='status'),
                            value=StringValue(value=status)
                        ))
                        
                        # Handle timestamp
                        timestamp = attributes.get('timestamp', '')
                        if isinstance(timestamp, datetime):
                            timestamp = str(timestamp.timestamp())
                        table_columns.append(TableResult.TableColumn(
                            name=StringValue(value='timestamp'),
                            value=StringValue(value=str(timestamp))
                        ))
                        
                        # Handle tags as JSON string
                        tags = attributes.get('tags', [])
                        tags_string = json.dumps(tags) if tags else json.dumps([])
                        table_columns.append(TableResult.TableColumn(
                            name=StringValue(value='tags'),
                            value=StringValue(value=tags_string)
                        ))
                        
                        # Create a new TableRow with the populated columns
                        table_row = TableResult.TableRow(columns=table_columns)
                        table_rows.append(table_row)
                    
                    # Create the table result
                    result = TableResult(
                        raw_query=StringValue(value=f"Dashboard: {dashboard_name}, Widget: {widget_title}"),
                        rows=table_rows,
                        total_count=UInt64Value(value=len(table_rows)),
                    )
                    
                    # Extract Datadog API domain and create metadata
                    api_domain = self._extract_api_domain_from_connector(datadog_connector)
                    task_params = {
                        'dashboard_id': dashboard_id
                    }
                    metadata = self._create_metadata_with_datadog_url(api_domain, "dashboard_multiple_widgets", task_params)
                    
                    # Return the logs result
                    return PlaybookTaskResult(
                        type=PlaybookTaskResultType.LOGS,
                        logs=result,
                        widget_id=StringValue(value=widget_id),
                        source=self.source,
                        metadata=metadata
                    )
            
            # Handle scalar responses as logs with name/value columns
            if resource_type == 'scalar':
                if 'data' in response and 'attributes' in response['data']:
                    attributes = response['data']['attributes']
                    
                    # Create table rows for scalar values
                    table_rows: [TableResult.TableRow] = []
                    
                    # Handle scalar response with columns
                    if 'columns' in attributes:
                        columns = attributes['columns']
                        
                        for column in columns:
                            column_name = column.get('name', '')
                            column_type = column.get('type', '')
                            column_values = column.get('values', [])
                            
                            # Skip empty values
                            if not column_values:
                                continue
                            
                            # Get the first value (scalar responses typically have a single value per column)
                            value = column_values[0]
                            
                            # Handle group type columns (like "service")
                            if column_type == 'group' and isinstance(value, list):
                                for group_value in value:
                                    table_columns = [
                                        TableResult.TableColumn(
                                            name=StringValue(value='name'),
                                            value=StringValue(value=column_name)
                                        ),
                                        TableResult.TableColumn(
                                            name=StringValue(value='value'),
                                            value=StringValue(value=str(group_value))
                                        )
                                    ]
                                    table_rows.append(TableResult.TableRow(columns=table_columns))
                            # Handle number type columns with units
                            elif column_type == 'number':
                                # Format value with unit if available
                                formatted_value = str(value)
                                
                                # Check for unit information
                                meta = column.get('meta', {})
                                unit_info = meta.get('unit', [])
                                
                                if unit_info and unit_info[0]:
                                    unit_data = unit_info[0]
                                    short_name = unit_data.get('short_name', '')
                                    
                                    if short_name:
                                        formatted_value = f"{value}{short_name}"
                                
                                table_columns = [
                                    TableResult.TableColumn(
                                        name=StringValue(value='name'),
                                        value=StringValue(value=column_name)
                                    ),
                                    TableResult.TableColumn(
                                        name=StringValue(value='value'),
                                        value=StringValue(value=formatted_value)
                                    )
                                ]
                                table_rows.append(TableResult.TableRow(columns=table_columns))
                            # Handle other types
                            else:
                                table_columns = [
                                    TableResult.TableColumn(
                                        name=StringValue(value='name'),
                                        value=StringValue(value=column_name)
                                    ),
                                    TableResult.TableColumn(
                                        name=StringValue(value='value'),
                                        value=StringValue(value=str(value))
                                    )
                                ]
                                table_rows.append(TableResult.TableRow(columns=table_columns))
                    
                    # Handle multiple values scalar response
                    elif 'values' in attributes:
                        values = attributes['values']
                        groups = attributes.get('groups', [])
                        
                        for idx, group in enumerate(groups):
                            if idx < len(values):
                                group_value = values[idx]
                                group_by_values = group.get('by', {})
                                
                                # Format value with unit if available
                                formatted_value = str(group_value)
                                if unit:
                                    formatted_value = f"{group_value} {unit}"
                                
                                # Use the first group_by value as the name, or 'value' if none
                                name = 'value'
                                if group_by_values:
                                    first_key = next(iter(group_by_values))
                                    name = f"{first_key}: {group_by_values[first_key]}"
                                
                                table_columns = [
                                    TableResult.TableColumn(
                                        name=StringValue(value='name'),
                                        value=StringValue(value=name)
                                    ),
                                    TableResult.TableColumn(
                                        name=StringValue(value='value'),
                                        value=StringValue(value=formatted_value)
                                    )
                                ]
                                table_rows.append(TableResult.TableRow(columns=table_columns))
                    
                    # Create the table result if we have rows
                    if table_rows:
                        result = TableResult(
                            raw_query=StringValue(value=f"Dashboard: {dashboard_name}, Widget: {widget_title}"),
                            rows=table_rows,
                            total_count=UInt64Value(value=len(table_rows)),
                        )
                        
                        # Extract Datadog API domain and create metadata
                        api_domain = self._extract_api_domain_from_connector(datadog_connector)
                        task_params = {
                            'dashboard_id': dashboard_id
                        }
                        metadata = self._create_metadata_with_datadog_url(api_domain, "dashboard_multiple_widgets", task_params)
                        
                        # Return the logs result
                        return PlaybookTaskResult(
                            type=PlaybookTaskResultType.LOGS,
                            logs=result,
                            widget_id=StringValue(value=widget_id),
                            source=self.source,
                            metadata=metadata
                        )
                    
                    # Extract Datadog API domain and create metadata
                    api_domain = self._extract_api_domain_from_connector(datadog_connector)
                    task_params = {
                        'dashboard_id': dashboard_id
                    }
                    metadata = self._create_metadata_with_datadog_url(api_domain, "dashboard_multiple_widgets", task_params)
                    
                    # If we couldn't extract any rows, return an error message
                    return PlaybookTaskResult(
                        type=PlaybookTaskResultType.TEXT,
                        text=TextResult(output=StringValue(
                            value=f"Could not extract scalar values from response for widget ID '{widget_id}' in dashboard '{dashboard_name}'"
                        )),
                        widget_id=StringValue(value=widget_id),
                        source=self.source,
                        metadata=metadata
                    )
            
            # Process the response into labeled metric timeseries for other resource types
            labeled_metric_timeseries_list = []
            
            # For timeseries responses (not scalar or event_list)
            if resource_type != 'scalar' and resource_type != 'event_list':
                if 'data' in response and 'attributes' in response['data']:
                    attributes = response['data']['attributes']
                    
                    if 'series' in attributes and 'values' in attributes:
                        series = attributes['series']
                        values = attributes['values']
                        times = attributes.get('times', [])
                    
                        # Process each series
                        for idx, series_item in enumerate(series):
                            if idx < len(values):
                                series_values = values[idx]
                                group_tags = series_item.get('group_tags', ['default'])
                                
                                # Create datapoints
                                datapoints = []
                                for time_idx, timestamp in enumerate(times):
                                    if time_idx < len(series_values):
                                        value = series_values[time_idx]
                                        datapoint = TimeseriesResult.LabeledMetricTimeseries.Datapoint(
                                            timestamp=int(timestamp),
                                            value=DoubleValue(value=value)
                                        )
                                        datapoints.append(datapoint)
                                
                                # Create metric labels
                                metric_labels = [
                                    LabelValuePair(
                                        name=StringValue(value='widget_name'), 
                                        value=StringValue(value=widget_title)
                                    ),
                                    LabelValuePair(
                                        name=StringValue(value='offset_seconds'), 
                                        value=StringValue(value='0')
                                    )
                                ]
                                
                                # Add tag information
                                for tag in group_tags:
                                    metric_labels.append(
                                        LabelValuePair(
                                            name=StringValue(value='tag'), 
                                            value=StringValue(value=tag)
                                        )
                                    )
                                
                                # Add to the list
                                labeled_metric_timeseries_list.append(
                                    TimeseriesResult.LabeledMetricTimeseries(
                                        metric_label_values=metric_labels,
                                        unit=StringValue(value=unit),
                                        datapoints=datapoints
                                    )
                                )

            # Check if we have any data points
            if not labeled_metric_timeseries_list:
                error_msg = f"No data points could be extracted from the response for widget ID '{widget_id}' (type: {widget_type}) in dashboard '{dashboard_name}' using resource_type '{resource_type}'"
                logger.error(error_msg)
                # Return empty timeseries instead of error text
                timeseries_result = TimeseriesResult(
                    metric_expression=StringValue(value=json.dumps(queries)),
                    metric_name=StringValue(value=widget_title),
                    labeled_metric_timeseries=[]  # Empty list
                )
                
                # Extract Datadog API domain and create metadata
                api_domain = self._extract_api_domain_from_connector(datadog_connector)
                task_params = {
                    'dashboard_id': dashboard_id
                }
                metadata = self._create_metadata_with_datadog_url(api_domain, "dashboard_multiple_widgets", task_params)

                task_result = PlaybookTaskResult(
                    type=PlaybookTaskResultType.TIMESERIES,
                    timeseries=timeseries_result,
                    widget_id=StringValue(value=widget_id),
                    source=self.source,
                    metadata=metadata
                )
                return task_result
            
            # Create the final result
            timeseries_result = TimeseriesResult(
                metric_expression=StringValue(value=json.dumps(queries)),
                metric_name=StringValue(value=widget_title),
                labeled_metric_timeseries=labeled_metric_timeseries_list
            )
            
            # Extract Datadog API domain and create metadata
            api_domain = self._extract_api_domain_from_connector(datadog_connector)
            task_params = {
                'dashboard_id': dashboard_id
            }
            metadata = self._create_metadata_with_datadog_url(api_domain, "dashboard_multiple_widgets", task_params)

            task_result = PlaybookTaskResult(
                type=PlaybookTaskResultType.TIMESERIES,
                timeseries=timeseries_result,
                widget_id=StringValue(value=widget_id),
                source=self.source,
                metadata=metadata
            )
            
            return task_result
        except Exception as e:
            logger.error(f"Error while executing Dashboard Widget by Name/ID task: {str(e)}")
            raise Exception(f"Error while executing Datadog task: {e}")



    def execute_dashboard_multiple_widgets_task(self, time_range: TimeRange, dd_task: Datadog,
                                          datadog_connector: ConnectorProto):
        """
        Task executor that extracts parameters from dashboard_multiple_widgets task and delegates to execute_dashboard_multiple_widgets
        
        Args:
            time_range: The time range to fetch metrics for
            dd_task: The Datadog task containing dashboard and widget information
            datadog_connector: The Datadog connector to use
            
        Returns:
            A PlaybookTaskResult containing timeseries data
        """
        # Extract all task parameters
        task = dd_task.dashboard_multiple_widgets
        widget_id = task.widget_id.value

        if widget_id:
            result = []
            response = self.execute_dashboard_multiple_widgets(
                time_range=time_range,
                dd_task=dd_task,
                widget_id=widget_id,
                datadog_connector=datadog_connector)
            result.append(response)
            return result
        
        else:
            result = []
            dashboard_name = task.dashboard_name.value
            
            # Fetch dashboard assets once
            client = PrototypeClient()
            assets = client.get_connector_assets(
                connector_type=Source.Name(datadog_connector.type),
                connector_id=str(datadog_connector.id.value),
                asset_type=SourceModelType.DATADOG_DASHBOARD,
            )
                
            if not assets or not assets.HasField('datadog') or not assets.datadog.assets:
                # Extract Datadog API domain and create metadata
                api_domain = self._extract_api_domain_from_connector(datadog_connector)
                metadata = self._create_metadata_with_datadog_url(api_domain, "dashboard_multiple_widgets")
                
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value="No dashboard assets found for the account")),
                    source=self.source,
                    metadata=metadata)
                    
            # The datadog assets are at assets.datadog.assets
            dd_assets: [DatadogDashboardModel] = assets.datadog.assets
            all_dashboard_asset: [DatadogDashboardModel] = [dd_asset.datadog_dashboard for dd_asset in dd_assets if 
                                                            dd_asset.type == SourceModelType.DATADOG_DASHBOARD]
            
            # Find the matching dashboard
            matching_dashboard = None
            for dashboard in all_dashboard_asset:
                if is_partial_match(dashboard.title.value, [dashboard_name]):
                    matching_dashboard = dashboard
                    break
                    
            if matching_dashboard:
                # Process all widgets in the matching dashboard
                for panel in matching_dashboard.panels:
                    for widget in panel.widgets:
                        widget_def = proto_to_dict(widget)
                        if widget_def.get("widget_type") != "note":
                            widget_id = str(widget.id.value)
                            response = self.execute_dashboard_multiple_widgets(
                                time_range=time_range,
                                dd_task=dd_task,
                                datadog_connector=datadog_connector,
                                widget_id=widget_id,
                                dashboard_entity=matching_dashboard)
                            result.append(response)
            else:
                # Extract Datadog API domain and create metadata
                api_domain = self._extract_api_domain_from_connector(datadog_connector)
                metadata = self._create_metadata_with_datadog_url(api_domain, "dashboard_multiple_widgets")
                
                return [PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"Dashboard with name '{dashboard_name}' not found")),
                    source=self.source,
                    metadata=metadata)]
            return result

    def filter_using_assets(self, dd_connector: ConnectorProto, service_name, filters: dict = None):
        try:
            client = PrototypeClient()
            assets: AccountConnectorAssets = client.get_connector_assets(
                connector_type=Source.Name(dd_connector.type),
                connector_id=str(dd_connector.id.value),
                asset_type=SourceModelType.DATADOG_SERVICE,
            )
            
            if not assets or not assets.HasField('datadog') or not assets.datadog.assets:
                logger.warning(f"DatadogSourceManager.query_dashboard_widget_ids_asset_descriptor:: No assets "
                               f"found for account: {dd_connector.account_id.value}, connector: {dd_connector.id.value}")
                return "[]", []
                
            # The datadog assets are at assets.datadog.assets
            dd_assets: [DatadogServiceAssetModel] = assets.datadog.assets
            all_service_asset: [DatadogServiceAssetModel] = [dd_asset.datadog_service for dd_asset in dd_assets if
                                                           dd_asset.type == SourceModelType.DATADOG_SERVICE]
  
            matching_metrics = []
            environments = []
            # Iterate through each service asset
            for service_asset in all_service_asset:
                # Check if this is the service we're looking for
                if service_asset.service_name.value == service_name:
                    # Extract environments from the service asset
                    for env in service_asset.environments:
                        environments.append(env)
                    
                    # Iterate through each metric in the service asset
                    for metric in service_asset.metrics:
                        # Check if the metric family is "trace"
                        if metric.metric_family.value == "trace":
                            # Check if the metric has a tag with "service:service_name"
                            service_tag_found = False
                            for tag in metric.tags:
                                if tag.value == f"service:{service_name}":
                                    service_tag_found = True
                                    break
                            
                            # If both conditions are met, add the metric value to our results
                            if service_tag_found:
                                matching_metrics.append(metric.metric.value)
            return matching_metrics, environments
        except Exception as e:
            logger.error(f"Error while accessing assets: {dd_connector.account_id.value}, connector: "
                     f"{dd_connector.id.value} with error: {e}")
            return None, None


    def execute_apm_queries(self, time_range: TimeRange, dd_task: Datadog, datadog_connector: ConnectorProto):
        
        task = dd_task.apm_query
        service_name = task.service_name.value
        
        # Extract metric_families parameter (optional, defaults to empty list)
        # Parse comma-separated string into list
        metric_families = []
        if task.metric_families and task.metric_families.value:
            metric_families = [f.strip() for f in task.metric_families.value.split(',') if f.strip()]

        # Extract environments parameter (optional, defaults to empty list)
        # Parse comma-separated string into list
        task_environments = []
        if task.environments and task.environments.value:
            task_environments = [e.strip() for e in task.environments.value.split(',') if e.strip()]

        dd_api_processor = self.get_connector_processor(datadog_connector)

        start_time = time_range.time_geq
        end_time = time_range.time_lt
        interval = task.interval.value if task.interval else 300
        interval = interval * 1000 # Convert to milliseconds
        matching_metrics, environments = self.filter_using_assets(datadog_connector, filters=None, service_name=service_name)
        
        # Match environments from task with environments from filter_using_assets
        query_env = "prod"  # Default environment
        if task_environments and environments:
            # Find the first matching environment
            for task_env in task_environments:
                if task_env in environments:
                    query_env = task_env
                    break
        elif environments and "production" in [env_val for env_val in environments]:
            # Fallback to old behavior if no task environments specified
            query_env = "production"
        
        if not matching_metrics:
            # Extract Datadog API domain and create metadata
            api_domain = self._extract_api_domain_from_connector(datadog_connector)
            task_params = {
                'service_name': service_name,
                'environment_name': query_env
            }
            metadata = self._create_metadata_with_datadog_url(api_domain, "apm_query", task_params)
            
            return [PlaybookTaskResult(type=PlaybookTaskResultType.TEXT, 
                                      text=TextResult(output=StringValue(value=f"No APM metrics found for service with name: {service_name}")), 
                                      source=self.source)]
        
        # Filter matching_metrics by metric_families if specified
        if metric_families:
            # Only keep metrics that belong to the specified metric families
            filtered_metrics = []
            for metric in matching_metrics:
                parts = metric.split('.')
                # Check if this metric belongs to any of the specified families
                for family in metric_families:
                    if metric.startswith(family):
                        filtered_metrics.append(metric)
                        break
            matching_metrics = filtered_metrics
            
            if not matching_metrics:
                return [PlaybookTaskResult(type=PlaybookTaskResultType.TEXT, 
                                          text=TextResult(output=StringValue(value=f"No metrics found for the specified metric families: {metric_families}")), 
                                          source=self.source)]
        
        response = dd_api_processor.fetch_query_results(service_name=service_name, env=query_env, start_time=start_time, end_time=end_time, matching_metrics=matching_metrics, interval=interval)
        
        if not response:
            # Extract Datadog API domain and create metadata
            api_domain = self._extract_api_domain_from_connector(datadog_connector)
            task_params = {
                'service_name': service_name,
                'environment_name': query_env
            }
            metadata = self._create_metadata_with_datadog_url(api_domain, "apm_query", task_params)
            
            return [PlaybookTaskResult(type=PlaybookTaskResultType.TEXT, 
                                      text=TextResult(output=StringValue(value=f"No data returned from Datadog for service: {service_name}")), 
                                      source=self.source)]

        # Process each dictionary in the response list
        result = []
        for item in response:
            # Create a single timeseries result for this dictionary with multiple lines
            labeled_metric_timeseries_list = []
            
            # Process each key-value pair in this dictionary
            for key, val in item.items():
                # Create a fresh datapoints list for each key-value pair
                datapoints = []
                series = val.get('series', [])
                if not series:
                    error_msg = f"No data returned from Datadog for query: {key}"
                    logger.error(error_msg)

                    # Return empty timeseries instead of error text
                    metric_labels = [
                    LabelValuePair(
                        name=StringValue(value='query'), 
                        value=StringValue(value=key)
                    )
                    ]

                    labeled_metric_timeseries = TimeseriesResult.LabeledMetricTimeseries(
                        metric_label_values=metric_labels,
                        datapoints=datapoints
                    )

                    timeseries_result = TimeseriesResult(
                        metric_expression=StringValue(value=json.dumps(key)),
                        metric_name=StringValue(value=service_name),
                        labeled_metric_timeseries=[]  # Empty list
                    )
                    
                    task_result = PlaybookTaskResult(
                        type=PlaybookTaskResultType.TIMESERIES,
                        timeseries=timeseries_result,
                        source=self.source
                    )
                    result.append(task_result)

                    continue
            
                # Extract the data points from the series
                data = series[0].get('pointlist', [])
                for datapoint in data:
                    timestamp = int(datapoint[0])
                    value = float(datapoint[1])
                    datapoint = TimeseriesResult.LabeledMetricTimeseries.Datapoint(
                                                timestamp=int(timestamp),
                                                value=DoubleValue(value=value))
                    datapoints.append(datapoint)
                
                # Create metric labels for this key-value pair
                metric_labels = [
                    LabelValuePair(
                        name=StringValue(value='query'), 
                        value=StringValue(value=key)
                    )
                ]

                # Create a labeled metric timeseries for this key-value pair
                labeled_metric_timeseries = TimeseriesResult.LabeledMetricTimeseries(
                    metric_label_values=metric_labels,
                    datapoints=datapoints,
                    unit=StringValue(value='seconds')  # Assuming unit is milliseconds
                )
                
                # Add to the list for this dictionary
                labeled_metric_timeseries_list.append(labeled_metric_timeseries)
            
            # If we have any timeseries data for this dictionary
            if labeled_metric_timeseries_list:
                # Create the final result
                if "error" in key.lower() or "errors" in key.lower():
                    timeseries_result = TimeseriesResult(
                        metric_expression=StringValue(value=json.dumps(key)),
                        metric_name=StringValue(value=service_name),
                        chart_type=StringValue(value="point_chart"),
                        labeled_metric_timeseries=labeled_metric_timeseries_list
                    )
                else:
                    timeseries_result = TimeseriesResult(
                        metric_expression=StringValue(value=json.dumps(key)),
                        metric_name=StringValue(value=service_name),
                        labeled_metric_timeseries=labeled_metric_timeseries_list
                    )
                
                # Extract Datadog API domain and create metadata
                api_domain = self._extract_api_domain_from_connector(datadog_connector)
                task_params = {
                    'service_name': service_name,
                    'environment_name': query_env
                }
                metadata = self._create_metadata_with_datadog_url(api_domain, "apm_query", task_params)

                # Create a single task result for this dictionary
                task_result = PlaybookTaskResult(
                    type=PlaybookTaskResultType.TIMESERIES,
                    timeseries=timeseries_result,
                    source=self.source,
                    metadata=metadata
                )
                result.append(task_result)
        return result

    def execute_span_search_execution(self, time_range: TimeRange, dd_task: Datadog,
                                      datadog_connector: ConnectorProto):
        try:
            if not datadog_connector:
                raise Exception("Task execution Failed:: No Datadog source found")

            task = dd_task.span_search_execution

            query = "*"
            if task.HasField("query") and task.query.value:
                query = task.query.value

            limit = 100
            if task.HasField("limit") and task.limit.value:
                limit = int(task.limit.value)

            cursor = None
            if task.HasField("cursor") and task.cursor.value:
                cursor = task.cursor.value

            dd_api_processor = self.get_connector_processor(datadog_connector)

            response = dd_api_processor.search_spans(
                start=time_range.time_geq,
                end=time_range.time_lt,
                query=query,
                cursor=cursor or '',
                limit=limit
            )

            if not response or not isinstance(response, dict):
                logger.warning(f"Datadog span search returned invalid response: {response}")
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"No spans returned from Datadog for query: {query}")),
                    source=self.source
                )

            meta = response.get("meta", {}) if isinstance(response, dict) else {}
            next_cursor = None
            if isinstance(meta, dict):
                page = meta.get("page", {})
                if isinstance(page, dict):
                    next_cursor = page.get("after")

            metadata_kwargs = {}
            if next_cursor:
                metadata_struct = dict_to_proto(
                    {"next_cursor": next_cursor},
                    Struct
                )
                metadata_kwargs["metadata"] = metadata_struct

            response_struct = dict_to_proto(response, Struct)
            api_response_result = ApiResponseResult(
                response_body=response_struct
            )

            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                api_response=api_response_result,
                source=self.source,
                **metadata_kwargs
            )
        except Exception as e:
            logger.error(f"Error while executing Datadog span search task: {e}")
            raise Exception(f"Error while executing Datadog task: {e}")

    def execute_generic_query(self, time_range: TimeRange, dd_task: Datadog, datadog_connector: ConnectorProto):
        """
        Execute a generic Datadog query task.
        
        Args:
            time_range: The time range to fetch metrics for
            dd_task: The Datadog task containing the query
            datadog_connector: The Datadog connector to use
            
        Returns:
            A PlaybookTaskResult containing timeseries data
        """
        try:
            if not datadog_connector:
                raise Exception("Task execution Failed:: No Datadog source found")

            task = dd_task.generic_query
            query = task.query.value
            interval = task.interval.value if task.interval else 300

            dd_api_processor = self.get_connector_processor(datadog_connector)

            # Execute the raw query
            response = dd_api_processor.execute_raw_query(
                start_time=time_range.time_geq,
                end_time=time_range.time_lt,
                query=query,
                interval=interval
            )

            if not response:
                # Extract Datadog API domain and create metadata
                api_domain = self._extract_api_domain_from_connector(datadog_connector)
                metadata = self._create_metadata_with_datadog_url(api_domain, "generic_query")
                
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"No data returned from Datadog for query: {query}")),
                    source=self.source,
                    metadata=metadata
                )

            # Process the response into timeseries format
            labeled_metric_timeseries_list = []
            
            # Extract series from response
            series = response.get('series', [])
            for s in series:
                # Create datapoints
                datapoints = []
                pointlist = s.get('pointlist', [])
                for point in pointlist:
                    if len(point) >= 2:  # Ensure we have both timestamp and value
                        timestamp = int(point[0])  # Timestamp is first element
                        value = float(point[1])    # Value is second element
                        datapoint = TimeseriesResult.LabeledMetricTimeseries.Datapoint(
                            timestamp=timestamp,
                            value=DoubleValue(value=value)
                        )
                        datapoints.append(datapoint)

                # Create metric labels from tags and metadata
                metric_labels = []
                
                # Add scope (tags)
                scope = s.get('scope', '')
                if scope:
                    metric_labels.append(
                        LabelValuePair(
                            name=StringValue(value='scope'),
                            value=StringValue(value=scope)
                        )
                    )
                
                # Add expression
                expression = s.get('expression', '')
                if expression:
                    metric_labels.append(
                        LabelValuePair(
                            name=StringValue(value='expression'),
                            value=StringValue(value=expression)
                        )
                    )

                # Add metric name
                metric_name = s.get('metric', '')
                if metric_name:
                    metric_labels.append(
                        LabelValuePair(
                            name=StringValue(value='metric'),
                            value=StringValue(value=metric_name)
                        )
                    )

                # Get unit if available
                unit = s.get('unit', [{}])[0].get('short_name', '') if s.get('unit') else ''

                # Create the labeled metric timeseries
                labeled_metric_timeseries = TimeseriesResult.LabeledMetricTimeseries(
                    metric_label_values=metric_labels,
                    datapoints=datapoints,
                    unit=StringValue(value=unit)
                )
                labeled_metric_timeseries_list.append(labeled_metric_timeseries)

            if not labeled_metric_timeseries_list:
                # Extract Datadog API domain and create metadata
                api_domain = self._extract_api_domain_from_connector(datadog_connector)
                metadata = self._create_metadata_with_datadog_url(api_domain, "generic_query")
                
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"No data points found for query: {query}")),
                    source=self.source,
                    metadata=metadata
                )

            # Create the final result
            timeseries_result = TimeseriesResult(
                metric_expression=StringValue(value=query),
                metric_name=StringValue(value=query),
                labeled_metric_timeseries=labeled_metric_timeseries_list
            )
            
            # Extract Datadog API domain and create metadata
            api_domain = self._extract_api_domain_from_connector(datadog_connector)
            metadata = self._create_metadata_with_datadog_url(api_domain, "generic_query")

            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TIMESERIES,
                timeseries=timeseries_result,
                source=self.source,
                metadata=metadata
            )

        except Exception as e:
            logger.error(f"Error while executing generic Datadog query: {e}")
            raise Exception(f"Error while executing Datadog task: {e}")

    def execute_get_dashboard_config_details(self, time_range: TimeRange, dd_task: Datadog, datadog_connector: ConnectorProto):
        """
        Execute a task to get dashboard configuration details.
        
        Args:
            time_range: The time range (not used for this task)
            dd_task: The Datadog task containing dashboard config details information
            datadog_connector: The Datadog connector to use
            
        Returns:
            A PlaybookTaskResult containing dashboard configuration details as API response
        """
        try:
            if not datadog_connector:
                raise Exception("Task execution Failed:: No Datadog source found")

            task = dd_task.get_dashboard_config_details
            dashboard_id = task.dashboard_id.value

            dd_api_processor = self.get_connector_processor(datadog_connector)

            # Get dashboard configuration details (already JSON string)
            response_json = dd_api_processor.get_dashboard_config_details(dashboard_id)

            # Import required modules
            from google.protobuf.struct_pb2 import Struct
            import json

            if not response_json:
                error_struct = Struct()
                error_struct.update({"error": f"Dashboard not found: {dashboard_id}"})
                
                # Extract Datadog API domain and create metadata
                api_domain = self._extract_api_domain_from_connector(datadog_connector)
                task_params = {
                    'dashboard_id': dashboard_id
                }
                metadata = self._create_metadata_with_datadog_url(api_domain, "get_dashboard_config_details", task_params)
                
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.API_RESPONSE,
                    api_response=ApiResponseResult(
                        response_status=UInt64Value(value=404),
                        response_body=error_struct
                    ),
                    source=self.source,
                    metadata=metadata
                )

            # Parse JSON string back to dict for Struct
            response_data = json.loads(response_json)
            response_struct = Struct()
            response_struct.update(response_data)

            # Extract Datadog API domain and create metadata
            api_domain = self._extract_api_domain_from_connector(datadog_connector)
            task_params = {
                'dashboard_id': dashboard_id
            }
            metadata = self._create_metadata_with_datadog_url(api_domain, "get_dashboard_config_details", task_params)

            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                api_response=ApiResponseResult(
                    response_status=UInt64Value(value=200),
                    response_body=response_struct
                ),
                source=self.source,
                metadata=metadata
            )

        except Exception as e:
            logger.error(f"Error while executing get dashboard config details task: {e}")
            raise Exception(f"Error while executing Datadog task: {e}")

    def execute_get_dashboard_variable_values(self, time_range: TimeRange, dd_task: Datadog, datadog_connector: ConnectorProto):
        """
        Execute a task to get dashboard variable values.
        
        Args:
            time_range: The time range (not used for this task)
            dd_task: The Datadog task containing dashboard variable values information
            datadog_connector: The Datadog connector to use
            
        Returns:
            A PlaybookTaskResult containing dashboard variable values as API response
        """
        try:
            if not datadog_connector:
                raise Exception("Task execution Failed:: No Datadog source found")

            task = dd_task.get_dashboard_variable_values
            dashboard_id = task.dashboard_id.value
            variable_name = task.variable_name.value if task.variable_name else None

            dd_api_processor = self.get_connector_processor(datadog_connector)

            # Get dashboard variable values (already JSON string)
            response_json = dd_api_processor.get_dashboard_variable_values(dashboard_id, variable_name)

            # Import required modules
            from google.protobuf.struct_pb2 import Struct
            import json

            if not response_json:
                error_struct = Struct()
                error_struct.update({"error": f"Dashboard not found: {dashboard_id}"})
                
                # Extract Datadog API domain and create metadata
                api_domain = self._extract_api_domain_from_connector(datadog_connector)
                task_params = {
                    'dashboard_id': dashboard_id
                }
                metadata = self._create_metadata_with_datadog_url(api_domain, "get_dashboard_variable_values", task_params)
                
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.API_RESPONSE,
                    api_response=ApiResponseResult(
                        response_status=UInt64Value(value=404),
                        response_body=error_struct
                    ),
                    source=self.source,
                    metadata=metadata
                )

            # Parse JSON string back to dict for Struct
            response_data = json.loads(response_json)
            response_struct = Struct()
            response_struct.update(response_data)

            # Extract Datadog API domain and create metadata
            api_domain = self._extract_api_domain_from_connector(datadog_connector)
            task_params = {
                'dashboard_id': dashboard_id
            }
            metadata = self._create_metadata_with_datadog_url(api_domain, "get_dashboard_variable_values", task_params)

            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                api_response=ApiResponseResult(
                    response_status=UInt64Value(value=200),
                    response_body=response_struct
                ),
                source=self.source,
                metadata=metadata
            )

        except Exception as e:
            logger.error(f"Error while executing get dashboard variable values task: {e}")
            raise Exception(f"Error while executing Datadog task: {e}")

    def execute_fetch_dashboards(self, time_range: TimeRange, dd_task: Datadog, datadog_connector: ConnectorProto):
        """
        Execute a task to fetch all dashboards.
        
        Args:
            time_range: The time range (not used for this task)
            dd_task: The Datadog task (no parameters needed)
            datadog_connector: The Datadog connector to use
            
        Returns:
            A PlaybookTaskResult containing list of dashboards as API response
        """
        try:
            if not datadog_connector:
                raise Exception("Task execution Failed:: No Datadog source found")

            dd_api_processor = self.get_connector_processor(datadog_connector)

            # Fetch all dashboards (already JSON string)
            response_json = dd_api_processor.fetch_dashboards()

            # Import required modules
            from google.protobuf.struct_pb2 import Struct
            import json

            if not response_json:
                error_struct = Struct()
                error_struct.update({"error": "No dashboards found"})
                
                # Extract Datadog API domain and create metadata
                api_domain = self._extract_api_domain_from_connector(datadog_connector)
                metadata = self._create_metadata_with_datadog_url(api_domain, "fetch_dashboards")
                
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.API_RESPONSE,
                    api_response=ApiResponseResult(
                        response_status=UInt64Value(value=404),
                        response_body=error_struct
                    ),
                    source=self.source,
                    metadata=metadata
                )

            # Parse JSON string back to dict for Struct
            response_data = json.loads(response_json)
            response_struct = Struct()
            response_struct.update(response_data)

            # Extract Datadog API domain and create metadata
            api_domain = self._extract_api_domain_from_connector(datadog_connector)
            metadata = self._create_metadata_with_datadog_url(api_domain, "fetch_dashboards")

            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                api_response=ApiResponseResult(
                    response_status=UInt64Value(value=200),
                    response_body=response_struct
                ),
                source=self.source,
                metadata=metadata
            )

        except Exception as e:
            logger.error(f"Error while executing fetch dashboards task: {e}")
            raise Exception(f"Error while executing Datadog task: {e}")

