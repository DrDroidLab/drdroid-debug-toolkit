import re
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List

import pytz
from google.protobuf.wrappers_pb2 import DoubleValue, StringValue

from core.integrations.source_api_processors.new_relic_graph_ql_processor import NewRelicGraphQlConnector
from core.integrations.source_manager import SourceManager
from core.protos.base_pb2 import TimeRange, Source, SourceModelType, SourceKeyType
from core.protos.connectors.connector_pb2 import Connector as ConnectorProto
from core.protos.literal_pb2 import LiteralType, Literal
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResult, TimeseriesResult, LabelValuePair, \
    PlaybookTaskResultType, TextResult, TableResult, ApiResponseResult
from core.protos.playbooks.source_task_definitions.new_relic_task_pb2 import NewRelic
from core.protos.ui_definition_pb2 import FormField, FormFieldType
from core.protos.assets.asset_pb2 import AccountConnectorAssets, AccountConnectorAssetsModelFilters
from core.utils.playbooks_client import PrototypeClient

from core.utils.credentilal_utils import generate_credentials_dict, get_connector_key_type_string, CATEGORY, DISPLAY_NAME, APPLICATION_MONITORING
from core.utils.string_utils import is_partial_match
from core.utils.time_utils import calculate_timeseries_bucket_size
from collections.abc import MutableMapping

logger = logging.getLogger(__name__)


def get_nrql_expression_result_alias(nrql_expression):
    pattern = r'AS\s+\'(.*?)\'|AS\s+(\w+)'
    match = re.search(pattern, nrql_expression, re.IGNORECASE)
    if match:
        return match.group(1) or match.group(2)
    return 'result'


def flatten_dict(d, parent_key='', sep='_'):
    """Recursively flattens a nested dictionary."""
    items = {}
    for k, v in d.items():
        if k == 'percentiles':
            k = 'percentile'
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, MutableMapping):
            items.update(flatten_dict(v, new_key, sep=sep))
        else:
            items[new_key] = v
    return items


def convert_list_to_flattened_dict(lst):
    """Converts the given list of dictionaries into a single flattened dictionary."""
    result = {}
    for item in lst:
        result.update(flatten_dict(item))
    return result


def get_first_matching_value(d, keys):
    """Returns the value of the first key found in the dictionary from the given list of keys."""
    for key in keys:
        if key in d:
            return d[key]
    return None


class NewRelicSourceManager(SourceManager):

    def __init__(self):
        self.source = Source.NEW_RELIC
        self.task_proto = NewRelic
        self.task_type_callable_map = {
            NewRelic.TaskType.ENTITY_APPLICATION_GOLDEN_METRIC_EXECUTION: {
                'executor': self.execute_entity_application_golden_metric_execution,
                'model_types': [SourceModelType.NEW_RELIC_ENTITY_APPLICATION],
                'result_type': PlaybookTaskResultType.TIMESERIES,
                'display_name': 'Fetch a New Relic golden metric',
                'category': 'Metrics',
                'is_agent_enabled': False,
                'form_fields': [
                    FormField(key_name=StringValue(value="application_entity_name"),
                              display_name=StringValue(value="Application"),
                              description=StringValue(value="Select Application"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT),
                    FormField(key_name=StringValue(value="golden_metric_name"),
                              display_name=StringValue(value="Metric"),
                              description=StringValue(value="Select Metric"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT),
                    FormField(key_name=StringValue(value="golden_metric_unit"),
                              display_name=StringValue(value="Unit"),
                              description=StringValue(value="Enter Unit"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT),
                    FormField(key_name=StringValue(value="golden_metric_nrql_expression"),
                              display_name=StringValue(value="Selected Query"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.MULTILINE_FT),
                ]
            },
            NewRelic.TaskType.ENTITY_APPLICATION_APM_METRIC_EXECUTION: {
                'executor': self.execute_entity_application_apm_metric_execution,
                'model_types': [SourceModelType.NEW_RELIC_ENTITY_APPLICATION],
                'result_type': PlaybookTaskResultType.TIMESERIES,
                'display_name': 'Fetch a New Relic APM metric',
                'category': 'Metrics',
                'form_fields': [
                    FormField(key_name=StringValue(value="application_entity_name"),
                              display_name=StringValue(value="Application"),
                              description=StringValue(value="Select Application"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT,
                              valid_values=[]),
                    FormField(key_name=StringValue(value="apm_metric_names"),
                              display_name=StringValue(value="APM Metric Names (Optional)"),
                              description=StringValue(value="Comma-separated list of metric names to fetch (leave blank for all)"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                ]
            },
            NewRelic.TaskType.ENTITY_DASHBOARD_WIDGET_NRQL_METRIC_EXECUTION: {
                'executor': self.execute_entity_dashboard_widget_nrql_metric_execution,
                'model_types': [SourceModelType.NEW_RELIC_ENTITY_DASHBOARD],
                'result_type': PlaybookTaskResultType.TIMESERIES,
                'display_name': 'Fetch a metric from New Relic dashboard',
                'category': 'Metrics',
                'is_agent_enabled': False,
                'form_fields': [
                    FormField(key_name=StringValue(value="dashboard_guid"),
                              display_name=StringValue(value="Dashboard"),
                              description=StringValue(value="Select Dashboard"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT),
                    FormField(key_name=StringValue(value="page_guid"),
                              display_name=StringValue(value="Page"),
                              description=StringValue(value="Select Page"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT),
                    FormField(key_name=StringValue(value="widget_title"),
                              display_name=StringValue(value="Widget Title"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT),
                    FormField(key_name=StringValue(value="widget_nrql_expression"),
                              display_name=StringValue(value="Selected Query"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.MULTILINE_FT),
                ]
            },
            NewRelic.TaskType.NRQL_METRIC_EXECUTION: {
                'executor': self.execute_nrql_metric_execution,
                'model_types': [],
                'result_type': PlaybookTaskResultType.TIMESERIES,
                'display_name': 'Fetch a custom NRQL query',
                'category': 'Metrics',
                'is_agent_enabled': False,
                'form_fields': [
                    FormField(key_name=StringValue(value="metric_name"),
                              display_name=StringValue(value="Metric Name"),
                              description=StringValue(value="Enter Metric Name"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="unit"),
                              display_name=StringValue(value="Unit"),
                              description=StringValue(value="Enter Unit"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="nrql_expression"),
                              display_name=StringValue(value="Selected Query"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.MULTILINE_FT),
                ]
            },
            NewRelic.TaskType.FETCH_DASHBOARD_WIDGETS: {
                'executor': self.execute_fetch_dashboard_widgets,
                'model_types': [SourceModelType.NEW_RELIC_ENTITY_DASHBOARD_V2],
                'result_type': PlaybookTaskResultType.TIMESERIES,
                'display_name': 'Fetch all widgets from a New Relic dashboard',
                'category': 'Metrics',
                'form_fields': [
                    FormField(key_name=StringValue(value="dashboard_name"),
                              display_name=StringValue(value="Dashboard Name"),
                              description=StringValue(value="Enter Dashboard Name"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="page_name"),
                              display_name=StringValue(value="Page Name (Optional)"),
                              description=StringValue(value="Enter Page Name to filter widgets"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="widget_names"),
                              display_name=StringValue(value="Widget Names (Optional)"),
                              description=StringValue(value="Enter widget names to filter"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                ]
            },
            NewRelic.TaskType.ENTITY_APPLICATION_APM_DATABASE_SUMMARY: {
                'executor': self.execute_entity_application_apm_database_summary,
                'model_types': [SourceModelType.NEW_RELIC_ENTITY_APPLICATION],
                'result_type': PlaybookTaskResultType.TIMESERIES,
                'display_name': 'Fetch New Relic APM database summary',
                'category': 'Metrics',
                'form_fields': [
                    FormField(key_name=StringValue(value="application_entity_guid"),
                              display_name=StringValue(value="Application"),
                              description=StringValue(value="Select Application"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT),
                    FormField(key_name=StringValue(value="sort_by"),
                              display_name=StringValue(value="Sort By"),
                              description=StringValue(value="Select sorting criteria"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.DROPDOWN_FT,
                              valid_values=[
                                  Literal(type=LiteralType.STRING, string=StringValue(value="Most Time Consuming")),
                                  Literal(type=LiteralType.STRING, string=StringValue(value="Slowest Query Time")),
                                  Literal(type=LiteralType.STRING, string=StringValue(value="Throughput (Calls per minute)")),
                              ],
                              default_value=Literal(type=LiteralType.STRING, string=StringValue(value="Most Time Consuming")),
                              is_optional=False),
                ]
            },
            NewRelic.TaskType.ENTITY_APPLICATION_APM_TRANSACTION_SUMMARY: {
                'executor': self.execute_entity_application_apm_transaction_summary,
                'model_types': [SourceModelType.NEW_RELIC_ENTITY_APPLICATION],
                'result_type': PlaybookTaskResultType.TIMESERIES,
                'display_name': 'Fetch New Relic APM transaction summary',
                'category': 'Metrics',
                'form_fields': [
                    FormField(key_name=StringValue(value="application_entity_guid"),
                              display_name=StringValue(value="Application"),
                              description=StringValue(value="Select Application"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT),
                    FormField(key_name=StringValue(value="sort_by"),
                              display_name=StringValue(value="Sort By"),
                              description=StringValue(value="Select sorting criteria"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.DROPDOWN_FT,
                              valid_values=[
                                  Literal(type=LiteralType.STRING, string=StringValue(value="Most Time Consuming")),
                                  Literal(type=LiteralType.STRING, string=StringValue(value="Slowest Average Response Time")),
                                  Literal(type=LiteralType.STRING, string=StringValue(value="Throughput (Calls per minute)")),
                              ],
                              default_value=Literal(type=LiteralType.STRING, string=StringValue(value="Most Time Consuming")),
                              is_optional=False),
                ]
            },
            NewRelic.TaskType.GET_DASHBOARD_VARIABLE_VALUES: {
                'executor': self.execute_get_dashboard_variable_values,
                'model_types': [SourceModelType.NEW_RELIC_ENTITY_DASHBOARD],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Get New Relic dashboard variable values',
                'category': 'Dashboard',
                'form_fields': [
                    FormField(key_name=StringValue(value="dashboard_guid"),
                              display_name=StringValue(value="Dashboard GUID"),
                              description=StringValue(value="Enter Dashboard GUID"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="variable_name"),
                              display_name=StringValue(value="Variable Name (Optional)"),
                              description=StringValue(value="Enter specific variable name to filter"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                ]
            },
            NewRelic.TaskType.FETCH_ALERT_CONDITIONS: {
                'executor': self.execute_fetch_alert_conditions,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Fetch Alert Conditions from New Relic',
                'category': 'Alerts',
                'form_fields': [],
            },
        }

        self.connector_form_configs = [
            {
                "name": StringValue(value="New Relic API Key Authentication"),
                "description": StringValue(value="Connect to New Relic using your API Key, Account ID, and API Domain."),
                "form_fields": {
                    SourceKeyType.NEWRELIC_API_KEY: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.NEWRELIC_API_KEY)),
                        display_name=StringValue(value="API Key"),
                        helper_text=StringValue(value="Enter your New Relic User API Key."),
                        description=StringValue(value='e.g. "1234567890abcdefghijklmnopqrstuvwxyz"'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                        is_sensitive=True
                    ),
                    SourceKeyType.NEWRELIC_APP_ID: FormField( # Note: Display name is Account ID
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.NEWRELIC_APP_ID)),
                        display_name=StringValue(value="Account ID"),
                        helper_text=StringValue(value="Enter your New Relic Account ID."),
                        description=StringValue(value='e.g. "1234567890"'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False
                    ),
                    SourceKeyType.NEWRELIC_API_DOMAIN: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.NEWRELIC_API_DOMAIN)),
                        display_name=StringValue(value="API Domain"),
                        helper_text=StringValue(value="Select your New Relic API Domain"),
                        description=StringValue(value='e.g. "api.newrelic.com" or "api.eu.newrelic.com"'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.DROPDOWN_FT,
                        valid_values=[
                            Literal(type=LiteralType.STRING, string=StringValue(value="api.newrelic.com")),
                            Literal(type=LiteralType.STRING, string=StringValue(value="api.eu.newrelic.com")),
                            Literal(type=LiteralType.STRING, string=StringValue(value="api.fedramp.newrelic.com")),
                        ],
                        default_value=Literal(type=LiteralType.STRING, string=StringValue(value="api.newrelic.com")),
                        is_optional=False
                    )
                }
            }
        ]
        self.connector_type_details = {
            DISPLAY_NAME: "NEW RELIC",
            CATEGORY: APPLICATION_MONITORING,
        }

    def get_connector_processor(self, grafana_connector, **kwargs):
        generated_credentials = generate_credentials_dict(grafana_connector.type, grafana_connector.keys)
        return NewRelicGraphQlConnector(**generated_credentials)

    def execute_entity_application_golden_metric_execution(self, time_range: TimeRange, nr_task: NewRelic,
                                                           nr_connector: ConnectorProto):
        try:
            if not nr_connector:
                raise Exception("Task execution Failed:: No New Relic source found")

            task = nr_task.entity_application_golden_metric_execution
            name = task.golden_metric_name.value
            unit = task.golden_metric_unit.value
            timeseries_offsets = task.timeseries_offsets

            nrql_expression = task.golden_metric_nrql_expression.value
            # Strip any trailing whitespace or newlines to avoid syntax errors
            nrql_expression = nrql_expression.strip()

            if 'timeseries' not in nrql_expression.lower():
                logger.info("Invalid NRQL expression. TIMESERIES is missing in the NRQL expression")
                nrql_expression = nrql_expression + 'TIMESERIES LIMIT'

            if 'limit max timeseries' in nrql_expression.lower():
                nrql_expression = re.sub('limit max timeseries', 'TIMESERIES 5 MINUTE', nrql_expression,
                                         flags=re.IGNORECASE)
            if 'since' not in nrql_expression.lower():
                time_since = time_range.time_geq
                time_until = time_range.time_lt
                total_seconds = (time_until - time_since)
                nrql_expression = nrql_expression + f' SINCE {total_seconds} SECONDS AGO'

            result_alias = get_nrql_expression_result_alias(nrql_expression)
            nr_gql_processor = self.get_connector_processor(nr_connector)

            print(
                "Playbook Task Downstream Request: Type -> {}, Account -> {}, Nrql_Expression -> {}".format(
                    "NewRelic", nr_connector.account_id.value, nrql_expression), flush=True)

            response = nr_gql_processor.execute_nrql_query(nrql_expression)
            if not response or 'results' not in response:
                return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT,
                                          text=TextResult(output=StringValue(
                                              value=f"No data returned from New Relic for nrql expression: {nrql_expression}")),
                                          source=self.source)

            results = response.get('results', [])
            metric_datapoints = []
            for item in results:
                utc_timestamp = item['beginTimeSeconds']
                utc_datetime = datetime.utcfromtimestamp(utc_timestamp)
                utc_datetime = utc_datetime.replace(tzinfo=pytz.UTC)
                val = item.get(result_alias)
                datapoint = TimeseriesResult.LabeledMetricTimeseries.Datapoint(
                    timestamp=int(utc_datetime.timestamp() * 1000), value=DoubleValue(value=val))
                metric_datapoints.append(datapoint)

            metric_label_values = [
                LabelValuePair(name=StringValue(value='offset_seconds'), value=StringValue(value='0'))
            ]
            labeled_metric_timeseries_list = [
                TimeseriesResult.LabeledMetricTimeseries(
                    metric_label_values=metric_label_values, unit=StringValue(value=unit), datapoints=metric_datapoints)
            ]

            # Process offset values if specified
            if timeseries_offsets:
                offsets = [offset for offset in timeseries_offsets]
                for offset in offsets:
                    adjusted_start_time = time_range.time_geq - offset
                    adjusted_end_time = time_range.time_lt - offset
                    total_seconds = adjusted_end_time - adjusted_start_time
                    adjusted_nrql_expression = re.sub(
                        r'SINCE\s+\d+\s+SECONDS\s+AGO', f'SINCE {total_seconds} SECONDS AGO', nrql_expression)

                    print(
                        "Playbook Task Downstream Request: Type -> {}, Account -> {}, Nrql_Expression -> {}, "
                        "Offset -> {}".format(
                            "NewRelic", nr_connector.account_id.value, adjusted_nrql_expression, offset), flush=True)

                    offset_response = nr_gql_processor.execute_nrql_query(adjusted_nrql_expression)
                    if not offset_response or 'results' not in offset_response:
                        print(f"No data returned from New Relic for offset {offset} seconds")
                        continue

                    offset_results = offset_response.get('results', [])
                    offset_metric_datapoints = []
                    for item in offset_results:
                        utc_timestamp = item['beginTimeSeconds']
                        utc_datetime = datetime.utcfromtimestamp(utc_timestamp)
                        utc_datetime = utc_datetime.replace(tzinfo=pytz.UTC)
                        val = item.get(result_alias)
                        datapoint = TimeseriesResult.LabeledMetricTimeseries.Datapoint(
                            timestamp=int(utc_datetime.timestamp() * 1000), value=DoubleValue(value=val))
                        offset_metric_datapoints.append(datapoint)

                    offset_metric_label_values = [
                        LabelValuePair(name=StringValue(value='offset_seconds'), value=StringValue(value=str(offset)))
                    ]
                    labeled_metric_timeseries_list.append(
                        TimeseriesResult.LabeledMetricTimeseries(
                            metric_label_values=offset_metric_label_values, unit=StringValue(value=unit),
                            datapoints=offset_metric_datapoints)
                    )

            timeseries_result = TimeseriesResult(
                metric_expression=StringValue(value=nrql_expression),
                metric_name=StringValue(value=name),
                labeled_metric_timeseries=labeled_metric_timeseries_list
            )
            task_result = PlaybookTaskResult(
                type=PlaybookTaskResultType.TIMESERIES,
                timeseries=timeseries_result,
                source=Source.NEW_RELIC
            )
            return task_result
        except Exception as e:
            raise Exception(f"Error while executing New Relic task: {e}")

    def _prepare_apm_metric_nrql(self, nrql_expression: str, time_range: TimeRange) -> str:
        """
        Prepares the APM metric NRQL query string for execution by standardizing
        the TIMESERIES and time range clauses based on the playbook context.
        (Adapted from _prepare_widget_nrql)

        Args:
            nrql_expression: The original NRQL query from the APM metric asset.
            time_range: The TimeRange object specifying the desired start and end times.

        Returns:
            The modified NRQL query string ready for execution.
        """
        return self._prepare_widget_nrql(nrql_expression, time_range) # Reuse existing logic for now

    def _prepare_database_nrql(self, nrql_expression: str, time_range: TimeRange, metric_name: str = "") -> str:
        """
        Prepares the database NRQL query string for execution by standardizing
        the TIMESERIES and time range clauses based on the playbook context.
        Uses the same robust approach as _prepare_widget_nrql.

        Args:
            nrql_expression: The original NRQL query from the database metric.
            time_range: The TimeRange object specifying the desired start and end times.
            metric_name: The name of the metric for chart type determination.

        Returns:
            The modified NRQL query string ready for execution.
        """
        nrql_expression = nrql_expression.strip()
        original_nrql = nrql_expression  # Keep a copy for logging

        # 1. Calculate desired time range in milliseconds
        start_ms = time_range.time_geq * 1000
        end_ms = time_range.time_lt * 1000
        total_seconds = time_range.time_lt - time_range.time_geq
        if total_seconds <= 0:
            # Default to 1 hour if range is invalid
            end_ms = int(datetime.now(tz=pytz.UTC).timestamp() * 1000)
            start_ms = end_ms - 3600 * 1000
            total_seconds = 3600
            logger.warning(
                f"Invalid time range provided for database query '{metric_name}'. Defaulting to last 1 hour. Original NRQL: {original_nrql}")

        # 2. Calculate appropriate bucket size for timeseries charts
        bucket_size = calculate_timeseries_bucket_size(total_seconds)
        calculated_timeseries_clause = f'TIMESERIES {bucket_size} SECONDS'
        calculated_time_range_clause = f'SINCE {start_ms} UNTIL {end_ms}'

        # 3. Remove existing time range clauses (SINCE, UNTIL)
        nrql_expression = re.sub(
            r'\bSINCE\s+(.*?)(?=\b(?:UNTIL|LIMIT|TIMESERIES|FACET)|\s*$)',
            '', nrql_expression, flags=re.IGNORECASE | re.DOTALL
        ).strip()
        nrql_expression = re.sub(
            r'\bUNTIL\s+(.*?)(?=\b(?:LIMIT|TIMESERIES|FACET)|\s*$)',
            '', nrql_expression, flags=re.IGNORECASE | re.DOTALL
        ).strip()

        # 4. Remove/Replace existing TIMESERIES clause
        nrql_expression = re.sub(
            r'(?:\bLIMIT\s+MAX\s+)?\bTIMESERIES(?:\s+MAX|\s+AUTO|\s+\d+\s+\w+)?',
            '', nrql_expression, flags=re.IGNORECASE
        ).strip()

        # 5. Determine chart type and add appropriate clauses
        chart_type, result_type = self._get_metric_chart_config(metric_name)
        
        # Add time range clause
        nrql_expression += f' {calculated_time_range_clause}'
        
        # Add TIMESERIES only for timeseries charts
        if chart_type == "timeseries":
            nrql_expression += f' {calculated_timeseries_clause}'

        # Clean up potential multiple spaces
        nrql_expression = re.sub(r'\s+', ' ', nrql_expression).strip()

        return nrql_expression

    def _prepare_transaction_nrql(self, nrql_expression: str, time_range: TimeRange, metric_name: str = "") -> str:
        """
        Prepares the transaction NRQL query string for execution by standardizing
        the TIMESERIES and time range clauses based on the playbook context.
        Uses the same robust approach as _prepare_widget_nrql.

        Args:
            nrql_expression: The original NRQL query from the transaction metric.
            time_range: The TimeRange object specifying the desired start and end times.
            metric_name: The name of the metric for chart type determination.

        Returns:
            The modified NRQL query string ready for execution.
        """
        nrql_expression = nrql_expression.strip()
        original_nrql = nrql_expression  # Keep a copy for logging

        # 1. Calculate desired time range in milliseconds
        start_ms = time_range.time_geq * 1000
        end_ms = time_range.time_lt * 1000
        total_seconds = time_range.time_lt - time_range.time_geq
        if total_seconds <= 0:
            # Default to 1 hour if range is invalid
            end_ms = int(datetime.now(tz=pytz.UTC).timestamp() * 1000)
            start_ms = end_ms - 3600 * 1000
            total_seconds = 3600
            logger.warning(
                f"Invalid time range provided for transaction query '{metric_name}'. Defaulting to last 1 hour. Original NRQL: {original_nrql}")

        # 2. Calculate appropriate bucket size for timeseries charts
        bucket_size = calculate_timeseries_bucket_size(total_seconds)
        calculated_timeseries_clause = f'TIMESERIES {bucket_size} SECONDS'
        calculated_time_range_clause = f'SINCE {start_ms} UNTIL {end_ms}'

        # 3. Remove existing time range clauses (SINCE, UNTIL)
        nrql_expression = re.sub(
            r'\bSINCE\s+(.*?)(?=\b(?:UNTIL|LIMIT|TIMESERIES|FACET)|\s*$)',
            '', nrql_expression, flags=re.IGNORECASE | re.DOTALL
        ).strip()
        nrql_expression = re.sub(
            r'\bUNTIL\s+(.*?)(?=\b(?:LIMIT|TIMESERIES|FACET)|\s*$)',
            '', nrql_expression, flags=re.IGNORECASE | re.DOTALL
        ).strip()

        # 4. Remove/Replace existing TIMESERIES clause
        nrql_expression = re.sub(
            r'(?:\bLIMIT\s+MAX\s+)?\bTIMESERIES(?:\s+MAX|\s+AUTO|\s+\d+\s+\w+)?',
            '', nrql_expression, flags=re.IGNORECASE
        ).strip()

        # 5. Determine chart type and add appropriate clauses
        chart_type, result_type = self._get_transaction_metric_chart_config(metric_name)
        
        # Add time range clause
        nrql_expression += f' {calculated_time_range_clause}'
        
        # Add TIMESERIES only for timeseries charts
        if chart_type == "timeseries":
            nrql_expression += f' {calculated_timeseries_clause}'

        # Clean up potential multiple spaces
        nrql_expression = re.sub(r'\s+', ' ', nrql_expression).strip()

        return nrql_expression

    def _parse_apm_metric_response(self, response: Optional[Dict[str, Any]], metric_name: str, unit: str) -> List[TimeseriesResult.LabeledMetricTimeseries]:
        """Parses the NRQL response for an APM metric and extracts timeseries data."""
        labeled_metric_timeseries_list = []
        if not response:
            logger.warning(f"No data returned for APM metric '{metric_name}'")
            return labeled_metric_timeseries_list

        raw_response = response.get('rawResponse', {}) if isinstance(response, dict) else {}
        facet_results = raw_response.get('facets', []) if isinstance(raw_response, dict) else []
        
        # Also check for direct results
        direct_results = response.get('results', []) if isinstance(response, dict) else []
        
        results_to_process = []
        is_faceted = False
        
        # Process faceted data (preferred for database queries)
        if facet_results and isinstance(facet_results, list):
            results_to_process = facet_results
            is_faceted = True
        elif direct_results and isinstance(direct_results, list):
            # Check if direct results have facet information
            if direct_results and isinstance(direct_results[0], dict) and 'facet' in direct_results[0]:
                # Group by facet
                facet_groups = {}
                for result in direct_results:
                    if isinstance(result, dict):
                        facet_value = result.get('facet', 'default')
                        # Handle facet values that might be lists or complex objects
                        if isinstance(facet_value, (list, tuple)):
                            facet_key = ' | '.join(str(v) for v in facet_value)
                        elif isinstance(facet_value, dict):
                            facet_key = str(facet_value)
                        else:
                            facet_key = str(facet_value)
                        
                        if facet_key not in facet_groups:
                            facet_groups[facet_key] = []
                        facet_groups[facet_key].append(result)
                
                # Convert to the format expected by the processor
                for facet_key, facet_data in facet_groups.items():
                    results_to_process.append({
                        'name': facet_key,
                        'timeSeries': facet_data
                    })
                is_faceted = True
            else:
                # Non-faceted results
                results_to_process = [{'timeSeries': direct_results, 'name': 'default'}]
        else:
            logger.warning(f"No 'results' or 'facets' found in response for APM metric '{metric_name}'")
            return labeled_metric_timeseries_list

        # Process each series (facet or 'default')
        for idx, series_data in enumerate(results_to_process):
            metric_datapoints = []
            
            # Safely get series name and convert to string
            series_name_raw = series_data.get('name', 'default')
            if isinstance(series_name_raw, (list, tuple)):
                series_name = ' | '.join(str(v) for v in series_name_raw)
            elif isinstance(series_name_raw, dict):
                series_name = str(series_name_raw)
            else:
                series_name = str(series_name_raw) if series_name_raw is not None else 'default'

            timeseries_data = series_data.get('timeSeries', [])
            # Handle edge cases where series_data might be the list or a single point
            if not timeseries_data and isinstance(series_data, list):
                timeseries_data = series_data
            elif not timeseries_data and 'beginTimeSeconds' in series_data:
                timeseries_data = [series_data]

            for ts_idx, ts in enumerate(timeseries_data):
                try:
                    if not isinstance(ts, dict):
                        continue
                    utc_timestamp = ts.get('beginTimeSeconds')
                    if utc_timestamp is None: 
                        continue
                    utc_datetime = datetime.utcfromtimestamp(utc_timestamp).replace(tzinfo=pytz.UTC)

                    val = None
                    potential_keys = ['result', 'score', 'count', 'average', 'sum', 'min', 'max', 'median'] # Common aggregates

                    # First, check if we have a results array with nested values
                    if 'results' in ts and isinstance(ts['results'], list) and len(ts['results']) > 0:
                        result_item = ts['results'][0]
                        if isinstance(result_item, dict):
                            for p_key in potential_keys:
                                if p_key in result_item and isinstance(result_item[p_key], (int, float)):
                                    val = float(result_item[p_key])
                                    break
                    
                    # If not found in results array, check top level
                    if val is None:
                        for p_key in potential_keys:
                           if p_key in ts and isinstance(ts[p_key], dict) and 'score' in ts[p_key] and isinstance(ts[p_key]['score'], (int, float)):
                                val = float(ts[p_key]['score'])
                                break
                           elif p_key in ts and isinstance(ts[p_key], (int, float)):
                                val = float(ts[p_key])
                                break

                    # Fallback: iterate through all numeric values if specific keys not found (excluding metadata)
                    if val is None:
                        exclude_keys = ['beginTimeSeconds', 'endTimeSeconds', 'facet', 'inspectedCount', 'results']
                        for k, v in ts.items():
                            if isinstance(v, (int, float)) and k not in exclude_keys:
                                val = float(v)
                                break

                    if val is None:
                        logger.warning(
                            f"Could not extract numeric value from datapoint {ts} for APM metric '{metric_name}', series '{series_name}'")
                        continue

                    datapoint = TimeseriesResult.LabeledMetricTimeseries.Datapoint(
                        timestamp=int(utc_datetime.timestamp() * 1000),
                        value=DoubleValue(value=val))
                    metric_datapoints.append(datapoint)

                except (ValueError, TypeError, KeyError, AttributeError) as e:
                    logger.warning(
                        f"Error processing datapoint {ts} for APM metric '{metric_name}', series '{series_name}': {str(e)}")
                    continue

            # Create LabeledMetricTimeseries if datapoints found
            if metric_datapoints:
                metric_label_values = [
                    LabelValuePair(name=StringValue(value='apm_metric_name'), value=StringValue(value=metric_name)),
                    LabelValuePair(name=StringValue(value='offset_seconds'), value=StringValue(value='0'))
                ]

                # Only add facet label if we have faceted data and series name is not default
                if is_faceted and series_name != 'default':
                    metric_label_values.append(
                        LabelValuePair(name=StringValue(value='facet'), value=StringValue(value=series_name))
                    )

                labeled_metric_timeseries_list.append(
                    TimeseriesResult.LabeledMetricTimeseries(
                        metric_label_values=metric_label_values,
                        unit=StringValue(value=unit),
                        datapoints=metric_datapoints
                    )
                )
            else:
                 print(f"No valid datapoints found for series '{series_name}' in APM metric '{metric_name}'")

        return labeled_metric_timeseries_list

    def _parse_database_metric_response(self, response: Dict[str, Any], metric_name: str) -> List[TimeseriesResult.LabeledMetricTimeseries]:
        """Alternative parsing method specifically for database metric responses that may have different structure."""
        labeled_metric_timeseries_list = []
        
        # Try to extract data from different possible structures
        results = response.get('results', [])
        if not results:
            print(f"No results found in database response for '{metric_name}'")
            return labeled_metric_timeseries_list
        
        # Check if results are grouped by facet already
        faceted_data = {}
        for result in results:
            if not isinstance(result, dict):
                continue
                
            # Extract facet information
            facet_key = 'default'
            if 'facet' in result:
                facet_value = result['facet']
                if isinstance(facet_value, (list, tuple)):
                    facet_key = ' | '.join(str(v) for v in facet_value)
                else:
                    facet_key = str(facet_value)
            
            # Initialize facet group if not exists
            if facet_key not in faceted_data:
                faceted_data[facet_key] = []
            
            faceted_data[facet_key].append(result)
        
        # Process each facet group
        for facet_name, facet_results in faceted_data.items():
            metric_datapoints = []
            
            for result in facet_results:
                timestamp = result.get('beginTimeSeconds')
                if timestamp is None:
                    continue
                    
                # Find the numeric value
                value = None
                for key, val in result.items():
                    if key not in ['beginTimeSeconds', 'endTimeSeconds', 'facet'] and isinstance(val, (int, float)):
                        value = float(val)
                        break
                
                if value is not None:
                    utc_datetime = datetime.utcfromtimestamp(timestamp).replace(tzinfo=pytz.UTC)
                    datapoint = TimeseriesResult.LabeledMetricTimeseries.Datapoint(
                        timestamp=int(utc_datetime.timestamp() * 1000),
                        value=DoubleValue(value=value))
                    metric_datapoints.append(datapoint)
            
            if metric_datapoints:
                metric_label_values = [
                    LabelValuePair(name=StringValue(value='apm_metric_name'), value=StringValue(value=metric_name)),
                    LabelValuePair(name=StringValue(value='offset_seconds'), value=StringValue(value='0'))
                ]
                
                if facet_name != 'default':
                    metric_label_values.append(
                        LabelValuePair(name=StringValue(value='facet'), value=StringValue(value=facet_name))
                    )
                
                labeled_metric_timeseries_list.append(
                    TimeseriesResult.LabeledMetricTimeseries(
                        metric_label_values=metric_label_values,
                        unit=StringValue(value=""),
                        datapoints=metric_datapoints
                    )
                )
        
        return labeled_metric_timeseries_list

    def _parse_bar_chart_response(self, response: Dict[str, Any], metric_name: str) -> List[TimeseriesResult.LabeledMetricTimeseries]:
        """Parse bar chart (non-timeseries) responses from New Relic."""
        labeled_metric_timeseries_list = []
        
        if not response or not isinstance(response, dict):
            print(f"No valid bar chart data for '{metric_name}'")
            return labeled_metric_timeseries_list
        
        # For bar chart data, we get results without timeseries
        results = response.get('results', [])
        if not results:
            print(f"No results found in bar chart response for '{metric_name}'")
            return labeled_metric_timeseries_list
        
        # Create a single datapoint for each facet value (bar chart data)
        for idx, result in enumerate(results):
            if not isinstance(result, dict):
                continue
                
            # Extract facet name
            facet_value = result.get('facet', f'item_{idx}')
            if isinstance(facet_value, (list, tuple)):
                facet_name = ' | '.join(str(v) for v in facet_value)
            else:
                facet_name = str(facet_value)
            
            # Extract the numeric value
            value = None
            for key, val in result.items():
                if key != 'facet' and isinstance(val, (int, float)):
                    value = float(val)
                    break
            
            if value is None:
                continue
            
            # Create a single datapoint at current time (for bar chart visualization)
            import time
            current_timestamp = int(time.time() * 1000)
            datapoint = TimeseriesResult.LabeledMetricTimeseries.Datapoint(
                timestamp=current_timestamp,
                value=DoubleValue(value=value)
            )
            
            # Create labels for this bar
            metric_label_values = [
                LabelValuePair(name=StringValue(value='apm_metric_name'), value=StringValue(value=metric_name)),
                LabelValuePair(name=StringValue(value='offset_seconds'), value=StringValue(value='0')),
                LabelValuePair(name=StringValue(value='facet'), value=StringValue(value=facet_name)),
                LabelValuePair(name=StringValue(value='chart_type'), value=StringValue(value='bar_chart'))
            ]
            
            labeled_metric_timeseries_list.append(
                TimeseriesResult.LabeledMetricTimeseries(
                    metric_label_values=metric_label_values,
                    unit=StringValue(value="ms"),  # Duration unit for database operations
                    datapoints=[datapoint]  # Single datapoint for bar chart
                )
            )
        
        return labeled_metric_timeseries_list
    
    def _convert_bar_chart_to_table(self, response: Dict[str, Any], metric_name: str) -> TableResult:
        """Converts bar chart response to TABLE format for proper visualization."""
        from google.protobuf.wrappers_pb2 import StringValue, UInt64Value
        
        if not response or not isinstance(response, dict):
            # Return empty table
            return TableResult(
                raw_query=StringValue(value=f"New Relic Database Query: {metric_name}"),
                total_count=UInt64Value(value=0),
                limit=UInt64Value(value=20),
                offset=UInt64Value(value=0),
                rows=[]
            )
        
        results = response.get('results', [])
        if not results:
            return TableResult(
                raw_query=StringValue(value=f"New Relic Database Query: {metric_name}"),
                total_count=UInt64Value(value=0),
                limit=UInt64Value(value=20),
                offset=UInt64Value(value=0),
                rows=[]
            )
        
        # Create table rows from the faceted results
        table_rows = []
        for result in results:
            if not isinstance(result, dict):
                continue
                
            # Extract facet name (operation name)
            facet_value = result.get('facet', 'Unknown')
            if isinstance(facet_value, (list, tuple)):
                operation_name = ' | '.join(str(v) for v in facet_value)
            else:
                operation_name = str(facet_value)
            
            # Extract the numeric value
            value = None
            for key, val in result.items():
                if key != 'facet' and isinstance(val, (int, float)):
                    value = val
                    break
            
            if value is not None:
                # Create table columns for this row
                table_columns = [
                    TableResult.TableColumn(
                        name=StringValue(value="Database Operation"),
                        value=StringValue(value=operation_name)
                    ),
                    TableResult.TableColumn(
                        name=StringValue(value="Duration (ms)" if "time" in metric_name.lower() else "Value"),
                        value=StringValue(value=f"{value:.2f}")
                    )
                ]
                table_rows.append(TableResult.TableRow(columns=table_columns))
        
        # Create table
        table = TableResult(
            raw_query=StringValue(value=f"New Relic Database Query: {metric_name}"),
            total_count=UInt64Value(value=len(table_rows)),
            limit=UInt64Value(value=20),
            offset=UInt64Value(value=0),
            rows=table_rows
        )
        
        return table

    def _convert_nrql_aggregate_to_table(self, response: dict, nrql_expression: str, metric_name: str) -> PlaybookTaskResult:
        """Converts a non-timeseries NRQL aggregate response to a TABLE result."""
        from google.protobuf.wrappers_pb2 import StringValue, UInt64Value

        results = response.get('results', [])
        if not results:
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(
                    value=f"No data returned from New Relic for nrql expression: {nrql_expression}")),
                source=self.source)

        table_rows = []
        for result in results:
            if not isinstance(result, dict):
                continue

            table_columns = []
            for key, val in result.items():
                table_columns.append(
                    TableResult.TableColumn(
                        name=StringValue(value=key),
                        value=StringValue(value=str(val))
                    )
                )
            if table_columns:
                table_rows.append(TableResult.TableRow(columns=table_columns))

        table = TableResult(
            raw_query=StringValue(value=nrql_expression),
            total_count=UInt64Value(value=len(table_rows)),
            limit=UInt64Value(value=20),
            offset=UInt64Value(value=0),
            rows=table_rows
        )

        return PlaybookTaskResult(
            type=PlaybookTaskResultType.TABLE,
            table=table,
            source=self.source
        )

    def execute_entity_application_apm_metric_execution(self, time_range: TimeRange, nr_task: NewRelic,
                                                        nr_connector: ConnectorProto):
        try:
            if not nr_connector:
                raise Exception("Task execution Failed:: No New Relic source found")

            task = nr_task.entity_application_apm_metric_execution
            application_name = task.application_entity_name.value
            timeseries_offsets = list(task.timeseries_offsets) # Ensure it's a list
            filter_metric_names_str = task.apm_metric_names.value if task.HasField('apm_metric_names') else ''
            filter_metric_names = [name.strip().lower() for name in filter_metric_names_str.split(',') if name.strip()]

            # 1. Get the specific application asset
            client = PrototypeClient()
            assets = client.get_connector_assets(
                connector_type=Source.Name(nr_connector.type),
                connector_id=str(nr_connector.id.value),
                asset_type=SourceModelType.NEW_RELIC_ENTITY_APPLICATION,
            )

            if not assets:
                return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT,
                                          text=TextResult(output=StringValue(
                                              value=f"Application asset with GUID '{application_name}' not found")),
                                          source=self.source)

            application_asset = None
            for newrelic_asset in assets.new_relic.assets:
                if newrelic_asset.type == SourceModelType.NEW_RELIC_ENTITY_APPLICATION and \
                   newrelic_asset.new_relic_entity_application.application_name.value == application_name:
                    application_asset = newrelic_asset.new_relic_entity_application
                    break

            if not application_asset:
                 return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT,
                                           text=TextResult(output=StringValue(
                                               value=f"Application asset with GUID '{application_name}' not found within returned data")),
                                           source=self.source)

            # 2. Filter APM metrics if requested
            all_apm_metrics = list(application_asset.apm_metrics)
            if not all_apm_metrics:
                return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT,
                                          text=TextResult(output=StringValue(
                                              value=f"No APM metrics defined for application with GUID '{application_name}'")),
                                          source=self.source)

            metrics_to_process = []
            if filter_metric_names:
                for metric in all_apm_metrics:
                    if metric.metric_name.value.lower() in filter_metric_names:
                        metrics_to_process.append(metric)
                if not metrics_to_process:
                     return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT,
                                           text=TextResult(output=StringValue(
                                               value=f"No APM metrics matching names {filter_metric_names_str} found for application with GUID")),
                                           source=self.source)
            else:
                metrics_to_process = all_apm_metrics # Process all if no filter

            # 3. Initialize the GraphQL processor
            nr_gql_processor = self.get_connector_processor(nr_connector)
            
            # 4. Get the entity account ID for more accurate queries
            entity_guid = application_asset.application_entity_guid.value
            entity_account_id = None
            try:
                entity_account_id = nr_gql_processor.get_entity_account_id(entity_guid)
                if entity_account_id:
                    logger.info(f"Retrieved account ID {entity_account_id} for entity GUID {entity_guid}")
                else:
                    logger.warning(f"Could not retrieve account ID for entity GUID {entity_guid}, using connector account ID")
            except Exception as e:
                logger.error(f"Error retrieving account ID for entity GUID {entity_guid}: {e}, using connector account ID")
                entity_account_id = None

            # 5. Process each APM metric
            task_results = []

            for apm_metric in metrics_to_process:
                try:
                    metric_name = apm_metric.metric_name.value
                    unit = apm_metric.metric_unit.value
                    base_nrql_expression = apm_metric.metric_nrql_expression.value

                    if not base_nrql_expression:
                         logger.warning(f"Skipping APM metric '{metric_name}' for application '{application_name}' as it has no NRQL query.")
                         continue

                    all_labeled_metric_timeseries = [] # Collect base and offset series
                    prepared_nrql = self._prepare_apm_metric_nrql(base_nrql_expression, time_range)

                    print(
                        "Playbook Task Downstream Request: Type -> {}, Account -> {}, App Name -> {}, Metric -> {}, NRQL -> {}".format(
                            "NewRelicAPM", entity_account_id or nr_connector.account_id.value, application_name, metric_name, prepared_nrql), flush=True)

                    response = nr_gql_processor.execute_nrql_query(prepared_nrql, entity_account_id)
                    base_timeseries = self._parse_apm_metric_response(response, metric_name, unit) # Already has offset 0 label
                    all_labeled_metric_timeseries.extend(base_timeseries)

                    for offset in timeseries_offsets:
                        if offset == 0: continue # Skip 0, already processed

                        adjusted_start_time = time_range.time_geq - offset
                        adjusted_end_time = time_range.time_lt - offset
                        adjusted_time_range = TimeRange(time_geq=adjusted_start_time, time_lt=adjusted_end_time)

                        offset_nrql = self._prepare_apm_metric_nrql(base_nrql_expression, adjusted_time_range)

                        print(
                            "Playbook Task Downstream Request: Type -> {}, Account -> {}, App GUID -> {}, Metric -> {}, NRQL -> {}, Offset -> {}".format(
                                "NewRelicAPM", entity_account_id or nr_connector.account_id.value, application_name, metric_name, offset_nrql, offset), flush=True)

                        offset_response = nr_gql_processor.execute_nrql_query(offset_nrql, entity_account_id)
                        offset_timeseries = self._parse_apm_metric_response(offset_response, metric_name, unit)

                        # Create new LabeledMetricTimeseries objects with updated labels for the offset
                        for series in offset_timeseries:
                            updated_labels = [
                                LabelValuePair(name=StringValue(value='apm_metric_name'), value=StringValue(value=metric_name)),
                                LabelValuePair(name=StringValue(value='offset_seconds'), value=StringValue(value=str(offset)))
                            ]
                            # Preserve facet label if present
                            facet_label = None
                            for label in series.metric_label_values:
                                if label.name.value == 'facet':
                                    facet_label = label
                                    break
                            if facet_label:
                                updated_labels.append(facet_label)

                            # Create a new LabeledMetricTimeseries with updated labels
                            new_labeled_metric_timeseries = TimeseriesResult.LabeledMetricTimeseries(
                                metric_label_values=updated_labels,
                                unit=series.unit,  # Reuse unit from original series
                                datapoints=series.datapoints # Reuse datapoints from original series
                            )
                            all_labeled_metric_timeseries.append(new_labeled_metric_timeseries)

                    if all_labeled_metric_timeseries:
                         # Use the base prepared NRQL for the expression field for consistency
                        timeseries_result = TimeseriesResult(metric_expression=StringValue(value=prepared_nrql),
                                                             metric_name=StringValue(value=metric_name),
                                                             labeled_metric_timeseries=all_labeled_metric_timeseries)
                        task_result = PlaybookTaskResult(type=PlaybookTaskResultType.TIMESERIES,
                                                         timeseries=timeseries_result, source=self.source)
                        task_results.append(task_result)
                    else:
                        logger.warning(f"No timeseries data could be parsed for APM metric '{metric_name}'.")

                except Exception as e:
                    logger.error(f"Error processing APM metric '{apm_metric.metric_name.value}' for application '{application_name}': {str(e)}", exc_info=True)
                    continue

            # Check if any results were generated
            if not task_results:
                filter_msg = f" matching names '{filter_metric_names_str}'" if filter_metric_names_str else ""
                logger.warning(f"No data retrieved for any APM metrics{filter_msg} in application '{application_name}'.")
                return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT,
                                          text=TextResult(output=StringValue(
                                              value=f"No data found for the specified APM metrics{filter_msg} in application '{application_name}'. Check metric configurations and time range.")),
                                          source=self.source)

            return task_results # Return list of results

        except Exception as e:
            logger.error(f"General Error executing Fetch APM Metrics task for app {task.application_entity_name.value}: {str(e)}", exc_info=True)
            raise Exception(f"Error while executing New Relic Fetch APM Metrics task: {e}")

    def execute_entity_application_apm_database_summary(self, time_range: TimeRange, nr_task: NewRelic,
                                                       nr_connector: ConnectorProto):
        try:
            if not nr_connector:
                raise Exception("Task execution Failed:: No New Relic source found")

            task = nr_task.entity_application_apm_database_summary
            application_guid = task.application_entity_guid.value
            sort_by = task.sort_by.value
            timeseries_offsets = list(task.timeseries_offsets)

            # Import the database queries
            from core.utils.static_mappings import NEWRELIC_APM_DATABASE_QUERIES

            # Get all queries to execute
            base_queries = NEWRELIC_APM_DATABASE_QUERIES["base_queries"]
            sort_by_queries = NEWRELIC_APM_DATABASE_QUERIES["sort_by_queries"].get(sort_by, {})
            
            # For "Most Time Consuming", we need special handling for the app name vs entity guid
            if sort_by == "Most Time Consuming":
                # Need to get application name for the Span query
                client = PrototypeClient()
                assets = client.get_connector_assets(
                    connector_type=Source.Name(nr_connector.type),
                    connector_id=str(nr_connector.id.value),
                    asset_type=SourceModelType.NEW_RELIC_ENTITY_APPLICATION,
                )

                if not assets:
                    return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT,
                                              text=TextResult(output=StringValue(
                                                  value=f"Application asset not found for '{application_guid}'")),
                                              source=self.source)

                application_asset = None
                for newrelic_asset in assets.new_relic.assets:
                    if newrelic_asset.type == SourceModelType.NEW_RELIC_ENTITY_APPLICATION and \
                       newrelic_asset.new_relic_entity_application.application_entity_guid.value == application_guid:
                        application_asset = newrelic_asset.new_relic_entity_application
                        break

                if not application_asset:
                    return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT,
                                              text=TextResult(output=StringValue(
                                                  value=f"Application asset not found for '{application_guid}'")),
                                              source=self.source)

                app_name_for_span = application_asset.application_name.value

            nr_gql_processor = self.get_connector_processor(nr_connector)
            
            # Get the entity account ID for more accurate queries
            entity_account_id = None
            try:
                entity_account_id = nr_gql_processor.get_entity_account_id(application_guid)
                if entity_account_id:
                    logger.info(f"Retrieved account ID {entity_account_id} for entity GUID {application_guid}")
                else:
                    logger.warning(f"Could not retrieve account ID for entity GUID {application_guid}, using connector account ID")
            except Exception as e:
                logger.error(f"Error retrieving account ID for entity GUID {application_guid}: {e}, using connector account ID")
                entity_account_id = None
            
            task_results = []

            # Combine all queries to execute
            all_queries = {}
            all_queries.update(base_queries)
            all_queries.update(sort_by_queries)

            for metric_name, nrql_template in all_queries.items():
                try:
                    # Prepare the NRQL query with time range injection
                    if sort_by == "Most Time Consuming" and metric_name == "Top 20 Database Operations":
                        # Use app name for Span queries
                        nrql_expression = nrql_template.format(f"'{app_name_for_span}'")
                    else:
                        # Use entity GUID for Metric queries
                        entity_guid = application_asset.application_entity_guid.value if 'application_asset' in locals() else application_guid
                        nrql_expression = nrql_template.format(f"'{entity_guid}'")

                    # Inject time range into the query
                    prepared_nrql = self._prepare_database_nrql(nrql_expression, time_range, metric_name)

                    print(
                        "Playbook Task Downstream Request: Type -> {}, Account -> {}, App -> {}, Sort By -> {}, Metric -> {}, NRQL -> {}".format(
                            "NewRelicAPMDatabaseSummary", entity_account_id or nr_connector.account_id.value, application_guid, sort_by, metric_name, prepared_nrql),
                        flush=True)

                    # Execute the NRQL query
                    response = nr_gql_processor.execute_nrql_query(prepared_nrql, entity_account_id)
                    
                    if not response:
                        print(f"No data returned for database metric '{metric_name}' with sort_by '{sort_by}'")
                        continue

                    # Choose appropriate parsing and result type based on chart type configuration
                    chart_type, result_type = self._get_metric_chart_config(metric_name)
                    
                    if chart_type == "bar_chart":
                        # Create TABLE result for bar charts
                        table = self._convert_bar_chart_to_table(response, metric_name)
                        task_result = PlaybookTaskResult(
                            type=PlaybookTaskResultType.TABLE,
                            table=table,
                            source=self.source
                        )
                        task_results.append(task_result)
                        continue  # Skip to next metric
                    else:
                        # Handle timeseries data
                        labeled_metric_timeseries_list = self._parse_apm_metric_response(response, metric_name, "")
                        
                        # Fallback for timeseries metrics that failed standard parsing
                        if not labeled_metric_timeseries_list and response:
                            print(f"Standard parsing failed for '{metric_name}', trying alternate database parsing")
                            labeled_metric_timeseries_list = self._parse_database_metric_response(response, metric_name)

                    # Handle timeseries offsets if specified
                    all_labeled_metric_timeseries = labeled_metric_timeseries_list.copy()
                    
                    for offset in timeseries_offsets:
                        if offset == 0:
                            continue  # Skip 0, already processed

                        adjusted_start_time = time_range.time_geq - offset
                        adjusted_end_time = time_range.time_lt - offset
                        adjusted_time_range = TimeRange(time_geq=adjusted_start_time, time_lt=adjusted_end_time)

                        offset_nrql = self._prepare_database_nrql(nrql_expression, adjusted_time_range, metric_name)

                        print(
                            "Playbook Task Downstream Request: Type -> {}, Account -> {}, App -> {}, Sort By -> {}, Metric -> {}, NRQL -> {}, Offset -> {}".format(
                                "NewRelicAPMDatabaseSummary", entity_account_id or nr_connector.account_id.value, application_guid, sort_by, metric_name, offset_nrql, offset),
                            flush=True)

                        offset_response = nr_gql_processor.execute_nrql_query(offset_nrql, entity_account_id)
                        offset_timeseries = self._parse_apm_metric_response(offset_response, metric_name, "")

                        # Update labels with offset information
                        for series in offset_timeseries:
                            updated_labels = []
                            for label in series.metric_label_values:
                                if label.name.value == 'offset_seconds':
                                    updated_labels.append(
                                        LabelValuePair(name=StringValue(value='offset_seconds'), value=StringValue(value=str(offset)))
                                    )
                                else:
                                    updated_labels.append(label)
                            
                            # Add offset label if not present
                            if not any(label.name.value == 'offset_seconds' for label in updated_labels):
                                updated_labels.append(
                                    LabelValuePair(name=StringValue(value='offset_seconds'), value=StringValue(value=str(offset)))
                                )

                            new_series = TimeseriesResult.LabeledMetricTimeseries(
                                metric_label_values=updated_labels,
                                unit=series.unit,
                                datapoints=series.datapoints
                            )
                            all_labeled_metric_timeseries.append(new_series)

                    if all_labeled_metric_timeseries:
                        timeseries_result = TimeseriesResult(
                            metric_expression=StringValue(value=prepared_nrql),
                            metric_name=StringValue(value=f"{metric_name} ({sort_by})"),
                            labeled_metric_timeseries=all_labeled_metric_timeseries
                        )
                        task_result = PlaybookTaskResult(
                            type=PlaybookTaskResultType.TIMESERIES,
                            timeseries=timeseries_result,
                            source=self.source
                        )
                        task_results.append(task_result)

                except Exception as metric_err:
                    logger.error(f"Error processing database metric '{metric_name}' for application '{application_guid}': {str(metric_err)}", exc_info=True)
                    continue

            # Check if any results were generated
            if not task_results:
                logger.warning(f"No data retrieved for any database metrics for application '{application_guid}' with sort_by '{sort_by}'.")
                return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT,
                                          text=TextResult(output=StringValue(
                                              value=f"No database metrics data found for application '{application_guid}' with sort criteria '{sort_by}'. Check application configuration and time range.")),
                                          source=self.source)

            return task_results  # Return list of results

        except Exception as e:
            logger.error(f"General Error executing APM Database Summary task for app {task.application_entity_name.value}: {str(e)}", exc_info=True)
            raise Exception(f"Error while executing New Relic APM Database Summary task: {e}")

    def execute_entity_application_apm_transaction_summary(self, time_range: TimeRange, nr_task: NewRelic,
                                                          nr_connector: ConnectorProto):
        try:
            if not nr_connector:
                raise Exception("Task execution Failed:: No New Relic source found")

            task = nr_task.entity_application_apm_transaction_summary
            application_guid = task.application_entity_guid.value
            sort_by = task.sort_by.value
            timeseries_offsets = list(task.timeseries_offsets)

            # Import the transaction queries
            from core.utils.static_mappings import NEWRELIC_APM_TRANSACTION_QUERIES

            # Get all queries to execute
            base_queries = NEWRELIC_APM_TRANSACTION_QUERIES["base_queries"]
            sort_by_queries = NEWRELIC_APM_TRANSACTION_QUERIES["sort_by_queries"].get(sort_by, {})
            
            nr_gql_processor = self.get_connector_processor(nr_connector)
            
            # Get the entity account ID for more accurate queries
            entity_account_id = None
            try:
                entity_account_id = nr_gql_processor.get_entity_account_id(application_guid)
                if entity_account_id:
                    logger.info(f"Retrieved account ID {entity_account_id} for entity GUID {application_guid}")
                else:
                    logger.warning(f"Could not retrieve account ID for entity GUID {application_guid}, using connector account ID")
            except Exception as e:
                logger.error(f"Error retrieving account ID for entity GUID {application_guid}: {e}, using connector account ID")
                entity_account_id = None
            
            task_results = []

            # Combine all queries to execute
            all_queries = {}
            all_queries.update(base_queries)
            all_queries.update(sort_by_queries)

            for metric_name, nrql_template in all_queries.items():
                try:
                    # Prepare the NRQL query with entity GUID
                    nrql_expression = nrql_template.format(f"'{application_guid}'")

                    # Inject time range into the query
                    prepared_nrql = self._prepare_transaction_nrql(nrql_expression, time_range, metric_name)

                    print(
                        "Playbook Task Downstream Request: Type -> {}, Account -> {}, App -> {}, Sort By -> {}, Metric -> {}, NRQL -> {}".format(
                            "NewRelicAPMTransactionSummary", entity_account_id or nr_connector.account_id.value, application_guid, sort_by, metric_name, prepared_nrql),
                        flush=True)

                    # Execute the NRQL query
                    response = nr_gql_processor.execute_nrql_query(prepared_nrql, entity_account_id)
                    print(response)
                    
                    if not response:
                        print(f"No data returned for transaction metric '{metric_name}' with sort_by '{sort_by}'")
                        continue

                    # Choose appropriate parsing and result type based on chart type configuration
                    chart_type, result_type = self._get_transaction_metric_chart_config(metric_name)
                    
                    if chart_type == "bar_chart":
                        # Create TABLE result for bar charts
                        table = self._convert_bar_chart_to_table(response, metric_name)
                        task_result = PlaybookTaskResult(
                            type=PlaybookTaskResultType.TABLE,
                            table=table,
                            source=self.source
                        )
                        task_results.append(task_result)
                        continue  # Skip to next metric
                    else:
                        # Handle timeseries data
                        labeled_metric_timeseries_list = self._parse_apm_metric_response(response, metric_name, "")
                        
                        # Fallback for timeseries metrics that failed standard parsing
                        if not labeled_metric_timeseries_list and response:
                            print(f"Standard parsing failed for '{metric_name}', trying alternate transaction parsing")
                            labeled_metric_timeseries_list = self._parse_database_metric_response(response, metric_name)

                    # Handle timeseries offsets if specified
                    all_labeled_metric_timeseries = labeled_metric_timeseries_list.copy()
                    
                    for offset in timeseries_offsets:
                        if offset == 0:
                            continue  # Skip 0, already processed

                        adjusted_start_time = time_range.time_geq - offset
                        adjusted_end_time = time_range.time_lt - offset
                        adjusted_time_range = TimeRange(time_geq=adjusted_start_time, time_lt=adjusted_end_time)

                        offset_nrql = self._prepare_transaction_nrql(nrql_expression, adjusted_time_range, metric_name)

                        print(
                            "Playbook Task Downstream Request: Type -> {}, Account -> {}, App -> {}, Sort By -> {}, Metric -> {}, NRQL -> {}, Offset -> {}".format(
                                "NewRelicAPMTransactionSummary", entity_account_id or nr_connector.account_id.value, application_guid, sort_by, metric_name, offset_nrql, offset),
                            flush=True)

                        offset_response = nr_gql_processor.execute_nrql_query(offset_nrql, entity_account_id)
                        offset_timeseries = self._parse_apm_metric_response(offset_response, metric_name, "")

                        # Update labels with offset information
                        for series in offset_timeseries:
                            updated_labels = []
                            for label in series.metric_label_values:
                                if label.name.value == 'offset_seconds':
                                    updated_labels.append(
                                        LabelValuePair(name=StringValue(value='offset_seconds'), value=StringValue(value=str(offset)))
                                    )
                                else:
                                    updated_labels.append(label)
                            
                            # Add offset label if not present
                            if not any(label.name.value == 'offset_seconds' for label in updated_labels):
                                updated_labels.append(
                                    LabelValuePair(name=StringValue(value='offset_seconds'), value=StringValue(value=str(offset)))
                                )

                            new_series = TimeseriesResult.LabeledMetricTimeseries(
                                metric_label_values=updated_labels,
                                unit=series.unit,
                                datapoints=series.datapoints
                            )
                            all_labeled_metric_timeseries.append(new_series)

                    if all_labeled_metric_timeseries:
                        timeseries_result = TimeseriesResult(
                            metric_expression=StringValue(value=prepared_nrql),
                            metric_name=StringValue(value=f"{metric_name} ({sort_by})"),
                            labeled_metric_timeseries=all_labeled_metric_timeseries
                        )
                        task_result = PlaybookTaskResult(
                            type=PlaybookTaskResultType.TIMESERIES,
                            timeseries=timeseries_result,
                            source=self.source
                        )
                        task_results.append(task_result)

                except Exception as metric_err:
                    logger.error(f"Error processing transaction metric '{metric_name}' for application '{application_guid}': {str(metric_err)}", exc_info=True)
                    continue

            # Check if any results were generated
            if not task_results:
                logger.warning(f"No data retrieved for any transaction metrics for application '{application_guid}' with sort_by '{sort_by}'.")
                return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT,
                                          text=TextResult(output=StringValue(
                                              value=f"No transaction metrics data found for application '{application_guid}' with sort criteria '{sort_by}'. Check application configuration and time range.")),
                                          source=self.source)

            return task_results  # Return list of results

        except Exception as e:
            logger.error(f"General Error executing APM Transaction Summary task for app {task.application_entity_name.value}: {str(e)}", exc_info=True)
            raise Exception(f"Error while executing New Relic APM Transaction Summary task: {e}")

    def _get_metric_chart_config(self, metric_name: str) -> tuple[str, str]:
        """Returns chart type and result type for a metric."""
        from core.utils.static_mappings import NEWRELIC_APM_DATABASE_QUERIES
        
        chart_config = NEWRELIC_APM_DATABASE_QUERIES.get("chart_types", {})
        
        for chart_type, config in chart_config.items():
            if metric_name in config.get("metrics", []):
                return chart_type, config.get("result_type", "TIMESERIES")
        
        # Default to timeseries for unknown metrics
        return "timeseries", "TIMESERIES"
    
    def _get_transaction_metric_chart_config(self, metric_name: str) -> tuple[str, str]:
        """Returns chart type and result type for a transaction metric."""
        from core.utils.static_mappings import NEWRELIC_APM_TRANSACTION_QUERIES
        
        chart_config = NEWRELIC_APM_TRANSACTION_QUERIES.get("chart_types", {})
        
        for chart_type, config in chart_config.items():
            if metric_name in config.get("metrics", []):
                return chart_type, config.get("result_type", "TIMESERIES")
        
        # Default to timeseries for unknown metrics
        return "timeseries", "TIMESERIES"

    def execute_entity_dashboard_widget_nrql_metric_execution(self, time_range: TimeRange, nr_task: NewRelic,
                                                              nr_connector: ConnectorProto):
        try:
            if not nr_connector:
                raise Exception("Task execution Failed:: No New Relic source found")

            task = nr_task.entity_dashboard_widget_nrql_metric_execution
            metric_name = task.widget_title.value
            if task.unit and task.unit.value:
                unit = task.unit.value
            else:
                unit = ''

            timeseries_offsets = None  # task.timeseries_offsets

            nrql_expression = task.widget_nrql_expression.value
            nrql_expression, has_timeseries = self._prepare_nrql(nrql_expression, time_range)

            nr_gql_processor = self.get_connector_processor(nr_connector)
            response = nr_gql_processor.execute_nrql_query(nrql_expression)
            if not response:
                return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT,
                                          text=TextResult(output=StringValue(
                                              value=f"No data returned from New Relic for nrql expression: {nrql_expression}")),
                                          source=self.source)

            if not has_timeseries:
                return self._convert_nrql_aggregate_to_table(response, nrql_expression, metric_name)

            labeled_metric_timeseries_list = []

            metric_function = ""
            if 'rawResponse' in response and 'metadata' in response['rawResponse'] and 'contents' in \
                    response['rawResponse']['metadata']:
                contents = response['rawResponse']['metadata'].get('contents', {}).get('timeSeries', {}).get('contents',
                                                                                                             [])
                if contents and len(contents) > 0:
                    metric_function = contents[0].get('function', '')

            # Process facets from rawResponse
            if 'rawResponse' in response and 'facets' in response['rawResponse']:
                facets = response['rawResponse']['facets']

                for facet in facets:
                    facet_name = facet.get('name', 'unknown')
                    metric_datapoints = []

                    # Process timeseries data for this facet
                    for ts in facet.get('timeSeries', []):
                        utc_timestamp = ts.get('beginTimeSeconds')
                        utc_datetime = datetime.utcfromtimestamp(utc_timestamp)
                        utc_datetime = utc_datetime.replace(tzinfo=pytz.UTC)

                        results = ts.get('results', [])
                        val = 0
                        if results and len(results) > 0:
                            val = results[0].get(metric_function, 0)

                            if val == 0 and len(results[0]) > 0:
                                first_key = next(iter(results[0]))
                                val = results[0].get(first_key, 0)

                        datapoint = TimeseriesResult.LabeledMetricTimeseries.Datapoint(
                            timestamp=int(utc_datetime.timestamp() * 1000),
                            value=DoubleValue(value=val))
                        metric_datapoints.append(datapoint)

                    # Add this facet as a labeled metric timeseries
                    labeled_metric_timeseries_list.append(
                        TimeseriesResult.LabeledMetricTimeseries(
                            metric_label_values=[
                                LabelValuePair(
                                    name=StringValue(value="facet"),
                                    value=StringValue(value=facet_name)
                                )
                            ],
                            unit=StringValue(value=unit),
                            datapoints=metric_datapoints
                        )
                    )

            # If no facets were found, try processing the results array directly
            elif 'results' in response:
                # Group results by facet
                facet_groups = {}
                for result in response['results']:
                    facet_name = result.get('facet', 'unknown')
                    if facet_name not in facet_groups:
                        facet_groups[facet_name] = []
                    facet_groups[facet_name].append(result)

                # Process each facet group
                for facet_name, results in facet_groups.items():
                    metric_datapoints = []

                    # Get the metric key (the key with the actual value)
                    metric_key = None
                    for key in results[0].keys():
                        if key not in ['facet', 'beginTimeSeconds', 'endTimeSeconds', 'segmentName']:
                            metric_key = key
                            break

                    if metric_key:
                        for result in results:
                            utc_timestamp = result.get('beginTimeSeconds')
                            utc_datetime = datetime.utcfromtimestamp(utc_timestamp)
                            utc_datetime = utc_datetime.replace(tzinfo=pytz.UTC)

                            val = result.get(metric_key, 0)
                            datapoint = TimeseriesResult.LabeledMetricTimeseries.Datapoint(
                                timestamp=int(utc_datetime.timestamp() * 1000),
                                value=DoubleValue(value=val))
                            metric_datapoints.append(datapoint)

                        labeled_metric_timeseries_list.append(
                            TimeseriesResult.LabeledMetricTimeseries(
                                metric_label_values=[
                                    LabelValuePair(
                                        name=StringValue(value="facet"),
                                        value=StringValue(value=facet_name)
                                    )
                                ],
                                unit=StringValue(value=unit),
                                datapoints=metric_datapoints
                            )
                        )

            # Create and return the final result
            timeseries_result = TimeseriesResult(
                metric_expression=StringValue(value=nrql_expression),
                metric_name=StringValue(value=metric_name),
                labeled_metric_timeseries=labeled_metric_timeseries_list
            )

            task_result = PlaybookTaskResult(
                type=PlaybookTaskResultType.TIMESERIES,
                timeseries=timeseries_result,
                source=self.source
            )

            return task_result
        except Exception as e:
            raise Exception(f"Error while executing New Relic task: {e}")

    def execute_nrql_metric_execution(self, time_range: TimeRange, nr_task: NewRelic,
                                      nr_connector: ConnectorProto):
        try:
            if not nr_connector:
                raise Exception("Task execution Failed:: No New Relic source found")

            task = nr_task.nrql_metric_execution
            metric_name = task.metric_name.value
            if task.unit and task.unit.value:
                unit = task.unit.value
            else:
                unit = ''
            timeseries_offsets = task.timeseries_offsets

            nrql_expression = task.nrql_expression.value
            nrql_expression, has_timeseries = self._prepare_nrql(nrql_expression, time_range)

            nr_gql_processor = self.get_connector_processor(nr_connector)

            print(
                "Playbook Task Downstream Request: Type -> {}, Account -> {}, Nrql_Expression -> {}".format(
                    "NewRelic", nr_connector.account_id.value, nrql_expression), flush=True)

            response = nr_gql_processor.execute_nrql_query(nrql_expression)
            if not response:
                return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT,
                                          text=TextResult(output=StringValue(
                                              value=f"No data returned from New Relic for nrql expression: {nrql_expression}")),
                                          source=self.source)

            if not has_timeseries:
                return self._convert_nrql_aggregate_to_table(response, nrql_expression, metric_name)

            labeled_metric_timeseries_list = []

            metric_function = ""
            if 'rawResponse' in response and 'metadata' in response['rawResponse'] and 'contents' in \
                    response['rawResponse']['metadata']:
                contents = response['rawResponse']['metadata'].get('contents', {}).get('timeSeries', {}).get('contents',
                                                                                                             [])
                if contents and len(contents) > 0:
                    metric_function = contents[0].get('function', '')

            if 'rawResponse' in response and 'facets' in response['rawResponse']:
                facets = response['rawResponse']['facets']

                for facet in facets:
                    facet_name = facet.get('name', 'unknown')
                    metric_datapoints = []

                    for ts in facet.get('timeSeries', []):
                        utc_timestamp = ts.get('beginTimeSeconds')
                        utc_datetime = datetime.utcfromtimestamp(utc_timestamp)
                        utc_datetime = utc_datetime.replace(tzinfo=pytz.UTC)

                        results = ts.get('results', [])
                        val = 0
                        if results and len(results) > 0:
                            val = results[0].get(metric_function, 0)

                            # Fallback to first key if function name not found
                            if val == 0 and len(results[0]) > 0:
                                first_key = next(iter(results[0]))
                                val = results[0].get(first_key, 0)

                        datapoint = TimeseriesResult.LabeledMetricTimeseries.Datapoint(
                            timestamp=int(utc_datetime.timestamp() * 1000), value=DoubleValue(value=val))
                        metric_datapoints.append(datapoint)

                    metric_label_values = [
                        LabelValuePair(name=StringValue(value='offset_seconds'), value=StringValue(value='0')),
                        LabelValuePair(name=StringValue(value='facet'), value=StringValue(value=facet_name))
                    ]

                    labeled_metric_timeseries_list.append(
                        TimeseriesResult.LabeledMetricTimeseries(
                            metric_label_values=metric_label_values,
                            unit=StringValue(value=unit),
                            datapoints=metric_datapoints
                        )
                    )

            elif 'results' in response:
                # Group results by facet if present
                facet_groups = {}
                has_facet = False

                for result in response['results']:
                    if 'facet' in result:
                        has_facet = True
                        facet_name = result.get('facet', 'unknown')
                        if facet_name not in facet_groups:
                            facet_groups[facet_name] = []
                        facet_groups[facet_name].append(result)

                if has_facet:
                    # Process each facet group
                    for facet_name, results in facet_groups.items():
                        metric_datapoints = []

                        # Get the metric key
                        metric_key = None
                        for key in results[0].keys():
                            if key not in ['facet', 'beginTimeSeconds', 'endTimeSeconds', 'segmentName']:
                                metric_key = key
                                break

                        if metric_key:
                            for result in results:
                                utc_timestamp = result.get('beginTimeSeconds')
                                utc_datetime = datetime.utcfromtimestamp(utc_timestamp)
                                utc_datetime = utc_datetime.replace(tzinfo=pytz.UTC)

                                val = result.get(metric_key, 0)
                                datapoint = TimeseriesResult.LabeledMetricTimeseries.Datapoint(
                                    timestamp=int(utc_datetime.timestamp() * 1000),
                                    value=DoubleValue(value=val))
                                metric_datapoints.append(datapoint)

                            metric_label_values = [
                                LabelValuePair(name=StringValue(value='offset_seconds'), value=StringValue(value='0')),
                                LabelValuePair(name=StringValue(value='facet'), value=StringValue(value=facet_name))
                            ]

                            labeled_metric_timeseries_list.append(
                                TimeseriesResult.LabeledMetricTimeseries(
                                    metric_label_values=metric_label_values,
                                    unit=StringValue(value=unit),
                                    datapoints=metric_datapoints
                                )
                            )
                else:
                    # No facets in results, process as a single series
                    metric_datapoints = []

                    # Find the metric key
                    metric_key = None
                    if len(response['results']) > 0:
                        for key in response['results'][0].keys():
                            if key not in ['beginTimeSeconds', 'endTimeSeconds']:
                                metric_key = key
                                break

                    if metric_key:
                        for result in response['results']:
                            utc_timestamp = result.get('beginTimeSeconds')
                            utc_datetime = datetime.utcfromtimestamp(utc_timestamp)
                            utc_datetime = utc_datetime.replace(tzinfo=pytz.UTC)

                            val = result.get(metric_key, 0)
                            datapoint = TimeseriesResult.LabeledMetricTimeseries.Datapoint(
                                timestamp=int(utc_datetime.timestamp() * 1000),
                                value=DoubleValue(value=val))
                            metric_datapoints.append(datapoint)

                        metric_label_values = [
                            LabelValuePair(name=StringValue(value='offset_seconds'), value=StringValue(value='0'))
                        ]

                        labeled_metric_timeseries_list.append(
                            TimeseriesResult.LabeledMetricTimeseries(
                                metric_label_values=metric_label_values,
                                unit=StringValue(value=unit),
                                datapoints=metric_datapoints
                            )
                        )

            # Process offset values if specified
            if timeseries_offsets:
                offsets = [offset for offset in timeseries_offsets]
                for offset in offsets:
                    adjusted_start_ms = (time_range.time_geq - offset) * 1000
                    adjusted_end_ms = (time_range.time_lt - offset) * 1000
                    adjusted_nrql_expression = re.sub(
                        r'SINCE\s+\d+\s+UNTIL\s+\d+',
                        f'SINCE {adjusted_start_ms} UNTIL {adjusted_end_ms}',
                        nrql_expression)

                    print(
                        "Playbook Task Downstream Request: Type -> {}, Account -> {}, Nrql_Expression -> {}, "
                        "Offset -> {}".format(
                            "NewRelic", nr_connector.account_id.value, adjusted_nrql_expression, offset), flush=True)

                    offset_response = nr_gql_processor.execute_nrql_query(adjusted_nrql_expression)
                    if not offset_response:
                        print(f"No data returned from New Relic for offset {offset} seconds")
                        continue

                    # Process the offset response - similar logic as above but with the offset labeled
                    if 'rawResponse' in offset_response and 'facets' in offset_response['rawResponse']:
                        facets = offset_response['rawResponse']['facets']

                        for facet in facets:
                            facet_name = facet.get('name', 'unknown')
                            offset_metric_datapoints = []

                            for ts in facet.get('timeSeries', []):
                                utc_timestamp = ts.get('beginTimeSeconds')
                                utc_datetime = datetime.utcfromtimestamp(utc_timestamp)
                                utc_datetime = utc_datetime.replace(tzinfo=pytz.UTC)

                                results = ts.get('results', [])
                                val = 0
                                if results and len(results) > 0:
                                    val = results[0].get(metric_function, 0)

                                    if val == 0 and len(results[0]) > 0:
                                        first_key = next(iter(results[0]))
                                        val = results[0].get(first_key, 0)

                                datapoint = TimeseriesResult.LabeledMetricTimeseries.Datapoint(
                                    timestamp=int(utc_datetime.timestamp() * 1000),
                                    value=DoubleValue(value=val))
                                offset_metric_datapoints.append(datapoint)

                            offset_metric_label_values = [
                                LabelValuePair(name=StringValue(value='offset_seconds'),
                                               value=StringValue(value=str(offset))),
                                LabelValuePair(name=StringValue(value='metric'),
                                               value=StringValue(value=metric_name)),
                                LabelValuePair(name=StringValue(value='facet'),
                                               value=StringValue(value=facet_name))
                            ]

                            labeled_metric_timeseries_list.append(
                                TimeseriesResult.LabeledMetricTimeseries(
                                    metric_label_values=offset_metric_label_values,
                                    unit=StringValue(value=unit),
                                    datapoints=offset_metric_datapoints
                                )
                            )

                    # Process offset response without facets
                    elif 'results' in offset_response:
                        facet_groups = {}
                        has_facet = False

                        for result in offset_response['results']:
                            if 'facet' in result:
                                has_facet = True
                                facet_name = result.get('facet', 'unknown')
                                if facet_name not in facet_groups:
                                    facet_groups[facet_name] = []
                                facet_groups[facet_name].append(result)

                        if has_facet:
                            for facet_name, results in facet_groups.items():
                                metric_datapoints = []

                                metric_key = None
                                for key in results[0].keys():
                                    if key not in ['facet', 'beginTimeSeconds', 'endTimeSeconds', 'segmentName']:
                                        metric_key = key
                                        break

                                if metric_key:
                                    for result in results:
                                        utc_timestamp = result.get('beginTimeSeconds')
                                        utc_datetime = datetime.utcfromtimestamp(utc_timestamp)
                                        utc_datetime = utc_datetime.replace(tzinfo=pytz.UTC)

                                        val = result.get(metric_key, 0)
                                        datapoint = TimeseriesResult.LabeledMetricTimeseries.Datapoint(
                                            timestamp=int(utc_datetime.timestamp() * 1000),
                                            value=DoubleValue(value=val))
                                        metric_datapoints.append(datapoint)

                                    offset_metric_label_values = [
                                        LabelValuePair(name=StringValue(value='offset_seconds'),
                                                       value=StringValue(value=str(offset))),
                                        LabelValuePair(name=StringValue(value='metric'),
                                                       value=StringValue(value=metric_name))
                                    ]

                                    labeled_metric_timeseries_list.append(
                                        TimeseriesResult.LabeledMetricTimeseries(
                                            metric_label_values=offset_metric_label_values,
                                            unit=StringValue(value=unit),
                                            datapoints=metric_datapoints
                                        )
                                    )
                        else:
                            offset_metric_datapoints = []

                            metric_key = None
                            if len(offset_response['results']) > 0:
                                for key in offset_response['results'][0].keys():
                                    if key not in ['beginTimeSeconds', 'endTimeSeconds']:
                                        metric_key = key
                                        break

                            if metric_key:
                                for result in offset_response['results']:
                                    utc_timestamp = result.get('beginTimeSeconds')
                                    utc_datetime = datetime.utcfromtimestamp(utc_timestamp)
                                    utc_datetime = utc_datetime.replace(tzinfo=pytz.UTC)

                                    val = result.get(metric_key, 0)
                                    datapoint = TimeseriesResult.LabeledMetricTimeseries.Datapoint(
                                        timestamp=int(utc_datetime.timestamp() * 1000),
                                        value=DoubleValue(value=val))
                                    metric_datapoints.append(datapoint)

                                offset_metric_label_values = [
                                    LabelValuePair(name=StringValue(value='offset_seconds'),
                                                   value=StringValue(value=str(offset))),
                                    LabelValuePair(name=StringValue(value='metric'),
                                                   value=StringValue(value=metric_name))
                                ]

                                labeled_metric_timeseries_list.append(
                                    TimeseriesResult.LabeledMetricTimeseries(
                                        metric_label_values=offset_metric_label_values,
                                        unit=StringValue(value=unit),
                                        datapoints=metric_datapoints
                                    )
                                )

            timeseries_result = TimeseriesResult(
                metric_expression=StringValue(value=nrql_expression),
                metric_name=StringValue(value=metric_name),
                labeled_metric_timeseries=labeled_metric_timeseries_list
            )
            task_result = PlaybookTaskResult(
                type=PlaybookTaskResultType.TIMESERIES,
                timeseries=timeseries_result,
                source=self.source
            )
            return task_result
        except Exception as e:
            raise Exception(f"Error while executing New Relic task: {e}")

    def _find_matching_dashboard_widgets(self, nr_connector: ConnectorProto, dashboard_name: str,
                                         page_name: Optional[str] = None, widget_names: Optional[list[str]] = None) -> \
            list[dict]:
        """Finds a dashboard by name and returns its filtered widgets."""
        client = PrototypeClient()
        assets = client.get_connector_assets(
            connector_type=Source.Name(nr_connector.type),
            connector_id=str(nr_connector.id.value),
            asset_type=SourceModelType.NEW_RELIC_ENTITY_DASHBOARD,
        )

        if not assets:
            raise Exception(f"No dashboard assets found for the account {nr_connector.account_id.value}")

        matching_widgets = []
        dashboard_found = False

        newrelic_assets = assets.new_relic.assets
        all_dashboard_entities = [newrelic_asset.new_relic_entity_dashboard for newrelic_asset
                                  in newrelic_assets if
                                  newrelic_asset.type == SourceModelType.NEW_RELIC_ENTITY_DASHBOARD]

        for dashboard_entity in all_dashboard_entities:
            current_dashboard_name = dashboard_entity.dashboard_name.value
            match = False

            if page_name:
                target_name = f"{dashboard_name} / {page_name}"
                if current_dashboard_name.lower() == target_name.lower():
                    match = True
            else:
                if current_dashboard_name.lower().startswith(dashboard_name.lower() + " / ") or \
                        current_dashboard_name.lower() == dashboard_name.lower():
                    match = True

            if match:
                dashboard_found = True
                for page in dashboard_entity.pages:
                    # If a specific page name is given, only process that page if names match case-insensitively
                    if page_name and page.page_name.value.lower() != page_name.lower():
                        continue

                    for widget in page.widgets:
                        widget_title = widget.widget_title.value if widget.widget_title.value else f"Widget {widget.widget_id.value}"
                        if widget_names and not is_partial_match(widget_title, widget_names):
                            continue

                        matching_widgets.append({
                            'title': widget_title,
                            'nrql': widget.widget_nrql_expression.value,
                            'type': widget.widget_type.value,
                            'id': widget.widget_id.value
                        })

                # If we were looking for a specific page and found it, stop searching further dashboards.
                if page_name:
                    break

        if not dashboard_found:
            raise Exception(f"Dashboard with name '{dashboard_name}' not found")

        if not matching_widgets:
            filter_msg = ""
            if page_name:
                filter_msg += f" in page '{page_name}'"
            if widget_names:
                filter_msg += f" matching names {widget_names}"
            raise Exception(f"No widgets found{filter_msg} in dashboard '{dashboard_name}'")

        return matching_widgets

    def _prepare_nrql(self, nrql_expression: str, time_range: TimeRange) -> tuple[str, bool]:
        """
        Prepares an NRQL query for execution by standardizing time range clauses
        and conditionally handling TIMESERIES based on the original query.

        Args:
            nrql_expression: The original NRQL query.
            time_range: The TimeRange object specifying the desired start and end times.

        Returns:
            A tuple of (prepared_nrql, has_timeseries) where has_timeseries indicates
            whether the query is a timeseries query.
        """
        nrql_expression = nrql_expression.strip()

        # 1. Detect if original query has TIMESERIES
        has_timeseries = bool(re.search(r'\bTIMESERIES\b', nrql_expression, re.IGNORECASE))

        # 2. Calculate desired time range in milliseconds
        start_ms = time_range.time_geq * 1000
        end_ms = time_range.time_lt * 1000
        total_seconds = time_range.time_lt - time_range.time_geq
        if total_seconds <= 0:
            end_ms = int(datetime.now(tz=pytz.UTC).timestamp() * 1000)
            start_ms = end_ms - 3600 * 1000
            total_seconds = 3600
            logger.warning(
                f"Invalid time range provided (start >= end). Defaulting to last 1 hour. Original NRQL: {nrql_expression}")

        calculated_time_range_clause = f'SINCE {start_ms} UNTIL {end_ms}'

        # 3. Extract and remove COMPARE WITH clause
        compare_with_clause = ''
        compare_with_match = re.search(
            r'(\bCOMPARE\s+WITH\s+(.*?)(?=\b(?:LIMIT|TIMESERIES|FACET)|\s*$))',
            nrql_expression, flags=re.IGNORECASE | re.DOTALL
        )
        if compare_with_match:
            compare_with_clause = ' ' + compare_with_match.group(1).strip()
            nrql_expression = nrql_expression[:compare_with_match.start(0)] + nrql_expression[compare_with_match.end(0):]
            nrql_expression = nrql_expression.strip()

        # 4. Remove existing SINCE, UNTIL clauses
        nrql_expression = re.sub(
            r'\bSINCE\s+(.*?)(?=\b(?:UNTIL|COMPARE|LIMIT|TIMESERIES|FACET)|\s*$)',
            '', nrql_expression, flags=re.IGNORECASE | re.DOTALL
        ).strip()
        nrql_expression = re.sub(
            r'\bUNTIL\s+(.*?)(?=\b(?:COMPARE|LIMIT|TIMESERIES|FACET)|\s*$)',
            '', nrql_expression, flags=re.IGNORECASE | re.DOTALL
        ).strip()

        # 5. Remove existing TIMESERIES clause (if present)
        nrql_expression = re.sub(
            r'(?:\bLIMIT\s+MAX\s+)?\bTIMESERIES(?:\s+MAX|\s+AUTO|\s+\d+\s+\w+)?',
            '', nrql_expression, flags=re.IGNORECASE
        ).strip()

        # 6. Append clauses in correct NRQL order
        nrql_expression += f' {calculated_time_range_clause}'
        if compare_with_clause:
            nrql_expression += compare_with_clause
        if has_timeseries:
            bucket_size = calculate_timeseries_bucket_size(total_seconds)
            nrql_expression += f' TIMESERIES {bucket_size} SECONDS'

        # Clean up multiple spaces
        nrql_expression = re.sub(r'\s+', ' ', nrql_expression).strip()

        return nrql_expression, has_timeseries

    def _prepare_widget_nrql(self, nrql_expression: str, time_range: TimeRange) -> str:
        """
        Prepares the NRQL query string for execution by standardizing
        the TIMESERIES and time range clauses based on the playbook context.

        Args:
            nrql_expression: The original NRQL query from the widget.
            time_range: The TimeRange object specifying the desired start and end times.

        Returns:
            The modified NRQL query string ready for execution.
        """
        nrql_expression = nrql_expression.strip()
        original_nrql = nrql_expression  # Keep a copy for logging

        # 1. Calculate desired time range in milliseconds
        start_ms = time_range.time_geq * 1000
        end_ms = time_range.time_lt * 1000
        total_seconds = time_range.time_lt - time_range.time_geq
        if total_seconds <= 0:
            # Default to 1 hour if range is invalid, recalculate timestamps
            end_ms = int(datetime.now(tz=pytz.UTC).timestamp() * 1000)
            start_ms = end_ms - 3600 * 1000
            total_seconds = 3600
            logger.warning(
                f"Invalid time range provided (start >= end). Defaulting to last 1 hour. Original NRQL: {original_nrql}")

        # 2. Calculate appropriate bucket size
        bucket_size = calculate_timeseries_bucket_size(total_seconds)
        calculated_timeseries_clause = f'TIMESERIES {bucket_size} SECONDS'
        calculated_time_range_clause = f'SINCE {start_ms} UNTIL {end_ms}'

        # 3. Extract and remove COMPARE WITH clause
        compare_with_clause = ''
        compare_with_match = re.search(
            r'(\bCOMPARE\s+WITH\s+(.*?)(?=\b(?:LIMIT|TIMESERIES|FACET)|\s*$))',
            nrql_expression, flags=re.IGNORECASE | re.DOTALL
        )
        if compare_with_match:
            # Extract the full clause including "COMPARE WITH"
            compare_with_clause = ' ' + compare_with_match.group(1).strip()
            # Remove the matched clause from the original expression
            nrql_expression = nrql_expression[:compare_with_match.start(0)] + nrql_expression[compare_with_match.end(0):]
            nrql_expression = nrql_expression.strip()


        # 4. Remove existing time range clauses (SINCE, UNTIL)
        # This regex handles various formats like 'X minutes ago', 'today', 'now', timestamps, etc.
        nrql_expression = re.sub(
            r'\bSINCE\s+(.*?)(?=\b(?:UNTIL|COMPARE|LIMIT|TIMESERIES|FACET)|\s*$)',
            '', nrql_expression, flags=re.IGNORECASE | re.DOTALL
        ).strip()
        nrql_expression = re.sub(
            r'\bUNTIL\s+(.*?)(?=\b(?:COMPARE|LIMIT|TIMESERIES|FACET)|\s*$)',
            '', nrql_expression, flags=re.IGNORECASE | re.DOTALL
        ).strip()
        # COMPARE WITH removal is handled above by extracting it first

        # 5. Remove/Replace existing TIMESERIES clause (any variation)
        # This regex aims to find 'TIMESERIES' possibly preceded by 'LIMIT MAX'
        # and followed by optional arguments ('MAX', 'AUTO', duration).
        # We replace whatever we find with an empty string first.
        nrql_expression = re.sub(
            r'(?:\bLIMIT\s+MAX\s+)?\bTIMESERIES(?:\s+MAX|\s+AUTO|\s+\d+\s+\w+)?',
            '', nrql_expression, flags=re.IGNORECASE
        ).strip()

        # 6. Append the calculated and extracted clauses in the correct NRQL order
        # Order: SINCE...UNTIL -> COMPARE WITH -> TIMESERIES
        nrql_expression += f' {calculated_time_range_clause}'
        if compare_with_clause:
            nrql_expression += compare_with_clause # Already includes leading space
        nrql_expression += f' {calculated_timeseries_clause}'

        # Clean up potential multiple spaces
        nrql_expression = re.sub(r'\s+', ' ', nrql_expression).strip()


        return nrql_expression

    # Refactored _parse_nrql_response
    def _parse_nrql_response(self, response: dict, widget_title: str) -> list[TimeseriesResult.LabeledMetricTimeseries]:
        """Parses the NRQL response and extracts timeseries data, handling COMPARE WITH and facets."""
        all_labeled_metric_timeseries = []
        if isinstance(response, list) and len(response) > 0:
             response_data = response[0]
        elif isinstance(response, dict):
             response_data = response
        else:
             logger.warning(f"Widget '{widget_title}': Unexpected response type: {type(response)}. Response: {response}")
             return []

        if not response_data:
            logger.warning(f"No data returned for widget '{widget_title}'")
            return all_labeled_metric_timeseries

        # --- Structure Detection --- 
        has_comparison = False
        is_faceted = False # Default, check later
        comparison_type = None # To track which structure was found

        # Check 1: results / previousResults structure (based on user example)
        if isinstance(response_data.get('results'), list) and isinstance(response_data.get('previousResults'), list):
            has_comparison = True
            comparison_type = 'results/previousResults'
            logger.debug(f"Widget '{widget_title}': Detected COMPARE WITH structure via results/previousResults keys.")
            # Facet check for this structure might involve checking metadata or if results contain facet keys
            if 'metadata' in response_data and isinstance(response_data['metadata'], dict) and response_data['metadata'].get('facets') is not None:
                 is_faceted = True # Assume standard facet structure applies if metadata indicates it
                 logger.debug(f"Widget '{widget_title}': Detected FACETS via metadata for results/previousResults structure.")
            # Add more specific facet checks if needed for this structure

        # Check 2: current / previous structure (fallback)
        elif 'current' in response_data and 'previous' in response_data:
            has_comparison = True
            comparison_type = 'current/previous'
            logger.debug(f"Widget '{widget_title}': Detected COMPARE WITH structure via current/previous keys.")
             # Facet check for this structure
            current_data_check = response_data.get('current', {})
            previous_data_check = response_data.get('previous', {})
            if isinstance(current_data_check, dict) and current_data_check.get('facets') or \
               isinstance(previous_data_check, dict) and previous_data_check.get('facets'):
                 is_faceted = True
                 logger.debug(f"Widget '{widget_title}': Detected FACETS for current/previous structure.")

        # --- Processing Logic --- 

        if has_comparison:
            current_results = []
            previous_results = []
            current_facets = []
            previous_facets = []

            # --- Data Extraction based on detected type --- 
            if comparison_type == 'results/previousResults':
                 current_results = response_data.get('results', [])
                 previous_results = response_data.get('previousResults', [])
                 if is_faceted:
                      # Need logic to handle facets if they don't follow current/previous pattern
                      logger.warning(f"Widget '{widget_title}': Faceted results/previousResults structure not fully implemented for parsing.")
                      is_faceted = False # Override for now

            elif comparison_type == 'current/previous':
                 current_data = response_data.get('current', {})
                 previous_data = response_data.get('previous', {})
                 if not isinstance(current_data, dict): current_data = {}
                 if not isinstance(previous_data, dict): previous_data = {}
                 
                 if is_faceted:
                     # Look in standard locations ('facets') and raw response locations
                     current_facets = current_data.get('facets', response_data.get('rawResponse', {}).get('facets', []))
                     previous_facets = previous_data.get('facets', response_data.get('compareWith', {}).get('facets', []))
                     if not isinstance(current_facets, list): current_facets = []
                     if not isinstance(previous_facets, list): previous_facets = []
                 else:
                     # Data is usually under timeSeries or results key
                     current_results = current_data.get('timeSeries', current_data.get('results', []))
                     previous_results = previous_data.get('timeSeries', previous_data.get('results', []))

            # Ensure results are lists
            if not isinstance(current_results, list): current_results = []
            if not isinstance(previous_results, list): previous_results = []

            # --- Process Data --- 
            if is_faceted:
                logger.debug(f"Widget '{widget_title}': Processing FACETED COMPARE WITH.")
                processed_facet_names = set()
                if current_facets:
                     logger.debug(f"Widget '{widget_title}': Processing {len(current_facets)} CURRENT facets.")
                     # ... (rest of current facet processing logic remains the same) ...
                     for facet_data in current_facets:
                         facet_name = facet_data.get('name', 'unknown_facet')
                         processed_facet_names.add(facet_name)
                         base_labels = [
                             LabelValuePair(name=StringValue(value='widget_name'), value=StringValue(value=widget_title)),
                             LabelValuePair(name=StringValue(value='comparison_period'), value=StringValue(value='current')),
                             LabelValuePair(name=StringValue(value='facet'), value=StringValue(value=facet_name))
                         ]
                         timeseries = facet_data.get('timeSeries', facet_data.get('results', []))
                         all_labeled_metric_timeseries.extend(self._process_timeseries_section(timeseries, widget_title, base_labels, comparison_type))
                else: logger.debug(f"Widget '{widget_title}': No CURRENT facets found.")
                
                if previous_facets:
                     logger.debug(f"Widget '{widget_title}': Processing {len(previous_facets)} PREVIOUS facets.")
                     # ... (rest of previous facet processing logic remains the same) ...
                     for facet_data in previous_facets:
                         facet_name = facet_data.get('name', 'unknown_facet')
                         if facet_name not in processed_facet_names: logger.warning(f"Widget '{widget_title}': Previous facet '{facet_name}' not found in current period.")
                         base_labels = [
                             LabelValuePair(name=StringValue(value='widget_name'), value=StringValue(value=widget_title)),
                             LabelValuePair(name=StringValue(value='comparison_period'), value=StringValue(value='previous')),
                             LabelValuePair(name=StringValue(value='facet'), value=StringValue(value=facet_name))
                         ]
                         timeseries = facet_data.get('timeSeries', facet_data.get('results', []))
                         all_labeled_metric_timeseries.extend(self._process_timeseries_section(timeseries, widget_title, base_labels, comparison_type))
                else: logger.debug(f"Widget '{widget_title}': No PREVIOUS facets found.")

            else: # Non-faceted comparison
                logger.debug(f"Widget '{widget_title}': Processing NON-FACETED COMPARE WITH ({comparison_type} structure).")
                
                logger.debug(f"Widget '{widget_title}': Processing CURRENT period data ({len(current_results)} points)." )
                current_labels = [
                    LabelValuePair(name=StringValue(value='widget_name'), value=StringValue(value=widget_title)),
                    LabelValuePair(name=StringValue(value='comparison_period'), value=StringValue(value='current'))
                ]
                all_labeled_metric_timeseries.extend(self._process_timeseries_section(current_results, widget_title, current_labels, comparison_type))

                logger.debug(f"Widget '{widget_title}': Processing PREVIOUS period data ({len(previous_results)} points).")
                previous_labels = [
                    LabelValuePair(name=StringValue(value='widget_name'), value=StringValue(value=widget_title)),
                    LabelValuePair(name=StringValue(value='comparison_period'), value=StringValue(value='previous'))
                ]
                all_labeled_metric_timeseries.extend(self._process_timeseries_section(previous_results, widget_title, previous_labels, comparison_type))

        else:
            # --- Handle Standard (Non-Compared) Response --- 
            logger.debug(f"Widget '{widget_title}': Standard response structure detected.")
            direct_results = response_data.get('results', [])
            facet_results = response_data.get('facets', response_data.get('rawResponse', {}).get('facets', [])) # Check top level and rawResponse
            if not isinstance(direct_results, list): direct_results = []
            if not isinstance(facet_results, list): facet_results = []
            comparison_type = None # No comparison

            if facet_results:
                is_faceted = True
                logger.debug(f"Widget '{widget_title}': Processing FACETED standard structure.")
                # ... (rest of standard facet processing logic remains the same) ...
                for facet_data in facet_results:
                     facet_name = facet_data.get('name', 'unknown_facet')
                     base_labels = [
                         LabelValuePair(name=StringValue(value='widget_name'), value=StringValue(value=widget_title)),
                         LabelValuePair(name=StringValue(value='facet'), value=StringValue(value=facet_name))
                     ]
                     timeseries = facet_data.get('timeSeries', facet_data.get('results', []))
                     all_labeled_metric_timeseries.extend(self._process_timeseries_section(timeseries, widget_title, base_labels, comparison_type))

            elif direct_results:
                logger.debug(f"Widget '{widget_title}': Processing NON-FACETED standard structure.")
                base_labels = [LabelValuePair(name=StringValue(value='widget_name'), value=StringValue(value=widget_title))]
                all_labeled_metric_timeseries.extend(self._process_timeseries_section(direct_results, widget_title, base_labels, comparison_type))
            else:
                logger.warning(f"Widget '{widget_title}': No 'results' or 'facets' found in standard response structure.")

        if not all_labeled_metric_timeseries:
            logger.warning(f"Widget '{widget_title}': Failed to parse any timeseries data from the response.")

        return all_labeled_metric_timeseries

    def execute_fetch_dashboard_widgets(self, time_range: TimeRange, nr_task: NewRelic,
                                        nr_connector: ConnectorProto):
        try:
            if not nr_connector:
                raise Exception("Task execution Failed:: No New Relic source found")

            task = nr_task.fetch_dashboard_widgets
            dashboard_name = task.dashboard_name.value
            page_name = task.page_name.value if task.HasField('page_name') and task.page_name.value else None
            widget_names_str = task.widget_names.value if task.HasField(
                'widget_names') and task.widget_names.value else ''
            widget_names = [name.strip() for name in widget_names_str.split(',') if name.strip()]

            nr_gql_processor = self.get_connector_processor(nr_connector)
            task_results = []

            try:
                # 1. Find dashboard and filter widgets using V2 function
                matching_widgets = self._find_matching_dashboard_widgets_v2(
                    nr_connector, dashboard_name, page_name, widget_names
                )

                # This handles potential duplicate widgets from multiple matching dashboard entities
                unique_widgets_by_content = {}
                for widget in matching_widgets:
                    # Sort NRQL expressions to ensure consistent ordering
                    sorted_expressions = sorted(widget.get('nrql_expressions', []))
                    content_key = (widget.get('title', ''), tuple(sorted_expressions))

                    # Only keep one widget with the same content
                    if content_key not in unique_widgets_by_content:
                        unique_widgets_by_content[content_key] = widget
                
                # Replace matching_widgets with the deduplicated list
                matching_widgets = list(unique_widgets_by_content.values())
                
            except Exception as e:
                # If dashboard/widgets not found, return a text result with the error
                logger.error(f"Error finding dashboard/widgets: {e}")
                return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT,
                                          text=TextResult(
                                              output=StringValue(value=f'Error finding dashboard/widgets: {e}')),
                                          source=self.source)

            # 2. Process each matching widget
            for widget in matching_widgets:
                all_widget_timeseries = [] # Initialize list to aggregate series for THIS widget
                widget_title = widget.get('title', 'Untitled Widget')
                nrql_expressions = widget.get('nrql_expressions', []) # Get list of NRQLs

                try:
                    if not nrql_expressions:
                        logger.warning(f"NewRelicSourceManager.execute_fetch_dashboard_widgets:: Skipping widget "
                                       f"'{widget_title}' as it has no NRQL queries.")
                        continue

                    # Deduplicate NRQL expressions before execution
                    unique_nrql_expressions = []
                    seen_nrql = set()
                    for nrql in nrql_expressions:
                        if nrql and nrql not in seen_nrql:
                            unique_nrql_expressions.append(nrql)
                            seen_nrql.add(nrql)

                    if not unique_nrql_expressions:
                         logger.warning(f"Skipping widget '{widget_title}' as it has no unique, non-empty NRQL queries after deduplication.")
                         continue

                    # 3. Iterate through each UNIQUE NRQL query for the widget
                    for idx, nrql in enumerate(unique_nrql_expressions):
                        try:
                            prepared_nrql = self._prepare_widget_nrql(nrql, time_range)

                            print(
                                "Playbook Task Downstream Request: Type -> {}, Account -> {}, Dashboard -> {}, Widget -> {}, Query Index -> {}, Nrql_Expression -> {}".format(
                                    "NewRelicDashboardV2", nr_connector.account_id.value, dashboard_name, widget_title, idx, prepared_nrql),
                                flush=True)

                            # 5. Execute NRQL
                            response = nr_gql_processor.execute_nrql_query(prepared_nrql)

                            # 6. Parse Response
                            labeled_metric_timeseries_list = self._parse_nrql_response(response, widget_title)

                            # 7. Add query index label and aggregate to the WIDGET's list
                            print(f"labeled_metric_timeseries_list: {len(labeled_metric_timeseries_list)}")
                            for series in labeled_metric_timeseries_list:
                                updated_labels = list(series.metric_label_values)
                                # Check if 'query_index' already exists (unlikely but safe)
                                existing_qi = next((lbl for lbl in updated_labels if lbl.name.value == 'query_index'), None)
                                if existing_qi:
                                     existing_qi.value.value = str(idx)
                                else:
                                     updated_labels.append(
                                         LabelValuePair(name=StringValue(value='query_index'), value=StringValue(value=str(idx)))
                                     )
                                # Create a new LabeledMetricTimeseries object with the updated labels
                                new_series = TimeseriesResult.LabeledMetricTimeseries(
                                    metric_label_values=updated_labels,
                                    unit=series.unit,  # Reuse unit from original series
                                    datapoints=series.datapoints # Reuse datapoints from original series
                                )
                                all_widget_timeseries.append(new_series) # Append the NEW series

                        except Exception as query_err:
                             logger.error(f"Error processing NRQL query index {idx} for widget '{widget_title}': {str(query_err)}", exc_info=True)
                             continue # Continue to next query

                    # 8. Create ONE Result PER WIDGET if any data was found across its queries
                    if all_widget_timeseries:
                        # Use a representative NRQL for the metric_expression field
                        representative_nrql = self._prepare_widget_nrql(nrql_expressions[0], time_range) if nrql_expressions else "Multiple Queries"

                        timeseries_result = TimeseriesResult(
                                             metric_expression=StringValue(value=representative_nrql),
                                             metric_name=StringValue(value=widget_title),
                                             labeled_metric_timeseries=all_widget_timeseries # Aggregated list for the widget
                                             )
                        task_result = PlaybookTaskResult(type=PlaybookTaskResultType.TIMESERIES,
                                                         timeseries=timeseries_result, source=self.source)
                        task_results.append(task_result) # Append the single result for this widget
                    else:
                        logger.warning(f"No timeseries data could be parsed for any query in widget '{widget_title}'.")

                except Exception as widget_err:
                    logger.error(f"Error processing widget '{widget_title}': {str(widget_err)}", exc_info=True)
                    continue # Continue processing other widgets even if one fails

            # Check if any results were generated across all widgets
            if not task_results:
                logger.warning(f"No data retrieved for any widgets in dashboard '{dashboard_name}'.")
                # Return a text message indicating no data was found for any widget processed.
                return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT,
                                          text=TextResult(output=StringValue(
                                              value=f"No data found for the specified widgets in dashboard '{dashboard_name}'. Check widget configurations and time range.")),
                                          source=self.source)

            # Return the list of task results (one per widget)
            return task_results

        except Exception as e:
            logger.error(f"General Error executing Fetch Dashboard Widgets task: {str(e)}", exc_info=True)
            # Raise a generic error for failures outside the widget processing loop
            raise Exception(f"Error while executing New Relic Fetch Dashboard Widgets task: {e}")

    # V2 version of the widget finder - Applying V1 Filtering Logic
    def _find_matching_dashboard_widgets_v2(self, nr_connector: ConnectorProto, dashboard_name: str,
                                             page_name: Optional[str] = None, widget_names: Optional[list[str]] = None) -> \
                list[dict]:
        """Finds V2 dashboards/widgets using the exact filtering logic from V1."""
        client = PrototypeClient()
        assets = client.get_connector_assets(
            connector_type=Source.Name(nr_connector.type),
            connector_id=str(nr_connector.id.value),
            asset_type=SourceModelType.NEW_RELIC_ENTITY_DASHBOARD_V2,
        )

        logger.info(f"NR Dashboard V2 Assets: {assets}")
        # Check if the nested assets are missing/empty
        if not assets or not assets.new_relic or not assets.new_relic.assets:
            logger.warning(f"No V2 dashboard assets found for the account {nr_connector.account_id.value}")
            # Return empty list instead of raising Exception here, let caller handle no data
            return []

        newrelic_assets = assets.new_relic.assets

        # Filter for V2 dashboard assets specifically
        all_dashboard_entities_v2 = [
            asset.new_relic_entity_dashboard_v2
            for asset in newrelic_assets
            if asset.HasField('new_relic_entity_dashboard_v2')
        ]

        if not all_dashboard_entities_v2:
             logger.warning(f"No V2 dashboard entities found in asset list for connector {nr_connector.id.value}")
             return []

        # 1. Group entities by dashboard_guid
        entities_by_guid = {}
        for entity in all_dashboard_entities_v2:
            guid = entity.dashboard_guid.value
            if guid not in entities_by_guid:
                entities_by_guid[guid] = []
            entities_by_guid[guid].append(entity)

        # 2. Find GUIDs matching the input dashboard_name using V1 logic
        matched_guids = set()
        dashboard_found = False
        for guid, entities in entities_by_guid.items():
            for entity in entities: # Check all names associated with this GUID
                current_dashboard_name = entity.dashboard_name.value
                match = False
                if page_name:
                    target_name = f"{dashboard_name} / {page_name}"
                    if current_dashboard_name.lower() == target_name.lower():
                        match = True
                else:
                    if current_dashboard_name.lower().startswith(dashboard_name.lower() + " / ") or \
                            current_dashboard_name.lower() == dashboard_name.lower():
                        match = True
                if match:
                    matched_guids.add(guid)
                    dashboard_found = True
                    # Optimization: If we find a match for this GUID, no need to check other names for the same GUID
                    break

        if not dashboard_found:
            raise Exception(f"Dashboard with name '{dashboard_name}' not found (V2 Check)")

        # 3. Collect unique widgets from matched GUIDs, respecting filters
        unique_widgets_data = {}

        for guid in matched_guids:
            processed_pages_in_guid = set()

            for entity in entities_by_guid[guid]: # Iterate entities for this matched GUID
                for page in entity.pages:
                    page_guid = page.page_guid.value
                    current_page_name = page.page_name.value

                    # Skip if page already processed for this GUID
                    if page_guid in processed_pages_in_guid:
                        continue

                    # Apply page name filter (if provided)
                    if page_name and current_page_name.lower() != page_name.lower():
                        continue

                    # Mark page as processed for this GUID
                    processed_pages_in_guid.add(page_guid)

                    # Process widgets on this unique page
                    for widget in page.widgets:
                        widget_id = widget.widget_id.value
                        widget_title = widget.widget_title.value if widget.widget_title.value else f"Widget_{widget_id}"

                        # Apply widget name filter (if provided)
                        if widget_names and not is_partial_match(widget_title, widget_names):
                            continue

                        # Use tuple key for deduplication across entities within the same GUID
                        widget_key = (guid, page_guid, widget_id)

                        if widget_key not in unique_widgets_data:
                             nrql_expressions = [expr.value for expr in widget.widget_nrql_expressions if expr.value]
                             unique_widgets_data[widget_key] = {
                                 'title': widget_title,
                                 'nrql_expressions': nrql_expressions,
                                 'type': widget.widget_type.value,
                                 'id': widget_id,
                                 'dashboard_guid': guid,
                                 'page_guid': page_guid
                             }
                        # else: Widget already found (possibly from another entity for the same GUID), ignore.

        # 4. Convert collected widgets to list
        matching_widgets = list(unique_widgets_data.values())

        if not matching_widgets:
            # Raise exception if filters resulted in no widgets
            filter_msg = ""
            if page_name: filter_msg += f" in page '{page_name}'"
            if widget_names: filter_msg += f" matching names {widget_names}"
            raise Exception(f"No widgets found{filter_msg} in dashboard '{dashboard_name}' (V2 Check)")

        return matching_widgets

    def _process_timeseries_section(self, section_data, widget_title, labels_to_add, comparison_type):
        """
        Processes a list of timeseries data points (potentially faceted) and extracts metrics.

        Args:
            section_data: The list containing timeseries data.
            widget_title: The title of the widget for labeling.
            labels_to_add: A list of base LabelValuePair protos to add to each generated series.
            comparison_type: Indicates structure ('results/previousResults', 'current/previous', or None).

        Returns:
            A list of TimeseriesResult.LabeledMetricTimeseries protos.
        """
        processed_series_list = []
        metric_datapoints_map = {} # {metric_name: [datapoints]}

        if not section_data:
            return []
        if isinstance(section_data, dict) and ('beginTimeSeconds' in section_data or 'epoch_millis' in section_data):
            timeseries_data = [section_data]
        elif isinstance(section_data, list):
            timeseries_data = section_data
        else:
            logger.warning(f"Widget '{widget_title}': Unexpected data type in _process_timeseries_section: {type(section_data)}.")
            return []

        for ts in timeseries_data:
            try:
                utc_timestamp = ts.get('beginTimeSeconds')
                if utc_timestamp is None:
                    epoch_millis = ts.get('epoch_millis')
                    if epoch_millis is not None:
                        utc_timestamp = epoch_millis / 1000
                    else:
                        continue

                utc_datetime = datetime.utcfromtimestamp(utc_timestamp).replace(tzinfo=pytz.UTC)
                timestamp_ms = int(utc_datetime.timestamp() * 1000)
                extracted_values = {} # {metric_name: value}

                # --- Value Extraction Logic --- 

                # Style 1: results/previousResults structure (metric alias is top-level key in ts)
                if comparison_type == 'results/previousResults':
                     # Iterate keys directly, skipping known metadata
                     ignore_keys = {'beginTimeSeconds', 'endTimeSeconds', 'epoch_millis', 'comparison'}
                     for key, value in ts.items():
                          if key not in ignore_keys:
                               if isinstance(value, (int, float)):
                                    extracted_values[key] = float(value)
                                    # Assume only one metric key besides metadata per ts object
                                    break 
                               elif value is None:
                                    # Handle null metric values gracefully (skip this timestamp for this metric)
                                    logger.debug(f"Widget '{widget_title}': Skipping null value for metric '{key}' at timestamp {timestamp_ms}")
                                    continue 
                               # Add handling for complex types like percentiles if needed here

                # Style 2: current/previous structure (Apdex, nested results)
                # Also used as fallback for standard non-compared results
                else: 
                     results_list = ts.get('results', [])
                     if isinstance(results_list, list) and len(results_list) > 0:
                         result_item = results_list[0]
                         if isinstance(result_item, dict):
                             found_primary_metric = False
                             # Prioritize Apdex Score
                             if 'score' in result_item and isinstance(result_item['score'], (int, float)):
                                 extracted_values['apdex_score'] = float(result_item['score'])
                                 found_primary_metric = True
                             
                             # If not Apdex, check other aggregates
                             if not found_primary_metric:
                                 potential_metric_keys = ['count', 'average', 'sum', 'min', 'max', 'median', 'result']
                                 for p_key in potential_metric_keys:
                                     if p_key in result_item:
                                         val = result_item[p_key]
                                         if isinstance(val, (int, float)):
                                             extracted_values[p_key] = float(val)
                                             # Consider breaking if only one expected
                                         elif val is None:
                                              logger.debug(f"Widget '{widget_title}': Skipping null value for metric '{p_key}' in nested result at timestamp {timestamp_ms}")

                             # Handle Percentiles 
                             if 'percentiles' in result_item and isinstance(result_item['percentiles'], dict):
                                 for perc_key, perc_val in result_item['percentiles'].items():
                                     if isinstance(perc_val, (int, float)):
                                         try:
                                             metric_key_suffix = str(perc_key).replace('.', '_')
                                             extracted_values[f'percentile_{metric_key_suffix}'] = float(perc_val)
                                         except ValueError:
                                             logger.debug(f"Widget '{widget_title}': Non-numeric percentile key '{perc_key}'")
                                     elif perc_val is None:
                                          logger.debug(f"Widget '{widget_title}': Skipping null value for percentile '{perc_key}' at timestamp {timestamp_ms}")

                     # Fallback: Check top-level keys (if no nested 'results' or empty)
                     elif not extracted_values: 
                         ignore_keys = {'beginTimeSeconds', 'endTimeSeconds', 'epoch_millis', 'name', 'facet', 'results'}
                         for key, item_value in ts.items():
                             if key not in ignore_keys:
                                 if key == 'apdex' and isinstance(item_value, dict) and 'score' in item_value and isinstance(item_value['score'], (int, float)):
                                     if 'apdex_score' not in extracted_values:
                                         extracted_values['apdex_score'] = float(item_value['score'])
                                 elif isinstance(item_value, (int, float)):
                                     if key not in extracted_values:
                                         extracted_values[key] = float(item_value)
                                 elif item_value is None:
                                     logger.debug(f"Widget '{widget_title}': Skipping null top-level value for key '{key}' at timestamp {timestamp_ms}")

                # --- Add extracted datapoints --- 
                if not extracted_values:
                     # Log if a timeslice yields no metrics after trying all methods
                     logger.debug(f"Widget '{widget_title}': No metric values extracted for timestamp {timestamp_ms}. Data: {ts}")

                for metric_name, val in extracted_values.items():
                    datapoint = TimeseriesResult.LabeledMetricTimeseries.Datapoint(
                        timestamp=timestamp_ms,
                        value=DoubleValue(value=val))

                    if metric_name not in metric_datapoints_map:
                        metric_datapoints_map[metric_name] = []
                    metric_datapoints_map[metric_name].append(datapoint)

            except (ValueError, TypeError, KeyError, AttributeError) as e:
                logger.warning(
                    f"Widget '{widget_title}': Error processing data point within timestamp {ts}: {str(e)}")
                continue

        # --- Create LabeledMetricTimeseries objects --- 
        if not metric_datapoints_map:
            # It's okay if a section has no data, just return empty
            # logger.debug(f"Widget '{widget_title}': No valid datapoints found for any metric in this section.")
            return []

        for metric_name, datapoints in metric_datapoints_map.items():
            if not datapoints: continue

            final_labels = list(labels_to_add)
            final_labels.append(LabelValuePair(name=StringValue(value='metric'), value=StringValue(value=metric_name)))

            processed_series_list.append(
                TimeseriesResult.LabeledMetricTimeseries(
                    metric_label_values=final_labels,
                    unit=StringValue(value=''),
                    datapoints=datapoints
                )
            )

        return processed_series_list

    def execute_get_dashboard_variable_values(self, time_range: TimeRange, nr_task: NewRelic,
                                               nr_connector: ConnectorProto):
        """
        Execute a task to get dashboard variable values.
        
        Args:
            time_range: The time range (not used for this task)
            nr_task: The New Relic task containing dashboard variable values information
            nr_connector: The New Relic connector to use
            
        Returns:
            A PlaybookTaskResult containing dashboard variable values as API response
        """
        try:
            if not nr_connector:
                raise Exception("Task execution Failed:: No New Relic source found")

            task = nr_task.get_dashboard_variable_values
            dashboard_guid = task.dashboard_guid.value
            variable_name = task.variable_name.value if task.HasField('variable_name') and task.variable_name.value else None

            nr_gql_processor = self.get_connector_processor(nr_connector)

            print(
                f"Playbook Task Downstream Request: Type -> New Relic GET_DASHBOARD_VARIABLE_VALUES, Dashboard_GUID -> {dashboard_guid}, Variable_Name -> {variable_name}",
                flush=True,
            )

            # Get dashboard variable values
            response_data = nr_gql_processor.get_dashboard_variable_values(dashboard_guid, variable_name)

            # Import required modules
            from google.protobuf.struct_pb2 import Struct
            from google.protobuf.wrappers_pb2 import UInt64Value
            from core.utils.proto_utils import dict_to_proto

            if not response_data or not response_data.get('variables'):
                error_struct = Struct()
                error_message = f"Dashboard not found or no variables found for dashboard GUID: {dashboard_guid}"
                if variable_name:
                    error_message += f", variable: {variable_name}"
                error_struct.update({"error": error_message})
                
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.API_RESPONSE,
                    api_response=ApiResponseResult(
                        response_status=UInt64Value(value=404),
                        response_body=error_struct
                    ),
                    source=self.source
                )

            # Convert response data to Struct
            if isinstance(response_data, dict):
                response_struct = Struct()
                response_struct.update(response_data)
            else:
                response_struct = dict_to_proto(response_data, Struct)

            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                api_response=ApiResponseResult(
                    response_status=UInt64Value(value=200),
                    response_body=response_struct
                ),
                source=self.source
            )

        except Exception as e:
            logger.error(f"Error while executing get dashboard variable values task: {e}")
            from google.protobuf.struct_pb2 import Struct
            from google.protobuf.wrappers_pb2 import UInt64Value
            
            error_struct = Struct()
            error_struct.update({"error": f"Error executing dashboard variable values task: {str(e)}"})
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                api_response=ApiResponseResult(
                    response_status=UInt64Value(value=500),
                    response_body=error_struct
                ),
                source=self.source
            )

    def execute_fetch_alert_conditions(self, time_range: TimeRange, nr_task: NewRelic,
                                      nr_connector: ConnectorProto):
        """
        Execute a task to fetch all alert conditions (alert rule configurations) from New Relic.
        
        Args:
            time_range: The time range (not used for this task)
            nr_task: The New Relic task
            nr_connector: The New Relic connector to use
            
        Returns:
            A PlaybookTaskResult containing alert conditions as API response
        """
        try:
            if not nr_connector:
                from google.protobuf.struct_pb2 import Struct
                from google.protobuf.wrappers_pb2 import UInt64Value
                
                response_data = {
                    "error": "Task execution Failed:: No New Relic source found",
                    "conditions": [],
                    "count": 0
                }
                response_struct = Struct()
                response_struct.update(response_data)
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.API_RESPONSE,
                    api_response=ApiResponseResult(
                        response_status=UInt64Value(value=500),
                        response_body=response_struct
                    ),
                    source=self.source
                )

            nr_gql_processor = self.get_connector_processor(nr_connector)

            print(
                f"Playbook Task Downstream Request: Type -> New Relic FETCH_ALERT_CONDITIONS",
                flush=True,
            )

            # Fetch all alert conditions (pagination handled internally)
            all_conditions = []
            cursor = None  # None will be converted to 'null' in the processor
            
            while True:
                conditions_result = nr_gql_processor.get_all_conditions(cursor)
                if not conditions_result:
                    break
                
                conditions = conditions_result.get('nrqlConditions', [])
                if conditions:
                    all_conditions.extend(conditions)
                
                # Check if there are more pages
                next_cursor = conditions_result.get('nextCursor')
                if not next_cursor or next_cursor == 'null':
                    break
                
                cursor = next_cursor

            # Convert response data to structured response
            response_data = {
                "conditions": all_conditions,
                "count": len(all_conditions),
                "total_count": conditions_result.get('totalCount', len(all_conditions)) if conditions_result else len(all_conditions)
            }

            # Import required modules
            from google.protobuf.struct_pb2 import Struct
            from google.protobuf.wrappers_pb2 import UInt64Value
            from core.utils.proto_utils import dict_to_proto

            # Convert response data to Struct
            if isinstance(response_data, dict):
                response_struct = Struct()
                response_struct.update(response_data)
            else:
                response_struct = dict_to_proto(response_data, Struct)

            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                api_response=ApiResponseResult(
                    response_status=UInt64Value(value=200),
                    response_body=response_struct
                ),
                source=self.source
            )

        except Exception as e:
            logger.error(f"Error while executing fetch alert conditions task: {e}", exc_info=True)
            from google.protobuf.struct_pb2 import Struct
            from google.protobuf.wrappers_pb2 import UInt64Value
            
            # Return error as API_RESPONSE
            response_data = {
                "error": f"Error executing fetch alert conditions task: {str(e)}",
                "conditions": [],
                "count": 0
            }
            response_struct = Struct()
            response_struct.update(response_data)

            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                api_response=ApiResponseResult(
                    response_status=UInt64Value(value=500),
                    response_body=response_struct
                ),
                source=self.source
            )

