import json
import logging
import re
import string
import requests
import ast
import urllib.parse
from typing import Optional, Union

from google.protobuf.struct_pb2 import Struct
from google.protobuf.wrappers_pb2 import DoubleValue, StringValue, Int64Value, UInt64Value, BoolValue

from core.integrations.source_api_processors.grafana_api_processor import GrafanaApiProcessor
from core.integrations.source_manager import SourceManager
from core.integrations.source_metadata_extractors.grafana_metadata_extractor import GrafanaSourceMetadataExtractor
from core.protos.base_pb2 import Source, SourceModelType, TimeRange
from core.protos.connectors.connector_pb2 import Connector as ConnectorProto
from core.protos.literal_pb2 import Literal, LiteralType
from core.protos.playbooks.playbook_commons_pb2 import (
    ApiResponseResult,
    LabelValuePair,
    PlaybookTaskResult,
    PlaybookTaskResultType,
    TextResult,
    TimeseriesResult,
    TableResult
)
from core.protos.playbooks.source_task_definitions.grafana_task_pb2 import Grafana
from core.protos.ui_definition_pb2 import FormField, FormFieldType
from core.protos.base_pb2 import Source, SourceKeyType
from core.utils.credentilal_utils import generate_credentials_dict, get_connector_key_type_string, DISPLAY_NAME, CATEGORY
from core.utils.proto_utils import dict_to_proto, proto_to_dict

logger = logging.getLogger(__name__)


def _convert_timestamp_to_grafana_time(timestamp_seconds):
    """Convert timestamp to Grafana time format (always use absolute time for precision)."""
    if not timestamp_seconds:
        return "now"
    
    try:
        # Always use absolute time format for precision
        # Convert to milliseconds for Grafana
        timestamp_ms = int(timestamp_seconds * 1000)
        return str(timestamp_ms)
    except Exception:
        return "now"


def buildGrafanaUrl(host_url: str, task_type: str, params: dict = None) -> str:
    """
    Build Grafana URLs for different task types.
    
    Args:
        host_url: Base Grafana host URL from connector
        task_type: Type of task ("dashboard", "datasource", "explore", "variables")
        params: Dictionary containing task-specific parameters
    
    Returns:
        Complete Grafana URL for the specific task type
    """
    if not host_url:
        return ""
    
    # Remove trailing slash from host URL if present
    base_url = host_url.rstrip('/')
    
    if params is None:
        params = {}
    
    if task_type == "dashboard":
        if 'dashboard_uid' in params:
            # Build dashboard URL with time range and variables
            url_params = []
            
            # Add time range parameters
            if 'from' in params:
                url_params.append(f"from={params['from']}")
            if 'to' in params:
                url_params.append(f"to={params['to']}")
            if 'timezone' in params:
                url_params.append(f"timezone={params['timezone']}")
            
            # Add template variables
            for key, value in params.items():
                if key.startswith('var-'):
                    url_params.append(f"{key}={urllib.parse.quote(str(value))}")
                elif key.startswith('var_'):
                    # Convert var_ to var- format
                    var_name = key[4:]  # Remove 'var_' prefix
                    url_params.append(f"var-{var_name}={urllib.parse.quote(str(value))}")
            
            # Add orgId if provided
            if 'orgId' in params:
                url_params.append(f"orgId={params['orgId']}")
            
            query_string = "&".join(url_params)
            return f"{base_url}/d/{params['dashboard_uid']}?{query_string}" if query_string else f"{base_url}/d/{params['dashboard_uid']}"
        else:
            return f"{base_url}/dashboards"
    
    elif task_type == "datasource":
        if 'datasource_uid' in params:
            # Build datasource explore URL using the new panes format
            url_params = []
            
            # Add schema version for new explore format
            url_params.append("schemaVersion=1")
            
            # Build panes parameter for new explore format
            if 'query' in params:
                # Create the panes structure
                panes_data = {
                    "vzh": {  # Using a consistent pane ID
                        "datasource": params['datasource_uid'],
                        "queries": [{
                            "refId": "A",
                            "expr": params['query'],
                            "range": True,
                            "datasource": {
                                "type": "prometheus",  # Default to prometheus, could be made configurable
                                "uid": params['datasource_uid']
                            },
                            "editorMode": "code",
                            "legendFormat": "__auto"
                        }],
                        "range": {
                            "from": params.get('from', 'now-1h'),
                            "to": params.get('to', 'now')
                        }
                    }
                }
                
                # Add panes parameter
                url_params.append(f"panes={urllib.parse.quote(json.dumps(panes_data))}")
            
            # Add time range parameters (for backward compatibility)
            if 'from' in params:
                url_params.append(f"from={params['from']}")
            if 'to' in params:
                url_params.append(f"to={params['to']}")
            if 'timezone' in params:
                url_params.append(f"timezone={params['timezone']}")
            
            # Add datasource UID (for backward compatibility)
            url_params.append(f"datasource={params['datasource_uid']}")
            
            # Add query if provided (for backward compatibility)
            if 'query' in params:
                url_params.append(f"queries={urllib.parse.quote(json.dumps([{'expr': params['query'], 'refId': 'A'}]))}")
            
            # Add orgId if provided
            if 'orgId' in params:
                url_params.append(f"orgId={params['orgId']}")
            
            query_string = "&".join(url_params)
            return f"{base_url}/explore?{query_string}" if query_string else f"{base_url}/explore"
        else:
            return f"{base_url}/datasources"
    
    elif task_type == "variables":
        if 'dashboard_uid' in params:
            # Build dashboard variables edit URL
            url_params = []
            
            # Add time range parameters
            if 'from' in params:
                url_params.append(f"from={params['from']}")
            if 'to' in params:
                url_params.append(f"to={params['to']}")
            if 'timezone' in params:
                url_params.append(f"timezone={params['timezone']}")
            
            # Add template variables
            for key, value in params.items():
                if key.startswith('var-'):
                    url_params.append(f"{key}={urllib.parse.quote(str(value))}")
                elif key.startswith('var_'):
                    # Convert var_ to var- format
                    var_name = key[4:]  # Remove 'var_' prefix
                    url_params.append(f"var-{var_name}={urllib.parse.quote(str(value))}")
            
            # Add orgId if provided
            if 'orgId' in params:
                url_params.append(f"orgId={params['orgId']}")
            
            # Add editview=variables to show variables panel
            url_params.append("editview=variables")
            
            query_string = "&".join(url_params)
            return f"{base_url}/d/{params['dashboard_uid']}?{query_string}" if query_string else f"{base_url}/d/{params['dashboard_uid']}?editview=variables"
        else:
            return f"{base_url}/dashboards"
    
    elif task_type == "explore":
        # Build explore URL for datasource queries using the new panes format
        url_params = []
        
        # Add schema version for new explore format
        url_params.append("schemaVersion=1")
        
        # Build panes parameter for new explore format
        if 'datasource_uid' in params and 'query' in params:
            # Create the panes structure
            panes_data = {
                "vzh": {  # Using a consistent pane ID
                    "datasource": params['datasource_uid'],
                    "queries": [{
                        "refId": "A",
                        "expr": params['query'],
                        "range": True,
                        "datasource": {
                            "type": "prometheus",  # Default to prometheus, could be made configurable
                            "uid": params['datasource_uid']
                        },
                        "editorMode": "code",
                        "legendFormat": "__auto"
                    }],
                    "range": {
                        "from": params.get('from', 'now-1h'),
                        "to": params.get('to', 'now')
                    }
                }
            }
            
            # Add panes parameter
            url_params.append(f"panes={urllib.parse.quote(json.dumps(panes_data))}")
        
        # Add time range parameters (for backward compatibility)
        if 'from' in params:
            url_params.append(f"from={params['from']}")
        if 'to' in params:
            url_params.append(f"to={params['to']}")
        if 'timezone' in params:
            url_params.append(f"timezone={params['timezone']}")
        
        # Add datasource UID (for backward compatibility)
        if 'datasource_uid' in params:
            url_params.append(f"datasource={params['datasource_uid']}")
        
        # Add query if provided (for backward compatibility)
        if 'query' in params:
            url_params.append(f"queries={urllib.parse.quote(json.dumps([{'expr': params['query'], 'refId': 'A'}]))}")
        
        # Add orgId if provided
        if 'orgId' in params:
            url_params.append(f"orgId={params['orgId']}")
        
        query_string = "&".join(url_params)
        return f"{base_url}/explore?{query_string}" if query_string else f"{base_url}/explore"
    
    else:
        logger.warning(f"Unsupported Grafana task type: {task_type}")
        return base_url


class GrafanaSourceManager(SourceManager):
    # Constants for dynamic interval calculation
    MAX_DATA_POINTS = 70
    MIN_STEP_SIZE_SECONDS = 60  # Minimum interval is 1 minute

    # Duration thresholds (seconds) mapped to minimum bucket size (seconds)
    _INTERVAL_THRESHOLDS_SECONDS = [
        (2592001, 43200),  # > 30 days -> 12h minimum step size
        (604801, 21600),  # > 7 days -> 6h minimum step size
        (86401, 10800),  # > 1 day -> 40m minimum step size
        (43201, 3600),  # > 12 hours -> 20m minimum step size
        (21601, 1800),  # > 6 hours -> 10m minimum step size
        (3601, 120),  # > 1 hour -> 2m minimum step size
        (1801, 60),  # > 30 minutes -> 1m minimum step size
    ]

    # Standard bucket sizes (seconds) to round up to
    STANDARD_STEP_SIZES_SECONDS = [30, 60, 120, 300, 600, 900, 1800, 3600, 10800, 21600, 43200, 86400]

    def __init__(self):
        self.source = Source.GRAFANA
        self.task_proto = Grafana
        self.task_type_callable_map = {
            Grafana.TaskType.PROMETHEUS_DATASOURCE_METRIC_EXECUTION: {
                "executor": self.execute_prometheus_datasource_metric_execution,
                "model_types": [SourceModelType.GRAFANA_PROMETHEUS_DATASOURCE],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "Query any of your Prometheus Data Sources from Grafana",
                "category": "Metrics",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="datasource_uid"),
                        display_name=StringValue(value="Data Source UID"),
                        description=StringValue(value="Select Data Source UID "),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TYPING_DROPDOWN_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="interval"),
                        display_name=StringValue(value="Step Size(Seconds)"),
                        description=StringValue(value="(Optional)Enter Step Size"),
                        data_type=LiteralType.LONG,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                    ),
                    FormField(
                        key_name=StringValue(value="query_type"),
                        display_name=StringValue(value="Query Type"),
                        description=StringValue(value='Select Query Type'),
                        data_type=LiteralType.STRING,
                        default_value=Literal(literal_type=LiteralType.STRING, string=StringValue(value="PromQL")),
                        valid_values=[
                            Literal(literal_type=LiteralType.STRING, string=StringValue(value="PromQL")),
                            Literal(literal_type=LiteralType.STRING, string=StringValue(value="Flux")),
                        ],
                        form_field_type=FormFieldType.DROPDOWN_FT,
                        is_optional=True,
                    ),
                    FormField(
                        key_name=StringValue(value="promql_expression"),
                        display_name=StringValue(value="Query Expression"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.MULTILINE_FT,
                    ),
                ],
            },
            Grafana.TaskType.EXECUTE_ALL_DASHBOARD_PANELS: {
                "executor": self.execute_all_dashboard_panels,
                "model_types": [SourceModelType.GRAFANA_DASHBOARD],
                "result_type": PlaybookTaskResultType.TIMESERIES,
                "display_name": "Execute queries for panels in a Grafana Dashboard",
                "category": "Metrics",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="dashboard_uid"),
                        display_name=StringValue(value="Dashboard UID"),
                        description=StringValue(value="Select Dashboard UID"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TYPING_DROPDOWN_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="panel_ids"),
                        display_name=StringValue(value="Panel IDs (Optional, Comma-separated)"),
                        description=StringValue(
                            value="Enter comma-separated panel IDs to execute only specific panels. Leave blank to execute all."),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                    ),
                    FormField(
                        key_name=StringValue(value="interval"),
                        display_name=StringValue(value="Step Size(Seconds)"),
                        description=StringValue(value="(Optional)Enter Step Size"),
                        data_type=LiteralType.LONG,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                    ),
                    FormField(
                        key_name=StringValue(value="template_variables"),
                        display_name=StringValue(value="Template Variables"),
                        description=StringValue(value="Template Variables"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.MULTILINE_FT,
                        is_optional=True,
                    ),
                ],
            },
            Grafana.TaskType.FETCH_DASHBOARD_VARIABLES: {
                "executor": self.execute_fetch_dashboard_variables,
                "model_types": [SourceModelType.GRAFANA_DASHBOARD],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "Fetch all variables and their values from a Grafana Dashboard",
                "category": "Metrics",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="dashboard_uid"),
                        display_name=StringValue(value="Dashboard UID"),
                        description=StringValue(value="Select Dashboard UID to fetch variables from"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TYPING_DROPDOWN_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="fixed_variables"),
                        display_name=StringValue(value="Fixed Variables"),
                        description=StringValue(value="(Optional) JSON object with variable names and their fixed values. Example: {\"namespace\": \"production\", \"cluster\": \"us-west-1\"}"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.MULTILINE_FT,
                        is_optional=True,
                    ),
                ],
            },
            Grafana.TaskType.LOKI_DATASOURCE_LOG_QUERY: {
                "executor": self.execute_loki_datasource_log_query,
                "model_types": [SourceModelType.GRAFANA_LOKI_DATASOURCE],
                "result_type": PlaybookTaskResultType.TABLE,
                "display_name": "Query logs from Loki Data Source",
                "category": "Logs",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="datasource_uid"),
                        display_name=StringValue(value="Data Source UID"),
                        description=StringValue(value="Select Loki Data Source UID"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TYPING_DROPDOWN_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="logql_query"),
                        display_name=StringValue(value="LogQL Query"),
                        description=StringValue(value="Enter LogQL query expression (e.g., {app=\"myapp\"} |= `error`)"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.MULTILINE_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="max_lines"),
                        display_name=StringValue(value="Max Lines"),
                        description=StringValue(value="Maximum number of log lines to return (default: 1000)"),
                        data_type=LiteralType.LONG,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                    ),
                    FormField(
                        key_name=StringValue(value="direction"),
                        display_name=StringValue(value="Direction"),
                        description=StringValue(value="Query direction: backward (newest first) or forward (oldest first)"),
                        data_type=LiteralType.STRING,
                        default_value=Literal(literal_type=LiteralType.STRING, string=StringValue(value="backward")),
                        valid_values=[
                            Literal(literal_type=LiteralType.STRING, string=StringValue(value="backward")),
                            Literal(literal_type=LiteralType.STRING, string=StringValue(value="forward")),
                        ],
                        form_field_type=FormFieldType.DROPDOWN_FT,
                        is_optional=True,
                    ),
                ],
            },
        }
        self.connector_form_configs = [
            {
                "name": StringValue(value="Grafana Configuration"),
                "description": StringValue(
                    value="Connect to Grafana using Host and API Key. SSL verification is optional."),
                "form_fields": {
                    SourceKeyType.GRAFANA_HOST: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.GRAFANA_HOST)),
                        display_name=StringValue(value="Grafana Host"),
                        description=StringValue(
                            value='e.g. "https://grafana.example.com", "http://localhost:3000", "https://my-grafana.internal:3000"'),
                        helper_text=StringValue(
                            value="Enter your Grafana instance URL including protocol (http/https) and port if non-standard"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                    ),
                    SourceKeyType.GRAFANA_API_KEY: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.GRAFANA_API_KEY)),
                        display_name=StringValue(value="API Key"),
                        description=StringValue(value='e.g. "glsa_1234567890abcdef1234567890abcdef"'),
                        helper_text=StringValue(
                            value="Enter your Grafana API Key from Configuration > API Keys with Admin or Editor role"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                        is_sensitive=True
                    ),
                    SourceKeyType.GRAFANA_TEAM_HOST: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.GRAFANA_TEAM_HOST)),
                        display_name=StringValue(value="Grafana Team Host"),
                        description=StringValue(value="Host name of team (if different from API host)"),
                        helper_text=StringValue(
                            value="Optional: Enter a different host URL for team-specific Grafana access. If not provided, the main Grafana Host will be used."),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                    ),
                    SourceKeyType.SSL_VERIFY: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.SSL_VERIFY)),
                        display_name=StringValue(value="SSL Verify"),
                        description=StringValue(
                            value="Enable or disable SSL certificate verification. Defaults to true."),
                        data_type=LiteralType.BOOLEAN,
                        form_field_type=FormFieldType.CHECKBOX_FT,
                        is_optional=True,
                        default_value=Literal(literal_type=LiteralType.BOOLEAN, boolean=BoolValue(value=True)),
                    ),
                }
            }
        ]
        self.connector_type_details = {
            DISPLAY_NAME: "GRAFANA",
            CATEGORY: APPLICATION_MONITORING,
        }

    def get_connector_processor(self, grafana_connector, **kwargs):
        generated_credentials = generate_credentials_dict(grafana_connector.type, grafana_connector.keys)
        return GrafanaApiProcessor(**generated_credentials)

    def _extract_host_url_from_connector(self, grafana_connector: ConnectorProto) -> str:
        """Extract the host URL from the Grafana connector."""
        if not grafana_connector or not grafana_connector.keys:
            return ""
        
        for key in grafana_connector.keys:
            if key.key_type == SourceKeyType.GRAFANA_HOST and key.key.value:
                return key.key.value
        
        return ""

    def _extract_team_host_url_from_connector(self, grafana_connector: ConnectorProto) -> str:
        """Extract the team host URL from the Grafana connector."""
        if not grafana_connector or not grafana_connector.keys:
            return ""
        
        for key in grafana_connector.keys:
            if key.key_type == SourceKeyType.GRAFANA_TEAM_HOST and key.key.value:
                return key.key.value
        
        return ""

    def _get_effective_host_url(self, grafana_connector: ConnectorProto) -> str:
        """Get the effective host URL, preferring team host if available, otherwise falling back to main host."""
        team_host = self._extract_team_host_url_from_connector(grafana_connector)
        if team_host:
            return team_host
        
        return self._extract_host_url_from_connector(grafana_connector)

    def _create_metadata_with_grafana_url(self, grafana_connector: ConnectorProto, task_type: str, params: dict = None) -> Struct:
        """Create metadata struct with Grafana URL using effective host (team host if available, otherwise main host)."""
        effective_host_url = self._get_effective_host_url(grafana_connector)
        
        # When using team host, remove orgId from params as it might not be needed
        if params is None:
            params = {}
        else:
            # Create a copy to avoid modifying the original params
            params = params.copy()
        
        # Check if we're using team host
        team_host = self._extract_team_host_url_from_connector(grafana_connector)
        if team_host and effective_host_url == team_host:
            # Remove orgId when using team host
            params.pop("orgId", None)
        print(f"Effective host URL: {effective_host_url}, params: {params}")
        grafana_url = buildGrafanaUrl(effective_host_url, task_type, params)
        metadata_dict = {
            "link": grafana_url
        }
        return dict_to_proto(metadata_dict, Struct)

    def _get_grafana_time_params(self, time_range: TimeRange) -> dict:
        """Get properly formatted time parameters for Grafana URLs using exact timestamps."""
        from_time = _convert_timestamp_to_grafana_time(time_range.time_geq)
        to_time = _convert_timestamp_to_grafana_time(time_range.time_lt)
        return {
            "from": from_time,
            "to": to_time,
            "timezone": "utc"
        }

    def _format_variable_dependency_instructions(self, variable_dependencies: dict) -> str:
        """
        Formats variable dependencies into human-readable instructions for the agent.
        
        Args:
            variable_dependencies: Dict mapping variable names to their dependencies
            
        Returns:
            Human-readable string explaining how to use fixed_variables for dependent variables
        """
        if not variable_dependencies:
            return ""
        
        instructions = []
        
        # Find variables that have dependencies
        dependent_variables = {var: deps for var, deps in variable_dependencies.items() if deps}
        
        if not dependent_variables:
            return "All variables are independent and can be queried without fixing other variables."
        
        # Group variables by their dependencies for cleaner instructions
        dependency_groups = {}
        for var_name, deps in dependent_variables.items():
            deps_key = tuple(sorted(deps))  # Use sorted tuple as key for grouping
            if deps_key not in dependency_groups:
                dependency_groups[deps_key] = []
            dependency_groups[deps_key].append(var_name)
        
        # Generate instructions for each dependency group
        for deps_tuple, var_list in dependency_groups.items():
            deps_list = list(deps_tuple)
            
            if len(var_list) == 1:
                var_name = var_list[0]
                if len(deps_list) == 1:
                    instructions.append(
                        f"'{var_name}' variable depends on '{deps_list[0]}'. "
                        f"To get {var_name} values for a specific {deps_list[0]}, "
                        f"use predefined_variables field: {{'{deps_list[0]}': 'your_value'}}"
                    )
                else:
                    deps_str = "', '".join(deps_list)
                    deps_json = ", ".join([f"'{dep}': 'your_value'" for dep in deps_list])
                    instructions.append(
                        f"'{var_name}' variable depends on '{deps_str}'. "
                        f"To get {var_name} values for specific values, "
                        f"use predefined_variables field: {{{deps_json}}}"
                    )
            else:
                var_names_str = "', '".join(var_list)
                if len(deps_list) == 1:
                    instructions.append(
                        f"Variables '{var_names_str}' depend on '{deps_list[0]}'. "
                        f"To get their values for a specific {deps_list[0]}, "
                        f"use predefined_variables field: {{'{deps_list[0]}': 'your_value'}}"
                    )
                else:
                    deps_str = "', '".join(deps_list)
                    deps_json = ", ".join([f"'{dep}': 'your_value'" for dep in deps_list])
                    instructions.append(
                        f"Variables '{var_names_str}' depend on '{deps_str}'. "
                        f"To get their values for specific values, "
                        f"use predefined_variables field: {{{deps_json}}}"
                    )
        
        # Add general instruction
        if instructions:
            instructions.append(
                "Note: When using predefined_variables, the dependent variables will be resolved "
                "based on the fixed values, allowing you to get context-specific results."
            )
        
        return " ".join(instructions)

    def execute_prometheus_datasource_metric_execution(self, time_range: TimeRange, grafana_task: Grafana,
                                                       grafana_connector: ConnectorProto):
        try:
            if not grafana_connector:
                raise Exception("Task execution Failed:: No Grafana source found")

            task = grafana_task.prometheus_datasource_metric_execution
            datasource_uid = task.datasource_uid.value
            interval = task.interval.value
            if interval is None:
                interval = 60
            interval_ms = interval * 1000
            query_type = task.query_type.value if task.query_type and task.query_type.value else "PromQL"
            metric_query = task.promql_expression.value

            grafana_api_processor = self.get_connector_processor(grafana_connector)

            if query_type == 'Flux':
                queries = [
                    {"query": metric_query, "datasource": {"uid": datasource_uid}, "refId": "A",
                     'rawQuery': True}
                ]
            else:
                queries = [{"expr": metric_query, "datasource": {"uid": datasource_uid}, "refId": "A"}]

            print(
                f"Playbook Task Downstream Request: Type -> Grafana, Datasource_Uid -> {datasource_uid}, "
                f"Promql_Metric_Query -> {metric_query}, Offset -> 0",
                flush=True,
            )

            formatted_queries = self._format_query_step_interval(queries, time_range)

            response = grafana_api_processor.panel_query_datasource_api(tr=time_range, queries=formatted_queries,
                                                                        interval_ms=interval_ms)

            if not response:
                # Create metadata with Grafana URL using effective host
                time_params = self._get_grafana_time_params(time_range)
                metadata = self._create_metadata_with_grafana_url(grafana_connector, "explore", {
                    "datasource_uid": datasource_uid,
                    "query": metric_query,
                    "orgId": "1",
                    **time_params
                })
                
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"No data returned from Grafana for query: {queries}")),
                    source=self.source,
                    metadata=metadata
                )

            # --- TimeseriesResult logic ---
            # Build a minimal panel_ref_map for the single query
            panel_ref_map = {
                "A": {
                    "panel_id": "prometheus_query",
                    "panel_title": metric_query,
                    "original_expr": metric_query,
                }
            }
            # Create metadata with Grafana URL using effective host
            # Get time parameters
            time_params = self._get_grafana_time_params(time_range)
            
            metadata = self._create_metadata_with_grafana_url(grafana_connector, "explore", {
                "datasource_uid": datasource_uid,
                "query": metric_query,
                "orgId": "1",
                **time_params
            })

            # Use the same timeseries parsing logic as dashboard panels
            timeseries_results = self._parse_grafana_response_frames(response, panel_ref_map, metadata)
            if timeseries_results:
                # Only one query, so return the first result with metadata
                return timeseries_results[0]

            # Fallback: return API response if no timeseries data found
            response_struct = dict_to_proto(response, Struct)
            output = ApiResponseResult(response_body=response_struct)
            task_result = PlaybookTaskResult(source=self.source, type=PlaybookTaskResultType.API_RESPONSE,
                                             api_response=output, metadata=metadata)
            return task_result
        except Exception as e:
            # Create metadata with Grafana URL using effective host
            time_params = self._get_grafana_time_params(time_range)
            metadata = self._create_metadata_with_grafana_url(grafana_connector, "explore", {
                "datasource_uid": datasource_uid,
                "query": metric_query,
                "orgId": "1",
                **time_params
            })
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=f"Error while executing Grafana task: {e}")),
                source=self.source,
                metadata=metadata
            )

    def execute_fetch_dashboard_variable_label_values(self, time_range: TimeRange, grafana_task: Grafana,
                                                      grafana_connector: ConnectorProto):
        try:
            if not grafana_connector:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value="Task execution Failed:: No Grafana source found")),
                    source=self.source
                )

            task = grafana_task.fetch_dashboard_variable_label_values
            datasource_uid = task.datasource_uid.value
            label_name = task.label_name.value

            grafana_api_processor = self.get_connector_processor(grafana_connector)

            print(
                f"Playbook Task Downstream Request: Type -> Grafana, Datasource_Uid -> {datasource_uid}, "
                f"Label_Name -> {label_name}",
                flush=True,
            )

            label_values = grafana_api_processor.fetch_dashboard_variable_label_values(datasource_uid, label_name)

            if not label_values:
                # Create metadata with Grafana URL using effective host
                time_params = self._get_grafana_time_params(time_range)
                metadata = self._create_metadata_with_grafana_url(grafana_connector, "explore", {
                    "datasource_uid": datasource_uid,
                    "query": f"label_values({label_name})",
                    "orgId": "1",
                    **time_params
                })
                
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"No label values returned from Grafana for label: {label_name}")),
                    source=self.source,
                    metadata=metadata
                )

            # Convert the list of label values to a structured response
            response_data = {
                "label_name": label_name,
                "datasource_uid": datasource_uid,
                "values": label_values,
                "count": len(label_values)
            }

            response_struct = dict_to_proto(response_data, Struct)
            output = ApiResponseResult(response_body=response_struct)

            # Create metadata with Grafana URL using effective host
            time_params = self._get_grafana_time_params(time_range)
            metadata = self._create_metadata_with_grafana_url(grafana_connector, "explore", {
                "datasource_uid": datasource_uid,
                "query": f"label_values({label_name})",
                "orgId": "1",
                **time_params
            })

            task_result = PlaybookTaskResult(source=self.source, type=PlaybookTaskResultType.API_RESPONSE,
                                             api_response=output, metadata=metadata)
            return task_result
        except Exception as e:
            # Create metadata with Grafana URL using effective host
            time_params = self._get_grafana_time_params(time_range)
            metadata = self._create_metadata_with_grafana_url(grafana_connector, "explore", {
                "datasource_uid": datasource_uid,
                "query": f"label_values({label_name})",
                "orgId": "1",
                **time_params
            })
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=f"Error while executing Grafana fetch dashboard variable label values task: {e}")),
                source=self.source,
                metadata=metadata
            )

    def execute_fetch_dashboard_variables(self, time_range: TimeRange, grafana_task: Grafana,
                                         grafana_connector: ConnectorProto):
        try:
            if not grafana_connector:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value="Task execution Failed:: No Grafana source found")),
                    source=self.source
                )

            # Access the task using the correct attribute name from the proto
            if hasattr(grafana_task, 'fetch_dashboard_variables'):
                task = grafana_task.fetch_dashboard_variables
            else:
                # Fallback for proto generation issues
                logger.warning("fetch_dashboard_variables attribute not found, trying alternative access")
                # Try to access by index in the oneof if the attribute is not available
                # Create metadata with Grafana URL using effective host
                time_params = self._get_grafana_time_params(time_range)
                metadata = self._create_metadata_with_grafana_url(grafana_connector, "variables", {
                    "dashboard_uid": "unknown",  # We don't have dashboard_uid in this error case
                    **time_params,
                    "orgId": "1"
                })
                
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value="Task type not properly configured in proto")),
                    source=self.source,
                    metadata=metadata
                )
            
            dashboard_uid = task.dashboard_uid.value
            
            # Parse fixed_variables if provided
            fixed_variables = {}
            if task.HasField("fixed_variables") and task.fixed_variables.value:
                try:
                    fixed_variables_str = task.fixed_variables.value.strip()
                    if fixed_variables_str:
                        # Handle both JSON and Python dict formats
                        if fixed_variables_str.startswith('{') and fixed_variables_str.endswith('}'):
                            try:
                                # Try JSON first (requires double quotes)
                                import json
                                fixed_variables = json.loads(fixed_variables_str)
                            except json.JSONDecodeError:
                                # Fallback to ast.literal_eval for Python dict format (allows single quotes)
                                import ast
                                fixed_variables = ast.literal_eval(fixed_variables_str)
                        else:
                            logger.warning(f"Fixed variables string does not appear to be a valid dict format: {fixed_variables_str}")
                            
                        if not isinstance(fixed_variables, dict):
                            logger.warning(f"Fixed variables is not a dict, got: {type(fixed_variables)}")
                            fixed_variables = {}
                        else:
                            logger.info(f"Using fixed variables: {fixed_variables}")
                            
                except (json.JSONDecodeError, ValueError, SyntaxError) as e:
                    logger.warning(f"Failed to parse fixed_variables '{task.fixed_variables.value}': {e}")
                    fixed_variables = {}

            grafana_api_processor = self.get_connector_processor(grafana_connector)

            print(
                f"Playbook Task Downstream Request: Type -> Grafana FETCH_DASHBOARD_VARIABLES, Dashboard_UID -> {dashboard_uid}, Fixed_Variables -> {fixed_variables}",
                flush=True,
            )

            variables_data = grafana_api_processor.get_dashboard_variables(dashboard_uid, fixed_variables, time_range)
            
            if not variables_data or not variables_data.get('variables'):
                # Create metadata with Grafana URL using effective host
                metadata = self._create_metadata_with_grafana_url(grafana_connector, "variables", {
                    "dashboard_uid": dashboard_uid,
                    **self._get_grafana_time_params(time_range),
                    "orgId": "1"
                })
                
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"No variables found for dashboard: {dashboard_uid}. Send an empty dictionary for template variables")),
                    source=self.source,
                    metadata=metadata
                )

            if not variables_data.get('variables'):
                message = variables_data.get('message', f"No variables found for dashboard: {dashboard_uid}. Send an empty dictionary for template variables")
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=message)),
                    source=self.source,
                    metadata=metadata,
                )

            # Ensure we have a proper Struct instance
            if isinstance(variables_data, dict):
                response_struct = Struct()
                response_struct.update(variables_data)
            else:
                response_struct = dict_to_proto(variables_data, Struct)
            
            output = ApiResponseResult(response_body=response_struct)

            task_result = PlaybookTaskResult(source=self.source, type=PlaybookTaskResultType.API_RESPONSE,
                                             api_response=output, metadata=metadata)
            return task_result
        except Exception as e:
            logger.error(f"Error while executing Grafana fetch dashboard variables task: {e}")
            
            # Create metadata with Grafana URL using effective host
            metadata = self._create_metadata_with_grafana_url(grafana_connector, "variables", {
                "dashboard_uid": dashboard_uid,
                **self._get_grafana_time_params(time_range),
                "orgId": "1"
            })
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=f"Error executing dashboard variables task: {str(e)}")),
                source=self.source,
                metadata=metadata
            )

    def _extract_template_variable_values(self, dashboard_dict: dict) -> dict:
        """Extracts current values from dashboard template variables."""
        template_vars_dict = {}
        dashboard_templating = dashboard_dict.get("templating", {})
        if isinstance(dashboard_templating, dict) and "list" in dashboard_templating:
            template_vars = dashboard_templating.get("list", [])
            logger.info(f"Found {len(template_vars)} template variables in dashboard")
            for var in template_vars:
                if not isinstance(var, dict):
                    continue
                var_name = var.get("name", "")
                if not var_name:
                    continue
                current = var.get("current", {})
                if isinstance(current, dict):
                    var_value = current.get("value")
                    template_vars_dict[var_name] = var_value
                    logger.debug(f"Template variable '{var_name}' = {var_value}")
                else:
                    logger.warning(f"Template variable '{var_name}' has no current value")
        else:
            logger.info("No template variables found in dashboard")
        return template_vars_dict

    def _is_coralogix_grafana(self, host_url: str) -> bool:
        """Check if the Grafana host is Coralogix-based."""
        if not host_url:
            return False
        return "coralogix.com/grafana" in host_url.lower()

    def _create_datasource_name_to_uid_mapping(self, grafana_api_processor, host_url: str = None) -> dict:
        """Creates a mapping from datasource names to UIDs by fetching from Grafana API."""
        try:
            datasources = grafana_api_processor.fetch_data_sources()
            if not datasources:
                logger.warning("No datasources found from Grafana API")
                return {}
            
            name_to_uid_map = {}
            uid_to_info_map = {}  # Also create reverse mapping for validation
            
            # Check if this is Coralogix Grafana
            is_coralogix = self._is_coralogix_grafana(host_url) if host_url else False
            
            # For Coralogix, create special mappings for prometheus and elasticsearch
            if is_coralogix:
                prometheus_uid = None
                elasticsearch_uid = None
                
                for ds in datasources:
                    if isinstance(ds, dict):
                        name = ds.get("name", "")
                        uid = ds.get("uid", "")
                        ds_type = ds.get("type", "").lower()
                        
                        if name and uid:
                            # Store all datasources normally
                            name_to_uid_map[name] = uid
                            uid_to_info_map[uid] = {
                                "name": name,
                                "type": ds_type,
                                "url": ds.get("url", ""),
                                "access": ds.get("access", "")
                            }
                            
                            # Track first prometheus and elasticsearch datasources for Coralogix
                            if ds_type == "prometheus" and prometheus_uid is None:
                                prometheus_uid = uid
                                logger.info(f"Found first Prometheus datasource for Coralogix: '{name}' (UID: {uid})")
                            elif ds_type == "elasticsearch" and elasticsearch_uid is None:
                                elasticsearch_uid = uid
                                logger.info(f"Found first Elasticsearch datasource for Coralogix: '{name}' (UID: {uid})")
                            
                            logger.debug(f"Mapped datasource '{name}' (type: {ds_type}) to UID '{uid}'")
                
                # Add special Coralogix mappings
                if prometheus_uid:
                    name_to_uid_map["prometheus"] = prometheus_uid
                    name_to_uid_map["${datasource}"] = prometheus_uid  # Common variable name
                if elasticsearch_uid:
                    name_to_uid_map["elasticsearch"] = elasticsearch_uid
                    name_to_uid_map["${datasource}"] = elasticsearch_uid  # Common variable name
                    
                logger.info(f"Created Coralogix datasource mapping with {len(name_to_uid_map)} total mappings")
            else:
                # Normal Grafana behavior
                prometheus_uid = None
                for ds in datasources:
                    if isinstance(ds, dict):
                        name = ds.get("name", "")
                        uid = ds.get("uid", "")
                        ds_type = ds.get("type", "")
                        if name and uid:
                            name_to_uid_map[name] = uid
                            uid_to_info_map[uid] = {
                                "name": name,
                                "type": ds_type,
                                "url": ds.get("url", ""),
                                "access": ds.get("access", "")
                            }
                            logger.debug(f"Mapped datasource '{name}' (type: {ds_type}) to UID '{uid}'")
                            
                            # Track first prometheus datasource for default mapping
                            if ds_type.lower() == "prometheus" and prometheus_uid is None:
                                prometheus_uid = uid
                
                # Add default datasource mapping
                if prometheus_uid:
                    name_to_uid_map["default"] = prometheus_uid
                    logger.info(f"Added default datasource mapping: 'default' -> '{prometheus_uid}'")
                elif name_to_uid_map:
                    # If no prometheus found, use the first available datasource
                    first_uid = list(uid_to_info_map.keys())[0]
                    name_to_uid_map["default"] = first_uid
                    logger.info(f"Added default datasource mapping: 'default' -> '{first_uid}' (first available)")
                
                logger.info(f"Created datasource mapping for {len(name_to_uid_map)} datasources")
            
            # Store the UID mapping for validation
            self._datasource_uid_info = uid_to_info_map
            
            return name_to_uid_map
        except Exception as e:
            logger.error(f"Failed to fetch datasources from Grafana API: {e}")
            return {}

    def _get_grafana_builtin_variables(self, time_range: TimeRange) -> dict:
        """Returns Grafana's built-in template variables with their values."""
        # Calculate time range duration in seconds
        duration_seconds = time_range.time_lt - time_range.time_geq
        
        # Calculate appropriate intervals based on duration
        if duration_seconds <= 300:  # 5 minutes
            interval = "5s"
            rate_interval = "15s"
        elif duration_seconds <= 1800:  # 30 minutes
            interval = "30s"
            rate_interval = "1m"
        elif duration_seconds <= 3600:  # 1 hour
            interval = "1m"
            rate_interval = "2m"
        elif duration_seconds <= 21600:  # 6 hours
            interval = "2m"
            rate_interval = "5m"
        elif duration_seconds <= 86400:  # 1 day
            interval = "5m"
            rate_interval = "10m"
        else:  # > 1 day
            interval = "10m"
            rate_interval = "20m"
        
        return {
            "__interval": interval,
            "__rate_interval": rate_interval,
            "__range": f"{duration_seconds}s",
            "__from": str(int(time_range.time_geq * 1000)),  # Convert to milliseconds
            "__to": str(int(time_range.time_lt * 1000)),     # Convert to milliseconds
        }

    def _resolve_template_variables_in_string(self, input_string: str, template_vars_dict: dict, time_range: TimeRange = None) -> str:
        """Resolves template variables (e.g., $var or ${var}) in a string."""
        if not input_string or not isinstance(input_string, str):
            return input_string

        resolved_string = input_string
        contains_multivalue = False
        
        # Get built-in Grafana variables
        builtin_vars = self._get_grafana_builtin_variables(time_range) if time_range else {}
        
        # Combine user variables with built-in variables (user variables take precedence)
        all_vars = {**builtin_vars, **template_vars_dict}
        
        # Regex to find $var or ${var} or ${var:format}
        var_refs = re.findall(r"\$\{([^}]+)(?::(?:csv|json|pipe|regex|distributed))?\}|\$([a-zA-Z0-9_]+)", input_string)
        
        if var_refs:
            logger.debug(f"Found template variable references in '{input_string}': {var_refs}")
            logger.debug(f"Available template variables: {list(all_vars.keys())}")
        
        for ref_tuple in var_refs:
            var_ref = next((item for item in ref_tuple if item), None)  # Get the captured group (variable name)
            if var_ref and var_ref in all_vars:
                var_value = all_vars[var_ref]
                # Basic handling for multi-value variables (join with '|')
                if isinstance(var_value, list):
                    replacement = "|".join(map(str, var_value)) if var_value else ""
                    if replacement:
                        contains_multivalue = True
                else:
                    replacement = str(var_value) if var_value is not None else ""
                logger.debug(f"Resolving ${var_ref} -> '{replacement}'")
                # Replace both ${var} and $var formats, ensuring word boundaries for $var
                resolved_string = re.sub(r"\$\{" + re.escape(var_ref) + r"(?::(?:csv|json|pipe|regex|distributed))?\}",
                                         replacement, resolved_string)
                resolved_string = re.sub(r"\$" + re.escape(var_ref) + r"\b", replacement, resolved_string)
            elif var_ref:
                logger.warning(f"Template variable '${var_ref}' referenced but not found in available variables: {list(all_vars.keys())}")
        
        # If we substituted any multi-value variables (joined with '|'),
        # upgrade equality matchers to regex matchers within label selectors: label="a|b" -> label=~"a|b"
        if contains_multivalue and "|" in resolved_string:
            def _upgrade_equals_to_regex_in_braces(match: re.Match) -> str:
                inner = match.group(0)
                # Only change plain '=' occurrences that have a quoted value containing a pipe
                # Keep existing '=~' or '!~' intact
                inner = re.sub(
                    r"(?<![!~])=\s*\"([^\"]*\|[^\"]*)\"",
                    lambda m: f"=~\"{m.group(1)}\"",
                    inner,
                )
                # Also upgrade '!=' to '!~' when multi-value regex is present
                inner = re.sub(
                    r"!=\s*\"([^\"]*\|[^\"]*)\"",
                    lambda m: f"!~\"{m.group(1)}\"",
                    inner,
                )
                return inner
            resolved_string = re.sub(r"\{[^}]*\}", _upgrade_equals_to_regex_in_braces, resolved_string)
        
        if resolved_string != input_string:
            logger.debug(f"Template variable resolution: '{input_string}' -> '{resolved_string}'")
        
        return resolved_string

    def _resolve_target_datasource(
            self, target_datasource: Union[dict, str, None], dashboard_datasource: Union[dict, None],
            template_vars_dict: dict, panel_id, datasource_name_to_uid_map: dict, time_range: TimeRange = None, host_url: str = None
    ) -> Union[dict, None]:
        """Resolves the datasource for a target, handling variables and defaults."""
        resolved_ds = {}
        datasource_value_to_resolve = None

        if isinstance(target_datasource, dict) and target_datasource.get("uid"):
            datasource_value_to_resolve = target_datasource["uid"]
        elif isinstance(target_datasource, str):
            datasource_value_to_resolve = target_datasource
        elif target_datasource is None or (isinstance(target_datasource, dict) and not target_datasource.get("uid")):
            # Use dashboard default datasource if target is null/empty
            if dashboard_datasource and dashboard_datasource.get("uid"):
                datasource_value_to_resolve = dashboard_datasource["uid"]
            else:
                logger.warning(
                    f"Target in panel {panel_id} has no datasource and no dashboard default datasource found.")
                return None  # Cannot proceed without a datasource
        else:
            # Handle potentially unsupported datasources like -- Grafana -- or unknown formats
            logger.info(
                f"Skipping target in panel {panel_id} due to potentially unsupported datasource: {target_datasource}")
            return None

        if not datasource_value_to_resolve:
            logger.warning(
                f"Could not determine datasource value to resolve for panel {panel_id}. Target DS: {target_datasource}")
            return None

        # Resolve any variables in the datasource string (UID or name)
        resolved_uid_or_name = self._resolve_template_variables_in_string(datasource_value_to_resolve,
                                                                          template_vars_dict, time_range)

        if not resolved_uid_or_name:
            logger.warning(
                f"Datasource variable resolution resulted in empty value for panel {panel_id}. Original: {datasource_value_to_resolve}")
            return None

        # Check if the resolved value is a datasource name and convert to UID
        actual_uid = resolved_uid_or_name
        if resolved_uid_or_name in datasource_name_to_uid_map:
            actual_uid = datasource_name_to_uid_map[resolved_uid_or_name]
            logger.debug(f"Resolved datasource name '{resolved_uid_or_name}' to UID '{actual_uid}' for panel {panel_id}")
        
        # Special handling for "default" datasource value
        elif resolved_uid_or_name == "default":
            # Try to find a suitable default datasource
            if hasattr(self, '_datasource_uid_info') and self._datasource_uid_info:
                # Look for prometheus datasource first (most common default)
                for uid, info in self._datasource_uid_info.items():
                    if info.get("type", "").lower() == "prometheus":
                        actual_uid = uid
                        logger.info(f"Resolved 'default' datasource to Prometheus UID '{actual_uid}' for panel {panel_id}")
                        break
                else:
                    # If no prometheus found, use the first available datasource
                    if self._datasource_uid_info:
                        first_uid = list(self._datasource_uid_info.keys())[0]
                        actual_uid = first_uid
                        logger.info(f"Resolved 'default' datasource to first available UID '{actual_uid}' for panel {panel_id}")
                    else:
                        logger.warning(f"No datasources available to resolve 'default' for panel {panel_id}")
            else:
                logger.warning(f"Cannot resolve 'default' datasource - no datasource info available for panel {panel_id}")
        
        # Special handling for Coralogix Grafana
        if self._is_coralogix_grafana(host_url) and resolved_uid_or_name not in datasource_name_to_uid_map:
            # For Coralogix, try to resolve based on datasource type
            if hasattr(self, '_datasource_uid_info') and self._datasource_uid_info:
                # Look for datasource type in the resolved name
                resolved_lower = resolved_uid_or_name.lower()
                if "prometheus" in resolved_lower:
                    # Try to find prometheus datasource
                    for uid, info in self._datasource_uid_info.items():
                        if info.get("type", "").lower() == "prometheus":
                            actual_uid = uid
                            logger.info(f"Coralogix: Resolved prometheus datasource to UID '{actual_uid}' for panel {panel_id}")
                            break
                elif "elasticsearch" in resolved_lower:
                    # Try to find elasticsearch datasource
                    for uid, info in self._datasource_uid_info.items():
                        if info.get("type", "").lower() == "elasticsearch":
                            actual_uid = uid
                            logger.info(f"Coralogix: Resolved elasticsearch datasource to UID '{actual_uid}' for panel {panel_id}")
                            break
        
        # Grafana API /api/ds/query requires UID.
        resolved_ds = {"uid": str(actual_uid)}

        return resolved_ds

    def _validate_datasource_uid(self, uid: str) -> bool:
        """Validates if a datasource UID exists and is accessible."""
        if not hasattr(self, '_datasource_uid_info') or not self._datasource_uid_info:
            logger.warning("Datasource UID validation not available - datasource mapping not created")
            return True  # Allow to proceed if we can't validate
        
        if uid not in self._datasource_uid_info:
            ds_info = self._datasource_uid_info.get(uid, {})
            logger.error(f"Datasource UID '{uid}' not found in available datasources. Available UIDs: {list(self._datasource_uid_info.keys())}")
            return False
        
        ds_info = self._datasource_uid_info[uid]
        logger.debug(f"Validated datasource UID '{uid}': {ds_info['name']} (type: {ds_info['type']})")
        return True

    def _prepare_grafana_queries(
            self, dashboard_dict: dict, template_vars_dict: dict, datasource_name_to_uid_map: dict, panel_ids_filter: Optional[list[str]] = None, time_range: TimeRange = None, host_url: str = None
    ) -> tuple[list[dict], dict]:
        """Builds the list of queries and the panel reference map from dashboard panels, optionally filtering by panel IDs."""
        all_queries = []
        panel_ref_map = {}
        panels = dashboard_dict.get("panels", [])
        dashboard_datasource = dashboard_dict.get("datasource")  # Default datasource for the dashboard
        letters = string.ascii_uppercase + string.digits
        query_idx = 0

        # Convert panel_ids_filter to a set for efficient lookup
        filter_set = set(panel_ids_filter) if panel_ids_filter else None

        for panel in panels:
            datasource_dict = panel.get("datasource") if "datasource" in panel else dashboard_datasource
            panel_id = panel.get("id")
            panel_title = panel.get("title", "")
            panel_type = panel.get("type", "")
            targets = panel.get("targets")

            if not targets or not panel_id:
                continue

            # Apply panel ID filter if provided
            if filter_set and str(panel_id) not in filter_set:
                continue

            for target in targets:
                if query_idx >= len(letters):
                    logger.warning(
                        f"Reached maximum query limit ({len(letters)}). Skipping remaining targets for dashboard.")
                    break

                raw_query = False
                expr = target.get("expr", "")
                if not expr:
                    expr = target.get("query", "")  # handling for raw flux query
                    if expr:
                        raw_query = True
                target_datasource_info = target.get("datasource")

                # Resolve Datasource
                resolved_datasource = self._resolve_target_datasource(target_datasource_info, datasource_dict,
                                                                      template_vars_dict, panel_id, datasource_name_to_uid_map, time_range, host_url)
                # Skip target if datasource resolution fails
                if not resolved_datasource and not expr.startswith(
                        "grafana"):  # Allow grafana expressions like grafana/alerting/list
                    continue

                # Resolve Expression Variables
                resolved_expr = self._resolve_template_variables_in_string(expr, template_vars_dict, time_range)
                if not resolved_expr:
                    continue  # Skip empty expressions

                # Validate datasource UID before adding query
                if resolved_datasource and resolved_datasource.get("uid"):
                    if not self._validate_datasource_uid(resolved_datasource["uid"]):
                        logger.warning(f"Skipping query for panel {panel_id} due to invalid datasource UID: {resolved_datasource['uid']}")
                        continue

                # Build Query Object
                ref_id = letters[query_idx]
                if raw_query:
                    query_obj = {"query": resolved_expr, "refId": ref_id}
                else:
                    query_obj = {"expr": resolved_expr, "refId": ref_id}
                if resolved_datasource:
                    query_obj["datasource"] = resolved_datasource

                all_queries.append(query_obj)
                panel_ref_map[ref_id] = {"panel_id": panel_id, "panel_title": panel_title, "panel_type": panel_type, "original_expr": expr}
                query_idx += 1

            if query_idx >= len(letters):
                break  # Break outer loop too if limit reached

        return all_queries, panel_ref_map

    def _parse_grafana_response_frames(self, response: dict, panel_ref_map: dict, metadata: Struct = None) -> list[PlaybookTaskResult]:
        """Parses the Grafana /api/ds/query response frames into a list of PlaybookTaskResults."""
        all_task_results = []
        if "results" not in response:
            logger.warning("No 'results' found in Grafana API response. This indicates a malformed response.")
            return []
        
        # Track statistics for better diagnostics
        total_ref_ids = len(response["results"])
        successful_ref_ids = 0
        failed_ref_ids = 0
        empty_ref_ids = 0
        
        print('total_ref_ids', total_ref_ids)
        
        for ref_id, result_data in response["results"].items():
            panel_info = panel_ref_map.get(ref_id)
            if not panel_info:
                logger.warning(f"Response for ref_id {ref_id} received but no mapping found. Skipping.")
                failed_ref_ids += 1
                continue

            print('result_data, panel_info, ref_id', result_data, panel_info, ref_id)
            
            # Check panel type to determine processing method
            panel_type = panel_info.get("panel_type", "timeseries").lower()
            panel_title = panel_info.get("panel_title", str(panel_info.get("panel_id")))
            legend_text = panel_title if panel_title else panel_info.get("original_expr", f"Query Ref: {ref_id}")
            
            if panel_type == "table":
                # Process as table data
                table_result = self._parse_frames_for_table(result_data, panel_info, ref_id)
                if table_result:
                    successful_ref_ids += 1
                    task_result = PlaybookTaskResult(
                        source=self.source,
                        type=PlaybookTaskResultType.TABLE,
                        table=table_result,
                        metadata=metadata
                    )
                    all_task_results.append(task_result)
                else:
                    print('result_data, panel_info, ref_id', result_data, panel_info, ref_id)
                    empty_ref_ids += 1
            elif panel_type == "stat":
                # Process as stat data (table output)
                table_result = self._parse_frames_for_stat(result_data, panel_info, ref_id)
                if table_result:
                    successful_ref_ids += 1
                    task_result = PlaybookTaskResult(
                        source=self.source,
                        type=PlaybookTaskResultType.TABLE,
                        table=table_result,
                        metadata=metadata
                    )
                    all_task_results.append(task_result)
                else:
                    print('result_data, panel_info, ref_id', result_data, panel_info, ref_id)
                    empty_ref_ids += 1
            elif panel_type == "gauge":
                # Process as gauge data (table output)
                table_result = self._parse_frames_for_gauge(result_data, panel_info, ref_id)
                if table_result:
                    successful_ref_ids += 1
                    task_result = PlaybookTaskResult(
                        source=self.source,
                        type=PlaybookTaskResultType.TABLE,
                        table=table_result,
                        metadata=metadata
                    )
                    all_task_results.append(task_result)
                else:
                    print('result_data, panel_info, ref_id', result_data, panel_info, ref_id)
                    empty_ref_ids += 1
            else:
                # Process as timeseries data (default behavior)
                ref_id_timeseries = self._parse_frames_for_ref_id(result_data, panel_info, ref_id)
                if ref_id_timeseries:
                    successful_ref_ids += 1
                    timeseries_result = TimeseriesResult(
                        labeled_metric_timeseries=ref_id_timeseries,
                        metric_expression=StringValue(value=legend_text),
                        metric_name=StringValue(value=panel_title),
                    )
                    task_result = PlaybookTaskResult(
                        source=self.source, 
                        type=PlaybookTaskResultType.TIMESERIES,
                        timeseries=timeseries_result,
                        metadata=metadata
                    )
                    all_task_results.append(task_result)
                else:
                    print('result_data, panel_info, ref_id', result_data, panel_info, ref_id)
                    empty_ref_ids += 1

        # Log summary statistics
        print(
            f"Grafana response parsing complete: {successful_ref_ids}/{total_ref_ids} ref_ids returned data, "
            f"{empty_ref_ids} returned no data, {failed_ref_ids} failed to process. "
            f"Total task results created: {len(all_task_results)}"
        )

        return all_task_results

    def _diagnose_dashboard_execution_issues(self, dashboard_uid: str, all_queries: list, panel_ref_map: dict, response: dict) -> None:
        """Provides diagnostic information for common dashboard execution issues."""
        if not response or "results" not in response:
            logger.warning(f"Dashboard {dashboard_uid}: No response or results from Grafana API")
            return

        # Check for common issues
        issues_found = []
        
        # Check if queries were prepared correctly
        if not all_queries:
            issues_found.append("No queries were prepared for execution")
        else:
            logger.info(f"Dashboard {dashboard_uid}: Prepared {len(all_queries)} queries for execution")
        
        # Check for missing ref_ids in response
        response_ref_ids = set(response["results"].keys())
        expected_ref_ids = set(panel_ref_map.keys())
        missing_ref_ids = expected_ref_ids - response_ref_ids
        extra_ref_ids = response_ref_ids - expected_ref_ids
        
        if missing_ref_ids:
            issues_found.append(f"Missing ref_ids in response: {missing_ref_ids}")
        if extra_ref_ids:
            issues_found.append(f"Unexpected ref_ids in response: {extra_ref_ids}")
        
        # Check for error responses
        error_ref_ids = []
        for ref_id, result_data in response["results"].items():
            if result_data.get("error"):
                error_ref_ids.append(f"{ref_id}: {result_data.get('error')}")
        
        if error_ref_ids:
            issues_found.append(f"Query errors: {error_ref_ids}")
        
        # Log summary
        if issues_found:
            logger.warning(f"Dashboard {dashboard_uid} execution issues: {'; '.join(issues_found)}")
        else:
            logger.info(f"Dashboard {dashboard_uid}: No obvious execution issues detected")

    def _parse_frames_for_ref_id(self, result_data: dict, panel_info: dict, ref_id: str) -> list:
        """Parses all frames within a single result_data block for a given ref_id."""
        ref_id_timeseries = []
        if "frames" not in result_data:
            status = result_data.get('status', 'unknown')
            error_msg = result_data.get('error', 'No error message provided')
            logger.warning(
                f"No 'frames' found in result for ref_id {ref_id} in panel {panel_info.get('panel_id', 'unknown')} "
                f"({panel_info.get('panel_title', 'unknown title')}). "
                f"Status: {status}, Error: {error_msg}. "
                f"This usually indicates the panel query failed or returned no data."
            )
            return ref_id_timeseries

        frames = result_data.get("frames", [])
        if not frames:
            logger.info(
                f"No frames in result for ref_id {ref_id} in panel {panel_info.get('panel_id', 'unknown')} "
                f"({panel_info.get('panel_title', 'unknown title')}). "
                f"This is normal for panels that return no data."
            )
            return ref_id_timeseries

        for frame_idx, frame in enumerate(frames):
            try:
                labeled_ts_list = self._parse_single_panel_frame(frame, panel_info, ref_id)
                ref_id_timeseries.extend(labeled_ts_list)
            except Exception as frame_ex:
                logger.error(
                    f"Error processing frame {frame_idx} for ref_id {ref_id} in panel {panel_info.get('panel_id', 'unknown')} "
                    f"({panel_info.get('panel_title', 'unknown title')}): {frame_ex}",
                    exc_info=True)
        return ref_id_timeseries

    def _parse_frames_for_table(self, result_data: dict, panel_info: dict, ref_id: str) -> TableResult:
        """Parses all frames within a single result_data block for a table panel."""
        if "frames" not in result_data:
            status = result_data.get('status', 'unknown')
            error_msg = result_data.get('error', 'No error message provided')
            logger.warning(
                f"No 'frames' found in table result for ref_id {ref_id} in panel {panel_info.get('panel_id', 'unknown')} "
                f"({panel_info.get('panel_title', 'unknown title')}). "
                f"Status: {status}, Error: {error_msg}. "
                f"This usually indicates the panel query failed or returned no data."
            )
            return None

        frames = result_data.get("frames", [])
        if not frames:
            logger.info(
                f"No frames in table result for ref_id {ref_id} in panel {panel_info.get('panel_id', 'unknown')} "
                f"({panel_info.get('panel_title', 'unknown title')}). "
                f"This is normal for panels that return no data."
            )
            return None

        # For table panels, we typically expect a single frame with tabular data
        if len(frames) > 1:
            logger.warning(
                f"Table panel {panel_info.get('panel_id', 'unknown')} returned {len(frames)} frames. "
                f"Processing only the first frame."
            )

        try:
            frame = frames[0]
            table_result = self._parse_single_table_frame(frame, panel_info, ref_id)
            return table_result
        except Exception as frame_ex:
            logger.error(
                f"Error processing table frame for ref_id {ref_id} in panel {panel_info.get('panel_id', 'unknown')} "
                f"({panel_info.get('panel_title', 'unknown title')}): {frame_ex}",
                exc_info=True)
            return None

    def _parse_single_table_frame(self, frame: dict, panel_info: dict, ref_id: str) -> TableResult:
        """Parses a single Grafana data frame into TableResult."""
        schema = frame.get("schema", {})
        data = frame.get("data", {})
        
        if not schema or not data or "values" not in data:
            missing_components = []
            if not schema:
                missing_components.append("schema")
            if not data:
                missing_components.append("data")
            elif "values" not in data:
                missing_components.append("data.values")
            
            logger.warning(
                f"Table frame for ref_id {ref_id} in panel {panel_info.get('panel_id', 'unknown')} "
                f"({panel_info.get('panel_title', 'unknown title')}) is missing: {', '.join(missing_components)}. "
                f"Skipping this frame."
            )
            return None

        # Extract field information from schema
        fields = schema.get("fields", [])
        if not fields:
            logger.warning(
                f"Table frame for ref_id {ref_id} in panel {panel_info.get('panel_id', 'unknown')} "
                f"({panel_info.get('panel_title', 'unknown title')}) has no fields in schema. Skipping."
            )
            return None

        # Extract column names and types
        column_names = []
        column_types = []
        for field in fields:
            field_name = field.get("name", "Unknown")
            field_type = field.get("type", "string")
            column_names.append(field_name)
            column_types.append(field_type)

        # Extract data values
        values = data.get("values", [])
        if not values or len(values) != len(fields):
            logger.warning(
                f"Table frame for ref_id {ref_id} has {len(values)} value arrays but {len(fields)} fields. "
                f"Data may be incomplete."
            )

        # Create table rows
        table_rows = []
        num_rows = len(values[0]) if values else 0
        
        for row_idx in range(num_rows):
            row_columns = []
            for col_idx, (col_name, col_type) in enumerate(zip(column_names, column_types)):
                # Get the value for this column and row
                value = ""
                if col_idx < len(values) and row_idx < len(values[col_idx]):
                    raw_value = values[col_idx][row_idx]
                    # Convert to string representation
                    if raw_value is None:
                        value = ""
                    else:
                        value = str(raw_value)
                
                column = TableResult.TableColumn(
                    name=StringValue(value=col_name),
                    type=StringValue(value=col_type),
                    value=StringValue(value=value)
                )
                row_columns.append(column)
            
            table_row = TableResult.TableRow(columns=row_columns)
            table_rows.append(table_row)

        # Create the table result
        panel_title = panel_info.get("panel_title", str(panel_info.get("panel_id")))
        raw_query = panel_info.get("original_expr", f"Query Ref: {ref_id}")
        
        table_result = TableResult(
            raw_query=StringValue(value=raw_query),
            total_count=UInt64Value(value=num_rows),
            limit=UInt64Value(value=num_rows),  # Assuming no pagination for now
            offset=UInt64Value(value=0),
            rows=table_rows
        )

        logger.info(
            f"Successfully parsed table frame for ref_id {ref_id} in panel {panel_info.get('panel_id', 'unknown')} "
            f"({panel_info.get('panel_title', 'unknown title')}): {num_rows} rows, {len(column_names)} columns"
        )

        return table_result

    def _parse_frames_for_stat(self, result_data: dict, panel_info: dict, ref_id: str) -> TableResult:
        """Parses all frames within a single result_data block for a stat panel."""
        if "frames" not in result_data:
            status = result_data.get('status', 'unknown')
            error_msg = result_data.get('error', 'No error message provided')
            logger.warning(
                f"No 'frames' found in stat result for ref_id {ref_id} in panel {panel_info.get('panel_id', 'unknown')} "
                f"({panel_info.get('panel_title', 'unknown title')}). "
                f"Status: {status}, Error: {error_msg}. "
                f"This usually indicates the panel query failed or returned no data."
            )
            return None

        frames = result_data.get("frames", [])
        if not frames:
            logger.info(
                f"No frames in stat result for ref_id {ref_id} in panel {panel_info.get('panel_id', 'unknown')} "
                f"({panel_info.get('panel_title', 'unknown title')}). "
                f"This is normal for panels that return no data."
            )
            return None

        # For stat panels, we typically expect a single frame with a single value
        if len(frames) > 1:
            logger.warning(
                f"Stat panel {panel_info.get('panel_id', 'unknown')} returned {len(frames)} frames. "
                f"Processing only the first frame."
            )

        try:
            frame = frames[0]
            text_result = self._parse_single_stat_frame(frame, panel_info, ref_id)
            return text_result
        except Exception as frame_ex:
            logger.error(
                f"Error processing stat frame for ref_id {ref_id} in panel {panel_info.get('panel_id', 'unknown')} "
                f"({panel_info.get('panel_title', 'unknown title')}): {frame_ex}",
                exc_info=True)
            return None

    def _parse_single_stat_frame(self, frame: dict, panel_info: dict, ref_id: str) -> TableResult:
        """Parses a single Grafana data frame into TableResult for stat panels."""
        schema = frame.get("schema", {})
        data = frame.get("data", {})
        
        if not schema or not data or "values" not in data:
            missing_components = []
            if not schema:
                missing_components.append("schema")
            if not data:
                missing_components.append("data")
            elif "values" not in data:
                missing_components.append("data.values")
            
            logger.warning(
                f"Stat frame for ref_id {ref_id} in panel {panel_info.get('panel_id', 'unknown')} "
                f"({panel_info.get('panel_title', 'unknown title')}) is missing: {', '.join(missing_components)}. "
                f"Skipping this frame."
            )
            return None

        # Extract field information from schema
        fields = schema.get("fields", [])
        if not fields:
            logger.warning(
                f"Stat frame for ref_id {ref_id} in panel {panel_info.get('panel_id', 'unknown')} "
                f"({panel_info.get('panel_title', 'unknown title')}) has no fields in schema. Skipping."
            )
            return None

        # Extract data values
        values = data.get("values", [])
        if not values:
            logger.warning(
                f"Stat frame for ref_id {ref_id} has no values. Skipping."
            )
            return None

        # For stat panels, create a simple table with one row containing the stat value
        # Extract column names and types
        column_names = []
        column_types = []
        for field in fields:
            field_name = field.get("name", "Value")
            field_type = field.get("type", "string")
            column_names.append(field_name)
            column_types.append(field_type)

        # Create a single row with the stat values
        table_rows = []
        row_columns = []
        
        for col_idx, (col_name, col_type) in enumerate(zip(column_names, column_types)):
            # Get the value for this column
            value = ""
            if col_idx < len(values) and values[col_idx]:
                raw_value = values[col_idx][0] if values[col_idx] else None
                
                if raw_value is not None:
                    # Format numeric values appropriately
                    if isinstance(raw_value, (int, float)):
                        if isinstance(raw_value, float):
                            # Format float to avoid unnecessary decimal places
                            value = f"{raw_value:.2f}".rstrip('0').rstrip('.')
                        else:
                            value = str(raw_value)
                    else:
                        value = str(raw_value)
            
            column = TableResult.TableColumn(
                name=StringValue(value=col_name),
                type=StringValue(value=col_type),
                value=StringValue(value=value)
            )
            row_columns.append(column)
        
        table_row = TableResult.TableRow(columns=row_columns)
        table_rows.append(table_row)

        # Create the table result
        panel_title = panel_info.get("panel_title", str(panel_info.get("panel_id")))
        raw_query = panel_info.get("original_expr", f"Query Ref: {ref_id}")
        
        table_result = TableResult(
            raw_query=StringValue(value=raw_query),
            total_count=UInt64Value(value=1),  # Stat panels typically have 1 row
            limit=UInt64Value(value=1),
            offset=UInt64Value(value=0),
            rows=table_rows
        )

        logger.info(
            f"Successfully parsed stat frame for ref_id {ref_id} in panel {panel_info.get('panel_id', 'unknown')} "
            f"({panel_info.get('panel_title', 'unknown title')}): {len(column_names)} columns, 1 row"
        )

        return table_result

    def _parse_frames_for_gauge(self, result_data: dict, panel_info: dict, ref_id: str) -> TableResult:
        """Parses all frames within a single result_data block for a gauge panel."""
        if "frames" not in result_data:
            status = result_data.get('status', 'unknown')
            error_msg = result_data.get('error', 'No error message provided')
            logger.warning(
                f"No 'frames' found in gauge result for ref_id {ref_id} in panel {panel_info.get('panel_id', 'unknown')} "
                f"({panel_info.get('panel_title', 'unknown title')}). "
                f"Status: {status}, Error: {error_msg}. "
                f"This usually indicates the panel query failed or returned no data."
            )
            return None

        frames = result_data.get("frames", [])
        if not frames:
            logger.info(
                f"No frames in gauge result for ref_id {ref_id} in panel {panel_info.get('panel_id', 'unknown')} "
                f"({panel_info.get('panel_title', 'unknown title')}). "
                f"This is normal for panels that return no data."
            )
            return None

        # For gauge panels, we typically expect a single frame with gauge values
        if len(frames) > 1:
            logger.warning(
                f"Gauge panel {panel_info.get('panel_id', 'unknown')} returned {len(frames)} frames. "
                f"Processing only the first frame."
            )

        try:
            frame = frames[0]
            table_result = self._parse_single_gauge_frame(frame, panel_info, ref_id)
            return table_result
        except Exception as frame_ex:
            logger.error(
                f"Error processing gauge frame for ref_id {ref_id} in panel {panel_info.get('panel_id', 'unknown')} "
                f"({panel_info.get('panel_title', 'unknown title')}): {frame_ex}",
                exc_info=True)
            return None

    def _parse_single_gauge_frame(self, frame: dict, panel_info: dict, ref_id: str) -> TableResult:
        """Parses a single Grafana data frame into TableResult for gauge panels."""
        schema = frame.get("schema", {})
        data = frame.get("data", {})
        
        if not schema or not data or "values" not in data:
            missing_components = []
            if not schema:
                missing_components.append("schema")
            if not data:
                missing_components.append("data")
            elif "values" not in data:
                missing_components.append("data.values")
            
            logger.warning(
                f"Gauge frame for ref_id {ref_id} in panel {panel_info.get('panel_id', 'unknown')} "
                f"({panel_info.get('panel_title', 'unknown title')}) is missing: {', '.join(missing_components)}. "
                f"Skipping this frame."
            )
            return None

        # Extract field information from schema
        fields = schema.get("fields", [])
        if not fields:
            logger.warning(
                f"Gauge frame for ref_id {ref_id} in panel {panel_info.get('panel_id', 'unknown')} "
                f"({panel_info.get('panel_title', 'unknown title')}) has no fields in schema. Skipping."
            )
            return None

        # Extract data values
        values = data.get("values", [])
        if not values:
            logger.warning(
                f"Gauge frame for ref_id {ref_id} has no values. Skipping."
            )
            return None

        # For gauge panels, create a simple table with one row containing the gauge value(s)
        # Extract column names and types
        column_names = []
        column_types = []
        for field in fields:
            field_name = field.get("name", "Value")
            field_type = field.get("type", "string")
            column_names.append(field_name)
            column_types.append(field_type)

        # Create a single row with the gauge values
        table_rows = []
        row_columns = []
        
        for col_idx, (col_name, col_type) in enumerate(zip(column_names, column_types)):
            # Get the value for this column
            value = ""
            if col_idx < len(values) and values[col_idx]:
                raw_value = values[col_idx][0] if values[col_idx] else None
                
                if raw_value is not None:
                    # Format numeric values appropriately
                    if isinstance(raw_value, (int, float)):
                        if isinstance(raw_value, float):
                            # Format float to avoid unnecessary decimal places
                            value = f"{raw_value:.2f}".rstrip('0').rstrip('.')
                        else:
                            value = str(raw_value)
                    else:
                        value = str(raw_value)
            
            column = TableResult.TableColumn(
                name=StringValue(value=col_name),
                type=StringValue(value=col_type),
                value=StringValue(value=value)
            )
            row_columns.append(column)
        
        table_row = TableResult.TableRow(columns=row_columns)
        table_rows.append(table_row)

        # Create the table result
        panel_title = panel_info.get("panel_title", str(panel_info.get("panel_id")))
        raw_query = panel_info.get("original_expr", f"Query Ref: {ref_id}")
        
        table_result = TableResult(
            raw_query=StringValue(value=raw_query),
            total_count=UInt64Value(value=1),  # Gauge panels typically have 1 row
            limit=UInt64Value(value=1),
            offset=UInt64Value(value=0),
            rows=table_rows
        )

        logger.info(
            f"Successfully parsed gauge frame for ref_id {ref_id} in panel {panel_info.get('panel_id', 'unknown')} "
            f"({panel_info.get('panel_title', 'unknown title')}): {len(column_names)} columns, 1 row"
        )

        return table_result

    def _parse_single_panel_frame(self, frame: dict, panel_info: dict, ref_id: str) -> list:
        """Parses a single Grafana data frame into LabeledMetricTimeseries."""
        parsed_timeseries = []
        schema = frame.get("schema", {})
        data = frame.get("data", {})
        
        # Enhanced error logging to help diagnose frame issues
        if not schema or not data or "values" not in data:
            missing_components = []
            if not schema:
                missing_components.append("schema")
            if not data:
                missing_components.append("data")
            elif "values" not in data:
                missing_components.append("data.values")
            
            # Log the actual frame structure for debugging
            frame_keys = list(frame.keys()) if isinstance(frame, dict) else "not a dict"
            data_keys = list(data.keys()) if isinstance(data, dict) else "not a dict"
            
            logger.warning(
                f"Skipping incomplete frame for ref_id {ref_id} in panel {panel_info.get('panel_id', 'unknown')} "
                f"({panel_info.get('panel_title', 'unknown title')}). "
                f"Missing: {', '.join(missing_components)}. "
                f"Frame keys: {frame_keys}, Data keys: {data_keys}. "
                f"This usually indicates the panel query returned no data or failed to execute."
            )
            return parsed_timeseries
        
        # Check if values array is empty (this is normal for queries with no data)
        values = data.get("values", [])
        if not values or (isinstance(values, list) and len(values) == 0):
            logger.info(
                f"Empty data values for ref_id {ref_id} in panel {panel_info.get('panel_id', 'unknown')} "
                f"({panel_info.get('panel_title', 'unknown title')}). "
                f"This is normal for queries that return no data in the specified time range."
            )
            return parsed_timeseries

        fields = schema.get("fields", [])
        if not fields:
            return parsed_timeseries

        time_idx, value_idx = -1, -1
        label_indices = []
        field_names = []

        for i, field in enumerate(fields):
            field_type = field.get("type")
            field_name = field.get("name")
            field_names.append(field_name)
            if field_type == "time":
                time_idx = i
            elif field_type == "number":
                if value_idx == -1:
                    value_idx = i
                else:
                    label_indices.append(i)
            else:
                label_indices.append(i)

        if time_idx == -1 or value_idx == -1:
            logger.warning(f"Could not find time or value field in frame for ref_id {ref_id}. Skipping frame.")
            return parsed_timeseries

        timestamps = data["values"][time_idx]
        values = data["values"][value_idx]
        num_points = len(timestamps)
        if num_points == 0:
            return parsed_timeseries

        # Frame-level/constant labels
        frame_labels = [
            LabelValuePair(name=StringValue(value="panel_id"), value=StringValue(value=str(panel_info["panel_id"]))),
            LabelValuePair(name=StringValue(value="panel_title"), value=StringValue(value=panel_info["panel_title"])),
            LabelValuePair(name=StringValue(value="ref_id"), value=StringValue(value=ref_id)),
            LabelValuePair(name=StringValue(value="original_expr"),
                           value=StringValue(value=panel_info["original_expr"])),
        ]
        if schema.get("name"):
            frame_labels.append(
                LabelValuePair(name=StringValue(value="series_name"), value=StringValue(value=schema["name"])))

        # Per-point labels
        label_data = {field_names[idx]: data["values"][idx] for idx in label_indices}
        label_field_names = [field_names[idx] for idx in label_indices]

        # Group points into series based on label values
        series_map = {}
        for i in range(num_points):
            point_label_values = tuple(label_data[name][i] for name in label_field_names)
            ts = timestamps[i]
            val = values[i]
            if val is None:
                continue

            if point_label_values not in series_map:
                series_map[point_label_values] = []
            series_map[point_label_values].append((ts, float(val)))

        # Create LabeledMetricTimeseries for each series
        for label_tuple, points in series_map.items():
            metric_labels = list(frame_labels)
            for j, label_value in enumerate(label_tuple):
                metric_labels.append(LabelValuePair(name=StringValue(value=label_field_names[j]),
                                                    value=StringValue(value=str(label_value))))

            datapoints = [
                TimeseriesResult.LabeledMetricTimeseries.Datapoint(timestamp=int(ts), value=DoubleValue(value=val)) for
                ts, val in sorted(points)
            ]

            if datapoints:
                labeled_ts = TimeseriesResult.LabeledMetricTimeseries(metric_label_values=metric_labels,
                                                                      datapoints=datapoints)
                parsed_timeseries.append(labeled_ts)

        return parsed_timeseries

    def execute_all_dashboard_panels(
            self, time_range: TimeRange, grafana_task: Grafana, grafana_connector: ConnectorProto
    ) -> list[PlaybookTaskResult]:
        """Executes all query targets for all panels in a given Grafana dashboard."""
        try:
            if not grafana_connector:
                return [PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value="Task execution Failed:: No Grafana source found")),
                    source=self.source
                )]

            task = grafana_task.execute_all_dashboard_panels
            dashboard_uid = task.dashboard_uid.value
            interval = task.interval.value
            if interval is None:
                interval = 60
            interval_ms = interval * 1000
            grafana_api_processor = self.get_connector_processor(grafana_connector)
            # Assuming panel_ids is a string field in the proto message (comma-separated)
            panel_ids_filter = None
            if task.HasField("panel_ids") and task.panel_ids.value:
                # Split the comma-separated string and remove any leading/trailing whitespace
                panel_ids_filter = [str(int(float(pid.strip()))) for pid in task.panel_ids.value.split(",") if
                                    pid.strip()]

            # 1. Fetch and parse dashboard details
            dashboard_details_response = grafana_api_processor.fetch_dashboard_details(dashboard_uid)
            if not dashboard_details_response or "dashboard" not in dashboard_details_response:
                raise Exception(f"Failed to fetch dashboard details for UID: {dashboard_uid}, {dashboard_details_response}")
            dashboard_dict = dashboard_details_response["dashboard"]

            # 2. Extract default template variable values and override with user-provided ones
            template_vars_dict = self._extract_template_variable_values(dashboard_dict)
                        
            # Handle template_variables from the task
            override_template_vars = {}
            if task.HasField("template_variables") and task.template_variables.value:
                try:                    
                    # Check if it's already a dict (most common case)
                    if isinstance(task.template_variables.value, dict):
                        user_vars = task.template_variables.value
                    # Check if it's a string that needs parsing
                    elif isinstance(task.template_variables.value, str):
                        str_value = task.template_variables.value
                        # If the string representation looks like a dict, try to evaluate it safely
                        if str_value.startswith('{') and str_value.endswith('}'):
                            # Use ast.literal_eval for safe evaluation of dict-like strings (handles single quotes)
                            import ast
                            try:
                                user_vars = ast.literal_eval(str_value)
                            except (ValueError, SyntaxError):
                                # Fallback to JSON parsing (requires double quotes)
                                user_vars = json.loads(str_value)
                        else:
                            # Try JSON parsing first, then fallback to ast.literal_eval
                            try:
                                user_vars = json.loads(str_value)
                            except json.JSONDecodeError:
                                # If JSON fails, try ast.literal_eval for Python literal strings
                                import ast
                                user_vars = ast.literal_eval(str_value)
                    else:
                        # For any other type, try to convert to string and parse
                        str_value = str(task.template_variables.value)
                        # If the string representation looks like a dict, try to evaluate it safely
                        if str_value.startswith('{') and str_value.endswith('}'):
                            # Use ast.literal_eval for safe evaluation of dict-like strings
                            import ast
                            try:
                                user_vars = ast.literal_eval(str_value)
                            except (ValueError, SyntaxError):
                                # Fallback to JSON parsing
                                user_vars = json.loads(str_value)
                        else:
                            user_vars = json.loads(str_value)
                    
                    if isinstance(user_vars, dict):
                        # Convert comma-separated strings to lists for multi-value variables
                        processed_vars = {}
                        for key, value in user_vars.items():
                            if isinstance(value, str) and ',' in value:
                                # Split comma-separated values and strip whitespace
                                processed_vars[key] = [v.strip() for v in value.split(',') if v.strip()]
                            else:
                                processed_vars[key] = value
                        override_template_vars = processed_vars
                    else:
                        print(f"Template variables from task is not a valid dict or JSON object: {task.template_variables.value}")
                except (json.JSONDecodeError, TypeError, ValueError, SyntaxError) as e:
                    print(f"Failed to process template_variables: {task.template_variables.value}, error: {str(e)}")
            
            if override_template_vars:
                template_vars_dict.update(override_template_vars)

            # 3. Create datasource name to UID mapping
            effective_host_url = self._get_effective_host_url(grafana_connector)
            datasource_name_to_uid_map = self._create_datasource_name_to_uid_mapping(grafana_api_processor, effective_host_url)
            
            # 4. Prepare queries for API call, passing the filter
            all_queries, panel_ref_map = self._prepare_grafana_queries(dashboard_dict, template_vars_dict,
                                                                       datasource_name_to_uid_map, panel_ids_filter, time_range, effective_host_url)

            if not all_queries:
                filter_message = f"matching filter IDs: {panel_ids_filter}" if panel_ids_filter else ""
                logger.warning(
                    f"No valid queries could be prepared for dashboard UID: {dashboard_uid} {filter_message}")
                
                # Create metadata with Grafana URL using effective host
                metadata = self._create_metadata_with_grafana_url(grafana_connector, "dashboard", {
                    "dashboard_uid": dashboard_uid,
                    **self._get_grafana_time_params(time_range),
                    "orgId": "1"
                })
                
                return [PlaybookTaskResult(
                    source=self.source,
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(
                        output=StringValue(
                            value=f"No valid metric queries could be found for dashboard UID: {dashboard_uid} {filter_message}")
                    ),
                    metadata=metadata
                )]

            # 4. Execute API call
            print(
                f"Playbook Task Downstream Request: Type -> Grafana, Dashboard UID -> {dashboard_uid}, "
                f"Executing {len(all_queries)} queries across all panels.",
                flush=True,
            )

            formatted_queries = self._format_query_step_interval(all_queries, time_range)
            
            # Log the queries being sent for debugging
            logger.info(f"Executing {len(formatted_queries)} queries for dashboard {dashboard_uid}")
            for i, query in enumerate(formatted_queries):
                ds_uid = query.get("datasource", {}).get("uid", "unknown")
                expr = query.get("expr", query.get("query", "unknown"))
                logger.debug(f"Query {i+1}: datasource={ds_uid}, expr='{expr[:100]}...'")
            
            try:
                response = grafana_api_processor.panel_query_datasource_api(tr=time_range, queries=formatted_queries,
                                                                            interval_ms=interval_ms)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    # Extract datasource UIDs from queries for better error reporting
                    ds_uids = [q.get("datasource", {}).get("uid") for q in formatted_queries if q.get("datasource", {}).get("uid")]
                    unique_ds_uids = list(set(ds_uids))
                    logger.error(f"Datasource not found (404) for dashboard {dashboard_uid}. "
                               f"Queries reference datasource UIDs: {unique_ds_uids}. "
                               f"Available datasources: {list(getattr(self, '_datasource_uid_info', {}).keys())}")
                raise

            if not response:
                logger.warning(f"No data returned from Grafana API for dashboard UID: {dashboard_uid}")
                
                # Create metadata with Grafana URL using effective host
                metadata = self._create_metadata_with_grafana_url(grafana_connector, "dashboard", {
                    "dashboard_uid": dashboard_uid,
                    **self._get_grafana_time_params(time_range),
                    "orgId": "1"
                })
                
                return [PlaybookTaskResult(
                    source=self.source,
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(
                        value=f"No data returned from Grafana API for dashboard UID: {dashboard_uid}")),
                    metadata=metadata
                )]

            # 5. Diagnose any execution issues
            self._diagnose_dashboard_execution_issues(dashboard_uid, all_queries, panel_ref_map, response)
            
            # 6. Create metadata with Grafana URL using effective host
            # Build template variables for URL
            url_template_vars = {}
            for key, value in template_vars_dict.items():
                url_template_vars[f"var-{key}"] = value
            
            metadata = self._create_metadata_with_grafana_url(grafana_connector, "dashboard", {
                "dashboard_uid": dashboard_uid,
                **self._get_grafana_time_params(time_range),
                "orgId": "1",
                **url_template_vars
            })
            
            # 7. Parse API response into Timeseries results
            all_task_results = self._parse_grafana_response_frames(response, panel_ref_map, metadata)

            if len(all_task_results) > 0:
                return all_task_results
            else:
                return [PlaybookTaskResult(
                    source=self.source, type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value="No data found for the given query"))
                )]

        except requests.exceptions.HTTPError as e:
            error_message = f"Error executing Grafana task for dashboard {grafana_task.execute_all_dashboard_panels.dashboard_uid.value}: {e}"
            try:
                # Try to get a more specific error from Grafana's response
                error_details = e.response.json()
                if 'message' in error_details:
                    error_message = f"Grafana API error for dashboard {grafana_task.execute_all_dashboard_panels.dashboard_uid.value}: {error_details['message']}"
            except (ValueError, AttributeError):
                # Fallback to the default error if response is not JSON or other issue
                pass
            logger.error(error_message, exc_info=True)
            
            # Create metadata with Grafana URL using effective host
            metadata = self._create_metadata_with_grafana_url(grafana_connector, "dashboard", {
                "dashboard_uid": grafana_task.execute_all_dashboard_panels.dashboard_uid.value,
                **self._get_grafana_time_params(time_range),
                "orgId": "1"
            })
            
            return [PlaybookTaskResult(
                source=self.source, type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=error_message)),
                metadata=metadata
            )]

        except Exception as e:
            logger.error(
                f"Error executing Grafana execute_all_dashboard_panels task for dashboard {grafana_task.execute_all_dashboard_panels.dashboard_uid.value}: {e}",
                exc_info=True,
            )
            # Return a text result indicating the error
            error_message = f"Error executing Grafana task for dashboard {grafana_task.execute_all_dashboard_panels.dashboard_uid.value}: {e}"
            
            # Create metadata with Grafana URL using effective host
            metadata = self._create_metadata_with_grafana_url(grafana_connector, "dashboard", {
                "dashboard_uid": grafana_task.execute_all_dashboard_panels.dashboard_uid.value,
                **self._get_grafana_time_params(time_range),
                "orgId": "1"
            })
            
            error_result = PlaybookTaskResult(
                source=self.source, type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=error_message)),
                metadata=metadata
            )
            return [error_result]

    def _calculate_bucket_size_seconds(self, total_seconds):
        """Calculate appropriate bucket size based on duration, aiming for < MAX_DATA_POINTS buckets."""
        if total_seconds <= 0:
            return self.MIN_STEP_SIZE_SECONDS

        ideal_bucket_size = (total_seconds + self.MAX_DATA_POINTS - 1) // self.MAX_DATA_POINTS

        min_bucket_for_duration = self.MIN_STEP_SIZE_SECONDS  # Start with the global minimum
        for duration_threshold, min_bucket_for_threshold in self._INTERVAL_THRESHOLDS_SECONDS:
            if total_seconds >= duration_threshold:
                min_bucket_for_duration = min_bucket_for_threshold
                break  # Found the largest applicable threshold, use its minimum

        # The actual bucket size must be at least the global minimum, the ideal size,
        # and the minimum required for the given duration.
        calculated_bucket_size = max(self.MIN_STEP_SIZE_SECONDS, ideal_bucket_size, min_bucket_for_duration)

        # Round up to the nearest standard bucket size
        for standard_size in self.STANDARD_STEP_SIZES_SECONDS:
            if calculated_bucket_size <= standard_size:
                return standard_size

        # If larger than the largest standard size, return the largest standard size
        return self.STANDARD_STEP_SIZES_SECONDS[-1]

    def _format_query_step_interval(self, queries, time_range: TimeRange):
        """
        Sets the maxDataPoints and calculates intervalMs for Grafana queries
        based on time range duration, thresholds, and standard bucket sizes,
        aiming for approximately MAX_DATA_POINTS data points.
        """
        # Calculate duration in seconds
        total_seconds = time_range.time_lt - time_range.time_geq
        if total_seconds <= 0:
            logger.warning(f"Invalid time range duration ({total_seconds}s), defaulting interval calculation.")
            total_seconds = self.MIN_STEP_SIZE_SECONDS

        interval_s = self._calculate_bucket_size_seconds(total_seconds)
        interval_ms = interval_s * 1000

        # Set maxDataPoints to our target number of buckets
        max_data_points = self.MAX_DATA_POINTS

        for q in queries:
            q["maxDataPoints"] = max_data_points
            q["intervalMs"] = interval_ms

        return queries

    def execute_loki_datasource_log_query(self, time_range: TimeRange, grafana_task: Grafana,
                                         grafana_connector: ConnectorProto):
        """Executes a LogQL query against a Loki datasource."""
        try:
            if not grafana_connector:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value="Task execution Failed:: No Grafana source found")),
                    source=self.source
                )

            task = grafana_task.loki_datasource_log_query
            datasource_uid = task.datasource_uid.value
            logql_query = task.logql_query.value
            max_lines = task.max_lines.value if task.max_lines and task.max_lines.value else 1000
            direction = task.direction.value if task.direction and task.direction.value else "backward"

            grafana_api_processor = self.get_connector_processor(grafana_connector)

            print(
                f"Playbook Task Downstream Request: Type -> Grafana Loki, Datasource_Uid -> {datasource_uid}, "
                f"LogQL_Query -> {logql_query}, Max_Lines -> {max_lines}, Direction -> {direction}",
                flush=True,
            )

            # Prepare the query for Loki datasource
            queries = [{
                "refId": "A",
                "expr": logql_query,
                "queryType": "range",
                "datasource": {"type": "loki", "uid": datasource_uid},
                "editorMode": "code",
                "direction": direction,
                "maxLines": max_lines,
                "step": "",
                "legendFormat": "",
                "intervalMs": 2000,  # Default interval for Loki queries
                "maxDataPoints": 1000
            }]

            # Format queries with time range
            formatted_queries = self._format_query_step_interval(queries, time_range)

            response = grafana_api_processor.panel_query_datasource_api(
                tr=time_range, 
                queries=formatted_queries,
                interval_ms=2000
            )

            if not response:
                # Create metadata with Grafana URL using effective host
                metadata = self._create_metadata_with_grafana_url(grafana_connector, "explore", {
                    "datasource_uid": datasource_uid,
                    "query": logql_query,
                    "orgId": "1",
                    **self._get_grafana_time_params(time_range)
                })
                
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"No data returned from Grafana Loki for query: {logql_query}")),
                    source=self.source,
                    metadata=metadata
                )

            # Parse the Loki response into a structured format
            parsed_logs = self._parse_loki_response(response, logql_query)
            
            if not parsed_logs:
                # Create metadata with Grafana URL using effective host
                metadata = self._create_metadata_with_grafana_url(grafana_connector, "explore", {
                    "datasource_uid": datasource_uid,
                    "query": logql_query,
                    "orgId": "1",
                    **self._get_grafana_time_params(time_range)
                })
                
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"No log entries found for query: {logql_query}")),
                    source=self.source,
                    metadata=metadata
                )

            # Create metadata with Grafana URL using effective host
            metadata = self._create_metadata_with_grafana_url(grafana_connector, "explore", {
                "datasource_uid": datasource_uid,
                "query": logql_query,
                "orgId": "1",
                **self._get_grafana_time_params(time_range)
            })

            # Convert to table format
            table_result = self._convert_logs_to_table(parsed_logs, logql_query, datasource_uid)
            task_result = PlaybookTaskResult(source=self.source, type=PlaybookTaskResultType.TABLE,
                                             table=table_result, metadata=metadata)
            return task_result

        except Exception as e:
            # Create metadata with Grafana URL using effective host
            time_params = self._get_grafana_time_params(time_range)
            metadata = self._create_metadata_with_grafana_url(grafana_connector, "explore", {
                "datasource_uid": datasource_uid,
                "query": logql_query,
                "orgId": "1",
                **time_params
            })
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=f"Error while executing Grafana Loki task: {e}")),
                source=self.source,
                metadata=metadata
            )

    def _parse_loki_response(self, response: dict, query: str) -> list[dict]:
        """Parses Loki response frames into structured log entries."""
        parsed_logs = []
        
        if "results" not in response:
            return parsed_logs

        for ref_id, result_data in response["results"].items():
            if "frames" not in result_data:
                continue

            for frame in result_data.get("frames", []):
                try:
                    schema = frame.get("schema", {})
                    data = frame.get("data", {})
                    
                    if not schema or not data or not data.get("values"):
                        continue

                    fields = schema.get("fields", [])
                    if not fields:
                        continue

                    # Find field indices
                    labels_idx = -1
                    time_idx = -1
                    line_idx = -1
                    ts_ns_idx = -1
                    id_idx = -1

                    for i, field in enumerate(fields):
                        field_name = field.get("name", "")
                        if field_name == "labels":
                            labels_idx = i
                        elif field_name == "Time":
                            time_idx = i
                        elif field_name == "Line":
                            line_idx = i
                        elif field_name == "tsNs":
                            ts_ns_idx = i
                        elif field_name == "id":
                            id_idx = i

                    if time_idx == -1 or line_idx == -1:
                        continue

                    # Extract data arrays
                    values = data["values"]
                    labels_data = values[labels_idx] if labels_idx != -1 else []
                    timestamps = values[time_idx] if time_idx != -1 else []
                    log_lines = values[line_idx] if line_idx != -1 else []
                    ts_ns_data = values[ts_ns_idx] if ts_ns_idx != -1 else []
                    id_data = values[id_idx] if id_idx != -1 else []

                    # Combine data into log entries
                    num_entries = len(timestamps)
                    for i in range(num_entries):
                        log_entry = {
                            "timestamp": timestamps[i] if i < len(timestamps) else None,
                            "log_line": log_lines[i] if i < len(log_lines) else "",
                            "labels": labels_data[i] if i < len(labels_data) else {},
                            "ts_ns": ts_ns_data[i] if i < len(ts_ns_data) else None,
                            "id": id_data[i] if i < len(id_data) else None,
                            "query": query
                        }
                        parsed_logs.append(log_entry)

                except Exception as frame_ex:
                    logger.error(f"Error processing Loki frame: {frame_ex}")
                    continue

        return parsed_logs

    def _extract_query_stats(self, response: dict) -> dict:
        """Extracts query statistics from Loki response metadata."""
        stats = {}
        
        if "results" not in response:
            return stats

        for ref_id, result_data in response["results"].items():
            if "frames" not in result_data:
                continue

            for frame in result_data.get("frames", []):
                schema = frame.get("schema", {})
                meta = schema.get("meta", {})
                
                if "stats" in meta:
                    for stat in meta["stats"]:
                        display_name = stat.get("displayName", "")
                        value = stat.get("value", 0)
                        unit = stat.get("unit", "")
                        
                        # Clean up display name for use as key
                        key = display_name.lower().replace(" ", "_").replace(":", "").replace("-", "_")
                        stats[key] = {
                            "value": value,
                            "unit": unit,
                            "display_name": display_name
                        }

                # Extract executed query string
                if "executedQueryString" in meta:
                    stats["executed_query"] = meta["executedQueryString"]

        return stats

    def _convert_logs_to_table(self, parsed_logs: list[dict], query: str, datasource_uid: str) -> TableResult:
        """Converts parsed Loki logs into a table format."""
        if not parsed_logs:
            return TableResult(
                raw_query=StringValue(value=f"Execute ```{query}```"),
                total_count=UInt64Value(value=0),
                rows=[]
            )

        # Convert logs to table rows following the Grafana Loki pattern
        table_rows = []
        for log_entry in parsed_logs:
            labels = log_entry.get("labels", {})
            timestamp = log_entry.get("timestamp")
            log_line = log_entry.get("log_line", "")
            
            # Format timestamp for display
            formatted_timestamp = ""
            if timestamp:
                try:
                    # Convert Unix timestamp (milliseconds) to readable format
                    import datetime
                    dt = datetime.datetime.fromtimestamp(timestamp / 1000, tz=datetime.timezone.utc)
                    formatted_timestamp = dt.strftime("%Y-%m-%d %H:%M:%S UTC")
                except (ValueError, TypeError):
                    formatted_timestamp = str(timestamp)
            
            # Create columns for this row - following the Grafana Loki pattern
            table_columns = []
            
            # Add timestamp column
            table_columns.append(TableResult.TableColumn(
                name=StringValue(value="timestamp"),
                value=StringValue(value=formatted_timestamp)
            ))
            
            # Add log line column
            table_columns.append(TableResult.TableColumn(
                name=StringValue(value="log"),
                value=StringValue(value=log_line)
            ))
            
            # Add all label columns
            for label_key, label_value in labels.items():
                table_columns.append(TableResult.TableColumn(
                    name=StringValue(value=str(label_key)),
                    value=StringValue(value=str(label_value))
                ))

            # Create the table row
            table_row = TableResult.TableRow(columns=table_columns)
            table_rows.append(table_row)

        return TableResult(
            raw_query=StringValue(value=f"Execute ```{query}```"),
            total_count=UInt64Value(value=len(parsed_logs)),
            rows=table_rows
        )
