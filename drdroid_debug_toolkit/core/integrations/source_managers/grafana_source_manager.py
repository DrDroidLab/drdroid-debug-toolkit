import json
import logging
import re
import string
import requests
import ast
import urllib.parse
from datetime import datetime, timezone
from typing import Optional, Union

from google.protobuf.struct_pb2 import Struct
from google.protobuf.wrappers_pb2 import DoubleValue, StringValue, Int64Value

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
)
from core.protos.playbooks.source_task_definitions.grafana_task_pb2 import Grafana
from core.protos.ui_definition_pb2 import FormField, FormFieldType
from core.utils.credentilal_utils import generate_credentials_dict
from core.utils.proto_utils import dict_to_proto, proto_to_dict

logger = logging.getLogger(__name__)


def _convert_to_milliseconds(time_value):
    """Convert various time formats to milliseconds timestamp."""
    if not time_value:
        return None
    
    try:
        # If it's already a number, assume it's already in milliseconds
        if isinstance(time_value, (int, float)):
            return int(time_value)
        
        # If it's a string, try to parse it
        if isinstance(time_value, str):
            # Try ISO format first
            try:
                dt = datetime.fromisoformat(time_value.replace('Z', '+00:00'))
                return int(dt.timestamp() * 1000)
            except ValueError:
                # Try parsing as timestamp
                try:
                    return int(float(time_value) * 1000)
                except ValueError:
                    pass
        
        return None
    except Exception:
        return None


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
            Grafana.TaskType.DATASOURCE_QUERY_EXECUTION: {
                "executor": self.execute_datasource_query_execution,
                "model_types": [SourceModelType.GRAFANA_PROMETHEUS_DATASOURCE, SourceModelType.GRAFANA_DASHBOARD],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "Query logs, metrics, or any data from any datasource in Grafana (Prometheus, Loki, InfluxDB, SQL, etc.) with customizable time intervals",
                "category": "Metrics & Logs",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="datasource_uid"),
                        display_name=StringValue(value="Data Source UID"),
                        description=StringValue(value="The datasource_uid to query. This is required to identify which datasource to query (logs, metrics, traces, SQL, etc.)."),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TYPING_DROPDOWN_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="interval"),
                        display_name=StringValue(value="Step Size (Seconds)"),
                        description=StringValue(value="Optional: Time interval between data points in seconds. Defaults to 60 seconds if not specified. Use smaller values for higher resolution data."),
                        data_type=LiteralType.LONG,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                    ),
                    FormField(
                        key_name=StringValue(value="query_type"),
                        display_name=StringValue(value="Query Type"),
                        description=StringValue(value="The type of query to execute. Use PromQL for Prometheus (metrics), Flux for InfluxDB, Loki for logs, SQL for databases, or any other datasource-specific query language."),
                        data_type=LiteralType.STRING,
                        default_value=Literal(type=LiteralType.STRING, string=StringValue(value="PromQL")),
                        valid_values=[
                            Literal(type=LiteralType.STRING, string=StringValue(value="PromQL")),
                            Literal(type=LiteralType.STRING, string=StringValue(value="Flux")),
                            Literal(type=LiteralType.STRING, string=StringValue(value="Loki")),
                        ],
                        form_field_type=FormFieldType.DROPDOWN_FT,
                        is_optional=True,
                    ),
                    FormField(
                        key_name=StringValue(value="query_expression"),
                        display_name=StringValue(value="Query Expression"),
                        description=StringValue(value="The query expression to execute against the specified datasource. Examples: PromQL: 'up', Flux: 'from(bucket: \"mybucket\")', Loki: '{job=\"varlogs\"}', SQL: 'SELECT * FROM table'. Use the appropriate query for logs, metrics, traces, or any data type supported by the datasource."),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.MULTILINE_FT,
                    ),
                ],
            },
            Grafana.TaskType.EXECUTE_ALL_DASHBOARD_PANELS: {
                "executor": self.execute_all_dashboard_panels,
                "model_types": [SourceModelType.GRAFANA_DASHBOARD],
                "result_type": PlaybookTaskResultType.TIMESERIES,
                "display_name": "Execute all metric queries from all panels in a Grafana dashboard simultaneously, returning comprehensive timeseries data with support for template variables and panel filtering",
                "category": "Metrics",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="dashboard_uid"),
                        display_name=StringValue(value="Dashboard UID"),
                        description=StringValue(value="MANDATORY: The unique identifier (UID) of the Grafana dashboard to execute. This is different from the dashboard ID and is typically a string like 'abc123'."),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TYPING_DROPDOWN_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="panel_ids"),
                        display_name=StringValue(value="Panel IDs (Optional, Comma-separated)"),
                        description=StringValue(
                            value="Optional: Comma-separated list of specific panel IDs to execute. If not provided, all panels in the dashboard will be executed. Example: '1,3,5' to execute only panels 1, 3, and 5."),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                    ),
                    FormField(
                        key_name=StringValue(value="interval"),
                        display_name=StringValue(value="Step Size (Seconds)"),
                        description=StringValue(value="Optional: Time interval between data points in seconds. Defaults to 60 seconds if not specified. Smaller values provide higher resolution but may increase query time."),
                        data_type=LiteralType.LONG,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                    ),
                    FormField(
                        key_name=StringValue(value="template_variables"),
                        display_name=StringValue(value="Template Variables"),
                        description=StringValue(value="""MANDATORY: JSON of template variables to set the dashboard variables. Set like "{\"var1\": \"val1\", \"var2\": \"val2\"}" - in JSON format.
                                                Set ALL VARIABLES of the dashboard according to the user query"""),
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
                "display_name": "Retrieve all template variables and their current values from a Grafana dashboard, including variable definitions, current selections, and available options for dynamic dashboard configuration",
                "category": "Metrics",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="dashboard_uid"),
                        display_name=StringValue(value="Dashboard UID"),
                        description=StringValue(value="MANDATORY: The unique identifier (UID) of the Grafana dashboard to fetch variables from. This will return all template variables defined in the dashboard along with their current values and available options."),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TYPING_DROPDOWN_FT,
                    ),
                ],
            },
            Grafana.TaskType.GET_DASHBOARD_CONFIG: {
                "executor": self.execute_get_dashboard_config,
                "model_types": [SourceModelType.GRAFANA_DASHBOARD],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "MANDATORY: Retrieves dashboard configuration details from Grafana, including dashboard metadata, panel configurations, and template variables",
                "category": "Configuration",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="dashboard_uid"),
                        display_name=StringValue(value="Dashboard UID"),
                        description=StringValue(value="The unique identifier (UID) of the Grafana dashboard to retrieve configuration details for. This will return the complete dashboard configuration including panels, variables, and metadata."),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TYPING_DROPDOWN_FT,
                    ),
                ],
            },
            Grafana.TaskType.FETCH_ALL_DASHBOARDS: {
                "executor": self.execute_fetch_all_dashboards,
                "model_types": [SourceModelType.GRAFANA_DASHBOARD],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "MANDATORY: Fetches all dashboards from Grafana with basic information like title, UID, folder, tags, etc.",
                "category": "Configuration",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="limit"),
                        display_name=StringValue(value="Limit"),
                        description=StringValue(value="Maximum number of dashboards to return. Defaults to 100 if not specified."),
                        data_type=LiteralType.LONG,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                        default_value=Literal(type=LiteralType.LONG, long=Int64Value(value=100)),
                    ),
                ],
            },
            Grafana.TaskType.FETCH_DATASOURCES: {
                "executor": self.execute_fetch_datasources,
                "model_types": [SourceModelType.GRAFANA_PROMETHEUS_DATASOURCE],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "MANDATORY: Fetches all datasources from Grafana with their configuration details",
                "category": "Configuration",
                "form_fields": [],
            },
            Grafana.TaskType.FETCH_FOLDERS: {
                "executor": self.execute_fetch_folders,
                "model_types": [SourceModelType.GRAFANA_DASHBOARD],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "MANDATORY: Fetches all folders from Grafana with their metadata and permissions",
                "category": "Configuration",
                "form_fields": [],
            },
        }

    def get_connector_processor(self, grafana_connector, **kwargs):
        generated_credentials = generate_credentials_dict(grafana_connector.type, grafana_connector.keys)
        return GrafanaApiProcessor(**generated_credentials)

    def _extract_host_url_from_connector(self, grafana_connector: ConnectorProto) -> str:
        """Extract the host URL from the Grafana connector."""
        if not grafana_connector or not grafana_connector.keys:
            return ""
        
        for key in grafana_connector.keys:
            if key.key_type == 21 and key.key.value:  # GRAFANA_HOST = 21
                return key.key.value
        
        return ""

    def _create_metadata_with_grafana_url(self, host_url: str, task_type: str, params: dict = None) -> Struct:
        """Create metadata struct with Grafana URL."""
        grafana_url = buildGrafanaUrl(host_url, task_type, params)
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

    def execute_datasource_query_execution(self, time_range: TimeRange, grafana_task: Grafana,
                                                       grafana_connector: ConnectorProto):
        try:
            if not grafana_connector:
                raise Exception("Task execution Failed:: No Grafana source found")

            task = grafana_task.datasource_query_execution
            datasource_uid = task.datasource_uid.value
            interval = task.interval.value
            if interval is None:
                interval = 60
            interval_ms = interval * 1000
            query_type = task.query_type.value if task.query_type and task.query_type.value else "PromQL"
            metric_query = task.query_expression.value

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
                # Extract host URL and create metadata with Grafana URL
                host_url = self._extract_host_url_from_connector(grafana_connector)
                time_params = self._get_grafana_time_params(time_range)
                metadata = self._create_metadata_with_grafana_url(host_url, "explore", {
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
                    "panel_id": f"{query_type.lower()}_query",
                    "panel_title": metric_query,
                    "original_expr": metric_query,
                }
            }
            # Extract host URL and create metadata with Grafana URL
            host_url = self._extract_host_url_from_connector(grafana_connector)
            # Get time parameters
            time_params = self._get_grafana_time_params(time_range)
            
            metadata = self._create_metadata_with_grafana_url(host_url, "explore", {
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
            # Extract host URL and create metadata with Grafana URL
            host_url = self._extract_host_url_from_connector(grafana_connector)
            time_params = self._get_grafana_time_params(time_range)
            metadata = self._create_metadata_with_grafana_url(host_url, "explore", {
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



    def execute_fetch_dashboard_variables(self, time_range: TimeRange, grafana_task: Grafana,
                                         grafana_connector: ConnectorProto):
        try:
            if not grafana_connector:
                raise Exception("Task execution Failed:: No Grafana source found")

            # Access the task using the correct attribute name from the proto
            if hasattr(grafana_task, 'fetch_dashboard_variables'):
                task = grafana_task.fetch_dashboard_variables
            else:
                # Fallback for proto generation issues
                logger.warning("fetch_dashboard_variables attribute not found, trying alternative access")
                # Try to access by index in the oneof if the attribute is not available
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value="Task type not properly configured in proto")),
                    source=self.source,
                )
            
            dashboard_uid = task.dashboard_uid.value

            print(
                f"Playbook Task Downstream Request: Type -> Grafana FETCH_DASHBOARD_VARIABLES, Dashboard_UID -> {dashboard_uid}",
                flush=True,
            )

            # Get connector credentials
            generated_credentials = generate_credentials_dict(grafana_connector.type, grafana_connector.keys)
            
            # Create metadata extractor instance
            grafana_metadata_extractor = GrafanaSourceMetadataExtractor(
                request_id="dashboard_variables_request",
                connector_name=grafana_connector.name.value,
                grafana_host=generated_credentials.get('grafana_host'),
                grafana_api_key=generated_credentials.get('grafana_api_key'),
                ssl_verify=generated_credentials.get('ssl_verify', 'true')
            )

            # Use metadata extractor to get dashboard variables directly
            variables_data = grafana_metadata_extractor.extract_dashboard_variables(dashboard_uid)

            # Extract host URL and create metadata with Grafana URL
            host_url = self._extract_host_url_from_connector(grafana_connector)
            metadata = self._create_metadata_with_grafana_url(host_url, "variables", {
                "dashboard_uid": dashboard_uid,
                **self._get_grafana_time_params(time_range),
                "orgId": "1"
            })

            if variables_data.get('error'):
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=variables_data['error'])),
                    source=self.source,
                    metadata=metadata
                )

            if not variables_data.get('variables'):
                message = variables_data.get('message', f"No variables found for dashboard: {dashboard_uid}. Send an empty dictionary for template variables")
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=message)),
                    source=self.source,
                    metadata=metadata
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
            
            # Extract host URL and create metadata with Grafana URL
            host_url = self._extract_host_url_from_connector(grafana_connector)
            metadata = self._create_metadata_with_grafana_url(host_url, "variables", {
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
            for var in template_vars:
                if not isinstance(var, dict):
                    continue
                var_name = var.get("name", "")
                if not var_name:
                    continue
                current = var.get("current", {})
                if isinstance(current, dict):
                    template_vars_dict[var_name] = current.get("value")
        return template_vars_dict

    def _create_datasource_name_to_uid_mapping(self, grafana_api_processor) -> dict:
        """Creates a mapping from datasource names to UIDs by fetching from Grafana API."""
        try:
            datasources = grafana_api_processor.fetch_data_sources()
            if not datasources:
                logger.warning("No datasources found from Grafana API")
                return {}
            
            name_to_uid_map = {}
            for ds in datasources:
                if isinstance(ds, dict):
                    name = ds.get("name", "")
                    uid = ds.get("uid", "")
                    if name and uid:
                        name_to_uid_map[name] = uid
                        logger.debug(f"Mapped datasource '{name}' to UID '{uid}'")
            
            logger.info(f"Created datasource mapping for {len(name_to_uid_map)} datasources")
            return name_to_uid_map
        except Exception as e:
            logger.error(f"Failed to fetch datasources from Grafana API: {e}")
            return {}

    def _resolve_template_variables_in_string(self, input_string: str, template_vars_dict: dict) -> str:
        """Resolves template variables (e.g., $var or ${var}) in a string."""
        if not input_string or not isinstance(input_string, str):
            return input_string

        resolved_string = input_string
        # Regex to find $var or ${var} or ${var:format}
        var_refs = re.findall(r"\$\{([^}]+)(?::(?:csv|json|pipe|regex|distributed))?\}|\$([a-zA-Z0-9_]+)", input_string)
        
        if var_refs:
            logger.debug(f"Found variable references in '{input_string}': {var_refs}")
            logger.debug(f"Available template variables: {template_vars_dict}")
        
        for ref_tuple in var_refs:
            var_ref = next((item for item in ref_tuple if item), None)  # Get the captured group (variable name)
            if var_ref and var_ref in template_vars_dict:
                var_value = template_vars_dict[var_ref]
                logger.debug(f"Resolving variable '{var_ref}' with value '{var_value}'")
                # Basic handling for multi-value variables (join with '|')
                if isinstance(var_value, list):
                    replacement = "|".join(map(str, var_value)) if var_value else ""
                else:
                    replacement = str(var_value) if var_value is not None else ""
                
                logger.debug(f"Replacement for '{var_ref}': '{replacement}'")
                # Replace both ${var} and $var formats, ensuring word boundaries for $var
                resolved_string = re.sub(r"\$\{" + re.escape(var_ref) + r"(?::(?:csv|json|pipe|regex|distributed))?\}",
                                         replacement, resolved_string)
                resolved_string = re.sub(r"\$" + re.escape(var_ref) + r"\b", replacement, resolved_string)
            else:
                logger.debug(f"Variable '{var_ref}' not found in template_vars_dict: {template_vars_dict}")
        
        if resolved_string != input_string:
            logger.debug(f"Resolved '{input_string}' to '{resolved_string}'")
        
        return resolved_string

    def _resolve_target_datasource(
            self, target_datasource: Union[dict, str, None], dashboard_datasource: Union[dict, None],
            template_vars_dict: dict, panel_id, datasource_name_to_uid_map: dict
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
        logger.debug(f"Resolving datasource '{datasource_value_to_resolve}' for panel {panel_id}")
        resolved_uid_or_name = self._resolve_template_variables_in_string(datasource_value_to_resolve,
                                                                          template_vars_dict)
        logger.debug(f"Resolved datasource '{datasource_value_to_resolve}' to '{resolved_uid_or_name}' for panel {panel_id}")

        if not resolved_uid_or_name:
            logger.warning(
                f"Datasource variable resolution resulted in empty value for panel {panel_id}. Original: {datasource_value_to_resolve}")
            return None

        # Check if the resolved value is a datasource name and convert to UID
        actual_uid = resolved_uid_or_name
        if resolved_uid_or_name in datasource_name_to_uid_map:
            actual_uid = datasource_name_to_uid_map[resolved_uid_or_name]
            logger.debug(f"Resolved datasource name '{resolved_uid_or_name}' to UID '{actual_uid}' for panel {panel_id}")
        
        # Grafana API /api/ds/query requires UID.
        resolved_ds = {"uid": str(actual_uid)}

        return resolved_ds

    def _prepare_grafana_queries(
            self, dashboard_dict: dict, template_vars_dict: dict, datasource_name_to_uid_map: dict, panel_ids_filter: Optional[list[str]] = None
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
                                                                      template_vars_dict, panel_id, datasource_name_to_uid_map)
                # Skip target if datasource resolution fails
                if not resolved_datasource and not expr.startswith(
                        "grafana"):  # Allow grafana expressions like grafana/alerting/list
                    continue

                # Resolve Expression Variables
                resolved_expr = self._resolve_template_variables_in_string(expr, template_vars_dict)
                if not resolved_expr:
                    continue  # Skip empty expressions

                # Build Query Object
                ref_id = letters[query_idx]
                if raw_query:
                    query_obj = {"query": resolved_expr, "refId": ref_id}
                else:
                    query_obj = {"expr": resolved_expr, "refId": ref_id}
                if resolved_datasource:
                    query_obj["datasource"] = resolved_datasource

                all_queries.append(query_obj)
                panel_ref_map[ref_id] = {"panel_id": panel_id, "panel_title": panel_title, "original_expr": expr}
                query_idx += 1

            if query_idx >= len(letters):
                break  # Break outer loop too if limit reached

        return all_queries, panel_ref_map

    def _parse_grafana_response_frames(self, response: dict, panel_ref_map: dict, metadata: Struct = None) -> list[PlaybookTaskResult]:
        """Parses the Grafana /api/ds/query response frames into a list of PlaybookTaskResults."""
        all_task_results = []
        if "results" not in response:
            return []

        for ref_id, result_data in response["results"].items():
            panel_info = panel_ref_map.get(ref_id)
            if not panel_info:
                logger.warning(f"Response for ref_id {ref_id} received but no mapping found. Skipping.")
                continue

            ref_id_timeseries = self._parse_frames_for_ref_id(result_data, panel_info, ref_id)

            # After processing all frames for a ref_id, create a TaskResult if data exists
            if ref_id_timeseries:
                panel_title = panel_info.get("panel_title", str(panel_info.get("panel_id")))
                legend_text = panel_title if panel_title else panel_info.get("original_expr", f"Query Ref: {ref_id}")

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

        return all_task_results

    def _parse_frames_for_ref_id(self, result_data: dict, panel_info: dict, ref_id: str) -> list:
        """Parses all frames within a single result_data block for a given ref_id."""
        ref_id_timeseries = []
        if "frames" not in result_data:
            logger.warning(f"No 'frames' found in result for ref_id {ref_id}. Status: {result_data.get('status')}")
            return ref_id_timeseries

        for frame in result_data.get("frames", []):
            try:
                labeled_ts_list = self._parse_single_panel_frame(frame, panel_info, ref_id)
                ref_id_timeseries.extend(labeled_ts_list)
            except Exception as frame_ex:
                logger.error(
                    f"Error processing frame for ref_id {ref_id} in panel {panel_info.get('panel_id')}: {frame_ex}",
                    exc_info=True)
        return ref_id_timeseries

    def _parse_single_panel_frame(self, frame: dict, panel_info: dict, ref_id: str) -> list:
        """Parses a single Grafana data frame into LabeledMetricTimeseries."""
        parsed_timeseries = []
        schema = frame.get("schema", {})
        data = frame.get("data", {})
        if not schema or not data or not data.get("values"):
            logger.warning(f"Skipping incomplete frame for ref_id {ref_id}")
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
                raise Exception("Task execution Failed:: No Grafana source found")

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
                    # Handle both cases: when it's already a dict or when it's a JSON string
                    logger.info(f"Task template variables value: {task.template_variables.value}, type: {type(task.template_variables.value)}")
                    user_vars = None
                    val = task.template_variables.value
                    if isinstance(val, dict):
                        user_vars = val
                    elif isinstance(val, str):
                        try:
                            user_vars = json.loads(val)
                        except json.JSONDecodeError:
                            logger.warning(f"Failed to decode template_variables as JSON, trying ast.literal_eval: {val}")
                            try:
                                user_vars = ast.literal_eval(val)
                            except (ValueError, SyntaxError) as e:
                                logger.warning(f"Could not parse template_variables: {e}")
                    
                    if isinstance(user_vars, dict):
                        override_template_vars = user_vars
                        logger.info(f"Successfully parsed template variables: {override_template_vars}")
                    else:
                        logger.warning(f"Template variables from task is not a valid object: {task.template_variables.value}")
                except Exception as e:
                    logger.warning(f"Error processing template variables: {e}, value: {task.template_variables.value}")
            
            if override_template_vars:
                template_vars_dict.update(override_template_vars)
                logger.info(f"Updated template vars dict with override: {template_vars_dict}")

            # 3. Create datasource name to UID mapping
            datasource_name_to_uid_map = self._create_datasource_name_to_uid_mapping(grafana_api_processor)

            # 4. Prepare queries for API call, passing the filter
            logger.info(f"Final template_vars_dict before query preparation: {template_vars_dict}")
            all_queries, panel_ref_map = self._prepare_grafana_queries(dashboard_dict, template_vars_dict,
                                                                       datasource_name_to_uid_map, panel_ids_filter)

            if not all_queries:
                filter_message = f"matching filter IDs: {panel_ids_filter}" if panel_ids_filter else ""
                logger.warning(
                    f"No valid queries could be prepared for dashboard UID: {dashboard_uid} {filter_message}")
                
                # Extract host URL and create metadata with Grafana URL
                host_url = self._extract_host_url_from_connector(grafana_connector)
                metadata = self._create_metadata_with_grafana_url(host_url, "dashboard", {
                    "dashboard_uid": dashboard_uid,
                    **self._get_grafana_time_params(time_range),
                    "orgId": "1"
                })
                
                return PlaybookTaskResult(
                    source=self.source,
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(
                        output=StringValue(
                            value=f"No valid metric queries could be found for dashboard UID: {dashboard_uid} {filter_message}")
                    ),
                    metadata=metadata
                )

            # 4. Execute API call
            print(
                f"Playbook Task Downstream Request: Type -> Grafana, Dashboard UID -> {dashboard_uid}, "
                f"Executing {len(all_queries)} queries across all panels.",
                flush=True,
            )

            formatted_queries = self._format_query_step_interval(all_queries, time_range)
            response = grafana_api_processor.panel_query_datasource_api(tr=time_range, queries=formatted_queries,
                                                                        interval_ms=interval_ms)

            if not response:
                logger.warning(f"No data returned from Grafana API for dashboard UID: {dashboard_uid}")
                
                # Extract host URL and create metadata with Grafana URL
                host_url = self._extract_host_url_from_connector(grafana_connector)
                metadata = self._create_metadata_with_grafana_url(host_url, "dashboard", {
                    "dashboard_uid": dashboard_uid,
                    **self._get_grafana_time_params(time_range),
                    "orgId": "1"
                })
                
                return PlaybookTaskResult(
                    source=self.source,
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(
                        value=f"No data returned from Grafana API for dashboard UID: {dashboard_uid}")),
                    metadata=metadata
                )

            # 5. Create metadata with Grafana URL
            host_url = self._extract_host_url_from_connector(grafana_connector)
            
            # Build template variables for URL
            url_template_vars = {}
            for key, value in template_vars_dict.items():
                url_template_vars[f"var-{key}"] = value
            
            metadata = self._create_metadata_with_grafana_url(host_url, "dashboard", {
                "dashboard_uid": dashboard_uid,
                **self._get_grafana_time_params(time_range),
                "orgId": "1",
                **url_template_vars
            })
            
            # 6. Parse API response into Timeseries results
            all_task_results = self._parse_grafana_response_frames(response, panel_ref_map, metadata)

            return all_task_results

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
            
            # Extract host URL and create metadata with Grafana URL
            host_url = self._extract_host_url_from_connector(grafana_connector)
            metadata = self._create_metadata_with_grafana_url(host_url, "dashboard", {
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
            
            # Extract host URL and create metadata with Grafana URL
            host_url = self._extract_host_url_from_connector(grafana_connector)
            metadata = self._create_metadata_with_grafana_url(host_url, "dashboard", {
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

    def execute_get_dashboard_config(self, time_range: TimeRange, grafana_task: Grafana,
                                     grafana_connector: ConnectorProto):
        """Executes the get dashboard config task to retrieve dashboard configuration details."""
        try:
            if not grafana_connector:
                raise Exception("Task execution Failed:: No Grafana source found")

            task = grafana_task.get_dashboard_config
            dashboard_uid = task.dashboard_uid.value

            grafana_api_processor = self.get_connector_processor(grafana_connector)

            print(
                f"Playbook Task Downstream Request: Type -> Grafana GET_DASHBOARD_CONFIG, Dashboard_UID -> {dashboard_uid}",
                flush=True,
            )

            dashboard_config = grafana_api_processor.get_dashboard_config_details(dashboard_uid)

            # Extract host URL and create metadata with Grafana URL
            host_url = self._extract_host_url_from_connector(grafana_connector)
            metadata = self._create_metadata_with_grafana_url(host_url, "dashboard", {
                "dashboard_uid": dashboard_uid,
                **self._get_grafana_time_params(time_range),
                "orgId": "1"
            })

            if not dashboard_config:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"No dashboard configuration found for UID: {dashboard_uid}")),
                    source=self.source,
                    metadata=metadata
                )

            response_struct = dict_to_proto(dashboard_config, Struct)
            output = ApiResponseResult(response_body=response_struct)

            task_result = PlaybookTaskResult(source=self.source, type=PlaybookTaskResultType.API_RESPONSE,
                                             api_response=output, metadata=metadata)
            return task_result
        except Exception as e:
            logger.error(f"Error while executing Grafana get dashboard config task: {e}")
            
            # Extract host URL and create metadata with Grafana URL
            host_url = self._extract_host_url_from_connector(grafana_connector)
            metadata = self._create_metadata_with_grafana_url(host_url, "dashboard", {
                "dashboard_uid": dashboard_uid,
                **self._get_grafana_time_params(time_range),
                "orgId": "1"
            })
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=f"Error executing dashboard config task: {str(e)}")),
                source=self.source,
                metadata=metadata
            )

    def execute_fetch_all_dashboards(self, time_range: TimeRange, grafana_task: Grafana,
                                     grafana_connector: ConnectorProto):
        """Executes the fetch all dashboards task to retrieve all dashboards from Grafana."""
        try:
            if not grafana_connector:
                raise Exception("Task execution Failed:: No Grafana source found")

            task = grafana_task.fetch_all_dashboards
            limit = task.limit.value if task.HasField("limit") and task.limit.value else 100

            grafana_api_processor = self.get_connector_processor(grafana_connector)

            print(
                f"Playbook Task Downstream Request: Type -> Grafana FETCH_ALL_DASHBOARDS, Limit -> {limit}",
                flush=True,
            )

            # Use the existing fetch_dashboards method which already returns the right format
            dashboards = grafana_api_processor.fetch_dashboards()

            # Extract host URL and create metadata with Grafana URL
            host_url = self._extract_host_url_from_connector(grafana_connector)
            metadata = self._create_metadata_with_grafana_url(host_url, "dashboard", {
                **self._get_grafana_time_params(time_range),
                "orgId": "1"
            })

            if not dashboards:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value="No dashboards found in Grafana")),
                    source=self.source,
                    metadata=metadata
                )

            # Limit the results if needed
            if limit and len(dashboards) > limit:
                dashboards = dashboards[:limit]

            response_data = {
                "status": "success",
                "total_count": len(dashboards),
                "limit": limit,
                "dashboards": dashboards
            }

            response_struct = dict_to_proto(response_data, Struct)
            output = ApiResponseResult(response_body=response_struct)

            task_result = PlaybookTaskResult(source=self.source, type=PlaybookTaskResultType.API_RESPONSE,
                                             api_response=output, metadata=metadata)
            return task_result
        except Exception as e:
            logger.error(f"Error while executing Grafana fetch all dashboards task: {e}")
            
            # Extract host URL and create metadata with Grafana URL
            host_url = self._extract_host_url_from_connector(grafana_connector)
            metadata = self._create_metadata_with_grafana_url(host_url, "dashboard", {
                **self._get_grafana_time_params(time_range),
                "orgId": "1"
            })
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=f"Error executing fetch all dashboards task: {str(e)}")),
                source=self.source,
                metadata=metadata
            )

    def execute_fetch_datasources(self, time_range: TimeRange, grafana_task: Grafana,
                                  grafana_connector: ConnectorProto):
        """Executes the fetch datasources task to retrieve all datasources from Grafana."""
        try:
            if not grafana_connector:
                raise Exception("Task execution Failed:: No Grafana source found")

            grafana_api_processor = self.get_connector_processor(grafana_connector)

            print(
                f"Playbook Task Downstream Request: Type -> Grafana FETCH_DATASOURCES",
                flush=True,
            )

            datasources = grafana_api_processor.fetch_data_sources()

            # Extract host URL and create metadata with Grafana URL
            host_url = self._extract_host_url_from_connector(grafana_connector)
            metadata = self._create_metadata_with_grafana_url(host_url, "datasource", {
                **self._get_grafana_time_params(time_range),
                "orgId": "1"
            })

            if not datasources:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value="No datasources found in Grafana")),
                    source=self.source,
                    metadata=metadata
                )

            response_data = {
                "status": "success",
                "total_count": len(datasources),
                "datasources": datasources
            }

            response_struct = dict_to_proto(response_data, Struct)
            output = ApiResponseResult(response_body=response_struct)

            task_result = PlaybookTaskResult(source=self.source, type=PlaybookTaskResultType.API_RESPONSE,
                                             api_response=output, metadata=metadata)
            return task_result
        except Exception as e:
            logger.error(f"Error while executing Grafana fetch datasources task: {e}")
            
            # Extract host URL and create metadata with Grafana URL
            host_url = self._extract_host_url_from_connector(grafana_connector)
            metadata = self._create_metadata_with_grafana_url(host_url, "datasource", {
                **self._get_grafana_time_params(time_range),
                "orgId": "1"
            })
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=f"Error executing fetch datasources task: {str(e)}")),
                source=self.source,
                metadata=metadata
            )

    def execute_fetch_folders(self, time_range: TimeRange, grafana_task: Grafana,
                              grafana_connector: ConnectorProto):
        """Executes the fetch folders task to retrieve all folders from Grafana."""
        try:
            if not grafana_connector:
                raise Exception("Task execution Failed:: No Grafana source found")

            grafana_api_processor = self.get_connector_processor(grafana_connector)

            print(
                f"Playbook Task Downstream Request: Type -> Grafana FETCH_FOLDERS",
                flush=True,
            )

            folders = grafana_api_processor.fetch_folders()

            # Extract host URL and create metadata with Grafana URL
            host_url = self._extract_host_url_from_connector(grafana_connector)
            metadata = self._create_metadata_with_grafana_url(host_url, "dashboard", {
                **self._get_grafana_time_params(time_range),
                "orgId": "1"
            })

            if not folders:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value="No folders found in Grafana")),
                    source=self.source,
                    metadata=metadata
                )

            response_data = {
                "status": "success",
                "total_count": len(folders),
                "folders": folders
            }

            response_struct = dict_to_proto(response_data, Struct)
            output = ApiResponseResult(response_body=response_struct)

            task_result = PlaybookTaskResult(source=self.source, type=PlaybookTaskResultType.API_RESPONSE,
                                             api_response=output, metadata=metadata)
            return task_result
        except Exception as e:
            logger.error(f"Error while executing Grafana fetch folders task: {e}")
            
            # Extract host URL and create metadata with Grafana URL
            host_url = self._extract_host_url_from_connector(grafana_connector)
            metadata = self._create_metadata_with_grafana_url(host_url, "dashboard", {
                **self._get_grafana_time_params(time_range),
                "orgId": "1"
            })
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=f"Error executing fetch folders task: {str(e)}")),
                source=self.source,
                metadata=metadata
            )
