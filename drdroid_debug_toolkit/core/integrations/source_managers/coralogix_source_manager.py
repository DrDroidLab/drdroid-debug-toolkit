import logging
import json
import ast
import re
import time
from datetime import datetime, timezone
from typing import Optional

from core.utils.credentilal_utils import generate_credentials_dict, get_connector_key_type_string, DISPLAY_NAME, CATEGORY, APPLICATION_MONITORING
from core.integrations.source_api_processors.coralogix_api_processor import CoralogixApiProcessor
from core.integrations.source_manager import SourceManager
from core.protos.assets.coralogix_asset_pb2 import CoralogixAssetModel, CoralogixDashboardAssetOptions
from drdroid_debug_toolkit.core.protos.base_pb2 import TimeRange
from drdroid_debug_toolkit.core.protos.connectors.connector_pb2 import Connector as ConnectorProto
from core.utils.playbooks_client import PrototypeClient
from drdroid_debug_toolkit.core.protos.base_pb2 import SourceKeyType, Source, SourceModelType
from drdroid_debug_toolkit.core.protos.literal_pb2 import LiteralType
from drdroid_debug_toolkit.core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResult, PlaybookTaskResultType, ApiResponseResult, TextResult, TimeseriesResult, TableResult, LabelValuePair
from google.protobuf.wrappers_pb2 import DoubleValue, StringValue, BoolValue, UInt64Value
from drdroid_debug_toolkit.core.protos.playbooks.source_task_definitions.coralogix_task_pb2 import Coralogix
from drdroid_debug_toolkit.core.protos.ui_definition_pb2 import FormField, FormFieldType
from google.protobuf.struct_pb2 import Struct
from drdroid_debug_toolkit.core.protos.literal_pb2 import Literal
from utils.proto_utils import dict_to_proto, proto_to_dict

logger = logging.getLogger(__name__)


def buildCoralogixUrl(domain: str, task_type: str, params: dict = None) -> str:
    """
    Build Coralogix URLs for different task types.
    
    Args:
        domain: Coralogix domain from connector (e.g., "your-domain.coralogix.com")
        task_type: Type of task ("FETCH_DASHBOARD_VARIABLES", "FETCH_DASHBOARD_WIDGETS")
        params: Dictionary containing task-specific parameters
    
    Returns:
        Complete Coralogix URL for the specific task type
    """
    if not domain:
        return ""
    
    # Ensure domain has proper protocol
    if not domain.startswith(('http://', 'https://')):
        base_url = f"https://{domain}"
    else:
        base_url = domain.rstrip('/')
    
    if params is None:
        params = {}
    
    # Only generate URLs for dashboard-related tasks
    if task_type in ["FETCH_DASHBOARD_VARIABLES", "FETCH_DASHBOARD_WIDGETS"]:
        if 'dashboard_id' in params:
            # Both tasks navigate to the same dashboard URL format
            return f"{base_url}/#/dashboards/{params['dashboard_id']}"
        else:
            return f"{base_url}/#/dashboards"
    
    else:
        logger.warning(f"Unsupported Coralogix task type: {task_type}")
        return ""


class CoralogixSourceManager(SourceManager):
    """
    Coralogix source manager for handling Coralogix integrations.
    Provides basic integration capabilities with test connection functionality.
    """

    def __init__(self):
        self.source = Source.CORALOGIX
        self.task_proto = Coralogix
        self.task_type_callable_map = {
            Coralogix.TaskType.FETCH_LOGS: {
                "executor": self.execute_fetch_logs,
                "model_types": [],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "Fetch Logs from Coralogix",
                "category": "Logs",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="query"),
                        display_name=StringValue(value="Lucene Query"),
                        description=StringValue(value="Lucene query to fetch logs (e.g., '*', 'level:ERROR', 'message:error AND service:api', 'timestamp:[2023-01-01 TO 2023-01-02]')"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.MULTILINE_FT,
                        is_optional=True,
                    ),
                    FormField(
                        key_name=StringValue(value="from_time"),
                        display_name=StringValue(value="From Time"),
                        description=StringValue(value="Start time for the query (e.g., 'now-1h', '2023-01-01T00:00:00Z')"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                    ),
                    FormField(
                        key_name=StringValue(value="to_time"),
                        display_name=StringValue(value="To Time"),
                        description=StringValue(value="End time for the query (e.g., 'now', '2023-01-01T01:00:00Z')"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                    ),
                    FormField(
                        key_name=StringValue(value="limit"),
                        display_name=StringValue(value="Limit"),
                        description=StringValue(value="Maximum number of logs to return (default: 100)"),
                        data_type=LiteralType.LONG,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                    ),
                ],
            },
            Coralogix.TaskType.FETCH_METRICS: {
                "executor": self.execute_fetch_metrics,
                "model_types": [],
                "result_type": PlaybookTaskResultType.TIMESERIES,
                "display_name": "Fetch Metrics from Coralogix",
                "category": "Metrics",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="query"),
                        display_name=StringValue(value="PromQL Query"),
                        description=StringValue(value="PromQL query for metrics (e.g., 'up', 'rate(http_requests_total[5m])', 'cpu_usage_percent{instance=\"server1\"}')"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.MULTILINE_FT,
                        is_optional=True,
                    ),
                    FormField(
                        key_name=StringValue(value="from_time"),
                        display_name=StringValue(value="From Time"),
                        description=StringValue(value="Start time for the query (e.g., 'now-1h', '2023-01-01T00:00:00Z')"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                    ),
                    FormField(
                        key_name=StringValue(value="to_time"),
                        display_name=StringValue(value="To Time"),
                        description=StringValue(value="End time for the query (e.g., 'now', '2023-01-01T01:00:00Z')"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                    ),
                    FormField(
                        key_name=StringValue(value="step"),
                        display_name=StringValue(value="Step"),
                        description=StringValue(value="Query resolution step width (e.g., '30s', '1m', '5m', '15m', '30m', '1h', '6h', '12h', '1d')"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                    ),
                ],
            },
            Coralogix.TaskType.FETCH_DASHBOARD_WIDGETS: {
                "executor": self.execute_fetch_dashboard_widgets,
                "model_types": [SourceModelType.CORALOGIX_DASHBOARD],
                "result_type": PlaybookTaskResultType.TIMESERIES,
                "display_name": "Execute queries for widgets in a Coralogix Dashboard",
                "category": "Metrics",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="dashboard_id"),
                        display_name=StringValue(value="Dashboard ID"),
                        description=StringValue(value="Select Dashboard ID"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TYPING_DROPDOWN_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="widget_ids"),
                        display_name=StringValue(value="Widget IDs (Optional, Comma-separated)"),
                        description=StringValue(
                            value="Enter comma-separated widget IDs to execute only specific widgets. Leave blank to execute all."),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                    ),
                    FormField(
                        key_name=StringValue(value="step"),
                        display_name=StringValue(value="Step"),
                        description=StringValue(value="Query resolution step width for metrics (e.g., '30s', '1m', '5m', '15m', '30m', '1h', '6h', '12h', '1d')"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                    ),
                    FormField(
                        key_name=StringValue(value="template_variables"),
                        display_name=StringValue(value="Template Variables"),
                        description=StringValue(value="JSON object with template variable values to override dashboard defaults. Supports single values or arrays for multiple values (e.g., {\"job\": \"frontend/web-server\"} or {\"job\": [\"frontend/web-server\", \"backend/user-service\"]})"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.MULTILINE_FT,
                        is_optional=True,
                    ),
                ],
            },
            Coralogix.TaskType.FETCH_DASHBOARD_VARIABLES: {
                "executor": self.execute_fetch_dashboard_variables,
                "model_types": [SourceModelType.CORALOGIX_DASHBOARD],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "Fetch all variables and their values from a Coralogix Dashboard",
                "category": "Metrics",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="dashboard_id"),
                        display_name=StringValue(value="Dashboard ID"),
                        description=StringValue(value="Select Dashboard ID to fetch variables from"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TYPING_DROPDOWN_FT,
                    ),
                ],
            }
        }
        
        self.connector_form_configs = [
            {
                "name": StringValue(value="Coralogix Configuration"),
                "description": StringValue(
                    value="Connect to Coralogix using API Key and Endpoint. SSL verification is optional. To enable links in tasks to Coralogix UI, enter this."
                ),
                "form_fields": {
                    SourceKeyType.CORALOGIX_API_KEY: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.CORALOGIX_API_KEY)),
                        display_name=StringValue(value="API Key"),
                        description=StringValue(value='e.g. "your-coralogix-api-key"'),
                        helper_text=StringValue(
                            value="Enter your Coralogix API Key from Settings > API Keys"
                        ),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                        is_sensitive=True
                    ),
                    SourceKeyType.CORALOGIX_ENDPOINT: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.CORALOGIX_ENDPOINT)),
                        display_name=StringValue(value="Endpoint"),
                        description=StringValue(value='e.g. "https://api.coralogix.com"'),
                        helper_text=StringValue(
                            value="Enter your Coralogix API endpoint URL"
                        ),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                    ),
                    SourceKeyType.CORALOGIX_DOMAIN: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.CORALOGIX_DOMAIN)),
                        display_name=StringValue(value="Domain"),
                        description=StringValue(value='e.g. "your-domain.coralogix.com"'),
                        helper_text=StringValue(
                            value="Enter your Coralogix domain for UI links (e.g., your-domain.coralogix.com)"
                        ),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                    ),
                    SourceKeyType.SSL_VERIFY: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.SSL_VERIFY)),
                        display_name=StringValue(value="SSL Verify"),
                        description=StringValue(
                            value="Enable or disable SSL certificate verification. Defaults to true."
                        ),
                        data_type=LiteralType.BOOLEAN,
                        form_field_type=FormFieldType.CHECKBOX_FT,
                        is_optional=True,
                        default_value=Literal(type=LiteralType.BOOLEAN, boolean=BoolValue(value=True)),
                    ),
                }
            }
        ]
        
        self.connector_type_details = {
            DISPLAY_NAME: "CORALOGIX",
            CATEGORY: APPLICATION_MONITORING,
        }

    def _validate_connector(self, coralogix_connector):
        """
        Validate that the connector is not None.
        
        Args:
            coralogix_connector: The Coralogix connector configuration
            
        Raises:
            Exception: If connector is None
        """
        if not coralogix_connector:
            raise Exception("No Coralogix connector configuration found")

    def get_connector_processor(self, coralogix_connector, **kwargs):
        """
        Get the Coralogix API processor instance.
        
        Args:
            coralogix_connector: The Coralogix connector configuration
            **kwargs: Additional keyword arguments
            
        Returns:
            CoralogixApiProcessor: Configured API processor instance
            
        Raises:
            Exception: If credentials cannot be generated or are invalid
        """
        self._validate_connector(coralogix_connector)
        
        generated_credentials = generate_credentials_dict(coralogix_connector.type, coralogix_connector.keys)
        
        if not generated_credentials:
            raise Exception("Failed to generate credentials for Coralogix connector. Please check your connector configuration.")
        
        # Validate required credentials
        if 'api_key' not in generated_credentials or not generated_credentials['api_key']:
            raise Exception("Coralogix API key is required but not found in connector configuration.")
        
        if 'endpoint' not in generated_credentials or not generated_credentials['endpoint']:
            raise Exception("Coralogix endpoint is required but not found in connector configuration.")
        
        # Filter out domain parameter as it's not needed for the API processor
        processor_credentials = {k: v for k, v in generated_credentials.items() if k != 'domain'}
        return CoralogixApiProcessor(**processor_credentials)

    def _extract_domain_from_connector(self, coralogix_connector: ConnectorProto) -> str:
        """Extract the domain from the Coralogix connector."""
        if not coralogix_connector or not coralogix_connector.keys:
            return ""
        
        for key in coralogix_connector.keys:
            if key.key_type == SourceKeyType.CORALOGIX_DOMAIN and key.key.value:
                return key.key.value
        
        return ""

    def _create_metadata_with_coralogix_url(self, domain: str, task_type: str, params: dict = None) -> Struct:
        """Create metadata struct with Coralogix URL."""
        if not domain:
            # Skip link generation if no domain is found
            return dict_to_proto({}, Struct)
        
        coralogix_url = buildCoralogixUrl(domain, task_type, params)
        metadata_dict = {
            "link": coralogix_url
        }
        return dict_to_proto(metadata_dict, Struct)

    def _get_coralogix_time_params(self, time_range: TimeRange) -> dict:
        """Get properly formatted time parameters for Coralogix URLs."""
        start_time = datetime.fromtimestamp(time_range.time_geq, tz=timezone.utc).isoformat()
        end_time = datetime.fromtimestamp(time_range.time_lt, tz=timezone.utc).isoformat()
        return {
            "from_time": start_time,
            "to_time": end_time
        }

    def test_connection(self, coralogix_connector):
        """
        Test the connection to Coralogix.
        
        Args:
            coralogix_connector: The Coralogix connector configuration
            
        Returns:
            bool: True if connection is successful
            
        Raises:
            Exception: If connection test fails
        """
        try:
            processor = self.get_connector_processor(coralogix_connector)
            return processor.test_connection()
            
        except Exception as e:
            logger.error(f"Error testing Coralogix connection: {e}")
            raise Exception(f"Failed to test Coralogix connection: {e}") from e

    def _parse_ndjson_results(self, ndjson_text):
        """
        Parse NDJSON text where each line may contain either {"result": {"results": [...]}} or {"results": [...]}.
        Returns a flat list of results.
        """
        results = []
        try:
            for line in ndjson_text.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue

                if isinstance(obj, dict):
                    if "result" in obj and isinstance(obj["result"], dict) and "results" in obj["result"]:
                        line_results = obj["result"].get("results", [])
                        if isinstance(line_results, list):
                            results.extend(line_results)
                    elif "results" in obj and isinstance(obj["results"], list):
                        results.extend(obj["results"])
        except Exception:
            return []

        return results

    def _normalize_logs_response(self, response):
        """
        Normalize various Coralogix logs response shapes into {"results": [...], "count": int}.
        Supports dict/list objects, stringified JSON/NDJSON, and legacy formats.
        Returns None if unable to normalize.
        """
        if isinstance(response, dict) and "results" in response and isinstance(response["results"], list):
            results = response["results"]
            return {"results": results, "count": int(response.get("count", len(results)))}

        if isinstance(response, dict) and "raw_response" in response and isinstance(response["raw_response"], str):
            parsed = self._parse_ndjson_results(response["raw_response"])
            if parsed:
                return {"results": parsed, "count": len(parsed)}

        if isinstance(response, list) and len(response) > 1 and isinstance(response[1], dict):
            second = response[1]
            if "result" in second and isinstance(second["result"], dict) and "results" in second["result"]:
                results = second["result"].get("results", [])
                if isinstance(results, list):
                    return {"results": results, "count": len(results)}

        if isinstance(response, str):
            obj = None
            try:
                obj = json.loads(response)
            except Exception:
                try:
                    obj = ast.literal_eval(response)
                except Exception:
                    obj = None

            if obj is not None:
                return self._normalize_logs_response(obj)

            parsed = self._parse_ndjson_results(response)
            if parsed:
                return {"results": parsed, "count": len(parsed)}

        return None

    def execute_fetch_logs(self, time_range: TimeRange, coralogix_task: Coralogix, coralogix_connector: ConnectorProto):
        """
        Execute the fetch logs task.
        
        Args:
            time_range: Time range for the query
            coralogix_task: The Coralogix task configuration
            coralogix_connector: The Coralogix connector configuration
            
        Returns:
            PlaybookTaskResult: Result containing the fetched logs
        """
        try:
            self._validate_connector(coralogix_connector)
            task = coralogix_task.fetch_logs
            
            # Get task parameters with defaults
            query = task.query.value if task.HasField("query") and task.query.value else "*"
            from_time = task.from_time.value if task.HasField("from_time") and task.from_time.value else "now-1h"
            to_time = task.to_time.value if task.HasField("to_time") and task.to_time.value else "now"
            limit = task.limit.value if task.HasField("limit") and task.limit.value else 100

            # Get the API processor
            processor = self.get_connector_processor(coralogix_connector)

            print(
                f"Playbook Task Downstream Request: Type -> Coralogix, Query -> {query}, "
                f"From -> {from_time}, To -> {to_time}, Limit -> {limit}"
            )

            # Fetch logs from Coralogix using Lucene query directly
            response = processor.fetch_logs(
                query=query,
                from_time=from_time,
                to_time=to_time,
                limit=limit
            )

            if not response:
                # Extract endpoint URL and create metadata with Coralogix URL
                domain = self._extract_domain_from_connector(coralogix_connector)
                time_params = self._get_coralogix_time_params(time_range)
                metadata = self._create_metadata_with_coralogix_url(domain, "logs", {
                    "query": query,
                    "from_time": from_time,
                    "to_time": to_time,
                    "limit": limit,
                    **time_params
                })
                
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"No logs returned from Coralogix for query: {query}")),
                    source=self.source,
                    metadata=metadata
                )

            
            # Handle response using normalizer first; fallback to legacy branches
            normalized = self._normalize_logs_response(response)

            if normalized is not None:
                actual_logs = normalized['results']
                log_count = normalized.get('count', len(actual_logs))

                try:
                    response_struct = dict_to_proto({'logs': actual_logs, 'count': log_count}, Struct)
                    output = ApiResponseResult(response_body=response_struct)

                    task_result = PlaybookTaskResult(
                        source=self.source, 
                        type=PlaybookTaskResultType.API_RESPONSE,
                        api_response=output
                    )
                except Exception as struct_error:
                    task_result = PlaybookTaskResult(
                        source=self.source,
                        type=PlaybookTaskResultType.TEXT,
                        text=TextResult(output=StringValue(value=f"Logs fetched successfully. Found {log_count} log entries. Full response: {str(actual_logs)}"))
                    )
            elif isinstance(response, dict) and 'results' in response:
                # New format: {'results': [...], 'count': 10}
                actual_logs = response['results']
                log_count = response.get('count', len(actual_logs))
                
                # Convert the actual log data to protobuf struct
                try:
                    response_struct = dict_to_proto({'logs': actual_logs, 'count': log_count}, Struct)
                    output = ApiResponseResult(response_body=response_struct)
                    
                    task_result = PlaybookTaskResult(
                        source=self.source, 
                        type=PlaybookTaskResultType.API_RESPONSE,
                        api_response=output
                    )
                except Exception as struct_error:
                    # Fallback to text result with full data
                    task_result = PlaybookTaskResult(
                        source=self.source,
                        type=PlaybookTaskResultType.TEXT,
                        text=TextResult(output=StringValue(value=f"Logs fetched successfully. Found {log_count} log entries. Full response: {str(actual_logs)}"))
                    )
            elif isinstance(response, list) and len(response) > 0:
                # Legacy format: list with queryId and result
                if 'result' in response[1]:
                    actual_logs = response[1]['result']['results']
                    
                    # Convert the actual log data to protobuf struct
                    try:
                        response_struct = dict_to_proto({'logs': actual_logs, 'count': len(actual_logs)}, Struct)
                        output = ApiResponseResult(response_body=response_struct)
                        
                        # Extract endpoint URL and create metadata with Coralogix URL
                        domain = self._extract_domain_from_connector(coralogix_connector)
                        time_params = self._get_coralogix_time_params(time_range)
                        metadata = self._create_metadata_with_coralogix_url(domain, "logs", {
                            "query": query,
                            "from_time": from_time,
                            "to_time": to_time,
                            "limit": limit,
                            **time_params
                        })
                        
                        task_result = PlaybookTaskResult(
                            source=self.source, 
                            type=PlaybookTaskResultType.API_RESPONSE,
                            api_response=output,
                            metadata=metadata
                        )
                    except Exception as struct_error:
                        # Fallback to text result with full data
                        task_result = PlaybookTaskResult(
                            source=self.source,
                            type=PlaybookTaskResultType.TEXT,
                            text=TextResult(output=StringValue(value=f"Logs fetched successfully. Found {len(actual_logs)} log entries. Full response: {str(actual_logs)}"))
                        )
                else:
                    # No results in the response
                    task_result = PlaybookTaskResult(
                        source=self.source,
                        type=PlaybookTaskResultType.TEXT,
                        text=TextResult(output=StringValue(value=f"No log results found. Response: {str(response)}"))
                    )
            else:
                # Fallback for unexpected response format
                task_result = PlaybookTaskResult(
                    source=self.source,
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"Unexpected response format: {str(response)}"))
                )
            
            return task_result
            
        except Exception as e:
            logger.error(f"Error executing Coralogix fetch logs task: {e}")
            
            # Extract endpoint URL and create metadata with Coralogix URL for error case
            domain = self._extract_domain_from_connector(coralogix_connector)
            time_params = self._get_coralogix_time_params(time_range)
            metadata = self._create_metadata_with_coralogix_url(domain, "logs", {
                "query": query,
                "from_time": from_time,
                "to_time": to_time,
                "limit": limit,
                **time_params
            })
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=f"Error while executing Coralogix logs task: {e}")),
                source=self.source,
                metadata=metadata
            )

    def execute_fetch_metrics(self, time_range: TimeRange, coralogix_task: Coralogix, coralogix_connector: ConnectorProto):
        """
        Execute the fetch metrics task.
        
        Args:
            time_range: Time range for the query
            coralogix_task: The Coralogix task configuration
            coralogix_connector: The Coralogix connector configuration
            
        Returns:
            PlaybookTaskResult: Result containing the fetched metrics as time series
        """
        try:
            self._validate_connector(coralogix_connector)
            task = coralogix_task.fetch_metrics
            
            # Get task parameters with defaults
            query = task.query.value if task.HasField("query") and task.query.value else "up"
            from_time = task.from_time.value if task.HasField("from_time") and task.from_time.value else "now-1h"
            to_time = task.to_time.value if task.HasField("to_time") and task.to_time.value else "now"
            step = task.step.value if task.HasField("step") and task.step.value else "1m"

            # Get the API processor
            processor = self.get_connector_processor(coralogix_connector)


            # Fetch metrics from Coralogix
            response = processor.fetch_metrics(
                query=query,
                from_time=from_time,
                to_time=to_time,
                step=step
            )

            if not response or not response.get('data') or not response['data'].get('result'):
                # Extract endpoint URL and create metadata with Coralogix URL
                domain = self._extract_domain_from_connector(coralogix_connector)
                time_params = self._get_coralogix_time_params(time_range)
                metadata = self._create_metadata_with_coralogix_url(domain, "metrics", {
                    "query": query,
                    "from_time": from_time,
                    "to_time": to_time,
                    "step": step,
                    **time_params
                })
                
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"No metrics returned from Coralogix for query: {query}")),
                    source=self.source,
                    metadata=metadata
                )

            # Process the Prometheus-compatible response into time series format
            labeled_metric_timeseries_list = []
            
            for item in response['data']['result']:
                # Extract metric labels
                labels = []
                if 'metric' in item:
                    for label_name, label_value in item['metric'].items():
                        labels.append(LabelValuePair(
                            name=StringValue(value=label_name),
                            value=StringValue(value=str(label_value))
                        ))
                
                # Extract time series data points
                datapoints = []
                if 'values' in item:
                    for value_pair in item['values']:
                        timestamp = int(float(value_pair[0]) * 1000)  # Convert to milliseconds
                        value = float(value_pair[1])
                        datapoints.append(TimeseriesResult.LabeledMetricTimeseries.Datapoint(
                            timestamp=timestamp,
                            value=DoubleValue(value=value)
                        ))
                
                # Create labeled metric time series
                labeled_metric_timeseries = TimeseriesResult.LabeledMetricTimeseries(
                    metric_label_values=labels,
                    datapoints=datapoints
                )
                labeled_metric_timeseries_list.append(labeled_metric_timeseries)
            
            # Create the time series result
            timeseries_result = TimeseriesResult(
                labeled_metric_timeseries=labeled_metric_timeseries_list
            )
            
            # Extract endpoint URL and create metadata with Coralogix URL
            domain = self._extract_domain_from_connector(coralogix_connector)
            time_params = self._get_coralogix_time_params(time_range)
            metadata = self._create_metadata_with_coralogix_url(domain, "metrics", {
                "query": query,
                "from_time": from_time,
                "to_time": to_time,
                "step": step,
                **time_params
            })
            
            task_result = PlaybookTaskResult(
                source=self.source, 
                type=PlaybookTaskResultType.TIMESERIES,
                timeseries=timeseries_result,
                metadata=metadata
            )
            
            return task_result
            
        except Exception as e:
            logger.error(f"Error executing Coralogix fetch metrics task: {e}")
            
            # Extract endpoint URL and create metadata with Coralogix URL for error case
            domain = self._extract_domain_from_connector(coralogix_connector)
            time_params = self._get_coralogix_time_params(time_range)
            metadata = self._create_metadata_with_coralogix_url(domain, "metrics", {
                "query": query,
                "from_time": from_time,
                "to_time": to_time,
                "step": step,
                **time_params
            })
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=f"Error while executing Coralogix metrics task: {e}")),
                source=self.source,
                metadata=metadata
            )

    def execute_fetch_dashboard_widgets(self, time_range: TimeRange, coralogix_task: Coralogix, coralogix_connector: ConnectorProto):
        """
        Execute queries for all widgets in a Coralogix dashboard.
        
        Args:
            time_range: Time range for the query
            coralogix_task: The Coralogix task configuration
            coralogix_connector: The Coralogix connector configuration
            
        Returns:
            list[PlaybookTaskResult]: List of results containing timeseries data from widget queries
        """
        try:
            self._validate_connector(coralogix_connector)
            task = coralogix_task.fetch_dashboard_widgets
            
            # Get task parameters with defaults
            dashboard_id = task.dashboard_id.value if task.HasField("dashboard_id") and task.dashboard_id.value else ""
            widget_ids_filter = None
            if task.HasField("widget_ids") and task.widget_ids.value:
                widget_ids_filter = [wid.strip() for wid in task.widget_ids.value.split(",") if wid.strip()]
            
            step = task.step.value if task.HasField("step") and task.step.value else "1m"
            
            # Handle template variables from the task
            template_vars_dict = {}
            if task.HasField("template_variables") and task.template_variables.value:
                try:
                    # Debug logging to understand the actual type
                    logger.debug(f"template_variables.value type: {type(task.template_variables.value)}, value: {task.template_variables.value}")
                    
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
                        # Process each variable value to handle both single values and arrays
                        for var_name, var_value in user_vars.items():
                            if isinstance(var_value, list):
                                # Keep as list for multiple values
                                template_vars_dict[var_name] = var_value
                            else:
                                # Single value
                                template_vars_dict[var_name] = var_value
                    else:
                        logger.warning(f"Template variables from task is not a valid dict or JSON object: {task.template_variables.value}")
                except (json.JSONDecodeError, TypeError, ValueError, SyntaxError) as e:
                    logger.warning(f"Failed to process template_variables: {task.template_variables.value}, error: {str(e)}")
            
            # Convert time_range to string format for API calls
            from_time = str(time_range.time_geq)
            to_time = str(time_range.time_lt)

            if not dashboard_id:
                raise Exception("Dashboard ID is required for fetching dashboard widgets")

            # Get the API processor
            processor = self.get_connector_processor(coralogix_connector)

            logger.info(
                f"Playbook Task Downstream Request: Type -> Coralogix, Dashboard ID -> {dashboard_id}, "
                f"Widget IDs Filter -> {widget_ids_filter}, Time Range -> {from_time} to {to_time}, Step -> {step}"
            )

            # 1. Get dashboard assets to extract widget information
            client = PrototypeClient()
            assets = client.get_connector_assets(
                connector_type=Source.Name(coralogix_connector.type),
                connector_id=str(coralogix_connector.id.value),
                asset_type=SourceModelType.CORALOGIX_DASHBOARD,
            )

            if not assets or not assets.coralogix or not assets.coralogix.assets:
                # Extract endpoint URL and create metadata with Coralogix URL
                domain = self._extract_domain_from_connector(coralogix_connector)
                time_params = self._get_coralogix_time_params(time_range)
                metadata = self._create_metadata_with_coralogix_url(domain, "FETCH_DASHBOARD_WIDGETS", {
                    "dashboard_id": dashboard_id,
                    "widget_ids": ",".join(widget_ids_filter) if widget_ids_filter else None,
                    **time_params
                })
                
                return [PlaybookTaskResult(
                    source=self.source,
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"No dashboard assets found for dashboard ID: {dashboard_id}")),
                    metadata=metadata
                )]

            # Find the dashboard asset matching the dashboard_id
            dashboard_asset = None
            for coralogix_asset in assets.coralogix.assets:
                if coralogix_asset.coralogix_dashboard.dashboard_id.value == dashboard_id:
                    dashboard_asset = coralogix_asset
                    break
            
            if not dashboard_asset:
                # Extract endpoint URL and create metadata with Coralogix URL
                domain = self._extract_domain_from_connector(coralogix_connector)
                time_params = self._get_coralogix_time_params(time_range)
                metadata = self._create_metadata_with_coralogix_url(domain, "FETCH_DASHBOARD_WIDGETS", {
                    "dashboard_id": dashboard_id,
                    "widget_ids": ",".join(widget_ids_filter) if widget_ids_filter else None,
                    **time_params
                })
                
                return [PlaybookTaskResult(
                    source=self.source,
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"No dashboard asset found for dashboard ID: {dashboard_id}")),
                    metadata=metadata
                )]
            
            # Get the dashboard asset
            dashboard_data = dashboard_asset.coralogix_dashboard
            
            # Convert dashboard JSON to dict
            dashboard_dict = proto_to_dict(dashboard_data.dashboard_json)
            
            # 2. Extract default template variable values and override with user-provided ones
            default_template_vars = self._extract_template_variable_values(dashboard_dict)
            if template_vars_dict:
                default_template_vars.update(template_vars_dict)
                logger.debug(f"Template variables after user override: {default_template_vars}")
            
            # 3. Extract and prepare widget queries with template variables
            widget_queries = self._extract_widget_queries(dashboard_dict, widget_ids_filter, default_template_vars)
            
            if not widget_queries:
                filter_message = f"matching filter IDs: {widget_ids_filter}" if widget_ids_filter else ""
                # Extract endpoint URL and create metadata with Coralogix URL
                domain = self._extract_domain_from_connector(coralogix_connector)
                time_params = self._get_coralogix_time_params(time_range)
                metadata = self._create_metadata_with_coralogix_url(domain, "FETCH_DASHBOARD_WIDGETS", {
                    "dashboard_id": dashboard_id,
                    "widget_ids": ",".join(widget_ids_filter) if widget_ids_filter else None,
                    **time_params
                })
                
                return [PlaybookTaskResult(
                    source=self.source,
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"No valid widget queries found for dashboard ID: {dashboard_id} {filter_message}")),
                    metadata=metadata
                )]

            # 4. Execute queries and collect results
            all_task_results = []
            
            for widget_query in widget_queries:
                try:
                    widget_id = widget_query['widget_id']
                    widget_title = widget_query['widget_title']
                    query_type = widget_query['query_type']
                    query_value = widget_query['query']
                    
                    logger.debug(f"Executing widget query: Widget ID -> {widget_id}, Title -> {widget_title}, Type -> {query_type}, Query -> {query_value}")
                    
                    if query_type == 'metrics':
                        # Execute metrics query
                        response = processor.fetch_metrics(
                            query=query_value,
                            from_time=from_time,
                            to_time=to_time,
                            step=step
                        )
                        
                        # Parse metrics response into timeseries
                        timeseries_result = self._parse_metrics_response_to_timeseries(response, widget_id, widget_title, query_value)
                        if timeseries_result:
                            all_task_results.append(timeseries_result)
                            
                    elif query_type == 'logs':
                        # Execute logs query
                        response = processor.fetch_logs(
                            query=query_value,
                            from_time=from_time,
                            to_time=to_time,
                            limit=1000
                        )
                        
                        # Parse logs response into timeseries (count-based)
                        timeseries_result = self._parse_logs_response_to_timeseries(response, widget_id, widget_title, query_value)
                        if timeseries_result:
                            all_task_results.append(timeseries_result)
                    
                    elif query_type == 'logs_table':
                        # Execute logs query for dataTable widget
                        response = processor.fetch_logs(
                            query=query_value,
                            from_time=from_time,
                            to_time=to_time,
                            limit=1000
                        )
                        
                        # Parse logs response into table format
                        columns = widget_query.get('columns', [])
                        table_result = self._parse_logs_response_to_table(response, widget_id, widget_title, query_value, columns)
                        if table_result:
                            all_task_results.append(table_result)
                    
                except Exception as widget_error:
                    logger.error(f"Error executing widget {widget_query.get('widget_id', 'unknown')}: {widget_error}")
                    # Add error result for this widget
                    error_result = PlaybookTaskResult(
                        source=self.source,
                        type=PlaybookTaskResultType.TEXT,
                        text=TextResult(output=StringValue(value=f"Error executing widget {widget_query.get('widget_id', 'unknown')}: {str(widget_error)}")),
                    )
                    all_task_results.append(error_result)

            if all_task_results:
                # Add metadata to all results
                domain = self._extract_domain_from_connector(coralogix_connector)
                time_params = self._get_coralogix_time_params(time_range)
                metadata = self._create_metadata_with_coralogix_url(domain, "FETCH_DASHBOARD_WIDGETS", {
                    "dashboard_id": dashboard_id,
                    "widget_ids": ",".join(widget_ids_filter) if widget_ids_filter else None,
                    **time_params
                })
                
                # Add metadata to all results that don't already have it
                for result in all_task_results:
                    if not result.metadata:
                        result.metadata = metadata
                
                return all_task_results
            else:
                # Extract endpoint URL and create metadata with Coralogix URL
                domain = self._extract_domain_from_connector(coralogix_connector)
                time_params = self._get_coralogix_time_params(time_range)
                metadata = self._create_metadata_with_coralogix_url(domain, "FETCH_DASHBOARD_WIDGETS", {
                    "dashboard_id": dashboard_id,
                    "widget_ids": ",".join(widget_ids_filter) if widget_ids_filter else None,
                    **time_params
                })
                
                return [PlaybookTaskResult(
                    source=self.source,
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"No data returned from any widgets in dashboard: {dashboard_id}")),
                    metadata=metadata
                )]
            
        except Exception as e:
            logger.error(f"Error executing Coralogix fetch dashboard widgets task: {e}")
            
            # Extract endpoint URL and create metadata with Coralogix URL for error case
            domain = self._extract_domain_from_connector(coralogix_connector)
            time_params = self._get_coralogix_time_params(time_range)
            metadata = self._create_metadata_with_coralogix_url(domain, "FETCH_DASHBOARD_WIDGETS", {
                "dashboard_id": dashboard_id,
                "widget_ids": ",".join(widget_ids_filter) if widget_ids_filter else None,
                **time_params
            })
            
            return [PlaybookTaskResult(
                source=self.source,
                type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=f"Error executing Coralogix fetch dashboard widgets task: {str(e)}")),
                metadata=metadata
            )]

    def execute_fetch_dashboard_variables(self, time_range: TimeRange, coralogix_task: Coralogix, coralogix_connector: ConnectorProto):
        """
        Execute the fetch dashboard variables task.
        
        Args:
            time_range: Time range for the query (not used for variables)
            coralogix_task: The Coralogix task configuration
            coralogix_connector: The Coralogix connector configuration
            
        Returns:
            PlaybookTaskResult: Result containing the dashboard variables
        """
        try:
            self._validate_connector(coralogix_connector)
            task = coralogix_task.fetch_dashboard_variables
            
            # Get task parameters
            dashboard_id = task.dashboard_id.value if task.HasField("dashboard_id") and task.dashboard_id.value else ""
            
            if not dashboard_id:
                raise Exception("Dashboard ID is required for fetching dashboard variables")

            logger.info(
                f"Playbook Task Downstream Request: Type -> Coralogix FETCH_DASHBOARD_VARIABLES, Dashboard ID -> {dashboard_id}"
            )

            # 1. Get dashboard assets to extract variable information
            client = PrototypeClient()
            assets = client.get_connector_assets(
                connector_type=Source.Name(coralogix_connector.type),
                connector_id=str(coralogix_connector.id.value),
                asset_type=SourceModelType.CORALOGIX_DASHBOARD,
            )

            if not assets or not assets.coralogix or not assets.coralogix.assets:
                # Extract endpoint URL and create metadata with Coralogix URL
                domain = self._extract_domain_from_connector(coralogix_connector)
                time_params = self._get_coralogix_time_params(time_range)
                metadata = self._create_metadata_with_coralogix_url(domain, "FETCH_DASHBOARD_VARIABLES", {
                    "dashboard_id": dashboard_id,
                    **time_params
                })
                
                return PlaybookTaskResult(
                    source=self.source,
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"No dashboard assets found for dashboard ID: {dashboard_id}")),
                    metadata=metadata
                )

            # Find the dashboard asset matching the dashboard_id
            dashboard_asset = None
            for coralogix_asset in assets.coralogix.assets:
                if coralogix_asset.coralogix_dashboard.dashboard_id.value == dashboard_id:
                    dashboard_asset = coralogix_asset
                    break
            
            if not dashboard_asset:
                # Extract endpoint URL and create metadata with Coralogix URL
                domain = self._extract_domain_from_connector(coralogix_connector)
                time_params = self._get_coralogix_time_params(time_range)
                metadata = self._create_metadata_with_coralogix_url(domain, "FETCH_DASHBOARD_VARIABLES", {
                    "dashboard_id": dashboard_id,
                    **time_params
                })
                
                return PlaybookTaskResult(
                    source=self.source,
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"No dashboard asset found for dashboard ID: {dashboard_id}")),
                    metadata=metadata
                )
            
            # Get the dashboard asset
            dashboard_data = dashboard_asset.coralogix_dashboard
            
            # Convert dashboard JSON to dict
            dashboard_dict = proto_to_dict(dashboard_data.dashboard_json)
            
            # 2. Get the API processor for executing queries
            processor = self.get_connector_processor(coralogix_connector)
            
            # 3. Extract variables from the dashboard and execute queries for query-based variables
            variables_data = self._extract_dashboard_variables(dashboard_dict, dashboard_id, processor)
            
            if not variables_data or not variables_data.get('variables'):
                # Extract endpoint URL and create metadata with Coralogix URL
                domain = self._extract_domain_from_connector(coralogix_connector)
                time_params = self._get_coralogix_time_params(time_range)
                metadata = self._create_metadata_with_coralogix_url(domain, "FETCH_DASHBOARD_VARIABLES", {
                    "dashboard_id": dashboard_id,
                    **time_params
                })
                
                return PlaybookTaskResult(
                    source=self.source,
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"No variables found for dashboard ID: {dashboard_id}. Send an empty dictionary for template variables")),
                    metadata=metadata
                )

            # Convert to protobuf struct
            response_struct = dict_to_proto(variables_data, Struct)
            output = ApiResponseResult(response_body=response_struct)
            
            # Extract endpoint URL and create metadata with Coralogix URL
            domain = self._extract_domain_from_connector(coralogix_connector)
            time_params = self._get_coralogix_time_params(time_range)
            metadata = self._create_metadata_with_coralogix_url(domain, "FETCH_DASHBOARD_VARIABLES", {
                "dashboard_id": dashboard_id,
                **time_params
            })
            
            task_result = PlaybookTaskResult(
                source=self.source, 
                type=PlaybookTaskResultType.API_RESPONSE,
                api_response=output,
                metadata=metadata
            )
            
            return task_result
            
        except Exception as e:
            logger.error(f"Error executing Coralogix fetch dashboard variables task: {e}")
            
            # Extract endpoint URL and create metadata with Coralogix URL for error case
            domain = self._extract_domain_from_connector(coralogix_connector)
            time_params = self._get_coralogix_time_params(time_range)
            metadata = self._create_metadata_with_coralogix_url(domain, "FETCH_DASHBOARD_VARIABLES", {
                "dashboard_id": dashboard_id,
                **time_params
            })
            
            return PlaybookTaskResult(
                source=self.source,
                type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=f"Error executing Coralogix fetch dashboard variables task: {str(e)}")),
                metadata=metadata
            )

    def _extract_dashboard_variables(self, dashboard_dict: dict, dashboard_id: str, processor=None) -> dict:
        """
        Extract variables from Coralogix dashboard configuration.
        
        Args:
            dashboard_dict: Dashboard configuration dictionary
            dashboard_id: Dashboard ID for reference
            processor: Coralogix API processor for executing queries
            
        Returns:
            dict: Variables data with current values and metadata
        """
        variables_data = {
            "dashboard_id": dashboard_id,
            "variables": {}
        }
        
        try:
            # Navigate to the dashboard variables section
            dashboard_info = dashboard_dict.get('dashboard', {})
            variables_v2 = dashboard_info.get('variablesV2', [])
            
            for var in variables_v2:
                if not isinstance(var, dict):
                    continue
                
                var_name = var.get('name', '')
                if not var_name:
                    continue
                
                var_info = {
                    "name": var_name,
                    "display_name": var.get('displayName', var_name),
                    "description": var.get('description', ''),
                    "display_type": var.get('displayType', ''),
                    "display_full_row": var.get('displayFullRow', False),
                    "variable_type": "unknown",
                    "current_value": None,
                    "current_label": None,
                    "allowed_values": [],
                    "default_value": None
                }
                
                # Extract current value
                value_obj = var.get('value', {})
                if value_obj:
                    # Handle different value types
                    if 'multiString' in value_obj:
                        multi_string = value_obj['multiString']
                        if 'list' in multi_string and 'values' in multi_string['list']:
                            values = multi_string['list']['values']
                            if values and len(values) > 0:
                                var_info["current_value"] = values[0].get('value', '')
                                var_info["current_label"] = values[0].get('label', '')
                    
                    elif 'singleString' in value_obj:
                        single_string = value_obj['singleString']
                        var_info["current_value"] = single_string.get('value', '')
                        var_info["current_label"] = single_string.get('label', '')
                    
                    elif 'text' in value_obj:
                        var_info["current_value"] = value_obj['text']
                
                # Extract source configuration and allowed values
                source_obj = var.get('source', {})
                if source_obj:
                    # Handle static variables
                    if 'static' in source_obj:
                        static_config = source_obj['static']
                        var_info["variable_type"] = "static"
                        
                        # Extract allowed values
                        if 'values' in static_config:
                            allowed_values = []
                            for val in static_config['values']:
                                if isinstance(val, dict):
                                    allowed_values.append({
                                        "value": val.get('value', ''),
                                        "label": val.get('label', ''),
                                        "is_default": val.get('isDefault', False)
                                    })
                                    # Set default value
                                    if val.get('isDefault', False):
                                        var_info["default_value"] = val.get('value', '')
                            var_info["allowed_values"] = allowed_values
                        
                        # Handle include all option
                        if 'allOption' in static_config:
                            all_option = static_config['allOption']
                            var_info["include_all"] = all_option.get('includeAll', False)
                            var_info["all_label"] = all_option.get('label', 'All')
                    
                    # Handle query-based variables
                    elif 'query' in source_obj:
                        query_config = source_obj['query']
                        var_info["variable_type"] = "query"
                        
                        # Handle different query types
                        if 'query' in query_config:
                            # Simple query string
                            var_info["query"] = query_config.get('query', '')
                        elif 'metricsQuery' in query_config:
                            # Metrics query structure - execute it to get actual values
                            metrics_query = query_config['metricsQuery']
                            var_info["metricsQuery"] = metrics_query
                            
                            # Execute the query to get actual allowed values
                            if processor:
                                try:
                                    allowed_values = self._execute_metrics_query_for_variable(metrics_query, processor)
                                    var_info["allowed_values"] = allowed_values
                                except Exception as e:
                                    logger.warning(f"Failed to execute metrics query for variable {var_name}: {e}")
                                    var_info["query_error"] = str(e)
                                    var_info["allowed_values"] = []
                        
                        # Common query configuration
                        var_info["datasource"] = query_config.get('datasource', {})
                        var_info["refresh"] = query_config.get('refreshStrategy', query_config.get('refresh', ''))
                        var_info["regex"] = query_config.get('regex', '')
                        var_info["sort"] = query_config.get('sort', '')
                        var_info["multi"] = query_config.get('multi', False)
                        var_info["include_all"] = query_config.get('includeAll', False)
                        var_info["all_value"] = query_config.get('allValue', '')
                        var_info["all_label"] = query_config.get('allLabel', 'All')
                        
                        # Handle allOption for query variables
                        if 'allOption' in query_config:
                            all_option = query_config['allOption']
                            var_info["include_all"] = all_option.get('includeAll', False)
                            var_info["all_label"] = all_option.get('label', 'All')
                        
                        # Handle valuesOrderDirection
                        if 'valuesOrderDirection' in query_config:
                            var_info["values_order_direction"] = query_config['valuesOrderDirection']
                    
                    # Handle textbox variables
                    elif 'text' in source_obj:
                        text_config = source_obj['text']
                        var_info["variable_type"] = "textbox"
                        var_info["default_value"] = text_config.get('value', '')
                
                # If no current value is set, use default
                if var_info["current_value"] is None and var_info["default_value"]:
                    var_info["current_value"] = var_info["default_value"]
                
                variables_data["variables"][var_name] = var_info
            
            # Add summary information
            variables_data["summary"] = {
                "total_variables": len(variables_data["variables"]),
                "variable_types": {
                    "static": len([v for v in variables_data["variables"].values() if v["variable_type"] == "static"]),
                    "query": len([v for v in variables_data["variables"].values() if v["variable_type"] == "query"]),
                    "textbox": len([v for v in variables_data["variables"].values() if v["variable_type"] == "textbox"])
                }
            }
            
        except Exception as e:
            logger.error(f"Error extracting variables from dashboard {dashboard_id}: {e}")
            variables_data["error"] = str(e)
            
        return variables_data

    def _execute_metrics_query_for_variable(self, metrics_query: dict, processor) -> list[dict]:
        """
        Execute a metrics query to get label values for a variable.
        
        Args:
            metrics_query: The metrics query structure from the variable
            processor: Coralogix API processor
            
        Returns:
            list[dict]: List of allowed values with value and label
        """
        try:
            # Extract metric name and label name from the metrics query
            if 'type' in metrics_query and 'labelValue' in metrics_query['type']:
                label_value_config = metrics_query['type']['labelValue']
                metric_name = label_value_config.get('metricName', {}).get('stringValue', '')
                label_name = label_value_config.get('labelName', {}).get('stringValue', '')
                
                if metric_name and label_name:
                    # Use Coralogix's supported API to get label values
                    allowed_values = self._fetch_label_values_from_coralogix(processor, label_name, metric_name)
                    return allowed_values
            
            return []
            
        except Exception as e:
            logger.error(f"Error executing metrics query for variable: {e}")
            raise e

    def _fetch_label_values_from_coralogix(self, processor, label_name: str, metric_name: str = None) -> list[dict]:
        """
        Fetch label values using Coralogix's supported API endpoints.
        
        Args:
            processor: Coralogix API processor
            label_name: The label name to get values for
            metric_name: Optional metric name to filter by
            
        Returns:
            list[dict]: List of allowed values with value and label
        """
        try:
            # Use the new fetch_label_values method from the processor
            response = processor.fetch_label_values(
                label_name=label_name,
                metric_name=metric_name,
                from_time="now-1h",
                to_time="now"
            )
            
            # Parse the response to extract label values
            if response and response.get('status') == 'success' and 'data' in response:
                values = response['data']
                return [{"value": val, "label": val} for val in values]
            
            return []
            
        except Exception as e:
            logger.error(f"Error fetching label values from Coralogix: {e}")
            return []


    def _extract_template_variable_values(self, dashboard_dict: dict) -> dict:
        """
        Extract current values from dashboard template variables.
        Based on the Coralogix dashboard structure with variablesV2.
        """
        template_vars_dict = {}
        try:
            dashboard_info = dashboard_dict.get('dashboard', {})
            variables_v2 = dashboard_info.get('variablesV2', [])
            
            for var in variables_v2:
                if not isinstance(var, dict):
                    continue
                
                var_name = var.get('name', '')
                if not var_name:
                    continue
                
                # Extract current value from the variable
                value_obj = var.get('value', {})
                if value_obj:
                    # Handle different value types
                    if 'multiString' in value_obj:
                        multi_string = value_obj['multiString']
                        if 'list' in multi_string and 'values' in multi_string['list']:
                            values = multi_string['list']['values']
                            if values and len(values) > 0:
                                # Extract all values from the multiString list
                                current_values = []
                                for val in values:
                                    if isinstance(val, dict) and 'value' in val:
                                        # Clean the value by removing control characters and extra whitespace
                                        clean_value = val['value'].strip()
                                        # Remove common control characters that appear in Coralogix dashboard data
                                        clean_value = clean_value.replace('\n', '').replace('\u0014', '').replace('\u0013', '')
                                        if clean_value:  # Only add non-empty values
                                            current_values.append(clean_value)
                                
                                # Store as list if multiple values, single value if only one
                                if len(current_values) == 1:
                                    template_vars_dict[var_name] = current_values[0]
                                elif len(current_values) > 1:
                                    template_vars_dict[var_name] = current_values
                                else:
                                    # No valid values found, skip this variable
                                    logger.warning(f"No valid values found for variable {var_name}")
                    
                    elif 'singleString' in value_obj:
                        single_string = value_obj['singleString']
                        current_value = single_string.get('value', '')
                        template_vars_dict[var_name] = current_value
                    
                    elif 'text' in value_obj:
                        template_vars_dict[var_name] = value_obj['text']
                
                # If no current value, try to get default from source
                if var_name not in template_vars_dict:
                    source_obj = var.get('source', {})
                    if source_obj:
                        # Handle static variables
                        if 'static' in source_obj:
                            static_config = source_obj['static']
                            if 'values' in static_config:
                                for val in static_config['values']:
                                    if isinstance(val, dict) and val.get('isDefault', False):
                                        template_vars_dict[var_name] = val.get('value', '')
                                        break
                        
                        # Handle textbox variables
                        elif 'text' in source_obj:
                            text_config = source_obj['text']
                            template_vars_dict[var_name] = text_config.get('value', '')
            
        except Exception as e:
            logger.error(f"Error extracting template variables from dashboard: {e}")
        
        return template_vars_dict

    def _resolve_template_variables_in_string(self, input_string: str, template_vars_dict: dict) -> str:
        """
        Resolves template variables (e.g., ${var}) in a string for PromQL queries.
        Handles both single and multiple values with proper operator conversion.
        """
        if not input_string or not isinstance(input_string, str):
            return input_string

        resolved_string = input_string
        var_refs = re.findall(r"\$\{([^}]+)\}", input_string)
        
        for var_ref in var_refs:
            if var_ref not in template_vars_dict:
                continue
                
            var_value = template_vars_dict[var_ref]
            
            # Normalize to list
            if isinstance(var_value, str) and ',' in var_value:
                var_value = [v.strip() for v in var_value.split(',') if v.strip()]
            elif not isinstance(var_value, list):
                var_value = [var_value] if var_value is not None else []
            
            # Filter out None values
            var_value = [v for v in var_value if v is not None]
            
            if not var_value:
                replacement = ""
            elif len(var_value) == 1:
                # Single value
                replacement = str(var_value[0])
                if self._needs_quoting_for_promql(replacement):
                    replacement = f'"{replacement}"'
            else:
                # Multiple values - create regex pattern
                escaped_values = [self._escape_for_promql_regex(str(val)) for val in var_value]
                replacement = '"' + '|'.join(escaped_values) + '"'

                # ⚡ FIX: replace `= ${var}` with `=~ replacement`
                pattern = rf'=\s*\$\{{{re.escape(var_ref)}\}}'
                resolved_string = re.sub(pattern, f'=~{replacement}', resolved_string)
                continue  # already replaced, skip normal replacement
            
            # Replace the variable (for single value case)
            resolved_string = resolved_string.replace(f"${{{var_ref}}}", replacement)
        
        if resolved_string != input_string:
            logger.debug(f"Template resolution: '{input_string}' -> '{resolved_string}'")
        return resolved_string

    def _escape_for_promql_regex(self, value: str) -> str:
        """Escape special regex characters for PromQL."""
        special_chars = r'|()[]{}^$*+?.'
        for char in special_chars:
            value = value.replace(char, f'\\{char}')
        return value

    def _needs_quoting_for_promql(self, value: str) -> bool:
        """
        Check if a value needs to be quoted in PromQL due to special characters.
        """
        if not value:
            return False
        
        # Characters that require quoting in PromQL label values
        special_chars = ['/', '\\', ':', ';', '=', '!', '~', '^', '$', '*', '+', '?', '(', ')', '[', ']', '{', '}', '|', '&', ' ', '\t', '\n']
        
        # Check if the value contains any special characters
        for char in special_chars:
            if char in value:
                return True
        
        # Also quote if the value starts with a number (to avoid confusion with numeric values)
        if value[0].isdigit():
            return True
            
        return False

    def _extract_widget_queries(self, dashboard_dict: dict, widget_ids_filter: Optional[list] = None, template_vars_dict: dict = None) -> list[dict]:
        """
        Extract queries from Coralogix dashboard widgets.
        
        Args:
            dashboard_dict: Dashboard configuration dictionary
            widget_ids_filter: Optional list of widget IDs to filter by
            template_vars_dict: Dictionary of template variable values for resolution
            
        Returns:
            list[dict]: List of widget query information
        """
        widget_queries = []
        
        try:
            # Navigate through the dashboard structure to find widgets
            dashboard_data = dashboard_dict.get('dashboard', {})
            layout = dashboard_data.get('layout', {})
            sections = layout.get('sections', [])
            
            # Convert widget_ids_filter to set for efficient lookup
            filter_set = set(widget_ids_filter) if widget_ids_filter else None
            
            for section in sections:
                rows = section.get('rows', [])
                for row in rows:
                    widgets = row.get('widgets', [])
                    for widget in widgets:
                        widget_id = widget.get('id', {}).get('value', '') if isinstance(widget.get('id'), dict) else str(widget.get('id', ''))
                        
                        # Apply widget ID filter if provided
                        if filter_set and widget_id not in filter_set:
                            continue
                        
                        widget_title = widget.get('title', f'Widget {widget_id}')
                        definition = widget.get('definition', {})
                        
                        # Extract queries based on widget type
                        query_info = self._extract_widget_query_info(widget_id, widget_title, definition, template_vars_dict)
                        if query_info:
                            widget_queries.extend(query_info)
                            
        except Exception as e:
            logger.error(f"Error extracting widget queries from dashboard: {e}")
            
        return widget_queries

    def _extract_widget_query_info(self, widget_id: str, widget_title: str, definition: dict, template_vars_dict: dict = None) -> list[dict]:
        """
        Extract query information from a single widget definition.
        
        Args:
            widget_id: Widget ID
            widget_title: Widget title
            definition: Widget definition dictionary
            template_vars_dict: Dictionary of template variable values for resolution
            
        Returns:
            list[dict]: List of query information for this widget
        """
        queries = []
        
        try:
            # Handle lineChart widgets (metrics)
            if 'lineChart' in definition:
                line_chart = definition['lineChart']
                query_definitions = line_chart.get('queryDefinitions', [])
                
                for i, query_def in enumerate(query_definitions):
                    query_info = query_def.get('query', {})
                    
                    # Handle metrics queries
                    if 'metrics' in query_info:
                        metrics_query = query_info['metrics']
                        promql_query = metrics_query.get('promqlQuery', {})
                        query_value = promql_query.get('value', '')
                        
                        if query_value:
                            # Resolve template variables in the query
                            resolved_query = self._resolve_template_variables_in_string(query_value, template_vars_dict or {})
                            
                            queries.append({
                                'widget_id': widget_id,
                                'widget_title': widget_title,
                                'query_type': 'metrics',
                                'query': resolved_query,
                                'original_query': query_value,  # Keep original for reference
                                'query_name': query_def.get('name', f'Query {i+1}')
                            })
            
            # Handle other widget types (logs, etc.)
            # Add more widget type handlers as needed
            elif 'barChart' in definition:
                # Handle bar chart widgets
                bar_chart = definition['barChart']
                query_definitions = bar_chart.get('queryDefinitions', [])
                
                for i, query_def in enumerate(query_definitions):
                    query_info = query_def.get('query', {})
                    
                    # Handle metrics queries
                    if 'metrics' in query_info:
                        metrics_query = query_info['metrics']
                        promql_query = metrics_query.get('promqlQuery', {})
                        query_value = promql_query.get('value', '')
                        
                        if query_value:
                            # Resolve template variables in the query
                            resolved_query = self._resolve_template_variables_in_string(query_value, template_vars_dict or {})
                            
                            queries.append({
                                'widget_id': widget_id,
                                'widget_title': widget_title,
                                'query_type': 'metrics',
                                'query': resolved_query,
                                'original_query': query_value,  # Keep original for reference
                                'query_name': query_def.get('name', f'Query {i+1}')
                            })
            
            # Handle DataPrime widgets (logs) - now using Lucene queries
            elif 'dataPrime' in definition:
                dataprime = definition['dataPrime']
                query_definitions = dataprime.get('queryDefinitions', [])
                
                for i, query_def in enumerate(query_definitions):
                    query_info = query_def.get('query', {})
                    
                    # Handle logs queries
                    if 'logs' in query_info:
                        logs_query = query_info['logs']
                        dataprime_query = logs_query.get('dataprimeQuery', {})
                        query_value = dataprime_query.get('value', '')
                        
                        if query_value:
                            # Resolve template variables in the query
                            resolved_query = self._resolve_template_variables_in_string(query_value, template_vars_dict or {})
                            
                            queries.append({
                                'widget_id': widget_id,
                                'widget_title': widget_title,
                                'query_type': 'logs',
                                'query': resolved_query,
                                'original_query': query_value,  # Keep original for reference
                                'query_name': query_def.get('name', f'Query {i+1}')
                            })
            
            # Handle dataTable widgets (logs in table format)
            elif 'dataTable' in definition:
                data_table = definition['dataTable']
                query_info = data_table.get('query', {})
                
                if 'dataprime' in query_info:
                    dataprime_query = query_info['dataprime']
                    dataprime_query_obj = dataprime_query.get('dataprimeQuery', {})
                    query_value = dataprime_query_obj.get('text', '')
                    
                    if query_value:
                        # Clean the query text by removing newlines and extra whitespace
                        cleaned_query = ' '.join(query_value.split())
                        
                        # Resolve template variables in the query
                        resolved_query = self._resolve_template_variables_in_string(cleaned_query, template_vars_dict or {})
                        
                        queries.append({
                            'widget_id': widget_id,
                            'widget_title': widget_title,
                            'query_type': 'logs_table',
                            'query': resolved_query,
                            'original_query': cleaned_query,  # Keep original for reference
                            'query_name': 'DataTable Query',
                            'columns': data_table.get('columns', [])
                        })
                            
        except Exception as e:
            logger.error(f"Error extracting query info from widget {widget_id}: {e}")
            
        return queries

    def _parse_metrics_response_to_timeseries(self, response: dict, widget_id: str, widget_title: str, query: str) -> Optional[PlaybookTaskResult]:
        """
        Parse Coralogix metrics response into TimeseriesResult.
        
        Args:
            response: API response from Coralogix metrics endpoint
            widget_id: Widget ID
            widget_title: Widget title
            query: Original query
            
        Returns:
            PlaybookTaskResult: Timeseries result or None if no data
        """
        try:
            if not response or not response.get('data') or not response['data'].get('result'):
                return None

            # Process the Prometheus-compatible response into time series format
            labeled_metric_timeseries_list = []
            
            for item in response['data']['result']:
                # Extract metric labels
                labels = [
                    LabelValuePair(name=StringValue(value="widget_id"), value=StringValue(value=widget_id)),
                    LabelValuePair(name=StringValue(value="widget_title"), value=StringValue(value=widget_title)),
                    LabelValuePair(name=StringValue(value="query"), value=StringValue(value=query)),
                ]
                
                if 'metric' in item:
                    for label_name, label_value in item['metric'].items():
                        labels.append(LabelValuePair(
                            name=StringValue(value=label_name),
                            value=StringValue(value=str(label_value))
                        ))
                
                # Extract time series data points
                datapoints = []
                if 'values' in item:
                    for value_pair in item['values']:
                        timestamp = int(float(value_pair[0]) * 1000)  # Convert to milliseconds
                        value = float(value_pair[1])
                        datapoints.append(TimeseriesResult.LabeledMetricTimeseries.Datapoint(
                            timestamp=timestamp,
                            value=DoubleValue(value=value)
                        ))
                
                # Create labeled metric time series
                labeled_metric_timeseries = TimeseriesResult.LabeledMetricTimeseries(
                    metric_label_values=labels,
                    datapoints=datapoints
                )
                labeled_metric_timeseries_list.append(labeled_metric_timeseries)
            
            if not labeled_metric_timeseries_list:
                return None
            
            # Create the time series result
            timeseries_result = TimeseriesResult(
                labeled_metric_timeseries=labeled_metric_timeseries_list,
                metric_expression=StringValue(value=query),
                metric_name=StringValue(value=widget_title),
            )
            
            return PlaybookTaskResult(
                source=self.source, 
                type=PlaybookTaskResultType.TIMESERIES,
                timeseries=timeseries_result
            )
            
        except Exception as e:
            logger.error(f"Error parsing metrics response for widget {widget_id}: {e}")
            return None

    def _parse_logs_response_to_timeseries(self, response: list, widget_id: str, widget_title: str, query: str) -> Optional[PlaybookTaskResult]:
        """
        Parse Coralogix logs response into TimeseriesResult (count-based).
        
        Args:
            response: API response from Coralogix logs endpoint
            widget_id: Widget ID
            widget_title: Widget title
            query: Original query
            
        Returns:
            PlaybookTaskResult: Timeseries result or None if no data
        """
        try:
            if not response or not isinstance(response, list) or len(response) < 2:
                return None

            # Handle DataPrime response structure
            if 'result' in response[1]:
                actual_logs = response[1]['result']['results']
                log_count = len(actual_logs)
                
                if log_count == 0:
                    return None
                
                # Create a simple timeseries with the log count
                # Use current timestamp as a single data point
                current_timestamp = int(time.time() * 1000)
                
                labels = [
                    LabelValuePair(name=StringValue(value="widget_id"), value=StringValue(value=widget_id)),
                    LabelValuePair(name=StringValue(value="widget_title"), value=StringValue(value=widget_title)),
                    LabelValuePair(name=StringValue(value="query"), value=StringValue(value=query)),
                    LabelValuePair(name=StringValue(value="metric_type"), value=StringValue(value="log_count")),
                ]
                
                datapoints = [
                    TimeseriesResult.LabeledMetricTimeseries.Datapoint(
                        timestamp=current_timestamp,
                        value=DoubleValue(value=float(log_count))
                    )
                ]
                
                labeled_metric_timeseries = TimeseriesResult.LabeledMetricTimeseries(
                    metric_label_values=labels,
                    datapoints=datapoints
                )
                
                timeseries_result = TimeseriesResult(
                    labeled_metric_timeseries=[labeled_metric_timeseries],
                    metric_expression=StringValue(value=query),
                    metric_name=StringValue(value=f"{widget_title} (Log Count)"),
                )
                
                return PlaybookTaskResult(
                    source=self.source, 
                    type=PlaybookTaskResultType.TIMESERIES,
                    timeseries=timeseries_result
                )
            
            return None
            
        except Exception as e:
            logger.error(f"Error parsing logs response for widget {widget_id}: {e}")
            return None

    def _parse_logs_response_to_table(self, response: list, widget_id: str, widget_title: str, query: str, columns: list = None) -> Optional[PlaybookTaskResult]:
        """
        Parse Coralogix logs response into API response for dataTable widgets.
        
        Args:
            response: API response from Coralogix logs endpoint
            widget_id: Widget ID
            widget_title: Widget title
            query: Original query
            columns: List of column definitions from the widget
            
        Returns:
            PlaybookTaskResult: API response result or None if no data
        """
        try:
            if not response or not isinstance(response, list) or len(response) < 2:
                return None

            # Handle DataPrime response structure
            if 'result' in response[1]:
                actual_logs = response[1]['result']['results']
                log_count = len(actual_logs)
                
                if log_count == 0:
                    return None
                
                # Convert the actual log data to protobuf struct (same as fetch_logs)
                try:
                    response_struct = dict_to_proto({'logs': actual_logs, 'count': log_count, 'widget_id': widget_id, 'widget_title': widget_title}, Struct)
                    output = ApiResponseResult(response_body=response_struct)
                    
                    task_result = PlaybookTaskResult(
                        source=self.source, 
                        type=PlaybookTaskResultType.API_RESPONSE,
                        api_response=output
                    )
                    return task_result
                except Exception as struct_error:
                    # Fallback to text result with full data
                    task_result = PlaybookTaskResult(
                        source=self.source,
                        type=PlaybookTaskResultType.TEXT,
                        text=TextResult(output=StringValue(value=f"Logs fetched successfully for widget {widget_title}. Found {log_count} log entries. Full response: {str(actual_logs)}"))
                    )
                    return task_result
            
            return None
            
        except Exception as e:
            logger.error(f"Error parsing logs response to table for widget {widget_id}: {e}")
            return None
