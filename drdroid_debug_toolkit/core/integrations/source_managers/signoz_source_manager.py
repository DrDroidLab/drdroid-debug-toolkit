import json
import logging
import typing
import uuid
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from google.protobuf.struct_pb2 import Struct
from google.protobuf.wrappers_pb2 import (
    BoolValue,
    DoubleValue,
    StringValue,
    Int64Value,
)

from core.integrations.source_api_processors.signoz_api_processor import SignozApiProcessor
from core.integrations.source_manager import SourceManager
from core.protos.assets.asset_pb2 import (
    AccountConnectorAssetsModelFilters,
    AccountConnectorAssets,
)
from core.protos.assets.signoz_asset_pb2 import (
    SignozDashboardModel,
)
from core.protos.base_pb2 import Source, SourceModelType, TimeRange, SourceKeyType
from core.protos.connectors.connector_pb2 import (
    Connector as ConnectorProto,
)
from core.protos.literal_pb2 import Literal, LiteralType
from core.protos.playbooks.playbook_commons_pb2 import (
    ApiResponseResult,
    LabelValuePair,
    PlaybookTaskResult,
    PlaybookTaskResultType,
    TableResult,
    TextResult,
    TimeseriesResult,
)
from core.protos.playbooks.source_task_definitions.signoz_task_pb2 import Signoz
from core.protos.ui_definition_pb2 import FormField, FormFieldType
from core.utils.credentilal_utils import generate_credentials_dict, get_connector_key_type_string, DISPLAY_NAME, CATEGORY, APPLICATION_MONITORING
from core.utils.proto_utils import dict_to_proto, proto_to_dict
from core.utils.time_utils import calculate_timeseries_bucket_size

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


def buildSignozUrl(api_url: str, task_type: str, params: dict = None) -> str:
    """
    Build SignOz URLs for different task types.
    
    Args:
        api_url: Base SignOz API URL from connector
        task_type: Type of task ("dashboards", "services", "metrics", "traces", "logs", "trace_analysis")
        params: Dictionary containing task-specific parameters
    
    Returns:
        Complete SignOz URL for the specific task type
    """
    if not api_url:
        return ""
    
    # Remove trailing slash from API URL if present
    base_url = api_url.rstrip('/')
    
    # Convert API URL to frontend URL (remove /api/v1 if present)
    if '/api/v1' in base_url:
        frontend_url = base_url.replace('/api/v1', '')
    else:
        frontend_url = base_url
    
    if params is None:
        params = {}
    
    if task_type == "dashboards":
        if 'dashboard_id' in params:
            query_params = []
            if 'start_time' in params:
                query_params.append(f"startTime={params['start_time']}")
            if 'end_time' in params:
                query_params.append(f"endTime={params['end_time']}")
            if 'duration' in params:
                query_params.append(f"relativeTime={params['duration']}")
            
            query_string = "&".join(query_params)
            return f"{frontend_url}/dashboard/{params['dashboard_id']}?{query_string}" if query_string else f"{frontend_url}/dashboard/{params['dashboard_id']}"
        else:
            return f"{frontend_url}/dashboard"
    
    elif task_type == "services":
        if 'service_name' in params:
            return f"{frontend_url}/services/{params['service_name']}"
        else:
            return f"{frontend_url}/services"
    
    elif task_type == "metrics":
        # For metrics with service_name, navigate to service page instead of metrics explorer
        if 'service_name' in params and params['service_name']:
            # Build URL with time parameters for service page
            url_params = []
            
            # Convert time parameters to milliseconds if provided
            if 'start_time' in params and params['start_time']:
                start_time_ms = _convert_to_milliseconds(params['start_time'])
                if start_time_ms:
                    url_params.append(f"startTime={start_time_ms}")
            if 'end_time' in params and params['end_time']:
                end_time_ms = _convert_to_milliseconds(params['end_time'])
                if end_time_ms:
                    url_params.append(f"endTime={end_time_ms}")
            if 'duration' in params and params['duration']:
                url_params.append(f"relativeTime={params['duration']}")
            
            # Build final URL - use service page format
            query_string = "&".join(url_params)
            return f"{frontend_url}/services/{params['service_name']}?{query_string}" if query_string else f"{frontend_url}/services/{params['service_name']}"
        
        # Fallback to metrics explorer for metrics without service_name
        # Get metrics list from params
        metrics_list = []
        if 'metrics' in params and params['metrics']:
            if isinstance(params['metrics'], str):
                try:
                    metrics_list = json.loads(params['metrics'])
                except (json.JSONDecodeError, TypeError):
                    metrics_list = [params['metrics']]  # Treat as single metric
            elif isinstance(params['metrics'], list):
                metrics_list = params['metrics']
        
        # Use first metric if available, otherwise use default
        metric_name = metrics_list[0] if metrics_list else "http_server_request_duration_sum"
        
        # Determine metric type and data type based on metric name patterns
        if "_sum" in metric_name:
            metric_type = "Sum"
        elif "_count" in metric_name:
            metric_type = "Count"
        elif "_bucket" in metric_name:
            metric_type = "Histogram"
        elif "_max" in metric_name:
            metric_type = "Max"
        elif "_min" in metric_name:
            metric_type = "Min"
        elif "_avg" in metric_name or "_average" in metric_name:
            metric_type = "Avg"
        else:
            metric_type = "Gauge"  # Default for most metrics
        
        # Determine data type based on metric type
        if metric_type in ["Count"]:
            data_type = "int64"
        else:
            data_type = "float64"  # Most metrics are float64
        
        # Get stepInterval from window parameter if provided
        step_interval = 60  # Default
        if 'window' in params and params['window']:
            try:
                step_interval = int(params['window'])
            except (ValueError, TypeError):
                step_interval = 60
        
        # Get limit from params
        limit_value = None
        if 'limit' in params and params['limit']:
            try:
                limit_value = int(params['limit'])
            except (ValueError, TypeError):
                limit_value = None
        
        # Build composite query structure with essential fields for SignOz compatibility
        composite_query = {
            "queryType": "builder",
            "builder": {
                "queryData": [{
                    "dataSource": "metrics",
                    "queryName": "A",
                    "aggregateOperator": "rate",
                    "aggregateAttribute": {
                        "key": metric_name,
                        "dataType": data_type,
                        "type": metric_type,
                        "isColumn": True,
                        "isJSON": False,
                        "id": f"{metric_name}--{data_type}--{metric_type}--true"
                    },
                    "timeAggregation": "rate",
                    "spaceAggregation": "sum",
                    "functions": [],
                    "filters": {
                        "items": [],
                        "op": "AND"
                    },
                    "expression": "A",
                    "disabled": False,
                    "stepInterval": step_interval,
                    "having": [],
                    "limit": limit_value,
                    "orderBy": [],
                    "groupBy": [],
                    "legend": "",
                    "reduceTo": "avg"
                }],
                "queryFormulas": []
            },
            "promql": [{
                "name": "A",
                "query": "",
                "legend": "",
                "disabled": False
            }],
            "clickhouse_sql": [{
                "name": "A",
                "legend": "",
                "disabled": False,
                "query": ""
            }],
            "id": str(uuid.uuid4())
        }
        
        # Build options for metrics explorer
        options = {
            "selectColumns": [],
            "maxLines": 2,
            "format": "raw",
            "fontSize": "small"
        }
        
        # Build URL with time parameters in milliseconds
        url_params = []
        
        # Convert time parameters to milliseconds if provided
        if 'start_time' in params and params['start_time']:
            start_time_ms = _convert_to_milliseconds(params['start_time'])
            if start_time_ms:
                url_params.append(f"startTime={start_time_ms}")
        if 'end_time' in params and params['end_time']:
            end_time_ms = _convert_to_milliseconds(params['end_time'])
            if end_time_ms:
                url_params.append(f"endTime={end_time_ms}")
        if 'duration' in params and params['duration']:
            url_params.append(f"relativeTime={params['duration']}")
        
        # URL encode the composite query and options
        composite_query_str = urllib.parse.quote(json.dumps(composite_query))
        options_str = urllib.parse.quote(json.dumps(options))
        url_params.append(f"compositeQuery={composite_query_str}")
        url_params.append(f"options={options_str}")
        
        # Build final URL - use /explorer path for fallback
        query_string = "&".join(url_params)
        return f"{frontend_url}/metrics-explorer/explorer?{query_string}"
    
    elif task_type == "traces":
        if 'trace_id' in params:
            return f"{frontend_url}/traces/{params['trace_id']}"
        else:
            # Get limit from params
            limit_value = None
            if 'limit' in params and params['limit']:
                try:
                    limit_value = int(params['limit'])
                except (ValueError, TypeError):
                    limit_value = None
            
            # Build composite query structure for traces matching SignOz format
            composite_query = {
                "queryType": "builder",
                "builder": {
                    "queryData": [{
                        "dataSource": "traces",
                        "queryName": "A",
                        "aggregateOperator": "noop",
                        "aggregateAttribute": {
                            "id": "------false",
                            "dataType": "",
                            "key": "",
                            "isColumn": False,
                            "type": "",
                            "isJSON": False
                        },
                        "timeAggregation": "rate",
                        "spaceAggregation": "sum",
                        "filters": {
                            "items": [],
                            "op": "AND"
                        },
                        "expression": "A",
                        "disabled": False,
                        "stepInterval": 60,
                        "limit": limit_value,
                        "orderBy": [{
                            "columnName": "timestamp",
                            "order": "desc"
                        }],
                        "groupBy": [],
                        "legend": "",
                        "reduceTo": "avg"
                    }],
                    "queryFormulas": []
                },
                "promql": [{
                    "name": "A",
                    "query": "",
                    "legend": "",
                    "disabled": False
                }],
                "clickhouse_sql": [{
                    "name": "A",
                    "legend": "",
                    "disabled": False,
                    "query": ""
                }],
                "id": str(uuid.uuid4())
            }
            
            # Add service filter if provided - minimal structure
            if 'service_name' in params and params['service_name']:
                service_filter = {
                    "id": str(uuid.uuid4()),
                    "op": "in",
                    "key": {
                        "id": "service.name",
                        "key": "service.name",
                        "dataType": "string",
                        "type": "resource",
                        "isColumn": True,
                        "isJSON": False
                    },
                    "value": [params['service_name']]
                }
                composite_query["builder"]["queryData"][0]["filters"]["items"].append(service_filter)
            
            # Build URL with time range parameters
            url_params = []
            
            # Convert time parameters to milliseconds if provided
            if 'start_time' in params and params['start_time']:
                start_time_ms = _convert_to_milliseconds(params['start_time'])
                if start_time_ms:
                    url_params.append(f"startTime={start_time_ms}")
            if 'end_time' in params and params['end_time']:
                end_time_ms = _convert_to_milliseconds(params['end_time'])
                if end_time_ms:
                    url_params.append(f"endTime={end_time_ms}")
            if 'duration' in params and params['duration']:
                url_params.append(f"relativeTime={params['duration']}")
            
            # Build options for traces
            options = {
                "selectColumns": [
                    {
                        "key": "serviceName",
                        "dataType": "string",
                        "type": "tag",
                        "isColumn": True,
                        "isJSON": False,
                        "id": "serviceName--string--tag--true",
                        "isIndexed": False
                    },
                    {
                        "key": "name",
                        "dataType": "string",
                        "type": "tag",
                        "isColumn": True,
                        "isJSON": False,
                        "id": "name--string--tag--true",
                        "isIndexed": False
                    }
                ],
                "maxLines": 2,
                "format": "raw",
                "fontSize": "small"
            }
            
            # URL encode the composite query and options
            composite_query_str = urllib.parse.quote(json.dumps(composite_query))
            options_str = urllib.parse.quote(json.dumps(options))
            url_params.append(f"compositeQuery={composite_query_str}")
            url_params.append(f"options={options_str}")
            
            # Build final URL
            query_string = "&".join(url_params)
            return f"{frontend_url}/traces-explorer?{query_string}"
    
    elif task_type == "logs":
        # Get limit from params
        limit_value = None
        if 'limit' in params and params['limit']:
            try:
                limit_value = int(params['limit'])
            except (ValueError, TypeError):
                limit_value = None
        
        # Build composite query structure for logs matching SignOz format
        composite_query = {
            "queryType": "builder",
            "builder": {
                "queryData": [{
                    "dataSource": "logs",
                    "queryName": "A",
                    "aggregateOperator": "noop",
                    "aggregateAttribute": {
                        "id": "------false",
                        "dataType": "",
                        "key": "",
                        "isColumn": False,
                        "type": "",
                        "isJSON": False
                    },
                    "timeAggregation": "rate",
                    "spaceAggregation": "sum",
                    "filters": {
                        "items": [],
                        "op": "AND"
                    },
                    "expression": "A",
                    "disabled": False,
                    "stepInterval": 60,
                    "limit": limit_value,
                    "orderBy": [{
                        "columnName": "timestamp",
                        "order": "desc"
                    }],
                    "groupBy": [],
                    "legend": "",
                    "reduceTo": "avg"
                }],
                "queryFormulas": []
            },
            "promql": [{
                "name": "A",
                "query": "",
                "legend": "",
                "disabled": False
            }],
            "clickhouse_sql": [{
                "name": "A",
                "legend": "",
                "disabled": False,
                "query": ""
            }],
            "id": str(uuid.uuid4())
        }
        
        # Add service filter if provided - minimal structure
        if 'service_name' in params and params['service_name']:
            service_filter = {
                "id": str(uuid.uuid4()),
                "op": "in",
                "key": {
                    "id": "service.name",
                    "key": "service.name",
                    "dataType": "string",
                    "type": "resource",
                    "isColumn": False,
                    "isJSON": False
                },
                "value": params['service_name']
            }
            composite_query["builder"]["queryData"][0]["filters"]["items"].append(service_filter)
        
        # Build URL with time range parameters
        url_params = []
        
        # Convert time parameters to milliseconds if provided
        if 'start_time' in params and params['start_time']:
            start_time_ms = _convert_to_milliseconds(params['start_time'])
            if start_time_ms:
                url_params.append(f"startTime={start_time_ms}")
        if 'end_time' in params and params['end_time']:
            end_time_ms = _convert_to_milliseconds(params['end_time'])
            if end_time_ms:
                url_params.append(f"endTime={end_time_ms}")
        if 'duration' in params and params['duration']:
            url_params.append(f"relativeTime={params['duration']}")
        
        # Build options for logs matching your example
        options = {
            "selectColumns": [
                {
                    "key": "timestamp",
                    "dataType": "string",
                    "type": "tag",
                    "isColumn": True,
                    "isJSON": False,
                    "id": "timestamp--string--tag--true",
                    "isIndexed": False
                },
                {
                    "key": "body",
                    "dataType": "string",
                    "type": "tag",
                    "isColumn": True,
                    "isJSON": False,
                    "id": "body--string--tag--true",
                    "isIndexed": False
                }
            ],
            "maxLines": 2,
            "format": "raw",
            "fontSize": "small"
        }
        
        # URL encode the composite query and options
        composite_query_str = urllib.parse.quote(json.dumps(composite_query))
        options_str = urllib.parse.quote(json.dumps(options))
        url_params.append(f"compositeQuery={composite_query_str}")
        url_params.append(f"options={options_str}")
        
        # Build final URL
        query_string = "&".join(url_params)
        return f"{frontend_url}/logs/logs-explorer?{query_string}"
    
    elif task_type == "trace_analysis":
        # For trace analysis, navigate to the trace explorer with the specific trace ID
        if 'trace_id' in params and params['trace_id']:
            # Build URL to view the specific trace
            trace_id = params['trace_id']
            
            # Add time range parameters if provided
            url_params = []
            if 'start_time' in params and params['start_time']:
                start_time_ms = _convert_to_milliseconds(params['start_time'])
                if start_time_ms:
                    url_params.append(f"startTime={start_time_ms}")
            if 'end_time' in params and params['end_time']:
                end_time_ms = _convert_to_milliseconds(params['end_time'])
                if end_time_ms:
                    url_params.append(f"endTime={end_time_ms}")
            if 'duration' in params and params['duration']:
                url_params.append(f"relativeTime={params['duration']}")
            
            # Add trace ID parameter
            url_params.append(f"traceId={trace_id}")
            
            query_string = "&".join(url_params)
            return f"{frontend_url}/traces/trace/{trace_id}?{query_string}" if query_string else f"{frontend_url}/traces/trace/{trace_id}"
        else:
            # If no trace ID, go to general traces explorer
            return f"{frontend_url}/traces"
    
    else:
        logger.warning(f"Unsupported SignOz task type: {task_type}")
        return frontend_url


def format_builder_queries(builder_queries):
    """Format builder queries ensuring double quotes and proper JSON structure."""
    json_str = json.dumps(builder_queries, ensure_ascii=False, indent=None)
    return json.loads(json_str)


class SignozDashboardQueryBuilder:
    """Handles the transformation of Signoz dashboard panel protos to API query formats."""

    def __init__(self, global_step: int, variables: dict):
        self.global_step = global_step
        self.variables = variables
        self.query_letter_ord = ord("A")

    def _get_next_query_letter(self) -> str:
        """Gets the next query letter (A, B, C...)."""
        letter = chr(self.query_letter_ord)
        self.query_letter_ord += 1
        if self.query_letter_ord > ord("Z"):
            logger.warning("More than 26 builder queries assigned letters, reusing.")
            # Reset or handle differently if needed, for now, warn and potentially reuse.
            self.query_letter_ord = ord("A")
        return letter

    def _clean_group_by_item(self, item: dict) -> dict:
        """Removes unnecessary keys from groupBy items."""
        if isinstance(item, dict):
            return {k: v for k, v in item.items() if k in ["key", "dataType", "type", "isColumn"]}
        return item

    def _clean_aggregate_attribute(self, agg_attr: dict) -> dict:
        """Removes unnecessary keys from aggregateAttribute."""
        if isinstance(agg_attr, dict):
            return {k: v for k, v in agg_attr.items() if k in ["key", "dataType", "type", "isColumn"]}
        return agg_attr

    def build_query_dict(self, query_data_proto) -> tuple[str, dict]:
        """Converts a QueryData proto to a dictionary suitable for the Signoz API."""
        query_dict = proto_to_dict(query_data_proto)
        current_letter = self._get_next_query_letter()

        # Apply global step and remove original
        query_dict.pop("step_interval", None)
        query_dict["stepInterval"] = self.global_step

        # Rename group_by
        if "group_by" in query_dict:
            query_dict["groupBy"] = query_dict.pop("group_by")

        # Clean nested structures
        if "groupBy" in query_dict and isinstance(query_dict["groupBy"], list):
            query_dict["groupBy"] = [self._clean_group_by_item(item) for item in query_dict["groupBy"]]

        if "aggregateAttribute" in query_dict:
            query_dict["aggregateAttribute"] = self._clean_aggregate_attribute(query_dict["aggregateAttribute"])

        # Remove potentially extraneous/generated keys
        query_dict.pop("aggregateOperator", None)  # Often derived server-side
        query_dict.pop("id", None)
        query_dict.pop("isJSON", None)

        # Assign query name and expression based on letter
        query_dict["queryName"] = current_letter
        query_dict["expression"] = current_letter  # Default expression

        # Set default legend from groupBy if legend is empty
        original_legend = query_dict.get("legend", "")
        group_by_keys = query_dict.get("groupBy", [])
        if not original_legend and isinstance(group_by_keys, list) and len(group_by_keys) > 0:
            first_group_key = group_by_keys[0].get("key")
            query_dict["legend"] = f"{{{{{first_group_key}}}}}" if first_group_key else ""
        else:
            # Keep original or empty legend if no suitable group by key
            query_dict["legend"] = original_legend

        # Ensure disabled field exists
        query_dict["disabled"] = query_dict.get("disabled", False)

        return current_letter, query_dict

    def build_panel_payload(self, panel_title: str, panel_type: str, panel_queries: dict, time_range: TimeRange) -> dict:
        """Builds the full API payload for a single panel's queries."""
        from_time = int(time_range.time_geq * 1000)
        to_time = int(time_range.time_lt * 1000)

        payload = {
            "start": from_time,
            "end": to_time,
            "step": self.global_step,
            "variables": self.variables,
            "compositeQuery": {
                "queryType": "builder",
                "panelType": panel_type,
                "builderQueries": panel_queries,
            },
        }
        return format_builder_queries(payload)  # Ensure correct JSON formatting


class SignozSourceManager(SourceManager):
    def __init__(self):
        self.source = Source.SIGNOZ
        self.task_proto = Signoz
        self.task_type_callable_map = {
            Signoz.TaskType.CLICKHOUSE_QUERY: {
                "executor": self.execute_clickhouse_query,
                "model_types": [],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "Execute Clickhouse Query",
                "category": "Query",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="query"),
                        display_name=StringValue(value="Query"),
                        description=StringValue(value="Enter Clickhouse SQL query"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.MULTILINE_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="step"),
                        display_name=StringValue(value="Step"),
                        description=StringValue(value="Step interval in seconds"),
                        data_type=LiteralType.LONG,
                        is_optional=True,
                        form_field_type=FormFieldType.TEXT_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="fill_gaps"),
                        display_name=StringValue(value="Fill Gaps"),
                        description=StringValue(value="Fill gaps in time series data (deprecated, not used in v5 API)"),
                        data_type=LiteralType.BOOLEAN,
                        is_optional=True,
                        default_value=Literal(
                            type=LiteralType.BOOLEAN,
                            boolean=BoolValue(value=False),
                        ),
                        form_field_type=FormFieldType.CHECKBOX_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="request_type"),
                        display_name=StringValue(value="Request Type"),
                        description=StringValue(value="Request type for v5 API - 'logs' or 'traces'"),
                        data_type=LiteralType.STRING,
                        is_optional=True,
                        default_value=Literal(
                            type=LiteralType.STRING,
                            string=StringValue(value="traces"),
                        ),
                        form_field_type=FormFieldType.DROPDOWN_FT,
                        valid_values=[
                            Literal(
                                type=LiteralType.STRING,
                                string=StringValue(value="traces"),
                            ),
                            Literal(
                                type=LiteralType.STRING,
                                string=StringValue(value="logs"),
                            ),
                        ],
                    ),
                ],
            },
            Signoz.TaskType.BUILDER_QUERY: {
                "executor": self.execute_builder_query,
                "model_types": [],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "Execute Query Builder Query",
                "category": "Query",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="builder_queries"),
                        display_name=StringValue(value="Builder Queries"),
                        description=StringValue(value="Enter the builder queries configuration"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.MULTILINE_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="step"),
                        display_name=StringValue(value="Step"),
                        description=StringValue(value="Step interval in seconds"),
                        data_type=LiteralType.LONG,
                        is_optional=True,
                        form_field_type=FormFieldType.TEXT_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="panel_type"),
                        display_name=StringValue(value="Panel Type"),
                        description=StringValue(value="Type of visualization panel"),
                        data_type=LiteralType.STRING,
                        is_optional=True,
                        default_value=Literal(
                            type=LiteralType.STRING,
                            string=StringValue(value="table"),
                        ),
                        form_field_type=FormFieldType.DROPDOWN_FT,
                        valid_values=[
                            Literal(
                                type=LiteralType.STRING,
                                string=StringValue(value="table"),
                            ),
                            Literal(
                                type=LiteralType.STRING,
                                string=StringValue(value="graph"),
                            ),
                            Literal(
                                type=LiteralType.STRING,
                                string=StringValue(value="value"),
                            ),
                        ],
                    ),
                ],
            },
            Signoz.TaskType.DASHBOARD_DATA: {
                "executor": self.execute_dashboard_data,
                "model_types": [],
                "result_type": PlaybookTaskResultType.TIMESERIES,  # Primary result type - can also return TABLE based on panel type
                "display_name": "Get Dashboard Data",
                "category": "Dashboard",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="dashboard_name"),
                        display_name=StringValue(value="Dashboard Name"),
                        description=StringValue(value="Enter the dashboard name"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="step"),
                        display_name=StringValue(value="Step"),
                        description=StringValue(value="Step interval in seconds"),
                        data_type=LiteralType.LONG,
                        is_optional=True,
                        form_field_type=FormFieldType.TEXT_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="variables_json"),
                        display_name=StringValue(value="Variables JSON"),
                        description=StringValue(value="Enter variable overrides as a JSON object"),
                        data_type=LiteralType.STRING,
                        is_optional=True,
                        default_value=Literal(
                            type=LiteralType.STRING,
                            string=StringValue(value="{}"),
                        ),
                        form_field_type=FormFieldType.MULTILINE_FT,  # Use multiline for JSON
                    ),
                ],
            },
            Signoz.TaskType.FETCH_DASHBOARDS: {
                "executor": self.execute_fetch_dashboards,
                "model_types": [],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "Fetch Dashboards",
                "category": "Dashboard",
                "form_fields": [],
            },
            Signoz.TaskType.FETCH_DASHBOARD_DETAILS: {
                "executor": self.execute_fetch_dashboard_details,
                "model_types": [],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "Fetch Dashboard Details",
                "category": "Dashboard",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="dashboard_id"),
                        display_name=StringValue(value="Dashboard ID"),
                        description=StringValue(value="Enter the dashboard ID"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                    ),
                ],
            },
            Signoz.TaskType.FETCH_SERVICES: {
                "executor": self.execute_fetch_services,
                "model_types": [],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "Fetch Services",
                "category": "Services",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="start_time"),
                        display_name=StringValue(value="Start Time"),
                        description=StringValue(value="Start time (RFC3339 or relative like 'now-2h')"),
                        data_type=LiteralType.STRING,
                        is_optional=True,
                        form_field_type=FormFieldType.TEXT_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="end_time"),
                        display_name=StringValue(value="End Time"),
                        description=StringValue(value="End time (RFC3339 or relative like 'now-30m')"),
                        data_type=LiteralType.STRING,
                        is_optional=True,
                        form_field_type=FormFieldType.TEXT_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="duration"),
                        display_name=StringValue(value="Duration"),
                        description=StringValue(value="Duration string (e.g., '2h', '90m')"),
                        data_type=LiteralType.STRING,
                        is_optional=True,
                        form_field_type=FormFieldType.TEXT_FT,
                    ),
                ],
            },
            Signoz.TaskType.FETCH_APM_METRICS: {
                "executor": self.execute_fetch_apm_metrics,
                "model_types": [],
                "result_type": PlaybookTaskResultType.TIMESERIES,
                "display_name": "Fetch APM Metrics",
                "category": "Metrics",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="service_name"),
                        display_name=StringValue(value="Service Name"),
                        description=StringValue(value="Name of the service to fetch metrics for"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="start_time"),
                        display_name=StringValue(value="Start Time"),
                        description=StringValue(value="Start time (RFC3339 or relative like 'now-2h')"),
                        data_type=LiteralType.STRING,
                        is_optional=True,
                        form_field_type=FormFieldType.TEXT_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="end_time"),
                        display_name=StringValue(value="End Time"),
                        description=StringValue(value="End time (RFC3339 or relative like 'now-30m')"),
                        data_type=LiteralType.STRING,
                        is_optional=True,
                        form_field_type=FormFieldType.TEXT_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="window"),
                        display_name=StringValue(value="Window"),
                        description=StringValue(value="Time window for aggregation (e.g., '1m', '5m')"),
                        data_type=LiteralType.STRING,
                        is_optional=True,
                        default_value=Literal(
                            type=LiteralType.STRING,
                            string=StringValue(value="1m"),
                        ),
                        form_field_type=FormFieldType.TEXT_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="operation_names"),
                        display_name=StringValue(value="Operation Names"),
                        description=StringValue(value="JSON array of operation names to filter by"),
                        data_type=LiteralType.STRING,
                        is_optional=True,
                        form_field_type=FormFieldType.MULTILINE_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="metrics"),
                        display_name=StringValue(value="Metrics"),
                        description=StringValue(value="JSON array of metrics to fetch (e.g., ['request_rate', 'error_rate'])"),
                        data_type=LiteralType.STRING,
                        is_optional=True,
                        form_field_type=FormFieldType.MULTILINE_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="duration"),
                        display_name=StringValue(value="Duration"),
                        description=StringValue(value="Duration string (e.g., '2h', '90m')"),
                        data_type=LiteralType.STRING,
                        is_optional=True,
                        form_field_type=FormFieldType.TEXT_FT,
                    ),
                ],
            },
            Signoz.TaskType.FETCH_LOGS: {
                "executor": self.execute_fetch_logs,
                "model_types": [],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "Fetch Logs",
                "category": "Observability",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="filter_expression"),
                        display_name=StringValue(value="Filter Expression"),
                        description=StringValue(value="Filter expression for logs (e.g., \"service.name = 'my-service'\" or \"severity_text = 'ERROR'\")"),
                        data_type=LiteralType.STRING,
                        is_optional=True,
                        form_field_type=FormFieldType.TEXT_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="start_time"),
                        display_name=StringValue(value="Start Time"),
                        description=StringValue(value="Start time (RFC3339 or relative like 'now-2h')"),
                        data_type=LiteralType.STRING,
                        is_optional=True,
                        form_field_type=FormFieldType.TEXT_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="end_time"),
                        display_name=StringValue(value="End Time"),
                        description=StringValue(value="End time (RFC3339 or relative like 'now-30m')"),
                        data_type=LiteralType.STRING,
                        is_optional=True,
                        form_field_type=FormFieldType.TEXT_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="duration"),
                        display_name=StringValue(value="Duration"),
                        description=StringValue(value="Duration string (e.g., '2h', '90m')"),
                        data_type=LiteralType.STRING,
                        is_optional=True,
                        form_field_type=FormFieldType.TEXT_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="limit"),
                        display_name=StringValue(value="Limit"),
                        description=StringValue(value="Maximum number of records to return"),
                        data_type=LiteralType.LONG,
                        is_optional=True,
                        default_value=Literal(
                            type=LiteralType.LONG,
                            long=Int64Value(value=100),
                        ),
                        form_field_type=FormFieldType.TEXT_FT,
                    ),
                ],
            },
            Signoz.TaskType.FETCH_LOGS_FOR_TRACE: {
                "executor": self.execute_fetch_logs_for_trace,
                "model_types": [],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "Fetch Logs for Trace",
                "category": "Observability",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="trace_id"),
                        display_name=StringValue(value="Trace ID"),
                        description=StringValue(value="The specific trace ID to fetch logs for"),
                        data_type=LiteralType.STRING,
                        is_optional=False,
                        form_field_type=FormFieldType.TEXT_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="limit"),
                        display_name=StringValue(value="Limit"),
                        description=StringValue(value="Maximum number of logs to return"),
                        data_type=LiteralType.LONG,
                        is_optional=True,
                        default_value=Literal(
                            type=LiteralType.LONG,
                            long=Int64Value(value=10),
                        ),
                        form_field_type=FormFieldType.TEXT_FT,
                    ),
                ],
            },
            Signoz.TaskType.FETCH_TRACES: {
                "executor": self.execute_fetch_traces,
                "model_types": [],
                "result_type": PlaybookTaskResultType.API_RESPONSE,
                "display_name": "Fetch Traces",
                "category": "Observability",
                "form_fields": [
                    FormField(
                        key_name=StringValue(value="filter_expression"),
                        display_name=StringValue(value="Filter Expression"),
                        description=StringValue(value="Filter expression for traces (e.g., \"name = 'GET /api/users'\" or \"status_code = '200'\")"),
                        data_type=LiteralType.STRING,
                        is_optional=True,
                        form_field_type=FormFieldType.TEXT_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="start_time"),
                        display_name=StringValue(value="Start Time"),
                        description=StringValue(value="Start time (RFC3339 or relative like 'now-2h')"),
                        data_type=LiteralType.STRING,
                        is_optional=True,
                        form_field_type=FormFieldType.TEXT_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="end_time"),
                        display_name=StringValue(value="End Time"),
                        description=StringValue(value="End time (RFC3339 or relative like 'now-30m')"),
                        data_type=LiteralType.STRING,
                        is_optional=True,
                        form_field_type=FormFieldType.TEXT_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="duration"),
                        display_name=StringValue(value="Duration"),
                        description=StringValue(value="Duration string (e.g., '2h', '90m')"),
                        data_type=LiteralType.STRING,
                        is_optional=True,
                        form_field_type=FormFieldType.TEXT_FT,
                    ),
                    FormField(
                        key_name=StringValue(value="limit"),
                        display_name=StringValue(value="Limit"),
                        description=StringValue(value="Maximum number of traces to return"),
                        data_type=LiteralType.LONG,
                        is_optional=True,
                        default_value=Literal(
                            type=LiteralType.LONG,
                            long=Int64Value(value=100),
                        ),
                        form_field_type=FormFieldType.TEXT_FT,
                    ),
                ],
            },
        }
        self.connector_form_configs = [
            {
                "name": StringValue(value="Signoz Connection"),
                "description": StringValue(value="Connect to Signoz using the API URL and an optional API Token."),
                "form_fields": {
                    SourceKeyType.SIGNOZ_API_URL: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.SIGNOZ_API_URL)),
                        display_name=StringValue(value="API URL"),
                        helper_text=StringValue(value="Enter the Signoz API URL"),
                        description=StringValue(value='e.g. "http://localhost:8888" or "https://query.signoz.io"'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False
                    ),
                    SourceKeyType.SIGNOZ_API_TOKEN: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.SIGNOZ_API_TOKEN)),
                        display_name=StringValue(value="API Token"),
                        helper_text=StringValue(value="(Optional) Enter your Signoz API Token for authentication."),
                        description=StringValue(value='e.g. "1234567890abcdefghijklmnopqrstuvwxyz"'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True, # API token might be optional depending on Signoz setup
                        is_sensitive=True
                    )
                }
            }
        ]
        self.connector_type_details = {
            DISPLAY_NAME: "SIGNOZ",
            CATEGORY: APPLICATION_MONITORING,
        }

    def _map_panel_type(self, panel_type):
        """
        Maps panel types from dashboard configuration to valid SigNoz API panel types.
        SigNoz API only supports: 'table', 'graph', 'value'
        """
        if not panel_type:
            return "graph"
        
        panel_type = panel_type.lower()
        
        # Mapping from various panel types to supported API types
        panel_type_mapping = {
            # Chart types that should be treated as graphs
            'bar': 'graph',
            'line': 'graph', 
            'area': 'graph',
            'histogram': 'graph',
            'pie': 'graph',
            'scatter': 'graph',
            'timeseries': 'graph',
            'graph': 'graph',
            
            # Table types
            'table': 'table',
            'list': 'table',
            
            # Value types (single number displays)
            'value': 'value',
            'stat': 'value',
            'singlestat': 'value',
            'gauge': 'value',
        }
        
        mapped_type = panel_type_mapping.get(panel_type, 'graph')
        
        if mapped_type != panel_type:
            logger.debug(f"Mapped panel type '{panel_type}' to '{mapped_type}' for SigNoz API compatibility")
        
        return mapped_type

    def get_connector_processor(self, signoz_connector, **kwargs):
        generated_credentials = generate_credentials_dict(signoz_connector.type, signoz_connector.keys)
        return SignozApiProcessor(**generated_credentials)

    def _extract_api_url_from_connector(self, signoz_connector: ConnectorProto) -> str:
        """Extract the API URL from the SignOz connector."""
        if not signoz_connector or not signoz_connector.keys:
            return ""
        
        for key in signoz_connector.keys:
            if key.key_type == SourceKeyType.SIGNOZ_API_URL and key.key.value:
                return key.key.value
        
        return ""

    def _create_metadata_with_signoz_url(self, api_url: str, task_type: str, params: dict = None) -> Struct:
        """Create metadata struct with SignOz URL."""
        signoz_url = buildSignozUrl(api_url, task_type, params)
        metadata_dict = {
            "signoz_url": signoz_url
        }
        return dict_to_proto(metadata_dict, Struct)

    # Helper function to get the step interval for queries
    def _get_step_interval(self, time_range: TimeRange, task_definition) -> int:
        """Calculates the step interval. Uses user-defined value if provided, otherwise calculates dynamically."""
        if task_definition and task_definition.HasField("step") and task_definition.step.value:
            return task_definition.step.value
        else:
            total_seconds = time_range.time_lt - time_range.time_geq
            return calculate_timeseries_bucket_size(total_seconds)

    # Helper function to convert a single Signoz query result item to TimeseriesResult
    def _convert_to_timeseries_result(
        self,
        query_result_item: dict,
        query_name: str,
        query_expression: str,
        legend: str = "",
    ) -> typing.Optional[TimeseriesResult]:
        """
        Convert a single Signoz query result item to TimeseriesResult format.
        """
        try:
            # Initialize the TimeseriesResult
            timeseries_result = TimeseriesResult(
                metric_name=StringValue(value=query_name),
                metric_expression=StringValue(value=query_expression),
            )

            # Process series directly within the item
            if "series" not in query_result_item:
                logger.warning(f"No 'series' found in query result item for {query_name}")
                return None

            for series in query_result_item["series"]:
                # Extract values
                datapoints = []
                if "values" in series:
                    for value in series["values"]:
                        if "timestamp" in value and "value" in value:
                            # Keep timestamp in milliseconds as expected by the protobuf
                            timestamp_ms = int(value["timestamp"])
                            try:
                                value_float = float(value["value"])
                                datapoints.append(
                                    TimeseriesResult.LabeledMetricTimeseries.Datapoint(
                                        timestamp=timestamp_ms,  # Pass milliseconds directly
                                        value=DoubleValue(value=value_float),
                                    )
                                )
                            except (ValueError, TypeError):
                                # Skip non-numeric values
                                logger.debug(f"Skipping non-numeric value in series for {query_name}: {value['value']}")
                                continue

                # Get labels if available
                metric_label_values = []
                if "labels" in series:
                    for label_key, label_value in series["labels"].items():
                        metric_label_values.append(
                            LabelValuePair(
                                name=StringValue(value=label_key),
                                value=StringValue(value=str(label_value)),
                            )
                        )

                # Add panel title as a label
                metric_label_values.append(
                    LabelValuePair(
                        name=StringValue(value="panel_title"),
                        value=StringValue(value=query_name),  # query_name holds the panel title
                    )
                )

                # Add legend as a label (if provided)
                query_name_letter = query_result_item.get("queryName", "")  # Get 'A', 'B', etc.
                metric_label_values.append(
                    LabelValuePair(
                        name=StringValue(value="metric_legend"),
                        # Use legend if available, otherwise fallback to query letter (A, B..)
                        value=StringValue(value=legend if legend else query_name_letter),
                    )
                )

                # Create and add the labeled metric timeseries
                if datapoints:
                    timeseries_result.labeled_metric_timeseries.append(
                        TimeseriesResult.LabeledMetricTimeseries(
                            metric_label_values=metric_label_values,
                            unit=StringValue(value=""),  # Unit info might not be directly available here
                            datapoints=datapoints,
                        )
                    )

            return timeseries_result if timeseries_result.labeled_metric_timeseries else None
        except Exception as e:
            logger.error(
                f"Error converting single query result to timeseries result for {query_name}: {e}",
                exc_info=True,
            )
            return None

    # Helper function to convert a single Signoz query result item to TableResult
    def _convert_to_table_result(self, query_result_item: dict, query_expression: str) -> typing.Optional[TableResult]:
        """
        Convert a single Signoz query result item to TableResult format.
        """
        try:
            # Initialize TableResult
            table_result = TableResult(
                raw_query=StringValue(value=query_expression),
                searchable=BoolValue(value=True),
            )

            # Process data from the single item
            if "table" in query_result_item:
                table_data = query_result_item["table"]
                if "rows" in table_data:
                    for row_data in table_data["rows"]:
                        if "data" in row_data:
                            # Each row has data fields
                            columns = []
                            for col_name, col_value in row_data["data"].items():
                                columns.append(
                                    TableResult.TableColumn(
                                        name=StringValue(value=col_name),
                                        type=StringValue(value=type(col_value).__name__),
                                        value=StringValue(value=str(col_value)),
                                    )
                                )
                            if columns:
                                table_result.rows.append(TableResult.TableRow(columns=columns))
            elif "series" in query_result_item:
                # For series data, convert to table format
                for series in query_result_item["series"]:
                    if "values" in series:
                        for value in series["values"]:
                            if "timestamp" in value and "value" in value:
                                columns = [
                                    TableResult.TableColumn(
                                        name=StringValue(value="timestamp_ms"),  # Indicate ms
                                        type=StringValue(value="number"),
                                        value=StringValue(value=str(value["timestamp"])),
                                    ),
                                    TableResult.TableColumn(
                                        name=StringValue(value="value"),
                                        type=StringValue(value="string"),  # Keep as string for flexibility
                                        value=StringValue(value=str(value["value"])),
                                    ),
                                ]
                                # Add labels as additional columns
                                if "labels" in series:
                                    for label_key, label_value in series["labels"].items():
                                        columns.append(
                                            TableResult.TableColumn(
                                                name=StringValue(value=label_key),
                                                type=StringValue(value="string"),
                                                value=StringValue(value=str(label_value)),
                                            )
                                        )
                                # Add original query name as a column
                                if "queryName" in query_result_item:
                                    columns.append(
                                        TableResult.TableColumn(
                                            name=StringValue(value="signoz_query_id"),
                                            type=StringValue(value="string"),
                                            value=StringValue(value=query_result_item["queryName"]),
                                        )
                                    )
                                table_result.rows.append(TableResult.TableRow(columns=columns))

            return table_result if table_result.rows else None
        except Exception as e:
            logger.error(
                f"Error converting single query result to table result: {e}",
                exc_info=True,
            )
            return None

    # Helper function to create the appropriate task result based on panel_type for a single query result
    def _create_task_result(
        self,
        query_result_item: dict,
        panel_type: str,
        query_name: str,
        query_expression: str,
        metadata: Struct = None,
    ) -> typing.Optional[PlaybookTaskResult]:
        """
        Create the appropriate task result for a single query's result based on its panel_type.
        """
        if not query_result_item:
            logger.warning(f"No data provided for query '{query_name}' to create task result.")
            # Return None, let the caller decide how to handle lack of data for one query
            return None

        if panel_type == "graph":
            # For graph panels, return timeseries result
            timeseries_result = self._convert_to_timeseries_result(query_result_item, query_name, query_expression)
            if timeseries_result:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TIMESERIES,
                    timeseries=timeseries_result,
                    source=self.source,
                    metadata=metadata,
                )
            else:
                logger.warning(f"Failed to convert graph panel data to timeseries for query '{query_name}'.")
                # Fallback or return None? Let's return None for now.
                return None
        elif panel_type == "table":
            # For table panels, return table result
            table_result = self._convert_to_table_result(query_result_item, query_expression)
            if table_result:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TABLE,
                    table=table_result,
                    source=self.source,
                    metadata=metadata,
                )
            else:
                logger.warning(f"Failed to convert table panel data to table result for query '{query_name}'.")
                return None

        # Default or for 'value' panel type, return API response for the single query item
        try:
            # We wrap the single query result item in a minimal structure expected by dict_to_proto
            response_struct = dict_to_proto({"result": query_result_item}, Struct)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                api_response=ApiResponseResult(
                    response_body=response_struct,
                    # Add query name/expression to metadata if needed
                ),
                source=self.source,
                metadata=metadata,
            )
        except Exception as e:
            logger.error(
                f"Failed to convert value/default panel data to API response for query '{query_name}': {e}",
                exc_info=True,
            )
            return None

    @staticmethod
    def _format_asset_variables(asset) -> str:
        """Formats the variables section for the asset descriptor."""
        if not asset.variables:
            return ""
        var_string = "  Variables:\n"
        for variable in asset.variables:
            var_name = variable.name.value
            var_type = variable.type.value
            var_selected = variable.selected_value.value
            var_desc = variable.description.value
            var_string += f"    - Name: `{var_name}`, Type: `{var_type}`, Selected: `{var_selected}`, Desc: `{var_desc}`\n"
        return var_string

    @staticmethod
    def _format_asset_panel_query(query_data) -> str:
        """Formats a single panel query for the asset descriptor."""
        query_name = query_data.queryName.value if query_data.HasField("queryName") else "Unknown"
        # Use proto_to_dict for a structured representation
        try:
            query_dict = proto_to_dict(query_data)
            # Limit verbosity by removing potentially large fields like filters for descriptor
            query_dict.pop("filters", None)
            query_string = json.dumps(query_dict, indent=2)  # Use indent 2 for less space
        except Exception:
            query_string = "[Error formatting query]"
        return f"        - Query Name: `{query_name}`, Query: `{query_string}`\n"

    @staticmethod
    def _format_asset_panel(panel) -> str:
        """Formats the panels section for the asset descriptor."""
        panel_id = panel.id.value
        panel_title = panel.title.value
        panel_type = panel.panel_type.value
        panel_string = f"    * Panel Title: `{panel_title}`, ID: `{panel_id}`, Type: `{panel_type}`\n"

        if panel.HasField("query") and panel.query.HasField("builder") and panel.query.builder.query_data:
            panel_string += "      Queries:\n"
            for query_data in panel.query.builder.query_data:
                panel_string += SignozSourceManager._format_asset_panel_query(query_data)
        return panel_string

    def execute_clickhouse_query(
        self,
        time_range: TimeRange,
        signoz_task: Signoz,
        signoz_connector: ConnectorProto,
    ) -> PlaybookTaskResult:
        try:
            if not signoz_connector:
                raise Exception("Task execution Failed:: No Signoz source found")

            task = signoz_task.clickhouse_query
            query = task.query.value
            
            # Get request_type from proto, default to "traces"
            request_type = task.request_type.value if task.HasField("request_type") and task.request_type.value else "traces"
            # Validate and normalize request_type
            if request_type not in ["logs", "traces"]:
                logger.warning(f"Invalid request_type '{request_type}', defaulting to 'traces'")
                request_type = "traces"

            signoz_api_processor = self.get_connector_processor(signoz_connector)

            # Execute the query using the new v5 API method
            result = signoz_api_processor.signoz_query_clickhouse(
                query=query,
                time_geq=time_range.time_geq,
                time_lt=time_range.time_lt,
                request_type=request_type
            )

            # Extract API URL and create metadata with SignOz URL
            api_url = self._extract_api_url_from_connector(signoz_connector)
            metadata = self._create_metadata_with_signoz_url(api_url, "metrics", {
                "start_time": int(time_range.time_geq * 1000),
                "end_time": int(time_range.time_lt * 1000),
                "query": query,
                "request_type": request_type
            })

            # Return raw API response as API_RESPONSE
            try:
                response_struct = dict_to_proto(result, Struct)
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.API_RESPONSE,
                    api_response=ApiResponseResult(response_body=response_struct),
                    source=self.source,
                    metadata=metadata,
                )
            except Exception as e:
                logger.error(f"Failed to convert result to API response: {e}", exc_info=True)
                raise Exception(f"Failed to process Clickhouse query result: {e}") from e
        except Exception as e:
            logger.error(f"Error while executing Signoz Clickhouse query task: {e}")
            raise Exception(f"Error while executing Signoz Clickhouse query task: {e}") from e

    def execute_builder_query(
        self,
        time_range: TimeRange,
        signoz_task: Signoz,
        signoz_connector: ConnectorProto,
    ) -> PlaybookTaskResult:
        try:
            if not signoz_connector:
                raise Exception("Task execution Failed:: No Signoz source found")

            task = signoz_task.builder_query
            # Clean and parse the builder queries
            builder_queries = json.loads(task.builder_queries.value)
            logger.debug(f"builder_queries: {builder_queries}")
            # Clean and format the queries
            cleaned_queries = format_builder_queries(builder_queries)

            step = self._get_step_interval(time_range, task)
            panel_type = task.panel_type.value if task.HasField("panel_type") else "table"

            signoz_api_processor = self.get_connector_processor(signoz_connector)

            # Convert timerange to milliseconds for Signoz API
            from_time = int(time_range.time_geq * 1000)
            to_time = int(time_range.time_lt * 1000)

            # Prepare the query payload
            payload = {
                "start": from_time,
                "end": to_time,
                "step": step,
                "variables": {},
                "compositeQuery": {
                    "queryType": "builder",
                    "panelType": panel_type,
                    "builderQueries": cleaned_queries,
                },
            }

            # Format the entire payload to ensure double quotes
            payload = format_builder_queries(payload)

            # Execute the query
            result = signoz_api_processor.execute_signoz_query(payload)
            logger.debug(f"result: {result}")
            query_name = "Builder Query"
            if cleaned_queries and isinstance(cleaned_queries, dict):
                first_key = next(iter(cleaned_queries), None)
                if first_key and "queryName" in cleaned_queries[first_key]:
                    query_name = cleaned_queries[first_key]["queryName"]
            
            # Extract API URL and create metadata with SignOz URL
            api_url = self._extract_api_url_from_connector(signoz_connector)
            metadata = self._create_metadata_with_signoz_url(api_url, "metrics", {
                "start_time": from_time,
                "end_time": to_time,
                "step": step
            })
            
            return self._create_task_result(result, panel_type, str(cleaned_queries), query_name, metadata)
        except Exception as e:
            logger.error(f"Error while executing Signoz builder query task: {e}")
            raise Exception(f"Error while executing Signoz builder query task: {e!s}") from e

    def _aggregate_panel_graph_results(self, api_query_results: list, panel_title: str, panel_queries: dict, metadata: Struct = None) -> typing.Optional[PlaybookTaskResult]:
        """Aggregates multiple query results for a 'graph' panel into a single TimeseriesResult."""
        all_labeled_metric_timeseries = []
        for query_result_item in api_query_results:
            query_letter = query_result_item.get("queryName")
            original_query_dict = panel_queries.get(query_letter, {})
            query_legend = original_query_dict.get("legend", query_letter or "")

            if "series" not in query_result_item:
                continue

            for series in query_result_item["series"]:
                datapoints = []
                if "values" in series:
                    for value in series["values"]:
                        if "timestamp" in value and "value" in value:
                            try:
                                timestamp_ms = int(value["timestamp"])
                                value_float = float(value["value"])
                                datapoints.append(
                                    TimeseriesResult.LabeledMetricTimeseries.Datapoint(timestamp=timestamp_ms, value=DoubleValue(value=value_float))
                                )
                            except (ValueError, TypeError):
                                continue  # Skip non-numeric

                if not datapoints:
                    continue  # Don't add series with no valid data

                metric_label_values = []
                if "labels" in series:
                    metric_label_values.extend(
                        [LabelValuePair(name=StringValue(value=k), value=StringValue(value=str(v))) for k, v in series["labels"].items()]
                    )
                metric_label_values.append(LabelValuePair(name=StringValue(value="panel_title"), value=StringValue(value=panel_title)))
                metric_label_values.append(LabelValuePair(name=StringValue(value="metric_legend"), value=StringValue(value=query_legend)))

                all_labeled_metric_timeseries.append(
                    TimeseriesResult.LabeledMetricTimeseries(
                        metric_label_values=metric_label_values, unit=StringValue(value=""), datapoints=datapoints
                    )
                )

        if all_labeled_metric_timeseries:
            aggregated_timeseries = TimeseriesResult(
                metric_name=StringValue(value=panel_title),
                metric_expression=StringValue(value=f"Aggregated queries for panel: {panel_title}"),
                labeled_metric_timeseries=all_labeled_metric_timeseries,
            )
            return PlaybookTaskResult(type=PlaybookTaskResultType.TIMESERIES, timeseries=aggregated_timeseries, source=self.source, metadata=metadata)
        else:
            logger.warning(f"No timeseries data processed for graph panel '{panel_title}'.")
            return None

    def _aggregate_panel_table_results(self, api_query_results: list, panel_title: str, metadata: Struct = None) -> typing.Optional[PlaybookTaskResult]:
        """Aggregates multiple query results for a 'table' panel into a single TableResult."""
        all_rows = []
        for query_result_item in api_query_results:
            query_letter = query_result_item.get("queryName", "")
            if "table" in query_result_item and "rows" in query_result_item["table"]:
                for row_data in query_result_item["table"]["rows"]:
                    if "data" in row_data:
                        columns = [
                            TableResult.TableColumn(
                                name=StringValue(value=k), type=StringValue(value=type(v).__name__), value=StringValue(value=str(v))
                            )
                            for k, v in row_data["data"].items()
                        ]
                        columns.append(
                            TableResult.TableColumn(
                                name=StringValue(value="signoz_query_id"), type=StringValue(value="string"), value=StringValue(value=query_letter)
                            )
                        )
                        if columns:
                            all_rows.append(TableResult.TableRow(columns=columns))
            elif "series" in query_result_item:  # Convert series to table rows
                for series in query_result_item["series"]:
                    if "values" in series:
                        for value in series["values"]:
                            if "timestamp" in value and "value" in value:
                                columns = [
                                    TableResult.TableColumn(
                                        name=StringValue(value="timestamp_ms"),
                                        type=StringValue(value="number"),
                                        value=StringValue(value=str(value["timestamp"])),
                                    ),
                                    TableResult.TableColumn(
                                        name=StringValue(value="value"),
                                        type=StringValue(value="string"),
                                        value=StringValue(value=str(value["value"])),
                                    ),
                                ]
                                if "labels" in series:
                                    columns.extend(
                                        [
                                            TableResult.TableColumn(
                                                name=StringValue(value=k), type=StringValue(value="string"), value=StringValue(value=str(v))
                                            )
                                            for k, v in series["labels"].items()
                                        ]
                                    )
                                columns.append(
                                    TableResult.TableColumn(
                                        name=StringValue(value="signoz_query_id"),
                                        type=StringValue(value="string"),
                                        value=StringValue(value=query_letter),
                                    )
                                )
                                all_rows.append(TableResult.TableRow(columns=columns))

        if all_rows:
            aggregated_table = TableResult(
                raw_query=StringValue(value=f"Aggregated queries for panel: {panel_title}"),
                rows=all_rows,
                searchable=BoolValue(value=True),
            )
            return PlaybookTaskResult(type=PlaybookTaskResultType.TABLE, table=aggregated_table, source=self.source, metadata=metadata)
        else:
            logger.warning(f"No table data processed for table panel '{panel_title}'.")
            return None

    def _execute_panel_queries(
        self,
        panel_info: dict,
        time_range: TimeRange,
        signoz_api_processor: SignozApiProcessor,
        query_builder: SignozDashboardQueryBuilder,  # Pass builder instance
    ) -> typing.Optional[PlaybookTaskResult]:
        """Executes queries for a single panel and aggregates results."""
        try:
            panel_type = panel_info["panel_type"]
            panel_title = panel_info["panel_title"]
            panel_queries = panel_info["queries"]  # Already built dicts

            if not panel_queries:
                logger.warning(f"No queries found for panel '{panel_title}'. Skipping.")
                return None

            # Build the payload using the QueryBuilder instance
            formatted_payload = query_builder.build_panel_payload(panel_title, panel_type, panel_queries, time_range)

            logger.debug(f"Executing {len(panel_queries)} queries for panel '{panel_title}' (type: {panel_type})")
            try:
                result = signoz_api_processor.execute_signoz_query(formatted_payload)
            except Exception as api_err:
                logger.error(f"API error executing queries for panel '{panel_title}': {api_err}", exc_info=True)
                # Return an error result specific to this panel? Or let the main loop handle? Let's return None.
                return None  # Indicate failure for this panel

            if result and "data" in result and "result" in result["data"] and isinstance(result["data"]["result"], list):
                api_query_results = result["data"]["result"]
                logger.info(f"Received {len(api_query_results)} results from composite query for panel '{panel_title}'.")

                if panel_type == "graph":
                    return self._aggregate_panel_graph_results(api_query_results, panel_title, panel_queries)
                elif panel_type == "table":
                    return self._aggregate_panel_table_results(api_query_results, panel_title)
                else:  # Default to API Response for 'value' or other types, returning the full composite result
                    try:
                        response_struct = dict_to_proto(result, Struct)
                        metadata_struct = dict_to_proto({"panel_title": panel_title, "panel_type": panel_type}, Struct)
                        return PlaybookTaskResult(
                            type=PlaybookTaskResultType.API_RESPONSE,
                            api_response=ApiResponseResult(response_body=response_struct, metadata=metadata_struct),
                            source=self.source,
                        )
                    except Exception as e:
                        logger.error(f"Failed to convert raw result to API response for panel '{panel_title}': {e}", exc_info=True)
                        return None  # Indicate failure
            else:
                logger.warning(f"No valid result data received for panel '{panel_title}'. Response: {result}")
                return None

        except Exception as e:
            logger.error(f"Error executing queries for panel '{panel_info.get('panel_title', 'UNKNOWN')}': {e}", exc_info=True)
            return None  # Indicate error for this panel

    def _parse_dashboard_variables(self, task: Signoz.DashboardDataTask) -> tuple[dict, typing.Optional[PlaybookTaskResult]]:
        """Parses variables JSON from task input, returning dict and error result if any."""
        variables_json = task.variables_json.value if task.HasField("variables_json") and task.variables_json.value else "{}"
        try:
            variables_dict = json.loads(variables_json) if variables_json.strip() else {}

            if not isinstance(variables_dict, dict):
                logger.warning(f"Parsed variables JSON is not a dictionary: {variables_json}. Using empty dictionary.")
                return {}, None  # Return empty dict, no error result needed for this case
            return variables_dict, None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse variables JSON: '{variables_json}'. Error: {e}", exc_info=True)
            error_result = PlaybookTaskResult(
                type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=f"Invalid Variables JSON provided: {e}")),
                source=self.source,
            )
            return {}, error_result  # Return empty dict and the error

    def _prepare_panel_queries(self, dashboard: "SignozDashboardModel", query_builder: SignozDashboardQueryBuilder) -> dict:
        """Processes panels to build a map of {panel_id: {panel_info}} including built query dicts."""
        panel_queries_map = {}
        for panel in dashboard.panels:
            if panel.query and panel.query.builder and panel.query.builder.query_data:
                raw_panel_type = panel.panel_type.value if panel.panel_type.value else "graph"  # Default type
                panel_type = self._map_panel_type(raw_panel_type)
                panel_title = panel.title.value if panel.title.value else f"Panel_{panel.id.value}"
                panel_id = panel.id.value

                built_queries = {}
                for query_data_proto in panel.query.builder.query_data:
                    try:
                        query_letter, query_dict = query_builder.build_query_dict(query_data_proto)
                        built_queries[query_letter] = query_dict
                    except Exception as build_err:
                        logger.error(f"Error building query dict for panel '{panel_title}' (ID: {panel_id}): {build_err}", exc_info=True)
                        # Decide: skip query, skip panel, or raise? Let's skip the query.
                        continue

                if built_queries:  # Only add panel if it has successfully built queries
                    panel_queries_map[panel_id] = {
                        "panel_title": panel_title,
                        "panel_type": panel_type,
                        "queries": built_queries,
                    }
        return panel_queries_map

    def _convert_panel_data_to_result(
        self, api_query_results: list, panel_title: str, dashboard_name: str, panel_type: str, metadata: Struct = None
    ) -> typing.Optional[PlaybookTaskResult]:
        """Converts panel data from API response to appropriate result format using existing aggregation logic."""
        try:
            # Create a mock panel_queries dict for the existing method - it only needs query letters and legends
            panel_queries = {}
            for query_result_item in api_query_results:
                query_letter = query_result_item.get("queryName", "")
                if query_letter:
                    panel_queries[query_letter] = {
                        "legend": query_result_item.get("legend", query_letter)
                    }
            
            # Use the appropriate aggregation method based on panel type
            if panel_type == "graph":
                task_result = self._aggregate_panel_graph_results(api_query_results, panel_title, panel_queries, metadata)
            elif panel_type == "table":
                task_result = self._aggregate_panel_table_results(api_query_results, panel_title, metadata)
            else:
                # Default to graph for unknown panel types
                logger.warning(f"Unknown panel type '{panel_type}' for panel '{panel_title}', defaulting to graph")
                task_result = self._aggregate_panel_graph_results(api_query_results, panel_title, panel_queries, metadata)
            
            if task_result:
                # Add dashboard context to the result
                if task_result.type == PlaybookTaskResultType.TIMESERIES:
                    # Add dashboard_name label to all metric timeseries
                    for labeled_metric in task_result.timeseries.labeled_metric_timeseries:
                        labeled_metric.metric_label_values.append(
                            LabelValuePair(
                                name=StringValue(value="dashboard_name"),
                                value=StringValue(value=dashboard_name)
                            )
                        )
                    # Update the metric expression to include dashboard context
                    task_result.timeseries.metric_expression.value = f"Dashboard: {dashboard_name}, Panel: {panel_title}"
                
                elif task_result.type == PlaybookTaskResultType.TABLE:
                    # Update the raw query to include dashboard context
                    task_result.table.raw_query.value = f"Dashboard: {dashboard_name}, Panel: {panel_title}"
                
                return task_result
            else:
                logger.warning(f"Failed to aggregate panel data for '{panel_title}' (type: {panel_type})")
                return None
                
        except Exception as e:
            logger.error(f"Error converting panel data for '{panel_title}' (type: {panel_type}): {e}", exc_info=True)
            return None

    def execute_dashboard_data(
        self,
        time_range: TimeRange,
        signoz_task: Signoz,
        signoz_connector: ConnectorProto,
    ) -> list[PlaybookTaskResult]:
        """Executes queries for all panels in a specified Signoz dashboard and returns timeseries or table data based on panel type."""
        try:
            if not signoz_connector:
                # Extract API URL and create metadata with SignOz URL
                api_url = self._extract_api_url_from_connector(signoz_connector)
                metadata = self._create_metadata_with_signoz_url(api_url, "dashboards", {
                    "dashboard_id": dashboard_name,
                    "start_time": start_time,
                    "end_time": end_time
                })
                return [
                    PlaybookTaskResult(
                        type=PlaybookTaskResultType.TEXT,
                        text=TextResult(output=StringValue(value="Signoz connector not found.")),
                        source=self.source,
                        metadata=metadata,
                    )
                ]

            task = signoz_task.dashboard_data
            dashboard_name = task.dashboard_name.value
            if not dashboard_name:
                # Extract API URL and create metadata with SignOz URL
                api_url = self._extract_api_url_from_connector(signoz_connector)
                metadata = self._create_metadata_with_signoz_url(api_url, "dashboards", {
                    "start_time": start_time,
                    "end_time": end_time
                })
                return [
                    PlaybookTaskResult(
                        type=PlaybookTaskResultType.TEXT,
                        text=TextResult(output=StringValue(value="Dashboard name must be provided.")),
                        source=self.source,
                        metadata=metadata,
                    )
                ]

            # Get parameters
            step = task.step.value if task.HasField("step") else None
            variables_json = task.variables_json.value if task.HasField("variables_json") else None

            # Convert time range to start/end times for the processor
            start_time = datetime.fromtimestamp(time_range.time_geq, tz=timezone.utc).isoformat()
            end_time = datetime.fromtimestamp(time_range.time_lt, tz=timezone.utc).isoformat()

            signoz_api_processor = self.get_connector_processor(signoz_connector)
            result = signoz_api_processor.fetch_dashboard_data(
                dashboard_name, start_time, end_time, step, variables_json
            )

            if result and result.get("status") == "success":
                # Extract API URL and create metadata with SignOz URL
                api_url = self._extract_api_url_from_connector(signoz_connector)
                metadata = self._create_metadata_with_signoz_url(api_url, "dashboards", {
                    "dashboard_id": dashboard_name,
                    "start_time": start_time,
                    "end_time": end_time
                })
                
                task_results = []
                dashboard_results = result.get("results", {})
                
                for panel_title, panel_data in dashboard_results.items():
                    if panel_data.get("status") == "success" and "data" in panel_data:
                        # Convert panel data to appropriate result type
                        panel_result_data = panel_data["data"]
                        if "data" in panel_result_data and "result" in panel_result_data["data"]:
                            api_query_results = panel_result_data["data"]["result"]
                            
                            if api_query_results:
                                # Get panel type from the API response, default to "graph"
                                panel_type = panel_data.get("panel_type", "graph")
                                
                                # Convert panel data using existing aggregation methods
                                panel_result = self._convert_panel_data_to_result(
                                    api_query_results, panel_title, dashboard_name, panel_type, metadata
                                )
                                
                                if panel_result:
                                    task_results.append(panel_result)
                                else:
                                    logger.warning(f"Failed to convert panel '{panel_title}' data (type: {panel_type})")
                    else:
                        logger.warning(f"Panel '{panel_title}' returned status: {panel_data.get('status', 'unknown')}")

                if task_results:
                    return task_results
                else:
                    # Extract API URL and create metadata with SignOz URL
                    api_url = self._extract_api_url_from_connector(signoz_connector)
                    metadata = self._create_metadata_with_signoz_url(api_url, "dashboards", {
                        "dashboard_id": dashboard_name,
                        "start_time": start_time,
                        "end_time": end_time
                    })
                    
                    return [
                        PlaybookTaskResult(
                            type=PlaybookTaskResultType.TEXT,
                            text=TextResult(output=StringValue(value=f"No timeseries data could be extracted from dashboard: {dashboard_name}")),
                            source=self.source,
                            metadata=metadata,
                        )
                    ]
            else:
                # Extract API URL and create metadata with SignOz URL
                api_url = self._extract_api_url_from_connector(signoz_connector)
                metadata = self._create_metadata_with_signoz_url(api_url, "dashboards", {
                    "dashboard_id": dashboard_name,
                    "start_time": start_time,
                    "end_time": end_time
                })
                
                error_msg = result.get("message", "Failed to fetch dashboard data") if result else "Failed to fetch dashboard data"
                return [
                    PlaybookTaskResult(
                        type=PlaybookTaskResultType.TEXT,
                        text=TextResult(output=StringValue(value=error_msg)),
                        source=self.source,
                        metadata=metadata,
                    )
                ]

        except Exception as e:
            logger.error(f"Error while executing Signoz dashboard data task: {e}", exc_info=True)
            # Extract API URL and create metadata with SignOz URL
            api_url = self._extract_api_url_from_connector(signoz_connector)
            metadata = self._create_metadata_with_signoz_url(api_url, "dashboards", {
                "dashboard_id": dashboard_name,
                "start_time": start_time,
                "end_time": end_time
            })
            
            return [
                PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"Unexpected error executing dashboard task: {e}")),
                    source=self.source,
                    metadata=metadata,
                )
            ]

    def execute_fetch_dashboards(
        self,
        time_range: TimeRange,
        signoz_task: Signoz,
        signoz_connector: ConnectorProto,
    ) -> PlaybookTaskResult:
        """Executes fetch dashboards task."""
        try:
            if not signoz_connector:
                raise Exception("Task execution Failed:: No Signoz source found")

            signoz_api_processor = self.get_connector_processor(signoz_connector)
            result = signoz_api_processor.fetch_dashboards()

            if result:
                # Handle different response formats from fetch_dashboards
                if isinstance(result, list):
                    # If result is a list of dashboards, wrap it in a dictionary
                    response_data = {"dashboards": result}
                elif isinstance(result, dict):
                    # If result is already a dictionary, use it as is
                    response_data = result
                else:
                    # Fallback: wrap in a generic structure
                    response_data = {"data": result}
                
                response_struct = dict_to_proto(response_data, Struct)
                
                # Extract API URL and create metadata with SignOz URL
                api_url = self._extract_api_url_from_connector(signoz_connector)
                metadata = self._create_metadata_with_signoz_url(api_url, "dashboards")
                
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.API_RESPONSE,
                    api_response=ApiResponseResult(response_body=response_struct),
                    source=self.source,
                    metadata=metadata,
                )
            else:
                # Extract API URL and create metadata with SignOz URL
                api_url = self._extract_api_url_from_connector(signoz_connector)
                metadata = self._create_metadata_with_signoz_url(api_url, "dashboards")
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.API_RESPONSE,
                    text=TextResult(output=StringValue(value="Failed to fetch dashboards")),
                    source=self.source,
                    metadata=metadata,
                )
        except Exception as e:
            logger.error(f"Error while executing Signoz fetch dashboards task: {e}")
            # Extract API URL and create metadata with SignOz URL for error case
            api_url = self._extract_api_url_from_connector(signoz_connector)
            metadata = self._create_metadata_with_signoz_url(api_url, "dashboards")
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                text=TextResult(output=StringValue(value=f"Error executing fetch dashboards task: {e}")),
                source=self.source,
                metadata=metadata,
            )

    def execute_fetch_dashboard_details(
        self,
        time_range: TimeRange,
        signoz_task: Signoz,
        signoz_connector: ConnectorProto,
    ) -> PlaybookTaskResult:
        """Executes fetch dashboard details task."""
        try:
            if not signoz_connector:
                raise Exception("Task execution Failed:: No Signoz source found")

            task = signoz_task.fetch_dashboard_details
            dashboard_id = task.dashboard_id.value
            if not dashboard_id:
                # Extract time parameters from time_range
                start_time = time_range.time_geq * 1000 if time_range.time_geq else None
                end_time = time_range.time_lt * 1000 if time_range.time_lt else None
                
                # Extract API URL and create metadata with SignOz URL
                api_url = self._extract_api_url_from_connector(signoz_connector)
                metadata = self._create_metadata_with_signoz_url(api_url, "dashboards", {
                    "start_time": start_time,
                    "end_time": end_time
                })
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.API_RESPONSE,
                    text=TextResult(output=StringValue(value="Dashboard ID must be provided.")),
                    source=self.source,
                    metadata=metadata,
                )

            signoz_api_processor = self.get_connector_processor(signoz_connector)
            result = signoz_api_processor.fetch_dashboard_details(dashboard_id)

            # Extract variables from dashboard metadata and attach as 'variables_extracted'
            try:
                variables_extracted = signoz_api_processor.extract_dashboard_variables_from_details(result, resolve_queries=True)
                if isinstance(result, dict):
                    # Attach into the response_data structure under a known key
                    result_with_vars = dict(result)
                    result_with_vars["variables_extracted"] = variables_extracted
                    result = result_with_vars
            except Exception as _var_err:
                logger.warning(f"Failed to extract Signoz dashboard variables for {dashboard_id}: {_var_err}")

            if result:
                # Handle different response formats from fetch_dashboard_details
                if isinstance(result, dict):
                    # If result is already a dictionary, use it as is
                    response_data = result
                else:
                    # Fallback: wrap in a generic structure
                    response_data = {"data": result}
                
                response_struct = dict_to_proto(response_data, Struct)
                
                # Extract time parameters from time_range
                start_time = time_range.time_geq * 1000 if time_range.time_geq else None
                end_time = time_range.time_lt * 1000 if time_range.time_lt else None
                
                # Extract API URL and create metadata with SignOz URL
                api_url = self._extract_api_url_from_connector(signoz_connector)
                metadata = self._create_metadata_with_signoz_url(api_url, "dashboards", {
                    "dashboard_id": dashboard_id,
                    "start_time": start_time,
                    "end_time": end_time
                })
                
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.API_RESPONSE,
                    api_response=ApiResponseResult(response_body=response_struct),
                    source=self.source,
                    metadata=metadata,
                )
            else:
                # Extract time parameters from time_range
                start_time = time_range.time_geq * 1000 if time_range.time_geq else None
                end_time = time_range.time_lt * 1000 if time_range.time_lt else None
                
                # Extract API URL and create metadata with SignOz URL
                api_url = self._extract_api_url_from_connector(signoz_connector)
                metadata = self._create_metadata_with_signoz_url(api_url, "dashboards", {
                    "dashboard_id": dashboard_id,
                    "start_time": start_time,
                    "end_time": end_time
                })
                
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.API_RESPONSE,
                    text=TextResult(output=StringValue(value=f"Failed to fetch dashboard details for ID: {dashboard_id}")),
                    source=self.source,
                    metadata=metadata,
                )
        except Exception as e:
            logger.error(f"Error while executing Signoz fetch dashboard details task: {e}")
            # Extract time parameters from time_range for error case
            start_time = time_range.time_geq * 1000 if time_range.time_geq else None
            end_time = time_range.time_lt * 1000 if time_range.time_lt else None
            
            # Extract API URL and create metadata with SignOz URL
            api_url = self._extract_api_url_from_connector(signoz_connector)
            metadata = self._create_metadata_with_signoz_url(api_url, "dashboards", {
                "dashboard_id": dashboard_id,
                "start_time": start_time,
                "end_time": end_time
            })
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                text=TextResult(output=StringValue(value=f"Error executing dashboard details task: {e}")),
                source=self.source,
                metadata=metadata,
            )

    def execute_fetch_services(
        self,
        time_range: TimeRange,
        signoz_task: Signoz,
        signoz_connector: ConnectorProto,
    ) -> PlaybookTaskResult:
        """Executes fetch services task."""
        try:
            if not signoz_connector:
                raise Exception("Task execution Failed:: No Signoz source found")

            task = signoz_task.fetch_services
            start_time = task.start_time.value if task.HasField("start_time") else None
            end_time = task.end_time.value if task.HasField("end_time") else None
            duration = task.duration.value if task.HasField("duration") else None

            signoz_api_processor = self.get_connector_processor(signoz_connector)
            result = signoz_api_processor.fetch_services(start_time, end_time, duration)

            if result:
                # Handle different response formats from fetch_services
                if isinstance(result, list):
                    # If result is a list of services, wrap it in a dictionary
                    response_data = {"services": result}
                elif isinstance(result, dict):
                    # If result is already a dictionary (e.g., error response), use it as is
                    response_data = result
                else:
                    # Fallback: wrap in a generic structure
                    response_data = {"data": result}
                
                response_struct = dict_to_proto(response_data, Struct)
                
                # Extract API URL and create metadata with SignOz URL
                api_url = self._extract_api_url_from_connector(signoz_connector)
                metadata = self._create_metadata_with_signoz_url(api_url, "services", {
                    "start_time": start_time,
                    "end_time": end_time,
                    "duration": duration
                })
                
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.API_RESPONSE,
                    api_response=ApiResponseResult(response_body=response_struct),
                    source=self.source,
                    metadata=metadata,
                )
            else:
                # Extract API URL and create metadata with SignOz URL
                api_url = self._extract_api_url_from_connector(signoz_connector)
                metadata = self._create_metadata_with_signoz_url(api_url, "services", {
                    "start_time": start_time,
                    "end_time": end_time,
                    "duration": duration
                })
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.API_RESPONSE,
                    text=TextResult(output=StringValue(value="Failed to fetch services")),
                    source=self.source,
                    metadata=metadata,
                )
        except Exception as e:
            logger.error(f"Error while executing Signoz fetch services task: {e}")
            # Extract API URL and create metadata with SignOz URL for error case
            api_url = self._extract_api_url_from_connector(signoz_connector)
            metadata = self._create_metadata_with_signoz_url(api_url, "services", {
                "start_time": start_time,
                "end_time": end_time,
                "duration": duration
            })
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                text=TextResult(output=StringValue(value=f"Error executing services task: {e}")),
                source=self.source,
                metadata=metadata,
            )

    def execute_fetch_apm_metrics(
        self,
        time_range: TimeRange,
        signoz_task: Signoz,
        signoz_connector: ConnectorProto,
    ) -> PlaybookTaskResult:
        """Executes fetch APM metrics task."""
        try:
            if not signoz_connector:
                raise Exception("Task execution Failed:: No Signoz source found")

            task = signoz_task.fetch_apm_metrics
            service_name = task.service_name.value
            start_time = task.start_time.value if task.HasField("start_time") else None
            end_time = task.end_time.value if task.HasField("end_time") else None
            window = task.window.value if task.HasField("window") else "1m"
            operation_names = task.operation_names.value if task.HasField("operation_names") else None
            metrics = task.metrics.value if task.HasField("metrics") else None
            duration = task.duration.value if task.HasField("duration") else None
            
            if not service_name:
                # Extract API URL and create metadata with SignOz URL
                api_url = self._extract_api_url_from_connector(signoz_connector)
                metadata = self._create_metadata_with_signoz_url(api_url, "metrics", {
                    "start_time": start_time,
                    "end_time": end_time,
                    "window": window,
                    "duration": duration
                })
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.API_RESPONSE,
                    text=TextResult(output=StringValue(value="Service name must be provided.")),
                    source=self.source,
                    metadata=metadata,
                )

            # Parse JSON arrays if provided
            if operation_names:
                try:
                    operation_names = json.loads(operation_names)
                except json.JSONDecodeError:
                    logger.warning(f"Invalid operation_names JSON: {operation_names}")
                    operation_names = None

            if metrics:
                try:
                    metrics = json.loads(metrics)
                except json.JSONDecodeError:
                    logger.warning(f"Invalid metrics JSON: {metrics}")
                    metrics = None

            signoz_api_processor = self.get_connector_processor(signoz_connector)
            result = signoz_api_processor.fetch_apm_metrics(
                service_name, start_time, end_time, window, operation_names, metrics, duration
            )

            if result and "error" not in result:
                # Extract API URL and create metadata with SignOz URL
                api_url = self._extract_api_url_from_connector(signoz_connector)
                metadata = self._create_metadata_with_signoz_url(api_url, "metrics", {
                    "start_time": start_time,
                    "end_time": end_time,
                    "window": window,
                    "duration": duration
                })
                
                # Convert to multiple timeseries results (one per metric)
                task_results = []
                
                if isinstance(result, dict) and "data" not in result:
                    # New format: dictionary of metric results
                    for metric_name, metric_result in result.items():
                        if isinstance(metric_result, dict) and "data" in metric_result:
                            timeseries_result = self._convert_single_metric_to_timeseries(metric_result, service_name, metric_name)
                            if timeseries_result:
                                task_results.append(PlaybookTaskResult(
                                    type=PlaybookTaskResultType.TIMESERIES,
                                    timeseries=timeseries_result,
                                    source=self.source,
                                    metadata=metadata,
                                ))
                else:
                    # Old format: single result
                    timeseries_result = self._convert_apm_metrics_to_timeseries(result, service_name)
                    if timeseries_result:
                        task_results.append(PlaybookTaskResult(
                            type=PlaybookTaskResultType.TIMESERIES,
                            timeseries=timeseries_result,
                            source=self.source,
                            metadata=metadata,
                        ))
                
                if task_results:
                    return task_results
                else:
                    return [PlaybookTaskResult(
                        type=PlaybookTaskResultType.API_RESPONSE,
                        text=TextResult(output=StringValue(value="Failed to convert APM metrics to timeseries format")),
                        source=self.source,
                        metadata=metadata,
                    )]
            else:
                # Extract API URL and create metadata with SignOz URL
                api_url = self._extract_api_url_from_connector(signoz_connector)
                metadata = self._create_metadata_with_signoz_url(api_url, "metrics", {
                    "start_time": start_time,
                    "end_time": end_time,
                    "window": window,
                    "duration": duration
                })
                
                error_msg = result.get("error", "Failed to fetch APM metrics") if result else "Failed to fetch APM metrics"
                return [PlaybookTaskResult(
                    type=PlaybookTaskResultType.API_RESPONSE,
                    text=TextResult(output=StringValue(value=error_msg)),
                    source=self.source,
                    metadata=metadata,
                )]
        except Exception as e:
            logger.error(f"Error while executing Signoz fetch APM metrics task: {e}")
            # Extract API URL and create metadata with SignOz URL for error case
            api_url = self._extract_api_url_from_connector(signoz_connector)
            metadata = self._create_metadata_with_signoz_url(api_url, "metrics", {
                "service_name": service_name,
                "start_time": start_time,
                "end_time": end_time,
                "window": window,
                "duration": duration
            })
            return [PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                text=TextResult(output=StringValue(value=f"Error executing APM metrics task: {e}")),
                source=self.source,
                metadata=metadata,
            )]

    def execute_fetch_logs(
        self,
        time_range: TimeRange,
        signoz_task: Signoz,
        signoz_connector: ConnectorProto,
    ) -> PlaybookTaskResult:
        """Executes fetch logs task with filter expression."""
        try:
            if not signoz_connector:
                raise Exception("Task execution Failed:: No Signoz source found")

            task = signoz_task.fetch_logs
            filter_expression = task.filter_expression.value if task.HasField("filter_expression") else None
            start_time = task.start_time.value if task.HasField("start_time") else None
            end_time = task.end_time.value if task.HasField("end_time") else None
            duration = task.duration.value if task.HasField("duration") else None
            limit = task.limit.value if task.HasField("limit") else 100

            signoz_api_processor = self.get_connector_processor(signoz_connector)
            result = signoz_api_processor.fetch_logs_with_filter(
                filter_expression, start_time, end_time, duration, limit
            )

            if result:
                # Handle different response formats from fetch_logs_with_filter
                if isinstance(result, dict):
                    # If result is already a dictionary, use it as is
                    response_data = result
                else:
                    # Fallback: wrap in a generic structure
                    response_data = {"data": result}
                
                response_struct = dict_to_proto(response_data, Struct)
                
                # Extract API URL and create metadata with SignOz URL
                api_url = self._extract_api_url_from_connector(signoz_connector)
                metadata = self._create_metadata_with_signoz_url(api_url, "fetch_logs", {
                    "filter_expression": filter_expression,
                    "start_time": start_time,
                    "end_time": end_time,
                    "duration": duration,
                    "limit": limit
                })
                
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.API_RESPONSE,
                    api_response=ApiResponseResult(response_body=response_struct),
                    source=self.source,
                    metadata=metadata,
                )
            else:
                # Extract API URL and create metadata with SignOz URL
                api_url = self._extract_api_url_from_connector(signoz_connector)
                metadata = self._create_metadata_with_signoz_url(api_url, "fetch_logs", {
                    "filter_expression": filter_expression,
                    "start_time": start_time,
                    "end_time": end_time,
                    "duration": duration,
                    "limit": limit
                })
                
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.API_RESPONSE,
                    text=TextResult(output=StringValue(value="Failed to fetch logs")),
                    source=self.source,
                    metadata=metadata,
                )
        except Exception as e:
            logger.error(f"Error while executing Signoz fetch logs task: {e}")
            # Extract API URL and create metadata with SignOz URL for error case
            api_url = self._extract_api_url_from_connector(signoz_connector)
            metadata = self._create_metadata_with_signoz_url(api_url, "fetch_logs", {
                "filter_expression": filter_expression,
                "start_time": start_time,
                "end_time": end_time,
                "duration": duration,
                "limit": limit
            })
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                text=TextResult(output=StringValue(value=f"Error executing fetch logs task: {e}")),
                source=self.source,
                metadata=metadata,
            )

    def _convert_apm_metrics_to_timeseries(self, apm_metrics_result: dict, service_name: str) -> typing.Optional[TimeseriesResult]:
        """Converts APM metrics result to TimeseriesResult format."""
        try:
            if not apm_metrics_result:
                return None

            timeseries_result = TimeseriesResult(
                metric_name=StringValue(value=f"APM Metrics - {service_name}"),
                metric_expression=StringValue(value=f"APM metrics for service: {service_name}"),
            )

            # Handle new dictionary format with separate metrics
            if isinstance(apm_metrics_result, dict) and "data" not in apm_metrics_result:
                # New format: dictionary of metric results
                for metric_name, metric_result in apm_metrics_result.items():
                    if not isinstance(metric_result, dict) or "data" not in metric_result:
                        continue
                    
                    data = metric_result["data"]
                    if "result" in data and isinstance(data["result"], list):
                        for query_result in data["result"]:
                            if "series" in query_result:
                                for series in query_result["series"]:
                                    datapoints = []
                                    if "values" in series:
                                        for value in series["values"]:
                                            if "timestamp" in value and "value" in value:
                                                try:
                                                    timestamp_ms = int(value["timestamp"])
                                                    value_float = float(value["value"])
                                                    datapoints.append(
                                                        TimeseriesResult.LabeledMetricTimeseries.Datapoint(
                                                            timestamp=timestamp_ms,
                                                            value=DoubleValue(value=value_float),
                                                        )
                                                    )
                                                except (ValueError, TypeError):
                                                    continue

                                    if datapoints:
                                        metric_label_values = []
                                        if "labels" in series:
                                            for label_key, label_value in series["labels"].items():
                                                metric_label_values.append(
                                                    LabelValuePair(
                                                        name=StringValue(value=label_key),
                                                        value=StringValue(value=str(label_value)),
                                                    )
                                                )

                                        # Add service name, metric name, and query info as labels
                                        metric_label_values.append(
                                            LabelValuePair(
                                                name=StringValue(value="service_name"),
                                                value=StringValue(value=service_name),
                                            )
                                        )
                                        metric_label_values.append(
                                            LabelValuePair(
                                                name=StringValue(value="metric_name"),
                                                value=StringValue(value=metric_name),
                                            )
                                        )
                                        metric_label_values.append(
                                            LabelValuePair(
                                                name=StringValue(value="query_name"),
                                                value=StringValue(value=query_result.get("queryName", "unknown")),
                                            )
                                        )

                                        timeseries_result.labeled_metric_timeseries.append(
                                            TimeseriesResult.LabeledMetricTimeseries(
                                                metric_label_values=metric_label_values,
                                                unit=StringValue(value=""),
                                                datapoints=datapoints,
                                            )
                                        )
            else:
                # Old format: single result with data
                if "data" not in apm_metrics_result:
                    return None

                data = apm_metrics_result["data"]
                if "result" in data and isinstance(data["result"], list):
                    for query_result in data["result"]:
                        if "series" in query_result:
                            for series in query_result["series"]:
                                datapoints = []
                                if "values" in series:
                                    for value in series["values"]:
                                        if "timestamp" in value and "value" in value:
                                            try:
                                                timestamp_ms = int(value["timestamp"])
                                                value_float = float(value["value"])
                                                datapoints.append(
                                                    TimeseriesResult.LabeledMetricTimeseries.Datapoint(
                                                        timestamp=timestamp_ms,
                                                        value=DoubleValue(value=value_float),
                                                    )
                                                )
                                            except (ValueError, TypeError):
                                                continue

                                if datapoints:
                                    metric_label_values = []
                                    if "labels" in series:
                                        for label_key, label_value in series["labels"].items():
                                            metric_label_values.append(
                                                LabelValuePair(
                                                    name=StringValue(value=label_key),
                                                    value=StringValue(value=str(label_value)),
                                                )
                                            )

                                    # Add service name and query info as labels
                                    metric_label_values.append(
                                        LabelValuePair(
                                            name=StringValue(value="service_name"),
                                            value=StringValue(value=service_name),
                                        )
                                    )
                                    metric_label_values.append(
                                        LabelValuePair(
                                            name=StringValue(value="query_name"),
                                            value=StringValue(value=query_result.get("queryName", "unknown")),
                                        )
                                    )

                                    timeseries_result.labeled_metric_timeseries.append(
                                        TimeseriesResult.LabeledMetricTimeseries(
                                            metric_label_values=metric_label_values,
                                            unit=StringValue(value=""),
                                            datapoints=datapoints,
                                        )
                                    )

            return timeseries_result if timeseries_result.labeled_metric_timeseries else None
        except Exception as e:
            logger.error(f"Error converting APM metrics to timeseries: {e}", exc_info=True)
            return None

    def _convert_single_metric_to_timeseries(self, metric_result: dict, service_name: str, metric_name: str) -> typing.Optional[TimeseriesResult]:
        """Converts a single metric result to TimeseriesResult format."""
        try:
            if not metric_result or "data" not in metric_result:
                return None

            # Create proper metric display names
            metric_display_names = {
                "request_rate": "Request Rate (ops/s)",
                "error_rate": "Error Rate (%)",
                "apdex": "Apdex Score",
                "latency": "Latency (ms)"
            }
            
            display_name = metric_display_names.get(metric_name, metric_name.replace('_', ' ').title())
            
            timeseries_result = TimeseriesResult(
                metric_name=StringValue(value=f"{display_name} - {service_name}"),
                metric_expression=StringValue(value=f"{display_name} for service: {service_name}"),
            )

            data = metric_result["data"]
            if "result" in data and isinstance(data["result"], list):
                for query_result in data["result"]:
                    if "series" in query_result:
                        for series in query_result["series"]:
                            datapoints = []
                            if "values" in series:
                                for value in series["values"]:
                                    if "timestamp" in value and "value" in value:
                                        try:
                                            timestamp_ms = int(value["timestamp"])
                                            value_float = float(value["value"])
                                            datapoints.append(
                                                TimeseriesResult.LabeledMetricTimeseries.Datapoint(
                                                    timestamp=timestamp_ms,
                                                    value=DoubleValue(value=value_float),
                                                )
                                            )
                                        except (ValueError, TypeError):
                                            continue

                            if datapoints:
                                metric_label_values = []
                                if "labels" in series:
                                    for label_key, label_value in series["labels"].items():
                                        metric_label_values.append(
                                            LabelValuePair(
                                                name=StringValue(value=label_key),
                                                value=StringValue(value=str(label_value)),
                                            )
                                        )

                                # Add service name and meaningful metric info as labels
                                metric_label_values.append(
                                    LabelValuePair(
                                        name=StringValue(value="service_name"),
                                        value=StringValue(value=service_name),
                                    )
                                )
                                
                                # Create meaningful series labels
                                query_name = query_result.get("queryName", "unknown")
                                if metric_name == "latency":
                                    # Map query names to percentile names
                                    percentile_names = {"A": "P50", "B": "P90", "C": "P99"}
                                    series_label = percentile_names.get(query_name, query_name)
                                elif metric_name == "error_rate" and query_name == "F1":
                                    # For error_rate F1 expression, use the expression legend
                                    series_label = "Error Percentage"
                                elif metric_name == "apdex" and query_name == "F1":
                                    # For apdex F1 expression, use the expression legend
                                    series_label = "Apdex"
                                else:
                                    # For other metrics, use the metric name
                                    series_label = display_name
                                
                                metric_label_values.append(
                                    LabelValuePair(
                                        name=StringValue(value="metric_type"),
                                        value=StringValue(value=series_label),
                                    )
                                )

                                timeseries_result.labeled_metric_timeseries.append(
                                    TimeseriesResult.LabeledMetricTimeseries(
                                        metric_label_values=metric_label_values,
                                        unit=StringValue(value=""),
                                        datapoints=datapoints,
                                    )
                                )

            return timeseries_result if timeseries_result.labeled_metric_timeseries else None
        except Exception as e:
            logger.error(f"Error converting single metric to timeseries: {e}", exc_info=True)
            return None

    def execute_fetch_logs_for_trace(
        self,
        time_range: TimeRange,
        signoz_task: Signoz,
        signoz_connector: ConnectorProto,
    ) -> PlaybookTaskResult:
        """Executes fetch logs for trace task."""
        try:
            if not signoz_connector:
                raise Exception("Task execution Failed:: No Signoz source found")

            task = signoz_task.fetch_logs_for_trace
            trace_id = task.trace_id.value if task.HasField("trace_id") else None
            limit = task.limit.value if task.HasField("limit") else 10

            if not trace_id:
                raise Exception("Trace ID is required for fetch logs for trace task")

            signoz_api_processor = self.get_connector_processor(signoz_connector)
            result = signoz_api_processor.fetch_logs_for_trace_id(trace_id, limit)

            if result:
                # Handle different response formats from fetch_logs_for_trace_id
                if isinstance(result, dict):
                    # If result is already a dictionary, use it as is
                    response_data = result
                else:
                    # Fallback: wrap in a generic structure
                    response_data = {"data": result}
                
                response_struct = dict_to_proto(response_data, Struct)
                
                # Extract API URL and create metadata with SignOz URL
                api_url = self._extract_api_url_from_connector(signoz_connector)
                metadata = self._create_metadata_with_signoz_url(api_url, "fetch_logs_for_trace", {
                    "trace_id": trace_id,
                    "limit": limit
                })
                
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.API_RESPONSE,
                    api_response=ApiResponseResult(response_body=response_struct),
                    source=self.source,
                    metadata=metadata,
                )
            else:
                # Extract API URL and create metadata with SignOz URL
                api_url = self._extract_api_url_from_connector(signoz_connector)
                metadata = self._create_metadata_with_signoz_url(api_url, "fetch_logs_for_trace", {
                    "trace_id": trace_id,
                    "limit": limit
                })
                
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.API_RESPONSE,
                    text=TextResult(output=StringValue(value="Failed to fetch logs for trace")),
                    source=self.source,
                    metadata=metadata,
                )
        except Exception as e:
            logger.error(f"Error while executing Signoz fetch logs for trace task: {e}")
            # Extract API URL and create metadata with SignOz URL for error case
            api_url = self._extract_api_url_from_connector(signoz_connector)
            metadata = self._create_metadata_with_signoz_url(api_url, "fetch_logs_for_trace", {
                "trace_id": trace_id,
                "limit": limit
            })
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                text=TextResult(output=StringValue(value=f"Error executing fetch logs for trace task: {e}")),
                source=self.source,
                metadata=metadata,
            )

    def execute_fetch_traces(
        self,
        time_range: TimeRange,
        signoz_task: Signoz,
        signoz_connector: ConnectorProto,
    ) -> PlaybookTaskResult:
        """Executes fetch traces task with multiple filters."""
        try:
            if not signoz_connector:
                raise Exception("Task execution Failed:: No Signoz source found")

            task = signoz_task.fetch_traces
            filter_expression = task.filter_expression.value if task.HasField("filter_expression") else None
            start_time = task.start_time.value if task.HasField("start_time") else None
            end_time = task.end_time.value if task.HasField("end_time") else None
            duration = task.duration.value if task.HasField("duration") else None
            limit = task.limit.value if task.HasField("limit") else 100

            signoz_api_processor = self.get_connector_processor(signoz_connector)
            result = signoz_api_processor.fetch_traces_with_filters(
                filter_expression, start_time, end_time, duration, limit
            )

            if result:
                # Handle different response formats from fetch_traces_with_filters
                if isinstance(result, dict):
                    # If result is already a dictionary, use it as is
                    response_data = result
                else:
                    # Fallback: wrap in a generic structure
                    response_data = {"data": result}
                
                response_struct = dict_to_proto(response_data, Struct)
                
                # Extract API URL and create metadata with SignOz URL
                api_url = self._extract_api_url_from_connector(signoz_connector)
                metadata = self._create_metadata_with_signoz_url(api_url, "fetch_traces", {
                    "start_time": start_time,
                    "end_time": end_time,
                    "duration": duration,
                    "filter_expression": filter_expression,
                    "limit": limit
                })
                
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.API_RESPONSE,
                    api_response=ApiResponseResult(response_body=response_struct),
                    source=self.source,
                    metadata=metadata,
                )
            else:
                # Extract API URL and create metadata with SignOz URL
                api_url = self._extract_api_url_from_connector(signoz_connector)
                metadata = self._create_metadata_with_signoz_url(api_url, "fetch_traces", {
                    "start_time": start_time,
                    "end_time": end_time,
                    "duration": duration,
                    "filter_expression": filter_expression,
                    "limit": limit
                })
                
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.API_RESPONSE,
                    text=TextResult(output=StringValue(value="Failed to fetch traces")),
                    source=self.source,
                    metadata=metadata,
                )
        except Exception as e:
            logger.error(f"Error while executing Signoz fetch traces task: {e}")
            # Extract API URL and create metadata with SignOz URL for error case
            api_url = self._extract_api_url_from_connector(signoz_connector)
            metadata = self._create_metadata_with_signoz_url(api_url, "fetch_traces", {
                "filter_expression": filter_expression,
                "start_time": start_time,
                "end_time": end_time,
                "duration": duration,
                "limit": limit
            })
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                text=TextResult(output=StringValue(value=f"Error executing fetch traces task: {e}")),
                source=self.source,
                metadata=metadata,
            )

    def analyze_trace_correlation(self, trace_data, spans_data, logs_data):
        """
        Analyze correlation between traces, spans, and logs
        
        Args:
            trace_data: Trace information
            spans_data: Spans data for the trace
            logs_data: Logs data for the trace
            
        Returns:
            Analysis insights and correlations
        """
        try:
            analysis = {
                "trace_summary": {
                    "trace_id": trace_data.get("trace_id"),
                    "total_spans": len(spans_data),
                    "total_logs": len(logs_data),
                    "duration_ms": 0,
                    "error_count": 0,
                    "service_count": 0
                },
                "span_analysis": {},
                "log_analysis": {},
                "correlations": []
            }

            # Analyze spans
            services = set()
            total_duration = 0
            error_spans = 0
            
            for span in spans_data:
                service_name = span.get("service_name")
                if service_name:
                    services.add(service_name)
                
                duration = span.get("duration_ns", 0)
                if duration:
                    total_duration = max(total_duration, duration)
                
                if span.get("has_error", False):
                    error_spans += 1
                
                # Analyze span logs
                span_logs = span.get("logs", [])
                if span_logs:
                    error_logs = [log for log in span_logs if log.get("level", "").upper() in ["ERROR", "FATAL"]]
                    analysis["span_analysis"][span.get("span_id")] = {
                        "log_count": len(span_logs),
                        "error_log_count": len(error_logs),
                        "has_errors": len(error_logs) > 0
                    }

            analysis["trace_summary"]["duration_ms"] = total_duration / 1_000_000  # Convert to milliseconds
            analysis["trace_summary"]["error_count"] = error_spans
            analysis["trace_summary"]["service_count"] = len(services)

            # Analyze logs
            log_levels = {}
            for log in logs_data:
                level = log.get("level", "INFO")
                log_levels[level] = log_levels.get(level, 0) + 1

            analysis["log_analysis"] = {
                "total_logs": len(logs_data),
                "log_levels": log_levels,
                "error_logs": log_levels.get("ERROR", 0) + log_levels.get("FATAL", 0)
            }

            # Find correlations
            for span in spans_data:
                span_id = span.get("span_id")
                span_logs = span.get("logs", [])
                
                if span_logs:
                    # Check for error correlation
                    has_span_error = span.get("has_error", False)
                    has_log_errors = any(log.get("level", "").upper() in ["ERROR", "FATAL"] for log in span_logs)
                    
                    if has_span_error and has_log_errors:
                        analysis["correlations"].append({
                            "type": "error_correlation",
                            "span_id": span_id,
                            "service": span.get("service_name"),
                            "operation": span.get("operation_name"),
                            "description": "Span error correlates with error logs"
                        })

            return analysis

        except Exception as e:
            logger.error(f"Error analyzing trace correlation: {e}")
            return {
                "error": f"Failed to analyze trace correlation: {str(e)}",
                "trace_summary": {},
                "span_analysis": {},
                "log_analysis": {},
                "correlations": []
            }
