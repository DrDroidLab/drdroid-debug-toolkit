import json
import logging
import re
from datetime import datetime, timedelta, timezone

import requests
from dateutil import parser as dateparser
from typing import Optional

from core.integrations.processor import Processor
from core.protos.base_pb2 import TimeRange
from core.settings import EXTERNAL_CALL_TIMEOUT

logger = logging.getLogger(__name__)


class SignozDashboardQueryBuilder:
    def __init__(self, global_step, variables):
        self.global_step = global_step
        self.variables = variables
        self.query_letter_ord = ord("A")

    def _get_next_query_letter(self):
        letter = chr(self.query_letter_ord)
        self.query_letter_ord += 1
        if self.query_letter_ord > ord("Z"):
            self.query_letter_ord = ord("A")
        return letter

    def build_query_dict(self, query_data):
        query_dict = dict(query_data)
        current_letter = self._get_next_query_letter()
        query_dict.pop("step_interval", None)
        query_dict["stepInterval"] = self.global_step
        if "group_by" in query_dict:
            query_dict["groupBy"] = query_dict.pop("group_by")
        query_dict["queryName"] = current_letter
        query_dict["expression"] = current_letter
        query_dict["disabled"] = query_dict.get("disabled", False)
        # Add pageSize for metrics queries
        if query_dict.get("dataSource") == "metrics":
            query_dict["pageSize"] = 10
        return current_letter, query_dict

    def build_panel_payload(self, panel_type, panel_queries, start_time, end_time, panel_data=None):
        # Ensure timestamps are in milliseconds
        def to_ms(ts):
            return int(ts * 1000) if ts < 1e12 else int(ts)

        payload = {
            "start": to_ms(start_time),
            "end": to_ms(end_time),
            "step": self.global_step,
            "variables": self.variables,
            "formatForWeb": False,
            "compositeQuery": {
                "queryType": "builder",
                "panelType": panel_type,
                "fillGaps": False,
                "builderQueries": panel_queries,
            },
        }
        
        # Add selectedTracesFields for list panels (traces) - try different approach
        if panel_type == "list" and panel_data:
            selected_traces_fields = panel_data.get("selectedTracesFields", [])
            
            # Try adding to compositeQuery only first
            if selected_traces_fields and len(selected_traces_fields) > 0:
                payload["compositeQuery"]["selectedTracesFields"] = selected_traces_fields
            else:
                # Provide default selectedTracesFields if none found
                default_traces_fields = [
                    {"dataType": "string", "id": "serviceName--string--tag--true", "isColumn": True, "isJSON": False, "key": "serviceName", "type": "tag"},
                    {"dataType": "string", "id": "name--string--tag--true", "isColumn": True, "isJSON": False, "key": "name", "type": "tag"},
                    {"dataType": "float64", "id": "durationNano--float64--tag--true", "isColumn": True, "isJSON": False, "key": "durationNano", "type": "tag"},
                    {"dataType": "string", "id": "httpMethod--string--tag--true", "isColumn": True, "isJSON": False, "key": "httpMethod", "type": "tag"},
                    {"dataType": "string", "id": "responseStatusCode--string--tag--true", "isColumn": True, "isJSON": False, "key": "responseStatusCode", "type": "tag"}
                ]
                payload["compositeQuery"]["selectedTracesFields"] = default_traces_fields
        
        # Add selectedLogFields for table panels (logs) - try compositeQuery only
        if panel_type == "table" and panel_data:
            selected_log_fields = panel_data.get("selectedLogFields", [])
            if selected_log_fields and len(selected_log_fields) > 0:
                payload["compositeQuery"]["selectedLogFields"] = selected_log_fields
        
        return json.loads(json.dumps(payload, ensure_ascii=False, indent=None))

# Hardcoded builder query templates for standard APM metrics (matching SigNoz frontend)
APM_METRIC_QUERIES = {
    "request_rate": {
        "dataSource": "metrics",
        "aggregateOperator": "sum_rate",
        "aggregateAttribute": {"key": "signoz_latency_count", "dataType": "float64", "isColumn": True, "type": ""},
        "timeAggregation": "rate",
        "spaceAggregation": "sum",
        "functions": [],
        "filters": None,  # Fill dynamically
        "expression": "A",
        "disabled": False,
        "stepInterval": None,  # Fill dynamically
        "having": [],
        "limit": None,
        "orderBy": [],
        "groupBy": [],
        "legend": "Operations",
        "reduceTo": "avg",
    },
    "error_rate": {
        "A": {
            "dataSource": "metrics",
            "queryName": "A",
            "aggregateOperator": "count",
            "aggregateAttribute": {"key": "signoz_calls_total", "dataType": "float64", "isColumn": True, "type": ""},
            "timeAggregation": "rate",
            "spaceAggregation": "sum",
            "functions": [],
            "filters": None,  # Fill dynamically with error status filter
            "expression": "A",
            "disabled": True,
            "stepInterval": None,  # Fill dynamically
            "having": [],
            "limit": None,
            "orderBy": [],
            "groupBy": [],
            "legend": "Error Count",
            "reduceTo": "avg",
            "temporality": "Delta",
        },
        "B": {
            "dataSource": "metrics",
            "queryName": "B",
            "aggregateOperator": "count",
            "aggregateAttribute": {"key": "signoz_calls_total", "dataType": "float64", "isColumn": True, "type": ""},
            "timeAggregation": "rate",
            "spaceAggregation": "sum",
            "functions": [],
            "filters": None,  # Fill dynamically with all calls filter
            "expression": "B",
            "disabled": True,
            "stepInterval": None,  # Fill dynamically
            "having": [],
            "limit": None,
            "orderBy": [],
            "groupBy": [],
            "legend": "Total Count",
            "reduceTo": "avg",
            "temporality": "Delta",
        },
        "F1": {
            "queryName": "F1",
            "expression": "A*100/B",
            "disabled": False,
            "legend": "Error Percentage",
        }
    },
    "apdex": {
        "A": {
            "dataSource": "metrics",
            "queryName": "A",
            "aggregateOperator": "count",
            "aggregateAttribute": {"key": "signoz_latency_count", "dataType": "float64", "isColumn": True, "type": ""},
            "timeAggregation": "rate",
            "spaceAggregation": "sum",
            "functions": [],
            "filters": None,  # Fill dynamically
            "expression": "A",
            "disabled": True,
            "stepInterval": None,  # Fill dynamically
            "having": [],
            "limit": None,
            "orderBy": [],
            "groupBy": [],
            "legend": "Total Requests",
            "reduceTo": "avg",
            "temporality": "Delta",
        },
        "B": {
            "dataSource": "metrics",
            "queryName": "B",
            "aggregateOperator": "count",
            "aggregateAttribute": {"key": "signoz_latency_bucket", "dataType": "float64", "isColumn": True, "type": ""},
            "timeAggregation": "rate",
            "spaceAggregation": "sum",
            "functions": [],
            "filters": None,  # Fill dynamically with satisfied requests filter
            "expression": "B",
            "disabled": True,
            "stepInterval": None,  # Fill dynamically
            "having": [],
            "limit": None,
            "orderBy": [],
            "groupBy": [],
            "legend": "Satisfied Requests",
            "reduceTo": "avg",
            "temporality": "Delta",
        },
        "C": {
            "dataSource": "metrics",
            "queryName": "C",
            "aggregateOperator": "count",
            "aggregateAttribute": {"key": "signoz_latency_bucket", "dataType": "float64", "isColumn": True, "type": ""},
            "timeAggregation": "rate",
            "spaceAggregation": "sum",
            "functions": [],
            "filters": None,  # Fill dynamically with tolerating requests filter
            "expression": "C",
            "disabled": True,
            "stepInterval": None,  # Fill dynamically
            "having": [],
            "limit": None,
            "orderBy": [],
            "groupBy": [],
            "legend": "Tolerating Requests",
            "reduceTo": "avg",
            "temporality": "Delta",
        },
        "F1": {
            "queryName": "F1",
            "expression": "((B + C)/2)/A",
            "disabled": False,
            "legend": "Apdex",
        }
    },
    # Latency metrics use traces data source for percentiles
    "latency": {
        "p50": {
            "dataSource": "traces",
            "aggregateOperator": "p50",
            "aggregateAttribute": {"key": "durationNano", "dataType": "float64", "isColumn": True, "type": "tag"},
            "timeAggregation": "",
            "spaceAggregation": "p50",
            "functions": [],
            "filters": None,  # Fill dynamically
            "expression": "A",
            "disabled": False,
            "stepInterval": None,  # Fill dynamically
            "having": [],
            "limit": None,
            "orderBy": [],
            "groupBy": [],
            "legend": "p50",
            "reduceTo": "avg",
        },
        "p90": {
            "dataSource": "traces",
            "aggregateOperator": "p90",
            "aggregateAttribute": {"key": "durationNano", "dataType": "float64", "isColumn": True, "type": "tag"},
            "timeAggregation": "",
            "spaceAggregation": "p90",
            "functions": [],
            "filters": None,  # Fill dynamically
            "expression": "B",
            "disabled": False,
            "stepInterval": None,  # Fill dynamically
            "having": [],
            "limit": None,
            "orderBy": [],
            "groupBy": [],
            "legend": "p90",
            "reduceTo": "avg",
        },
        "p99": {
            "dataSource": "traces",
            "aggregateOperator": "p99",
            "aggregateAttribute": {"key": "durationNano", "dataType": "float64", "isColumn": True, "type": "tag"},
            "timeAggregation": "",
            "spaceAggregation": "p99",
            "functions": [],
            "filters": None,  # Fill dynamically
            "expression": "C",
            "disabled": False,
            "stepInterval": None,  # Fill dynamically
            "having": [],
            "limit": None,
            "orderBy": [],
            "groupBy": [],
            "legend": "p99",
            "reduceTo": "avg",
        },
    },
}


class SignozApiProcessor(Processor):
    def __init__(self, signoz_api_url, signoz_api_token=None):
        self.signoz_api_url = signoz_api_url.rstrip('/') if signoz_api_url else ''
        
        if not self.signoz_api_url:
            raise ValueError("SignozApiProcessor: API URL cannot be empty")
            
        # Set default headers
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
        # Add authentication if provided
        if signoz_api_token:
            # Use SIGNOZ-API-KEY header for authentication instead of Authorization
            self.headers['SIGNOZ-API-KEY'] = signoz_api_token
            
            logger.debug(f"SignozApiProcessor initialized with URL: {self.signoz_api_url} and API key")
        else:
            logger.warning("SignozApiProcessor initialized without API key")
    
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
    
    def _clean_query_dict(self, obj):
        """
        Recursively clean query dictionaries by removing problematic fields
        that might cause SigNoz API validation errors.
        """
        if isinstance(obj, dict):
            # Fields that should be removed from all levels
            # Do NOT remove 'id' or 'isJSON' as SigNoz expects these on nested keys
            fields_to_remove = ['step_interval']
            cleaned = {}
            
            for key, value in obj.items():
                if key not in fields_to_remove:
                    cleaned[key] = self._clean_query_dict(value)
            
            return cleaned
        elif isinstance(obj, list):
            return [self._clean_query_dict(item) for item in obj]
        else:
            return obj
    
    def test_connection(self):
        """Test the connection to Signoz API"""
        try:
            url = f"{self.signoz_api_url}/api/v1/health"
            response = requests.get(url, headers=self.headers, timeout=20)
            logger.debug(f"Response: {response.text}")
            logger.info(f"Response: {response.text}")
            if response and response.status_code == 200:
                return True
            else:
                status_code = response.status_code if response else None
                raise Exception(f"Failed to connect with Signoz. Status Code: {status_code}. Response Text: {response.text}")
        except Exception as e:
            logger.error(f"Exception occurred while fetching signoz health with error: {e}")
            raise e
    
    def fetch_dashboards(self):
        """Fetch all dashboards from Signoz"""
        try:
            response = requests.get(
                f"{self.signoz_api_url}/api/v1/dashboards", 
                headers=self.headers,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Exception when fetching dashboards: {e}")
            raise e
    
    def fetch_dashboard_details(self, dashboard_id):
        """Fetch details of a specific dashboard"""
        try:
            logger.debug(f"Fetching dashboard details for {dashboard_id}")
            response = requests.get(
                f"{self.signoz_api_url}/api/v1/dashboards/{dashboard_id}", 
                headers=self.headers,
                timeout=30
            )
            
            response.raise_for_status()
            return response.json()["data"]
        except Exception as e:
            logger.error(f"Exception when fetching dashboard details: {e}")
            raise e
    
    def fetch_alerts(self):
        """Fetch all alerts from Signoz"""
        try:
            # Make a debug log of the headers and URL being used
            logger.debug(f"Fetching alerts from: {self.signoz_api_url}/api/v1/alerts")
            logger.debug(f"Using headers: {self.headers}")
            
            response = requests.get(
                f"{self.signoz_api_url}/api/v1/alerts", 
                headers=self.headers,
                timeout=30
            )
            
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Exception when fetching alerts: {e}")
            raise e
    
    def fetch_alert_details(self, alert_id):
        """Fetch details of a specific alert"""
        try:
            response = requests.get(
                f"{self.signoz_api_url}/api/v1/alerts/{alert_id}", 
                headers=self.headers,
                timeout=30
            )
            
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Exception when fetching alert details: {e}")
            raise e
    
    def query_metrics(self, time_range: TimeRange, query, step=None, aggregation=None):
        """Query metrics from Signoz ClickhouseDB"""
        try:
            from_time = int(time_range.time_geq * 1000)
            to_time = int(time_range.time_lt * 1000)
            
            payload = {
                "query": query,
                "start": from_time,
                "end": to_time
            }
            
            if step:
                payload["step"] = step
                
            if aggregation:
                payload["aggregation"] = aggregation
            
            response = requests.post(
                f"{self.signoz_api_url}/api/v1/metrics/query",
                headers=self.headers,
                json=payload,
                timeout=30
            )
            
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Exception when querying metrics: {e}")
            raise e
    
    def query_dashboard_panel(self, time_range: TimeRange, dashboard_id, panel_id):
        """Query metrics for a specific dashboard panel"""
        try:
            # First get the dashboard details to extract the panel query
            dashboard = self.fetch_dashboard_details(dashboard_id)
            if not dashboard:
                logger.error(f"Failed to fetch dashboard {dashboard_id}")
                return None
            
            panel = None
            for p in dashboard.get("panels", []):
                if p.get("id") == panel_id:
                    panel = p
                    break
            
            if not panel:
                logger.error(f"Panel {panel_id} not found in dashboard {dashboard_id}")
                return None
            
            # Extract query from panel
            queries = panel.get("queries", [])
            if not queries:
                logger.error(f"No queries found in panel {panel_id}")
                return None
            
            # Execute each query
            results = []
            for query in queries:
                query_result = self.query_metrics(
                    time_range,
                    query.get("query"),
                    step=panel.get("step"),
                    aggregation=panel.get("aggregation")
                )
                if query_result:
                    results.append(query_result)
            
            return {
                "panel": panel,
                "results": results
            }
        except Exception as e:
            logger.error(f"Exception when querying dashboard panel: {e}")
            raise e

    def execute_signoz_query(self, query_payload):
        """Execute a Clickhouse SQL query using the Signoz query range API"""
        try:
            logger.debug(f"Executing Clickhouse query with payload: {query_payload}")
            
            response = requests.post(
                f"{self.signoz_api_url}/api/v4/query_range",
                headers=self.headers,
                json=query_payload,
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to execute Clickhouse query: {response.status_code} - {response.text}")
                raise Exception(f"Failed to execute Clickhouse query: {response.status_code} - {response.text}")
        except Exception as e:
            logger.error(f"Exception when executing Clickhouse query: {e}")
            raise e 
    
    def _get_time_range(self, start_time=None, end_time=None, duration=None, default_hours=3):
        """
        Returns (start_dt, end_dt) as UTC datetimes.
        - If start_time and end_time are provided, use those.
        - Else if duration is provided, use (now - duration, now).
        - Else, use (now - default_hours, now).
        """
        now_dt = datetime.now(timezone.utc)
        if start_time and end_time:
            start_dt = self._parse_time(start_time)
            end_dt = self._parse_time(end_time)
            if not start_dt or not end_dt:
                start_dt = now_dt - timedelta(hours=default_hours)
                end_dt = now_dt
        elif duration:
            dur_ms = self._parse_duration(duration)
            if dur_ms is None:
                dur_ms = default_hours * 60 * 60 * 1000
            start_dt = now_dt - timedelta(milliseconds=dur_ms)
            end_dt = now_dt
        else:
            start_dt = now_dt - timedelta(hours=default_hours)
            end_dt = now_dt
        return start_dt, end_dt

    def fetch_services(self, start_time=None, end_time=None, duration=None):
        """
        Fetches all instrumented services from SigNoz.
        Accepts start_time and end_time as RFC3339 or relative strings (e.g., 'now-2h', 'now-30m'), or a duration string (e.g., '2h', '90m').
        If duration is provided, uses that as the window ending at now.
        If start_time and end_time are provided, uses those. Defaults to last 24 hours.
        Returns a list of services or error details.
        """
        # Use standardized time range logic
        start_dt, end_dt = self._get_time_range(start_time, end_time, duration, default_hours=24)
        start_ns = int(start_dt.timestamp() * 1_000_000_000)
        end_ns = int(end_dt.timestamp() * 1_000_000_000)

        try:
            url = f"{self.signoz_api_url}/api/v1/services"
            payload = {"start": str(start_ns), "end": str(end_ns), "tags": []}
            response = requests.post(url, headers=self.headers, json=payload, timeout=120)
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to fetch services: {response.status_code} - {response.text}")
                return {"status": "error", "message": f"Failed to fetch services: {response.status_code}", "details": response.text}
        except Exception as e:
            logger.error(f"Exception when fetching services: {e}")
            return {"status": "error", "message": str(e)}

    def _parse_step(self, step):
        """Parse step interval from string like '5m', '1h', or integer seconds."""
        if isinstance(step, int):
            return step
        if isinstance(step, str):
            match = re.match(r"^(\d+)([smhd])$", step)
            if match:
                value, unit = match.groups()
                value = int(value)
                if unit == "s":
                    return value
                elif unit == "m":
                    return value * 60
                elif unit == "h":
                    return value * 3600
                elif unit == "d":
                    return value * 86400
            else:
                try:
                    return int(step)
                except Exception:
                    logger.error(f"Failed to parse step: {step}")
                    pass
        return 60  # default

    def _parse_duration(self, duration_str):
        """Parse duration string like '2h', '90m' into milliseconds."""
        if not duration_str or not isinstance(duration_str, str):
            return None
        match = re.match(r"^(\d+)([hm])$", duration_str.strip().lower())
        if match:
            value, unit = match.groups()
            value = int(value)
            if unit == "h":
                return value * 60 * 60 * 1000
            elif unit == "m":
                return value * 60 * 1000
        try:
            # fallback: try to parse as integer minutes
            value = int(duration_str)
            return value * 60 * 1000
        except Exception as e:
            logger.error(f"_parse_duration: Exception parsing '{duration_str}': {e}")
        return None

    def _parse_time(self, time_str):
        """
        Parse a time string in RFC3339, 'now', or 'now-2h', 'now-30m', etc. Returns a UTC datetime.
        Logs errors if parsing fails.
        """
        if not time_str or not isinstance(time_str, str):
            logger.error(f"_parse_time: Invalid input (not a string): {time_str}")
            return None
        time_str_orig = time_str
        time_str = time_str.strip().lower()
        if time_str.startswith("now"):
            if "-" in time_str:
                match = re.match(r"now-(\d+)([smhd])", time_str)
                if match:
                    value, unit = match.groups()
                    value = int(value)
                    if unit == "s":
                        delta = timedelta(seconds=value)
                    elif unit == "m":
                        delta = timedelta(minutes=value)
                    elif unit == "h":
                        delta = timedelta(hours=value)
                    elif unit == "d":
                        delta = timedelta(days=value)
                    else:
                        delta = timedelta()
                    logger.debug(f"_parse_time: Parsed relative time '{time_str_orig}' as now - {value}{unit}")
                    return datetime.now(timezone.utc) - delta
            logger.debug(f"_parse_time: Parsed 'now' as current UTC time for input '{time_str_orig}'")
            return datetime.now(timezone.utc)
        else:
            try:
                dt = dateparser.parse(time_str_orig)
                if dt is None:
                    logger.error(f"_parse_time: dateparser.parse returned None for input '{time_str_orig}'")
                    return None
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                logger.debug(f"_parse_time: Successfully parsed '{time_str_orig}' as {dt.isoformat()}")
                return dt.astimezone(timezone.utc)
            except Exception as e:
                logger.error(f"_parse_time: Exception parsing '{time_str_orig}': {e}")
                return None

    def _post_query_range(self, payload):
        """
        Helper method to POST to /api/v4/query_range and handle response.
        """
        url = f"{self.signoz_api_url}/api/v4/query_range"
        logger.debug(f"Querying: {payload}")
        logger.debug(f"URL: {url}")
        try:
            response = requests.post(url, headers=self.headers, json=payload, timeout=120)
            if response.status_code == 200:
                try:
                    resp_json = response.json()
                    logger.debug("response json:::", resp_json)
                    return resp_json
                except Exception as e:
                    logger.error(f"Failed to parse JSON: {e}, response text: {response.text}")
                    return {"error": f"Failed to parse JSON: {e}", "raw_response": response.text}
            else:
                logger.error(f"Failed to query metrics: {response.status_code} - {response.text}")
                return {"error": f"HTTP {response.status_code}", "raw_response": response.text}
        except Exception as e:
            logger.error(f"Exception when posting to query_range: {e}")
            raise e

    def _post_query_range_v5(self, payload):
        """
        Helper method to POST to /api/v5/query_range and handle response.
        """
        url = f"{self.signoz_api_url}/api/v5/query_range"
        logger.debug(f"Querying v5: {payload}")
        logger.debug(f"URL: {url}")
        try:
            response = requests.post(url, headers=self.headers, json=payload, timeout=30)
            if response.status_code == 200:
                try:
                    resp_json = response.json()
                    logger.debug("response json:::", resp_json)
                    return resp_json
                except Exception as e:
                    logger.error(f"Failed to parse JSON: {e}, response text: {response.text}")
                    return {"error": f"Failed to parse JSON: {e}", "raw_response": response.text}
            else:
                logger.error(f"Failed to query metrics: {response.status_code} - {response.text}")
                return {"error": f"HTTP {response.status_code}", "raw_response": response.text}
        except Exception as e:
            logger.error(f"Exception in _post_query_range_v5: {e}")
            return {"error": f"Exception: {e}"}

    def fetch_dashboard_data(self, dashboard_name, start_time=None, end_time=None, step=None, variables_json=None, duration=None):
        """
        Fetches dashboard data for all panels in a specified Signoz dashboard by name.
        Accepts start_time and end_time as RFC3339 or relative strings (e.g., 'now-2h', 'now-30m'), or a duration string (e.g., '2h', '90m').
        If duration is provided, uses that as the window ending at now. If start_time and end_time are provided, uses those. Defaults to last 3 hours.
        Returns a dict with panel results.
        """
        # Use standardized time range logic
        start_dt, end_dt = self._get_time_range(start_time, end_time, duration, default_hours=3)
        from_time = int(start_dt.timestamp() * 1000)
        to_time = int(end_dt.timestamp() * 1000)
        try:
            dashboards = self.fetch_dashboards()
            if not dashboards or "data" not in dashboards:
                return {"status": "error", "message": "No dashboards found"}
            dashboard_id = None
            for d in dashboards["data"]:
                dashboard_data = d.get("data", {})
                if dashboard_data.get("title") == dashboard_name:
                    logger.debug(f"Found dashboard: {d}")
                    dashboard_id = d.get("uuid", d.get("id"))
                    break
            if not dashboard_id:
                return {"status": "error", "message": f"Dashboard '{dashboard_name}' not found"}
            dashboard_details = self.fetch_dashboard_details(dashboard_id)
            if not dashboard_details:
                return {"status": "error", "message": f"Dashboard details not found for '{dashboard_name}'"}
            # Panels are nested under 'data' in the dashboard details
            panels = dashboard_details.get("data", {}).get("widgets", [])
            logger.debug(f"dashboard_details: {dashboard_details.get('data', {})}")
            logger.debug(f"panels: {panels}")
            if not panels:
                return {"status": "error", "message": f"No panels found in dashboard '{dashboard_name}'"}
            # Build variables payload (resolved non-QUERY first, then QUERY, then overrides)
            variables = self._build_variables_payload(dashboard_details, variables_json, duration or "3h")
            # Step
            global_step = step if step is not None else 60
            query_builder = SignozDashboardQueryBuilder(global_step, variables)
            panel_results = {}
            logger.debug(f"panels: {panels}")
            print("Found num panels", len(panels))
            for panel in panels:
                panel_title = panel.get("title") or f"Panel_{panel.get('id', '')}"
                raw_panel_type = panel.get("panelTypes") or panel.get("panelType") or panel.get("type") or "graph"
                panel_type = self._map_panel_type(raw_panel_type)
                logger.debug(f"Processing panel '{panel_title}': raw_type='{raw_panel_type}' -> mapped_type='{panel_type}'")
                queries = []
                # Only process builder queries
                if (
                    isinstance(panel.get("query"), dict)
                    and panel["query"].get("queryType") == "builder"
                    and isinstance(panel["query"].get("builder"), dict)
                    and isinstance(panel["query"]["builder"].get("queryData"), list)
                ):
                    queries = panel["query"]["builder"]["queryData"]
                if not queries:
                    panel_results[panel_title] = {"status": "skipped", "message": "No builder queries in panel"}
                    continue
                built_queries = {}
                query_formulas = []
                try:
                    if (
                        isinstance(panel.get("query"), dict)
                        and isinstance(panel["query"].get("builder"), dict)
                        and isinstance(panel["query"]["builder"].get("queryFormulas"), list)
                    ):
                        query_formulas = panel["query"]["builder"].get("queryFormulas", [])
                except Exception:
                    query_formulas = []
                for query_data in queries:
                    if not isinstance(query_data, dict):
                        continue
                    # Build query dict - clean and format properly
                    # First, recursively clean the entire query data to remove problematic fields
                    cleaned_query_data = self._clean_query_dict(query_data)
                    query_dict = dict(cleaned_query_data)
                    
                    # Handle step interval - remove old format and set new
                    query_dict.pop("stepInterval", None)   # Remove if exists to avoid conflicts
                    query_dict["stepInterval"] = global_step
                    
                    # Normalize filter operators (SigNoz is case sensitive)
                    if isinstance(query_dict.get("filters"), dict):
                        items = query_dict["filters"].get("items")
                        if isinstance(items, list):
                            for item in items:
                                if isinstance(item, dict) and isinstance(item.get("op"), str):
                                    op = item["op"].lower()
                                    # Map to correct SigNoz operators
                                    op_mapping = {
                                        "in": "in", 
                                        "nin": "nin",
                                        "=": "=",
                                        "!=": "!=",
                                        ">": ">",
                                        "<": "<",
                                        ">=": ">=",
                                        "<=": "<=",
                                        "like": "like",
                                        "nlike": "nlike"
                                    }
                                    item["op"] = op_mapping.get(op, op)

                    # Handle groupBy field name conversion (only if old format exists)
                    if "group_by" in query_dict and "groupBy" not in query_dict:
                        query_dict["groupBy"] = query_dict.pop("group_by")
                    
                    # Ensure required fields are set correctly
                    query_dict["disabled"] = query_dict.get("disabled", False)
                    
                    # Ensure queryName is set (required by API)
                    if "queryName" not in query_dict:
                        query_dict["queryName"] = query_dict.get("expression", "A")
                    
                    data_source = query_dict.get("dataSource")

                    # pageSize is valid for logs/traces but NOT for metrics
                    if data_source in ("logs", "traces"):
                        query_dict["pageSize"] = query_dict.get("pageSize", 100)
                    else:
                        query_dict.pop("pageSize", None)

                    # Remove fields known to cause issues in metrics builder queries
                    if data_source == "metrics":
                        # Remove fields that cause 500 errors for metrics queries
                        query_dict.pop("orderBy", None)
                        query_dict.pop("limit", None)
                        # Keep timeAggregation if present in incoming config (SigNoz tolerates it)
                        # Ensure defaults for expected fields
                        query_dict["spaceAggregation"] = query_dict.get("spaceAggregation", "sum")
                        query_dict["reduceTo"] = query_dict.get("reduceTo", "avg")
                        # Strip UI-only fields from aggregateAttribute and filters
                        if isinstance(query_dict.get("aggregateAttribute"), dict):
                            agg_attr = query_dict["aggregateAttribute"]
                            # Reduce to minimal schema: only 'key'
                            if isinstance(agg_attr.get("key"), str):
                                query_dict["aggregateAttribute"] = {"key": agg_attr["key"]}
                            else:
                                query_dict["aggregateAttribute"].pop("id", None)
                                query_dict["aggregateAttribute"].pop("isJSON", None)
                                query_dict["aggregateAttribute"].pop("type", None)
                                query_dict["aggregateAttribute"].pop("dataType", None)
                                query_dict["aggregateAttribute"].pop("isColumn", None)
                        # Normalize groupBy for metrics to minimal schema
                        if isinstance(query_dict.get("groupBy"), list):
                            normalized_group_by = []
                            for grp in query_dict["groupBy"]:
                                if isinstance(grp, dict) and isinstance(grp.get("key"), str):
                                    normalized_group_by.append({"key": grp["key"]})
                                elif isinstance(grp, str):
                                    normalized_group_by.append({"key": grp})
                            query_dict["groupBy"] = normalized_group_by
                        # Ensure timeAggregation is 'rate' for rate operators
                        agg_op = query_dict.get("aggregateOperator")
                        if isinstance(agg_op, str) and agg_op.lower().endswith("_rate"):
                            query_dict["timeAggregation"] = "rate"
                        if isinstance(query_dict.get("filters"), dict):
                            # Normalize top-level op
                            if isinstance(query_dict["filters"].get("op"), str):
                                top_op = query_dict["filters"]["op"].upper()
                                query_dict["filters"]["op"] = top_op if top_op in ("AND", "OR") else "AND"
                            items = query_dict["filters"].get("items")
                            if isinstance(items, list):
                                for item in items:
                                    if isinstance(item, dict):
                                        item.pop("id", None)
                                        if isinstance(item.get("key"), dict):
                                            item["key"].pop("id", None)
                                            item["key"].pop("isJSON", None)
                            
                    elif data_source == "traces":
                        # For traces, keep orderBy, limit, and timeAggregation as they're expected
                        # but ensure pageSize is set
                        if "pageSize" not in query_dict:
                            query_dict["pageSize"] = 100
                    
                    # Ensure having field is a list if it exists
                    if "having" in query_dict and not isinstance(query_dict["having"], list):
                        query_dict["having"] = []
                    
                    # Ensure functions field is a list if it exists
                    if "functions" in query_dict and not isinstance(query_dict["functions"], list):
                        query_dict["functions"] = []
                    
                    # Use queryName as the key
                    query_key = query_dict.get("queryName", "A")
                    built_queries[query_key] = query_dict
                if not built_queries:
                    panel_results[panel_title] = {"status": "skipped", "message": "No valid builder queries in panel"}
                    continue
                # Build payload - match SigNoz expected structure exactly
                payload = {
                    "start": from_time,
                    "end": to_time,
                    "step": global_step,
                    "variables": variables,
                    "formatForWeb": True,
                    "compositeQuery": {
                        "queryType": "builder",
                        "panelType": panel_type,
                        "builderQueries": built_queries,
                    },
                }
                if query_formulas:
                    payload["compositeQuery"]["queryFormulas"] = query_formulas
                payload = json.loads(json.dumps(payload, ensure_ascii=False, indent=None))
                try:
                    logger.debug(f"Sending payload for panel '{panel_title}': {json.dumps(payload, indent=2)}")
                    result = self._post_query_range(payload)
                    panel_results[panel_title] = {"status": "success", "data": result, "panel_type": panel_type}
                except Exception as e:
                    logger.error(f"Failed to execute panel '{panel_title}': {e} with payload: {payload}")
                    panel_results[panel_title] = {"status": "error", "message": str(e), "panel_type": panel_type}
            return {"status": "success", "dashboard": dashboard_name, "results": panel_results}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def fetch_apm_metrics(self, service_name, start_time=None, end_time=None, window="1m", operation_names=None, metrics=None, duration=None):
        """
        Fetches standard APM metrics for a given service and time range using separate API calls for each metric.
        This approach returns separate graphs for each metric type (request_rate, error_rate, latency_avg).
        Accepts start_time and end_time as RFC3339 or relative strings (e.g., 'now-2h', 'now-30m'), or a duration string (e.g., '2h', '90m').
        If duration is provided, uses that as the window ending at now. If start_time and end_time are provided, uses those. Defaults to last 3 hours.
        """
        # Use standardized time range logic
        start_dt, end_dt = self._get_time_range(start_time, end_time, duration, default_hours=3)
        from_time = int(start_dt.timestamp() * 1000)
        to_time = int(end_dt.timestamp() * 1000)
        step_val = self._parse_step(window)
        if not metrics:
            metrics = ["request_rate", "error_rate", "apdex", "latency"]
        # First, let's check if the service exists and get available services
        try:
            services_response = self.fetch_services()
            if isinstance(services_response, dict) and services_response.get('status') == 'success':
                services_data = services_response.get('data', [])
                service_names = [s.get('serviceName', '') for s in services_data if isinstance(s, dict)]
                if service_name not in service_names:
                    # Try to find a similar service name
                    similar_services = [s for s in service_names if service_name.lower() in s.lower() or s.lower() in service_name.lower()]
                    if similar_services:
                        logger.warning(f"Service '{service_name}' not found. Similar services: {similar_services}")
        except Exception as e:
            logger.warning(f"Error fetching services: {e}")
        
        # Results dictionary to store each metric's data
        results = {}
        
        # Process each metric separately
        for metric_key in metrics:
            
            if metric_key == "latency":
                # For latency, we need to create a composite query with p50, p90, p99
                builder_queries = {}
                for subkey, template in APM_METRIC_QUERIES["latency"].items():
                    import copy
                    q = copy.deepcopy(template)
                    q["stepInterval"] = step_val
                    # Fill filters - use correct tag names for traces
                    filters = [
                        {
                            "key": {"key": "serviceName", "dataType": "string", "isColumn": True, "type": "tag"},
                            "op": "=",
                            "value": service_name,
                        }
                    ]
                    if operation_names:
                        filters.append(
                            {
                                "key": {"key": "name", "dataType": "string", "isColumn": True, "type": "tag"},
                                "op": "in",
                                "value": operation_names,
                            }
                        )
                    q["filters"] = {"items": filters, "op": "AND"}
                    q["queryName"] = q.get("expression")  # Use A, B, or C
                    builder_queries[q["queryName"]] = q
                
                payload = {
                    "start": from_time,
                    "end": to_time,
                    "step": step_val,
                    "variables": {},
                    "formatForWeb": False,
                    "compositeQuery": {
                        "queryType": "builder", 
                        "panelType": "graph", 
                        "fillGaps": False,
                        "builderQueries": builder_queries
                    },
                }
                result = self._post_query_range(payload)
                results[metric_key] = result
                
            elif metric_key in APM_METRIC_QUERIES:
                # Check if this is a composite query (like error_rate)
                if isinstance(APM_METRIC_QUERIES[metric_key], dict) and "F1" in APM_METRIC_QUERIES[metric_key]:
                    # Handle composite queries (error_rate with A, B, F1)
                    import copy
                    builder_queries = {}
                    template = APM_METRIC_QUERIES[metric_key]
                    
                    # Process A, B, C queries (for apdex) or A, B queries (for error_rate)
                    query_keys = ["A", "B", "C"] if metric_key == "apdex" else ["A", "B"]
                    for subkey in query_keys:
                        if subkey in template:
                            q = copy.deepcopy(template[subkey])
                            q["stepInterval"] = step_val
                            
                            # Fill filters - use correct tag names from SigNoz
                            if metric_key == "apdex":
                                # For apdex, use tag type for service_name
                                filters = [
                                    {"key": {"key": "service_name", "dataType": "string", "isColumn": False, "type": "tag"}, "op": "=", "value": service_name}
                                ]
                            else:
                                # For other metrics, use resource type for service_name
                                filters = [
                                    {"key": {"key": "service_name", "dataType": "string", "isColumn": False, "type": "resource"}, "op": "IN", "value": [service_name]}
                                ]
                            
                            if operation_names:
                                filters.append(
                                    {"key": {"key": "operation", "dataType": "string", "isColumn": False, "type": "tag"}, "op": "IN", "value": operation_names}
                                )
                            
                            # Add specific filters based on metric and query
                            if metric_key == "error_rate" and subkey == "A":
                                # Error count query
                                filters.append(
                                    {"key": {"key": "status_code", "dataType": "int64", "isColumn": False, "type": "tag"}, "op": "IN", "value": ["STATUS_CODE_ERROR"]}
                                )
                            elif metric_key == "apdex":
                                if subkey == "A":
                                    # Total requests - no additional filters needed
                                    pass
                                elif subkey == "B":
                                    # Satisfied requests (≤ 500ms, no errors)
                                    filters.extend([
                                        {"key": {"key": "status_code", "dataType": "string", "isColumn": False, "type": "tag"}, "op": "!=", "value": "STATUS_CODE_ERROR"},
                                        {"key": {"key": "le", "dataType": "string", "isColumn": False, "type": "tag"}, "op": "=", "value": "500"}
                                    ])
                                elif subkey == "C":
                                    # Tolerating requests (≤ 2000ms, no errors)
                                    filters.extend([
                                        {"key": {"key": "status_code", "dataType": "string", "isColumn": False, "type": "tag"}, "op": "!=", "value": "STATUS_CODE_ERROR"},
                                        {"key": {"key": "le", "dataType": "string", "isColumn": False, "type": "tag"}, "op": "=", "value": "2000"}
                                    ])
                            
                            q["filters"] = {"items": filters, "op": "AND"}
                            builder_queries[subkey] = q
                    
                    # Add the expression query (F1)
                    if "F1" in template:
                        builder_queries["F1"] = template["F1"]
                    
                    payload = {
                        "start": from_time,
                        "end": to_time,
                        "step": step_val,
                        "variables": {},
                        "formatForWeb": False,
                        "compositeQuery": {
                            "queryType": "builder", 
                            "panelType": "graph", 
                            "fillGaps": True,
                            "builderQueries": builder_queries
                        },
                    }
                    result = self._post_query_range(payload)
                    results[metric_key] = result
                else:
                    # Handle single queries (request_rate)
                    import copy
                    q = copy.deepcopy(APM_METRIC_QUERIES[metric_key])
                    q["stepInterval"] = step_val
                    q["queryName"] = "A"  # Single query
                    
                    # Fill filters - use correct tag names from SigNoz
                    filters = [
                        {"key": {"key": "service_name", "dataType": "string", "isColumn": False, "type": "resource"}, "op": "IN", "value": [service_name]}
                    ]
                    if operation_names:
                        filters.append(
                            {"key": {"key": "operation", "dataType": "string", "isColumn": False, "type": "tag"}, "op": "IN", "value": operation_names}
                        )
                    
                    q["filters"] = {"items": filters, "op": "AND"}
                    
                    payload = {
                        "start": from_time,
                        "end": to_time,
                        "step": step_val,
                        "variables": {},
                        "formatForWeb": False,
                        "compositeQuery": {
                            "queryType": "builder", 
                            "panelType": "graph", 
                            "fillGaps": False,
                            "builderQueries": {"A": q}
                        },
                    }
                    result = self._post_query_range(payload)
                    results[metric_key] = result
        
        # Log summary of results
        for metric, result in results.items():
            if isinstance(result, dict) and result.get('status') == 'success':
                data = result.get('data', {})
                result_data = data.get('result', [])
                series_count = 0
                for item in result_data:
                    if isinstance(item, dict) and 'series' in item:
                        series_count += len(item['series'])
                logger.debug(f"APM metric {metric}: {series_count} series found")
            else:
                logger.warning(f"APM metric {metric}: No data or error")
        
        return results

    def execute_clickhouse_query_tool(
        self,
        query,
        time_geq,
        time_lt,
        panel_type="table",
        fill_gaps=False,
        step=60,
    ):
        """
        Tool: Execute a Clickhouse SQL query via the Signoz API.
        """
        from_time = int(time_geq * 1000)
        to_time = int(time_lt * 1000)
        payload = {
            "start": from_time,
            "end": to_time,
            "step": step,
            "variables": {},
            "formatForWeb": True,
            "compositeQuery": {
                "queryType": "clickhouse_sql",
                "panelType": panel_type,
                "fillGaps": fill_gaps,
                "chQueries": {
                    "A": {
                        "name": "A",
                        "legend": "",
                        "disabled": False,
                        "query": query,
                    }
                },
            },
        }
        return self._post_query_range(payload)

    def execute_builder_query_tool(
        self,
        builder_queries,
        time_geq,
        time_lt,
        panel_type="table",
        step=60,
    ):
        """
        Tool: Execute a Signoz builder query via the Signoz API.
        """
        from_time = int(time_geq * 1000)
        to_time = int(time_lt * 1000)
        payload = {
            "start": from_time,
            "end": to_time,
            "step": step,
            "variables": {},
            "compositeQuery": {
                "queryType": "builder",
                "panelType": panel_type,
                "builderQueries": builder_queries,
            },
        }
        return self._post_query_range(payload)

    def fetch_traces_or_logs(self, data_type, start_time=None, end_time=None, duration=None, service_name=None, limit=100):
        """
        Fetch traces or logs from SigNoz using ClickHouse SQL.
        
        Args:
            data_type: Either 'traces' or 'logs'
            start_time: Start time as RFC3339 or relative string
            end_time: End time as RFC3339 or relative string  
            duration: Duration string (e.g., '2h', '90m')
            service_name: Optional service name filter
            limit: Maximum number of records to return
            
        Returns:
            Dict with status, message, data, and query used
        """
        try:
            # Use standardized time range logic
            start_dt, end_dt = self._get_time_range(start_time, end_time, duration, default_hours=3)
            from_time = int(start_dt.timestamp() * 1000)
            to_time = int(end_dt.timestamp() * 1000)
            limit = int(limit) if limit else 100
            
            if data_type == "traces":
                # For traces, use ClickHouse SQL approach
                table = "signoz_traces.distributed_signoz_index_v3"
                select_cols = "traceID, serviceName, name, durationNano, statusCode, timestamp"
                where_clauses = [
                    f"timestamp >= toDateTime64({int(start_dt.timestamp())}, 9)", 
                    f"timestamp < toDateTime64({int(end_dt.timestamp())}, 9)"
                ]
                if service_name:
                    where_clauses.append(f"serviceName = '{service_name}'")
                
                where_sql = " AND ".join(where_clauses)
                query = f"SELECT {select_cols} FROM {table} WHERE {where_sql} LIMIT {limit}"
                
                result = self.execute_clickhouse_query_tool(
                    query=query, 
                    time_geq=int(start_dt.timestamp()), 
                    time_lt=int(end_dt.timestamp()), 
                    panel_type="table", 
                    fill_gaps=False, 
                    step=60
                )
                
            elif data_type == "logs":
                # For logs, use the builder query approach that matches the working payload
                filters = {"items": [], "op": "AND"}
                
                if service_name:
                    filters["items"].append({
                        "key": {"key": "service.name", "dataType": "string", "isColumn": False, "type": "resource"},
                        "op": "IN",
                        "value": [service_name]
                    })
                
                builder_queries = {
                    "A": {
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
                        "functions": [],
                        "filters": filters,
                        "expression": "A",
                        "disabled": False,
                        "stepInterval": 60,
                        "having": [],
                        "limit": None,
                        "orderBy": [
                            {"columnName": "timestamp", "order": "desc"},
                            {"columnName": "id", "order": "desc"}
                        ],
                        "groupBy": [],
                        "legend": "",
                        "reduceTo": "avg",
                        "offset": 0,
                        "pageSize": limit
                    }
                }
                
                payload = {
                    "start": from_time,
                    "end": to_time,
                    "step": 60,
                    "variables": {},
                    "compositeQuery": {
                        "queryType": "builder",
                        "panelType": "list",
                        "fillGaps": False,
                        "builderQueries": builder_queries
                    }
                }
                
                result = self._post_query_range(payload)
                
            else:
                return {
                    "status": "error", 
                    "message": f"Invalid data_type: {data_type}. Must be 'traces' or 'logs'."
                }
            
            return {
                "status": "success", 
                "message": f"Fetched {data_type}", 
                "data": result, 
                "query": query if data_type == "traces" else "builder_query"
            }
        except Exception as e:
            return {
                "status": "error", 
                "message": f"Failed to fetch {data_type}: {e!s}"
            } 

    def fetch_logs_with_filter(self, filter_expression, start_time=None, end_time=None, duration=None, limit=100):
        """
        Fetch logs from SigNoz using a filter expression.
        
        Args:
            filter_expression: Filter expression for logs in SigNoz format.
                Examples:
                - "resource.service.name = 'emailservice'"
                - "attributes.severity_text = 'ERROR'"
                - "resource.service.name = 'emailservice' AND attributes.severity_text = 'ERROR'"
                
                Use SigNoz field names as documented in their API docs.
                
            start_time: Start time as RFC3339 or relative string
            end_time: End time as RFC3339 or relative string  
            duration: Duration string (e.g., '2h', '90m')
            limit: Maximum number of records to return
            
        Returns:
            Dict with status, message, data, and query used
        """
        try:
            # Use standardized time range logic
            start_dt, end_dt = self._get_time_range(start_time, end_time, duration, default_hours=3)
            from_time = int(start_dt.timestamp() * 1000)
            to_time = int(end_dt.timestamp() * 1000)
            limit = int(limit) if limit else 100
            
            # Use SigNoz v5 API format which supports filter.expression directly
            # Pass the expression directly to SigNoz - let SigNoz handle parsing
            # This implementation uses the v5 API endpoint for better filter expression support
            query_spec = {
                "name": "A",
                "signal": "logs",
                "order": [
                    {
                        "key": {
                            "name": "timestamp"
                        },
                        "direction": "desc"
                    },
                    {
                        "key": {
                            "name": "id"
                        },
                        "direction": "desc"
                    }
                ],
                "offset": 0,
                "limit": limit
            }
            
            # Add filter expression if provided - pass it directly to SigNoz
            if filter_expression:
                query_spec["filter"] = {
                    "expression": filter_expression
                }
            
            payload = {
                "start": from_time,
                "end": to_time,
                "requestType": "raw",
                "variables": {},
                "compositeQuery": {
                    "queries": [
                        {
                            "type": "builder_query",
                            "spec": query_spec
                        }
                    ]
                }
            }
            
            # Use v5 API which supports filter.expression format
            result = self._post_query_range_v5(payload)
            
            return {
                "status": "success", 
                "message": f"Fetched logs with filter: {filter_expression}", 
                "data": result, 
                "query": "builder_query"
            }
        except Exception as e:
            return {
                "status": "error", 
                "message": f"Failed to fetch logs with filter: {e!s}"
            }

    def fetch_logs_for_trace_id(self, trace_id, limit=10):
        """
        Fetch logs for a specific trace ID.
        
        Args:
            trace_id: The trace ID to fetch logs for
            limit: Maximum number of logs to return (default: 10)
            
        Returns:
            Dict with status, message, data, and query used
        """
        try:
            if not trace_id:
                return {
                    "status": "error",
                    "message": "Trace ID is required"
                }
            
            # Calculate time range (last 24 hours)
            end_dt = datetime.now(timezone.utc)
            start_dt = end_dt - timedelta(hours=24)
            from_time = int(start_dt.timestamp() * 1000)
            to_time = int(end_dt.timestamp() * 1000)
            limit = int(limit) if limit else 10
            
            # Build filter for trace_id
            filters = {
                "items": [
                    {
                        "key": {"key": "trace_id", "dataType": "string", "isColumn": False, "type": "attribute"},
                        "op": "=",
                        "value": trace_id
                    }
                ], 
                "op": "AND"
            }
            
            builder_queries = {
                "A": {
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
                    "functions": [],
                    "filters": filters,
                    "expression": "A",
                    "disabled": False,
                    "stepInterval": 60,
                    "having": [],
                    "limit": None,
                    "orderBy": [
                        {"columnName": "timestamp", "order": "desc"},
                        {"columnName": "id", "order": "desc"}
                    ],
                    "groupBy": [],
                    "legend": "",
                    "reduceTo": "avg",
                    "offset": 0,
                    "pageSize": limit
                }
            }
            
            payload = {
                "start": from_time,
                "end": to_time,
                "step": 60,
                "variables": {},
                "compositeQuery": {
                    "queryType": "builder",
                    "panelType": "list",
                    "fillGaps": False,
                    "builderQueries": builder_queries
                }
            }
            
            result = self._post_query_range(payload)
            
            return {
                "status": "success", 
                "message": f"Fetched logs for trace ID: {trace_id}", 
                "data": result, 
                "query": "builder_query"
            }
        except Exception as e:
            return {
                "status": "error", 
                "message": f"Failed to fetch logs for trace ID {trace_id}: {e!s}"
            }

    def fetch_traces_with_filters(self, filter_expression, start_time=None, end_time=None, duration=None, limit=100):
        """
        Fetch traces from SigNoz using a filter expression. Returns all spans for each trace.
        
        Args:
            filter_expression: Filter expression for traces. 
                Can use either ClickHouse column names (e.g., "serviceName = 'emailservice'") 
                or common field names (e.g., "service.name = 'emailservice'") which will be 
                converted to ClickHouse column names.
            start_time: Start time as RFC3339 or relative string
            end_time: End time as RFC3339 or relative string  
            duration: Duration string (e.g., '2h', '90m')
            limit: Maximum number of traces to return
            
        Returns:
            Dict with status, message, data, and query used
        """
        try:
            # Use standardized time range logic
            start_dt, end_dt = self._get_time_range(start_time, end_time, duration, default_hours=3)
            from_time = int(start_dt.timestamp() * 1000)
            to_time = int(end_dt.timestamp() * 1000)
            limit = int(limit) if limit else 100
            
            # Use ClickHouse SQL approach with correct column names (same as working fetch_traces_or_logs)
            table = "signoz_traces.distributed_signoz_index_v3"
            select_cols = "traceID, spanID, serviceName, name, durationNano, statusCode, timestamp, httpMethod, httpUrl"
            where_clauses = [
                f"timestamp >= toDateTime64({int(start_dt.timestamp())}, 9)", 
                f"timestamp < toDateTime64({int(end_dt.timestamp())}, 9)"
            ]
            
            # Add filter expression as WHERE clause if provided
            if filter_expression:
               where_clauses.append(filter_expression)
            
            where_sql = " AND ".join(where_clauses)
            query = f"SELECT {select_cols} FROM {table} WHERE {where_sql} ORDER BY traceID, timestamp LIMIT {limit}"
            
            result = self.execute_clickhouse_query_tool(
                query=query, 
                time_geq=int(start_dt.timestamp()), 
                time_lt=int(end_dt.timestamp()), 
                panel_type="table", 
                fill_gaps=False, 
                step=60
            )
            
            return {
                "status": "success", 
                "message": f"Fetched traces with filter: {filter_expression}", 
                "data": result, 
                "query": query
            }
        except Exception as e:
            return {
                "status": "error", 
                "message": f"Failed to fetch traces with filters: {e!s}"
            }

    # -------------------------------
    # Dashboard Variables Extraction
    # -------------------------------
    def _parse_clickhouse_table_values(self, ch_response) -> list:
        """Extract a flat list of cell values from a ClickHouse 'table' response.

        Expects the response format returned by /api/v4/query_range with panelType 'table'.
        """
        try:
            if not ch_response or not isinstance(ch_response, dict):
                return []

            data = ch_response.get("data", {})
            results = data.get("result", [])
            values = []
            for item in results:
                table = item.get("table")
                if not table:
                    continue
                rows = table.get("rows", [])
                for row in rows:
                    row_data = row.get("data", {})
                    for _, v in row_data.items():
                        if v is None:
                            continue
                        values.append(str(v))
            # Return unique, sorted values for stability
            return sorted(list({v for v in values}))
        except Exception as e:
            logger.error(f"Failed to parse ClickHouse table values: {e}")
            return []

    def _resolve_query_variable_values(self, query_sql: str, duration: str = "3h") -> list:
        """Execute a ClickHouse SQL query to resolve allowed values for a variable."""
        try:
            start_dt, end_dt = self._get_time_range(None, None, duration, default_hours=3)
            # Use table panel type for simpler parsing
            resp = self.execute_clickhouse_query_tool(
                query=query_sql,
                time_geq=int(start_dt.timestamp()),
                time_lt=int(end_dt.timestamp()),
                panel_type="table",
                fill_gaps=False,
                step=60,
            )
            return self._parse_clickhouse_table_values(resp)
        except Exception as e:
            logger.error(f"Failed to resolve variable query values: {e}")
            return []

    def _substitute_template_placeholders(self, query_sql: str, variables: dict) -> str:
        """Replace Go-template style placeholders {{ .name }} with literal values from variables.

        - Strings are single-quoted with escaping
        - Lists default to first element (to keep simple equality expressions working)
        """
        try:
            if not isinstance(query_sql, str) or not query_sql:
                return query_sql
            if not isinstance(variables, dict):
                variables = {}

            import re

            def _format_value(val):
                # If list, pick first value to avoid syntax issues in '=' context
                if isinstance(val, list):
                    if not val:
                        return ""  # will be treated as empty string -> ''
                    val = val[0]
                # Numbers: return as-is
                if isinstance(val, (int, float)):
                    return str(val)
                # Booleans
                if isinstance(val, bool):
                    return '1' if val else '0'
                # Default to string; single-quote and escape
                s = str(val)
                s = s.replace("'", "''")
                return f"'{s}'"

            pattern = re.compile(r"\{\{\s*\.(\w+)\s*\}\}")

            def _repl(match):
                key = match.group(1)
                if key in variables:
                    return _format_value(variables[key])
                # Not found -> empty quoted string
                return "''"

            return pattern.sub(_repl, query_sql)
        except Exception:
            return query_sql

    # -------------------------------
    # Dashboard Variables Helpers
    # -------------------------------
    def _get_dashboard_variables_map(self, dashboard_details: dict) -> dict:
        """Return the raw variables map from dashboard details (id -> var_def)."""
        if not isinstance(dashboard_details, dict):
            return {}
        data = dashboard_details.get("data", {})
        if not isinstance(data, dict):
            return {}
        vars_map = data.get("variables")
        return vars_map if isinstance(vars_map, dict) else {}

    def _extract_non_query_values(self, variables_map: dict) -> dict:
        """Resolve non-QUERY variable values keyed by variable name."""
        resolved: dict = {}
        for var_id, var_def in (variables_map or {}).items():
            if not isinstance(var_def, dict):
                continue
            var_type = (var_def.get("type") or "").upper()
            if var_type == "QUERY":
                continue
            name = var_def.get("name") or var_def.get("key") or var_id
            value = var_def.get("selectedValue") or var_def.get("textboxValue") or var_def.get("customValue")
            if var_def.get("multiSelect") and not isinstance(value, list):
                value = [value] if value is not None else []
            resolved[name] = value
        return resolved

    def _resolve_query_allowed_values_with_dependencies(self, var_def: dict, non_query_values: dict, duration: str) -> list:
        """Substitute dependent placeholders in the query and return allowed values."""
        query_sql = var_def.get("queryValue")
        if not isinstance(query_sql, str) or not query_sql.strip():
            return []
        substituted = self._substitute_template_placeholders(query_sql, non_query_values or {})
        return self._resolve_query_variable_values(substituted, duration)

    def _format_value_for_multiselect(self, value, multi: bool):
        if multi:
            if value is None:
                return []
            return value if isinstance(value, list) else [value]

    # -------------------------------
    # Trace Analysis Methods
    # -------------------------------
    
    def get_random_trace_ids(self, limit=5, time_range_hours=24, service_name=None):
        """
        Fetch random trace IDs from Signoz
        
        Args:
            limit: Number of random trace IDs to return (default: 5)
            time_range_hours: Time range in hours to look back (default: 24)
            service_name: Optional service name filter
            
        Returns:
            List of trace IDs
        """
        try:
            # Calculate time range
            end_dt = datetime.now(timezone.utc)
            start_dt = end_dt - timedelta(hours=time_range_hours)
            
            # Build the query
            where_clauses = [
                f"timestamp >= toDateTime64({int(start_dt.timestamp())}, 9)",
                f"timestamp < toDateTime64({int(end_dt.timestamp())}, 9)"
            ]
            
            if service_name:
                where_clauses.append(f"serviceName = '{service_name}'")
            
            where_sql = " AND ".join(where_clauses)
            query = f"""
                SELECT DISTINCT traceID 
                FROM signoz_traces.distributed_signoz_index_v3 
                WHERE {where_sql}
                ORDER BY rand() 
                LIMIT {limit}
            """
            
            # Execute the query
            result = self.execute_clickhouse_query_tool(
                query=query,
                time_geq=int(start_dt.timestamp()),
                time_lt=int(end_dt.timestamp()),
                panel_type="table",
                fill_gaps=False,
                step=60
            )
            
            # Extract trace IDs from response
            trace_ids = []
            if result and result.get("data", {}).get("result"):
                for item in result["data"]["result"]:
                    table = item.get("table", {})
                    rows = table.get("rows", [])
                    for row in rows:
                        row_data = row.get("data", {})
                        trace_id = row_data.get("traceID")
                        if trace_id:
                            trace_ids.append(trace_id)
            
            logger.info(f"Retrieved {len(trace_ids)} random trace IDs")
            return trace_ids
            
        except Exception as e:
            logger.error(f"Failed to get random trace IDs: {e}")
            return []
    
    def get_trace_spans(self, trace_id, include_attributes=True, limit=100):
        """
        Fetch all spans for a specific trace ID
        
        Args:
            trace_id: The trace ID to fetch spans for
            include_attributes: Whether to include span attributes
            
        Returns:
            List of span data dictionaries
        """
        try:
            # Calculate time range (last 24 hours)
            end_dt = datetime.now(timezone.utc)
            start_dt = end_dt - timedelta(hours=24)
            from_time = int(start_dt.timestamp() * 1000)
            to_time = int(end_dt.timestamp() * 1000)
            
            # Use ClickHouse SQL approach (same as existing working code)
            table = "signoz_traces.distributed_signoz_index_v3"
            select_cols = "traceID, spanID, serviceName, name, durationNano, statusCode, timestamp"
            where_clauses = [
                f"timestamp >= toDateTime64({int(start_dt.timestamp())}, 9)", 
                f"timestamp < toDateTime64({int(end_dt.timestamp())}, 9)",
                f"traceID = '{trace_id}'"
            ]
            
            where_sql = " AND ".join(where_clauses)
            query = f"SELECT {select_cols} FROM {table} WHERE {where_sql} LIMIT {limit}"
            
            
            result = self.execute_clickhouse_query_tool(
                query=query, 
                time_geq=int(start_dt.timestamp()), 
                time_lt=int(end_dt.timestamp()), 
                panel_type="table", 
                fill_gaps=False, 
                step=60
            )
            
            # Extract spans from response
            spans = []
            # Check if result is an error response
            if result and "error" in result:
                return spans
            
            # Process the trace API v4 response - correct structure
            if result and result.get("data", {}).get("result"):
                for item in result["data"]["result"]:
                    table = item.get("table", {})
                    rows = table.get("rows", [])
                    for row in rows:
                        row_data = row.get("data", {})
                        if isinstance(row_data, dict):
                            # Get the actual span ID from the response if available
                            actual_span_id = row_data.get("spanID") or f"span_{len(spans)}_{trace_id}"
                            
                            span_data = {
                                "span_id": actual_span_id,
                                "trace_id": row_data.get("traceID", trace_id),
                                "timestamp": row_data.get("timestamp"),
                                "service_name": row_data.get("serviceName", "unknown"),
                                "operation_name": row_data.get("name", "unknown"),
                                "duration_ns": row_data.get("durationNano", 0),
                                "status_code": row_data.get("statusCode", 0),
                                "has_error": int(row_data.get("statusCode", 0)) >= 400
                            }
                            
                            # Add additional attributes if requested
                            if include_attributes:
                                span_data.update({
                                    "http_method": row_data.get("httpMethod"),
                                    "http_url": row_data.get("httpUrl"),
                                    "http_status_code": row_data.get("httpStatusCode"),
                                    "rpc_method": row_data.get("rpcMethod"),
                                    "rpc_service": row_data.get("rpcService")
                                })
                            
                            spans.append(span_data)
            
            return spans
            
        except Exception as e:
            logger.error(f"Failed to get spans for trace {trace_id}: {e}")
            return []
    
    def get_logs_for_trace(self, trace_id, span_ids=None, limit=100):
        """
        Fetch logs related to a trace using the proper Signoz Logs API
        
        Args:
            trace_id: The trace ID to fetch logs for
            span_ids: Optional list of specific span IDs to filter by
            limit: Maximum number of logs to return
            
        Returns:
            List of log data dictionaries
        """
        try:
            # Calculate time range (last 24 hours)
            end_dt = datetime.now(timezone.utc)
            start_dt = end_dt - timedelta(hours=24)
            from_time = int(start_dt.timestamp() * 1000)
            to_time = int(end_dt.timestamp() * 1000)
            
            # Build filter expression for trace_id
            filter_expression = f"trace_id = '{trace_id}'"
            if span_ids:
                span_ids_str = "', '".join(span_ids)
                filter_expression += f" AND span_id IN ('{span_ids_str}')"
            
            # Use the exact working logs pattern from existing code
            filters = {
                "items": [
                    {
                        "key": {"key": "trace_id", "dataType": "string", "isColumn": False, "type": "attribute"},
                        "op": "=",
                        "value": trace_id
                    }
                ], 
                "op": "AND"
            }
            
            builder_queries = {
                "A": {
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
                    "functions": [],
                    "filters": filters,
                    "expression": "A",
                    "disabled": False,
                    "stepInterval": 60,
                    "having": [],
                    "limit": None,
                    "orderBy": [
                        {"columnName": "timestamp", "order": "desc"},
                        {"columnName": "id", "order": "desc"}
                    ],
                    "groupBy": [],
                    "legend": "",
                    "reduceTo": "avg",
                    "offset": 0,
                    "pageSize": limit
                }
            }
            
            payload = {
                "start": from_time,
                "end": to_time,
                "step": 60,
                "variables": {},
                "compositeQuery": {
                    "queryType": "builder",
                    "panelType": "list",
                    "fillGaps": False,
                    "builderQueries": builder_queries
                }
            }
            
            # Execute using the proper API v4 endpoint
            result = self._post_query_range(payload)
            
            # Extract logs from response
            logs = []
            
            # Check if result is an error response
            if result and "error" in result:
                return logs
            
            # Process the logs API v5 response
            if result and result.get("data", {}).get("result"):
                for item in result["data"]["result"]:
                    if item.get("queryName") == "A":  # Our query name
                        # Handle list format (v4 logs API)
                        if "list" in item:
                            log_list = item.get("list", [])
                            for log_entry in log_list:
                                if isinstance(log_entry, dict):
                                    # Extract data from nested attributes.data structure
                                    attributes_data = log_entry.get("data", {})
                                    
                                    log_data = {
                                        "timestamp": log_entry.get("timestamp"),
                                        "level": attributes_data.get("severity_text", "INFO"),
                                        "attributes": attributes_data  # Keep all the nested data as attributes
                                    }
                                    logs.append(log_data)
                        else:
                            # Handle table format (fallback)
                            table = item.get("table", {})
                            rows = table.get("rows", [])
                            for row in rows:
                                row_data = row.get("data", {})
                                if isinstance(row_data, dict):
                                    log_data = {
                                        "timestamp": row_data.get("timestamp"),
                                        "level": row_data.get("severity_text", "INFO"),
                                        "message": row_data.get("body"),
                                        "trace_id": row_data.get("trace_id"),
                                        "span_id": row_data.get("span_id"),
                                        "service_name": row_data.get("service_name", "unknown"),
                                        "attributes": {k: v for k, v in row_data.items() 
                                                     if k not in ["timestamp", "severity_text", "body", "trace_id", "span_id", "service_name"]}
                                    }
                                    logs.append(log_data)
            
            return logs
            
        except Exception as e:
            logger.error(f"Failed to get logs for trace {trace_id}: {e}")
            return []
    
    def get_logs_for_spans(self, span_data_list, limit_per_span=50):
        """
        Fetch logs for multiple spans efficiently
        
        Args:
            span_data_list: List of span data dictionaries containing span_id and trace_id
            limit_per_span: Maximum logs per span
            
        Returns:
            Dictionary mapping span_id to list of logs
        """
        try:
            span_logs = {}
            
            for span_data in span_data_list:
                span_id = span_data.get("span_id")
                trace_id = span_data.get("trace_id")
                
                if not span_id or not trace_id:
                    continue
                
                # Get logs for this specific span
                logs = self.get_logs_for_trace(trace_id, [span_id], limit_per_span)
                span_logs[span_id] = logs
            
            logger.info(f"Retrieved logs for {len(span_logs)} spans")
            return span_logs
            
        except Exception as e:
            logger.error(f"Failed to get logs for spans: {e}")
            return {}
    
    def execute_trace_analysis(self, trace_id, max_spans_per_trace=None, max_logs_per_span=50, include_span_attributes=True):
        """
        Execute complete trace analysis workflow for a specific trace ID
        
        Args:
            trace_id: The specific trace ID to analyze
            max_spans_per_trace: Limit spans per trace (None for no limit)
            max_logs_per_span: Limit logs per span
            include_span_attributes: Whether to include detailed span attributes
            
        Returns:
            Complete trace analysis results
        """
        try:
            
            if not trace_id:
                return {
                    "status": "error",
                    "message": "Trace ID is required",
                    "data": None
                }
            
            # Step 1: Get spans for the specific trace
            spans = self.get_trace_spans(trace_id, include_span_attributes, max_spans_per_trace or 100)
            

            if not spans:
                logger.warning(f"No spans found for trace ID: {trace_id}, but continuing with log analysis")
            
            # Limit spans if specified
            if max_spans_per_trace and len(spans) > max_spans_per_trace:
                spans = spans[:max_spans_per_trace]
                logger.info(f"Limited spans to {max_spans_per_trace} (found {len(spans)} total)")
            
            # Step 2: Get logs for the trace (using trace_id filter)
            try:
                trace_logs = self.get_logs_for_trace(trace_id, limit=max_logs_per_span * len(spans))
            except Exception as e:
                trace_logs = []
            
            # Step 3: Get logs for individual spans (optional, for more detailed correlation)
            span_logs = {}
            if spans:
                try:
                    span_data_list = [{"span_id": span["span_id"], "trace_id": trace_id} for span in spans]
                    span_logs = self.get_logs_for_spans(span_data_list, max_logs_per_span)
                except Exception as e:
                    logger.error(f"Failed to get logs for spans: {e}")
                    span_logs = {}
            
            # Step 4: Combine spans with their logs
            spans_with_logs = []
            for span in spans:
                span_id = span["span_id"]
                # Get logs specific to this span
                span_specific_logs = span_logs.get(span_id, [])
                span["logs"] = span_specific_logs
                spans_with_logs.append(span)
            
            # Step 5: Create analysis results
            services_involved = []
            has_errors = False
            
            if spans:
                services_involved = list(set(span.get("service_name") for span in spans if span.get("service_name")))
                has_errors = any(span.get("has_error", False) for span in spans)
            else:
                # Try to extract service information from logs if no spans found
                services_involved = list(set(log.get("service_name") for log in trace_logs if log.get("service_name")))
                has_errors = any(log.get("level", "").upper() in ["ERROR", "FATAL"] for log in trace_logs)
            
            # Determine success message based on what we found
            if spans and trace_logs:
                message = f"Successfully analyzed trace {trace_id} with {len(spans_with_logs)} spans and {len(trace_logs)} logs"
            elif spans:
                message = f"Successfully analyzed trace {trace_id} with {len(spans_with_logs)} spans (no logs found)"
            elif trace_logs:
                message = f"Successfully analyzed trace {trace_id} with {len(trace_logs)} logs (no spans found)"
            else:
                message = f"Trace {trace_id} found but no spans or logs retrieved"
            
            analysis_results = {
                "status": "success",
                "message": message,
                "summary": {
                    "trace_id": trace_id,
                    "total_spans": len(spans_with_logs),
                    "total_trace_logs": len(trace_logs),
                    "total_span_logs": sum(len(logs) for logs in span_logs.values()),
                    "services_involved": services_involved,
                    "has_errors": has_errors,
                    "spans_found": len(spans) > 0,
                    "logs_found": len(trace_logs) > 0
                },
                "trace_data": {
                    "trace_id": trace_id,
                    "spans": spans_with_logs,
                    "trace_logs": trace_logs,  # All logs for the trace
                    "span_count": len(spans_with_logs),
                    "log_count": len(trace_logs)
                }
            }
            
            return analysis_results
            
        except Exception as e:
            logger.error(f"Trace analysis failed for {trace_id}: {e}")
            return {
                "status": "error",
                "message": f"Trace analysis failed: {str(e)}",
                "data": None
            }
        
        # Final safety check - this should never be reached, but just in case
        print(f"Trace analysis method reached end without returning - this should not happen")
        return {
            "status": "error",
            "message": "Trace analysis method failed to return a result",
            "data": None
        }

    def _build_variables_payload(self, dashboard_details: dict, variables_json: Optional[str], duration: str) -> dict:
        """
        Build the variables payload to send to SigNoz query_range:
        - Resolve non-QUERY vars first
        - Use them to resolve QUERY vars when selectedValue is absent
        - Map by both id and name; omit None
        - Merge user overrides by name or id
        """
        variables_payload: dict = {}
        variables_map = self._get_dashboard_variables_map(dashboard_details)
        name_to_id: dict = {}

        # 1) Non-QUERY first (by name and id)
        non_query_values = self._extract_non_query_values(variables_map)
        for var_id, var_def in variables_map.items():
            if not isinstance(var_def, dict):
                continue
            var_type = (var_def.get("type") or "").upper()
            if var_type == "QUERY":
                continue
            name = var_def.get("name") or var_def.get("key") or var_id
            value = non_query_values.get(name)
            value = self._format_value_for_multiselect(value, bool(var_def.get("multiSelect")))
            if value is not None:
                variables_payload[var_id] = value
                variables_payload[name] = value
                name_to_id[name] = var_id

        # 2) QUERY variables (resolve only if needed)
        for var_id, var_def in variables_map.items():
            if not isinstance(var_def, dict):
                continue
            var_type = (var_def.get("type") or "").upper()
            if var_type != "QUERY":
                continue
            name = var_def.get("name") or var_def.get("key") or var_id
            value = var_def.get("selectedValue")
            if value in (None, ""):
                allowed = self._resolve_query_allowed_values_with_dependencies(var_def, non_query_values, duration)
                value = self._format_value_for_multiselect(allowed, bool(var_def.get("multiSelect")))
            if value is not None and value != "":
                variables_payload[var_id] = value
                variables_payload[name] = value
                name_to_id[name] = var_id

        # 3) Merge user overrides (by name or id)
        if variables_json:
            try:
                user_vars = json.loads(variables_json)
                if isinstance(user_vars, dict):
                    for k, v in user_vars.items():
                        variables_payload[k] = v
                        if k in name_to_id and name_to_id[k]:
                            variables_payload[name_to_id[k]] = v
            except Exception:
                pass

        return variables_payload

    def extract_dashboard_variables_from_details(self, dashboard_details: dict, resolve_queries: bool = True, duration: str = "3h") -> dict:
        """
        Extract variables from Signoz dashboard details and optionally resolve QUERY variable values.

        Returns a dict: {
          "variables": {
             <var_name>: {
                "type": <TYPE>,
                "selected_value": <current>,
                "allowed_values": [..],
                "query": <query_sql_if_any>
             },
             ...
          }
        }
        """
        try:
            variables_section = {}
            if not dashboard_details or not isinstance(dashboard_details, dict):
                return {"variables": variables_section}

            # Variables live under data.variables in Signoz dashboard payload
            data = dashboard_details.get("data", {}) if isinstance(dashboard_details.get("data", {}), dict) else {}
            variables_map = data.get("variables") or dashboard_details.get("variables") or {}

            if not isinstance(variables_map, dict):
                return {"variables": variables_section}

            # First pass: collect non-QUERY variables by name
            non_query_values = {}
            for _id, var_def in variables_map.items():
                if not isinstance(var_def, dict):
                    continue
                var_name = var_def.get("name") or var_def.get("key") or _id
                var_type = (var_def.get("type") or "").upper()
                if var_type != "QUERY":
                    val = var_def.get("selectedValue") or var_def.get("textboxValue") or var_def.get("customValue")
                    if var_def.get("multiSelect") and not isinstance(val, list):
                        val = [val] if val is not None else []
                    non_query_values[var_name] = val

            # Second pass: build the final structure with QUERY resolution using non-QUERY values
            for _id, var_def in variables_map.items():
                if not isinstance(var_def, dict):
                    continue
                var_name = var_def.get("name") or var_def.get("key") or _id
                var_type = (var_def.get("type") or "").upper()
                selected_value = var_def.get("selectedValue")
                query_sql = var_def.get("queryValue")

                allowed_values = []
                if resolve_queries and var_type == "QUERY" and isinstance(query_sql, str) and query_sql.strip():
                    substituted = self._substitute_template_placeholders(query_sql, non_query_values)
                    allowed_values = self._resolve_query_variable_values(substituted, duration=duration)

                variables_section[var_name] = {
                    "type": var_type,
                    "selected_value": selected_value,
                    "allowed_values": allowed_values,
                    "query": query_sql or "",
                }

            return {"variables": variables_section}
        except Exception as e:
            logger.error(f"Failed to extract dashboard variables: {e}")
            return {"variables": {}}

    def get_dashboard_variables(self, dashboard_id: str, resolve_queries: bool = True, duration: str = "3h") -> dict:
        """Fetch dashboard details and return extracted variables, optionally resolving QUERY values."""
        try:
            details = self.fetch_dashboard_details(dashboard_id)
            return self.extract_dashboard_variables_from_details(details, resolve_queries=resolve_queries, duration=duration)
        except Exception as e:
            logger.error(f"Failed to get dashboard variables for {dashboard_id}: {e}")
            return {"variables": {}}