import json
import logging
import re
from datetime import datetime, timedelta, timezone

import requests
from dateutil import parser as dateparser

from core.integrations.processor import Processor
from core.protos.base_pb2 import TimeRange

logger = logging.getLogger(__name__)


# Hardcoded builder query templates for standard APM metrics (matching SigNoz frontend)
APM_METRIC_QUERIES = {
    "request_rate": {
        "dataSource": "metrics",
        "aggregateOperator": "sum_rate",
        "aggregateAttribute": {"key": "signoz_latency.count", "dataType": "float64", "isColumn": True, "type": ""},
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
        "legend": "Request Rate",
        "reduceTo": "avg",
    },
    "error_rate": {
        "dataSource": "metrics",
        "aggregateOperator": "sum_rate",
        "aggregateAttribute": {"key": "signoz_errors.count", "dataType": "float64", "isColumn": True, "type": ""},
        "timeAggregation": "rate",
        "spaceAggregation": "sum",
        "functions": [],
        "filters": None,  # Fill dynamically
        "expression": "B",
        "disabled": False,
        "stepInterval": None,  # Fill dynamically
        "having": [],
        "limit": None,
        "orderBy": [],
        "groupBy": [],
        "legend": "Error Rate",
        "reduceTo": "avg",
    },
    # Latency metrics use a multi-query structure: sum, count, then quantile/avg
    "latency": {
        "sum": {
            "dataSource": "metrics",
            "aggregateOperator": "sum",
            "aggregateAttribute": {"key": "signoz_latency.sum", "dataType": "float64", "isColumn": True, "type": ""},
            "timeAggregation": "sum",
            "spaceAggregation": "sum",
            "functions": [],
            "filters": None,  # Fill dynamically
            "expression": "C",
            "disabled": False,
            "stepInterval": None,  # Fill dynamically
            "having": [],
            "limit": None,
            "orderBy": [],
            "groupBy": [],
            "legend": "Latency Sum",
            "reduceTo": "avg",
        },
        "count": {
            "dataSource": "metrics",
            "aggregateOperator": "sum",
            "aggregateAttribute": {"key": "signoz_latency.count", "dataType": "float64", "isColumn": True, "type": ""},
            "timeAggregation": "sum",
            "spaceAggregation": "sum",
            "functions": [],
            "filters": None,  # Fill dynamically
            "expression": "D",
            "disabled": False,
            "stepInterval": None,  # Fill dynamically
            "having": [],
            "limit": None,
            "orderBy": [],
            "groupBy": [],
            "legend": "Latency Count",
            "reduceTo": "avg",
        },
        "avg": {
            "dataSource": "metrics",
            "aggregateOperator": "divide",
            "aggregateAttribute": {"key": "signoz_latency.sum", "dataType": "float64", "isColumn": True, "type": ""},
            "timeAggregation": "avg",
            "spaceAggregation": "avg",
            "functions": [],
            "filters": None,
            "expression": "C/D",
            "disabled": False,
            "stepInterval": None,
            "having": [],
            "limit": None,
            "orderBy": [],
            "groupBy": [],
            "legend": "Latency Avg",
            "reduceTo": "avg",
        },
    },
}


class SignozApiProcessor(Processor):
    def __init__(self, signoz_api_url, signoz_api_token=None, ssl_verify="true"):
        self.signoz_api_url = signoz_api_url.rstrip("/") if signoz_api_url else ""
        self.__ssl_verify = not (ssl_verify and ssl_verify.lower() == "false")
        
        if not self.signoz_api_url:
            raise ValueError("SignozApiProcessor: API URL cannot be empty")
            
        # Set default headers
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        # Add authentication if provided
        if signoz_api_token:
            # Use SIGNOZ-API-KEY header for authentication instead of Authorization
            self.headers["SIGNOZ-API-KEY"] = signoz_api_token
            
            logger.debug(f"SignozApiProcessor initialized with URL: {self.signoz_api_url} and API key")
        else:
            logger.warning("SignozApiProcessor initialized without API key")
    
    def test_connection(self):
        """Test the connection to Signoz API"""
        try:
            response = requests.get(
                f"{self.signoz_api_url}/api/v1/health", 
                headers=self.headers,
                verify=self.__ssl_verify,
                timeout=30
            )
            
            if response.status_code < 300:
                return True
            else:
                logger.error(f"Failed to connect to Signoz API: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            logger.error(f"Exception when testing connection to Signoz API: {e}")
            raise e
    
    def fetch_dashboards(self):
        """Fetch all dashboards from Signoz"""
        try:
            response = requests.get(
                f"{self.signoz_api_url}/api/v1/dashboards", 
                headers=self.headers,
                verify=self.__ssl_verify,
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to fetch dashboards: {response.status_code} - {response.text}")
                return None
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
                verify=self.__ssl_verify,
                timeout=30
            )
            
            if response.status_code == 200:
                response_data = response.json()
                return response_data.get("data", response_data)
            else:
                logger.error(f"Failed to fetch dashboard details: {response.status_code} - {response.text}")
                return None
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
                verify=self.__ssl_verify,
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to fetch alerts: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Exception when fetching alerts: {e}")
            raise e
    
    def fetch_alert_details(self, alert_id):
        """Fetch details of a specific alert"""
        try:
            response = requests.get(
                f"{self.signoz_api_url}/api/v1/alerts/{alert_id}", 
                headers=self.headers,
                verify=self.__ssl_verify,
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to fetch alert details: {response.status_code} - {response.text}")
                return None
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
                verify=self.__ssl_verify,
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to query metrics: {response.status_code} - {response.text}")
                return None
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
                verify=self.__ssl_verify,
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to execute Clickhouse query: {response.status_code} - {response.text}")
                return None
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
            response = requests.post(url, headers=self.headers, json=payload, verify=self.__ssl_verify, timeout=30)
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
            response = requests.post(url, headers=self.headers, json=payload, verify=self.__ssl_verify, timeout=30)
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
                    dashboard_id = d.get("id")
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
            # Parse variables
            variables = {}
            if variables_json:
                try:
                    variables = json.loads(variables_json)
                    if not isinstance(variables, dict):
                        variables = {}
                except Exception:
                    variables = {}
            # Step
            global_step = step if step is not None else 60
            panel_results = {}
            logger.debug(f"panels: {panels}")
            for panel in panels:
                panel_title = panel.get("title") or f"Panel_{panel.get('id', '')}"
                panel_type = panel.get("panelTypes") or panel.get("panelType") or panel.get("type") or "graph"
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
                for query_data in queries:
                    if not isinstance(query_data, dict):
                        continue
                    # Build query dict similar to the original code
                    query_dict = dict(query_data)
                    query_dict.pop("step_interval", None)
                    query_dict["stepInterval"] = global_step
                    if "group_by" in query_dict:
                        query_dict["groupBy"] = query_dict.pop("group_by")
                    query_dict["disabled"] = query_dict.get("disabled", False)
                    # Add pageSize for metrics queries
                    if query_dict.get("dataSource") == "metrics":
                        query_dict["pageSize"] = 10
                    built_queries[query_dict.get("expression", "A")] = query_dict
                if not built_queries:
                    panel_results[panel_title] = {"status": "skipped", "message": "No valid builder queries in panel"}
                    continue
                # Build payload
                payload = {
                    "start": from_time,
                    "end": to_time,
                    "step": global_step,
                    "variables": variables,
                    "formatForWeb": False,
                    "compositeQuery": {
                        "queryType": "builder",
                        "panelType": panel_type,
                        "fillGaps": False,
                        "builderQueries": built_queries,
                    },
                }
                payload = json.loads(json.dumps(payload, ensure_ascii=False, indent=None))
                try:
                    result = self._post_query_range(payload)
                    panel_results[panel_title] = {"status": "success", "data": result}
                except Exception as e:
                    panel_results[panel_title] = {"status": "error", "message": str(e)}
            return {"status": "success", "dashboard": dashboard_name, "results": panel_results}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def fetch_apm_metrics(self, service_name, start_time=None, end_time=None, window="1m", operation_names=None, metrics=None, duration=None):
        """
        Fetches standard APM metrics for a given service and time range using hardcoded builder query templates.
        Accepts start_time and end_time as RFC3339 or relative strings (e.g., 'now-2h', 'now-30m'), or a duration string (e.g., '2h', '90m').
        If duration is provided, uses that as the window ending at now. If start_time and end_time are provided, uses those. Defaults to last 3 hours.
        """
        # Use standardized time range logic
        start_dt, end_dt = self._get_time_range(start_time, end_time, duration, default_hours=3)
        from_time = int(start_dt.timestamp() * 1000)
        to_time = int(end_dt.timestamp() * 1000)
        step_val = self._parse_step(window)
        if not metrics:
            metrics = ["request_rate", "error_rate", "latency_avg"]
        builder_queries = {}
        query_name_counter = 65  # ASCII 'A'
        for metric_key in metrics:
            if metric_key == "latency_avg":
                # Add sum, count, and avg queries for latency
                for subkey, template in APM_METRIC_QUERIES["latency"].items():
                    import copy

                    q = copy.deepcopy(template)
                    q["stepInterval"] = step_val
                    # Fill filters
                    filters = [
                        {
                            "key": {"key": "service.name", "dataType": "string", "isColumn": False, "type": "resource"},
                            "op": "IN",
                            "value": [service_name],
                        }
                    ]
                    if operation_names:
                        filters.append(
                            {
                                "key": {"key": "operation", "dataType": "string", "isColumn": False, "type": "tag"},
                                "op": "IN",
                                "value": operation_names,
                            }
                        )
                    if subkey in ("sum", "count"):
                        q["filters"] = {"items": filters, "op": "AND"}
                    q["queryName"] = q.get("expression")  # Use C, D, or C/D
                    builder_queries[q["queryName"]] = q
            elif metric_key in APM_METRIC_QUERIES:
                import copy

                q = copy.deepcopy(APM_METRIC_QUERIES[metric_key])
                q["stepInterval"] = step_val
                filters = [
                    {"key": {"key": "service.name", "dataType": "string", "isColumn": False, "type": "resource"}, "op": "IN", "value": [service_name]}
                ]
                if operation_names:
                    filters.append(
                        {"key": {"key": "operation", "dataType": "string", "isColumn": False, "type": "tag"}, "op": "IN", "value": operation_names}
                    )
                q["filters"] = {"items": filters, "op": "AND"}
                q["queryName"] = chr(query_name_counter)
                query_name_counter += 1
                builder_queries[q["queryName"]] = q
        payload = {
            "start": from_time,
            "end": to_time,
            "step": step_val,
            "variables": {},
            "compositeQuery": {"queryType": "builder", "panelType": "graph", "builderQueries": builder_queries},
        }
        return self._post_query_range(payload)

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