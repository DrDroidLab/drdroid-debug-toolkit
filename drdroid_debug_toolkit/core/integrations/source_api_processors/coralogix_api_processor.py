import logging
import requests
import json
import subprocess
from datetime import datetime, timedelta
from core.integrations.processor import Processor

logger = logging.getLogger(__name__)


class CoralogixApiProcessor(Processor):
    """
    Coralogix API processor for interacting with Coralogix APIs.
    Based on Coralogix Developer Portal APIs: https://coralogix.com/docs/developer-portal/apis/
    """
    
    def __init__(self, api_key: str, endpoint: str, ssl_verify: str = 'true', domain: str = None):
        """
        Initialize Coralogix API processor.
        
        Args:
            api_key: Coralogix API key
            endpoint: Coralogix endpoint URL (e.g., https://api.coralogix.com)
            ssl_verify: Whether to verify SSL certificates ('true' or 'false')
        """
        self.__api_key = api_key
        self.__endpoint = endpoint.rstrip('/')
        self.__ssl_verify = False if ssl_verify and ssl_verify.lower() == 'false' else True
        
        # Set up headers for API requests
        self.headers = {
            'Authorization': f'Bearer {self.__api_key}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

    def test_connection(self):
        """
        Test the connection to Coralogix API.
        
        Returns:
            bool: True if connection is successful, False otherwise
            
        Raises:
            Exception: If connection test fails
        """
        try:
            # Use the correct Coralogix API endpoint for testing connection
            start_time_rfc3339 = self._parse_time_to_rfc3339("now-4h")
            end_time_rfc3339 = self._parse_time_to_rfc3339("now")
            
            url = f'{self.__endpoint}/api/v1/dataprime/query'
            
            payload = {
                "query": "* | limit 1",
                "metadata": {
                    "tier": "TIER_FREQUENT_SEARCH",
                    "syntax": "QUERY_SYNTAX_LUCENE",
                    "startDate": start_time_rfc3339,
                    "endDate": end_time_rfc3339,
                    "defaultSource": "logs"
                }
            }
            
            response = requests.post(
                url, 
                headers=self.headers, 
                json=payload,
                verify=self.__ssl_verify, 
                timeout=60
            )

            if response.status_code == 200:
                logger.info(f"Successfully connected to Coralogix API at {url}")
                return True
            elif response.status_code == 401:
                raise Exception("Authentication failed: Invalid API key")
            elif response.status_code == 403:
                logger.info(response.text)
                raise Exception(f"Access forbidden: API key lacks required permissions, {response.text}")
            else:
                raise Exception(
                    f"Failed to connect with Coralogix. Status Code: {response.status_code}. "
                    f"Response Text: {response.text}"
                )
                
        except requests.exceptions.Timeout:
            raise Exception("Connection timeout: Coralogix API did not respond within 30 seconds")
        except requests.exceptions.ConnectionError:
            raise Exception(f"Connection error: Unable to reach Coralogix endpoint {self.__endpoint}")
        except Exception as e:
            logger.error(f"Exception occurred while testing Coralogix connection: {e}")
            raise e

    def get_connection(self):
        """
        Get connection information (required by Processor base class).
        
        Returns:
            dict: Connection information
        """
        return {
            'endpoint': self.__endpoint,
            'api_key': self.__api_key[:8] + '...' if self.__api_key else None,  # Mask API key
            'ssl_verify': self.__ssl_verify
        }

    def _parse_time_string(self, time_str: str) -> str:
        """
        Convert time string to Unix timestamp string for Coralogix API.
        
        Args:
            time_str: Time string like 'now', 'now-1h', 'now-24h', or RFC3339 format
            
        Returns:
            str: Unix timestamp as string
        """
        try:
            if time_str == 'now':
                return str(int(datetime.now().timestamp()))
            elif time_str.startswith('now-'):
                # Parse relative time like 'now-1h', 'now-24h', 'now-30m'
                duration_str = time_str[4:]  # Remove 'now-'
                
                # Parse duration
                if duration_str.endswith('h'):
                    hours = int(duration_str[:-1])
                    target_time = datetime.now() - timedelta(hours=hours)
                elif duration_str.endswith('m'):
                    minutes = int(duration_str[:-1])
                    target_time = datetime.now() - timedelta(minutes=minutes)
                elif duration_str.endswith('d'):
                    days = int(duration_str[:-1])
                    target_time = datetime.now() - timedelta(days=days)
                elif duration_str.endswith('s'):
                    seconds = int(duration_str[:-1])
                    target_time = datetime.now() - timedelta(seconds=seconds)
                else:
                    # Default to hours if no unit specified
                    hours = int(duration_str)
                    target_time = datetime.now() - timedelta(hours=hours)
                
                return str(int(target_time.timestamp()))
            else:
                # Try to parse as RFC3339 or other format
                try:
                    # Try RFC3339 format first
                    dt = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
                    return str(int(dt.timestamp()))
                except ValueError:
                    # If that fails, try parsing as Unix timestamp
                    return str(int(float(time_str)))
        except Exception as e:
            logger.warning(f"Failed to parse time string '{time_str}', using as-is: {e}")
            return time_str


    def fetch_logs(self, query: str = "*", from_time: str = "now-1h", to_time: str = "now", limit: int = 100):
        """
        Fetch logs from Coralogix using Lucene queries via direct query API.
        
        Args:
            query: Lucene query string
            from_time: Start time for the query (e.g., "now-1h", "2023-01-01T00:00:00Z")
            to_time: End time for the query (e.g., "now", "2023-01-01T01:00:00Z")
            limit: Maximum number of logs to return
            
        Returns:
            dict: API response containing logs
        """
        try:
            # Parse time strings to RFC3339 format
            start_time_rfc3339 = self._parse_time_to_rfc3339(from_time)
            end_time_rfc3339 = self._parse_time_to_rfc3339(to_time)
            
            # Use the direct query API endpoint
            url = f'{self.__endpoint}/api/v1/dataprime/query'
            
            # Direct query payload structure with proper format
            payload = {
                "query": query,
                "metadata": {
                    "tier": "TIER_FREQUENT_SEARCH",
                    "syntax": "QUERY_SYNTAX_LUCENE",
                    "startDate": start_time_rfc3339,
                    "endDate": end_time_rfc3339,
                    "defaultSource": "logs",
                    "limit": limit
                }
            }
            print(url, payload)
            
            response = requests.post(
                url, 
                headers=self.headers, 
                json=payload, 
                verify=self.__ssl_verify, 
                timeout=60
            )
            print(f"response: {response.text.strip()}")
            if response.status_code == 200:
                # Parse NDJSON response format
                try:
                    lines = response.text.strip().split('\n')
                    results = []
                    
                    for line in lines:
                        if line.strip():
                            line_data = json.loads(line)
                            
                            # Check for results in different possible structures
                            if "result" in line_data and "results" in line_data["result"]:
                                # Regular query results format
                                results.extend(line_data["result"]["results"])
                            elif "results" in line_data:
                                # Direct results format
                                results.extend(line_data["results"])
                    
                    return {"results": results, "count": len(results)}
                        
                except Exception as e:
                    # Return the raw response as text if parsing fails
                    return {"raw_response": response.text, "status": "success", "error": str(e)}
            else:
                raise Exception(f"Direct query endpoint failed with status {response.status_code}: {response.text}")
                
        except Exception as e:
            logger.error(f"Exception occurred while fetching logs: {e}")
            raise e

    def fetch_metrics(self, query: str, from_time: str = "now-1h", to_time: str = "now", step: str = "1m"):
        """
        Fetch metrics from Coralogix using the Prometheus-compatible Metrics API.
        
        Args:
            query: PromQL query string
            from_time: Start time for the query
            to_time: End time for the query
            step: Query resolution step width
            
        Returns:
            dict: API response containing metrics time series
        """
        try:
            # Use the existing endpoint and replace 'api' with 'ng-api-http' for metrics
            metrics_endpoint = self.__endpoint.replace('api.', 'ng-api-http.')
            
            # Parse time strings to Unix timestamps
            start_timestamp = self._parse_time_string(from_time)
            end_timestamp = self._parse_time_string(to_time)
            
            # Use Prometheus-compatible Metrics API
            url = f'{metrics_endpoint}/metrics/api/v1/query_range'
            
            # Prometheus API parameters
            params = {
                'query': query,
                'start': start_timestamp,
                'end': end_timestamp,
                'step': step
            }
            
            
            response = requests.get(
                url, 
                headers=self.headers, 
                params=params,  # Use query parameters for Prometheus API
                verify=self.__ssl_verify, 
                timeout=60
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                error_msg = f"Failed to fetch metrics. Status Code: {response.status_code}. Response Text: {response.text}"
                raise Exception(error_msg)
                
        except Exception as e:
            logger.error(f"Exception occurred while fetching metrics: {e}")
            raise e

    def get_health_status(self):
        """
        Get health status of Coralogix API.
        
        Returns:
            dict: Health status information
        """
        try:
            # Use the team endpoint as a health check since it's a simple GET request
            url = f'{self.__endpoint}/mgmt/openapi/v1/dashboards/folders'
            response = requests.get(
                url, 
                headers=self.headers, 
                verify=self.__ssl_verify, 
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                raise Exception(
                    f"Failed to get health status. Status Code: {response.status_code}. "
                    f"Response Text: {response.text}"
                )
                
        except Exception as e:
            logger.error(f"Exception occurred while getting health status: {e}")
            raise e


    def fetch_dashboards(self):
        """
        Fetch all custom dashboards from Coralogix.
        
        Returns:
            dict: API response containing dashboards
        """
        try:
            # Use the correct dashboards catalog API endpoint
            url = f'{self.__endpoint}/mgmt/openapi/v1/dashboards/catalog'
            
            response = requests.get(
                url, 
                headers=self.headers, 
                verify=self.__ssl_verify, 
                timeout=60
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                raise Exception(
                    f"Failed to fetch dashboards. Status Code: {response.status_code}. "
                    f"Response Text: {response.text}"
                )
                
        except Exception as e:
            logger.error(f"Exception occurred while fetching dashboards: {e}")
            raise e

    def fetch_dashboard_details(self, dashboard_id: str):
        """
        Fetch detailed information about a specific dashboard including its widgets.
        
        Args:
            dashboard_id: The ID of the dashboard to fetch
            
        Returns:
            dict: API response containing dashboard details and widgets
        """
        try:
            # Use the specific dashboard API endpoint
            url = f'{self.__endpoint}/mgmt/openapi/v1/dashboards/dashboards/{dashboard_id}'
            
            response = requests.get(
                url, 
                headers=self.headers, 
                verify=self.__ssl_verify, 
                timeout=60
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                raise Exception(
                    f"Failed to fetch dashboard details for ID {dashboard_id}. Status Code: {response.status_code}. "
                    f"Response Text: {response.text}"
                )
                
        except Exception as e:
            logger.error(f"Exception occurred while fetching dashboard details for ID {dashboard_id}: {e}")
            raise e

    def fetch_dashboard_widgets(self, dashboard_id: str):
        """
        Fetch all widgets for a specific dashboard.
        
        Args:
            dashboard_id: The ID of the dashboard to fetch widgets for
            
        Returns:
            dict: API response containing dashboard widgets
        """
        try:
            # Use the dashboard widgets API endpoint
            url = f'{self.__endpoint}/mgmt/openapi/v1/dashboards/{dashboard_id}/widgets'
            
            response = requests.get(
                url, 
                headers=self.headers, 
                verify=self.__ssl_verify, 
                timeout=60
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                raise Exception(
                    f"Failed to fetch widgets for dashboard ID {dashboard_id}. Status Code: {response.status_code}. "
                    f"Response Text: {response.text}"
                )
                
        except Exception as e:
            logger.error(f"Exception occurred while fetching widgets for dashboard ID {dashboard_id}: {e}")
            raise e

    def execute_lucene_query(self, query: str, from_time: str = None, to_time: str = None, source_type: str = "logs"):
        """
        Execute a Lucene query using the Coralogix direct query API.
        
        Args:
            query: Lucene query string
            from_time: Start time for the query (e.g., "now-1h", "2024-01-01T00:00:00Z")
            to_time: End time for the query (e.g., "now", "2024-01-01T23:59:59Z")
            source_type: Type of source (logs, metrics, spans)
            
        Returns:
            dict: API response containing query results
        """
        try:
            # Parse time strings to RFC3339 format
            start_time_rfc3339 = self._parse_time_to_rfc3339(from_time or "now-1h")
            end_time_rfc3339 = self._parse_time_to_rfc3339(to_time or "now")
            
            url = f'{self.__endpoint}/api/v1/dataprime/query'
            
            payload = {
                "query": query,
                "metadata": {
                    "tier": "TIER_FREQUENT_SEARCH",
                    "syntax": "QUERY_SYNTAX_LUCENE",
                    "startDate": start_time_rfc3339,
                    "endDate": end_time_rfc3339,
                    "defaultSource": source_type
                }
            }
            
            response = requests.post(
                url, 
                headers=self.headers, 
                json=payload,
                verify=self.__ssl_verify, 
                timeout=60
            )
            
            if response.status_code == 200:
                # Parse NDJSON response format
                lines = response.text.strip().split('\n')
                results = []
                
                for line in lines:
                    if line.strip():
                        line_data = json.loads(line)
                        
                        # Check for results in different possible structures
                        if "result" in line_data and "results" in line_data["result"]:
                            results.extend(line_data["result"]["results"])
                        elif "results" in line_data:
                            results.extend(line_data["results"])
                
                return {"results": results, "count": len(results)}
            else:
                raise Exception(f"Direct query failed with status {response.status_code}: {response.text}")
                
        except Exception as e:
            logger.error(f"Error executing Lucene query: {e}")
            raise

    def execute_logs_query(self, query: str, from_time: str = None, to_time: str = None, limit: int = 1000):
        """
        Execute a logs query using Lucene.
        
        Args:
            query: Logs query string
            from_time: Start time for the query
            to_time: End time for the query
            limit: Maximum number of results
            
        Returns:
            dict: API response containing logs
        """
        try:
            # Add limit to the query if not already present
            if 'limit' not in query.lower():
                query = f"{query} | limit {limit}"
            
            return self.execute_lucene_query(query, from_time, to_time, "logs")
        except Exception as e:
            logger.error(f"Error executing logs query: {e}")
            raise

    def execute_metrics_query(self, query: str, from_time: str = None, to_time: str = None):
        """
        Execute a metrics query using Lucene.
        
        Args:
            query: Metrics query string
            from_time: Start time for the query
            to_time: End time for the query
            
        Returns:
            dict: API response containing metrics
        """
        try:
            return self.execute_lucene_query(query, from_time, to_time, "metrics")
        except Exception as e:
            logger.error(f"Error executing metrics query: {e}")
            raise

    def execute_spans_query(self, query: str, from_time: str = None, to_time: str = None):
        """
        Execute a spans query using Lucene.
        
        Args:
            query: Spans query string
            from_time: Start time for the query
            to_time: End time for the query
            
        Returns:
            dict: API response containing spans
        """
        try:
            return self.execute_lucene_query(query, from_time, to_time, "spans")
        except Exception as e:
            logger.error(f"Error executing spans query: {e}")
            raise

    def execute_widget_query(self, widget_config: dict, from_time: str = None, to_time: str = None):
        """
        Execute a query from a widget configuration.
        This method determines the query type and executes accordingly.
        
        Args:
            widget_config: Widget configuration containing query information
            from_time: Start time for the query
            to_time: End time for the query
            
        Returns:
            dict: API response containing query results
        """
        try:
            widget_type = widget_config.get('type', '')
            query = widget_config.get('query', '')
            
            if not query:
                logger.warning(f"Widget {widget_config.get('id', 'unknown')} has no query")
                return None
            
            # Determine query type based on widget configuration
            if 'logs' in widget_type.lower() or 'log' in widget_type.lower():
                return self.execute_logs_query(query, from_time, to_time)
            elif 'metric' in widget_type.lower():
                return self.execute_metrics_query(query, from_time, to_time)
            elif 'span' in widget_type.lower() or 'trace' in widget_type.lower():
                return self.execute_spans_query(query, from_time, to_time)
            else:
                # Default to logs query if type is unclear
                logger.info(f"Unknown widget type {widget_type}, defaulting to logs query")
                return self.execute_logs_query(query, from_time, to_time)
                
        except Exception as e:
            logger.error(f"Error executing widget query: {e}")
            raise

    def fetch_label_values(self, label_name: str, metric_name: str = None, from_time: str = "now-1h", to_time: str = "now"):
        """
        Fetch label values for a specific label name using the Prometheus-compatible API.
        Based on the curl command: GET /metrics/api/v1/label/{label_name}/values
        
        Args:
            label_name: The label name to get values for
            metric_name: Optional metric name to filter by (using match[] parameter)
            from_time: Start time for the query (e.g., "now-1h", "2023-01-01T00:00:00Z")
            to_time: End time for the query (e.g., "now", "2023-01-01T01:00:00Z")
            
        Returns:
            dict: API response containing label values
        """
        try:
            # Use the metrics endpoint (ng-api-http) for label values
            metrics_endpoint = self.__endpoint.replace('api.', 'ng-api-http.')
            
            # Parse time strings to RFC3339 format for the API
            start_time_rfc3339 = self._parse_time_to_rfc3339(from_time)
            end_time_rfc3339 = self._parse_time_to_rfc3339(to_time)
            
            # Build the URL for label values
            url = f'{metrics_endpoint}/metrics/api/v1/label/{label_name}/values'
            
            # Set up parameters
            params = {
                'start': start_time_rfc3339,
                'end': end_time_rfc3339
            }
            
            # Add metric filter if provided
            if metric_name:
                params['match[]'] = f'{{__name__="{metric_name}"}}'
            
            # Use Bearer token format for authorization
            headers = {
                'Authorization': f'Bearer {self.__api_key}',
                'Accept': 'application/json'
            }
            
            response = requests.get(
                url,
                headers=headers,
                params=params,
                verify=self.__ssl_verify,
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                error_msg = f"Failed to fetch label values for {label_name}. Status Code: {response.status_code}. Response Text: {response.text}"
                raise Exception(error_msg)
                
        except Exception as e:
            logger.error(f"Exception occurred while fetching label values for {label_name}: {e}")
            raise e

    def fetch_labels(self, from_time: str = "now-1h", to_time: str = "now"):
        """
        Fetch all available labels using the Prometheus-compatible API.
        Based on the curl command: GET /metrics/api/v1/labels
        
        Args:
            from_time: Start time for the query (e.g., "now-1h", "2023-01-01T00:00:00Z")
            to_time: End time for the query (e.g., "now", "2023-01-01T01:00:00Z")
            
        Returns:
            dict: API response containing all available labels
        """
        try:
            # Use the metrics endpoint (ng-api-http) for labels
            metrics_endpoint = self.__endpoint.replace('api.', 'ng-api-http.')
            
            # Parse time strings to RFC3339 format for the API
            start_time_rfc3339 = self._parse_time_to_rfc3339(from_time)
            end_time_rfc3339 = self._parse_time_to_rfc3339(to_time)
            
            # Build the URL for labels
            url = f'{metrics_endpoint}/metrics/api/v1/labels'
            
            # Set up parameters
            params = {
                'start': start_time_rfc3339,
                'end': end_time_rfc3339
            }
            
            # Use Bearer token format for authorization
            headers = {
                'Authorization': f'Bearer {self.__api_key}',
                'Accept': 'application/json'
            }
            
            response = requests.get(
                url,
                headers=headers,
                params=params,
                verify=self.__ssl_verify,
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                error_msg = f"Failed to fetch labels. Status Code: {response.status_code}. Response Text: {response.text}"
                raise Exception(error_msg)
                
        except Exception as e:
            logger.error(f"Exception occurred while fetching labels: {e}")
            raise e

    def fetch_index_mappings(self, index_pattern: str = "*"):
        """
        Fetch index mappings from Coralogix using the Elasticsearch-compatible API.
        Based on the curl command: GET /data/os-api/*/_mapping
        
        Args:
            index_pattern: Index pattern to fetch mappings for (default: "*" for all indices)
            
        Returns:
            dict: API response containing index mappings
        """
        try:
            # Use the Elasticsearch-compatible API endpoint for index mappings
            # The endpoint structure is: /data/os-api/{index_pattern}/_mapping
            url = f'{self.__endpoint}/data/os-api/{index_pattern}/_mapping'
            
            # Use Bearer token format for authorization
            headers = {
                'Authorization': f'Bearer {self.__api_key}',
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
            
            response = requests.get(
                url,
                headers=headers,
                verify=self.__ssl_verify,
                timeout=60
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                error_msg = f"Failed to fetch index mappings for pattern '{index_pattern}'. Status Code: {response.status_code}. Response Text: {response.text}"
                raise Exception(error_msg)
                
        except Exception as e:
            logger.error(f"Exception occurred while fetching index mappings for pattern '{index_pattern}': {e}")
            raise e

    def _parse_time_to_rfc3339(self, time_str: str) -> str:
        """
        Convert time string to RFC3339 format for Prometheus API.
        
        Args:
            time_str: Time string like 'now', 'now-1h', 'now-24h', or RFC3339 format
            
        Returns:
            str: RFC3339 formatted time string
        """
        try:
            if time_str == 'now':
                return datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
            elif time_str.startswith('now-'):
                # Parse relative time like 'now-1h', 'now-24h', 'now-30m'
                duration_str = time_str[4:]  # Remove 'now-'
                
                # Parse duration
                if duration_str.endswith('h'):
                    hours = int(duration_str[:-1])
                    target_time = datetime.now() - timedelta(hours=hours)
                elif duration_str.endswith('m'):
                    minutes = int(duration_str[:-1])
                    target_time = datetime.now() - timedelta(minutes=minutes)
                elif duration_str.endswith('d'):
                    days = int(duration_str[:-1])
                    target_time = datetime.now() - timedelta(days=days)
                elif duration_str.endswith('s'):
                    seconds = int(duration_str[:-1])
                    target_time = datetime.now() - timedelta(seconds=seconds)
                else:
                    # Default to hours if no unit specified
                    hours = int(duration_str)
                    target_time = datetime.now() - timedelta(hours=hours)
                
                return target_time.strftime('%Y-%m-%dT%H:%M:%SZ')
            else:
                # Try to parse as RFC3339 or other format
                try:
                    # Try RFC3339 format first
                    dt = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
                    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                except ValueError:
                    # If that fails, try parsing as Unix timestamp
                    dt = datetime.fromtimestamp(float(time_str))
                    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        except Exception as e:
            logger.warning(f"Failed to parse time string '{time_str}', using as-is: {e}")
            return time_str

    def fetch_alert_defs(self):
        """
        Fetch all alert definition configurations from Coralogix using gRPC API.
        
        Uses grpcurl to call the gRPC service:
        com.coralogixapis.alerts.v3.AlertDefsService/ListAlertDefs
        
        Returns:
            dict: API response containing alert definitions
        """
        try:
            # Extract the domain from endpoint (e.g., https://api.eu2.coralogix.com -> api.eu2.coralogix.com)
            endpoint_url = self.__endpoint
            if endpoint_url.startswith('http://'):
                endpoint_url = endpoint_url[7:]
            elif endpoint_url.startswith('https://'):
                endpoint_url = endpoint_url[8:]
            
            # Remove trailing slash and any path
            domain = endpoint_url.split('/')[0]
            
            # Determine the gRPC host (usually api.coralogix.com or api.eu2.coralogix.com)
            # If domain contains 'eu2', use api.eu2.coralogix.com, otherwise use api.coralogix.com
            if 'eu2' in domain.lower() or 'eu' in domain.lower():
                grpc_host = 'api.eu2.coralogix.com'
            else:
                grpc_host = 'api.coralogix.com'
            
            logger.debug(f"Fetching alert definitions from Coralogix gRPC: {grpc_host}:443")
            
            # Build grpcurl command
            service_method = "com.coralogixapis.alerts.v3.AlertDefsService/ListAlertDefs"
            
            # Use grpcurl via subprocess
            cmd = [
                'grpcurl',
                '-H', f'Authorization: Bearer {self.__api_key}',
                '-d', '',  # Empty data for ListAlertDefs
                f'{grpc_host}:443',
                service_method
            ]
            
            try:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=30,
                    check=True
                )
                
                # Parse JSON response from grpcurl
                response_json = json.loads(result.stdout)
                logger.info(f"Successfully fetched alert definitions from Coralogix")
                return response_json
                
            except subprocess.CalledProcessError as e:
                error_msg = f"grpcurl command failed: {e.stderr}"
                logger.error(error_msg)
                raise Exception(f"Failed to fetch alert definitions from Coralogix gRPC API: {error_msg}")
            except subprocess.TimeoutExpired:
                raise Exception("Timeout while fetching alert definitions from Coralogix gRPC API")
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse grpcurl response as JSON: {e}")
                raise Exception(f"Invalid JSON response from Coralogix gRPC API: {e}")
            except FileNotFoundError:
                raise Exception("grpcurl not found. Please install grpcurl to use the alert definitions API: https://github.com/fullstorydev/grpcurl")
                
        except Exception as e:
            logger.error(f"Exception occurred while fetching alert definitions: {e}")
            raise e