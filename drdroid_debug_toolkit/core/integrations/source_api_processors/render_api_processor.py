import logging
import requests
from urllib.parse import urlencode
from core.integrations.processor import Processor

logger = logging.getLogger(__name__)


class RenderAPIProcessor(Processor):
    def __init__(self, api_key):
        self.__api_key = api_key
        self.base_url = 'https://api.render.com/v1'
        self.headers = {
            'Authorization': f'Bearer {self.__api_key}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }

    def list_deploys(self, service_id):
        """List deployment history for a service."""
        try:
            url = f"{self.base_url}/services/{service_id}/deploys"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error listing deploys for service {service_id}: {e}")
            raise Exception(f"Failed to list deploys: {e}")

    def get_deploy(self, service_id, deploy_id):
        """Get details about a specific deployment."""
        try:
            url = f"{self.base_url}/services/{service_id}/deploys/{deploy_id}"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error getting deploy {deploy_id} for service {service_id}: {e}")
            raise Exception(f"Failed to get deploy: {e}")

    def list_services(self, include_previews=False):
        """List all services in your Render account."""
        try:
            url = f"{self.base_url}/services"
            
            # Try without parameters first (most Render API endpoints don't support include_previews)
            response = requests.get(url, headers=self.headers)
            
            # If that fails, try with the parameter
            if response.status_code == 400 and include_previews:
                response = requests.get(url, headers=self.headers)
            
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error listing services: {e}")
            raise Exception(f"Failed to list services: {e}")

    def get_service(self, service_id):
        """Get details about a specific service."""
        try:
            url = f"{self.base_url}/services/{service_id}"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error getting service {service_id}: {e}")
            raise Exception(f"Failed to get service: {e}")

    def fetch_logs(self, service_id, start_time=None, end_time=None, limit=None,
                   instance=None, host=None, status_code=None, method=None,
                   task=None, task_run=None, level=None, type=None, text=None, path=None):
        """Fetch logs for a specific service.
        
        According to Render API docs: https://api-docs.render.com/reference/list-logs
        - startTime and endTime must be Unix timestamps (epoch seconds)
        - All filter parameters are arrays of strings
        """
        try:
            # First, get the service details to extract the ownerId
            service_url = f"{self.base_url}/services/{service_id}"
            service_response = requests.get(service_url, headers=self.headers)
            service_response.raise_for_status()
            service_data = service_response.json()
            
            # Extract ownerId from service data
            owner_id = service_data.get('ownerId')
            if not owner_id:
                raise Exception("Could not find ownerId in service data")
            
            # Now call the logs API with the required ownerId
            url = f"{self.base_url}/logs"
            params = {
                'ownerId': owner_id,
                'resource': [service_id]  # resource is required and should be an array
            }
            
            # Helper function to convert time to ISO 8601 format
            # Render API expects ISO 8601 format strings (e.g., "2006-01-02T15:04:05Z07:00")
            def to_iso8601(time_value):
                """Convert various time formats to ISO 8601 string."""
                if not time_value:
                    return None
                import datetime
                from datetime import timezone
                if isinstance(time_value, str):
                    # If already ISO 8601 format, try to parse and normalize it
                    try:
                        # Handle Z suffix
                        time_str = time_value.replace('Z', '+00:00')
                        dt = datetime.datetime.fromisoformat(time_str)
                        # Ensure timezone-aware
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        # Format as ISO 8601 with Z suffix
                        return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                    except:
                        # If parsing fails, try to parse as Unix timestamp
                        try:
                            ts = float(time_value)
                            # If timestamp is very large, assume milliseconds, else seconds
                            if ts > 1e10:
                                dt = datetime.datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
                            else:
                                dt = datetime.datetime.fromtimestamp(ts, tz=timezone.utc)
                            return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                        except:
                            # Return as-is if we can't parse it
                            return time_value
                elif isinstance(time_value, (int, float)):
                    # Convert Unix timestamp to ISO 8601
                    # If timestamp is very large, assume milliseconds, else seconds
                    if time_value > 1e10:
                        dt = datetime.datetime.fromtimestamp(time_value / 1000, tz=timezone.utc)
                    else:
                        dt = datetime.datetime.fromtimestamp(time_value, tz=timezone.utc)
                    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                return None
            
            # Time parameters - Render API expects ISO 8601 format strings
            if start_time:
                start_iso = to_iso8601(start_time)
                if start_iso:
                    params['startTime'] = start_iso
            else:
                # Default: 1 hour ago
                import datetime
                from datetime import timezone
                default_start = datetime.datetime.now(timezone.utc) - datetime.timedelta(hours=1)
                params['startTime'] = default_start.strftime('%Y-%m-%dT%H:%M:%SZ')
            
            if end_time:
                end_iso = to_iso8601(end_time)
                if end_iso:
                    params['endTime'] = end_iso
            else:
                # Default: now
                import datetime
                from datetime import timezone
                params['endTime'] = datetime.datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            
            if limit:
                params['limit'] = limit
            
            # Helper function to convert filter values to arrays
            def to_filter_array(value):
                """Convert value to array of strings for filter parameters."""
                if not value:
                    return None
                if isinstance(value, list):
                    # Filter out empty values and convert to strings
                    filtered = [str(v).strip() for v in value if v]
                    return filtered if filtered else None
                if isinstance(value, str):
                    # Handle comma-separated strings
                    values = [v.strip() for v in value.split(',') if v.strip()]
                    return values if values else None
                return [str(value).strip()] if value else None
            
            # Add filter parameters - all are arrays of strings according to API docs
            if instance:
                instance_array = to_filter_array(instance)
                if instance_array:
                    params['instance'] = instance_array
            if host:
                host_array = to_filter_array(host)
                if host_array:
                    params['host'] = host_array
            if status_code:
                status_code_array = to_filter_array(status_code)
                if status_code_array:
                    params['statusCode'] = status_code_array
            if method:
                method_array = to_filter_array(method)
                if method_array:
                    params['method'] = method_array
            if task:
                task_array = to_filter_array(task)
                if task_array:
                    params['task'] = task_array
            if task_run:
                task_run_array = to_filter_array(task_run)
                if task_run_array:
                    params['taskRun'] = task_run_array
            if level:
                level_array = to_filter_array(level)
                if level_array:
                    params['level'] = level_array
            if type:
                type_array = to_filter_array(type)
                if type_array:
                    params['type'] = type_array
            if text:
                text_array = to_filter_array(text)
                if text_array:
                    params['text'] = text_array
            if path:
                path_array = to_filter_array(path)
                if path_array:
                    params['path'] = path_array
            
            # Use urlencode with doseq=True to properly handle arrays
            # This creates multiple query parameters: instance=val1&instance=val2
            # which is what the Render API expects for array parameters
            query_string = urlencode(params, doseq=True)
            full_url = f"{url}?{query_string}"
            
            logger.debug(f"Fetching logs from Render API")
            logger.debug(f"URL: {full_url}")
            logger.debug(f"Params dict: {params}")
            
            response = requests.get(full_url, headers=self.headers)
            
            if response.status_code != 200:
                logger.error(f"Render API error {response.status_code}: {response.text}")
                logger.error(f"Request URL: {full_url}")
                logger.error(f"Request params: {params}")
            
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Error fetching logs for service {service_id}: {e}")
            raise Exception(f"Failed to fetch logs: {e}")

    def test_connection(self):
        """Test the connection to Render API by making a simple request."""
        try:
            # Use the list_services endpoint to test the connection
            # This is a lightweight call that should work with any valid API key
            url = f"{self.base_url}/services"
            response = requests.get(url, headers=self.headers)
            
            if response.status_code == 200:
                return True
            elif response.status_code == 401:
                raise Exception("Render API connection failed: Invalid API key")
            elif response.status_code == 403:
                raise Exception("Render API connection failed: API key lacks required permissions")
            else:
                raise Exception(f"Render API connection failed: {response.status_code}, {response.text}")
        except requests.exceptions.RequestException as e:
            logger.error(f"RenderAPIProcessor.test_connection:: Network error occurred: {e}")
            raise Exception(f"Network error: {e}")
        except Exception as e:
            logger.error(f"RenderAPIProcessor.test_connection:: Exception occurred with error: {e}")
            raise e
