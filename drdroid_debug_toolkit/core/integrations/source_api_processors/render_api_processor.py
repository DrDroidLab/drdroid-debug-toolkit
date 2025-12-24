import logging
import requests
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
            
            # Helper function to convert time to Unix timestamp
            def to_unix_timestamp(time_value):
                """Convert ISO 8601 string or datetime to Unix timestamp (seconds)."""
                if not time_value:
                    return None
                import datetime
                from datetime import timezone
                if isinstance(time_value, str):
                    # Try parsing ISO 8601 format
                    try:
                        dt = datetime.datetime.fromisoformat(time_value.replace('Z', '+00:00'))
                        return int(dt.timestamp())
                    except:
                        # If parsing fails, assume it's already a timestamp string
                        try:
                            return int(float(time_value))
                        except:
                            return None
                elif isinstance(time_value, (int, float)):
                    return int(time_value)
                return None
            
            # Time parameters - Render API expects Unix timestamps (epoch seconds)
            if start_time:
                start_ts = to_unix_timestamp(start_time)
                if start_ts:
                    params['startTime'] = start_ts
            else:
                # Default: 1 hour ago
                import datetime
                from datetime import timezone
                default_start = datetime.datetime.now(timezone.utc) - datetime.timedelta(hours=1)
                params['startTime'] = int(default_start.timestamp())
            
            if end_time:
                end_ts = to_unix_timestamp(end_time)
                if end_ts:
                    params['endTime'] = end_ts
            else:
                # Default: now
                import datetime
                from datetime import timezone
                params['endTime'] = int(datetime.datetime.now(timezone.utc).timestamp())
            
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
            
            response = requests.get(url, headers=self.headers, params=params)
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
