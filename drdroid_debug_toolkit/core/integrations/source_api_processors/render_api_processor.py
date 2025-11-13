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
        """Fetch logs for a specific service."""
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
            
            # Time parameters might be required
            if start_time:
                params['startTime'] = start_time
            else:
                # If no start time provided, use a default (e.g., 1 hour ago)
                import datetime
                from datetime import timezone
                default_start = datetime.datetime.now(timezone.utc) - datetime.timedelta(hours=1)
                # Use ISO 8601 format as required by Render API
                params['startTime'] = default_start.strftime('%Y-%m-%dT%H:%M:%SZ')
            
            if end_time:
                params['endTime'] = end_time
            else:
                # If no end time provided, use current time
                import datetime
                from datetime import timezone
                # Use ISO 8601 format as required by Render API
                params['endTime'] = datetime.datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            
            if limit:
                params['limit'] = limit
            
            # Add filter parameters (arrays are passed as lists, requests handles them correctly)
            if instance:
                params['instance'] = instance if isinstance(instance, list) else [instance]
            if host:
                params['host'] = host if isinstance(host, list) else [host]
            if status_code:
                params['statusCode'] = status_code if isinstance(status_code, list) else [status_code]
            if method:
                params['method'] = method if isinstance(method, list) else [method]
            if task:
                params['task'] = task if isinstance(task, list) else [task]
            if task_run:
                params['taskRun'] = task_run if isinstance(task_run, list) else [task_run]
            if level:
                params['level'] = level if isinstance(level, list) else [level]
            if type:
                params['type'] = type if isinstance(type, list) else [type]
            if text:
                params['text'] = text if isinstance(text, list) else [text]
            if path:
                params['path'] = path if isinstance(path, list) else [path]
            
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
