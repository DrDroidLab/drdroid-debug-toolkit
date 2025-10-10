from datetime import datetime
import json
import logging
import requests
from requests.auth import HTTPBasicAuth

from core.integrations.processor import Processor
from core.settings import EXTERNAL_CALL_TIMEOUT

logger = logging.getLogger(__name__)


class JenkinsAPIProcessor(Processor):
    def __init__(self, url, username, api_token=None, crumb=False):
        self.config = {
            'url': url
        }
        self.username = username
        self.api_token = api_token
        self.crumb_enabled = crumb
        self.crumb = None
        self.crumb_header = None

        # For API token auth
        if not self.crumb_enabled:
            if not api_token:
                raise Exception("API Token is required when not using crumb authentication")
            self.auth = HTTPBasicAuth(username, api_token)
        else:
            # For crumb-based auth, we'll still use basic auth with username
            self.auth = HTTPBasicAuth(username, api_token if api_token else "")

    def _make_request(self, method, url, **kwargs):
        """
        Helper method to make HTTP requests with proper authentication
        """
        try:
            headers = kwargs.pop('headers', {})
            # Extract timeout from kwargs if provided, otherwise use default
            timeout = kwargs.pop('timeout', EXTERNAL_CALL_TIMEOUT)

            # Add crumb header if using crumb authentication
            if self.crumb_enabled:
                if not self.crumb:
                    self.fetch_crumb()
                if self.crumb and self.crumb_header:
                    headers[self.crumb_header] = self.crumb
            kwargs['headers'] = headers
            kwargs['auth'] = self.auth
            response = requests.request(method, url, timeout=timeout, **kwargs)

            # Handle crumb expiration
            if response.status_code == 403 and self.crumb_enabled:
                # Refresh crumb and retry once
                self.crumb = None
                self.fetch_crumb()
                if self.crumb and self.crumb_header:
                    headers[self.crumb_header] = self.crumb
                    kwargs['headers'] = headers
                    response = requests.request(method, url, timeout=timeout, **kwargs)

            return response
        except Exception as e:
            logger.error(f"Request failed: {e}")
            raise e

    def test_connection(self):
        try:
            url = f"{self.config['url']}/api/json"
            response = self._make_request('GET', url)
            response.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"Exception occurred while testing Jenkins connection with error: {e}")
            raise e

    def fetch_crumb(self):
        """
        Fetch the Jenkins crumb for CSRF protection.
        """
        try:
            url = f"{self.config['url']}/crumbIssuer/api/json"
            # Use direct request here to avoid recursive _make_request call
            response = requests.get(url, auth=self.auth, timeout=EXTERNAL_CALL_TIMEOUT)

            if response.status_code == 200:
                crumb_data = response.json()
                self.crumb = crumb_data.get("crumb")
                self.crumb_header = crumb_data.get("crumbRequestField")
                return self.crumb
            else:
                logger.error(f"Failed to fetch Jenkins crumb: {response.status_code}, {response.text}")
                return None

        except Exception as e:
            logger.error(f"Error fetching crumb: {e}")
            return None

    def run_job(self, job_name, parameters=None, timeout=120):
        """
        Trigger a Jenkins job build.
        Args:
            job_name: Full path of the job to run (e.g., folder/job or nested/folder/job)
            parameters: Optional dictionary of build parameters
            timeout: Request timeout in seconds
        """
        try:
            # Construct the job path URL properly
            path_segments = job_name.split('/')
            url_path = ""
            for segment in path_segments:
                if segment:  # Skip empty segments
                    url_path += f"/job/{requests.utils.quote(segment)}"
            
            # Construct URL based on whether parameters are provided
            if parameters and isinstance(parameters, dict) and parameters:  # Check if parameters is a non-empty dict
                url = f"{self.config['url']}{url_path}/buildWithParameters"
                response = self._make_request('POST', url, data=parameters, timeout=timeout)
            else:
                url = f"{self.config['url']}{url_path}/build?delay=0sec"
                response = self._make_request('POST', url, data='', timeout=timeout)
            
            if response.status_code == 201:
                return True
            else:
                raise Exception(f"Failed to trigger job build: {response.status_code}, {response.text}")
        except Exception as e:
            logger.error(f"Exception occurred while triggering Jenkins job with error: {e}")
            raise e

    def get_all_jobs_recursive(self, folder_path="", timeout=120, depth=0, max_depth=10):
        """
        Recursively get all jobs, including those in folders.
        
        Args:
            folder_path: The path to the folder (empty for root)
            timeout: Request timeout in seconds
            depth: Current recursion depth (used internally)
            max_depth: Maximum recursion depth to prevent infinite loops
            
        Returns:
            List of jobs with their full paths and metadata
        """
        # Guard against too much recursion
        if depth > max_depth:
            print(f"Maximum recursion depth reached at {folder_path}. Stopping recursion.", flush=True)
            return []
        try:
            # Build the URL based on whether we're checking root or a folder
            if folder_path:
                # For folder paths, construct the URL properly with /job/ between each component
                path_segments = folder_path.split('/')
                url_path = ""
                for segment in path_segments:
                    if segment:  # Skip empty segments
                        url_path += f"/job/{requests.utils.quote(segment)}"
                
                url = f"{self.config['url']}{url_path}/api/json?tree=jobs[name,url,_class]"
                print(f"Fetching jobs from: {url}", flush=True)
            else:
                url = f"{self.config['url']}/api/json?tree=jobs[name,url,_class]"
                
            response = self._make_request('GET', url, timeout=timeout)
            
            if response.status_code != 200:
                logger.error(f"Failed to fetch Jenkins jobs: {response.status_code}, {response.text}")
                return []
                
            data = response.json()
            jobs = data.get('jobs', [])
            result = []
            
            for job in jobs:
                job_name = job.get("name", "")
                job_class = job.get("_class", "")
                
                # Build the full path for this job
                full_path = f"{folder_path}/{job_name}" if folder_path else job_name
                
                # Add this job to our result list
                result.append({
                    "name": job_name,
                    "class": job_class,
                    "full_path": full_path
                })
                
                # If this is a folder, recursively get its jobs
                if "folder" in job_class.lower() or "directory" in job_class.lower():
                    print(f"Found folder: {full_path}", flush=True)
                    # Add a debug print to help with troubleshooting
                    nested_jobs = self.get_all_jobs_recursive(full_path, timeout, depth=depth+1, max_depth=max_depth)
                    if nested_jobs:
                        print(f"Found {len(nested_jobs)} nested jobs in {full_path}", flush=True)
                    else:
                        print(f"No nested jobs found in {full_path}", flush=True)
                    result.extend(nested_jobs)
                    
            return result
        except Exception as e:
            logger.error(f"Error fetching jobs recursively: {e}")
            return []
    
    def get_job_parameters(self, job_name, timeout=120):
        """Get parameters for a specific job.
        
        Args:
            job_name: Full path of the job (e.g., folder/job or nested/folder/job)
            timeout: Request timeout in seconds
            
        Returns:
            List of parameter definitions with name, type, default value, etc.
        """
        try:
            # Construct the job path URL properly
            path_segments = job_name.split('/')
            url_path = ""
            for segment in path_segments:
                if segment:  # Skip empty segments
                    url_path += f"/job/{requests.utils.quote(segment)}"
                    
            url = f"{self.config['url']}{url_path}/api/json"
            response = self._make_request('GET', url, timeout=timeout)

            if response.status_code == 200:
                data = response.json()
                property_list = data.get('property', [])
                for prop in property_list:
                    if 'parameterDefinitions' in prop:
                        return prop['parameterDefinitions']
                return []  # No parameters found
            else:
                logger.error(f"Failed to get job parameters: {response.status_code}, {response.text}")
                return []
        except Exception as e:
            logger.error(f"Exception occurred while getting job parameters with error: {e}")
            return []

    def get_all_branches(self, job_name, timeout=120):
        """Get all branches for a multibranch pipeline job.
        
        Args:
            job_name: Full path of the multibranch job (e.g., folder/job or nested/folder/job)
            timeout: Request timeout in seconds
            
        Returns:
            List of branch information
        """
        try:
            # Construct the job path URL properly
            path_segments = job_name.split('/')
            url_path = ""
            for segment in path_segments:
                if segment:  # Skip empty segments
                    url_path += f"/job/{requests.utils.quote(segment)}"
            
            base_url = f"{self.config['url']}{url_path}"
            
            # Check if this is a multibranch job
            job_info_url = f"{base_url}/api/json?tree=_class,jobs[name,lastBuild[number,timestamp,result]]"
            job_info_response = self._make_request('GET', job_info_url, timeout=timeout)
            
            if job_info_response.status_code != 200:
                logger.error(f"Failed to fetch job info: {job_info_response.status_code}, {job_info_response.text}")
                raise Exception(f"Failed to fetch job info: {job_info_response.status_code}, {job_info_response.text}")
            
            job_data = job_info_response.json()
            job_class = job_data.get('_class', '')
            
            # Check if this is a multibranch pipeline job
            is_multibranch = (
                'multibranch' in job_class.lower() or 
                'WorkflowMultiBranchProject' in job_class or
                'MultiBranchProject' in job_class
            )
            
            if not is_multibranch:
                raise Exception(f"Job '{job_name}' is not a multibranch pipeline job (class: {job_class})")
            
            # Get all branches
            sub_jobs = job_data.get('jobs', [])
            branches = []
            
            for branch in sub_jobs:
                branch_name = branch.get('name', '')
                last_build = branch.get('lastBuild')
                
                branch_info = {
                    'Branch Name': branch_name,
                    'Has Builds': 'Yes' if last_build else 'No',
                    'Last Build Number': last_build.get('number', 'N/A') if last_build else 'N/A',
                    'Last Build Status': last_build.get('result', 'N/A') if last_build else 'N/A',
                    'Last Build Time': datetime.fromtimestamp(last_build.get('timestamp', 0) / 1000.0).strftime('%Y-%m-%d %H:%M:%S') if last_build and last_build.get('timestamp') else 'N/A'
                }
                branches.append(branch_info)
            
            logger.info(f"Found {len(branches)} branches in multibranch job '{job_name}'")
            return branches
            
        except Exception as e:
            logger.error(f"Exception occurred while fetching branches with error: {e}")
            raise e

    def get_last_build(self, job_name, branch_name=None, timeout=120):
        """Get the last build details for a job.
        Automatically detects job type using the _class attribute.
        For regular jobs: returns the last build details.
        For multibranch pipeline jobs: requires branch_name parameter.
        
        Args:
            job_name: Full path of the job (e.g., folder/job or nested/folder/job)
            branch_name: Optional branch name for multibranch jobs
            timeout: Request timeout in seconds
            
        Returns:
            List of build details
        """
        try:
            # Construct the job path URL properly
            path_segments = job_name.split('/')
            url_path = ""
            for segment in path_segments:
                if segment:  # Skip empty segments
                    url_path += f"/job/{requests.utils.quote(segment)}"
            
            base_url = f"{self.config['url']}{url_path}"
            
            # Step 1: Check job type using the _class attribute
            job_info_url = f"{base_url}/api/json?tree=_class,jobs[name,lastBuild[number,timestamp,result]]"
            job_info_response = self._make_request('GET', job_info_url, timeout=timeout)
            
            if job_info_response.status_code != 200:
                logger.error(f"Failed to fetch job info: {job_info_response.status_code}, {job_info_response.text}")
                raise Exception(f"Failed to fetch job info: {job_info_response.status_code}, {job_info_response.text}")
            
            job_data = job_info_response.json()
            job_class = job_data.get('_class', '')
            
            # Check if this is a multibranch pipeline job using the class name
            is_multibranch = (
                'multibranch' in job_class.lower() or 
                'WorkflowMultiBranchProject' in job_class or
                'MultiBranchProject' in job_class
            )
            
            if not is_multibranch:
                # This is a regular job
                logger.info(f"Detected regular job (class: {job_class})")
                return self._get_regular_job_build(job_name, url_path, timeout)
            
            # Step 2: This is a multibranch job - require branch_name parameter
            logger.info(f"Detected multibranch job (class: {job_class})")
            
            if not branch_name:
                raise Exception("Branch name must be specified for multibranch pipeline jobs. Use the 'Get All Branches' task to see available branches.")
            
            # Step 3: Get build details for the specific branch
            return self._get_specific_branch_build(job_name, url_path, branch_name, timeout)
            
        except Exception as e:
            logger.error(f"Exception occurred while fetching Jenkins last build with error: {e}")
            raise e

    def _get_regular_job_build(self, job_name, url_path, timeout):
        """Get build details for a regular (non-multibranch) job."""
        try:
            url = f"{self.config['url']}{url_path}/lastBuild/api/json"
            response = self._make_request('GET', url, timeout=timeout)
            
            if response.status_code != 200:
                logger.error(f"Failed to fetch Jenkins last build: {response.status_code}, {response.text}")
                raise Exception(f"Failed to fetch Jenkins last build: {response.status_code}, {response.text}")
            
            data = response.json()
            return self._format_build_result(data, job_name, url_path, timeout)
            
        except Exception as e:
            logger.error(f"Error fetching regular job build: {e}")
            raise e

    def _get_specific_branch_build(self, job_name, url_path, branch_name, timeout):
        """Get build details for a specific branch in a multibranch job."""
        try:
            # Construct the branch-specific URL path
            branch_url_path = f"{url_path}/job/{requests.utils.quote(branch_name)}"
            detailed_build_url = f"{self.config['url']}{branch_url_path}/lastBuild/api/json"
            
            logger.info(f"Fetching build details for branch '{branch_name}' in job '{job_name}'")
            
            response = self._make_request('GET', detailed_build_url, timeout=timeout)
            if response.status_code != 200:
                if response.status_code == 404:
                    raise Exception(f"Branch '{branch_name}' not found in multibranch job '{job_name}'")
                else:
                    raise Exception(f"Failed to fetch build details for branch '{branch_name}': {response.status_code}, {response.text}")
            
            data = response.json()
            build_result = self._format_build_result(data, f"{job_name}/{branch_name}", branch_url_path, timeout)
            
            # Add branch information to the result
            if build_result:
                build_result[0]['Branch'] = branch_name
                build_result[0]['Job Type'] = 'Multibranch Pipeline'
            
            return build_result
            
        except Exception as e:
            logger.error(f"Error fetching build details for branch {branch_name}: {e}")
            raise e

    def _format_build_result(self, data, job_name, url_path, timeout):
        """Format build data into the expected result format."""
        try:
            build_time = data.get('timestamp')
            number = data.get('number', None)
            status = data.get('result', None)
            actions = data.get('actions', [])
            build_parameters = {}
            
            for action in actions:
                if 'parameters' in action:
                    for param in action['parameters']:
                        if 'name' in param and 'value' in param:
                            build_parameters[param['name']] = param['value']
                        elif 'name' in param:
                            build_parameters[param['name']] = None
            
            # Fetch logs
            logs = ""
            if number:
                logs_url = f"{self.config['url']}{url_path}/{number}/consoleText"
                logs_response = self._make_request('GET', logs_url, timeout=timeout)
                if logs_response.status_code == 200:
                    logs = logs_response.text
            
            return [{
                'Time': datetime.fromtimestamp(build_time / 1000.0).strftime('%Y-%m-%d %H:%M:%S') if build_time else 'N/A',
                'Build #': number,
                'Status': status,
                'Parameters': json.dumps(build_parameters, indent=2) if build_parameters else 'No parameters',
                'Logs': logs
            }]
            
        except Exception as e:
            logger.error(f"Error formatting build result: {e}")
            return [{
                'Time': 'N/A',
                'Build #': 'N/A',
                'Status': 'Error',
                'Parameters': 'Error formatting result',
                'Logs': f'Error: {str(e)}'
            }]
