import logging
import re

import requests

from core.integrations.processor import Processor
from core.protos.base_pb2 import TimeRange

logger = logging.getLogger(__name__)


class GrafanaApiProcessor(Processor):
    client = None

    def __init__(self, grafana_host, grafana_api_key, ssl_verify='true'):
        self.__host = grafana_host
        self.__api_key = grafana_api_key
        self.__ssl_verify = False if ssl_verify and ssl_verify.lower() == 'false' else True
        self.headers = {
            'Authorization': f'Bearer {self.__api_key}'
        }

    def test_connection(self):
        try:
            url = '{}/api/datasources'.format(self.__host)
            response = requests.get(url, headers=self.headers, verify=self.__ssl_verify, timeout=20)
            if response and response.status_code == 200:
                return True
            else:
                status_code = response.status_code if response else None
                raise Exception(
                    f"Failed to connect with Grafana. Status Code: {status_code}. Response Text: {response.text}")
        except Exception as e:
            logger.error(f"Exception occurred while fetching grafana data sources with error: {e}")
            raise e

    def check_api_health(self):
        try:
            url = '{}/api/health'.format(self.__host)
            response = requests.get(url, headers=self.headers, verify=self.__ssl_verify, timeout=20)
            if response and response.status_code == 200:
                return response.json()
            else:
                status_code = response.status_code if response else None
                raise Exception(
                    f"Failed to connect with Grafana. Status Code: {status_code}. Response Text: {response.text}")
        except Exception as e:
            logger.error(f"Exception occurred while checking grafana api health with error: {e}")
            raise e

    def fetch_data_sources(self):
        try:
            url = '{}/api/datasources'.format(self.__host)
            response = requests.get(url, headers=self.headers, verify=self.__ssl_verify)
            if response and response.status_code == 200:
                return response.json()
        except Exception as e:
            logger.error(f"Exception occurred while fetching grafana data sources with error: {e}")
            raise e

    def fetch_dashboards(self):
        try:
            url = '{}/api/search'.format(self.__host)
            response = requests.get(url, headers=self.headers, verify=self.__ssl_verify)
            if response and response.status_code == 200:
                return response.json()
        except Exception as e:
            logger.error(f"Exception occurred while fetching grafana dashboards with error: {e}")
            raise e

    def fetch_dashboard_details(self, uid):
        try:
            url = '{}/api/dashboards/uid/{}'.format(self.__host, uid)
            response = requests.get(url, headers=self.headers, verify=self.__ssl_verify)
            if response and response.status_code == 200:
                return response.json()
        except Exception as e:
            logger.error(f"Exception occurred while fetching grafana dashboard details with error: {e}")
            raise e

    # Promql Datasource APIs
    def fetch_promql_metric_labels(self, promql_datasource_uid, metric_name):
        try:
            url = '{}/api/datasources/proxy/uid/{}/api/v1/labels?match[]={}'.format(self.__host, promql_datasource_uid,
                                                                                    metric_name)
            response = requests.get(url, headers=self.headers, verify=self.__ssl_verify)
            if response and response.status_code == 200:
                return response.json()
        except Exception as e:
            logger.error(f"Exception occurred while fetching promql metric labels with error: {e}")
            raise e

    def fetch_promql_metric_label_values(self, promql_datasource_uid, metric_name, label_name):
        try:
            url = '{}/api/datasources/proxy/uid/{}/api/v1/label/{}/values?match[]={}'.format(self.__host,
                                                                                             promql_datasource_uid,
                                                                                             label_name, metric_name)
            response = requests.get(url, headers=self.headers, verify=self.__ssl_verify)
            if response and response.status_code == 200:
                return response.json()
        except Exception as e:
            logger.error(f"Exception occurred while fetching promql metric labels with error: {e}")
            raise e

    # TODO(MG): Only kept for backward compatibility. Remove this method.
    def fetch_promql_metric_timeseries(self, promql_datasource_uid, query, start, end, step):
        try:
            url = '{}/api/datasources/proxy/uid/{}/api/v1/query_range?query={}&start={}&end={}&step={}'.format(
                self.__host, promql_datasource_uid, query, start, end, step)
            response = requests.get(url, headers=self.headers, verify=self.__ssl_verify)
            if response and response.status_code == 200:
                return response.json()
        except Exception as e:
            logger.error(f"Exception occurred while getting promql metric timeseries with error: {e}")
            raise e

    def fetch_alert_rules(self):
        try:
            url = '{}/api/v1/provisioning/alert-rules'.format(self.__host)
            response = requests.get(url, headers=self.headers, verify=self.__ssl_verify)
            if response and response.status_code == 200:
                return response.json()
            else:
                raise Exception(
                    f"Failed to fetch alert rules. Status Code: {response.status_code}. Response Text: {response.text}")
        except Exception as e:
            logger.error(f"Exception occurred while fetching grafana alert rules with error: {e}")
            raise e

    def fetch_dashboard_variable_label_values(self, promql_datasource_uid, label_name, metric_match_filter=None):
        try:
            url = f'{self.__host}/api/datasources/proxy/uid/{promql_datasource_uid}/api/v1/label/{label_name}/values'
            params = {}

            if metric_match_filter:
                params['match[]'] = metric_match_filter

            response = requests.get(url, headers=self.headers, params=params, verify=self.__ssl_verify)
            if response and response.status_code == 200:
                return response.json().get('data', [])
            else:
                logger.error(f"Failed to fetch label values for {label_name}. Status: {response.status_code}, Body: {response.text}")
                return []
        except Exception as e:
            logger.error(f"Exception occurred while fetching promql metric labels for {label_name} with error: {e}")
            return []

    def get_datasource_by_uid(self, ds_uid):
        """Fetches datasource details by its UID."""
        try:
            url = f'{self.__host}/api/datasources/uid/{ds_uid}'
            response = requests.get(url, headers=self.headers, verify=self.__ssl_verify)
            if response and response.status_code == 200:
                return response.json()
            logger.error(f"Failed to get datasource for uid {ds_uid}. Status: {response.status_code}, Body: {response.text}")
            return None
        except Exception as e:
            logger.error(f"Exception fetching datasource {ds_uid}: {e}")
            return None

    def get_default_datasource_by_type(self, ds_type):
        """Fetches the default datasource of a given type."""
        try:
            datasources = self.fetch_data_sources()
            if not datasources:
                return None
            
            # Find the default datasource of the specified type
            for ds in datasources:
                if ds.get('type') == ds_type and ds.get('isDefault', False):
                    return ds
            # If no default found, return the first one of that type
            for ds in datasources:
                if ds.get('type') == ds_type:
                    return ds
            return None
        except Exception as e:
            logger.error(f"Exception fetching default datasource of type {ds_type}: {e}")
            return None

    def get_dashboard_variables(self, dashboard_uid):
        """
        Fetches and resolves all variables for a given Grafana dashboard.
        Handles dependencies between variables and supports multiple variable types.
        """
        import re
        
        try:
            dashboard_data = self.fetch_dashboard_details(dashboard_uid)
            
            if not dashboard_data or 'dashboard' not in dashboard_data:
                logger.error("Could not fetch or parse dashboard data.")
                return {}

            dashboard_json = dashboard_data['dashboard']
            variables = dashboard_json.get('templating', {}).get('list', [])
            
            resolved_variables = {}

            for var in variables:
                var_name = var.get('name')
                var_type = var.get('type')

                if not var_name or not var_type:
                    continue
                
                values = []
                if var_type == 'query':
                    values = self._resolve_query_variable(var, resolved_variables)
                elif var_type == 'datasource':
                    values = self._resolve_datasource_variable(var)
                elif var_type == 'custom':
                    query = self._substitute_variables(var.get('query', ''), resolved_variables)
                    values = [v.strip() for v in query.split(',')]
                elif var_type == 'constant':
                    values = [self._substitute_variables(var.get('query', ''), resolved_variables)]
                elif var_type == 'textbox':
                    current_val = var.get('current', {}).get('value')
                    query_val = self._substitute_variables(var.get('query', ''), resolved_variables)
                    values = [current_val or query_val]
                elif var_type == 'interval':
                    query = self._substitute_variables(var.get('query', ''), resolved_variables)
                    values = [v.strip() for v in query.split(',')]
                
                if values:
                    resolved_variables[var_name] = values

            logger.info(f"For dashboard '{dashboard_json.get('title')}', fetched variable values: {resolved_variables}")
            return {
                'dashboard_title': dashboard_json.get('title'),
                'dashboard_uid': dashboard_uid,
                'variables': resolved_variables
            }
        except Exception as e:
            logger.error(f"Exception occurred while fetching dashboard variables for {dashboard_uid}: {e}")
            return {}

    def _substitute_variables(self, query_string, resolved_variables):
        """Substitutes variables in query strings."""
        import re
        
        for name, value in resolved_variables.items():
            sub_value = value[0] if isinstance(value, list) and value else (value if isinstance(value, str) else "")
            query_string = re.sub(r'\$' + re.escape(name) + r'\b', sub_value, query_string)
            query_string = re.sub(r'\$\{' + re.escape(name) + r'\}', sub_value, query_string)
        return query_string

    def _resolve_datasource_variable(self, var):
        """Resolves a 'datasource' type variable."""
        ds_type = var.get('query')
        if not ds_type:
            return []
        
        try:
            datasources = self.fetch_data_sources()
            if not datasources:
                return []
            
            # Get all datasources of this type
            matching_datasources = [ds['uid'] for ds in datasources if ds.get('type') == ds_type]
            
            # If the current value is 'default', we should return the UID of the default datasource
            current_value = var.get('current', {}).get('value')
            if current_value == 'default':
                default_ds = self.get_default_datasource_by_type(ds_type)
                if default_ds:
                    return [default_ds['uid']]
            
            return matching_datasources
        except Exception as e:
            logger.error(f"Exception fetching datasources: {e}")
            return []

    def _get_label_match_filter(self, query_string):
        """
        Convert label_values queries by replacing variable references with regex wildcards.
        """
        label_values_pattern = r'label_values\(([^)]+),\s*[^)]+\)'
        
        match = re.search(label_values_pattern, query_string)
        if not match:
            return query_string
        
        # Extract the metric part (everything before the last comma)
        full_match = match.group(1)
        
        # Replace all variable references ($variable) with ".*"
        # This pattern matches $followed by word characters (letters, digits, underscore)
        variable_pattern = r'\"\$\w+\"'
        converted_metric = re.sub(variable_pattern, '".*"', full_match)

        return converted_metric

    def _resolve_query_variable(self, var, resolved_variables):
        """
        Resolves a 'query' type variable.
        Currently supports Prometheus datasources.
        """
        import re
        
        datasource = var.get('datasource')
        query = var.get('query')

        if not datasource or not query:
            return []

        ds_uid = datasource.get('uid') if isinstance(datasource, dict) else datasource
        ds_uid = self._substitute_variables(ds_uid, resolved_variables)

        # Handle the case where ds_uid is "default" - need to resolve it to actual datasource
        if ds_uid == 'default':
            ds_type = datasource.get('type') if isinstance(datasource, dict) else 'prometheus'  # assume prometheus if not specified
            datasource_details = self.get_default_datasource_by_type(ds_type)
            if datasource_details:
                ds_uid = datasource_details['uid']
            else:
                logger.warning(f"Could not find default datasource of type '{ds_type}' for query variable '{var.get('name')}'.")
                return []
        else:
            datasource_details = self.get_datasource_by_uid(ds_uid)

        if not datasource_details or datasource_details.get('type') != 'prometheus':
            logger.warning(f"Unsupported or unknown datasource type for query variable '{var.get('name')}'.")
            return []
        
        query = self._substitute_variables(str(query), resolved_variables)

        # Prometheus query handling
        # Case 1: label_values(label) or label_values(metric, label)
        label_values_match = re.search(r'label_values\((?:.*\s*,\s*)?(\w+)\)', query)
        if label_values_match:
            label = label_values_match.group(1)
            return self.fetch_dashboard_variable_label_values(ds_uid, label, self._get_label_match_filter(query))

        # Case 2: metrics(pattern) -> label_values(__name__)
        if re.match(r'metrics\(.*\)', query):
            return self.fetch_dashboard_variable_label_values(ds_uid, '__name__')

        # Case 3: Generic PromQL query (including query_result(query))
        try:
            if query.startswith('query_result(') and query.endswith(')'):
                query = query[len('query_result('):-1]
            
            url = f'{self.__host}/api/datasources/proxy/uid/{ds_uid}/api/v1/query'
            params = {'query': query}
            response = requests.get(url, headers=self.headers, params=params, verify=self.__ssl_verify)
            if response and response.status_code == 200:
                results = response.json().get('data', {}).get('result', [])
                values = []
                for res in results:
                    metric_labels = res.get('metric', {})
                    metric_str = "{" + ", ".join([f'{k}="{v}"' for k,v in metric_labels.items()]) + "}"
                    
                    if 'regex' in var and var['regex']:
                        match = re.search(var['regex'], metric_str)
                        if match:
                            values.append(match.group(1) if len(match.groups()) > 0 else match.group(0))
                    else:
                        # Default behavior: extract value of a label if there is one other than __name__
                        # otherwise, the __name__
                        non_name_labels = {k: v for k, v in metric_labels.items() if k != '__name__'}
                        if len(non_name_labels) == 1:
                            values.append(list(non_name_labels.values())[0])
                        else:
                            values.append(metric_labels.get('__name__', metric_str))
                return sorted(list(set(values)))
            else:
                logger.error(f"Query failed for '{query}'. Status: {response.status_code}, Body: {response.text}")
                return []
        except Exception as e:
            logger.error(f"Exception during generic query execution for '{query}': {e}")
            return []

    def panel_query_datasource_api(self, tr: TimeRange, queries, interval_ms=300000):
        try:
            if not queries or len(queries) == 0:
                raise ValueError("No queries provided.")

            url = f"{self.__host}/api/ds/query"

            from_tr = int(tr.time_geq * 1000)
            to_tr = int(tr.time_lt * 1000)

            for query in queries:
                query["intervalMs"] = interval_ms # 5 minutes default, in milliseconds

            payload = {
                "queries": queries,
                "from": str(from_tr),
                "to": str(to_tr)
            }

            response = requests.post(url, headers=self.headers, json=payload)

            if response.status_code == 429:
                logger.info("Grafana query API responded with 429 (rate limited). Headers: %s", response.headers)
                return None

            response.raise_for_status()

            return response.json()
        except requests.exceptions.HTTPError as e:
            logger.error("Grafana query API error: status code %s, response: %s", e.response.status_code, e.response.text)
            raise e
        except Exception as e:
            logger.error("Exception occurred while querying Grafana datasource: %s", e)
            raise e
