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
            'Authorization': f'Bearer {self.__api_key}',
            'Content-Type': 'application/json'
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
        """
        Fetches alert rules from Grafana using two different APIs:
        1. Ruler API (/api/ruler/grafana/api/v1/rules) - Returns all alert rules including UI-created ones
        2. Provisioning API (/api/v1/provisioning/alert-rules) - Returns only provisioned rules (fallback)
        """
        try:
            # First try the Ruler API which returns all alert rules (including UI-created)
            url = '{}/api/ruler/grafana/api/v1/rules'.format(self.__host)
            response = requests.get(url, headers=self.headers, verify=self.__ssl_verify)
            if response and response.status_code == 200:
                ruler_data = response.json()
                # Ruler API returns rules grouped by namespace/folder
                # Flatten the structure to return a list of all rules
                alert_rules = []
                for namespace, groups in ruler_data.items():
                    for group in groups:
                        group_name = group.get('name', '')
                        for rule in group.get('rules', []):
                            rule['namespace'] = namespace
                            rule['group'] = group_name
                            alert_rules.append(rule)
                if alert_rules:
                    return alert_rules
                # If ruler API returns empty, try provisioning API as fallback
            
            # Fallback to Provisioning API (only returns provisioned rules)
            url = '{}/api/v1/provisioning/alert-rules'.format(self.__host)
            response = requests.get(url, headers=self.headers, verify=self.__ssl_verify)
            if response and response.status_code == 200:
                return response.json()
            elif response and response.status_code == 404:
                # Alerting API not available (older Grafana version or alerting not enabled)
                logger.info("Grafana alerting API not available (404) - alerting may not be enabled")
                return None
            else:
                raise Exception(
                    f"Failed to fetch alert rules. Status Code: {response.status_code}. Response Text: {response.text}")
        except Exception as e:
            logger.error(f"Exception occurred while fetching grafana alert rules with error: {e}")
            raise e

    def fetch_dashboard_variable_label_values(self, promql_datasource_uid, label_name, metric_match_filter=None, time_range: TimeRange = None):
        try:
            url = f'{self.__host}/api/datasources/proxy/uid/{promql_datasource_uid}/api/v1/label/{label_name}/values'
            params = {}

            if metric_match_filter:
                params['match[]'] = metric_match_filter
                logger.debug(f"Fetching label values for '{label_name}' with match filter: {metric_match_filter}")

            # Add time range parameters if provided
            if time_range:
                # Convert TimeRange to Unix timestamps for Prometheus API
                params['start'] = str(int(time_range.time_geq))
                params['end'] = str(int(time_range.time_lt))
                logger.debug(f"Using time range {params['start']} to {params['end']} for label '{label_name}'")

            response = requests.get(url, headers=self.headers, params=params, verify=self.__ssl_verify)
            if response and response.status_code == 200:
                data = response.json().get('data', [])
                logger.debug(f"Successfully fetched {len(data)} values for label '{label_name}'")
                return data
            else:
                error_body = response.text if response else "No response"
                logger.error(f"Failed to fetch label values for {label_name}. Status: {response.status_code}, Body: {error_body}")
                
                # If the error is related to regex parsing, try without the match filter
                if response.status_code == 400 and metric_match_filter and "parse error" in error_body:
                    logger.warning(f"Regex parse error with match filter '{metric_match_filter}', retrying without filter")
                    params_fallback = {}
                    # Keep time range parameters in fallback
                    if time_range:
                        params_fallback['start'] = str(int(time_range.time_geq))
                        params_fallback['end'] = str(int(time_range.time_lt))
                    
                    response_fallback = requests.get(url, headers=self.headers, params=params_fallback, verify=self.__ssl_verify)
                    if response_fallback and response_fallback.status_code == 200:
                        data = response_fallback.json().get('data', [])
                        logger.info(f"Fallback successful: fetched {len(data)} values for label '{label_name}' without filter")
                        return data
                
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

    def fetch_loki_labels(self, loki_datasource_uid):
        """
        Fetches available labels from a Loki datasource.
        
        Args:
            loki_datasource_uid: The UID of the Loki datasource
            
        Returns:
            List of available label names
        """
        try:
            url = f'{self.__host}/api/datasources/proxy/uid/{loki_datasource_uid}/loki/api/v1/labels'
            response = requests.get(url, headers=self.headers, verify=self.__ssl_verify, timeout=20)
            if response and response.status_code == 200:
                data = response.json()
                if data.get('status') == 'success' and 'data' in data:
                    labels = data['data']
                    logger.debug(f"Successfully fetched {len(labels)} labels from Loki datasource {loki_datasource_uid}")
                    return labels
                else:
                    logger.error(f"Unexpected response format from Loki labels API: {data}")
                    return []
            else:
                error_body = response.text if response else "No response"
                logger.error(f"Failed to fetch Loki labels. Status: {response.status_code}, Body: {error_body}")
                return []
        except Exception as e:
            logger.error(f"Exception occurred while fetching Loki labels for datasource {loki_datasource_uid}: {e}")
            return []

    def get_dashboard_variables(self, dashboard_uid, fixed_variables=None, time_range: TimeRange = None):
        """
        Fetches and resolves all variables for a given Grafana dashboard.
        Handles dependencies between variables and supports multiple variable types.
        
        Args:
            dashboard_uid: The UID of the dashboard
            fixed_variables: Dict of variable names to fixed values that should not be resolved
            time_range: TimeRange object to use for time-based variable queries
        """
        import re
        
        if fixed_variables is None:
            fixed_variables = {}
        
        try:
            dashboard_data = self.fetch_dashboard_details(dashboard_uid)
            
            if not dashboard_data or 'dashboard' not in dashboard_data:
                logger.error("Could not fetch or parse dashboard data.")
                return {}

            dashboard_json = dashboard_data['dashboard']
            variables = dashboard_json.get('templating', {}).get('list', [])
            
            # Start with fixed variables as the base
            resolved_variables = dict(fixed_variables)
            variable_dependencies = {}  # Track dependencies for each variable
            logger.info(f"Starting variable resolution with fixed variables: {fixed_variables}")

            for var in variables:
                var_name = var.get('name')
                var_type = var.get('type')

                if not var_name or not var_type:
                    continue
                
                # Analyze dependencies for this variable
                dependencies = []
                query_string = ""
                
                if var_type == 'query':
                    query_string = str(var.get('query', ''))
                elif var_type == 'custom':
                    query_string = str(var.get('query', ''))
                elif var_type == 'constant':
                    query_string = str(var.get('query', ''))
                elif var_type == 'textbox':
                    query_string = str(var.get('query', ''))
                elif var_type == 'interval':
                    query_string = str(var.get('query', ''))
                elif var_type == 'datasource':
                    # Datasource variables can have dependencies in their datasource UID
                    datasource_info = var.get('datasource', {})
                    if isinstance(datasource_info, dict):
                        datasource_uid = datasource_info.get('uid', '')
                        if datasource_uid:
                            query_string = datasource_uid
                    # Also check the query field for datasource type filters
                    ds_query = var.get('query', '')
                    if ds_query:
                        query_string = f"{query_string} {ds_query}".strip()
                
                if query_string:
                    dependencies = self._extract_variable_dependencies(query_string)
                
                # Store dependencies for this variable
                variable_dependencies[var_name] = dependencies
                
                if dependencies:
                    logger.info(f"Variable '{var_name}' (type: {var_type}) depends on: {dependencies}")
                    # Check if any dependencies are not yet resolved
                    unresolved_deps = [dep for dep in dependencies if dep not in resolved_variables]
                    if unresolved_deps:
                        logger.warning(f"Variable '{var_name}' has unresolved dependencies: {unresolved_deps}. Consider fixing these variables first.")
                else:
                    logger.info(f"Variable '{var_name}' (type: {var_type}) has no dependencies")
                
                # Skip variables that are already fixed
                if var_name in fixed_variables:
                    logger.info(f"Skipping variable '{var_name}' as it's fixed to: {fixed_variables[var_name]}")
                    # Ensure fixed variable value is in list format for consistency
                    if not isinstance(resolved_variables[var_name], list):
                        resolved_variables[var_name] = [resolved_variables[var_name]]
                    continue
                
                values = []
                if var_type == 'query':
                    # Use wildcard querying when fixed_variables is empty or when dependencies are unresolved
                    if not fixed_variables or any(dep not in resolved_variables for dep in dependencies):
                        values = self._resolve_query_variable_wildcard(var, time_range)
                    else:
                        values = self._resolve_query_variable(var, resolved_variables, time_range)
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
                    logger.info(f"Resolved variable '{var_name}' to: {values[:5]}{'...' if len(values) > 5 else ''} ({len(values)} total values)")
                else:
                    resolved_variables[var_name] = [""]

            logger.info(f"For dashboard '{dashboard_json.get('title')}', fetched variable values: {resolved_variables}")
            logger.info(f"Variable dependencies detected: {variable_dependencies}")
            
            return {
                'dashboard_title': dashboard_json.get('title'),
                'dashboard_uid': dashboard_uid,
                'variables': resolved_variables,
                'variable_dependencies': variable_dependencies
            }
        except Exception as e:
            logger.error(f"Exception occurred while fetching dashboard variables for {dashboard_uid}: {e}")
            return {}

    def _extract_variable_dependencies(self, query_string):
        """
        Extracts variable references from query strings.
        Returns a list of variable names that this query depends on.
        
        Handles formats like:
        - $variable
        - ${variable}
        - ${variable:format}
        """
        import re
        
        if not query_string or not isinstance(query_string, str):
            return []
        
        dependencies = set()
        
        # Pattern to match $var or ${var} or ${var:format}
        var_refs = re.findall(r"\$\{([^}:]+)(?::[^}]*)?\}|\$([a-zA-Z0-9_]+)", query_string)
        
        for ref_tuple in var_refs:
            # Get the captured group (variable name)
            var_name = next((item for item in ref_tuple if item), None)
            if var_name:
                dependencies.add(var_name)
        
        return sorted(list(dependencies))

    def _substitute_variables(self, query_string, resolved_variables):
        """Substitutes variables in query strings."""
        import re
        
        for name, value in resolved_variables.items():
            # Handle both list and single value formats
            if isinstance(value, list) and value:
                sub_value = str(value[0])
            elif isinstance(value, (str, int, float)):
                sub_value = str(value)
            else:
                sub_value = ""
            
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

    def _get_label_match_filter(self, query_string, resolved_variables=None):
        """
        Convert label_values queries by replacing variable references with regex wildcards.
        If resolved_variables is provided, uses multi-value regex alternations.
        """
        if resolved_variables is None:
            resolved_variables = {}
            
        label_values_pattern = r'label_values\(([^)]+),\s*[^)]+\)'
        
        match = re.search(label_values_pattern, query_string)
        if not match:
            return query_string
        
        # Extract the metric part (everything before the last comma)
        full_match = match.group(1)
        
        # If we have resolved variables, use the more sophisticated approach
        if resolved_variables:
            return self._build_multi_value_regex_filter(full_match, resolved_variables)
        
        # Fallback to simple wildcard replacement
        # Replace all variable references ($variable) with ".*"
        # This pattern matches $followed by word characters (letters, digits, underscore)
        variable_pattern = r'\"\$\w+\"'
        converted_metric = re.sub(variable_pattern, '".*"', full_match)

        return converted_metric

    def _build_multi_value_regex_filter(self, metric_expr, resolved_variables):
        """
        Build a single Prometheus series selector for label_values() that accounts for
        multi-value variables by using regex alternations in a single query.
        Examples:
          namespace="$ns" with ns=[ns1, ns2] -> namespace=~"ns1|ns2"
        If the label_values() call does not provide a metric selector, returns None.
        """
        import re
        
        # Replace quoted variable matchers label="${var}" or label="$var" with regex alternations
        var_label_pattern = (
            r'([a-zA-Z_][a-zA-Z0-9_]*)\s*(=|!=|=~|!~)\s*"(?:\\?\$\{([a-zA-Z_][a-zA-Z0-9_]*)\}|\\?\$([a-zA-Z_][a-zA-Z0-9_]*))"'
        )

        def build_regex_from_values(values):
            def prometheus_escape(value):
                """
                Escape special characters for Prometheus regex, handling dash specially.
                Prometheus uses RE2 regex engine which has different escaping rules.
                """
                if not value:
                    return ""
                
                # Convert to string if not already
                value_str = str(value)
                
                # For Prometheus regex, we need to be careful with certain characters
                # RE2 doesn't support all the same escape sequences as Python's re module
                # Escape basic regex metacharacters but handle dash specially
                escaped = value_str
                
                # Escape basic metacharacters that are safe to escape in RE2
                metacharacters = r'\.^$*+?{}[]|()'
                for char in metacharacters:
                    escaped = escaped.replace(char, '\\' + char)
                
                # Don't escape dash with backslash as it can cause issues in RE2
                # Instead, we'll rely on proper placement in alternation groups
                return escaped
            
            if isinstance(values, list) and values:
                escaped_values = [prometheus_escape(v) for v in values]
                return "|".join(escaped_values)
            if isinstance(values, str) and values:
                return prometheus_escape(values)
            return ".*"

        def replace_var_label(m):
            label = m.group(1)
            op = m.group(2)
            var_name = m.group(3) or m.group(4)
            values = resolved_variables.get(var_name, [])
            regex = build_regex_from_values(values)
            new_op = '!~' if op in ('!=', '!~') else '=~'
            return f'{label}{new_op}"{regex}"'

        converted = re.sub(var_label_pattern, replace_var_label, metric_expr)

        # Replace remaining bare variable tokens (e.g., in metric names) with an alternation group
        # $var or ${var}
        def replace_bare_var(m):
            var_name = m.group(1) or m.group(2)
            values = resolved_variables.get(var_name, [])
            regex = build_regex_from_values(values)
            # For metric names, parentheses without quotes are acceptable in regex contexts; here they
            # will simply expand inside the selector context or be harmless text. As a safe fallback,
            # just return the alternation (no quotes) to avoid breaking syntax outside label matchers.
            return f'({regex})'

        converted = re.sub(r'\$\{([a-zA-Z_][a-zA-Z0-9_]*)\}|\$([a-zA-Z_][a-zA-Z0-9_]*)', replace_bare_var, converted)

        # Fallback: if the selector becomes too long, skip match[] to avoid 414
        # This will fetch label values across all namespaces (wildcard behavior)
        try:
            max_selector_len = 1400
            if len(converted) > max_selector_len:
                return None
        except Exception:
            # Defensive: on any unexpected issue, do not block resolution
            return None

        return converted

    def _resolve_query_variable_wildcard(self, var, time_range: TimeRange = None):
        """
        Resolves a 'query' type variable using wildcard querying when no fixed variables are provided.
        This method fetches all possible values without considering variable dependencies.
        
        Args:
            var: Variable definition from dashboard
            time_range: TimeRange object for time-based queries
        """
        import re
        
        datasource = var.get('datasource')
        query = var.get('query')

        if not datasource or not query:
            return []

        ds_uid = datasource.get('uid') if isinstance(datasource, dict) else datasource

        # Handle the case where ds_uid is "default" - need to resolve it to actual datasource
        if ds_uid == 'default':
            ds_type = datasource.get('type') if isinstance(datasource, dict) else 'prometheus'  # assume prometheus if not specified
            datasource_details = self.get_default_datasource_by_type(ds_type)
            if datasource_details:
                ds_uid = datasource_details['uid']
            else:
                logger.warning(f"Could not find default datasource of type '{ds_type}' for query variable '{var.get('name')}'.")
                return []
        # Handle the case where ds_uid is a variable reference like $datasource
        elif isinstance(ds_uid, str) and (ds_uid.startswith('$') or ds_uid.startswith('${')):
            # For wildcard queries, we need to resolve datasource variables
            # Try to get the default datasource of the expected type
            ds_type = datasource.get('type') if isinstance(datasource, dict) else 'prometheus'
            datasource_details = self.get_default_datasource_by_type(ds_type)
            if datasource_details:
                original_ds_uid = ds_uid
                ds_uid = datasource_details['uid']
                logger.info(f"Resolved datasource variable '{original_ds_uid}' to default datasource of type '{ds_type}': {ds_uid}")
            else:
                logger.warning(f"Could not resolve datasource variable '{ds_uid}' for query variable '{var.get('name')}'. No default datasource of type '{ds_type}' found.")
                return []
        else:
            datasource_details = self.get_datasource_by_uid(ds_uid)

        if not datasource_details or datasource_details.get('type') != 'prometheus':
            logger.warning(f"Unsupported or unknown datasource type for query variable '{var.get('name')}'.")
            return []
        
        # Prometheus query handling with wildcard approach
        raw_query = str(query)

        # Case 1: label_values(label) or label_values(metric, label)
        label_values_match = re.search(r'label_values\((?:.*\s*,\s*)?(\w+)\)', raw_query)
        if label_values_match:
            label = label_values_match.group(1)
            # For wildcard querying, we don't use any metric filter - fetch all values
            logger.debug(f"Using wildcard querying for label '{label}' in variable '{var.get('name')}'")
            return self.fetch_dashboard_variable_label_values(ds_uid, label, None, time_range)

        # Case 2: metrics(pattern) -> label_values(__name__)
        if re.match(r'metrics\(.*\)', raw_query):
            logger.debug(f"Using wildcard querying for metrics in variable '{var.get('name')}'")
            return self.fetch_dashboard_variable_label_values(ds_uid, '__name__', None, time_range)

        # Case 3: Generic PromQL query (including query_result(query))
        try:
            # For wildcard queries, we need to handle variable substitution differently
            # Replace variables with wildcards or remove them entirely
            wildcard_query = self._convert_to_wildcard_query(raw_query)
            
            if wildcard_query.startswith('query_result(') and wildcard_query.endswith(')'):
                wildcard_query = wildcard_query[len('query_result('):-1]
            
            url = f'{self.__host}/api/datasources/proxy/uid/{ds_uid}/api/v1/query'
            params = {'query': wildcard_query}
            
            # Add time range parameters if provided
            if time_range:
                # Use the end time for instant queries (most recent data)
                params['time'] = str(int(time_range.time_lt))
                logger.debug(f"Using time parameter {params['time']} for wildcard query variable '{var.get('name')}'")
            
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
                logger.error(f"Wildcard query failed for '{wildcard_query}'. Status: {response.status_code}, Body: {response.text}")
                return []
        except Exception as e:
            logger.error(f"Exception during wildcard query execution for '{raw_query}': {e}")
            return []

    def _convert_to_wildcard_query(self, query_string):
        """
        Converts a query string with variables to a wildcard query by replacing
        variable references with wildcards or removing them.
        
        Args:
            query_string: Original query string with variable references
            
        Returns:
            Modified query string suitable for wildcard querying
        """
        import re
        
        if not query_string:
            return query_string
        
        # Replace variable references in label matchers with wildcards
        # Pattern: label="${var}" or label="$var" -> label=~".*"
        var_label_pattern = r'([a-zA-Z_][a-zA-Z0-9_]*)\s*(=|!=|=~|!~)\s*"(?:\\?\$\{[^}]+\}|\\?\$[a-zA-Z_][a-zA-Z0-9_]*)"'
        
        def replace_with_wildcard(m):
            label = m.group(1)
            op = m.group(2)
            # Use regex match for wildcard behavior
            new_op = '!~' if op in ('!=', '!~') else '=~'
            return f'{label}{new_op}".*"'
        
        wildcard_query = re.sub(var_label_pattern, replace_with_wildcard, query_string)
        
        # Replace remaining variable references with wildcards
        # Pattern: $var or ${var} -> .*
        wildcard_query = re.sub(r'\$\{[^}]+\}|\$[a-zA-Z_][a-zA-Z0-9_]*', '.*', wildcard_query)
        
        logger.debug(f"Converted query '{query_string}' to wildcard query '{wildcard_query}'")
        return wildcard_query

    def _resolve_query_variable(self, var, resolved_variables, time_range: TimeRange = None):
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
        
        # Prometheus query handling
        # IMPORTANT: Handle label_values against the raw query (with $vars) so we can
        # build a multi-value regex alternation from resolved_variables.
        raw_query = str(query)

        # Case 1: label_values(label) or label_values(metric, label)
        label_values_match = re.search(r'label_values\((?:.*\s*,\s*)?(\w+)\)', raw_query)
        if label_values_match:
            label = label_values_match.group(1)
            metric_filter = self._get_label_match_filter(raw_query, resolved_variables)
            return self.fetch_dashboard_variable_label_values(ds_uid, label, metric_filter, time_range)

        # Case 2: metrics(pattern) -> label_values(__name__)
        if re.match(r'metrics\(.*\)', raw_query):
            return self.fetch_dashboard_variable_label_values(ds_uid, '__name__', None, time_range)

        # Case 3: Generic PromQL query (including query_result(query))
        try:
            # For generic queries, do the standard substitution now
            query = self._substitute_variables(str(query), resolved_variables)
            if query.startswith('query_result(') and query.endswith(')'):
                query = query[len('query_result('):-1]
            
            url = f'{self.__host}/api/datasources/proxy/uid/{ds_uid}/api/v1/query'
            params = {'query': query}
            
            # Add time range parameters if provided
            if time_range:
                # Use the end time for instant queries (most recent data)
                params['time'] = str(int(time_range.time_lt))
                logger.debug(f"Using time parameter {params['time']} for query variable '{var.get('name')}'")
            
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
