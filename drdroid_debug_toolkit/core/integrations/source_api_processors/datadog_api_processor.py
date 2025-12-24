import json
import logging

import requests
from datadog_api_client import ApiClient, Configuration
from datadog_api_client.exceptions import ApiException
from datadog_api_client.v1.api.authentication_api import AuthenticationApi
from datadog_api_client.v1.api.aws_integration_api import AWSIntegrationApi
from datadog_api_client.v1.api.aws_logs_integration_api import AWSLogsIntegrationApi
from datadog_api_client.v1.api.azure_integration_api import AzureIntegrationApi
from datadog_api_client.v1.api.dashboards_api import DashboardsApi
from datadog_api_client.v1.api.monitors_api import MonitorsApi
from datadog_api_client.v1.model.authentication_validation_response import AuthenticationValidationResponse
from datadog_api_client.v1.model.azure_account_list_response import AzureAccountListResponse
from datadog_api_client.v2.api.cloudflare_integration_api import CloudflareIntegrationApi
from datadog_api_client.v2.api.confluent_cloud_api import ConfluentCloudApi
from datadog_api_client.v2.api.fastly_integration_api import FastlyIntegrationApi
from datadog_api_client.v2.api.gcp_integration_api import GCPIntegrationApi
from datadog_api_client.v2.api.metrics_api import MetricsApi
from datadog_api_client.v2.model.formula_limit import FormulaLimit
from datadog_api_client.v2.model.metrics_data_source import MetricsDataSource
from datadog_api_client.v2.model.metrics_timeseries_query import MetricsTimeseriesQuery
from datadog_api_client.v2.model.query_formula import QueryFormula
from datadog_api_client.v2.model.query_sort_order import QuerySortOrder
from datadog_api_client.v2.model.timeseries_formula_query_request import TimeseriesFormulaQueryRequest
from datadog_api_client.v2.model.timeseries_formula_query_response import TimeseriesFormulaQueryResponse
from datadog_api_client.v2.model.timeseries_formula_request import TimeseriesFormulaRequest
from datadog_api_client.v2.model.timeseries_formula_request_attributes import TimeseriesFormulaRequestAttributes
from datadog_api_client.v2.model.timeseries_formula_request_queries import TimeseriesFormulaRequestQueries
from datadog_api_client.v2.model.timeseries_formula_request_type import TimeseriesFormulaRequestType
from datadog_api_client.v2.api.logs_api import LogsApi
from datadog_api_client.v2.model.logs_list_request import LogsListRequest
from datadog_api_client.v2.model.logs_list_request_page import LogsListRequestPage
from datadog_api_client.v2.model.logs_query_filter import LogsQueryFilter
from datadog_api_client.v2.model.logs_sort import LogsSort

from core.protos.base_pb2 import TimeRange
from datetime import datetime, timezone

from core.integrations.processor import Processor
from core.utils.http_utils import make_request_with_retry
from core.settings import EXTERNAL_CALL_TIMEOUT

logger = logging.getLogger(__name__)


class DatadogApiProcessor(Processor):
    def __init__(self, dd_app_key, dd_api_key, dd_api_domain=None):
        self.__dd_app_key = dd_app_key
        self.__dd_api_key = dd_api_key
        self.dd_api_domain = dd_api_domain

        if dd_api_domain:
            self.__dd_host = 'https://api.{}'.format(dd_api_domain)
        else:
            self.__dd_host = 'https://api.{}'.format('datadoghq.com')
        self.dd_dependencies_url = self.__dd_host + "/api/v1/service_dependencies"

        self.headers = {
            'Content-Type': 'application/json',
            'DD-API-KEY': dd_api_key,
            'DD-APPLICATION-KEY': dd_app_key,
            'Accept': 'application/json'
        }
        self._QUERY_TEMPLATES_1 = [
            ["sum:trace.django.request.hits{{env:{env},service:{service}}}.as_count()"],
            ["sum:trace.django.request.errors{{env:{env},service:{service}}}.as_count()"],
            ["p95:trace.django.request{{env:{env},service:{service}}}",
             "p99:trace.django.request{{env:{env},service:{service}}}"]
        ]
        self._QUERY_TEMPLATES_2 = [
            ["sum:trace.flask.request.hits{{env:{env},service:{service}}}.as_count()"],
            ["sum:trace.flask.request.errors{{env:{env},service:{service}}}.as_count()"],
            ["p95:trace.flask.request{{env:{env},service:{service}}}",
             "p99:trace.flask.request{{env:{env},service:{service}}}"]
        ] 
        self._QUERY_TEMPLATES_3 = [
            ["sum:trace.postgres.query.hits{{env:{env},service:{service}}}.as_count()"],
            ["sum:trace.postgres.query.errors{{env:{env},service:{service}}}.as_count()"],
            ["p95:trace.postgres.query{{env:{env},service:{service}}}",
             "p99:trace.postgres.query{{env:{env},service:{service}}}"]
        ] 
        self._QUERY_TEMPLATES_4 = [
            ["sum:trace.redis.command.hits{{env:{env},service:{service}}}.as_count()"],
            ["sum:trace.redis.command.errors{{env:{env},service:{service}}}.as_count()"],
            ["p95:trace.redis.command{{env:{env},service:{service}}}",
             "p99:trace.redis.command{{env:{env},service:{service}}}"]
        ] 
        self._QUERY_TEMPLATES_5 = [
            ["sum:trace.celery.run.hits{{env:{env},service:{service}}}.as_count()"],
            ["sum:trace.celery.run.errors{{env:{env},service:{service}}}.as_count()"],
            ["p95:trace.celery.run{{env:{env},service:{service}}}",
             "p99:trace.celery.run{{env:{env},service:{service}}}"]
        ] 
        self._QUERY_TEMPLATES_6 = [
            ["sum:trace.requests.request.hits{{env:{env},service:{service}}}.as_count()"],
            ["sum:trace.requests.request.errors{{env:{env},service:{service}}}.as_count()"],
            ["p95:trace.requests.request{{env:{env},service:{service}}}",
             "p99:trace.requests.request{{env:{env},service:{service}}}"]
        ] 
        self._QUERY_TEMPLATES_7 = [
            ["sum:trace.jinja2.request.hits{{env:{env},service:{service}}}.as_count()"],
            ["sum:trace.jinja2.request.errors{{env:{env},service:{service}}}.as_count()"],
            ["p95:trace.jinja2.request{{env:{env},service:{service}}}",
             "p99:trace.jinja2.request{{env:{env},service:{service}}}"]
        ] 
        self._QUERY_TEMPLATES_8 = [
            ["sum:trace.s3.command.hits{{env:{env},service:{service}}}.as_count()"],
            ["sum:trace.s3.command.errors{{env:{env},service:{service}}}.as_count()"],
            ["p95:trace.s3.command{{env:{env},service:{service}}}",
             "p99:trace.s3.command{{env:{env},service:{service}}}"]
        ]
        self._QUERY_TEMPLATES_9 = [
            ["sum:trace.celery.beat.tick.hits{{env:{env},service:{service}}}.as_count()"],
            ["sum:trace.celery.beat.tick.errors{{env:{env},service:{service}}}.as_count()"],
            ["p95:trace.celery.beat.tick{{env:{env},service:{service}}}",
             "p99:trace.celery.beat.tick{{env:{env},service:{service}}}"]
        ]

        # Additional templates for missing frameworks
        self._QUERY_TEMPLATES_10 = [  # trace.redis.query
            ["sum:trace.redis.query.hits{{env:{env},service:{service}}}.as_count()"],
            ["sum:trace.redis.query.errors{{env:{env},service:{service}}}.as_count()"],
            ["p95:trace.redis.query{{env:{env},service:{service}}}",
             "p99:trace.redis.query{{env:{env},service:{service}}}"]
        ]

        self._QUERY_TEMPLATES_11 = [  # trace.postgresql.query
            ["sum:trace.postgresql.query.hits{{env:{env},service:{service}}}.as_count()"],
            ["sum:trace.postgresql.query.errors{{env:{env},service:{service}}}.as_count()"],
            ["p95:trace.postgresql.query{{env:{env},service:{service}}}",
             "p99:trace.postgresql.query{{env:{env},service:{service}}}"]
        ]

        self._QUERY_TEMPLATES_12 = [  # trace.Internal
            ["sum:trace.Internal.hits{{env:{env},service:{service}}}.as_count()"],
            ["sum:trace.Internal.errors{{env:{env},service:{service}}}.as_count()"],
            ["p95:trace.Internal{{env:{env},service:{service}}}",
             "p99:trace.Internal{{env:{env},service:{service}}}"]
        ]

        self._QUERY_TEMPLATES_13 = [  # trace.http.server.request
            ["sum:trace.http.server.request.hits{{env:{env},service:{service}}}.as_count()"],
            ["sum:trace.http.server.request.errors{{env:{env},service:{service}}}.as_count()"],
            ["p95:trace.http.server.request{{env:{env},service:{service}}}",
             "p99:trace.http.server.request{{env:{env},service:{service}}}"]
        ]

        self._QUERY_TEMPLATES_14 = [  # trace.http.client.request
            ["sum:trace.http.client.request.hits{{env:{env},service:{service}}}.as_count()"],
            ["sum:trace.http.client.request.errors{{env:{env},service:{service}}}.as_count()"],
            ["p95:trace.http.client.request{{env:{env},service:{service}}}",
             "p99:trace.http.client.request{{env:{env},service:{service}}}"]
        ]

        self._QUERY_TEMPLATES_15 = [  # trace.grpc.server.request
            ["sum:trace.grpc.server.request.hits{{env:{env},service:{service}}}.as_count()"],
            ["sum:trace.grpc.server.request.errors{{env:{env},service:{service}}}.as_count()"],
            ["p95:trace.grpc.server.request{{env:{env},service:{service}}}",
             "p99:trace.grpc.server.request{{env:{env},service:{service}}}"]
        ]

        self._QUERY_TEMPLATES_16 = [  # trace.grpc.client.request
            ["sum:trace.grpc.client.request.hits{{env:{env},service:{service}}}.as_count()"],
            ["sum:trace.grpc.client.request.errors{{env:{env},service:{service}}}.as_count()"],
            ["p95:trace.grpc.client.request{{env:{env},service:{service}}}",
             "p99:trace.grpc.client.request{{env:{env},service:{service}}}"]
        ]

        self._QUERY_TEMPLATES_17 = [  # trace.graphql.server.request
            ["sum:trace.graphql.server.request.hits{{env:{env},service:{service}}}.as_count()"],
            ["p95:trace.graphql.server.request{{env:{env},service:{service}}}",
             "p99:trace.graphql.server.request{{env:{env},service:{service}}}"]
        ]

        self._QUERY_TEMPLATES_18 = [  # trace.Consumer
            ["sum:trace.Consumer.hits{{env:{env},service:{service}}}.as_count()"],
            ["p95:trace.Consumer{{env:{env},service:{service}}}",
             "p99:trace.Consumer{{env:{env},service:{service}}}"]
        ]

        self._QUERY_TEMPLATES_19 = [  # trace.client.request
            ["sum:trace.client.request.hits{{env:{env},service:{service}}}.as_count()"],
            ["sum:trace.client.request.errors{{env:{env},service:{service}}}.as_count()"],
            ["p95:trace.client.request{{env:{env},service:{service}}}",
             "p99:trace.client.request{{env:{env},service:{service}}}"]
        ]

        self._QUERY_TEMPLATES_20 = [  # trace.bullmq.publish
            ["sum:trace.bullmq.publish.hits{{env:{env},service:{service}}}.as_count()"],
            ["p95:trace.bullmq.publish{{env:{env},service:{service}}}",
             "p99:trace.bullmq.publish{{env:{env},service:{service}}}"]
        ]

        self._QUERY_TEMPLATES_21 = [  # trace.bullmq.process
            ["sum:trace.bullmq.process.hits{{env:{env},service:{service}}}.as_count()"],
            ["sum:trace.bullmq.process.errors{{env:{env},service:{service}}}.as_count()"],
            ["p95:trace.bullmq.process{{env:{env},service:{service}}}",
             "p99:trace.bullmq.process{{env:{env},service:{service}}}"]
        ]

        self._QUERY_TEMPLATES_22 = [  # trace.bullmq.create
            ["sum:trace.bullmq.create.hits{{env:{env},service:{service}}}.as_count()"],
            ["p95:trace.bullmq.create{{env:{env},service:{service}}}",
             "p99:trace.bullmq.create{{env:{env},service:{service}}}"]
        ]

        self._QUERY_TEMPLATES_23 = [  # trace.better_sqlite3.query
            ["sum:trace.better_sqlite3.query.hits{{env:{env},service:{service}}}.as_count()"],
            ["p95:trace.better_sqlite3.query{{env:{env},service:{service}}}",
             "p99:trace.better_sqlite3.query{{env:{env},service:{service}}}"]
        ]

        self._QUERY_TEMPLATES_24 = [  # trace.aws.mappings.request
            ["sum:trace.aws.mappings.request.hits{{env:{env},service:{service}}}.as_count()"],
            ["p95:trace.aws.mappings.request{{env:{env},service:{service}}}",
             "p99:trace.aws.mappings.request{{env:{env},service:{service}}}"]
        ]

        # Mapping of framework names to their respective query templates 
        # self.map_of_framework_to_query_templates = {
        #     "trace.django.request": self._QUERY_TEMPLATES_1,
        #     "trace.flask.request": self._QUERY_TEMPLATES_2,
        #     "trace.postgres.query": self._QUERY_TEMPLATES_3,
        #     "trace.redis.command": self._QUERY_TEMPLATES_4,
        #     "trace.celery.run": self._QUERY_TEMPLATES_5,
        #     "trace.requests.request": self._QUERY_TEMPLATES_6,
        #     "trace.jinja2.request": self._QUERY_TEMPLATES_7,
        #     "trace.s3.command": self._QUERY_TEMPLATES_8,
        #     "trace.celery.beat": self._QUERY_TEMPLATES_9
        # }

        # Dynamically build the map
        query_templates_list = [
            self._QUERY_TEMPLATES_1,
            self._QUERY_TEMPLATES_2,
            self._QUERY_TEMPLATES_3,
            self._QUERY_TEMPLATES_4,
            self._QUERY_TEMPLATES_5,
            self._QUERY_TEMPLATES_6,
            self._QUERY_TEMPLATES_7,
            self._QUERY_TEMPLATES_8,
            self._QUERY_TEMPLATES_9,
            self._QUERY_TEMPLATES_10,
            self._QUERY_TEMPLATES_11,
            self._QUERY_TEMPLATES_12,
            self._QUERY_TEMPLATES_13,
            self._QUERY_TEMPLATES_14,
            self._QUERY_TEMPLATES_15,
            self._QUERY_TEMPLATES_16,
            self._QUERY_TEMPLATES_17,
            self._QUERY_TEMPLATES_18,
            self._QUERY_TEMPLATES_19,
            self._QUERY_TEMPLATES_20,
            self._QUERY_TEMPLATES_21,
            self._QUERY_TEMPLATES_22,
            self._QUERY_TEMPLATES_23,
            self._QUERY_TEMPLATES_24
        ]

        self.map_of_framework_to_query_templates = {}

        for template in query_templates_list:
            first_query = template[0][0]
            if '.hits' in first_query:
                key = first_query.split('sum:')[1].split('.hits')[0]
            else:
                raise ValueError(f"Unexpected query format (no '.hits' found): {first_query}")

            self.map_of_framework_to_query_templates[key] = template

    def build_queries(self, service_name="*", env="prod", matching_metrics=None):
        """
        Replaces `{service}` and `{env}` placeholders in each query template.
        Processes ALL matching metrics to get their framework keys.
        """
        queries = []
        processed_frameworks = set()  # Track which frameworks we've already processed
        
        for metric in matching_metrics:
            # Extract framework key by finding the base framework
            parts = metric.split('.')
            
            # Handle different metric structures
            if len(parts) < 2:
                continue
            elif len(parts) == 2:
                # e.g., "trace.hits" -> "trace" (but this might be too generic)
                framework_key = parts[0]
            else:
                # Find the framework by looking for known metric types
                # Known metric types: hits, errors, duration, apdex, etc.
                metric_types = ['hits', 'errors', 'duration', 'apdex', 'p50', 'p75', 'p90', 'p95', 'p99', 'p99.9', 'max', 'mean', 'min', 'stddev']
                
                framework_key = None
                for i, part in enumerate(parts):
                    if part in metric_types:
                        # Found a metric type, everything before it is the framework
                        framework_key = '.'.join(parts[:i])
                        break
                
                if not framework_key:
                    # Fallback: try to match against known template keys
                    # Try different combinations to find a match
                    for i in range(len(parts), 0, -1):
                        candidate = '.'.join(parts[:i])
                        if candidate in self.map_of_framework_to_query_templates:
                            framework_key = candidate
                            break
                    
                    if not framework_key:
                        # Last resort: remove the last part
                        framework_key = '.'.join(parts[:-1])
            
            # Skip if we've already processed this framework
            if framework_key in processed_frameworks:
                continue
                
            if framework_key in self.map_of_framework_to_query_templates:
                query_templates = self.map_of_framework_to_query_templates[framework_key]
                processed_frameworks.add(framework_key)
                
                for template in query_templates:
                    if len(template) == 1:
                        # Single query template
                        template_query = template[0]
                        try:
                            query = template_query.format(service=service_name, env=env)
                            queries.append([query])
                        except ValueError as e:
                            logger.error(f"Error formatting query template: {template_query} with service={service_name}, env={env}. Error: {e}")
                            # Fallback to string replacement
                            query = template_query.replace("{service}", str(service_name)).replace("{env}", str(env))
                            queries.append([query])
                    else:
                        # Multiple query templates
                        query_mini_list = []
                        for query in template:
                            try:
                                formatted_query = query.format(service=service_name, env=env)
                                query_mini_list.append(formatted_query)
                            except ValueError as e:
                                logger.error(f"Error formatting query template: {query} with service={service_name}, env={env}. Error: {e}")
                                # Fallback to string replacement
                                formatted_query = query.replace("{service}", str(service_name)).replace("{env}", str(env))
                                query_mini_list.append(formatted_query)
                        if query_mini_list:  # Only add if we have queries
                            queries.append(query_mini_list)
            else:
                logger.warning(f"No query templates found for framework: {framework_key}")
        
        if not queries:
            logger.error(f"No queries generated for any of the {len(matching_metrics)} matching metrics")
            return []
        
        return queries
    
    def fetch_query_results(self, service_name="*", env="prod", start_time=None, end_time=None, matching_metrics=None, interval=300000):
        """
        Fetches query results for each generated query string.
        """
        base_url = self.__dd_host + "/api/v1/query"
        
        results = []
        queries = self.build_queries(service_name, env, matching_metrics)
        if not queries:
            logger.error("No queries generated. Please check the service name and matching metrics.")
            return results

        for query_group in queries:  # each sublist
            result = {}
            for i, query in enumerate(query_group):
                # Add a small delay between queries to avoid rate limiting
                if i > 0:
                    import time
                    time.sleep(0.1)  # 100ms delay between queries
                params = {
                    "from": start_time,
                    "to": end_time,
                    "query": query,
                    "interval": interval
                }
                headers = {
                    "DD-API-KEY": self.__dd_api_key,
                    "DD-APPLICATION-KEY": self.__dd_app_key
                }

                logger.info(f"Executing query: {query}")
                response = requests.get(base_url, params=params, headers=headers)
                logger.info(f"Query response status: {response.status_code}")
                
                # Check for rate limiting - EXIT IMMEDIATELY
                if response.status_code == 429:
                    logger.warning(f"Rate limit exceeded for query: {query}. Exiting task immediately.")
                    rate_limit_reset = response.headers.get('x-ratelimit-reset')
                    if rate_limit_reset:
                        logger.info(f"Rate limit will reset in {rate_limit_reset} seconds")
                    # Add what we have so far to results
                    if result:
                        results.append(result)
                    # Return immediately with what we've collected so far
                    return results
                elif response.ok:
                    response_data = response.json()
                    # Check if the response indicates no data vs other issues
                    if 'series' in response_data and len(response_data['series']) == 0:
                        logger.info(f"No data returned from Datadog for query: {query}")
                        result[query] = {"message": "No data available", "data": response_data}
                    else:
                        result[query] = response_data
                else:
                    logger.error(f"Query failed with status {response.status_code}: {query}")
                    result[query] = {"error": f"Status {response.status_code}: {response.text}"}
            
            results.append(result)  # One dictionary per sublist
        return results
    
    def get_connection(self):
        try:
            configuration = Configuration()
            configuration.api_key["apiKeyAuth"] = self.__dd_api_key
            configuration.api_key["appKeyAuth"] = self.__dd_app_key
            if self.dd_api_domain:
                configuration.server_variables["site"] = self.dd_api_domain
            configuration.unstable_operations["query_timeseries_data"] = True
            configuration.compress = False
            configuration.enable_retry = True
            configuration.max_retries = 20
            return configuration
        except Exception as e:
            logger.error(f"Error while initializing Datadog API Processor: {e}")
            raise Exception("Error while initializing Datadog API Processor: {}".format(e))

    def test_connection(self):
        try:
            configuration = self.get_connection()
            with ApiClient(configuration) as api_client:
                api_instance = AuthenticationApi(api_client)
                response: AuthenticationValidationResponse = api_instance.validate()
                if not response.get('valid', False):
                    raise Exception("Datadog API connection is not valid. Check API Key")
                return True
        except ApiException as e:
            logger.error("Exception when calling AuthenticationApi->validate: %s\n" % e)
            raise e

    def fetch_metric_timeseries(self, tr: TimeRange, specific_metric, interval=300000):
        metric_queries = specific_metric.get('queries', None)
        formulas = specific_metric.get('formulas', None)
        from_tr = int(tr.time_geq * 1000)
        to_tr = int(tr.time_lt * 1000)
        if not metric_queries:
            return None
        query_formulas: [QueryFormula] = []
        if formulas:
            for f in formulas:
                query_formulas.append(
                    QueryFormula(formula=f['formula'], limit=FormulaLimit(count=10, order=QuerySortOrder.DESC)))

        timeseries_queries: [MetricsTimeseriesQuery] = []
        for query in metric_queries:
            timeseries_queries.append(MetricsTimeseriesQuery(
                data_source=MetricsDataSource.METRICS,
                name=query['name'],
                query=query['query']
            ))

        body = TimeseriesFormulaQueryRequest(
            data=TimeseriesFormulaRequest(
                attributes=TimeseriesFormulaRequestAttributes(
                    formulas=query_formulas,
                    _from=from_tr,
                    interval=interval,
                    queries=TimeseriesFormulaRequestQueries(timeseries_queries),
                    to=to_tr,
                ),
                type=TimeseriesFormulaRequestType.TIMESERIES_REQUEST,
            ),
        )
        configuration = self.get_connection()
        with ApiClient(configuration) as api_client:
            api_instance = MetricsApi(api_client)
            try:
                response: TimeseriesFormulaQueryResponse = api_instance.query_timeseries_data(body=body)
                if response:
                    result = response.data.attributes
                    return result
            except Exception as e:
                logger.error(f"Exception occurred while fetching metric timeseries with error: {e}")
                raise e

    def fetch_monitor_details(self, monitor_id):
        try:
            configuration = self.get_connection()
            with ApiClient(configuration) as api_client:
                api_instance = MonitorsApi(api_client)
                response = api_instance.get_monitor(monitor_id)
                return response
        except Exception as e:
            logger.error(f"Exception occurred while fetching monitor details with error: {e}")
            raise e

    def get_metric_data(self, start, end, query):
        try:
            configuration = self.get_connection()
            with ApiClient(configuration) as api_client:
                api_instance = MetricsApi(api_client)
                metric_data = api_instance.query_metrics(int(start), int(end), query)
                return metric_data
        except Exception as e:
            logger.error(e)
            raise e

    def get_span_duration_aggregation(self, start, end, query):
        url = self.__dd_host + "/api/v2/spans/analytics/aggregate"

        payload = json.dumps({
            "data": {
                "attributes": {
                    "compute": [
                        {
                            "aggregation": "avg",
                            "interval": "1m",
                            "type": "timeseries"
                        }
                    ],
                    "filter": {
                        "from": start,
                        "query": query,
                        "to": end
                    },
                    "group_by": [
                        {"facet": "resource_name"}
                    ],
                },
                "type": "aggregate_request"
            }
        })

        headers = {
            'DD-APPLICATION-KEY': self.__dd_app_key,
            'DD-API-KEY': self.__dd_api_key,
            'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        return response.json()

    def get_span_count_aggregation(self, start, end, query):
        url = self.__dd_host + "/api/v2/spans/analytics/aggregate"

        payload = json.dumps({
            "data": {
                "attributes": {
                    "compute": [
                        {
                            "aggregation": "count",
                            "interval": "1m",
                            "type": "timeseries"
                        }
                    ],
                    "filter": {
                        "from": start,
                        "query": query,
                        "to": end
                    },
                    "group_by": [
                        {"facet": "resource_name"}
                    ],
                },
                "type": "aggregate_request"
            }
        })

        headers = {
            'DD-APPLICATION-KEY': self.__dd_app_key,
            'DD-API-KEY': self.__dd_api_key,
            'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        return response.json()

    def search_spans(self, start, end, query, cursor='', limit=10):
        url = self.__dd_host + "/api/v2/spans/events/search"

        if start:
            start_iso = datetime.fromtimestamp(start, tz=timezone.utc).isoformat()
        else:
            start_iso = datetime.now(tz=timezone.utc).isoformat()

        if end:
            end_iso = datetime.fromtimestamp(end, tz=timezone.utc).isoformat()
        else:
            end_iso = datetime.now(tz=timezone.utc).isoformat()

        # Build page object - only include cursor if provided
        page_obj = {"limit": limit}
        if cursor:
            page_obj["cursor"] = cursor

        payload = {
            "data": {
                "type": "search_request",
                "attributes": {
                    "filter": {
                        "from": start_iso,
                        "query": query if query else "*",
                        "to": end_iso
                    },
                    "options": {
                        "timezone": "UTC"
                    },
                    "page": page_obj,
                    "sort": "-timestamp"
                }
            }
        }

        headers = {
            'DD-APPLICATION-KEY': self.__dd_app_key,
            'DD-API-KEY': self.__dd_api_key,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        try:
            response = make_request_with_retry("POST", url, headers=headers, payload=json.dumps(payload), default_resend_delay=10)
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(
                    f"DatadogApiProcessor.search_spans:: Error occurred searching spans with status_code: {response.status_code}, response: {response.text}")
                return None
        except Exception as e:
            logger.error(f"DatadogApiProcessor.search_spans:: Exception occurred searching spans with error: {e}")
            raise e

    def aggregate_spans(self, start, end, query, aggregation="count", compute_type="timeseries",
                        interval_seconds=None, group_by=None):
        url = self.__dd_host + "/api/v2/spans/analytics/aggregate"

        aggregation = (aggregation or "count").lower()
        compute_type = (compute_type or "timeseries").lower()

        compute_entry = {
            "aggregation": aggregation
        }

        if compute_type:
            compute_entry["type"] = compute_type

        if compute_type == "timeseries" and interval_seconds:
            try:
                interval_seconds = int(interval_seconds)
            except (TypeError, ValueError):
                interval_seconds = None
            if interval_seconds:
                if interval_seconds % 3600 == 0:
                    hours = interval_seconds // 3600
                    compute_entry["interval"] = f"{hours}h"
                elif interval_seconds % 60 == 0:
                    minutes = interval_seconds // 60
                    compute_entry["interval"] = f"{minutes}m"
                else:
                    compute_entry["interval"] = f"{interval_seconds}s"

        filter_from = datetime.fromtimestamp(start, tz=timezone.utc).isoformat()
        filter_to = datetime.fromtimestamp(end, tz=timezone.utc).isoformat()

        attributes = {
            "compute": [compute_entry],
            "filter": {
                "from": filter_from,
                "query": query if query else "*",
                "to": filter_to
            }
        }

        if group_by:
            attributes["group_by"] = [{"facet": facet} for facet in group_by if facet]

        payload = {
            "data": {
                "attributes": attributes,
                "type": "aggregate_request"
            }
        }

        response = requests.post(
            url,
            headers=self.headers,
            json=payload,
            timeout=EXTERNAL_CALL_TIMEOUT
        )

        if response.status_code == 200:
            return response.json()

        logger.error(
            "DatadogApiProcessor.aggregate_spans:: Error %s while aggregating spans. Response: %s",
            response.status_code,
            response.text
        )
        return None

    def get_metric_data_using_api(self, start, end, interval, queries, query_formula):
        url = self.__dd_host + "/api/v2/query/timeseries"
        payload_dict = {"data": {
            "attributes": {"formulas": query_formula, "from": start, "interval": interval * 1000,
                           "queries": queries, "to": end}, "type": "timeseries_request"}}

        result_dict = {}
        print(url, self.headers, payload_dict)
        response = requests.request("POST", url, headers=self.headers, json=payload_dict)
        print("Datadog R2D2 Handler Log:: Query V2 TS API", {"response": response.text})
        logger.info("Datadog R2D2 Handler Log:: Query V2 TS API", {"response": response.status_code})
        if response.status_code == 429:
            logger.info('Datadog R2D2 Handler Log:: Query V2 TS API Response: 429. response.headers', response.headers)

        if response.status_code == 200:
            response_json = json.loads(response.text)
            series = response_json['data']['attributes']['series']
            data = response_json['data']['attributes']['values']
            num_of_series = len(series)
            num_of_data = len(data)
            if num_of_series == num_of_data:
                for i in range(num_of_series):
                    series_labels = series[i]['group_tags']
                    if not series_labels:
                        series_labels = ['*']
                    series_data = data[i]
                    if len(series_labels) == len(series_data):
                        for j in range(len(series_labels)):
                            result_dict[series_labels[j]] = series_data[j]
        return result_dict

    def get_downstream_services(self, service_name, env):
        if not env:
            env = 'prod'
        url = self.dd_dependencies_url + "/{}?env={}".format(service_name, env)
        response = requests.request("GET", url, headers=self.headers)
        return json.loads(response.text).get('calls', [])

    def get_upstream_services(self, service_name, env):
        if not env:
            env = 'prod'
        url = self.dd_dependencies_url + "/{}?env={}".format(service_name, env)
        response = requests.request("GET", url, headers=self.headers)
        print('get_upstream_services response', response.status_code)
        return json.loads(response.text).get('called_by', [])

    def fetch_monitors(self):
        try:
            configuration = self.get_connection()
            with ApiClient(configuration) as api_client:
                api_instance = MonitorsApi(api_client)
                response = api_instance.list_monitors()
                return response
        except Exception as e:
            logger.error(f"Exception occurred while fetching monitors with error: {e}")
            raise e

    def query_monitors(self, query: str = None, sort: str = None):
        """
        Query Datadog monitors with optional filters.
        Pagination is handled internally with reasonable defaults.
        
        Args:
            query: Optional query string to filter monitors (e.g., "status:alert type:metric")
            sort: Sort order string (e.g., "name,asc" or "status,desc")
            
        Returns:
            List of monitors matching the query
        """
        try:
            # Pagination defaults - handled internally
            per_page = 100  # Reasonable default for fetching monitors
            
            # Use the search endpoint for query-based filtering
            if query:
                url = f"{self.__dd_host}/api/v1/monitor/search"
                headers = {
                    "DD-API-KEY": self.__dd_api_key,
                    "DD-APPLICATION-KEY": self.__dd_app_key,
                    "Accept": "application/json"
                }
                
                # Datadog monitor search uses GET request with query parameters
                # According to API docs: query, page, per_page, sort are all query string parameters
                all_monitors = []
                max_pages = 50  # Limit to prevent excessive API calls
                
                # Try the search endpoint - if it fails with 400, the query might be invalid
                try:
                    for current_page in range(0, max_pages):  # Start from 0 as per API docs
                        params = {
                            "query": query,
                            "page": current_page,
                            "per_page": per_page
                        }
                        if sort:
                            params["sort"] = sort
                        
                        response = requests.get(url, headers=headers, params=params, timeout=EXTERNAL_CALL_TIMEOUT)
                        
                        # If we get a 400 error, the query syntax might be invalid
                        if response.status_code == 400:
                            error_detail = response.text
                            logger.warning(f"Monitor search returned 400 for query '{query}': {error_detail}")
                            # Return empty result instead of failing
                            return {
                                "monitors": [],
                                "counts": {
                                    "total": 0
                                },
                                "error": f"Invalid query syntax: {error_detail}"
                            }
                        
                        response.raise_for_status()
                        result = response.json()
                        
                        # Check if result has monitors
                        if isinstance(result, dict):
                            monitors = result.get('monitors', [])
                            if not monitors:
                                break  # No more monitors
                            
                            all_monitors.extend(monitors)
                            
                            # Check if there are more results
                            counts = result.get('counts', {})
                            total = counts.get('total', len(all_monitors))
                            
                            if len(all_monitors) >= total or len(monitors) < per_page:
                                break  # Got all monitors or last page
                        else:
                            # Unexpected response format, return what we have
                            break
                    
                    # Return in expected format
                    return {
                        "monitors": all_monitors,
                        "counts": {
                            "total": len(all_monitors)
                        }
                    }
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 400:
                        error_detail = e.response.text
                        logger.warning(f"Monitor search failed with 400 for query '{query}': {error_detail}")
                        # Return empty result instead of raising
                        return {
                            "monitors": [],
                            "counts": {
                                "total": 0
                            },
                            "error": f"Invalid query syntax: {error_detail}"
                        }
                    raise
            else:
                # If no query, use list_monitors (this returns all monitors)
                configuration = self.get_connection()
                with ApiClient(configuration) as api_client:
                    api_instance = MonitorsApi(api_client)
                    response = api_instance.list_monitors()
                    
                    # Convert to consistent format (list of monitor dicts)
                    if hasattr(response, 'to_dict'):
                        # If it's a Datadog API response object, convert it
                        response_dict = response.to_dict()
                        monitors = response_dict if isinstance(response_dict, list) else response_dict.get('monitors', [])
                    elif isinstance(response, list):
                        # If it's already a list, convert each monitor to dict
                        monitors = []
                        for monitor in response:
                            if hasattr(monitor, 'to_dict'):
                                monitors.append(monitor.to_dict())
                            elif isinstance(monitor, dict):
                                monitors.append(monitor)
                            else:
                                monitors.append(monitor)
                    else:
                        # Fallback: try to convert to dict
                        monitors = response if isinstance(response, list) else []
                    
                    # Return in consistent format matching search endpoint
                    return {
                        "monitors": monitors,
                        "counts": {
                            "total": len(monitors)
                        }
                    }
        except Exception as e:
            logger.error(f"Exception occurred while querying monitors with error: {e}")
            raise e

    def create_monitor(self, monitor_definition: dict):
        """
        Create a new Datadog monitor.

        Args:
            monitor_definition: Dictionary containing monitor configuration with required fields:
                - name: Name of the monitor
                - type: Type of monitor (e.g., 'metric alert', 'query alert', 'service check', etc.)
                - query: The monitor query
                Optional fields:
                - message: Message to include with notifications
                - tags: List of tags for the monitor
                - priority: Monitor priority (1-5)
                - options: Additional monitor options (thresholds, notify_no_data, etc.)

        Returns:
            Dictionary containing the created monitor details
        """
        try:
            url = f"{self.__dd_host}/api/v1/monitor"
            headers = {
                "DD-API-KEY": self.__dd_api_key,
                "DD-APPLICATION-KEY": self.__dd_app_key,
                "Content-Type": "application/json",
                "Accept": "application/json"
            }

            response = requests.post(url, headers=headers, json=monitor_definition, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            error_detail = e.response.text if e.response else str(e)
            logger.error(f"Failed to create monitor: {error_detail}")
            raise Exception(f"Failed to create monitor: {error_detail}")
        except Exception as e:
            logger.error(f"Exception occurred while creating monitor: {e}")
            raise e

    def update_monitor(self, monitor_id: int, monitor_definition: dict):
        """
        Update an existing Datadog monitor.

        Args:
            monitor_id: ID of the monitor to update
            monitor_definition: Dictionary containing monitor fields to update:
                - name: Name of the monitor
                - type: Type of monitor
                - query: The monitor query
                - message: Message to include with notifications
                - tags: List of tags for the monitor
                - priority: Monitor priority (1-5)
                - options: Additional monitor options

        Returns:
            Dictionary containing the updated monitor details
        """
        try:
            url = f"{self.__dd_host}/api/v1/monitor/{monitor_id}"
            headers = {
                "DD-API-KEY": self.__dd_api_key,
                "DD-APPLICATION-KEY": self.__dd_app_key,
                "Content-Type": "application/json",
                "Accept": "application/json"
            }

            response = requests.put(url, headers=headers, json=monitor_definition, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            error_detail = e.response.text if e.response else str(e)
            logger.error(f"Failed to update monitor {monitor_id}: {error_detail}")
            raise Exception(f"Failed to update monitor {monitor_id}: {error_detail}")
        except Exception as e:
            logger.error(f"Exception occurred while updating monitor {monitor_id}: {e}")
            raise e

    def delete_monitor(self, monitor_id: int):
        """
        Delete a Datadog monitor.

        Args:
            monitor_id: ID of the monitor to delete

        Returns:
            Dictionary containing deletion confirmation
        """
        try:
            url = f"{self.__dd_host}/api/v1/monitor/{monitor_id}"
            headers = {
                "DD-API-KEY": self.__dd_api_key,
                "DD-APPLICATION-KEY": self.__dd_app_key,
                "Accept": "application/json"
            }

            response = requests.delete(url, headers=headers, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return {"deleted_monitor_id": monitor_id, "success": True}
        except requests.exceptions.HTTPError as e:
            error_detail = e.response.text if e.response else str(e)
            logger.error(f"Failed to delete monitor {monitor_id}: {error_detail}")
            raise Exception(f"Failed to delete monitor {monitor_id}: {error_detail}")
        except Exception as e:
            logger.error(f"Exception occurred while deleting monitor {monitor_id}: {e}")
            raise e

    def fetch_dashboards(self):
        try:
            configuration = self.get_connection()
            with ApiClient(configuration) as api_client:
                api_instance = DashboardsApi(api_client)
                response = api_instance.list_dashboards()
                dashboard_dict = response.to_dict()

                import json

                def convert_datetime(value):
                    if isinstance(value, dict):
                        return {k: convert_datetime(v) for k, v in value.items()}
                    if isinstance(value, list):
                        return [convert_datetime(item) for item in value]
                    if isinstance(value, datetime):
                        return value.isoformat()
                    return value

                cleaned_response = convert_datetime(dashboard_dict)
                return json.dumps(cleaned_response, indent=2)
        except Exception as e:
            logger.error(f"Exception occurred while fetching dashboards with error: {e}")
            raise e

    def fetch_dashboard_details(self, dashboard_id):
        try:
            configuration = self.get_connection()
            with ApiClient(configuration) as api_client:
                api_instance = DashboardsApi(api_client)
                response = api_instance.get_dashboard(
                    dashboard_id=dashboard_id,
                )
                return response.to_dict()
        except Exception as e:
            logger.error(f"Exception occurred while fetching dashboard details with error: {e}")
            raise e

    def fetch_aws_integrations(self):
        try:
            configuration = self.get_connection()
            with ApiClient(configuration) as api_client:
                api_instance = AWSIntegrationApi(api_client)
                response = api_instance.list_aws_accounts()
                return response.to_dict()
        except ApiException as e:
            logger.error("Exception when calling AWSIntegrationApi->list_aws_accounts: %s\n" % e)
            raise e
        except Exception as e:
            logger.error(f"Exception occurred while fetching AWS integrations with error: {e}")
            raise e

    def fetch_aws_log_integrations(self):
        try:
            configuration = self.get_connection()
            with ApiClient(configuration) as api_client:
                api_instance = AWSLogsIntegrationApi(api_client)
                response = api_instance.list_aws_logs_integrations()
                return response
        except ApiException as e:
            logger.error("Exception when calling AWSLogsIntegrationApi->list_aws_logs_integrations: %s\n" % e)
            raise e
        except Exception as e:
            logger.error(f"Exception occurred while fetching AWS log integrations with error: {e}")
            raise e

    def fetch_azure_integrations(self):
        try:
            configuration = self.get_connection()
            with ApiClient(configuration) as api_client:
                api_instance = AzureIntegrationApi(api_client)
                response: AzureAccountListResponse = api_instance.list_azure_integration()
                return response
        except ApiException as e:
            logger.error("Exception when calling AzureIntegrationApi->list_azure_integration: %s\n" % e)
            raise e
        except Exception as e:
            logger.error(f"Exception occurred while fetching Azure integrations with error: {e}")
            raise e

    def fetch_cloudflare_integrations(self):
        try:
            configuration = self.get_connection()
            with ApiClient(configuration) as api_client:
                api_instance = CloudflareIntegrationApi(api_client)
                response = api_instance.list_cloudflare_accounts()
                return response.to_dict()
        except ApiException as e:
            logger.error("Exception when calling CloudflareIntegrationApi->list_cloudflare_accounts: %s\n" % e)
            raise e
        except Exception as e:
            logger.error(f"Exception occurred while fetching Cloudflare integrations with error: {e}")
            raise e

    def fetch_confluent_integrations(self):
        try:
            configuration = self.get_connection()
            with ApiClient(configuration) as api_client:
                api_instance = ConfluentCloudApi(api_client)
                response = api_instance.list_confluent_account()
                return response.to_dict()
        except ApiException as e:
            logger.error("Exception when calling ConfluentCloudApi->list_confluent_account: %s\n" % e)
            raise e
        except Exception as e:
            logger.error(f"Exception occurred while fetching Confluent integrations with error: {e}")
            raise e

    def fetch_fastly_integrations(self):
        try:
            configuration = self.get_connection()
            with ApiClient(configuration) as api_client:
                api_instance = FastlyIntegrationApi(api_client)
                response = api_instance.list_fastly_accounts()
                return response.to_dict()
        except ApiException as e:
            logger.error("Exception when calling FastlyIntegrationApi->list_fastly_accounts: %s\n" % e)
            raise e
        except Exception as e:
            logger.error(f"Exception occurred while fetching Fastly integrations with error: {e}")
            raise e

    def fetch_gcp_integrations(self):
        try:
            configuration = self.get_connection()
            with ApiClient(configuration) as api_client:
                api_instance = GCPIntegrationApi(api_client)
                response = api_instance.list_gcpsts_accounts()
                return response.to_dict()
        except ApiException as e:
            logger.error("Exception when calling GCPIntegrationApi->list_gcpsts_accounts: %s\n" % e)
            raise e
        except Exception as e:
            logger.error(f"Exception occurred while fetching GCP integrations with error: {e}")
            raise e

    def fetch_service_map(self, env, start=None, end=None):
        try:
            if not env:
                env = 'prod'
            url = self.dd_dependencies_url + "/?env={}".format(env)
            if start and end:
                url += "&start={}&end={}".format(int(start), int(end))

            response = requests.request("GET", url, headers=self.headers)
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            print(f"Exception occurred while fetching service map with error: {e}")
            raise e

    def fetch_metrics(self, filter_tags=None):
        try:
            configuration = self.get_connection()
            with ApiClient(configuration) as api_client:
                api_instance = MetricsApi(api_client)
                if filter_tags:
                    response = api_instance.list_tag_configurations(filter_tags=filter_tags)
                else:
                    response = api_instance.list_tag_configurations()
                return response.to_dict()
        except ApiException as e:
            logger.error("Exception when calling MetricsApi->list_tag_configurations: %s\n" % e)
            raise e
        except Exception as e:
            logger.error(f"Exception occurred while fetching metrics with error: {e}")
            raise e

    def fetch_metric_tags(self, metric_name):
        try:
            configuration = self.get_connection()
            with ApiClient(configuration) as api_client:
                api_instance = MetricsApi(api_client)
                response = api_instance.list_tags_by_metric_name(metric_name=metric_name)
                return response.to_dict()
        except ApiException as e:
            # Check if it's a 429 rate limit error
            if e.status == 429:
                logger.warning(f"Rate limit exceeded when fetching metric tags for {metric_name}. Headers: {e.headers}")
                # Extract rate limit reset time from headers (Datadog uses x-ratelimit-reset)
                rate_limit_reset = e.headers.get('x-ratelimit-reset')
                if rate_limit_reset:
                    import time
                    reset_time = int(rate_limit_reset)
                    logger.info(f"Rate limit will reset in {reset_time} seconds...")
                    time.sleep(reset_time)
                    # Retry the request once
                    return self.fetch_metric_tags(metric_name)
                else:
                    # Fallback: wait 60 seconds if no reset time provided
                    logger.warning("No rate limit reset time provided, waiting 60 seconds...")
                    time.sleep(60)
                    return self.fetch_metric_tags(metric_name)
            else:
                logger.error("Exception when calling MetricsApi->list_tags_by_metric_name: %s\n" % e)
                raise e
        except Exception as e:
            logger.error("Exception occurred while fetching metric tags with error: %s\n" % e)
            raise e

    def fetch_logs(self, query, tr: TimeRange, limit=100):
        try:
            configuration = self.get_connection()
            from_tr = str(tr.time_geq * 1000)
            to_tr = str(tr.time_lt * 1000)
            body = LogsListRequest(
                filter=LogsQueryFilter(
                    query=query,
                    _from=from_tr,
                    to=to_tr,
                ),
                sort=LogsSort.TIMESTAMP_DESCENDING,
                page=LogsListRequestPage(
                    limit=limit,
                ),
            )
            with ApiClient(configuration) as api_client:
                api_instance = LogsApi(api_client)
                response = api_instance.list_logs(body=body)
                result = response.data
                return result

        except ApiException as e:
            logger.error("Exception when calling LogsApi->list_logs: %s\n" % e)
            raise e
        except Exception as e:
            logger.error(f"Exception occurred while fetching logs with error: {e}")
            raise e
    

    def widget_query_timeseries_points_api(self, tr: TimeRange, queries, formulas, resource_type=None, interval_ms=None):
        try:
            request_type = 'scalar_request'
            url = self.__dd_host + "/api/v2/query/scalar"
            attribute_payload = {'queries': queries, 'formulas': formulas}
            from_tr = int(tr.time_geq * 1000)
            to_tr = int(tr.time_lt * 1000)
            attribute_payload['from'] = from_tr
            attribute_payload['to'] = to_tr

            if resource_type == 'timeseries':
                url = self.__dd_host + "/api/v2/query/timeseries"
                # Only set interval if explicitly provided
                if interval_ms:
                    attribute_payload['interval'] = interval_ms
                    logger.info(f"Using provided interval of {interval_ms} ms for time range: {from_tr} to {to_tr}")
                else:
                    logger.info(f"Letting Datadog API determine the appropriate interval for time range: {from_tr} to {to_tr}")
                request_type = 'timeseries_request'
            
            payload = {'data': {'attributes': attribute_payload, 'type': request_type}}
            response = requests.request("POST", url, headers=self.headers, json=payload)
            if response.status_code == 429:
                logger.info('Datadog R2D2 Handler Log:: Query V2 TS API Response: 429. response.headers',
                            response.headers)
                return None
            elif response.status_code == 200:
                response_json = json.loads(response.text)
                return response_json
            else:
                logger.error(f"Datadog R2D2 Handler Log:: Query V2 TS API Response: {response.status_code}. "
                             f"response.text: {response.text}")
            return None
        except ApiException as e:
            logger.error("Exception when calling MetricsApi->metric: %s\n" % e)
            raise e
        except Exception as e:
            logger.error(f"Exception occurred while fetching metric with error: {e}")
            raise e

    def widget_query_logs_stream_api(self, tr: TimeRange, query_string: str, limit: int = 1000):
        """
        Queries logs data for a widget of type 'list_stream' using the logs/events/search API.

        :param tr: TimeRange object with time_geq and time_lt attributes (Unix time in seconds).
        :param query_string: Log query string from the widget (e.g., 'service:holocron-celery-worker AND investigation_session_id:abc123').
        :param limit: Max number of logs to retrieve (default is 1000).
        :return: Parsed JSON response or None on failure.
        """
        try:
            url = self.__dd_host + "/api/v2/logs/events/search"
            payload = {
                "filter": {
                    "from": datetime.fromtimestamp(tr.time_geq, tz=timezone.utc).isoformat(),
                    "to": datetime.fromtimestamp(tr.time_lt, tz=timezone.utc).isoformat(),

                    "query": "service:holocron-celery-worker"#query_string
                },
                "sort": "desc",
                "page": {
                    "limit": limit
                }
            }

            response = requests.request("POST", url, headers=self.headers, json=payload)

            if response.status_code == 429:
                logger.warning('Datadog R2D2 Handler Log:: Logs Stream API Response: 429 - Rate Limited. Headers: %s',
                            response.headers)
                return None
            elif response.status_code == 200:
                response_json = json.loads(response.text)
                return response_json
            else:
                logger.error(f"Datadog R2D2 Handler Log:: Logs Stream API Response: {response.status_code}. "
                            f"Response Body: {response.text}")
                return None

        except ApiException as e:
            logger.error("Exception when calling LogsApi->logs_search: %s\n" % e)
            raise e
        except Exception as e:
            logger.error(f"Exception occurred while fetching logs stream data: {e}")
            raise e

    def execute_raw_query(self, start_time, end_time, query, interval=None):
        """
        Execute a raw Datadog query directly using the metrics query API.
        
        Args:
            start_time (int): Start time in Unix timestamp (seconds)
            end_time (int): End time in Unix timestamp (seconds)
            query (str): The Datadog query string (e.g. "avg:system.cpu.user{*}")
            interval (int, optional): Interval in seconds for data point rollup
            
        Returns:
            dict: The response from Datadog API containing the query results
        """
        try:
            url = self.__dd_host + "/api/v1/query"
            
            params = {
                "from": int(start_time),
                "to": int(end_time),
                "query": query
            }
            
            # Only add interval if specified
            if interval:
                params["interval"] = interval

            response = requests.get(url, params=params, headers=self.headers)
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Error executing Datadog query. Status code: {response.status_code}, Response: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Exception occurred while executing raw query: {e}")
            raise e

    def get_dashboard_config_details(self, dashboard_id):
        """
        Get detailed configuration for a specific dashboard.
        
        Args:
            dashboard_id (str): The dashboard ID to get configuration for
            
        Returns:
            str: JSON string of the dashboard configuration details
        """
        try:
            # Get raw dashboard details from Datadog API
            dashboard_details = self.fetch_dashboard_details(dashboard_id)
            
            if not dashboard_details:
                return None
            
            # Convert to JSON string with datetime handling
            import json
            from datetime import datetime
            
            def json_serializer(obj):
                """JSON serializer for objects not serializable by default json code"""
                if isinstance(obj, datetime):
                    return obj.isoformat()
                raise TypeError(f"Type {type(obj)} not serializable")
            
            return json.dumps(dashboard_details, indent=2, default=json_serializer)
                
        except Exception as e:
            logger.error(f"Exception occurred while getting dashboard config details: {e}")
            raise e

    def get_dashboard_variable_values(self, dashboard_id, variable_name=None):
        """
        Get available values for dashboard template variables.
        
        Args:
            dashboard_id (str): The dashboard ID to get variable values for
            variable_name (str, optional): Specific variable name to get values for
            
        Returns:
            str: JSON string of the template variables
        """
        try:
            # Get raw dashboard details and extract template variables
            dashboard_details = self.fetch_dashboard_details(dashboard_id)
            
            if not dashboard_details:
                return None
            
            template_variables = dashboard_details.get('template_variables', [])
            
            # If variable_name is specified, filter the variables
            if variable_name:
                template_variables = [var for var in template_variables if var.get('name') == variable_name]
            
            result = {
                'template_variables': template_variables,
                'dashboard_id': dashboard_id,
                'variable_name': variable_name
            }
            
            # Convert to JSON string with datetime handling
            import json
            from datetime import datetime
            
            def json_serializer(obj):
                """JSON serializer for objects not serializable by default json code"""
                if isinstance(obj, datetime):
                    return obj.isoformat()
                raise TypeError(f"Type {type(obj)} not serializable")
            
            return json.dumps(result, indent=2, default=json_serializer)
            
        except Exception as e:
            logger.error(f"Exception occurred while getting dashboard variable values: {e}")
            raise e

    def fetch_logs_for_field_extraction(self, start_time, end_time, limit=10000):
        """
        Fetch logs for field extraction analysis with pagination support.
        
        Args:
            start_time (int): Start time in Unix timestamp (seconds)
            end_time (int): End time in Unix timestamp (seconds)
            limit (int): Maximum number of logs to fetch (default: 10000)
            
        Returns:
            list: List of log entries for field analysis
        """
        logger.info(f"fetch_logs_for_field_extraction called with limit={limit}, start_time={start_time}, end_time={end_time}")
        try:
            # Use a broad query to get diverse log samples
            query = "*"  # Get all logs
            
            # Convert timestamps to milliseconds for Datadog API
            from_tr = str(start_time * 1000)
            to_tr = str(end_time * 1000)
            
            all_logs = []
            cursor = None
            batch_size = 1000  # Datadog API limit per request
            total_fetched = 0
            previous_cursor = None
            
            configuration = self.get_connection()
            
            while total_fetched < limit:
                # Calculate how many logs to fetch in this batch
                remaining = limit - total_fetched
                current_batch_size = min(batch_size, remaining)
                
                # Prepare request body
                page_config = LogsListRequestPage(limit=current_batch_size)
                if cursor:
                    page_config.cursor = cursor
                    logger.info(f"Using cursor for pagination: {cursor}")
                
                body = LogsListRequest(
                    filter=LogsQueryFilter(
                        query=query,
                        _from=from_tr,
                        to=to_tr,
                    ),
                    sort=LogsSort.TIMESTAMP_DESCENDING,
                    page=page_config,
                )
                
                with ApiClient(configuration) as api_client:
                    api_instance = LogsApi(api_client)
                    response = api_instance.list_logs(body=body)
                    
                    if not response or not response.data:
                        logger.info(f"No more logs available. Fetched {total_fetched} logs total.")
                        break
                    
                    # Convert to list of dictionaries for easier processing
                    batch_logs = []
                    for log in response.data:
                        log_dict = log.to_dict()
                        batch_logs.append(log_dict)
                    
                    all_logs.extend(batch_logs)
                    total_fetched += len(batch_logs)
                    
                    logger.info(f"Fetched {len(batch_logs)} logs in this batch. Total: {total_fetched}")
                    
                    # Debug: Log response structure to understand pagination
                    logger.info(f"Response type: {type(response)}")
                    if hasattr(response, 'meta'):
                        logger.info(f"Response meta: {response.meta}")
                        if hasattr(response.meta, 'page'):
                            logger.info(f"Response meta.page: {response.meta.page}")
                            if hasattr(response.meta.page, 'after'):
                                logger.info(f"Response meta.page.after: {response.meta.page.after}")
                    
                    # Check if we have a next page cursor
                    # For v2 API, the cursor is in response.meta.page.after
                    cursor = None
                    if hasattr(response, 'meta') and hasattr(response.meta, 'page'):
                        if hasattr(response.meta.page, 'after') and response.meta.page.after:
                            cursor = response.meta.page.after
                            logger.info(f"Found pagination cursor: {cursor}")
                            
                            # Check if cursor is the same as previous (infinite loop protection)
                            if cursor == previous_cursor:
                                logger.warning("Cursor is the same as previous request. Breaking to avoid infinite loop.")
                                break
                            previous_cursor = cursor
                        else:
                            logger.info("No pagination cursor found. Reached end of logs.")
                            break
                    else:
                        # If no pagination info, we've likely reached the end
                        logger.info("No pagination meta found. Assuming end of logs.")
                        break
                    
                    # Add a small delay between requests to be respectful to the API
                    import time
                    time.sleep(0.1)
            
            logger.info(f"Successfully fetched {len(all_logs)} logs for field extraction")
            return all_logs
                    
        except ApiException as e:
            logger.error(f"Exception when calling LogsApi->list_logs for field extraction: {e}")
            if e.status == 429:
                logger.warning("Rate limit exceeded when fetching logs for field extraction")
            return all_logs if 'all_logs' in locals() else []
        except Exception as e:
            logger.error(f"Exception occurred while fetching logs for field extraction: {e}")
            return all_logs if 'all_logs' in locals() else []

def format_results_as_entries(results):
    """
    Transform the results into the desired format with an entries array
    """
    entries = []

    for result in results:
        entry = {
            "service_name": result.get('service'),
            "downstream_services": result.get('downstream').split(',') if result.get('downstream') else []
        }

        # Only include fields that have values
        entry = {k: v for k, v in entry.items() if v}

        entries.append(entry)

    return {"entries": entries}


def extract_services_and_downstream(account_id, model_data=None):
    logger.info("Processing service relationships...")
    try:
        service_downstream_map = {}

        # If model_data is provided, use it instead of querying the database
        if model_data:
            for service, metadata in model_data.items():
                if service not in service_downstream_map:
                    service_downstream_map[service] = set()

                # Look for 'calls' in production environments
                prod_env_tags = ['prod', 'production', 'prd', 'prod_env', 'production_env',
                                 'production_environment', 'prod_environment']

                for env_tag in prod_env_tags:
                    if env_tag in metadata and 'calls' in metadata[env_tag]:
                        downstream_services = metadata[env_tag]['calls']
                        if downstream_services:
                            service_downstream_map[service].update(downstream_services)

        results = [
            {'service': service, 'downstream': ','.join(sorted(downstream)) if downstream else ''}
            for service, downstream in service_downstream_map.items()]

        if results:
            logger.info(f"Total unique services: {len(results)}")
            service_catalog_data = format_results_as_entries(results)

            return service_catalog_data
        else:
            logger.info("No data found matching the criteria")
            empty_catalog = {"entries": []}

        return empty_catalog
    except Exception as e:
        logger.error(f"Error executing query or processing results: {e}")
        import traceback
        traceback.print_exc()
        empty_catalog = {"entries": []}

        return empty_catalog