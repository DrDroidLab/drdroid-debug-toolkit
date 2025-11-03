import logging
import requests
import json
import base64
from typing import List, Dict, Any
from datetime import datetime, timedelta
from core.protos.base_pb2 import TimeRange

from elasticsearch import Elasticsearch

from core.integrations.processor import Processor
from core.settings import EXTERNAL_CALL_TIMEOUT

logger = logging.getLogger(__name__)


class ElasticSearchApiProcessor(Processor):
    client = None

    # Thresholds in seconds (descending order) mapped to ES interval strings
    _INTERVAL_THRESHOLDS = [
        (31536001, "1d"),  # > 1 year
        (2592001, "12h"),  # > 30 days
        (604801, "6h"),  # > 7 days
        (86401, "3h"),  # > 1 day
        (43201, "1h"),  # > 12 hours
        (21601, "30m"),  # > 6 hours
        (3601, "5m"),  # > 1 hour
        (1800, "2m"),  # >= 30 minutes
        (1200, "1m"),  # >= 20 minutes
    ]
    _DEFAULT_INTERVAL = "30s"  # Interval for ranges < 20 minutes

    def __init__(self, protocol: str, host: str, port: str, api_key_id: str, api_key: str, verify_certs: bool = False,
                 kibana_host: str = None):
        self.protocol = protocol
        self.host = host
        self.port = int(port) if port else 9200
        self.verify_certs = verify_certs
        self.__api_key_id = api_key_id
        self.__api_key = api_key
        self.encoded_api_key = base64.b64encode(f"{self.__api_key_id}:{self.__api_key}".encode()).decode()

        self.kibana_host = kibana_host
        self.headers = {
            "Authorization": f"ApiKey {self.__api_key}",
            "Content-Type": "application/json"
        }
        self.kibana_headers = {
            "Authorization": f"ApiKey {self.encoded_api_key}",
            "Content-Type": "application/json",
            "kbn-xsrf": "true"  # Required for Kibana API calls
        }
        self.apm_headers = {
            "Authorization": f"ApiKey {self.encoded_api_key}",
            "Content-Type": "application/json",
        }

    def get_connection(self):
        try:
            import elasticsearch  # Import the module itself
            client = elasticsearch.Elasticsearch(
                [f"{self.protocol}://{self.host}:{self.port}"],
                api_key=(self.__api_key_id, self.__api_key),
                verify_certs=self.verify_certs
            )
            return client
        except Exception as e:
            logger.error(f"Exception occurred while creating elasticsearch connection with error: {e}")
            raise e

    def test_connection(self):
        try:
            connection = self.get_connection()
            indices = connection.indices.get_alias()
            connection.close()
            if len(list(indices.keys())) > 0:
                return True
            else:
                raise Exception("Elasticsearch Connection Error:: No indices found in elasticsearch")
        except Exception as e:
            logger.error(f"Exception occurred while fetching elasticsearch indices with error: {e}")
            raise e

    def fetch_indices(self):
        try:
            connection = self.get_connection()
            indices = connection.indices.get_alias()
            connection.close()
            return list(indices.keys())
        except Exception as e:
            logger.error(f"Exception occurred while fetching elasticsearch indices with error: {e}")
            raise e

    def query(self, index, query):
        try:
            connection = self.get_connection()
            result = connection.search(index=index, body=query, pretty=True)
            connection.close()
            return result
        except Exception as e:
            # Handle index not found errors specifically
            error_msg = str(e)
            if ("NotFoundError" in str(type(e).__name__) or 
                "index_not_found_exception" in error_msg or 
                "no such index" in error_msg.lower()):
                logger.warning(f"Elasticsearch index '{index}' not found: {e}")
                # Return an empty result structure that matches what the caller expects
                return {
                    "hits": {
                        "hits": [],
                        "total": {"value": 0}
                    }
                }
            
            logger.error(f"Exception occurred while fetching elasticsearch data with error: {e}")
            raise e

    def get_document(self, index, doc_id):
        try:
            connection = self.get_connection()
            result = connection.get(index=index, id=doc_id, pretty=True, preference="_primary_first")
            connection.close()
            return result
        except Exception as e:
            logger.error(f"Exception occurred while fetching elasticsearch data with error: {e}")
            raise e

    def get_cluster_health(self):
        try:
            connection = self.get_connection()
            result = connection.cluster.health()
            connection.close()

            # Convert ObjectApiResponse to dict
            if hasattr(result, 'body'):
                # For Elasticsearch 8.x client
                return result.body
            elif hasattr(result, 'meta'):
                # Alternative approach for some client versions
                return dict(result)
            else:
                # Fallback for older client versions
                return result
        except Exception as e:
            logger.error(f"Exception occurred while fetching elasticsearch cluster health with error: {e}")
            raise e

    def get_nodes_stats(self):
        try:
            connection = self.get_connection()
            result = connection.nodes.stats()
            connection.close()

            # Convert response to dict
            if hasattr(result, 'body'):
                return result.body
            elif hasattr(result, 'meta'):
                return dict(result)
            else:
                return result
        except Exception as e:
            logger.error(f"Exception occurred while fetching elasticsearch nodes stats with error: {e}")
            raise e

    def get_cat_indices(self):
        try:
            connection = self.get_connection()
            result = connection.cat.indices(v=True, format="json")
            connection.close()

            # Convert response to dict
            if hasattr(result, 'body'):
                return result.body
            elif hasattr(result, 'meta'):
                return dict(result)
            else:
                return result
        except Exception as e:
            logger.error(f"Exception occurred while fetching elasticsearch cat indices with error: {e}")
            raise e

    def get_cat_thread_pool_search(self):
        try:
            connection = self.get_connection()
            result = connection.cat.thread_pool(thread_pool_patterns="search", v=True, format="json")
            connection.close()

            # Convert response to dict
            if hasattr(result, 'body'):
                return result.body
            elif hasattr(result, 'meta'):
                return dict(result)
            else:
                return result
        except Exception as e:
            logger.error(f"Exception occurred while fetching elasticsearch cat thread pool search with error: {e}")
            raise e

    def _calculate_histogram_interval(self, start_time, end_time):
        """
        Calculate the appropriate histogram interval based on the time range.
        Uses standard intervals defined in _INTERVAL_THRESHOLDS.
        
        Args:
            start_time (int): Start time in epoch seconds
            end_time (int): End time in epoch seconds
            
        Returns:
            str: Elasticsearch interval string (e.g., "30s", "1h", "1d")
        """
        time_range_seconds = end_time - start_time

        for threshold, interval in self._INTERVAL_THRESHOLDS:
            if time_range_seconds >= threshold:
                return interval

        return self._DEFAULT_INTERVAL

    def fetch_monitoring_cluster_stats(self, start_time=None, end_time=None, interval=None):
        client = self.get_connection()
        try:
            # Calculate appropriate interval based on time range
            if not interval:
                interval = self._calculate_histogram_interval(start_time, end_time)

            result = client.search(
                index=".monitoring-es-*",
                body={
                    "size": 0,
                    "query": {
                        "range": {
                            "@timestamp": {
                                "gte": start_time,
                                "lt": end_time,
                                "format": "epoch_second"
                            }
                        }
                    },
                    "aggs": {
                        "per_minute": {
                            "date_histogram": {
                                "field": "@timestamp",
                                "fixed_interval": interval
                            },
                            "aggs": {
                                "query_total": {
                                    "max": {
                                        "field": "indices_stats._all.total.search.query_total"
                                    }
                                },
                                "search_rate": {
                                    "derivative": {
                                        "buckets_path": "query_total",
                                        "unit": "second",
                                    }
                                },
                                "index_total": {
                                    "max": {
                                        "field": "indices_stats._all.total.indexing.index_total"
                                    }
                                },
                                "indexing_rate": {
                                    "derivative": {
                                        "buckets_path": "index_total",
                                        "unit": "second"
                                    }
                                },
                                "index_primary": {
                                    "max": {
                                        "field": "indices_stats._all.primaries.indexing.index_total"
                                    }
                                },
                                "indexing_rate_primary": {
                                    "derivative": {
                                        "buckets_path": "index_primary",
                                        "unit": "second"
                                    }
                                },
                                "query_total_sum": {
                                    "sum": {
                                        "field": "indices_stats._all.total.search.query_total"
                                    }
                                },
                                "query_time_sum": {
                                    "sum": {
                                        "field": "indices_stats._all.total.search.query_time_in_millis"
                                    }
                                },
                                "index_primary_sum": {
                                    "sum": {
                                        "field": "indices_stats._all.primaries.indexing.index_total"
                                    }
                                },
                                "index_time": {
                                    "sum": {
                                        "field": "elasticsearch.index.primaries.indexing.index_time_in_millis"
                                    }
                                }
                            }
                        }
                    }
                }
            )
            client.close()
            # Convert response to dict
            if hasattr(result, 'body'):
                return result.body
            elif hasattr(result, 'meta'):
                return dict(result)
            else:
                return result
        except Exception as e:
            logger.error(f"Error fetching monitoring data: {e}")
            raise e

    ######################################################## APM TASKS ########################################################

    def get_time_series_metrics(self, service_name: str, time_window: str, start_time=None, end_time=None,
                                interval: str = "5m", indices: List[str] = ["traces-apm-*"]) -> List[Dict]:
        """
        Get time series metrics (throughput, error rate, latency) for a service.
        
        Args:
            service_name (str): Name of the service to get metrics for
            time_window (str): Time window for metrics calculation (e.g., "1h", "1d")
            start_time (datetime): Start time for metrics calculation
            end_time (datetime): End time for metrics calculation
            interval (str): Interval for time series data (e.g., "5m", "1h")
        
        Returns:
            List[Dict]: List of dictionaries containing metrics for each time interval
        """
        try:
            # Set default end_time to now if not provided
            if end_time is None:
                end_time = datetime.utcnow()

            # Calculate start_time based on time_window if not provided
            if start_time is None:
                try:
                    if time_window.endswith('h'):
                        hours = int(time_window[:-1])
                        start_time = end_time - timedelta(hours=hours)
                    elif time_window.endswith('d'):
                        days = int(time_window[:-1])
                        start_time = end_time - timedelta(days=days)
                    else:
                        # Default to 1 hour if time_window format is invalid
                        logger.warning(f"Invalid time window format: {time_window}. Using default 1h window.")
                        start_time = end_time - timedelta(hours=1)
                except ValueError:
                    logger.warning(f"Invalid time window format: {time_window}. Using default 1h window.")
                    start_time = end_time - timedelta(hours=1)

            # Format timestamps for Elasticsearch
            start_time_str = start_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            end_time_str = end_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")

            time_series_data = []

            for index_pattern in indices:
                client = self.get_connection()
                try:
                    logger.info(f"Querying time series metrics for service: {service_name}")
                    logger.info(f"Time range: {start_time_str} to {end_time_str}")
                    logger.info(f"Interval: {interval}")

                    result = client.search(
                        index=index_pattern,
                        body={
                            "size": 0,
                            "query": {
                                "bool": {
                                    "must": [
                                        {"term": {"service.name": service_name}},
                                        {"range": {
                                            "@timestamp": {
                                                "gte": start_time_str,
                                                "lte": end_time_str
                                            }
                                        }}
                                    ]
                                }
                            },
                            "aggs": {
                                "time_series": {
                                    "date_histogram": {
                                        "field": "@timestamp",
                                        "fixed_interval": interval,
                                        "min_doc_count": 0
                                    },
                                    "aggs": {
                                        "throughput": {
                                            "value_count": {
                                                "field": "transaction.id"
                                            }
                                        },
                                        "error_count": {
                                            "terms": {
                                                "field": "transaction.result",
                                                "size": 10
                                            }
                                        },
                                        "latency_p95": {
                                            "percentiles": {
                                                "field": "transaction.duration.us",
                                                "percents": [95],
                                                "missing": 0
                                            }
                                        },
                                        "latency_p99": {
                                            "percentiles": {
                                                "field": "transaction.duration.us",
                                                "percents": [99],
                                                "missing": 0
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    )

                    # Convert response to dict
                    if hasattr(result, 'body'):
                        result = result.body
                    elif hasattr(result, 'meta'):
                        result = dict(result)

                    if 'aggregations' in result:
                        buckets = result['aggregations']['time_series']['buckets']

                        for bucket in buckets:
                            timestamp = bucket['key_as_string']
                            total_requests = bucket['throughput']['value']

                            # Calculate error count
                            error_count = 0
                            for error_bucket in bucket['error_count']['buckets']:
                                if error_bucket['key'] == 'error':
                                    error_count = error_bucket['doc_count']
                                    break

                            # Get latency values
                            latency_p95 = bucket['latency_p95']['values']['95.0']
                            latency_p99 = bucket['latency_p99']['values']['99.0']

                            # Calculate metrics for this interval
                            interval_seconds = pd.Timedelta(interval).total_seconds()
                            metrics = {
                                "timestamp": timestamp,
                                "throughput": round(total_requests / interval_seconds, 2),  # requests per second
                                "error_rate": round((error_count / total_requests * 100) if total_requests > 0 else 0,
                                                    2),  # percentage
                                "latency_p95": round(latency_p95 / 1000, 2),  # convert to milliseconds
                                "latency_p99": round(latency_p99 / 1000, 2),  # convert to milliseconds
                                "total_requests": total_requests
                            }
                            time_series_data.append(metrics)

                        return time_series_data

                except Exception as e:
                    logger.error(f"Error querying metrics for index {index_pattern}: {str(e)}")
                    continue
                finally:
                    client.close()

            return time_series_data

        except Exception as e:
            logger.error(f"Exception occurred while fetching time series metrics with error: {e}")
            raise e

    ######################################################## KIBANA DASHBOARDS TASKS ########################################################
    def get_dashboard_by_name(self, dashboard_name: str) -> Dict[str, Any]:
        """
        Fetch dashboard details with improved Lens panel parsing
        """
        url = f"https://{self.kibana_host}/api/saved_objects/_find?type=dashboard"
        params = {
            "type": "dashboard",
            "fields": ["title", "panelsJSON", "attributes"],
            "per_page": 1000
        }

        try:
            response = requests.get(url, headers=self.kibana_headers, params=params)
            response.raise_for_status()

            for obj in response.json().get('saved_objects', []):
                if obj.get('attributes', {}).get('title') == dashboard_name:
                    dashboard_data = obj
                    panels = dashboard_data.get('attributes', {}).get('panelsJSON', '[]')

                    try:
                        panels_data = json.loads(panels)
                    except json.JSONDecodeError:
                        panels_data = []

                    widgets = []
                    for panel in panels_data:
                        # Use panel ID as fallback identifier
                        widget_id = panel.get('panelIndex') or panel.get('id')
                        widget = {
                            'id': widget_id,
                            'type': panel.get('type'),
                        }

                        # Extract Lens-specific configuration
                        if widget['type'] == 'lens':
                            # Get access to embedded attributes and state
                            attributes = panel.get('embeddableConfig', {}).get('attributes', {})
                            state = attributes.get('state', {})
                            visualization = state.get('visualization', {})

                            # Extract title (with fallback)
                            title = attributes.get('title') or visualization.get('title')
                            if not title:
                                # Find a meaningful name from the visualization if possible
                                layers = visualization.get('layers', [])
                                if layers and 'splitAccessor' in layers[0]:
                                    # Try to use the split field as part of the title
                                    datasource_states = state.get('datasourceStates', {})
                                    form_based = datasource_states.get('formBased', {}).get('layers', {})
                                    for layer_id, layer_data in form_based.items():
                                        columns = layer_data.get('columns', {})
                                        split_accessor = layers[0].get('splitAccessor')
                                        if split_accessor and split_accessor in columns:
                                            field = columns[split_accessor].get('sourceField')
                                            if field:
                                                title = f"Chart of {field}"
                                                break

                            widget['title'] = title or f"Unnamed_{widget_id}"

                            # Extract accessor information from layers with intuitive naming
                            widget['yaxis'] = []  # Y-axis metrics (accessors)
                            widget['xaxis'] = []  # X-axis dimension (xAccessor)
                            widget['splits'] = []  # Series/Category splits (splitAccessor)

                            layers = visualization.get('layers', [])
                            for layer in layers:
                                # Get corresponding columns data
                                layer_id = layer.get('layerId')
                                datasource_states = state.get('datasourceStates', {})
                                form_based = datasource_states.get('formBased', {}).get('layers', {})
                                layer_columns = form_based.get(layer_id, {}).get('columns', {})

                                # Process Y-axis values
                                for accessor_id in layer.get('accessors', []):
                                    if accessor_id in layer_columns:
                                        col_data = layer_columns[accessor_id]
                                        widget['yaxis'].append({
                                            'id': accessor_id,
                                            'label': col_data.get('label'),
                                            'field': col_data.get('sourceField'),
                                            'operation': col_data.get('operationType'),
                                        })

                                # Process X-axis dimension
                                x_accessor = layer.get('xAccessor')
                                if x_accessor and x_accessor in layer_columns:
                                    col_data = layer_columns[x_accessor]
                                    widget['xaxis'].append({
                                        'id': x_accessor,
                                        'label': col_data.get('label'),
                                        'field': col_data.get('sourceField'),
                                        'operation': col_data.get('operationType'),
                                    })

                                # Process series/category splits
                                split_accessor = layer.get('splitAccessor')
                                if split_accessor and split_accessor in layer_columns:
                                    col_data = layer_columns[split_accessor]
                                    widget['splits'].append({
                                        'id': split_accessor,
                                        'label': col_data.get('label'),
                                        'field': col_data.get('sourceField'),
                                        'operation': col_data.get('operationType'),
                                        'params': col_data.get('params', {})
                                    })
                        else:
                            # For non-lens widgets, just extract the title with fallback
                            widget['title'] = panel.get('title') or f"Unnamed_{widget_id}"
                            widget['yaxis'] = []
                            widget['xaxis'] = []
                            widget['splits'] = []

                        widgets.append(widget)

                    return {
                        'id': dashboard_data.get('id'),
                        'title': dashboard_name,
                        'description': dashboard_data.get('attributes', {}).get('description'),
                        'widgets': widgets
                    }

            return {}

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching dashboard: {str(e)}")
            return {}

    def get_dashboard_widget_data(self, dashboard_name, time_range: TimeRange) -> List[Dict[str, Any]]:
        """
        Fetch dashboard and all its widget data for a given time range
        
        Args:
            dashboard_name: The name of the dashboard to fetch
            time_range: Dictionary with time_geq and time_lt as Unix timestamps
        
        Returns:
            List of dictionaries, each containing a widget's configuration and data
        """
        # Get dashboard configuration first
        dashboard = self.get_dashboard_by_name(dashboard_name)

        if not dashboard:
            logger.error(f"Dashboard '{dashboard_name}' not found")
            return []

        widgets = dashboard.get('widgets', [])
        widget_data_list = []

        # For each widget, generate a query and fetch data
        for widget in widgets:
            # Only process Lens widgets (they have the visualization data we need)
            if widget.get('type') != 'lens':
                continue

            # Generate Elasticsearch query for this widget
            es_query = self.get_elasticsearch_query_for_widget(widget, time_range)

            # Execute the query against Elasticsearch
            widget_data = self.execute_elasticsearch_query(es_query)

            # Add widget metadata to the results
            result = {
                'id': widget.get('id'),
                'title': widget.get('title'),
                'type': widget.get('type'),
                'configuration': {
                    'xaxis': widget.get('xaxis', []),
                    'yaxis': widget.get('yaxis', []),
                    'splits': widget.get('splits', [])
                },
                'data': widget_data
            }

            widget_data_list.append(result)

        return widget_data_list

    def execute_elasticsearch_query(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute an Elasticsearch query and return the results
        
        Args:
            query: The Elasticsearch query to execute
        
        Returns:
            The query results
        """
        url = f"https://{self.host}/_search"
        try:
            response = requests.post(
                url,
                headers=self.apm_headers,
                json=query
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error executing Elasticsearch query: {str(e)}")
            return {"error": str(e)}

    def get_elasticsearch_query_for_widget(self, widget: Dict[str, Any], time_range: TimeRange):
        """
        Generate an Elasticsearch query based on widget configuration
        
        Args:
            widget: The widget configuration with xaxis, yaxis, and splits
            time_range: Object containing time_geq and time_lt as Unix timestamps
        
        Returns:
            Elasticsearch query object
        """
        # Mapping from Lens operation types to Elasticsearch aggregation types
        operation_to_agg = {
            "count": "value_count",
            "sum": "sum",
            "avg": "avg",
            "min": "min",
            "max": "max",
            "median": "percentiles",  # Special case - percentiles with percent: [50]
            "percentile": "percentiles",
            "cardinality": "cardinality",
            "terms": "terms",
            "date_histogram": "date_histogram",
            # Add more as needed
        }

        # Mapping for special parameter cases
        operation_params = {
            "median": {"percents": [50]},
            "percentile": lambda p: {"percents": [p]},  # Function to handle custom percentiles
        }

        # Extract time range from the provided format
        time_geq = time_range.time_geq * 1000
        time_lt = time_range.time_lt * 1000

        # Start building the query with Unix timestamp range
        query = {
            "size": 0,
            "query": {
                "range": {
                    "@timestamp": {
                        "gte": time_geq,
                        "lt": time_lt,
                        "format": "epoch_millis"  # Specify that we're using Unix timestamps
                    }
                }
            },
            "aggs": {}
        }

        # Extract field information from the widget
        splits = widget.get('splits', [])
        x_axis = widget.get('xaxis', [])
        y_axis = widget.get('yaxis', [])

        # If we have no dimensions to aggregate on, return a simple query
        if not splits and not x_axis and not y_axis:
            return query

        # Start building the aggregation hierarchy
        current_agg = query["aggs"]

        # First level: split fields (typically service.name or other categorical fields)
        if splits:
            split_field = splits[0].get('field')
            split_op = splits[0].get('operation')

            if split_field and split_op == 'terms':
                # Get size parameter if available, default to 10
                size = splits[0].get('params', {}).get('size', 10)

                current_agg["services"] = {
                    "terms": {
                        "field": split_field,
                        "size": size
                    },
                    "aggs": {}
                }
                current_agg = current_agg["services"]["aggs"]

        # Second level: time buckets (for x-axis, typically date_histogram)
        if x_axis:
            time_field = x_axis[0].get('field')
            time_op = x_axis[0].get('operation')

            if time_field and time_op == 'date_histogram':
                # Calculate appropriate interval based on time range
                time_range_ms = time_lt - time_geq

                # Simple interval calculation logic
                if time_range_ms <= 3600000:  # 1 hour or less
                    interval = "30s"
                elif time_range_ms <= 86400000:  # 1 day or less
                    interval = "5m"
                elif time_range_ms <= 604800000:  # 1 week or less
                    interval = "1h"
                else:
                    interval = "1d"

                current_agg["per_interval"] = {
                    "date_histogram": {
                        "field": time_field,
                        "fixed_interval": interval,
                        "min_doc_count": 0,
                        "extended_bounds": {
                            "min": time_geq,
                            "max": time_lt
                        },
                        "format": "epoch_millis"
                    },
                    "aggs": {}
                }
                current_agg = current_agg["per_interval"]["aggs"]

        # Third level: metrics (for y-axis)
        if y_axis:
            metric_field = y_axis[0].get('field')
            metric_op = y_axis[0].get('operation')

            if metric_field and metric_op:
                # Get corresponding Elasticsearch aggregation
                es_agg_type = operation_to_agg.get(metric_op, metric_op)

                # Determine metric name (can be customized based on the operation)
                metric_name = f"{metric_op}_{metric_field.replace('.', '_')}"

                # Create the metric aggregation
                metric_agg = {
                    es_agg_type: {
                        "field": metric_field
                    }
                }

                # Handle special cases with additional parameters
                if metric_op in operation_params:
                    if callable(operation_params[metric_op]):
                        # For functions that need parameters (like custom percentiles)
                        params = operation_params[metric_op](95)  # Default to 95th percentile
                    else:
                        # For fixed parameters (like median = 50th percentile)
                        params = operation_params[metric_op]

                    metric_agg[es_agg_type].update(params)

                current_agg[metric_name] = metric_agg

        return query

    ############################## FOR METADATA EXTRACTION ##############################
    def list_all_dashboards(self) -> List[Dict[str, Any]]:
        """
        List all dashboards with their basic information
        Returns a list of dictionaries containing dashboard id, title, and description
        """
        url = f"https://{self.kibana_host}/api/saved_objects/_find"
        params = {
            "type": "dashboard",
            "fields": ["title", "description"],
            "per_page": 1000
        }

        try:
            response = requests.get(url, headers=self.kibana_headers, params=params)
            response.raise_for_status()

            dashboards = []
            for obj in response.json().get('saved_objects', []):
                dashboard = {
                    'id': obj.get('id'),
                    'title': obj.get('attributes', {}).get('title'),
                    'description': obj.get('attributes', {}).get('description', '')
                }
                if dashboard['id'] and dashboard['title']:
                    dashboards.append(dashboard)

            return dashboards

        except requests.exceptions.RequestException as e:
            logger.error(f"Error listing dashboards: {str(e)}")
            return []

    def list_all_index_patterns(self) -> List[Dict[str, Any]]:
        """
        List all index patterns from Kibana with pagination support
        Returns a list of dictionaries containing index pattern id, title, time field name, and name
        """
        if not self.kibana_host:
            logger.warning("Kibana host not configured, cannot fetch index patterns")
            return []
            
        url = f"https://{self.kibana_host}/api/saved_objects/_find"
        index_patterns = []
        page = 1
        per_page = 100  # Use smaller page size for better performance
        
        try:
            while True:
                params = {
                    "type": "index-pattern",
                    "fields": ["title", "timeFieldName", "name"],
                    "per_page": per_page,
                    "page": page
                }

                response = requests.get(url, headers=self.kibana_headers, params=params)
                response.raise_for_status()
                
                response_data = response.json()
                saved_objects = response_data.get('saved_objects', [])
                
                # Process current page results
                for obj in saved_objects:
                    attributes = obj.get('attributes', {})
                    index_pattern = {
                        'id': obj.get('id'),
                        'title': attributes.get('title'),
                        'time_field_name': attributes.get('timeFieldName', ''),
                        'name': attributes.get('name', '')
                    }
                    if index_pattern['id'] and index_pattern['title']:
                        index_patterns.append(index_pattern)
                
                # Check if we have more pages
                total = response_data.get('total', 0)
                current_count = (page - 1) * per_page + len(saved_objects)
                
                if current_count >= total or len(saved_objects) < per_page:
                    # No more pages or we got fewer results than requested
                    break
                    
                page += 1
                
                # Safety check to prevent infinite loops
                if page > 100:  # Max 10,000 index patterns (100 pages * 100 per page)
                    logger.warning("Reached maximum page limit while fetching index patterns")
                    break

            logger.info(f"Successfully fetched {len(index_patterns)} index patterns from Kibana")
            return index_patterns

        except requests.exceptions.RequestException as e:
            logger.error(f"Error listing index patterns: {str(e)}")
            return []

    def get_index_pattern_mapping(self, index_pattern_title: str) -> Dict[str, Any]:
        """
        Get the mapping for a specific index pattern from ElasticSearch
        
        Args:
            index_pattern_title: The title/pattern of the index (e.g., "apm-*", "logs-*")
            
        Returns:
            Dictionary containing the mapping information for the index pattern
        """
        try:
            url = f"{self.protocol}://{self.host}:{self.port}/{index_pattern_title}/_mapping"
            
            logger.info(f"Fetching mapping from URL: {url}")
            logger.debug(f"Using headers: {dict(self.headers)}")
            
            response = requests.get(url, headers=self.kibana_headers, verify=self.verify_certs)
            
            logger.info(f"Response status code: {response.status_code}")
            
            # Handle 404 - index pattern doesn't match any indices
            if response.status_code == 404:
                logger.warning(f"No indices found matching pattern '{index_pattern_title}' (404 response)")
                return {}
            
            response.raise_for_status()
            
            # Log response size before parsing
            response_text = response.text
            logger.info(f"Response size: {len(response_text)} characters")
            
            mapping_data = response.json()
            
            # Check if we got any mappings
            if not mapping_data:
                logger.warning(f"Empty mapping response for index pattern '{index_pattern_title}'")
                return {}
            
            logger.info(f"Successfully fetched mapping for index pattern '{index_pattern_title}': {len(mapping_data)} indices")
            logger.debug(f"Mapping keys: {list(mapping_data.keys())}")
            
            return mapping_data
            
        except requests.exceptions.RequestException as e:
            # Handle index not found errors specifically
            error_msg = str(e)
            if "401" in error_msg or "unauthorized" in error_msg.lower():
                logger.error(f"Authentication error for index pattern '{index_pattern_title}': {e}")
                logger.error("Please check if the API key is valid and has the required permissions for accessing mappings")
                return {}
            elif "404" in error_msg or "index_not_found_exception" in error_msg.lower():
                logger.warning(f"Index pattern '{index_pattern_title}' not found: {e}")
                return {}
            else:
                logger.error(f"Error fetching mapping for index pattern '{index_pattern_title}': {str(e)}")
                return {}
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response for index pattern '{index_pattern_title}': {e}")
            logger.debug(f"Response content: {response.text[:1000]}...")  # Log first 1000 chars
            return {}
        except Exception as e:
            logger.error(f"Unexpected error fetching mapping for index pattern '{index_pattern_title}': {str(e)}")
            return {}

    def list_all_services(self, index_pattern: str = "traces-apm-*") -> List[Dict[str, Any]]:
        """
        List all services from APM indices
        Returns a list of dictionaries containing service name and document count
        """
        try:
            client = self.get_connection()
            result = client.search(
                index=index_pattern,
                body={
                    "size": 0,
                    "aggs": {
                        "services": {
                            "terms": {
                                "field": "service.name",
                                "size": 1000
                            }
                        }
                    }
                }
            )
            client.close()

            # Convert response to dict
            if hasattr(result, 'body'):
                result = result.body
            elif hasattr(result, 'meta'):
                result = dict(result)

            services = []
            if 'aggregations' in result:
                for bucket in result['aggregations']['services']['buckets']:
                    services.append({
                        'name': bucket['key'],
                        'count': bucket['doc_count']
                    })

            return services

        except Exception as e:
            logger.error(f"Error listing services: {str(e)}")
            return []

    def get_service_metrics_by_transaction(self, service_name: str, start_time: datetime, end_time: datetime,
                                           interval: str = "5m", index_pattern: str = "traces-apm-*") -> List[Dict]:
        """
        Fetch throughput, error rate, and latency grouped by transaction name for a given service.

        Args:
            service_name (str): Name of the service (application)
            start_time (datetime): Start time for the query
            end_time (datetime): End time for the query
            interval (str): Interval for the time series (e.g., "5m", "1h")

        Returns:
            List[Dict]: List of dictionaries with transaction name and metrics time series
        """
        start_time_str = start_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        end_time_str = end_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        query = {
            "size": 0,
            "query": {
                "bool": {
                    "must": [
                        {"term": {"service.name": service_name}},
                        {"range": {
                            "@timestamp": {
                                "gte": start_time_str,
                                "lte": end_time_str
                            }
                        }}
                    ]
                }
            },
            "aggs": {
                "time_series": {
                    "date_histogram": {
                        "field": "@timestamp",
                        "fixed_interval": interval,
                        "min_doc_count": 0
                    },
                    "aggs": {
                        "transactions_by_name": {
                            "terms": {
                                "field": "transaction.name",
                                "size": 50
                            },
                            "aggs": {
                                "throughput": {
                                    "value_count": {
                                        "field": "transaction.id"
                                    }
                                },
                                "error_count": {
                                    "terms": {
                                        "field": "transaction.result",
                                        "size": 10
                                    }
                                },
                                "latency_p95": {
                                    "percentiles": {
                                        "field": "transaction.duration.us",
                                        "percents": [95],
                                        "missing": 0
                                    }
                                },
                                "latency_p99": {
                                    "percentiles": {
                                        "field": "transaction.duration.us",
                                        "percents": [99],
                                        "missing": 0
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        client = self.get_connection()
        try:
            result = client.search(index=index_pattern, body=query)
            # Convert response to dict
            if hasattr(result, 'body'):
                result = result.body
            elif hasattr(result, 'meta'):
                result = dict(result)

            buckets = result.get('aggregations', {}).get('time_series', {}).get('buckets', [])
            # Structure: {transaction_name: [{timestamp, throughput, error_rate, latency_p95, latency_p99}, ...]}
            transaction_series = {}

            for bucket in buckets:
                timestamp = bucket['key_as_string']
                for txn_bucket in bucket['transactions_by_name']['buckets']:
                    txn_name = txn_bucket['key']
                    total_requests = txn_bucket['throughput']['value']

                    # Calculate error count
                    error_count = 0
                    for error_bucket in txn_bucket['error_count']['buckets']:
                        if error_bucket['key'] == 'error':
                            error_count = error_bucket['doc_count']
                            break

                    # Get latency values
                    latency_p95 = txn_bucket['latency_p95']['values']['95.0']
                    latency_p99 = txn_bucket['latency_p99']['values']['99.0']

                    # Calculate metrics for this interval
                    interval_seconds = pd.Timedelta(interval).total_seconds()
                    metrics = {
                        "timestamp": timestamp,
                        "throughput": round(total_requests / interval_seconds, 2),  # requests per second
                        "error_rate": round((error_count / total_requests * 100) if total_requests > 0 else 0, 2),
                        # percentage
                        "latency_p95": round(latency_p95 / 1000, 2),  # convert to milliseconds
                        "latency_p99": round(latency_p99 / 1000, 2),  # convert to milliseconds
                        "total_requests": total_requests
                    }

                    if txn_name not in transaction_series:
                        transaction_series[txn_name] = []
                    transaction_series[txn_name].append(metrics)

            # Convert to list of dicts for easier consumption
            return [
                {"transaction_name": txn, "series": series}
                for txn, series in transaction_series.items()
            ]
        except Exception as e:
            logger.error(f"Error fetching throughput by transaction: {e}")
            raise e
        finally:
            client.close()

    def get_transaction_names_by_service(self, service_name: str, start_time: datetime = None,
                                         end_time: datetime = None, index_pattern: str = "traces-apm-*") -> List[Dict[str, Any]]:
        """
        Get all transaction names for a given service.

        Args:
            service_name (str): Name of the service (application)
            start_time (datetime, optional): Start time for the query. Defaults to 4 hours ago.
            end_time (datetime, optional): End time for the query. Defaults to current time.

        Returns:
            List[Dict]: List of dictionaries containing unique transaction names and document count
        """
        try:
            # Set default time range if not provided
            if end_time is None:
                end_time = datetime.utcnow()
            if start_time is None:
                start_time = end_time - timedelta(hours=24)

            start_time_str = start_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            end_time_str = end_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")

            query = {
                "size": 0,
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"service.name": service_name}},
                            {"range": {
                                "@timestamp": {
                                    "gte": start_time_str,
                                    "lte": end_time_str
                                }
                            }}
                        ]
                    }
                },
                "aggs": {
                    "transactions": {
                        "terms": {
                            "field": "transaction.name",
                            "size": 1000  # Increase size to get more transactions
                        }
                    }
                }
            }

            client = self.get_connection()
            try:
                result = client.search(index=index_pattern, body=query)

                # Convert response to dict
                if hasattr(result, 'body'):
                    result = result.body
                elif hasattr(result, 'meta'):
                    result = dict(result)

                transactions = []
                seen_transactions = set()  # To ensure uniqueness

                if 'aggregations' in result:
                    for bucket in result['aggregations']['transactions']['buckets']:
                        transaction_name = bucket['key']
                        # Only add if we haven't seen this transaction name before
                        if transaction_name not in seen_transactions:
                            seen_transactions.add(transaction_name)
                            transactions.append({
                                'transaction_name': transaction_name,
                                'count': bucket['doc_count']
                            })

                return transactions

            except Exception as e:
                logger.error(f"Error fetching transaction names for service {service_name}: {e}")
                raise e
            finally:
                client.close()

        except Exception as e:
            logger.error(f"Exception occurred while fetching transaction names with error: {e}")
            raise e

    def get_traces_for_service(self, service_name: str, start_time: datetime, end_time: datetime, max_count: int = 100,
                               transaction_name: str = None, error_only: bool = False, sort_by: str = "@timestamp",
                               sort_order: str = "desc", include_summary: bool = True, index_pattern: str = "traces-apm-*") -> Dict[str, Any]:
        try:
            start_time_str = start_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            end_time_str = end_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")

            # Build query conditions
            must_conditions = [
                {"term": {"service.name": service_name}},
                {"range": {
                    "@timestamp": {
                        "gte": start_time_str,
                        "lte": end_time_str
                    }
                }}
            ]

            # Add transaction name filter if provided
            if transaction_name:
                must_conditions.append({"term": {"transaction.name": transaction_name}})

            # Add error filter if requested
            if error_only:
                must_conditions.append({"term": {"transaction.result": "error"}})

            # Build the query
            query = {
                "size": max_count,
                "query": {
                    "bool": {
                        "must": must_conditions
                    }
                },
                "sort": [
                    {sort_by: {"order": sort_order}}
                ]
            }

            client = self.get_connection()
            try:
                result = client.search(index=index_pattern, body=query)

                # Convert response to dict
                if hasattr(result, 'body'):
                    result = result.body
                elif hasattr(result, 'meta'):
                    result = dict(result)

                # Add summary if requested
                if include_summary and result and 'hits' in result:
                    hits = result['hits']['hits']
                    total_traces = len(hits)

                    if total_traces > 0:
                        # Analyze traces for summary
                        durations = []
                        error_count = 0
                        success_count = 0
                        transaction_counts = {}

                        for hit in hits:
                            source = hit['_source']

                            # Count errors vs successes
                            tx_result = source.get('transaction', {}).get('result', '')
                            if tx_result == 'error':
                                error_count += 1
                            else:
                                success_count += 1

                            # Collect duration data
                            duration_us = source.get('transaction', {}).get('duration', {}).get('us', 0)
                            if duration_us > 0:
                                duration_ms = duration_us / 1000
                                durations.append(duration_ms)

                            # Count transactions
                            tx_name = source.get('transaction', {}).get('name', 'Unknown')
                            transaction_counts[tx_name] = transaction_counts.get(tx_name, 0) + 1

                        # Calculate statistics
                        avg_duration_ms = sum(durations) / len(durations) if durations else 0
                        p95_duration_ms = sorted(durations)[int(len(durations) * 0.95)] if durations else 0
                        p99_duration_ms = sorted(durations)[int(len(durations) * 0.99)] if durations else 0

                        # Get top transactions
                        top_transactions = sorted(
                            transaction_counts.items(),
                            key=lambda x: x[1],
                            reverse=True
                        )[:10]  # Top 10 transactions

                        # Calculate error rate
                        error_rate_percentage = (error_count / total_traces * 100) if total_traces > 0 else 0

                        # Add summary to result
                        result['summary'] = {
                            "total_traces": total_traces,
                            "error_count": error_count,
                            "success_count": success_count,
                            "avg_duration_ms": round(avg_duration_ms, 2),
                            "p95_duration_ms": round(p95_duration_ms, 2),
                            "p99_duration_ms": round(p99_duration_ms, 2),
                            "top_transactions": [
                                {"transaction_name": name, "count": count}
                                for name, count in top_transactions
                            ],
                            "error_rate_percentage": round(error_rate_percentage, 2)
                        }
                    else:
                        # No traces found, add empty summary
                        result['summary'] = {
                            "total_traces": 0,
                            "error_count": 0,
                            "success_count": 0,
                            "avg_duration_ms": 0,
                            "p95_duration_ms": 0,
                            "p99_duration_ms": 0,
                            "top_transactions": [],
                            "error_rate_percentage": 0
                        }

                return result

            except Exception as e:
                logger.error(f"Error fetching traces for service {service_name}: {e}")
                raise e
            finally:
                client.close()

        except Exception as e:
            logger.error(f"Exception occurred while fetching traces with error: {e}")
            raise e

    def get_traces_for_transaction(self, transaction_name: str, start_time: datetime, end_time: datetime,
                                   max_count: int = 100, service_name: str = None, error_only: bool = False,
                                   sort_by: str = "@timestamp", sort_order: str = "desc",
                                   include_summary: bool = True, index_pattern: str = "traces-apm-*") -> Dict[str, Any]:
        try:
            start_time_str = start_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            end_time_str = end_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")

            # Build query conditions
            must_conditions = [
                {"term": {"transaction.name": transaction_name}},
                {"range": {
                    "@timestamp": {
                        "gte": start_time_str,
                        "lte": end_time_str
                    }
                }}
            ]

            # Add service name filter if provided
            if service_name:
                must_conditions.append({"term": {"service.name": service_name}})

            # Add error filter if requested
            if error_only:
                must_conditions.append({"term": {"transaction.result": "error"}})

            # Build the query
            query = {
                "size": max_count,
                "query": {
                    "bool": {
                        "must": must_conditions
                    }
                },
                "sort": [
                    {sort_by: {"order": sort_order}}
                ]
            }

            client = self.get_connection()
            try:
                result = client.search(index=index_pattern, body=query)

                # Convert response to dict
                if hasattr(result, 'body'):
                    result = result.body
                elif hasattr(result, 'meta'):
                    result = dict(result)

                # Add summary if requested
                if include_summary and result and 'hits' in result:
                    hits = result['hits']['hits']
                    total_traces = len(hits)

                    if total_traces > 0:
                        # Analyze traces for summary
                        durations = []
                        error_count = 0
                        success_count = 0
                        services_breakdown = {}
                        time_distribution = {}

                        for hit in hits:
                            source = hit['_source']

                            # Count errors vs successes
                            tx_result = source.get('transaction', {}).get('result', '')
                            if tx_result == 'error':
                                error_count += 1
                            else:
                                success_count += 1

                            # Collect duration data
                            duration_us = source.get('transaction', {}).get('duration', {}).get('us', 0)
                            if duration_us > 0:
                                duration_ms = duration_us / 1000
                                durations.append(duration_ms)

                            # Track service breakdown (only if service_name is None)
                            if service_name is None:
                                service = source.get('service', {}).get('name', 'Unknown')
                                if service not in services_breakdown:
                                    services_breakdown[service] = {
                                        'total': 0,
                                        'errors': 0,
                                        'success': 0
                                    }
                                services_breakdown[service]['total'] += 1
                                if tx_result == 'error':
                                    services_breakdown[service]['errors'] += 1
                                else:
                                    services_breakdown[service]['success'] += 1

                            # Track time distribution (hourly)
                            timestamp = source.get('@timestamp', '')
                            if timestamp:
                                try:
                                    # Parse timestamp and extract hour
                                    from datetime import datetime
                                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                                    hour = dt.strftime('%Y-%m-%d %H:00')
                                    time_distribution[hour] = time_distribution.get(hour, 0) + 1
                                except:
                                    pass  # Skip if timestamp parsing fails

                        # Calculate statistics
                        avg_duration_ms = sum(durations) / len(durations) if durations else 0
                        p95_duration_ms = sorted(durations)[int(len(durations) * 0.95)] if durations else 0
                        p99_duration_ms = sorted(durations)[int(len(durations) * 0.99)] if durations else 0

                        # Calculate error rate
                        error_rate_percentage = (error_count / total_traces * 100) if total_traces > 0 else 0

                        # Convert services breakdown to list format
                        services_list = []
                        if service_name is None and services_breakdown:
                            for service, stats in services_breakdown.items():
                                services_list.append({
                                    "service_name": service,
                                    "total_traces": stats['total'],
                                    "error_count": stats['errors'],
                                    "success_count": stats['success'],
                                    "error_rate_percentage": round(
                                        (stats['errors'] / stats['total'] * 100) if stats['total'] > 0 else 0, 2)
                                })
                            # Sort by total traces descending
                            services_list.sort(key=lambda x: x['total_traces'], reverse=True)

                        # Add summary to result
                        result['summary'] = {
                            "transaction_name": transaction_name,
                            "service_name": service_name,
                            "total_traces": total_traces,
                            "error_count": error_count,
                            "success_count": success_count,
                            "avg_duration_ms": round(avg_duration_ms, 2),
                            "p95_duration_ms": round(p95_duration_ms, 2),
                            "p99_duration_ms": round(p99_duration_ms, 2),
                            "error_rate_percentage": round(error_rate_percentage, 2),
                            "services_breakdown": services_list,
                            "time_distribution": dict(sorted(time_distribution.items()))
                        }
                    else:
                        # No traces found, add empty summary
                        result['summary'] = {
                            "transaction_name": transaction_name,
                            "service_name": service_name,
                            "total_traces": 0,
                            "error_count": 0,
                            "success_count": 0,
                            "avg_duration_ms": 0,
                            "p95_duration_ms": 0,
                            "p99_duration_ms": 0,
                            "error_rate_percentage": 0,
                            "services_breakdown": [],
                            "time_distribution": {}
                        }

                return result

            except Exception as e:
                logger.error(f"Error fetching traces for transaction {transaction_name}: {e}")
                raise e
            finally:
                client.close()

        except Exception as e:
            logger.error(f"Exception occurred while fetching traces for transaction with error: {e}")
            raise e

    def get_unique_url_paths_for_service(self, service_name: str, index_pattern: str = "traces-apm-*") -> List[str]:
        """
        Get all unique URL paths for a given service (from transactions).
        
        Args:
            service_name: Name of the service to get paths for
            index_pattern: Elasticsearch index pattern to search in
            
        Returns:
            List of unique URL paths
        """
        try:
            query = {
                "size": 0,
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"service.name": service_name}},
                            {"term": {"processor.event": "transaction"}}
                        ]
                    }
                },
                "aggs": {
                    "paths": {
                        "terms": {
                            "field": "url.path",
                            "size": 1000
                        }
                    }
                }
            }
            
            client = self.get_connection()
            try:
                result = client.search(index=index_pattern, body=query)
                
                # Convert response to dict
                if hasattr(result, 'body'):
                    result = result.body
                elif hasattr(result, 'meta'):
                    result = dict(result)
                
                paths = []
                if 'aggregations' in result and 'paths' in result['aggregations']:
                    for bucket in result['aggregations']['paths']['buckets']:
                        paths.append(bucket['key'])
                
                return paths
                
            except Exception as e:
                logger.error(f"Error fetching URL paths for service {service_name}: {e}")
                return []
            finally:
                client.close()
                
        except Exception as e:
            logger.error(f"Exception occurred while fetching URL paths: {e}")
            return []

    def get_trace_ids_for_service_and_path(self, service_name: str, url_path: str, limit: int = 5, 
                                          index_pattern: str = "traces-apm-*") -> List[str]:
        """
        Get up to `limit` unique trace IDs for a given service and URL path.
        
        Args:
            service_name: Name of the service
            url_path: URL path to filter by
            limit: Maximum number of trace IDs to return
            index_pattern: Elasticsearch index pattern to search in
            
        Returns:
            List of unique trace IDs
        """
        try:
            query = {
                "size": limit,
                "_source": ["trace.id"],
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"service.name": service_name}},
                            {"term": {"processor.event": "transaction"}},
                            {"term": {"url.path": url_path}}
                        ]
                    }
                }
            }
            
            client = self.get_connection()
            try:
                result = client.search(index=index_pattern, body=query)
                
                # Convert response to dict
                if hasattr(result, 'body'):
                    result = result.body
                elif hasattr(result, 'meta'):
                    result = dict(result)
                
                trace_ids = set()
                if 'hits' in result and 'hits' in result['hits']:
                    for hit in result['hits']['hits']:
                        tid = hit.get('_source', {}).get('trace', {}).get('id')
                        if tid:
                            trace_ids.add(tid)
                
                return list(trace_ids)
                
            except Exception as e:
                logger.error(f"Error fetching trace IDs for service {service_name} and path {url_path}: {e}")
                return []
            finally:
                client.close()
                
        except Exception as e:
            logger.error(f"Exception occurred while fetching trace IDs: {e}")
            return []

    def get_downstream_calls_for_trace(self, trace_id: str, index_pattern: str = "traces-apm-*") -> List[Dict[str, Any]]:
        """
        For a given trace ID, find all unique downstream calls from spans.
        
        Args:
            trace_id: The trace ID to analyze
            index_pattern: Elasticsearch index pattern to search in
            
        Returns:
            List of dictionaries, each containing service and path information
        """
        try:
            query = {
                "size": 1000,
                "_source": ["span", "destination", "service.name", "url.original"],
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"trace.id": trace_id}},
                            {"term": {"processor.event": "span"}},
                            {"exists": {"field": "span.destination"}}
                        ]
                    }
                }
            }
            
            client = self.get_connection()
            try:
                result = client.search(index=index_pattern, body=query)
                
                # Convert response to dict
                if hasattr(result, 'body'):
                    result = result.body
                elif hasattr(result, 'meta'):
                    result = dict(result)
                
                calls = []
                if 'hits' in result and 'hits' in result['hits']:
                    for hit in result['hits']['hits']:
                        source = hit.get('_source', {})
                        span = source.get('span', {})
                        
                        # Extract destination service information
                        dest_service = (
                            span.get('destination', {}).get('service', {}).get('resource') or
                            span.get('destination', {}).get('service', {}).get('name') or
                            span.get('service', {}).get('target', {}).get('name')
                        )
                        
                        dest_type = span.get('destination', {}).get('service', {}).get('type')
                        dest_resource = span.get('destination', {}).get('address')
                        dest_name = span.get('destination', {}).get('service', {}).get('name')
                        dest_path = source.get('url', {}).get('original')
                        
                        if dest_service and dest_service != source.get('service', {}).get('name'):
                            calls.append({
                                'service': dest_service, 
                                'type': dest_type, 
                                'name': dest_name, 
                                'resource': dest_resource, 
                                'path': dest_path, 
                                'trace_id': trace_id
                            })
                
                # Remove duplicates while preserving order
                seen = set()
                unique_calls = []
                for call in calls:
                    call_tuple = tuple(sorted(call.items()))
                    if call_tuple not in seen:
                        seen.add(call_tuple)
                        unique_calls.append(call)
                
                return unique_calls
                
            except Exception as e:
                logger.error(f"Error fetching downstream calls for trace {trace_id}: {e}")
                return []
            finally:
                client.close()
                
        except Exception as e:
            logger.error(f"Exception occurred while fetching downstream calls: {e}")
            return []
