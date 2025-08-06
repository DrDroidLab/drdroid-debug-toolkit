import json

from google.protobuf.struct_pb2 import Struct
from google.protobuf.wrappers_pb2 import StringValue, UInt64Value, Int64Value

from core.integrations.source_api_processors.open_search_api_processor import OpenSearchApiProcessor
from core.integrations.source_manager import SourceManager
from core.protos.base_pb2 import Source, SourceModelType, TimeRange
from core.protos.connectors.connector_pb2 import Connector as ConnectorProto
from core.protos.literal_pb2 import LiteralType, Literal
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResultType, TableResult, PlaybookTaskResult, TextResult, \
    ApiResponseResult
from core.protos.playbooks.source_task_definitions.open_search_task_pb2 import OpenSearch
from core.protos.ui_definition_pb2 import FormField, FormFieldType
from core.utils.credentilal_utils import generate_credentials_dict
from core.utils.proto_utils import dict_to_proto


class OpenSearchSourceManager(SourceManager):

    def __init__(self):
        self.source = Source.OPEN_SEARCH
        self.task_proto = OpenSearch
        self.task_type_callable_map = {
            OpenSearch.TaskType.QUERY_LOGS: {
                'executor': self.execute_query_logs,
                'model_types': [SourceModelType.OPEN_SEARCH_INDEX],
                'result_type': PlaybookTaskResultType.LOGS,
                'display_name': 'Query Logs from an OpenSearch Index',
                'category': 'Logs',
                'form_fields': [
                    FormField(key_name=StringValue(value="index"),
                              display_name=StringValue(value="Index"),
                              description=StringValue(value='Select Index'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT),
                    FormField(key_name=StringValue(value="query_dsl"),
                              display_name=StringValue(value="Query DSL"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.MULTILINE_FT),
                    FormField(key_name=StringValue(value="limit"),
                              display_name=StringValue(value="Enter Limit"),
                              data_type=LiteralType.LONG,
                              default_value=Literal(type=LiteralType.LONG, long=Int64Value(value=2000)),
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="offset"),
                              display_name=StringValue(value="Enter Offset"),
                              data_type=LiteralType.LONG,
                              default_value=Literal(type=LiteralType.LONG, long=Int64Value(value=0)),
                              form_field_type=FormFieldType.TEXT_FT),
                ]
            },
            OpenSearch.TaskType.DELETE_INDEX: {
                'executor': self.execute_delete_index,
                'model_types': [SourceModelType.OPEN_SEARCH_INDEX],
                'result_type': PlaybookTaskResultType.LOGS,
                'display_name': 'Delete OpenSearch Index (Use with caution)',
                'category': 'Actions',
                'form_fields': [
                    FormField(key_name=StringValue(value="index"),
                              display_name=StringValue(value="Index"),
                              description=StringValue(value='Select Index'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT),
                ]
            },
            OpenSearch.TaskType.GET_NODE_STATS: {
                'executor': self.execute_get_node_stats,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Get OpenSearch Node Stats',
                'category': 'Metrics',
                'form_fields': []
            },
            OpenSearch.TaskType.GET_INDEX_STATS: {
                'executor': self.execute_get_index_stats,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Get OpenSearch Indices Stats',
                'category': 'Metrics',
                'form_fields': []
            },
            OpenSearch.TaskType.GET_CLUSTER_HEALTH: {
                'executor': self.execute_get_cluster_health,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Get OpenSearch Cluster Health',
                'category': 'Metrics',
                'form_fields': []
            },
            OpenSearch.TaskType.GET_CLUSTER_SETTINGS: {
                'executor': self.execute_get_cluster_settings,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Get OpenSearch Cluster Settings',
                'category': 'Metrics',
                'form_fields': []
            },
            OpenSearch.TaskType.GET_CLUSTER_STATS: {
                'executor': self.execute_get_cluster_stats,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Get OpenSearch Cluster Stats',
                'category': 'Metrics',
                'form_fields': []
            },
            OpenSearch.TaskType.GET_INDEX_AND_SHARD_RECOVERIES: {
                'executor': self.execute_get_index_and_shard_recoveries,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Get OpenSearch Index and Shard Recoveries',
                'category': 'Metrics',
                'form_fields': []
            },
            OpenSearch.TaskType.GET_INDICES: {
                'executor': self.execute_get_indices,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Get OpenSearch Indices',
                'category': 'Metrics',
                'form_fields': []
            },
            OpenSearch.TaskType.GET_PENDING_TASKS: {
                'executor': self.execute_get_pending_tasks,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Get OpenSearch Pending Tasks',
                'category': 'Metrics',
                'form_fields': []
            },
            OpenSearch.TaskType.GET_SHARDS: {
                'executor': self.execute_get_shards,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Get OpenSearch Shards',
                'category': 'Metrics',
                'form_fields': []
            },
            OpenSearch.TaskType.GET_TASKS: {
                'executor': self.execute_get_tasks,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Get OpenSearch Tasks',
                'category': 'Metrics',
                'form_fields': []
            }
        }

    def get_connector_processor(self, os_connector, **kwargs):
        generated_credentials = generate_credentials_dict(os_connector.type, os_connector.keys)
        return OpenSearchApiProcessor(**generated_credentials)

    def execute_query_logs(self, time_range: TimeRange, os_task: OpenSearch,
                           os_connector: ConnectorProto):
        try:
            if not os_connector:
                raise ValueError("OpenSearchSourceManager.execute_query_logs:: Task execution Failed:: "
                                 "No OpenSearch source found")

            query_logs = os_task.query_logs
            if not query_logs.index or not query_logs.index.value:
                raise ValueError("OpenSearchSourceManager.execute_query_logs:: Task execution Failed:: No index found")

            index = query_logs.index.value
            query_dsl = query_logs.query_dsl.value.strip()
            limit = query_logs.limit.value if query_logs.limit.value else 2000
            offset = query_logs.offset.value if query_logs.offset.value else 0
            sort_desc = query_logs.sort_desc.value if query_logs.sort_desc.value else ""
            timestamp_field = query_logs.timestamp_field.value if query_logs.timestamp_field.value else ""

            os_client = self.get_connector_processor(os_connector)

            sort = []
            if timestamp_field:
                sort.append({timestamp_field: "desc"})
            if sort_desc:
                sort.append({sort_desc: "desc"})
            sort.append({"_score": "desc"})

            parsed_query_dsl = json.loads(query_dsl)

            query = {
                "query": {
                    "bool": {
                        "must": [
                            parsed_query_dsl["query"]
                        ]
                    }
                },
                "size": limit,
                "from": offset,
                "sort": sort
            }

            if timestamp_field:
                query["query"]["bool"]["must"].append({
                    "range": {
                        timestamp_field: {
                            "gte": time_range.time_geq * 1000,
                            "lt": time_range.time_lt * 1000
                        }
                    }
                })

            result = os_client.query(index, query)

            if 'hits' not in result or not result['hits']['hits']:
                return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT, text=TextResult(output=StringValue(
                    value=f"No data returned from Open Search for query: {query_dsl} on index: {index}")),
                                          source=self.source)

            hits = result['hits']['hits']
            total_hits = result['hits']['total']['value']
            table_rows = []
            for hit in hits:
                table_columns = []
                for column, value in hit['_source'].items():
                    table_column = TableResult.TableColumn(name=StringValue(value=column),
                                                           value=StringValue(value=str(value)))
                    table_columns.append(table_column)
                table_rows.append(TableResult.TableRow(columns=table_columns))

            table = TableResult(raw_query=StringValue(value=f"Execute ```{query_dsl}``` on index {index}"),
                                total_count=UInt64Value(value=total_hits),
                                rows=table_rows)
            return PlaybookTaskResult(type=PlaybookTaskResultType.LOGS, logs=table, source=self.source)

        except Exception as e:
            raise Exception(f"OpenSearchSourceManager.execute_query_logs:: Error while executing OpenSearch task: "
                            f"{str(e)}")

    def execute_delete_index(self, time_range: TimeRange, os_task: OpenSearch,
                             os_connector: ConnectorProto):
        try:
            if not os_connector:
                raise ValueError("OpenSearchSourceManager.execute_delete_index:: Task execution Failed:: "
                                 "No OpenSearch source found")

            delete_index = os_task.delete_index
            if not delete_index.index or not delete_index.index.value:
                raise ValueError(
                    "OpenSearchSourceManager.execute_delete_index:: Task execution Failed:: No index found")

            index = delete_index.index.value
            os_client = self.get_connector_processor(os_connector)
            result = os_client.delete_index(index)

            if result and 'acknowledged' in result and result['acknowledged']:
                return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT, text=TextResult(output=StringValue(
                    value=f"Index {index} deleted successfully")), source=self.source)
            return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT, text=TextResult(output=StringValue(
                value=f"Failed to delete Index {index}")), source=self.source)
        except Exception as e:
            raise Exception(
                f"OpenSearchSourceManager.execute_delete_index:: Error while executing OpenSearch task: {str(e)}")

    def execute_get_node_stats(self, time_range: TimeRange, os_task: OpenSearch,
                               os_connector: ConnectorProto):
        try:
            if not os_connector:
                raise ValueError("OpenSearchSourceManager.execute_get_node_stats:: Task execution Failed:: "
                                 "No OpenSearch source found")

            os_client = self.get_connector_processor(os_connector)
            result = os_client.get_node_stats()
            result_struct = dict_to_proto(result, Struct)
            api_response = ApiResponseResult(response_body=result_struct)
            return PlaybookTaskResult(type=PlaybookTaskResultType.API_RESPONSE, api_response=api_response,
                                      source=self.source)
        except Exception as e:
            raise Exception(
                f"OpenSearchSourceManager.execute_get_node_stats:: Error while executing OpenSearch task: {str(e)}")

    def execute_get_index_stats(self, time_range: TimeRange, os_task: OpenSearch,
                                os_connector: ConnectorProto):
        try:
            if not os_connector:
                raise ValueError("OpenSearchSourceManager.execute_get_index_stats:: Task execution Failed:: "
                                 "No OpenSearch source found")

            os_client = self.get_connector_processor(os_connector)
            result = os_client.get_index_stats()
            result_struct = dict_to_proto(result, Struct)
            api_response = ApiResponseResult(response_body=result_struct)
            return PlaybookTaskResult(type=PlaybookTaskResultType.API_RESPONSE, api_response=api_response,
                                      source=self.source)
        except Exception as e:
            raise Exception(
                f"OpenSearchSourceManager.execute_get_index_stats:: Error while executing OpenSearch task: {str(e)}")
        
    # cluster health task functions
    def execute_get_cluster_health(self, time_range: TimeRange, os_task: OpenSearch,
                                   os_connector: ConnectorProto):
        try:
            if not os_connector:
                raise ValueError("OpenSearchSourceManager.execute_get_cluster_health:: Task execution Failed:: "
                                 "No OpenSearch source found")

            os_client = self.get_connector_processor(os_connector)
            result = os_client.get_cluster_health()
            result_struct = dict_to_proto(result, Struct)
            api_response = ApiResponseResult(response_body=result_struct)
            return PlaybookTaskResult(type=PlaybookTaskResultType.API_RESPONSE, api_response=api_response,
                                      source=self.source)
        except Exception as e:
            raise Exception(
                f"OpenSearchSourceManager.execute_get_cluster_health:: Error while executing OpenSearch task: {str(e)}")
        
    # cluster settings task functions
    def execute_get_cluster_settings(self, time_range: TimeRange, os_task: OpenSearch,
                                     os_connector: ConnectorProto):
        try:
            if not os_connector:
                raise ValueError("OpenSearchSourceManager.execute_get_cluster_settings:: Task execution Failed:: "
                                 "No OpenSearch source found")

            os_client = self.get_connector_processor(os_connector)
            result = os_client.get_cluster_settings()
            result_struct = dict_to_proto(result, Struct)
            api_response = ApiResponseResult(response_body=result_struct)
            return PlaybookTaskResult(type=PlaybookTaskResultType.API_RESPONSE, api_response=api_response,
                                      source=self.source)
        except Exception as e:
            raise Exception(
                f"OpenSearchSourceManager.execute_get_cluster_settings:: Error while executing OpenSearch task: {str(e)}")

    # cluster stats task functions
    def execute_get_cluster_stats(self, time_range: TimeRange, os_task: OpenSearch,
                                  os_connector: ConnectorProto):
        try:
            if not os_connector:
                raise ValueError("OpenSearchSourceManager.execute_get_cluster_stats:: Task execution Failed:: "
                                 "No OpenSearch source found")

            os_client = self.get_connector_processor(os_connector)
            result = os_client.get_cluster_stats()
            result_struct = dict_to_proto(result, Struct)
            api_response = ApiResponseResult(response_body=result_struct)
            return PlaybookTaskResult(type=PlaybookTaskResultType.API_RESPONSE, api_response=api_response,
                                      source=self.source)
        except Exception as e:
            raise Exception(
                f"OpenSearchSourceManager.execute_get_cluster_stats:: Error while executing OpenSearch task: {str(e)}")

    # index and shard recoveries task functions
    def execute_get_index_and_shard_recoveries(self, time_range: TimeRange, os_task: OpenSearch,
                                               os_connector: ConnectorProto):
        try:
            if not os_connector:
                raise ValueError("OpenSearchSourceManager.execute_get_index_and_shard_recoveries:: Task execution Failed:: "
                                 "No OpenSearch source found")

            os_client = self.get_connector_processor(os_connector)
            result = os_client.get_index_and_shard_recoveries()
            result_struct = dict_to_proto(result, Struct)
            api_response = ApiResponseResult(response_body=result_struct)
            return PlaybookTaskResult(type=PlaybookTaskResultType.API_RESPONSE, api_response=api_response,
                                      source=self.source)
        except Exception as e:
            raise Exception(
                f"OpenSearchSourceManager.execute_get_index_and_shard_recoveries:: Error while executing OpenSearch task: {str(e)}")

    # indices task functions
    def execute_get_indices(self, time_range: TimeRange, os_task: OpenSearch,
                            os_connector: ConnectorProto):
        try:
            if not os_connector:
                raise ValueError("OpenSearchSourceManager.execute_get_indices:: Task execution Failed:: "
                                 "No OpenSearch source found")

            os_client = self.get_connector_processor(os_connector)
            result = os_client.get_indices()
            result_struct = dict_to_proto(result, Struct)
            api_response = ApiResponseResult(response_body=result_struct)
            return PlaybookTaskResult(type=PlaybookTaskResultType.API_RESPONSE, api_response=api_response,
                                      source=self.source)
        except Exception as e:
            raise Exception(
                f"OpenSearchSourceManager.execute_get_indices:: Error while executing OpenSearch task: {str(e)}")

    # pending tasks task functions
    def execute_get_pending_tasks(self, time_range: TimeRange, os_task: OpenSearch,
                                  os_connector: ConnectorProto):
        try:
            if not os_connector:
                raise ValueError("OpenSearchSourceManager.execute_get_pending_tasks:: Task execution Failed:: "
                                 "No OpenSearch source found")

            os_client = self.get_connector_processor(os_connector)
            result = os_client.get_pending_tasks()
            result_struct = dict_to_proto(result, Struct)
            api_response = ApiResponseResult(response_body=result_struct)
            return PlaybookTaskResult(type=PlaybookTaskResultType.API_RESPONSE, api_response=api_response,
                                      source=self.source)
        except Exception as e:
            raise Exception(
                f"OpenSearchSourceManager.execute_get_pending_tasks:: Error while executing OpenSearch task: {str(e)}")

    # shards task functions
    def execute_get_shards(self, time_range: TimeRange, os_task: OpenSearch,
                           os_connector: ConnectorProto):
        try:
            if not os_connector:
                raise ValueError("OpenSearchSourceManager.execute_get_shards:: Task execution Failed:: "
                                 "No OpenSearch source found")

            os_client = self.get_connector_processor(os_connector)
            result = os_client.get_shards()
            result_struct = dict_to_proto(result, Struct)
            api_response = ApiResponseResult(response_body=result_struct)
            return PlaybookTaskResult(type=PlaybookTaskResultType.API_RESPONSE, api_response=api_response,
                                      source=self.source)
        except Exception as e:
            raise Exception(
                f"OpenSearchSourceManager.execute_get_shards:: Error while executing OpenSearch task: {str(e)}")

    # tasks task functions
    def execute_get_tasks(self, time_range: TimeRange, os_task: OpenSearch,
                          os_connector: ConnectorProto):
        try:
            if not os_connector:
                raise ValueError("OpenSearchSourceManager.execute_get_tasks:: Task execution Failed:: "
                                 "No OpenSearch source found")

            os_client = self.get_connector_processor(os_connector)
            result = os_client.get_tasks()
            result_struct = dict_to_proto(result, Struct)
            api_response = ApiResponseResult(response_body=result_struct)
            return PlaybookTaskResult(type=PlaybookTaskResultType.API_RESPONSE, api_response=api_response,
                                      source=self.source)
        except Exception as e:
            raise Exception(
                f"OpenSearchSourceManager.execute_get_tasks:: Error while executing OpenSearch task: {str(e)}")
