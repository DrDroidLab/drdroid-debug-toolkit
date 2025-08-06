import json
import threading

import ast

from google.protobuf.wrappers_pb2 import StringValue, UInt64Value, Int64Value

from core.integrations.source_api_processors.mongodb_processor import MongoDBProcessor
from core.integrations.source_manager import SourceManager
from core.protos.connectors.connector_pb2 import Connector as ConnectorProto
from core.protos.base_pb2 import TimeRange, Source
from core.protos.literal_pb2 import LiteralType, Literal
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResult, TableResult, PlaybookTaskResultType
from core.protos.playbooks.source_task_definitions.mongodb_task_pb2 import MongoDB
from core.protos.ui_definition_pb2 import FormField, FormFieldType
from core.utils.credentilal_utils import generate_credentials_dict


class TimeoutException(Exception):
    pass


class MongoDBSourceManager(SourceManager):

    def __init__(self):
        self.source = Source.MONGODB
        self.task_proto = MongoDB
        self.task_type_callable_map = {
            MongoDB.TaskType.DATA_FETCH: {
                'executor': self.execute_mongo_query,
                'model_types': [],
                'result_type': PlaybookTaskResultType.LOGS,
                'display_name': 'Query a MongoDB Database',
                'category': 'Database',
                'form_fields': [
                    FormField(key_name=StringValue(value="database"),
                              display_name=StringValue(value="Database"),
                              description=StringValue(value='Enter Database'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="collection"),
                              display_name=StringValue(value="Collection"),
                              description=StringValue(value='Enter Collection'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="filters"),
                              display_name=StringValue(value="Filters"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              default_value=Literal(type=LiteralType.STRING, string=StringValue(value='\{\}'))),
                    FormField(key_name=StringValue(value="projection"),
                              display_name=StringValue(value="Projection"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              default_value=Literal(type=LiteralType.STRING, string=StringValue(value='null'))),
                    FormField(key_name=StringValue(value="order_by_field"),
                              display_name=StringValue(value="Sort order"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              default_value=Literal(type=LiteralType.STRING, string=StringValue(value='[]'))),
                    FormField(key_name=StringValue(value="limit"),
                              display_name=StringValue(value="Limit"),
                              data_type=LiteralType.LONG,
                              form_field_type=FormFieldType.TEXT_FT,
                              default_value=Literal(type=LiteralType.STRING, string=StringValue(value='10'))),
                    FormField(key_name=StringValue(value="timeout"),
                              display_name=StringValue(value="Timeout (in seconds)"),
                              description=StringValue(value='Enter Timeout (in seconds)'),
                              data_type=LiteralType.LONG,
                              default_value=Literal(type=LiteralType.LONG, long=Int64Value(value=120)),
                              form_field_type=FormFieldType.TEXT_FT)
                ]
            },
        }

    def get_connector_processor(self, mongodb_connector, **kwargs):
        generated_credentials = generate_credentials_dict(mongodb_connector.type, mongodb_connector.keys)
        return MongoDBProcessor(**generated_credentials)

    def execute_mongo_query(self, time_range: TimeRange, mongodb_task: MongoDB,
                            mongodb_connector: ConnectorProto):
        try:
            if not mongodb_connector:
                raise Exception("Task execution Failed:: No MongoDB source found")

            mongo_query = mongodb_task.data_fetch
            collection = mongo_query.collection.value
            database = mongo_query.database.value

            if not database or not collection:
                raise Exception("Task execution Failed:: Database and Collection are required")

            filters = json.loads(mongo_query.filters.value) if mongo_query.filters.value else {}
            projection = json.loads(mongo_query.projection.value) if mongo_query.projection.value else {}
            order_by_field = ast.literal_eval(
                mongo_query.order_by_field.value) if mongo_query.order_by_field.value else []
            limit = mongo_query.limit.value
            timeout = mongo_query.timeout.value if mongo_query.timeout.value else 120

            def query_db():
                nonlocal count_result, result, exception
                try:
                    mongodb_processor = self.get_connector_processor(mongodb_connector)
                    count_result = mongodb_processor.get_query_result_count(database, collection, filters, limit,
                                                                            timeout=timeout)
                    result = mongodb_processor.get_query_result(database, collection, filters, projection,
                                                                order_by_field, limit, timeout=timeout)
                except Exception as e:
                    exception = e

            count_result = None
            result = None
            exception = None

            query_thread = threading.Thread(target=query_db)
            query_thread.start()
            query_thread.join(timeout)

            if query_thread.is_alive():
                raise TimeoutException(f"Function 'execute_mongo_query' exceeded the timeout of {timeout} seconds")

            if exception:
                raise exception

            print("Playbook Task Downstream Request: Type -> {}, Account -> {}".format("MongoDB",
                                                                                       mongodb_connector.account_id.value),
                  database, collection, filters, projection, order_by_field, limit, flush=True)

            table_rows: [TableResult.TableRow] = []
            for r in list(result):
                table_columns = []
                for key, value in r.items():
                    table_columns.append(TableResult.TableColumn(name=StringValue(value=str(key)),
                                                                 value=StringValue(value=str(value))))

                table_row = TableResult.TableRow(columns=table_columns)
                table_rows.append(table_row)

            table = TableResult(raw_query=StringValue(value=f"Execute ```{filters}```"),
                                total_count=UInt64Value(value=len(list(result))),
                                rows=table_rows)

            return PlaybookTaskResult(type=PlaybookTaskResultType.LOGS, logs=table, source=self.source)
        except TimeoutException as te:
            raise Exception(f"Timeout error while executing MongoDB task: {te}")
        except Exception as e:
            raise Exception(f"Error while executing MongoDB task: {e}")
