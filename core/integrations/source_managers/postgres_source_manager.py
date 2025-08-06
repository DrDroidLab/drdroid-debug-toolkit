import threading

from google.protobuf.wrappers_pb2 import StringValue, UInt64Value, Int64Value

from core.integrations.source_api_processors.postgres_db_processor import PostgresDBProcessor
from core.integrations.source_manager import SourceManager
from core.protos.base_pb2 import Source, TimeRange, SourceModelType, SourceKeyType
from core.protos.connectors.connector_pb2 import Connector as ConnectorProto
from core.protos.literal_pb2 import LiteralType, Literal
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResult, TableResult, PlaybookTaskResultType
from core.protos.playbooks.source_task_definitions.sql_data_fetch_task_pb2 import SqlDataFetch
from core.protos.ui_definition_pb2 import FormField, FormFieldType
from core.utils.credentilal_utils import generate_credentials_dict


class TimeoutException(Exception):
    pass


class PostgresSourceManager(SourceManager):

    def __init__(self):
        self.source = Source.POSTGRES
        self.task_proto = SqlDataFetch
        self.task_type_callable_map = {
            SqlDataFetch.TaskType.SQL_QUERY: {
                'executor': self.execute_sql_query,
                'model_types': [SourceModelType.POSTGRES_QUERY],
                'result_type': PlaybookTaskResultType.TABLE,
                'display_name': 'Query a Postgres Database',
                'category': 'Database',
                'form_fields': [
                    FormField(key_name=StringValue(value="database"),
                              display_name=StringValue(value="Database"),
                              description=StringValue(value='Enter Database'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="query"),
                              display_name=StringValue(value="Query"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.MULTILINE_FT),
                    FormField(key_name=StringValue(value="timeout"),
                              display_name=StringValue(value="Timeout (in seconds)"),
                              description=StringValue(value='Enter Timeout (in seconds)'),
                              data_type=LiteralType.LONG,
                              default_value=Literal(type=LiteralType.LONG, long=Int64Value(value=120)),
                              form_field_type=FormFieldType.TEXT_FT)
                ]
            },
        }

    def get_connector_processor(self, pg_connector, **kwargs):
        generated_credentials = generate_credentials_dict(pg_connector.type, pg_connector.keys)
        if kwargs and 'database' in kwargs:
            generated_credentials['database'] = kwargs['database']
        return PostgresDBProcessor(**generated_credentials)

    def execute_sql_query(self, time_range: TimeRange, pg_task: SqlDataFetch,
                          pg_connector: ConnectorProto):
        try:
            if not pg_connector:
                raise Exception("Task execution Failed:: No Postgres source found")

            sql_query = pg_task.sql_query
            order_by_column = sql_query.order_by_column.value
            limit = sql_query.limit.value
            offset = sql_query.offset.value
            query = sql_query.query.value
            query = query.strip()
            database = sql_query.database.value
            timeout = sql_query.timeout.value if sql_query.timeout.value else 120

            if not database:
                pg_keys = pg_connector.keys
                for key in pg_keys:
                    if key.key_type == SourceKeyType.POSTGRES_DATABASE:
                        database = key.key.value
                        break
            if not database:
                raise Exception("Task execution Failed:: No Postgres database found")

            if query[-1] == ';':
                query = query[:-1]

            def query_db():
                nonlocal result, exception
                try:
                    pg_db_processor = self.get_connector_processor(pg_connector, database=database)
                    result = pg_db_processor.get_query_result(query, timeout=timeout)
                except Exception as e:
                    exception = e

            count_result = 0
            result = None
            exception = None

            query_thread = threading.Thread(target=query_db)
            query_thread.start()
            query_thread.join(timeout)

            if query_thread.is_alive():
                raise TimeoutException(f"Function 'execute_sql_query' exceeded the timeout of {timeout} seconds")

            if exception:
                raise exception

            print("Playbook Task Downstream Request: Type -> {}, Account -> {}, Query -> {}".format(
                "Postgres", pg_connector.account_id.value, query), flush=True)

            table_rows: [TableResult.TableRow] = []
            for row in result:
                table_columns = []
                for column, value in row.items():
                    table_column = TableResult.TableColumn(name=StringValue(value=column),
                                                           value=StringValue(value=str(value)))
                    table_columns.append(table_column)
                table_rows.append(TableResult.TableRow(columns=table_columns))
            table = TableResult(raw_query=StringValue(value=f'Execute {query} on {database}'),
                                total_count=UInt64Value(value=int(count_result)),
                                limit=UInt64Value(value=limit),
                                offset=UInt64Value(value=offset),
                                rows=table_rows)
            return PlaybookTaskResult(type=PlaybookTaskResultType.TABLE, table=table, source=self.source)
        except Exception as e:
            raise Exception(f"Error while executing Postgres task: {e}")
