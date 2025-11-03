import threading

from google.protobuf.wrappers_pb2 import StringValue, UInt64Value, Int64Value

from core.integrations.source_api_processors.db_connection_string_processor import DBConnectionStringProcessor
from core.integrations.source_manager import SourceManager
from core.protos.base_pb2 import Source, TimeRange, SourceKeyType, SourceModelType
from core.protos.connectors.connector_pb2 import Connector as ConnectorProto
from core.protos.literal_pb2 import LiteralType, Literal
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResult, TableResult, PlaybookTaskResultType
from core.protos.playbooks.source_task_definitions.sql_data_fetch_task_pb2 import SqlDataFetch
from core.protos.ui_definition_pb2 import FormField, FormFieldType
from core.utils.credentilal_utils import generate_credentials_dict, get_connector_key_type_string, DISPLAY_NAME, CATEGORY, DATABASES


class TimeoutException(Exception):
    pass


class SqlDatabaseConnectionSourceManager(SourceManager):

    def __init__(self):
        self.source = Source.SQL_DATABASE_CONNECTION
        self.task_proto = SqlDataFetch
        self.task_type_callable_map = {
            SqlDataFetch.TaskType.SQL_QUERY: {
                'executor': self.execute_sql_query,
                'model_types': [SourceModelType.SQL_DATABASE_TABLE],
                'result_type': PlaybookTaskResultType.TABLE,
                'display_name': 'Query any SQL Database',
                'category': 'Database',
                'form_fields': [
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
        self.connector_form_configs = [
            {
                "name": StringValue(value="SQL Database Connection String Configuration"),
                "description": StringValue(value="Connect to your SQL database using a connection string."),
                "form_fields": {
                    SourceKeyType.SQL_DATABASE_CONNECTION_STRING_URI: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.SQL_DATABASE_CONNECTION_STRING_URI)),
                        display_name=StringValue(value="Connection String URI"),
                        description=StringValue(value='e.g. "postgresql://user:pass@localhost:5432/mydb", "mysql://user:pass@db.example.com:3306/prod_db"'),
                        helper_text=StringValue(value="Enter the full connection string URI for your SQL database"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                        is_sensitive=True
                    )
                }
            }
        ]
        self.connector_type_details = {
            DISPLAY_NAME: "SQL DATABASE CONNECTION",
            CATEGORY: DATABASES,
        }

    def get_connector_processor(self, sql_db_connector, **kwargs):
        generated_credentials = generate_credentials_dict(sql_db_connector.type, sql_db_connector.keys)
        return DBConnectionStringProcessor(**generated_credentials)

    def execute_sql_query(self, time_range: TimeRange, sql_data_fetch_task: SqlDataFetch,
                          sql_db_connector: ConnectorProto):
        try:
            if not sql_db_connector:
                raise Exception("Task execution Failed:: No SQL Database source found")

            sql_query = sql_data_fetch_task.sql_query
            order_by_column = sql_query.order_by_column.value
            limit = sql_query.limit.value
            offset = sql_query.offset.value
            query = sql_query.query.value
            query = query.strip()
            database = sql_query.database.value
            timeout = sql_query.timeout.value if sql_query.timeout.value else 120

            if query[-1] == ';':
                query = query[:-1]

            def query_db():
                nonlocal query_result, exception
                try:
                    sql_db_processor = self.get_connector_processor(sql_db_connector)
                    query_result = sql_db_processor.get_query_result(query, timeout=timeout)
                except Exception as e:
                    exception = e

            count_result = 0
            query_result = None
            exception = None

            query_thread = threading.Thread(target=query_db)
            query_thread.start()
            query_thread.join(timeout)

            if query_thread.is_alive():
                raise TimeoutException(f"Function 'execute_sql_query' exceeded the timeout of {timeout} seconds")

            if exception:
                raise exception

            print("Playbook Task Downstream Request: Type -> {}, Account -> {}, Query -> {}".format(
                "SQL Database", sql_db_connector.account_id.value, query), flush=True)

            table_rows: [TableResult.TableRow] = []
            col_names = list(query_result.keys())
            query_result = query_result.fetchall()
            for row in query_result:
                table_columns = []
                for i, value in enumerate(row):
                    table_column = TableResult.TableColumn(name=StringValue(value=col_names[i]),
                                                           value=StringValue(value=str(value)))
                    table_columns.append(table_column)
                table_rows.append(TableResult.TableRow(columns=table_columns))
            table = TableResult(raw_query=StringValue(value=f'Execute {query} on {database}'),
                                total_count=UInt64Value(value=int(count_result)),
                                limit=UInt64Value(value=limit),
                                offset=UInt64Value(value=offset),
                                rows=table_rows)
            return PlaybookTaskResult(type=PlaybookTaskResultType.TABLE, table=table, source=self.source)
        except TimeoutException as te:
            raise Exception(f"Timeout error while executing Sql Database task: {te}")
        except Exception as e:
            raise Exception(f"Error while executing Sql Database task: {e}")
