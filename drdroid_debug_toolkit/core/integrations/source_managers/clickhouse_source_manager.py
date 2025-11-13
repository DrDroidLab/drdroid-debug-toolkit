import threading
import logging

from google.protobuf.wrappers_pb2 import StringValue, UInt64Value, Int64Value

from core.integrations.source_api_processors.clickhouse_db_processor import ClickhouseDBProcessor
from core.integrations.source_manager import SourceManager
from core.protos.base_pb2 import Source, TimeRange, SourceModelType, SourceKeyType
from core.protos.connectors.connector_pb2 import Connector as ConnectorProto
from core.protos.literal_pb2 import LiteralType, Literal
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResult, TableResult, PlaybookTaskResultType
from core.protos.playbooks.source_task_definitions.sql_data_fetch_task_pb2 import SqlDataFetch
from core.protos.ui_definition_pb2 import FormField, FormFieldType
from core.utils.credentilal_utils import generate_credentials_dict, get_connector_key_type_string, CATEGORY, DISPLAY_NAME, DATABASES

logger = logging.getLogger(__name__)

class TimeoutException(Exception):
    pass


class ClickhouseSourceManager(SourceManager):

    def __init__(self):
        self.source = Source.CLICKHOUSE
        self.task_proto = SqlDataFetch
        self.task_type_callable_map = {
            SqlDataFetch.TaskType.SQL_QUERY: {
                'executor': self.execute_sql_query,
                'model_types': [SourceModelType.CLICKHOUSE_DATABASE],
                'result_type': PlaybookTaskResultType.TABLE,
                'display_name': 'Query a Clickhouse Database',
                'category': 'Database',
                'form_fields': [
                    FormField(key_name=StringValue(value="database"),
                              display_name=StringValue(value="Database"),
                              description=StringValue(value='Select Database'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT),
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
                "name": StringValue(value="ClickHouse Connection Details"),
                "description": StringValue(value="Configure connection to your ClickHouse database."),
                "form_fields": {
                    SourceKeyType.CLICKHOUSE_INTERFACE: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.CLICKHOUSE_INTERFACE)),
                        display_name=StringValue(value="Interface"),
                        description=StringValue(value='e.g. "http" (default), "https" (secure), "native" (binary protocol)'),
                        helper_text=StringValue(value="Select the ClickHouse interface protocol"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.DROPDOWN_FT,
                        default_value=Literal(type=LiteralType.STRING, string=StringValue(value="http")),
                        valid_values=[
                            Literal(type=LiteralType.STRING, string=StringValue(value="http")),
                            Literal(type=LiteralType.STRING, string=StringValue(value="https")),
                            Literal(type=LiteralType.STRING, string=StringValue(value="native"))
                        ],
                        is_optional=False
                    ),
                    SourceKeyType.CLICKHOUSE_HOST: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.CLICKHOUSE_HOST)),
                        display_name=StringValue(value="Host"),
                        description=StringValue(value='e.g. "clickhouse.example.com", "ch-prod.internal", "10.0.0.1"'),
                        helper_text=StringValue(value="Enter the hostname or IP address of your ClickHouse server"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False
                    ),
                    SourceKeyType.CLICKHOUSE_PORT: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.CLICKHOUSE_PORT)),
                        display_name=StringValue(value="Port"),
                        description=StringValue(value='e.g. "8123" (HTTP), "8443" (HTTPS), "9000" (native protocol)'),
                        helper_text=StringValue(value="Enter the port number for your ClickHouse server"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False
                    ),
                    SourceKeyType.CLICKHOUSE_USER: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.CLICKHOUSE_USER)),
                        display_name=StringValue(value="User"),
                        description=StringValue(value='e.g. "default", "clickhouse_admin", "analytics_user"'),
                        helper_text=StringValue(value="Enter the username for ClickHouse authentication"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True
                    ),
                    SourceKeyType.CLICKHOUSE_PASSWORD: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.CLICKHOUSE_PASSWORD)),
                        display_name=StringValue(value="Password"),
                        description=StringValue(value='e.g. "MySecureP@ssw0rd!"'),
                        helper_text=StringValue(value="Enter the password for ClickHouse authentication"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                        is_sensitive=True
                    )
                }
            }
        ]
        self.connector_type_details = {
            DISPLAY_NAME: "CLICKHOUSE",
            CATEGORY: DATABASES,
        }

    def get_connector_processor(self, clickhouse_connector, **kwargs):
        generated_credentials = generate_credentials_dict(clickhouse_connector.type, clickhouse_connector.keys)
        generated_credentials['database'] = kwargs.get('database', None)
        return ClickhouseDBProcessor(**generated_credentials)

    def execute_sql_query(self, time_range: TimeRange, clickhouse_task: SqlDataFetch,
                          clickhouse_connector: ConnectorProto):
        try:
            if not clickhouse_connector:
                raise Exception("Task execution Failed:: No Clickhouse source found")

            sql_query = clickhouse_task.sql_query
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
                nonlocal result, exception
                try:
                    clickhouse_db_processor = self.get_connector_processor(clickhouse_connector, database=database)
                    result = clickhouse_db_processor.get_query_result(query, timeout=timeout)
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

            table_rows: [TableResult.TableRow] = []
            for row in result.result_set:
                table_columns = []
                for i, column in enumerate(result.column_names):
                    table_column = TableResult.TableColumn(name=StringValue(value=column),
                                                           value=StringValue(value=str(row[i])))
                    table_columns.append(table_column)
                table_rows.append(TableResult.TableRow(columns=table_columns))
            table = TableResult(raw_query=StringValue(value=f'Execute ```{query}``` on {database}'),
                                total_count=UInt64Value(value=int(count_result.result_set[0][0])),
                                limit=UInt64Value(value=limit),
                                offset=UInt64Value(value=offset),
                                rows=table_rows)
            return PlaybookTaskResult(type=PlaybookTaskResultType.TABLE, table=table, source=self.source)
        except TimeoutException as te:
            raise Exception(f"Timeout error while executing Clickhouse task: {te}")
        except Exception as e:
            raise Exception(f"Error while executing Clickhouse task: {e}")
