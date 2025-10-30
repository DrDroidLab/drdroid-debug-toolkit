from google.protobuf.wrappers_pb2 import StringValue, UInt64Value, Int64Value

from core.integrations.source_api_processors.bigquery_api_processor import BigQueryApiProcessor
from core.integrations.source_manager import SourceManager
from core.protos.base_pb2 import Source, TimeRange, SourceKeyType
from core.protos.connectors.connector_pb2 import Connector as ConnectorProto
from core.protos.playbooks.playbook_commons_pb2 import TextResult
from core.protos.literal_pb2 import LiteralType, Literal
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResult, TableResult, PlaybookTaskResultType
from core.protos.playbooks.source_task_definitions.big_query_task_pb2 import BigQuery
from core.protos.ui_definition_pb2 import FormField, FormFieldType
from core.utils.credentilal_utils import generate_credentials_dict, get_connector_key_type_string, CATEGORY, DISPLAY_NAME, ANALYTICS


class BigQuerySourceManager(SourceManager):

    def __init__(self):
        self.source = Source.BIG_QUERY
        self.task_proto = BigQuery
        self.task_type_callable_map = {
            BigQuery.TaskType.QUERY_TABLE: {
                'executor': self.execute_query_table,
                'model_types': [],
                'result_type': PlaybookTaskResultType.TABLE,
                'display_name': 'Query Table from a BigQuery Dataset',
                'category': 'Tables',
                'form_fields': [
                    FormField(key_name=StringValue(value="query"),
                              display_name=StringValue(value="SQL Query"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.MULTILINE_FT),
                    FormField(key_name=StringValue(value="limit"),
                              display_name=StringValue(value="Enter Limit"),
                              data_type=LiteralType.LONG,
                              default_value=Literal(type=LiteralType.LONG, long=Int64Value(value=1000)),
                              form_field_type=FormFieldType.TEXT_FT),
                ]
            },
        }
        self.connector_form_configs = [
            {
                "name": StringValue(value="Google BigQuery Service Account Authentication"),
                "description": StringValue(value="Connect to Google BigQuery using a Service Account JSON key and Project ID."),
                "form_fields": {
                    SourceKeyType.BIG_QUERY_PROJECT_ID: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.BIG_QUERY_PROJECT_ID)),
                        display_name=StringValue(value="Project ID"),
                        description=StringValue(value='e.g. "my-project-123", "analytics-prod-456", "data-warehouse-789"'),
                        helper_text=StringValue(value="Enter your Google Cloud Project ID"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False
                    ),
                    SourceKeyType.BIG_QUERY_SERVICE_ACCOUNT_JSON: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.BIG_QUERY_SERVICE_ACCOUNT_JSON)),
                        display_name=StringValue(value="Service Account JSON"),
                        description=StringValue(value='e.g. {\n  "type": "service_account",\n  "project_id": "my-project-123",\n  "private_key_id": "abc123...",\n  "private_key": "-----BEGIN PRIVATE KEY-----\\n..."\n}'),
                        helper_text=StringValue(value="Paste the content of your Google Cloud Service Account JSON key file"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.MULTILINE_FT,
                        is_optional=False
                    )
                }
            }
        ]
        self.connector_type_details = {
            DISPLAY_NAME: "BIG QUERY",
            CATEGORY: ANALYTICS,
        }

    def get_connector_processor(self, bq_connector, **kwargs):
        generated_credentials = generate_credentials_dict(bq_connector.type, bq_connector.keys)
        return BigQueryApiProcessor(**generated_credentials)

    def execute_query_table(self, time_range: TimeRange, bq_task: BigQuery,
                            bq_connector: ConnectorProto):
        try:
            if not bq_connector:
                raise Exception("Task execution Failed:: No BigQuery source found")

            query_table = bq_task.query_table
            dataset = query_table.dataset.value
            table = query_table.table.value
            query = query_table.query.value
            limit = query_table.limit.value if query_table.limit.value else 1000

            if not dataset or not table:
                raise Exception("Task execution Failed:: No dataset or table found")

            query = query.strip()

            bq_client = self.get_connector_processor(bq_connector)

            full_query = f"{query} LIMIT {limit}"

            print("Playbook Task Downstream Request: Type -> {}, Account -> {}, Query -> {}".format(
                "BigQuery", bq_connector.account_id.value, full_query), flush=True)

            bq_job = bq_client.query(full_query)
            if not bq_job or not bq_job.total_rows:
                return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT, text=TextResult(output=StringValue(
                    value=f"No data returned from Big Query for query: {full_query}")), source=self.source)

            rows = [dict(row) for row in bq_job]
            count_result = len(rows)
            if count_result == 0:
                return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT, text=TextResult(output=StringValue(
                    value=f"No data returned from Big Query for query: {full_query}")), source=self.source)

            table_rows: [TableResult.TableRow] = []
            for row in rows:
                table_columns = []
                for column, value in row.items():
                    table_column = TableResult.TableColumn(name=StringValue(value=column),
                                                           value=StringValue(value=str(value)))
                    table_columns.append(table_column)
                table_rows.append(TableResult.TableRow(columns=table_columns))
            table_result = TableResult(raw_query=StringValue(value=f"Execute ```{query}``` on table {table}"),
                                       total_count=UInt64Value(value=count_result),
                                       rows=table_rows)
            return PlaybookTaskResult(type=PlaybookTaskResultType.TABLE, table=table_result, source=self.source)
        except Exception as e:
            raise Exception(f"Error while executing BigQuery task: {e}")
