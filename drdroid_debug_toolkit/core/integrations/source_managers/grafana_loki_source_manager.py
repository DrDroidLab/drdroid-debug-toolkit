import logging
import re
from datetime import datetime, timedelta

from google.protobuf.wrappers_pb2 import StringValue, UInt64Value, Int64Value, BoolValue

from core.integrations.source_api_processors.grafana_loki_api_processor import GrafanaLokiApiProcessor
from core.utils.logql_utils import cleanup_logql_query, LogQLValidationError

logger = logging.getLogger(__name__)
from core.integrations.source_manager import SourceManager
from core.protos.base_pb2 import TimeRange, Source, SourceKeyType
from core.protos.connectors.connector_pb2 import Connector as ConnectorProto
from core.protos.literal_pb2 import LiteralType, Literal
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResult, PlaybookTaskResultType, TableResult, TextResult
from core.protos.playbooks.source_task_definitions.grafana_loki_task_pb2 import GrafanaLoki
from core.protos.ui_definition_pb2 import FormField, FormFieldType
from core.utils.credentilal_utils import generate_credentials_dict, get_connector_key_type_string, CATEGORY, DISPLAY_NAME, APPLICATION_MONITORING


class GrafanaLokiSourceManager(SourceManager):

    def __init__(self):
        self.source = Source.GRAFANA_LOKI
        self.task_proto = GrafanaLoki
        self.task_type_callable_map = {
            GrafanaLoki.TaskType.QUERY_LOGS: {
                'executor': self.execute_query_logs,
                'model_types': [],
                'result_type': PlaybookTaskResultType.LOGS,
                'display_name': 'Query Logs from Grafana Loki',
                'category': 'Logs',
                'form_fields': [
                    FormField(key_name=StringValue(value="query"),
                              display_name=StringValue(value="Query"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.MULTILINE_FT),
                    FormField(key_name=StringValue(value="limit"),
                              display_name=StringValue(value="Limit"),
                              data_type=LiteralType.LONG,
                              default_value=Literal(type=LiteralType.LONG, long=Int64Value(value=10)),
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="start_time"),
                              display_name=StringValue(value="Start Time"),
                              data_type=LiteralType.LONG,
                              is_date_time_field=True,
                              default_value=Literal(type=LiteralType.LONG, long=Int64Value(
                                  value=int((datetime.now() - timedelta(minutes=30)).timestamp()))),
                              form_field_type=FormFieldType.DATE_FT),
                    FormField(key_name=StringValue(value="end_time"),
                              display_name=StringValue(value="End Time"),
                              data_type=LiteralType.LONG,
                              is_date_time_field=True,
                              default_value=Literal(type=LiteralType.LONG, long=Int64Value(
                                  value=int((datetime.now()).timestamp()))),
                              form_field_type=FormFieldType.DATE_FT),
                ]
            }
        }

        self.connector_form_configs = [
            {
                "name": StringValue(value="Grafana Loki Connection (Full)"),
                "description": StringValue(value="Connect to Grafana Loki specifying Protocol, Host, Port, X-Scope-OrgID, and SSL verification."),
                "form_fields": {
                    SourceKeyType.GRAFANA_LOKI_PROTOCOL: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.GRAFANA_LOKI_PROTOCOL)),
                        display_name=StringValue(value="Protocol"),
                        description=StringValue(value='Choose "http" for local/development or "https" for production environments'),
                        helper_text=StringValue(value="Select the protocol used to connect to your Grafana Loki instance"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.DROPDOWN_FT,
                        valid_values=[Literal(type=LiteralType.STRING, string=StringValue(value='http')),
                                      Literal(type=LiteralType.STRING, string=StringValue(value='https'))],
                        default_value=Literal(type=LiteralType.STRING, string=StringValue(value='http')),
                        is_optional=False
                    ),
                    SourceKeyType.GRAFANA_LOKI_HOST: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.GRAFANA_LOKI_HOST)),
                        display_name=StringValue(value="Host"),
                        description=StringValue(value='e.g. "loki.example.com", "localhost", "10.0.0.10"'),
                        helper_text=StringValue(value="Enter your Grafana Loki host address (domain name or IP, without protocol or port)"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False
                    ),
                    SourceKeyType.GRAFANA_LOKI_PORT: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.GRAFANA_LOKI_PORT)),
                        display_name=StringValue(value="Port"),
                        description=StringValue(value='e.g. 3100 (default), 9096 (with proxy), 443 (HTTPS)'),
                        helper_text=StringValue(value="Enter the port number where Grafana Loki is listening (defaults to 3100 if empty)"),
                        data_type=LiteralType.LONG,
                        form_field_type=FormFieldType.TEXT_FT,
                        default_value=Literal(type=LiteralType.LONG, long=Int64Value(value=3100)),
                        is_optional=True
                    ),
                    SourceKeyType.X_SCOPE_ORG_ID: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.X_SCOPE_ORG_ID)),
                        display_name=StringValue(value="X-Scope-OrgID"),
                        description=StringValue(value='e.g. "tenant1", "org123", "team-prod"'),
                        helper_text=StringValue(value="Enter your organization ID for multi-tenant Loki setups (defaults to 'anonymous' if empty)"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True
                    ),
                    SourceKeyType.SSL_VERIFY: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.SSL_VERIFY)),
                        display_name=StringValue(value="SSL Verify"),
                        description=StringValue(value="Enable for production environments, disable for self-signed certificates"),
                        helper_text=StringValue(value="Toggle SSL certificate verification (recommended to keep enabled unless using self-signed certificates)"),
                        data_type=LiteralType.BOOLEAN,
                        form_field_type=FormFieldType.CHECKBOX_FT,
                        default_value=Literal(type=LiteralType.BOOLEAN, boolean=BoolValue(value=True)),
                        is_optional=True
                    )
                }
            },
            {
                "name": StringValue(value="Grafana Loki Connection (No SSL Verify)"),
                "description": StringValue(value="Connect to Grafana Loki specifying Protocol, Host, Port, and X-Scope-OrgID."),
                "form_fields": {
                    SourceKeyType.GRAFANA_LOKI_PROTOCOL: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.GRAFANA_LOKI_PROTOCOL)),
                        display_name=StringValue(value="Protocol"),
                        description=StringValue(value='Choose "http" for local/development or "https" for production environments'),
                        helper_text=StringValue(value="Select the protocol used to connect to your Grafana Loki instance"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.DROPDOWN_FT,
                        valid_values=[Literal(type=LiteralType.STRING, string=StringValue(value='http')),
                                      Literal(type=LiteralType.STRING, string=StringValue(value='https'))],
                        default_value=Literal(type=LiteralType.STRING, string=StringValue(value='http')),
                        is_optional=False
                    ),
                    SourceKeyType.GRAFANA_LOKI_HOST: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.GRAFANA_LOKI_HOST)),
                        display_name=StringValue(value="Host"),
                        description=StringValue(value='e.g. "loki.example.com", "localhost", "10.0.0.10"'),
                        helper_text=StringValue(value="Enter your Grafana Loki host address (domain name or IP, without protocol or port)"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False
                    ),
                    SourceKeyType.GRAFANA_LOKI_PORT: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.GRAFANA_LOKI_PORT)),
                        display_name=StringValue(value="Port"),
                        description=StringValue(value='e.g. 3100 (default), 9096 (with proxy), 443 (HTTPS)'),
                        helper_text=StringValue(value="Enter the port number where Grafana Loki is listening (defaults to 3100 if empty)"),
                        data_type=LiteralType.LONG,
                        form_field_type=FormFieldType.TEXT_FT,
                        default_value=Literal(type=LiteralType.LONG, long=Int64Value(value=3100)),
                        is_optional=True
                    ),
                    SourceKeyType.X_SCOPE_ORG_ID: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.X_SCOPE_ORG_ID)),
                        display_name=StringValue(value="X-Scope-OrgID"),
                        description=StringValue(value='e.g. "tenant1", "org123", "team-prod"'),
                        helper_text=StringValue(value="Enter your organization ID for multi-tenant Loki setups (defaults to 'anonymous' if empty)"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True
                    )
                }
            }
        ]
        self.connector_type_details = {
            DISPLAY_NAME: "GRAFANA LOKI",
            CATEGORY: APPLICATION_MONITORING,
        }

    def get_connector_processor(self, grafana_loki_connector, **kwargs):
        generated_credentials = generate_credentials_dict(grafana_loki_connector.type, grafana_loki_connector.keys)
        return GrafanaLokiApiProcessor(**generated_credentials)

    def _cleanup_logql_query(self, query: str) -> str:
        """
        Clean up and validate a LogQL query using the shared utility.

        This handles:
        - Double-encoded quotes
        - Unicode/smart quotes
        - Whitespace/newlines
        - Empty selector validation
        """
        try:
            return cleanup_logql_query(query, validate=True)
        except LogQLValidationError as e:
            # Log the validation error but don't raise - let Loki return the proper error
            logger.warning(f"LogQL query validation warning: {e}")
            # Still return the cleaned query without validation
            return cleanup_logql_query(query, validate=False)

    def execute_query_logs(self, time_range: TimeRange, grafana_loki_task: GrafanaLoki,
                           grafana_loki_connector: ConnectorProto):
        try:
            if not grafana_loki_connector:
                raise Exception("Task execution Failed:: No Grafana Loki source found")

            tr_end_time = time_range.time_lt
            tr_start_time = time_range.time_geq
            task = grafana_loki_task.query_logs

            current_datetime = datetime.utcfromtimestamp(tr_end_time)
            current_datetime = datetime.utcfromtimestamp(
                task.end_time.value) if task.end_time.value else current_datetime

            evaluation_time = datetime.utcfromtimestamp(tr_start_time)
            evaluation_time = datetime.utcfromtimestamp(
                task.start_time.value) if task.start_time.value else evaluation_time

            start_time = evaluation_time.isoformat() + "Z"
            end_time = current_datetime.isoformat() + "Z"

            # Clean up the query - remove extra whitespace/newlines
            query = self._cleanup_logql_query(task.query.value)

            limit = task.limit.value if task.limit.value else 2000

            grafana_loki_api_processor = self.get_connector_processor(grafana_loki_connector)

            print(
                "Playbook Task Downstream Request: Type -> {}, Account -> {}, Query -> {}, Start_Time "
                "-> {}, End_Time -> {}".format("Grafana", grafana_loki_connector.account_id.value, query, start_time,
                                               end_time), flush=True)

            response = grafana_loki_api_processor.query(query, start_time, end_time, limit)
            if not response:
                return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT, text=TextResult(output=StringValue(
                    value=f"No data returned from Grafana Loki for query: {query}")), source=self.source)

            result = response.get('data', {}).get('result', [])
            table_rows: [TableResult.TableRow] = []
            for r in result:
                table_meta_columns = []
                data_rows = []
                for key, value in r.items():
                    if key == 'stream' or key == 'metric':
                        for k, v in value.items():
                            table_meta_columns.append(TableResult.TableColumn(name=StringValue(value=str(k)),
                                                                              value=StringValue(value=str(v))))
                    elif key == 'values':
                        for v in value:
                            table_columns = []
                            for i, val in enumerate(v):
                                if i == 0:
                                    key = 'timestamp'
                                else:
                                    key = 'log'
                                table_columns.append(TableResult.TableColumn(name=StringValue(value=key),
                                                                             value=StringValue(value=str(val))))
                            data_rows.append(table_columns)
                for dc in data_rows:
                    update_columns = table_meta_columns + dc
                    table_row = TableResult.TableRow(columns=update_columns)
                    table_rows.append(table_row)
            table = TableResult(raw_query=StringValue(value=f"Execute ```{query}```"),
                                total_count=UInt64Value(value=len(result)),
                                rows=table_rows)
            return PlaybookTaskResult(type=PlaybookTaskResultType.LOGS, logs=table, source=self.source)
        except Exception as e:
            raise Exception(f"Error while executing Grafana task: {e}")
