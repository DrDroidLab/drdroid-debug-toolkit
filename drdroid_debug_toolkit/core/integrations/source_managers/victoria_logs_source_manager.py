import json
from datetime import datetime

from google.protobuf.wrappers_pb2 import StringValue, UInt64Value, Int64Value, BoolValue

from integrations.source_api_processors.victoria_logs_api_processor import VictoriaLogsApiProcessor
from core.integrations.source_manager import SourceManager
from core.protos.base_pb2 import TimeRange, Source, SourceKeyType, SourceModelType
from core.protos.connectors.connector_pb2 import Connector as ConnectorProto
from core.protos.literal_pb2 import LiteralType, Literal
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResult, PlaybookTaskResultType, TableResult, \
    TextResult
from core.protos.playbooks.source_task_definitions.victoria_logs_task_pb2 import VictoriaLogs
from core.protos.ui_definition_pb2 import FormField, FormFieldType
from core.utils.credentilal_utils import DISPLAY_NAME, CATEGORY, APPLICATION_MONITORING, get_connector_key_type_string, generate_credentials_dict


class VictoriaLogsSourceManager(SourceManager):

    def __init__(self):
        # Use VICTORIA_METRICS connector type
        self.source = Source.VICTORIA_LOGS
        self.task_proto = VictoriaLogs
        self.task_type_callable_map = {
            VictoriaLogs.TaskType.QUERY_LOGS: {
                'executor': self.execute_query_logs,
                'model_types': [],
                'result_type': PlaybookTaskResultType.LOGS,
                'display_name': 'Query Logs (VictoriaLogs LogsQL)',
                'category': 'Logs',
                'form_fields': [
                    FormField(key_name=StringValue(value="query"),
                              display_name=StringValue(value="LogsQL Query"),
                              description=StringValue(value='Use LogsQL. Example: `_time:1h {app="nginx"} error | count()`'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.MULTILINE_FT),
                    FormField(key_name=StringValue(value="limit"),
                              display_name=StringValue(value="Limit"),
                              data_type=LiteralType.LONG,
                              default_value=Literal(type=LiteralType.LONG, long=Int64Value(value=100)),
                              form_field_type=FormFieldType.TEXT_FT)
                ]
            }
        }

        # Connection form - VictoriaLogs specific keys
        self.connector_form_configs = [
            {
                "name": StringValue(value="VictoriaLogs Connection"),
                "description": StringValue(value="Connect to VictoriaLogs by specifying protocol, host, port, optional headers and SSL verification."),
                "form_fields": {
                    SourceKeyType.VICTORIA_LOGS_PROTOCOL: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.VICTORIA_LOGS_PROTOCOL)),
                        display_name=StringValue(value="Protocol"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.DROPDOWN_FT,
                        valid_values=[Literal(type=LiteralType.STRING, string=StringValue(value='http')),
                                      Literal(type=LiteralType.STRING, string=StringValue(value='https'))],
                        default_value=Literal(type=LiteralType.STRING, string=StringValue(value='http')),
                        is_optional=False
                    ),
                    SourceKeyType.VICTORIA_LOGS_HOST: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.VICTORIA_LOGS_HOST)),
                        display_name=StringValue(value="Host"),
                        description=StringValue(value='e.g. "vlogs.example.com" or "localhost"'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False
                    ),
                    SourceKeyType.VICTORIA_LOGS_PORT: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.VICTORIA_LOGS_PORT)),
                        display_name=StringValue(value="Port"),
                        description=StringValue(value='Default 9428 for VictoriaLogs'),
                        data_type=LiteralType.LONG,
                        form_field_type=FormFieldType.TEXT_FT,
                        default_value=Literal(type=LiteralType.LONG, long=Int64Value(value=9428)),
                        is_optional=True
                    ),
                    SourceKeyType.MCP_SERVER_AUTH_HEADERS: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.MCP_SERVER_AUTH_HEADERS)),
                        display_name=StringValue(value="Custom Headers (JSON)"),
                        helper_text=StringValue(value="Optional JSON headers e.g. {\"Authorization\": \"Bearer <token>\"}"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.MULTILINE_FT,
                        is_optional=True
                    ),
                    SourceKeyType.SSL_VERIFY: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.SSL_VERIFY)),
                        display_name=StringValue(value="SSL Verify"),
                        data_type=LiteralType.BOOLEAN,
                        form_field_type=FormFieldType.CHECKBOX_FT,
                        default_value=Literal(type=LiteralType.BOOLEAN, boolean=BoolValue(value=True)),
                        is_optional=True
                    )
                }
            }
        ]

        self.connector_type_details = {
            DISPLAY_NAME: "VICTORIA LOGS",
            CATEGORY: APPLICATION_MONITORING,
        }

    def get_connector_processor(self, connector: ConnectorProto, **kwargs):
        generated_credentials = generate_credentials_dict(connector.type, connector.keys) or {}
        return VictoriaLogsApiProcessor(**generated_credentials)

    def execute_query_logs(self, time_range: TimeRange, task: VictoriaLogs, connector: ConnectorProto):
        try:
            if not connector:
                raise Exception("Task execution Failed:: No VictoriaLogs source found")

            q = task.query_logs.query.value
            limit = task.query_logs.limit.value if task.query_logs.limit and task.query_logs.limit.value else 100

            # Convert playbook TimeRange to RFC3339 timestamps
            # Only pass start/end if the query doesn't already specify a _time filter
            start_arg = None
            end_arg = None
            if q is None:
                q = ''
            if '_time:' not in q:
                try:
                    # time_geq and time_lt are expected to be epoch seconds
                    start_dt = datetime.utcfromtimestamp(int(time_range.time_geq))
                    end_dt = datetime.utcfromtimestamp(int(time_range.time_lt))
                    start_arg = start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                    end_arg = end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                except Exception:
                    # Fallback: no start/end args
                    start_arg = None
                    end_arg = None

            api = self.get_connector_processor(connector)

            print(
                "Playbook Task Downstream Request: Type -> {}, Account -> {}, LogsQL -> {}".format(
                    "VictoriaLogs", connector.account_id.value, q), flush=True)

            response = api.query_logsql(q, limit=int(limit) if '| limit' not in q else None,
                                        start=start_arg, end=end_arg)
            if not response:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"No data returned from VictoriaLogs for query: {q}")),
                    source=self.source
                )

            # Normalize into TableResult similar to Loki
            rows = self._convert_response_to_table_rows(response)
            # Include args used in execution for transparency
            args_desc = []
            if start_arg:
                args_desc.append(f"start={start_arg}")
            if end_arg:
                args_desc.append(f"end={end_arg}")
            if '| limit' not in q and limit:
                args_desc.append(f"limit={int(limit)}")
            args_str = (" (" + ", ".join(args_desc) + ")") if args_desc else ""

            table = TableResult(
                raw_query=StringValue(value=f"Execute ```{q}```{args_str}"),
                total_count=UInt64Value(value=len(rows)),
                rows=rows
            )
            return PlaybookTaskResult(type=PlaybookTaskResultType.LOGS, logs=table, source=self.source)
        except Exception as e:
            raise Exception(f"Error while executing VictoriaLogs task: {e}")

    def execute_fetch_field_values(self, time_range: TimeRange, task: VictoriaLogs, connector: ConnectorProto):
        try:
            if not connector:
                raise Exception("Task execution Failed:: No VictoriaLogs source found")

            ff = task.fetch_field_values
            field_name = ff.field_name.value
            time_filter = ff.time_filter.value if ff.time_filter and ff.time_filter.value else '_time:1h'
            limit = ff.limit.value if ff.limit and ff.limit.value else 100

            api = self.get_connector_processor(connector)
            print(
                "Playbook Task Downstream Request: Type -> {}, Account -> {}, Field -> {}, Time -> {}, Limit -> {}".format(
                    "VictoriaLogs", connector.account_id.value, field_name, time_filter, limit), flush=True)

            response = api.fetch_field_values(field_name, time_filter=time_filter, limit=int(limit))
            if not response:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"No values returned for field: {field_name}")),
                    source=self.source
                )

            rows = self._convert_response_to_table_rows(response)
            table = TableResult(
                raw_query=StringValue(value=f"Execute ```{time_filter} | field_values {field_name} | limit {int(limit)}```"),
                total_count=UInt64Value(value=len(rows)),
                rows=rows
            )
            return PlaybookTaskResult(type=PlaybookTaskResultType.LOGS, logs=table, source=self.source)
        except Exception as e:
            raise Exception(f"Error while executing VictoriaLogs field values task: {e}")

    def _convert_response_to_table_rows(self, response: dict) -> list[TableResult.TableRow]:
        rows: list[TableResult.TableRow] = []
        if not isinstance(response, dict):
            return rows

        # VictoriaLogs returns JSON arrays for functions like field_values or a table-like structure for queries
        # Try to accommodate common response shapes.
        data = response.get('data') if isinstance(response.get('data'), list) else response.get('result') or response

        # If plain text was returned, place in a single-row table
        if isinstance(response.get('text'), str):
            tr = TableResult.TableRow(columns=[
                TableResult.TableColumn(name=StringValue(value='text'), value=StringValue(value=str(response['text'])))
            ])
            rows.append(tr)
            return rows

        if isinstance(data, list):
            for item in data:
                columns = []
                if isinstance(item, dict):
                    for k, v in item.items():
                        columns.append(TableResult.TableColumn(name=StringValue(value=str(k)),
                                                               value=StringValue(value=str(v))))
                else:
                    columns.append(TableResult.TableColumn(name=StringValue(value='value'),
                                                           value=StringValue(value=str(item))))
                rows.append(TableResult.TableRow(columns=columns))
            return rows

        # Fallback: single row with raw JSON
        columns = [TableResult.TableColumn(name=StringValue(value='raw'),
                                           value=StringValue(value=json.dumps(response)[:100000]))]
        rows.append(TableResult.TableRow(columns=columns))
        return rows
