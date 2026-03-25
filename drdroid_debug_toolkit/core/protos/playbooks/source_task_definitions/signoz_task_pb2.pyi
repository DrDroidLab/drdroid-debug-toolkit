from google.protobuf import wrappers_pb2 as _wrappers_pb2
from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Signoz(_message.Message):
    __slots__ = ("type", "clickhouse_query", "builder_query", "dashboard_data", "fetch_dashboards", "fetch_dashboard_details", "fetch_services", "fetch_apm_metrics", "fetch_logs", "fetch_logs_for_trace", "fetch_traces", "fetch_alert_rules", "fetch_service_map", "fetch_alerts_summary")
    class TaskType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        UNKNOWN: _ClassVar[Signoz.TaskType]
        CLICKHOUSE_QUERY: _ClassVar[Signoz.TaskType]
        BUILDER_QUERY: _ClassVar[Signoz.TaskType]
        DASHBOARD_DATA: _ClassVar[Signoz.TaskType]
        FETCH_DASHBOARDS: _ClassVar[Signoz.TaskType]
        FETCH_DASHBOARD_DETAILS: _ClassVar[Signoz.TaskType]
        FETCH_SERVICES: _ClassVar[Signoz.TaskType]
        FETCH_APM_METRICS: _ClassVar[Signoz.TaskType]
        FETCH_LOGS: _ClassVar[Signoz.TaskType]
        FETCH_LOGS_FOR_TRACE: _ClassVar[Signoz.TaskType]
        FETCH_TRACES: _ClassVar[Signoz.TaskType]
        FETCH_ALERT_RULES: _ClassVar[Signoz.TaskType]
        FETCH_SERVICE_MAP: _ClassVar[Signoz.TaskType]
        FETCH_ALERTS_SUMMARY: _ClassVar[Signoz.TaskType]
    UNKNOWN: Signoz.TaskType
    CLICKHOUSE_QUERY: Signoz.TaskType
    BUILDER_QUERY: Signoz.TaskType
    DASHBOARD_DATA: Signoz.TaskType
    FETCH_DASHBOARDS: Signoz.TaskType
    FETCH_DASHBOARD_DETAILS: Signoz.TaskType
    FETCH_SERVICES: Signoz.TaskType
    FETCH_APM_METRICS: Signoz.TaskType
    FETCH_LOGS: Signoz.TaskType
    FETCH_LOGS_FOR_TRACE: Signoz.TaskType
    FETCH_TRACES: Signoz.TaskType
    FETCH_ALERT_RULES: Signoz.TaskType
    FETCH_SERVICE_MAP: Signoz.TaskType
    FETCH_ALERTS_SUMMARY: Signoz.TaskType
    class ClickhouseQueryTask(_message.Message):
        __slots__ = ("query", "request_type")
        QUERY_FIELD_NUMBER: _ClassVar[int]
        REQUEST_TYPE_FIELD_NUMBER: _ClassVar[int]
        query: _wrappers_pb2.StringValue
        request_type: _wrappers_pb2.StringValue
        def __init__(self, query: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., request_type: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...
    class BuilderQueryTask(_message.Message):
        __slots__ = ("builder_queries", "step", "panel_type")
        BUILDER_QUERIES_FIELD_NUMBER: _ClassVar[int]
        STEP_FIELD_NUMBER: _ClassVar[int]
        PANEL_TYPE_FIELD_NUMBER: _ClassVar[int]
        builder_queries: _wrappers_pb2.StringValue
        step: _wrappers_pb2.Int32Value
        panel_type: _wrappers_pb2.StringValue
        def __init__(self, builder_queries: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., step: _Optional[_Union[_wrappers_pb2.Int32Value, _Mapping]] = ..., panel_type: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...
    class DashboardDataTask(_message.Message):
        __slots__ = ("dashboard_name", "step", "panel_type", "variables_json", "panel_ids")
        DASHBOARD_NAME_FIELD_NUMBER: _ClassVar[int]
        STEP_FIELD_NUMBER: _ClassVar[int]
        PANEL_TYPE_FIELD_NUMBER: _ClassVar[int]
        VARIABLES_JSON_FIELD_NUMBER: _ClassVar[int]
        PANEL_IDS_FIELD_NUMBER: _ClassVar[int]
        dashboard_name: _wrappers_pb2.StringValue
        step: _wrappers_pb2.Int32Value
        panel_type: _wrappers_pb2.StringValue
        variables_json: _wrappers_pb2.StringValue
        panel_ids: _wrappers_pb2.StringValue
        def __init__(self, dashboard_name: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., step: _Optional[_Union[_wrappers_pb2.Int32Value, _Mapping]] = ..., panel_type: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., variables_json: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., panel_ids: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...
    class FetchDashboardsTask(_message.Message):
        __slots__ = ()
        def __init__(self) -> None: ...
    class FetchDashboardDetailsTask(_message.Message):
        __slots__ = ("dashboard_id",)
        DASHBOARD_ID_FIELD_NUMBER: _ClassVar[int]
        dashboard_id: _wrappers_pb2.StringValue
        def __init__(self, dashboard_id: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...
    class FetchServicesTask(_message.Message):
        __slots__ = ("start_time", "end_time", "duration")
        START_TIME_FIELD_NUMBER: _ClassVar[int]
        END_TIME_FIELD_NUMBER: _ClassVar[int]
        DURATION_FIELD_NUMBER: _ClassVar[int]
        start_time: _wrappers_pb2.StringValue
        end_time: _wrappers_pb2.StringValue
        duration: _wrappers_pb2.StringValue
        def __init__(self, start_time: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., end_time: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., duration: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...
    class FetchApmMetricsTask(_message.Message):
        __slots__ = ("service_name", "start_time", "end_time", "window", "operation_names", "metrics", "duration")
        SERVICE_NAME_FIELD_NUMBER: _ClassVar[int]
        START_TIME_FIELD_NUMBER: _ClassVar[int]
        END_TIME_FIELD_NUMBER: _ClassVar[int]
        WINDOW_FIELD_NUMBER: _ClassVar[int]
        OPERATION_NAMES_FIELD_NUMBER: _ClassVar[int]
        METRICS_FIELD_NUMBER: _ClassVar[int]
        DURATION_FIELD_NUMBER: _ClassVar[int]
        service_name: _wrappers_pb2.StringValue
        start_time: _wrappers_pb2.StringValue
        end_time: _wrappers_pb2.StringValue
        window: _wrappers_pb2.StringValue
        operation_names: _wrappers_pb2.StringValue
        metrics: _wrappers_pb2.StringValue
        duration: _wrappers_pb2.StringValue
        def __init__(self, service_name: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., start_time: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., end_time: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., window: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., operation_names: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., metrics: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., duration: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...
    class FetchLogsTask(_message.Message):
        __slots__ = ("filter_expression", "start_time", "end_time", "duration", "limit")
        FILTER_EXPRESSION_FIELD_NUMBER: _ClassVar[int]
        START_TIME_FIELD_NUMBER: _ClassVar[int]
        END_TIME_FIELD_NUMBER: _ClassVar[int]
        DURATION_FIELD_NUMBER: _ClassVar[int]
        LIMIT_FIELD_NUMBER: _ClassVar[int]
        filter_expression: _wrappers_pb2.StringValue
        start_time: _wrappers_pb2.StringValue
        end_time: _wrappers_pb2.StringValue
        duration: _wrappers_pb2.StringValue
        limit: _wrappers_pb2.Int32Value
        def __init__(self, filter_expression: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., start_time: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., end_time: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., duration: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., limit: _Optional[_Union[_wrappers_pb2.Int32Value, _Mapping]] = ...) -> None: ...
    class FetchLogsForTraceTask(_message.Message):
        __slots__ = ("trace_id", "limit")
        TRACE_ID_FIELD_NUMBER: _ClassVar[int]
        LIMIT_FIELD_NUMBER: _ClassVar[int]
        trace_id: _wrappers_pb2.StringValue
        limit: _wrappers_pb2.Int32Value
        def __init__(self, trace_id: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., limit: _Optional[_Union[_wrappers_pb2.Int32Value, _Mapping]] = ...) -> None: ...
    class FetchTracesTask(_message.Message):
        __slots__ = ("filter_expression", "start_time", "end_time", "duration", "limit")
        FILTER_EXPRESSION_FIELD_NUMBER: _ClassVar[int]
        START_TIME_FIELD_NUMBER: _ClassVar[int]
        END_TIME_FIELD_NUMBER: _ClassVar[int]
        DURATION_FIELD_NUMBER: _ClassVar[int]
        LIMIT_FIELD_NUMBER: _ClassVar[int]
        filter_expression: _wrappers_pb2.StringValue
        start_time: _wrappers_pb2.StringValue
        end_time: _wrappers_pb2.StringValue
        duration: _wrappers_pb2.StringValue
        limit: _wrappers_pb2.Int32Value
        def __init__(self, filter_expression: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., start_time: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., end_time: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., duration: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., limit: _Optional[_Union[_wrappers_pb2.Int32Value, _Mapping]] = ...) -> None: ...
    class FetchAlertRulesTask(_message.Message):
        __slots__ = ()
        def __init__(self) -> None: ...
    class FetchServiceMapTask(_message.Message):
        __slots__ = ("start_time", "end_time", "duration")
        START_TIME_FIELD_NUMBER: _ClassVar[int]
        END_TIME_FIELD_NUMBER: _ClassVar[int]
        DURATION_FIELD_NUMBER: _ClassVar[int]
        start_time: _wrappers_pb2.StringValue
        end_time: _wrappers_pb2.StringValue
        duration: _wrappers_pb2.StringValue
        def __init__(self, start_time: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., end_time: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., duration: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...
    class FetchAlertsSummaryTask(_message.Message):
        __slots__ = ("start_time", "end_time", "duration", "rule_id", "state", "labels")
        START_TIME_FIELD_NUMBER: _ClassVar[int]
        END_TIME_FIELD_NUMBER: _ClassVar[int]
        DURATION_FIELD_NUMBER: _ClassVar[int]
        RULE_ID_FIELD_NUMBER: _ClassVar[int]
        STATE_FIELD_NUMBER: _ClassVar[int]
        LABELS_FIELD_NUMBER: _ClassVar[int]
        start_time: _wrappers_pb2.StringValue
        end_time: _wrappers_pb2.StringValue
        duration: _wrappers_pb2.StringValue
        rule_id: _wrappers_pb2.StringValue
        state: _wrappers_pb2.StringValue
        labels: _wrappers_pb2.StringValue
        def __init__(self, start_time: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., end_time: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., duration: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., rule_id: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., state: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., labels: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...) -> None: ...
    TYPE_FIELD_NUMBER: _ClassVar[int]
    CLICKHOUSE_QUERY_FIELD_NUMBER: _ClassVar[int]
    BUILDER_QUERY_FIELD_NUMBER: _ClassVar[int]
    DASHBOARD_DATA_FIELD_NUMBER: _ClassVar[int]
    FETCH_DASHBOARDS_FIELD_NUMBER: _ClassVar[int]
    FETCH_DASHBOARD_DETAILS_FIELD_NUMBER: _ClassVar[int]
    FETCH_SERVICES_FIELD_NUMBER: _ClassVar[int]
    FETCH_APM_METRICS_FIELD_NUMBER: _ClassVar[int]
    FETCH_LOGS_FIELD_NUMBER: _ClassVar[int]
    FETCH_LOGS_FOR_TRACE_FIELD_NUMBER: _ClassVar[int]
    FETCH_TRACES_FIELD_NUMBER: _ClassVar[int]
    FETCH_ALERT_RULES_FIELD_NUMBER: _ClassVar[int]
    FETCH_SERVICE_MAP_FIELD_NUMBER: _ClassVar[int]
    FETCH_ALERTS_SUMMARY_FIELD_NUMBER: _ClassVar[int]
    type: Signoz.TaskType
    clickhouse_query: Signoz.ClickhouseQueryTask
    builder_query: Signoz.BuilderQueryTask
    dashboard_data: Signoz.DashboardDataTask
    fetch_dashboards: Signoz.FetchDashboardsTask
    fetch_dashboard_details: Signoz.FetchDashboardDetailsTask
    fetch_services: Signoz.FetchServicesTask
    fetch_apm_metrics: Signoz.FetchApmMetricsTask
    fetch_logs: Signoz.FetchLogsTask
    fetch_logs_for_trace: Signoz.FetchLogsForTraceTask
    fetch_traces: Signoz.FetchTracesTask
    fetch_alert_rules: Signoz.FetchAlertRulesTask
    fetch_service_map: Signoz.FetchServiceMapTask
    fetch_alerts_summary: Signoz.FetchAlertsSummaryTask
    def __init__(self, type: _Optional[_Union[Signoz.TaskType, str]] = ..., clickhouse_query: _Optional[_Union[Signoz.ClickhouseQueryTask, _Mapping]] = ..., builder_query: _Optional[_Union[Signoz.BuilderQueryTask, _Mapping]] = ..., dashboard_data: _Optional[_Union[Signoz.DashboardDataTask, _Mapping]] = ..., fetch_dashboards: _Optional[_Union[Signoz.FetchDashboardsTask, _Mapping]] = ..., fetch_dashboard_details: _Optional[_Union[Signoz.FetchDashboardDetailsTask, _Mapping]] = ..., fetch_services: _Optional[_Union[Signoz.FetchServicesTask, _Mapping]] = ..., fetch_apm_metrics: _Optional[_Union[Signoz.FetchApmMetricsTask, _Mapping]] = ..., fetch_logs: _Optional[_Union[Signoz.FetchLogsTask, _Mapping]] = ..., fetch_logs_for_trace: _Optional[_Union[Signoz.FetchLogsForTraceTask, _Mapping]] = ..., fetch_traces: _Optional[_Union[Signoz.FetchTracesTask, _Mapping]] = ..., fetch_alert_rules: _Optional[_Union[Signoz.FetchAlertRulesTask, _Mapping]] = ..., fetch_service_map: _Optional[_Union[Signoz.FetchServiceMapTask, _Mapping]] = ..., fetch_alerts_summary: _Optional[_Union[Signoz.FetchAlertsSummaryTask, _Mapping]] = ...) -> None: ...
