from integrations.source_metadata_extractor import SourceMetadataExtractor
from core.integrations.source_api_processors.victoria_logs_api_processor import VictoriaLogsApiProcessor
from core.protos.base_pb2 import Source, SourceModelType
from core.utils.logging_utils import log_function_call


class VictoriaLogsSourceMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id: str, connector_name: str, protocol='http', host=None, port=9428, ssl_verify=True, headers_json=None,
                 account_id=None, connector_id=None, **kwargs):
        # Accept both explicit params and uppercase credential keys passed from generic factory
        protocol = kwargs.get('VICTORIA_LOGS_PROTOCOL', protocol)
        host = kwargs.get('VICTORIA_LOGS_HOST', host)
        port = kwargs.get('VICTORIA_LOGS_PORT', port)
        ssl_verify = kwargs.get('SSL_VERIFY', ssl_verify)
        headers_json = kwargs.get('MCP_SERVER_AUTH_HEADERS', headers_json)

        # Normalize types
        try:
            port = int(port) if port is not None else 9428
        except Exception:
            port = 9428
        if isinstance(ssl_verify, str):
            ssl_verify = ssl_verify.lower() in ['true', '1', 'yes']

        self._processor = VictoriaLogsApiProcessor(
            VICTORIA_LOGS_PROTOCOL=protocol,
            VICTORIA_LOGS_HOST=host,
            VICTORIA_LOGS_PORT=port,
            SSL_VERIFY=ssl_verify,
            MCP_SERVER_AUTH_HEADERS=headers_json,
        )
        super().__init__(request_id, connector_name, Source.VICTORIA_LOGS)

    @log_function_call
    def extract_log_fields(self, time_filter='_time:1d'):
        """
        Fetch field names from VictoriaLogs using LogsQL `field_names` pipe.
        Returns a dict with a list of field names under 'fields'.
        """
        result = self._processor.fetch_field_names(time_filter=time_filter)

        fields = []
        # Expect either {data: [ {field: name}, ... ]} or a flat list; normalize
        data = result.get('data', result.get('result', [])) if isinstance(result, dict) else []
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    # Look for common keys
                    name = item.get('field') or item.get('name') or item.get('value')
                    if name:
                        fields.append(str(name))
                else:
                    fields.append(str(item))

        payload = {'fields': sorted(set(fields))}

        if payload['fields']:
            # Persist discovered field names under VictoriaLogs field model type
            self.create_or_update_model_metadata(SourceModelType.VICTORIA_LOGS_FIELD, 'victoria_logs_field_names', payload)

        return payload