from core.integrations.source_metadata_extractor import SourceMetadataExtractor
from core.integrations.source_api_processors.open_search_api_processor import OpenSearchApiProcessor, logger
from core.protos.base_pb2 import Source, SourceModelType
from core.utils.logging_utils import log_function_call


class OpenSearchSourceMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id: str, connector_name: str, protocol: str, host: str, username: str, password: str,
                 verify_certs: bool = False, port: str = None):
        self.__os_api_processor = OpenSearchApiProcessor(protocol, host, username, password, verify_certs, port)
        super().__init__(request_id, connector_name, Source.OPEN_SEARCH)

    @log_function_call
    def extract_index(self, save_to_db=False):
        model_type = SourceModelType.OPEN_SEARCH_INDEX
        try:
            response = self.__os_api_processor.get_indices()
            indexes = response.get('api_response', []) if isinstance(response, dict) else []
        except Exception as e:
            logger.error(f"Error while fetching OpenSearch indices: {e}")
            return
        if not indexes:
            return
        model_data = {}
        for index_info in indexes:
            index_name = index_info.get('index', '') if isinstance(index_info, dict) else str(index_info)
            if not index_name:
                continue
            if isinstance(index_info, dict):
                index_metadata = {
                    'health': index_info.get('health', ''),
                    'status': index_info.get('status', ''),
                    'uuid': index_info.get('uuid', ''),
                    'pri': index_info.get('pri', ''),
                    'rep': index_info.get('rep', ''),
                    'docs_count': index_info.get('docs.count', ''),
                    'docs_deleted': index_info.get('docs.deleted', ''),
                    'store_size': index_info.get('store.size', ''),
                    'pri_store_size': index_info.get('pri.store.size', '')
                }
            else:
                index_metadata = {}
            model_data[index_name] = index_metadata
        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data
