from core.integrations.source_metadata_extractor import SourceMetadataExtractor
from core.integrations.source_api_processors.open_search_api_processor import OpenSearchApiProcessor, logger
from core.protos.base_pb2 import Source, SourceModelType
from core.utils.logging_utils import log_function_call


class OpenSearchSourceMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id: str, connector_name: str, protocol: str, host: str, username: str, password: str,
                 verify_certs: bool = False, port: str = None):
        self.__os_api_processor = OpenSearchApiProcessor(protocol, host, username, password, verify_certs, port)
        super().__init__(request_id, connector_name, Source.PAGER_DUTY)

    @log_function_call
    def extract_index(self, save_to_db=False):
        model_type = SourceModelType.OPEN_SEARCH_INDEX
        try:
            indexes = self.__os_api_processor.fetch_indices()
        except Exception as e:
            logger.error(f"Error while fetching OpenSearch indices: {e}")
            return
        if not indexes:
            return
        model_data = {}
        for ind in indexes:
            model_data[ind] = {}
            if save_to_db:
                self.create_or_update_model_metadata(model_type, ind, {})
        return model_data
