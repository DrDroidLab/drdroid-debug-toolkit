import logging

from core.integrations.source_metadata_extractor import SourceMetadataExtractor
from core.integrations.source_api_processors.azure_api_processor import AzureApiProcessor
from core.protos.base_pb2 import Source, SourceModelType
from core.utils.logging_utils import log_function_call

logger = logging.getLogger(__name__)


class AzureConnectorMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id: str, connector_name: str, subscription_id: str, tenant_id: str, client_id: str,
                 client_secret: str):
        self.__client = AzureApiProcessor(subscription_id, tenant_id, client_id, client_secret)
        super().__init__(request_id, connector_name, Source.AZURE)

    @log_function_call
    def extract_workspaces(self):
        model_type = SourceModelType.AZURE_WORKSPACE
        try:
            workspaces = self.__client.fetch_workspaces()
        except Exception as e:
            logger.error(f'Error fetching databases: {e}')
            return
        if not workspaces:
            return
        model_data = {}
        for w in workspaces:
            workspace_id = w.get('customer_id', '')
            model_data[workspace_id] = w
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)

    @log_function_call
    def extract_resources(self):
        model_type = SourceModelType.AZURE_RESOURCE
        try:
            resources = self.__client.fetch_resources()
        except Exception as e:
            logger.error(f'Error fetching azure resources: {e}')
            return
        if not resources:
            return

        model_data = {}
        for r in resources:
            # resource_id = r.get('id', '')
            name = r.get('name', '')
            model_data[name] = r
        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
