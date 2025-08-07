import logging

from core.integrations.source_metadata_extractor import SourceMetadataExtractor
from core.integrations.source_api_processors.elastic_search_api_processor import ElasticSearchApiProcessor
from core.protos.base_pb2 import Source, SourceModelType
from core.utils.logging_utils import log_function_call

logger = logging.getLogger(__name__)


class ElasticSearchSourceMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id: str, connector_name: str, protocol: str, host: str, port: str, api_key_id: str,
                 api_key: str, verify_certs=False, kibana_host: str=None):
        self.__es_api_processor = ElasticSearchApiProcessor(
            protocol=protocol,
            host=host,
            port=port,
            api_key_id=api_key_id,
            api_key=api_key,
            verify_certs=verify_certs,
            kibana_host=kibana_host
        )

        super().__init__(request_id, connector_name, Source.ELASTIC_SEARCH)

    @log_function_call
    def extract_index(self):
        model_type = SourceModelType.ELASTIC_SEARCH_INDEX
        try:
            indexes = self.__es_api_processor.fetch_indices()
        except Exception as e:
            logger.error(f'Error fetching databases: {e}')
            return
        if not indexes:
            return
        model_data = {}
        for ind in indexes:
            model_data[ind] = {}
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)

    @log_function_call
    def extract_services(self, save_to_db=False):
        """Extract all services from ElasticSearch APM indices"""
        model_type = SourceModelType.ELASTIC_SEARCH_SERVICES
        model_data = {}
        try:
            services = self.__es_api_processor.list_all_services()
            for service in services:
                service_name = service['name']
                model_data[service_name] = service
            if save_to_db and len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
        except Exception as e:
            logger.error(f'Error extracting ElasticSearch services: {e}')
        return model_data

    @log_function_call
    def extract_dashboards(self, save_to_db=False):
        """Extract all dashboards and their widget details from Kibana"""
        model_type = SourceModelType.ELASTIC_SEARCH_DASHBOARDS
        model_data = {}
        try:
            # Get list of all dashboards
            dashboards = self.__es_api_processor.list_all_dashboards()
            
            for dashboard in dashboards:
                dashboard_id = dashboard['id']
                dashboard_title = dashboard['title']
                
                # Get detailed dashboard information including widgets
                dashboard_details = self.__es_api_processor.get_dashboard_by_name(dashboard_title)
                
                if dashboard_details:
                    model_data[dashboard_id] = {
                        'id': dashboard_id,
                        'title': dashboard_title,
                        'description': dashboard['description'],
                        'widgets': dashboard_details.get('widgets', [])
                    }
            
            if save_to_db and len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
                        
        except Exception as e:
            logger.error(f'Error extracting ElasticSearch dashboards: {e}')
            
        return model_data

