import logging
import re

from core.integrations.source_metadata_extractor import SourceMetadataExtractor
from core.integrations.source_api_processors.elastic_search_api_processor import ElasticSearchApiProcessor
from core.protos.base_pb2 import Source, SourceModelType
from core.utils.logging_utils import log_function_call
from core.integrations.source_managers.elastic_search_source_manager import ACCOUNT_INDEX_MAPPING
logger = logging.getLogger(__name__)


DEFAULT_INDEX_PATTERN = "*apm*"

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
            indexes = self.__es_api_processor.get_cat_indices()
        except Exception as e:
            logger.error(f'Error fetching elastic search indexes: {e}')
            return
        if not indexes:
            return
        model_data = {}
        for index_info in indexes:
            index_name = index_info.get('index', '')
            if index_name:
                # Extract comprehensive index metadata
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
                model_data[index_name] = index_metadata
                self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    def _extract_service_dependencies(self, service_name: str, index_pattern: str = None) -> dict:
        """
        Extract downstream services and resources for a given service
        
        Args:
            service_name: Name of the service to analyze
            index_pattern: Elasticsearch index pattern to search in
            
        Returns:
            Dictionary containing downstream services and resources
        """
        if not index_pattern:
            index_pattern = DEFAULT_INDEX_PATTERN
            
        try:
            # Get unique URL paths for this service
            paths = self.__es_api_processor.get_unique_url_paths_for_service(service_name, index_pattern)
            
            # Normalize paths by replacing numbers with <param>
            normalized_paths = set()
            for path in paths:
                normalized_path = re.sub(r'\d+', '<param>', path)
                normalized_paths.add(normalized_path)
            
            # Get downstream services by analyzing traces
            downstream_services = set()
            paths_checked = set()
            
            for path in paths:
                normalized_path = re.sub(r'\d+', '<param>', path)
                if normalized_path in paths_checked:
                    continue
                    
                # Skip certain paths that might not be relevant
                if any(skip_path in normalized_path for skip_path in [
                    '/api/sales-panel/sales/transaction/callback/update/',
                    '/api/sales-panel/sales/transaction/update/',
                    '/api/referral/public/get-payout-status/'
                ]):
                    continue
                
                # Get sample traces for this path
                trace_ids = self.__es_api_processor.get_trace_ids_for_service_and_path(
                    service_name, path, limit=3, index_pattern=index_pattern
                )
                
                # For each trace, get unique destinations
                for trace_id in trace_ids:
                    downstream_calls = self.__es_api_processor.get_downstream_calls_for_trace(
                        trace_id, index_pattern
                    )
                    
                    for call in downstream_calls:
                        call_id = call.get('resource') or call.get('name')
                        if call_id:
                            downstream_services.add(call_id)
                            
                paths_checked.add(normalized_path)
            
            return {
                'downstream': sorted(list(downstream_services)),
                'resources': sorted(list(normalized_paths))
            }

        except Exception as e:
            logger.error(f'Error extracting dependencies for service {service_name}: {e}')
            return {'downstream': [], 'resources': []}

    @log_function_call
    def extract_services(self):
        """Extract all services from ElasticSearch APM indices with downstream dependencies and resources"""
        model_type = SourceModelType.ELASTIC_SEARCH_SERVICES
        model_data = {}
        try:
            services = self.__es_api_processor.list_all_services(index_pattern=DEFAULT_INDEX_PATTERN)
            for service in services:
                service_name = service['name']
                
                # Get base service metadata
                service_metadata = {
                    'name': service_name,
                    'count': service.get('count', 0)
                }
                
                # Extract downstream dependencies and resources
                dependencies = self._extract_service_dependencies(service_name)
                service_metadata.update(dependencies)
                
                model_data[service_name] = service_metadata
                
                self.create_or_update_model_metadata(model_type, model_data)
        except Exception as e:
            logger.error(f'Error extracting ElasticSearch services: {e}')
        return model_data

    @log_function_call
    def extract_dashboards(self):
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
            
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
                        
        except Exception as e:
            logger.error(f'Error extracting ElasticSearch dashboards: {e}')
            
        return model_data

