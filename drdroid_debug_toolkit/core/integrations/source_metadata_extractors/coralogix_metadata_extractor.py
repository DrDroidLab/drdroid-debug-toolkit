import logging
from core.integrations.source_metadata_extractor import SourceMetadataExtractor
from core.integrations.source_api_processors.coralogix_api_processor import CoralogixApiProcessor
from core.protos.base_pb2 import Source, SourceModelType
from core.utils.logging_utils import log_function_call

logger = logging.getLogger(__name__)


def extract_widget_query(widget: dict, **kwargs) -> str:
    """
    Extract the query from a widget configuration.
    This method handles different widget types and extracts their queries.
    
    Args:
        widget: Widget configuration dictionary
        **kwargs: Additional keyword arguments (ignored to prevent parameter errors)
        
    Returns:
        str: Extracted query string or None if no query found
    """
    try:
        widget_type = widget.get("type", "").lower()
        
        # Handle DataPrime widgets (now using Lucene queries)
        if widget_type == "dataprime":
            # Extract query from DataPrime widget configuration (converted to Lucene)
            config = widget.get("config", {})
            query = config.get("query", "")
            if query:
                return query
        
        # Handle other widget types that might contain queries
        elif widget_type in ["logs", "metrics", "spans"]:
            # Extract query from widget configuration
            config = widget.get("config", {})
            query = config.get("query", "")
            if query:
                return query
        
        # Handle widgets with embedded query configurations
        elif "query" in widget:
            return widget["query"]
        
        # Handle widgets with query in config
        elif "config" in widget and "query" in widget["config"]:
            return widget["config"]["query"]
        
        return None
        
    except Exception as e:
        logger.error(f"Error extracting query from widget: {e}")
        return None


class CoralogixSourceMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id: str, connector_name: str, api_key, endpoint, ssl_verify="true", domain=None):
        self.__coralogix_api_processor = CoralogixApiProcessor(api_key, endpoint, ssl_verify, domain)
        super().__init__(request_id, connector_name, Source.CORALOGIX)

    @log_function_call
    def extract_dashboards(self):
        """
        Extract custom dashboards and their widgets from Coralogix in one operation.
        
        Args:
            save_to_db: Whether to save extracted data to database
            
        Returns:
            dict: Extracted dashboard and widget data
        """
        model_type = SourceModelType.CORALOGIX_DASHBOARD
        try:
            dashboards = self.__coralogix_api_processor.fetch_dashboards()
        except Exception as e:
            logger.error(f'Error fetching Coralogix dashboards: {e}')
            return
        
        if not dashboards:
            return
            
        model_data = {}
        
        # Handle different response formats
        if isinstance(dashboards, dict):
            if 'dashboards' in dashboards:
                dashboard_list = dashboards['dashboards']
            elif 'data' in dashboards:
                dashboard_list = dashboards['data']
            elif 'items' in dashboards:
                dashboard_list = dashboards['items']
            else:
                # If it's a dict but doesn't have expected keys, treat it as a single dashboard
                dashboard_list = [dashboards]
        elif isinstance(dashboards, list):
            dashboard_list = dashboards
        else:
            logger.warning(f"Unexpected dashboard response format: {type(dashboards)}")
            return
            
        for dashboard in dashboard_list:
            if not isinstance(dashboard, dict):
                continue
                
            dashboard_id = dashboard.get('id') or dashboard.get('dashboard_id')
            if not dashboard_id:
                continue
                
            try:
                # Fetch detailed dashboard information including widgets
                dashboard_details = self.__coralogix_api_processor.fetch_dashboard_details(dashboard_id)
                if not dashboard_details:
                    continue
                    
                dashboard_title = dashboard_details.get('title', f'Dashboard {dashboard_id}')
                widgets = dashboard_details.get('widgets', [])
                
                # Store dashboard metadata
                dashboard_metadata = {
                    'dashboard_id': dashboard_id,
                    'dashboard_title': dashboard_title,
                    'dashboard_json': dashboard_details,
                    'widgets': []
                }
                
                # Extract widget metadata from the same dashboard details
                for widget in widgets:
                    if not isinstance(widget, dict):
                        continue
                        
                    widget_id = widget.get('id')
                    if not widget_id:
                        continue
                        
                    # Extract widget metadata
                    widget_metadata = {
                        'widget_id': widget_id,
                        'dashboard_id': dashboard_id,
                        'dashboard_title': dashboard_title,
                        'widget_type': widget.get('type', 'unknown'),
                        'widget_title': widget.get('title', f'Widget {widget_id}'),
                        'widget_config': widget.get('config', {}),
                        'widget_query': extract_widget_query(widget),
                        'widget_json': widget
                    }
                    
                    # Add widget to dashboard metadata
                    dashboard_metadata['widgets'].append(widget_metadata)
                
                # Store the complete dashboard with widgets
                model_data[dashboard_id] = dashboard_metadata
                    
            except Exception as e:
                logger.error(f'Error extracting Coralogix dashboard {dashboard_id}: {e}')
                continue

        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)

        return model_data

    @log_function_call
    def extract_index_mappings(self, index_pattern="*"):
        """
        Extract index mappings from Coralogix.
        
        Args:
            index_pattern: Index pattern to fetch mappings for (default: "*" for all indices)
            
        Returns:
            dict: Extracted index mapping data
        """
        model_type = SourceModelType.CORALOGIX_INDEX_MAPPING
        try:
            index_mappings = self.__coralogix_api_processor.fetch_index_mappings(index_pattern)
        except Exception as e:
            logger.error(f'Error fetching Coralogix index mappings: {e}')
            return
        print("Received index mappings: ", index_mappings)
        if not index_mappings:
            return
            
        model_data = {}
        
        # Handle the index mappings response structure
        # The response typically contains index names as keys with their mapping data
        for index_name, mapping_data in index_mappings.items():
            if not isinstance(mapping_data, dict):
                continue
                
            try:
                # Extract mapping information
                index_metadata = {
                    'index_name': index_name,
                    'index_pattern': index_pattern,
                    'mapping_data': mapping_data,
                    'mapping_type': 'elasticsearch',
                    'description': f'Index mapping for {index_name}'
                }
                
                # Store the index mapping
                model_data[index_name] = index_metadata

            except Exception as e:
                logger.error(f'Error extracting Coralogix index mapping {index_name}: {e}')
                continue
        logger.info(f'Extracted {len(model_data)} index mappings')
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data
