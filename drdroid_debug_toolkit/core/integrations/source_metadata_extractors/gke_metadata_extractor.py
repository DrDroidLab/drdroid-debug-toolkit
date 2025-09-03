import logging
from core.integrations.source_metadata_extractor import SourceMetadataExtractor
from core.integrations.source_api_processors.gke_api_processor import GkeApiProcessor
from core.protos.base_pb2 import Source, SourceModelType
from core.utils.logging_utils import log_function_call

logger = logging.getLogger(__name__)

class GkeSourceMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id: str, connector_name: str, project_id, service_account_json):
        self.__project_id = project_id
        self.__service_account_json = service_account_json
        self.gke_api_processor = GkeApiProcessor(self.__project_id, self.__service_account_json)
        super().__init__(request_id, connector_name, Source.GKE)

    @log_function_call
    def extract_clusters(self):
        try:
            model_data = {}
            model_type = SourceModelType.GKE_CLUSTER
            
            clusters = self.gke_api_processor.list_clusters()
            if not clusters:
                return model_data
            
            # Group clusters by zone
            zone_clusters = {}
            for c in clusters:
                zone = c.get('zone', 'us-central1-a')  # Default zone if not available
                if zone not in zone_clusters:
                    zone_clusters[zone] = []
                zone_clusters[zone].append(c['name'])
            
            # Create metadata for each zone
            for zone, cluster_names in zone_clusters.items():
                if not cluster_names:
                    continue
                
                metadata = {
                    'zone': zone,
                    'clusters': cluster_names
                }

                try:
                    self.create_or_update_model_metadata(model_type, zone, metadata)
                except Exception as e:
                    logger.error(f"Failed to save metadata for zone {zone}: {str(e)}")

            return model_data
            
        except Exception as e:
            logger.error(f"Exception in GKE extract_clusters method: {str(e)}")
            raise

