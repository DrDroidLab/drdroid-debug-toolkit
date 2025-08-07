from core.integrations.source_metadata_extractor import SourceMetadataExtractor
from core.integrations.source_api_processors.gke_api_processor import GkeApiProcessor
from core.protos.base_pb2 import Source, SourceModelType
from core.utils.logging_utils import log_function_call


class GkeSourceMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id: str, connector_name: str, project_id, service_account_json):
        self.__project_id = project_id
        self.__service_account_json = service_account_json
        super().__init__(request_id, connector_name, Source.GKE)

    @log_function_call
    def extract_clusters(self):
        model_data = {}
        gke_api_processor = GkeApiProcessor(self.__project_id, self.__service_account_json)
        model_type = SourceModelType.GKE_CLUSTER
        clusters = gke_api_processor.list_clusters()
        if not clusters:
            return model_data
        for c in clusters:
            clusters = model_data.get(c['zone'], [])
            clusters.append(c['name'])
            model_data[c['zone']] = clusters
        for zone in model_data:
            clusters = model_data[zone]
            if not clusters:
                continue
            model_data[zone] = {'clusters': clusters}
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
