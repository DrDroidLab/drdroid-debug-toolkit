from core.integrations.source_api_processors.ops_genie_api_processor import OpsGenieApiProcessor
from core.integrations.source_metadata_extractor import SourceMetadataExtractor
from drdroid_debug_toolkit.core.protos.base_pb2 import Source as ConnectorType, SourceModelType as ConnectorMetadataModelTypeProto


class OpsGenieConnectorMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id: str, connector_name: str, ops_genie_api_key: str):
        self.__og_processor = OpsGenieApiProcessor(ops_genie_api_key)

        super().__init__(request_id, connector_name, ConnectorType.OPS_GENIE)

    def extract_escalation_policy(self):
        model_type = ConnectorMetadataModelTypeProto.OPS_GENIE_ESCALATION
        all_escalation_policies = self.__og_processor.fetch_escalation_policies()
        model_data = {}
        for policy in all_escalation_policies:
            policy_id = policy['id']
            model_data[policy_id] = policy
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    def extract_teams(self):
        model_type = ConnectorMetadataModelTypeProto.OPS_GENIE_TEAM
        all_teams = self.__og_processor.fetch_teams()
        model_data = {}
        for team in all_teams:
            team_id = team['id']
            model_data[team_id] = team
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data
