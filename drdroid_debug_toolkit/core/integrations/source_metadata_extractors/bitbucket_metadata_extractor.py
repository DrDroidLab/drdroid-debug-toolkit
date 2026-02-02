import logging

from core.integrations.source_metadata_extractor import SourceMetadataExtractor
from core.integrations.source_api_processors.bitbucket_api_processor import BitbucketAPIProcessor
from core.protos.base_pb2 import Source, SourceModelType
from core.utils.logging_utils import log_function_call

logger = logging.getLogger(__name__)


class BitbucketSourceMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id, connector_name, api_key, workspace, **kwargs):
        self.workspace = workspace
        self.bb_processor = BitbucketAPIProcessor(api_key, workspace)
        super().__init__(request_id, connector_name, Source.BITBUCKET)

    @log_function_call
    def extract_repos(self):
        model_data = {}
        model_type = SourceModelType.BITBUCKET_REPOSITORY

        try:
            repos = self.bb_processor.list_repositories()
            if not repos:
                return model_data
            for repo in repos:
                slug = repo.get('slug', '')
                model_data[slug] = repo
        except Exception as e:
            logger.error(f'Error extracting Bitbucket repositories: {e}')
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)

    @log_function_call
    def extract_members(self):
        model_data = {}
        model_type = SourceModelType.BITBUCKET_WORKSPACE_MEMBER

        try:
            members = self.bb_processor.list_workspace_members()
            if not members:
                return model_data
            for member in members:
                user = member.get('user', {})
                display_name = user.get('display_name', '')
                model_data[display_name] = member
        except Exception as e:
            logger.error(f'Error extracting Bitbucket workspace members: {e}')
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
