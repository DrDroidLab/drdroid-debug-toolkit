import logging

from core.integrations.source_metadata_extractor import SourceMetadataExtractor
from core.integrations.source_api_processors.github_app_api_processor import GithubAppAPIProcessor
from core.protos.base_pb2 import Source, SourceModelType
from core.utils.logging_utils import log_function_call

logger = logging.getLogger(__name__)


class GithubAppSourceMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id, connector_name, app_id, private_key, installation_id, org):
        self.org = org
        self.gh_processor = GithubAppAPIProcessor(app_id, private_key, installation_id, org)
        super().__init__(request_id, connector_name, Source.GITHUB)

    @log_function_call
    def extract_repos(self):
        model_data = {}
        model_type = SourceModelType.GITHUB_REPOSITORY

        try:
            repos = self.gh_processor.list_all_repos()
            if not repos:
                return model_data
            for repo in repos:
                repo_name = repo['name']
                # Fetch README content and add to metadata
                readme_content = self.gh_processor.fetch_readme_content(repo_name)
                if readme_content:
                    repo['readme'] = readme_content
                model_data[repo_name] = repo
        except Exception as e:
            logger.error(f'Error extracting Github repositories: {e}')
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)

    @log_function_call
    def extract_members(self):
        model_data = {}
        model_type = SourceModelType.GITHUB_MEMBER

        try:
            members = self.gh_processor.list_all_members()
            if not members:
                return model_data
            for member in members:
                model_data[member['login']] = member
        except Exception as e:
            logger.error(f'Error extracting Github members: {e}')
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
