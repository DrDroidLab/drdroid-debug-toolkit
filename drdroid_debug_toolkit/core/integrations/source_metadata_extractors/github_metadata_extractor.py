import logging
import base64
import binascii

from core.integrations.source_metadata_extractor import SourceMetadataExtractor
from core.integrations.source_api_processors.github_api_processor import GithubAPIProcessor
from core.protos.base_pb2 import Source, SourceModelType
from core.utils.logging_utils import log_function_call

logger = logging.getLogger(__name__)


class GithubSourceMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id, connector_name, api_key, org):
        self.org = org
        self.gh_processor = GithubAPIProcessor(api_key, org)
        super().__init__(request_id, connector_name, Source.GITHUB)

    def _fetch_readme_content(self, repo_name):
        """
        Fetch README file content from repository root.
        Tries common README file names in order of preference.
        Returns the decoded content as a string, or None if not found.
        """
        readme_file_names = ['README.md', 'README', 'README.txt', 'README.rst', 'readme.md', 'readme']
        
        for readme_file in readme_file_names:
            try:
                file_data = self.gh_processor.fetch_file(repo_name, readme_file)
                if file_data and file_data.get('content'):
                    # Decode base64 content
                    content_encoded = file_data['content']
                    # GitHub API returns content with newlines, so we need to remove them
                    content_encoded = content_encoded.replace('\n', '')
                    try:
                        content_decoded = base64.b64decode(content_encoded).decode('utf-8')
                        return content_decoded
                    except (binascii.Error, UnicodeDecodeError) as e:
                        logger.warning(f'Error decoding README content for {repo_name}/{readme_file}: {e}')
                        continue
            except Exception as e:
                # File not found or other error, try next README file name
                logger.debug(f'Could not fetch {readme_file} for repo {repo_name}: {e}')
                continue
        
        return None

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
                readme_content = self._fetch_readme_content(repo_name)
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
