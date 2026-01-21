import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

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

    @log_function_call
    def extract_repos(self):
        model_data = {}
        model_type = SourceModelType.GITHUB_REPOSITORY

        try:
            repos = self.gh_processor.list_all_repos()
            if not repos:
                return model_data
            
            total_repos = len(repos)
            logger.info(f'📦 Found {total_repos} repositories. Fetching README contents in parallel...')
            
            def fetch_repo_with_readme(repo):
                """Fetch README for a single repo."""
                repo_name = repo['name']
                try:
                    readme_content = self.gh_processor.fetch_readme_content(repo_name)
                    if readme_content:
                        repo['readme'] = readme_content
                except Exception as e:
                    logger.error(f'Error fetching README for repo {repo_name}: {e}')
                return repo
            
            # Process repos in parallel with ThreadPoolExecutor
            processed_count = 0
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(fetch_repo_with_readme, repo): repo for repo in repos}
                for future in as_completed(futures):
                    processed_count += 1
                    if processed_count % 20 == 0 or processed_count == total_repos:
                        logger.info(f'⏳ Processed {processed_count}/{total_repos} repos ({processed_count * 100 // total_repos}%)')
                    
                    repo = future.result()
                    model_data[repo['name']] = repo
                    
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
