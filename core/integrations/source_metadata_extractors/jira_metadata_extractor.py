import logging

from core.integrations.source_api_processors.jira_api_processor import JiraApiProcessor
from core.integrations.source_metadata_extractor import SourceMetadataExtractor
from core.protos.base_pb2 import Source, SourceModelType
from core.utils.logging_utils import log_function_call

logger = logging.getLogger(__name__)


class JiraSourceMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id: str, connector_name: str, jira_cloud_api_key: str, jira_domain: str,
                 jira_email: str):
        self.jira_processor = JiraApiProcessor(jira_cloud_api_key, jira_domain, jira_email)
        super().__init__(request_id, connector_name, Source.JIRA_CLOUD)

    @log_function_call
    def extract_projects(self):
        model_data = {}
        model_type = SourceModelType.JIRA_PROJECT
        try:
            projects = self.jira_processor.list_all_projects()
            if not projects:
                return model_data
            for project in projects:
                model_data[project['key']] = project
        except Exception as e:
            logger.error(f'Error extracting Jira projects: {e}')
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)

    @log_function_call
    def extract_users(self):
        model_data = {}
        model_type = SourceModelType.JIRA_USER
        try:
            users = self.jira_processor.list_all_users()
            if not users:
                return model_data
            for user in users:
                if 'accountType' in user and user['accountType'] == 'atlassian':
                    model_data[user['accountId']] = user
        except Exception as e:
            logger.error(f'Error extracting Jira users: {e}')
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
