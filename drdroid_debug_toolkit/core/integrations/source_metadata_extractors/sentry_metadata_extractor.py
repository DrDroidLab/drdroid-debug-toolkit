from core.integrations.source_metadata_extractor import SourceMetadataExtractor
from core.integrations.source_api_processors.sentry_api_processor import SentryApiProcessor
from core.protos.base_pb2 import Source, SourceModelType
from core.utils.logging_utils import log_function_call
import logging

logger = logging.getLogger(__name__)


class SentrySourceMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id: str, connector_name: str, api_key, org_slug):
        self.sentry_processor = SentryApiProcessor(api_key, org_slug)
        super().__init__(request_id, connector_name, Source.SENTRY)

    @log_function_call
    def extract_projects(self):
        model_data = {}
        model_type = SourceModelType.SENTRY_PROJECT
        try:
            projects = self.sentry_processor.fetch_projects()
            if not projects:
                logger.warning("No projects found or error occurred while fetching projects.")
                return model_data

            print(f"Found {len(projects)} Sentry projects", flush=True)

            for project in projects:
                project_name = project.get('name')
                if not project_name:
                    logger.warning(f"Project missing name field, skipping: {project}")
                    continue

                model_data[project_name] = project
                print(f"Saving to DB: {project_name}", flush=True)
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
        except Exception as e:
            logger.error(f'Error extracting Sentry projects: {e}')

        return model_data