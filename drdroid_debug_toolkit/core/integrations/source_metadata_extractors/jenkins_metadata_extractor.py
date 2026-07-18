import logging
from copy import deepcopy

from core.integrations.source_api_processors.jenkins_api_processor import JenkinsAPIProcessor
from core.integrations.source_metadata_extractor import SourceMetadataExtractor
from core.protos.base_pb2 import Source, SourceModelType
from core.utils.logging_utils import log_function_call

logger = logging.getLogger(__name__)


class JenkinsSourceMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id: str, connector_name: str, url: str, username: str, api_token: str,
                 crumb: bool = False, ):
        self.jenkins_processor = JenkinsAPIProcessor(url, username, api_token, crumb=crumb)
        # Source.JENKINS, not ARGOCD. The active base class persists rows with
        # connector_type=self.source and treats each (account, connector, source, model_type)
        # batch as the authoritative set, so a wrong source silently files Jenkins jobs under
        # ArgoCD. In prod this put all 537 of account 40720's jobs on connector_type=65.
        # NOTE: after deploying this, the pre-existing connector_type=65 / model_type=2901 rows
        # must be deactivated by hand -- the authoritative-set logic is scoped by connector_type,
        # so it will never see, and therefore never retire, the mis-filed rows.
        super().__init__(request_id, connector_name, Source.JENKINS)

    @log_function_call
    def extract_jobs(self):
        model_data = {}
        model_type = SourceModelType.JENKINS_JOBS
        try:
            jobs_response = self.jenkins_processor.test_connection()
            if jobs_response is not True:
                logger.error("Jenkins connection test failed.")
                return model_data

            logger.info("Starting Jenkins job extraction...")
            # Get all jobs recursively (including those in folders)
            all_jobs = self.jenkins_processor.get_all_jobs_recursive()
            
            if not all_jobs:
                logger.error("No jobs found or error occurred while fetching jobs.")
                return model_data

            print(f"Found {len(all_jobs)} total Jenkins jobs/folders", flush=True)
            
            # Process jobs in groups by path depth to ensure parent folders are processed first
            # Group jobs by their depth in the folder hierarchy
            jobs_by_depth = {}
            for job in all_jobs:
                job_full_path = job.get("full_path", "")
                depth = len(job_full_path.split('/')) - 1
                if depth not in jobs_by_depth:
                    jobs_by_depth[depth] = []
                jobs_by_depth[depth].append(job)
            
            # Process jobs in order from shallowest to deepest
            for depth in sorted(jobs_by_depth.keys()):
                jobs_at_depth = jobs_by_depth[depth]
                print(f"Processing {len(jobs_at_depth)} items at depth {depth}", flush=True)
                
                for job in jobs_at_depth:
                    try:
                        job_name = job.get("name", "")
                        job_class = job.get("class", "")
                        job_full_path = job.get("full_path", "")
                        
                        is_folder = "folder" in job_class.lower() or "directory" in job_class.lower()

                        if job_full_path:
                            # Folders are containers, not runnable jobs -- persisting them meant
                            # they were indexed as jobs downstream and offered to the agent as
                            # valid build targets. Their children are collected separately by
                            # get_all_jobs_recursive, so skipping the folder loses nothing.
                            # (This branch previously built a local `metadata` dict that was
                            # never used, so folders were saved regardless of the check.)
                            if is_folder:
                                print(f"Skipping folder (not saving to DB): {job_full_path}", flush=True)
                                continue

                            parameters = self.jenkins_processor.get_job_parameters(job_full_path)

                            # Use full_path as the unique identifier, but keep the simple name
                            metadata = {
                                "name": job_name,
                                "full_path": job_full_path,
                                "class": job_class,
                                "parameters": parameters,
                                # Lets the agent tell a multibranch job (which requires a branch)
                                # from a regular one without having to trigger the error first.
                                "is_multibranch": "multibranch" in job_class.lower(),
                            }
                            if '/' in job_full_path:
                                metadata['parent_folder'] = '/'.join(job_full_path.split('/')[:-1])

                            print(f"Saving to DB: {job_full_path}", flush=True)
                            model_data[job_full_path] = metadata
                    except KeyError as e:
                        logger.error(f"Missing key {e} in job: {job}")
        except Exception as e:
            logger.error(f'Error extracting Jenkins jobs: {e}')
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data
