import logging

from core.integrations.source_api_processors.databricks_api_processor import DatabricksApiProcessor
from core.integrations.source_metadata_extractor import SourceMetadataExtractor
from core.protos.base_pb2 import Source, SourceModelType
from core.utils.logging_utils import log_function_call

logger = logging.getLogger(__name__)


class DatabricksMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, *args, databricks_host=None, databricks_token=None, **kwargs):
        self.__processor = DatabricksApiProcessor(databricks_host, databricks_token)
        super().__init__(*args, source=Source.DATABRICKS, **kwargs)

    @log_function_call
    def extract_jobs(self):
        model_type = SourceModelType.DATABRICKS_JOB
        model_data = {}
        try:
            jobs = self.__processor.list_jobs()
            for job in jobs:
                job_id = str(job.get('job_id', ''))
                if job_id:
                    model_data[job_id] = job
            if model_data:
                self.create_or_update_model_metadata(model_type, model_data)
        except Exception as e:
            logger.error(f'Error extracting Databricks jobs: {e}')
        return model_data

    @log_function_call
    def extract_clusters(self):
        model_type = SourceModelType.DATABRICKS_CLUSTER
        model_data = {}
        try:
            clusters = self.__processor.list_clusters()
            for cluster in clusters:
                cluster_id = str(cluster.get('cluster_id', ''))
                if cluster_id:
                    model_data[cluster_id] = cluster
            if model_data:
                self.create_or_update_model_metadata(model_type, model_data)
        except Exception as e:
            logger.error(f'Error extracting Databricks clusters: {e}')
        return model_data

    @log_function_call
    def extract_sql_warehouses(self):
        model_type = SourceModelType.DATABRICKS_SQL_WAREHOUSE
        model_data = {}
        try:
            warehouses = self.__processor.list_sql_warehouses()
            for warehouse in warehouses:
                warehouse_id = str(warehouse.get('id', ''))
                if warehouse_id:
                    model_data[warehouse_id] = warehouse
            if model_data:
                self.create_or_update_model_metadata(model_type, model_data)
        except Exception as e:
            logger.error(f'Error extracting Databricks SQL warehouses: {e}')
        return model_data
