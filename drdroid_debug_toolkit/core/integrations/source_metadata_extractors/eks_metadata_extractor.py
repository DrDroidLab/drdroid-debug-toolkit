import logging

from core.integrations.source_metadata_extractor import SourceMetadataExtractor
from core.integrations.source_api_processors.eks_api_processor import EKSApiProcessor
from core.protos.base_pb2 import Source, SourceModelType
from core.utils.logging_utils import log_function_call

logger = logging.getLogger(__name__)


class EksSourceMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id: str, connector_name: str, region: str, k8_role_arn: str, aws_access_key: str = None,
                 aws_secret_key: str = None, aws_assumed_role_arn: str = None):
        self.__region = region
        self.__aws_access_key = aws_access_key
        self.__aws_secret_key = aws_secret_key
        self.__k8_role_arn = k8_role_arn
        self.__aws_assumed_role_arn = aws_assumed_role_arn
        super().__init__(request_id, connector_name, Source.EKS)

    @log_function_call
    def extract_clusters(self):
        model_data = {}
        aws_boto3_processor = EKSApiProcessor(self.__region, self.__aws_access_key, self.__aws_secret_key,
                                              self.__aws_assumed_role_arn)
        model_type = SourceModelType.EKS_CLUSTER
        clusters = aws_boto3_processor.eks_list_clusters()
        if not clusters:
            return model_data
        model_data[self.__region] = {'clusters': clusters}
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
