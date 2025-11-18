import logging
from typing import Optional

# Lightweight prod-env detection mirroring core.utils.playbooks_client
IS_PROD_ENV = False
try:
    from django.conf import settings  # type: ignore
    IS_PROD_ENV = bool(getattr(settings, "IS_PROD_ENV", False))
except Exception:
    pass

from core.integrations.source_api_processors.argocd_api_processor import ArgoCDAPIProcessor
from core.integrations.source_metadata_extractor import SourceMetadataExtractor
from core.protos.base_pb2 import Source, SourceModelType
from core.utils.logging_utils import log_function_call

logger = logging.getLogger(__name__)


class ArgoCDSourceMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id: str, connector_name: str, argocd_server, argocd_token):
        self.argocd_processor = ArgoCDAPIProcessor(argocd_server, argocd_token)
        super().__init__(request_id, connector_name, Source.ARGOCD)

    @log_function_call
    def extract_applications(self):
        model_data = {}
        model_type = SourceModelType.ARGOCD_APPS
        try:
            applications = self.argocd_processor.get_deployment_info()
            if not applications:
                return model_data

            for application in applications.get('items', []):  # Iterate over application items
                try:
                    app_name = application.get('metadata', {}).get('name', '')  # Access the name safely
                    path = application.get('spec', {}).get('source', {}).get('path', '')
                    if app_name:
                        model_data[app_name] = {"name": app_name, "path": path}
                except KeyError as e:
                    logger.error(f"Missing key {e} in application: {application}")
        except Exception as e:
            logger.error(f'Error extracting ArgoCD applications: {e}')
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)

