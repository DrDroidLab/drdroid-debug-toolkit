import logging
from core.integrations.source_api_processors.bash_processor import BashProcessor
from core.integrations.source_metadata_extractor import SourceMetadataExtractor
from core.protos.base_pb2 import Source, SourceModelType
from core.utils.logging_utils import log_function_call

logger = logging.getLogger(__name__)


class BashSourceMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id: str, connector_name: str, remote_host=None, remote_password=None, remote_pem=None,
                 port=None, remote_user=None):
        self.bash_processor = BashProcessor(remote_host, remote_password, remote_pem, port, remote_user)
        self.ssh_server = remote_host if remote_host else None
        self.ssh_user = remote_user if remote_user else None
        super().__init__(request_id, connector_name, Source.BASH)

    @log_function_call
    def extract_ssh_server(self):
        if self.ssh_server:
            if '@' in self.ssh_server:
                self.ssh_server = self.ssh_server.split('@')[1]
            model_data = {self.ssh_server: {}}
            model_type = SourceModelType.SSH_SERVER
            self.create_or_update_model_metadata(model_type, model_data)

    @log_function_call
    def extract_ssh_user(self):
        model_type = SourceModelType.SSH_USER
        if self.ssh_user:
            model_data = {self.ssh_user: {}}
            self.create_or_update_model_metadata(model_type, model_data)
        elif self.ssh_server and '@' in self.ssh_server:
            self.ssh_user = self.ssh_server.split('@')[0]
            model_data = {self.ssh_user: {}}
            self.create_or_update_model_metadata(model_type, model_data)
