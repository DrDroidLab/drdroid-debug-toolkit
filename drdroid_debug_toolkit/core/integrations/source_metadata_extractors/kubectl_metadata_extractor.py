import json
import logging

from core.integrations.source_metadata_extractor import SourceMetadataExtractor
from core.integrations.source_api_processors.kubectl_api_processor import KubectlApiProcessor
from core.protos.base_pb2 import Source, SourceModelType

from core.utils.logging_utils import log_function_call
from core.utils.simplify_network_map import simplify_network_map, validate_network_map_data

logger = logging.getLogger(__name__)


class KubernetesMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id: str, connector_name: str, **credentials):
        super().__init__(request_id, connector_name, Source.KUBERNETES)
        self.__kubectl_api_processor = KubectlApiProcessor(**credentials)

    @log_function_call
    def extract_namespaces(self):
        model_type = SourceModelType.KUBERNETES_NAMESPACE
        model_data = {}
        try:
            command = "get namespaces -o json"
            json_output = self.__kubectl_api_processor.execute_command(command)
            data = json.loads(json_output)
            for item in data.get('items', []):
                metadata = item.get('metadata', {})
                namespace_name = metadata.get('name')
                if not namespace_name:
                    continue
                model_data[namespace_name] = item
            logger.info(f"Extracted {len(model_data)} namespaces")
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
        except Exception as e:
            logger.error(f"Error extracting Kubernetes namespaces: {e}")

    @log_function_call
    def extract_services(self):
        model_type = SourceModelType.KUBERNETES_SERVICE
        model_data = {}
        try:
            command = "get services --all-namespaces -o json"
            json_output = self.__kubectl_api_processor.execute_command(command)
            data = json.loads(json_output)
            for item in data.get('items', []):
                metadata = item.get('metadata', {})
                service_name = metadata.get('name')
                service_namespace = metadata.get('namespace', 'default')
                if not service_name:
                    continue
                namespaced_name = f"{service_namespace}/{service_name}"
                model_data[namespaced_name] = item
            logger.info(f"Extracted {len(model_data)} services from all namespaces")
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
        except Exception as e:
            logger.error(f"Error extracting Kubernetes services: {e}")

    @log_function_call
    def extract_deployments(self):
        model_type = SourceModelType.KUBERNETES_DEPLOYMENT
        model_data = {}
        try:
            command = "get deployments --all-namespaces -o json"
            json_output = self.__kubectl_api_processor.execute_command(command)
            data = json.loads(json_output)
            for item in data.get('items', []):
                metadata = item.get('metadata', {})
                deployment_name = metadata.get('name')
                deployment_namespace = metadata.get('namespace', 'default')
                if not deployment_name:
                    continue
                namespaced_name = f"{deployment_namespace}/{deployment_name}"
                model_data[namespaced_name] = item
            logger.info(f"Extracted {len(model_data)} deployments from all namespaces")
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
        except Exception as e:
            logger.error(f"Error extracting Kubernetes deployments: {e}")

    @log_function_call
    def extract_ingresses(self):
        model_type = SourceModelType.KUBERNETES_INGRESS
        model_data = {}
        try:
            command = "get ingresses --all-namespaces -o json"
            json_output = self.__kubectl_api_processor.execute_command(command)
            data = json.loads(json_output)
            for item in data.get('items', []):
                metadata = item.get('metadata', {})
                ingress_name = metadata.get('name')
                ingress_namespace = metadata.get('namespace', 'default')
                if not ingress_name:
                    continue
                namespaced_name = f"{ingress_namespace}/{ingress_name}"
                model_data[namespaced_name] = item
            logger.info(f"Extracted {len(model_data)} ingresses from all namespaces")
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
        except Exception as e:
            logger.error(f"Error extracting Kubernetes ingresses: {e}")

    @log_function_call
    def extract_network_policies(self):
        model_type = SourceModelType.KUBERNETES_NETWORK_POLICY
        model_data = {}
        try:
            command = "get networkpolicies --all-namespaces -o json"
            json_output = self.__kubectl_api_processor.execute_command(command)
            data = json.loads(json_output)
            for item in data.get('items', []):
                metadata = item.get('metadata', {})
                policy_name = metadata.get('name')
                policy_namespace = metadata.get('namespace', 'default')
                if not policy_name:
                    continue
                namespaced_name = f"{policy_namespace}/{policy_name}"
                model_data[namespaced_name] = item
            logger.info(f"Extracted {len(model_data)} network policies from all namespaces")
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
        except Exception as e:
            logger.error(f"Error extracting Kubernetes network policies: {e}")

    @log_function_call
    def extract_pod_autoscalers(self):
        model_type = SourceModelType.KUBERNETES_HPA
        model_data = {}
        try:
            command = "get hpa --all-namespaces -o json"
            json_output = self.__kubectl_api_processor.execute_command(command)
            data = json.loads(json_output)
            for item in data.get('items', []):
                metadata = item.get('metadata', {})
                hpa_name = metadata.get('name')
                hpa_namespace = metadata.get('namespace', 'default')
                if not hpa_name:
                    continue
                namespaced_name = f"{hpa_namespace}/{hpa_name}"
                model_data[namespaced_name] = item
            logger.info(f"Extracted {len(model_data)} pod autoscalers from all namespaces")
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
        except Exception as e:
            logger.error(f"Error extracting Kubernetes pod autoscalers: {e}")

    @log_function_call
    def extract_replicasets(self):
        model_type = SourceModelType.KUBERNETES_REPLICASET
        model_data = {}
        try:
            command = "get replicasets --all-namespaces -o json"
            json_output = self.__kubectl_api_processor.execute_command(command)
            data = json.loads(json_output)
            for item in data.get('items', []):
                metadata = item.get('metadata', {})
                rs_name = metadata.get('name')
                rs_namespace = metadata.get('namespace', 'default')
                if not rs_name:
                    continue
                namespaced_name = f"{rs_namespace}/{rs_name}"
                model_data[namespaced_name] = item
            logger.info(f"Extracted {len(model_data)} replicasets from all namespaces")
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
        except Exception as e:
            logger.error(f"Error extracting Kubernetes replicasets: {e}")

    @log_function_call
    def extract_statefulsets(self):
        model_type = SourceModelType.KUBERNETES_STATEFULSET
        model_data = {}
        try:
            command = "get statefulsets --all-namespaces -o json"
            json_output = self.__kubectl_api_processor.execute_command(command)
            data = json.loads(json_output)
            for item in data.get('items', []):
                metadata = item.get('metadata', {})
                ss_name = metadata.get('name')
                ss_namespace = metadata.get('namespace', 'default')
                if not ss_name:
                    continue
                namespaced_name = f"{ss_namespace}/{ss_name}"
                model_data[namespaced_name] = item
            logger.info(f"Extracted {len(model_data)} statefulsets from all namespaces")
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
        except Exception as e:
            logger.error(f"Error extracting Kubernetes statefulsets: {e}")

    @log_function_call
    def extract_network_map(self):
        """
        Extract network map using otterize network-mapper.
        This feature only works in native Kubernetes mode.
        """
        model_type = SourceModelType.KUBERNETES_NETWORK_MAP
        model_data = {}
        
        # Check if we're in native connection mode
        if not self.__kubectl_api_processor.native_connection_mode:
            logger.info("Network map extraction is only available in native Kubernetes mode. Skipping.")
            return None
        
        try:
            # Execute otterize network-mapper export command using the API processor
            command_args = ["otterize", "network-mapper", "export", "--format", "json"]
            logger.info("Executing otterize network-mapper command to extract network map")
            
            # Use the API processor's method for non-kubectl commands
            raw_output = self.__kubectl_api_processor.execute_non_kubectl_command(command_args)
            
            if raw_output is None:
                logger.warning("No output received from otterize network-mapper command")
                return None
            
            if not raw_output.strip():
                logger.warning("Empty output received from otterize network-mapper command")
                return None
            
            # Parse JSON output
            try:
                raw_network_map = json.loads(raw_output)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse otterize network-mapper JSON output: {e}")
                logger.debug(f"Raw output was: {raw_output[:500]}...")  # Log first 500 chars for debugging
                return None
            
            # Simplify and clean up the network map data
            simplified_network_map = simplify_network_map(raw_network_map)
            
            # Validate the simplified data
            if not validate_network_map_data(simplified_network_map):
                logger.error("Network map data validation failed")
                return None
            
            # Store the network map with a single key
            model_data["cluster_network_map"] = simplified_network_map
            
            logger.info(f"Successfully extracted network map with {simplified_network_map.get('summary', {}).get('total_services', 0)} services "
                       f"and {simplified_network_map.get('summary', {}).get('total_connections', 0)} connections")
            
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
                return simplified_network_map
            
        except Exception as e:
            logger.error(f"Error extracting Kubernetes network map: {e}")
            return None
