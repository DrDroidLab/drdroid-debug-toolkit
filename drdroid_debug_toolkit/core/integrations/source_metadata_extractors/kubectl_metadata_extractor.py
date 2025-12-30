import json
import logging
from typing import Dict, Any

from core.integrations.source_metadata_extractor import SourceMetadataExtractor
from core.integrations.source_api_processors.kubectl_api_processor import KubectlApiProcessor
from core.protos.base_pb2 import Source, SourceModelType

from core.utils.logging_utils import log_function_call
from core.utils.simplify_network_map import simplify_network_map, validate_network_map_data

logger = logging.getLogger(__name__)

def get_kubernetes_deployments(account_id, model_data=None) -> Dict[str, Any]:
    deployment_uids = None
    # Use the keys from the model_data
    deployment_uids = list(model_data.keys())

    if not deployment_uids:
        logger.info("No deployment UIDs found in provided model_data")
        return {"entries": []}

    # OpenAI API call disabled - return all deployments without filtering
    logger.info("OpenAI API call disabled - returning all deployments without filtering")
    result = {
        "entries": [{"service_name": uid} for uid in deployment_uids]
    }

    print(f"Returning {len(result.get('entries', []))} deployments without OpenAI processing")
    return result


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
        except Exception as e:
            logger.error(f"Error extracting Kubernetes namespaces: {e}")
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

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

        except Exception as e:
            logger.error(f"Error extracting Kubernetes services: {e}")
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

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

        except Exception as e:
            logger.error(f"Error extracting Kubernetes deployments: {e}")
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

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

        except Exception as e:
            logger.error(f"Error extracting Kubernetes ingresses: {e}")
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

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

        except Exception as e:
            logger.error(f"Error extracting Kubernetes network policies: {e}")
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

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

        except Exception as e:
            logger.error(f"Error extracting Kubernetes pod autoscalers: {e}")
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

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
        except Exception as e:
            logger.error(f"Error extracting Kubernetes replicasets: {e}")
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

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
        except Exception as e:
            logger.error(f"Error extracting Kubernetes statefulsets: {e}")
        
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    # Namespace-specific extraction methods for real-time processing
    @log_function_call
    def extract_deployments_for_namespace(self, namespace):
        model_type = SourceModelType.KUBERNETES_DEPLOYMENT
        model_data = {}

        try:
            command = f"get deployments -n {namespace} -o json"

            json_output = self.__kubectl_api_processor.execute_command(command)
            data = json.loads(json_output)

            for item in data.get('items', []):
                metadata = item.get('metadata', {})

                deployment_name = metadata.get('name')
                deployment_namespace = metadata.get('namespace', namespace)

                if not deployment_name:
                    continue

                namespaced_name = f"{deployment_namespace}/{deployment_name}"

                model_data[namespaced_name] = item

        except Exception as e:
            logger.error(f"Error extracting Kubernetes deployments for namespace {namespace}: {e}")
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_services_for_namespace(self, namespace):
        model_type = SourceModelType.KUBERNETES_SERVICE
        model_data = {}

        try:
            command = f"get services -n {namespace} -o json"

            json_output = self.__kubectl_api_processor.execute_command(command)
            data = json.loads(json_output)

            for item in data.get('items', []):
                metadata = item.get('metadata', {})

                service_name = metadata.get('name')
                service_namespace = metadata.get('namespace', namespace)

                if not service_name:
                    continue

                namespaced_name = f"{service_namespace}/{service_name}"

                model_data[namespaced_name] = item

        except Exception as e:
            logger.error(f"Error extracting Kubernetes services for namespace {namespace}: {e}")
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_ingresses_for_namespace(self, namespace):
        model_type = SourceModelType.KUBERNETES_INGRESS
        model_data = {}

        try:
            command = f"get ingresses -n {namespace} -o json"

            json_output = self.__kubectl_api_processor.execute_command(command)
            data = json.loads(json_output)

            for item in data.get('items', []):
                metadata = item.get('metadata', {})

                ingress_name = metadata.get('name')
                ingress_namespace = metadata.get('namespace', namespace)

                if not ingress_name:
                    continue

                namespaced_name = f"{ingress_namespace}/{ingress_name}"

                model_data[namespaced_name] = item

        except Exception as e:
            logger.error(f"Error extracting Kubernetes ingresses for namespace {namespace}: {e}")
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_network_policies_for_namespace(self, namespace):
        model_type = SourceModelType.KUBERNETES_NETWORK_POLICY
        model_data = {}

        try:
            command = f"get networkpolicies -n {namespace} -o json"

            json_output = self.__kubectl_api_processor.execute_command(command)
            data = json.loads(json_output)

            for item in data.get('items', []):
                metadata = item.get('metadata', {})

                policy_name = metadata.get('name')
                policy_namespace = metadata.get('namespace', namespace)

                if not policy_name:
                    continue

                namespaced_name = f"{policy_namespace}/{policy_name}"

                model_data[namespaced_name] = item

        except Exception as e:
            logger.error(f"Error extracting Kubernetes network policies for namespace {namespace}: {e}")
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_replicasets_for_namespace(self, namespace):
        model_type = SourceModelType.KUBERNETES_REPLICASET
        model_data = {}

        try:
            command = f"get replicasets -n {namespace} -o json"

            json_output = self.__kubectl_api_processor.execute_command(command)
            data = json.loads(json_output)

            for item in data.get('items', []):
                metadata = item.get('metadata', {})

                rs_name = metadata.get('name')
                rs_namespace = metadata.get('namespace', namespace)

                if not rs_name:
                    continue

                namespaced_name = f"{rs_namespace}/{rs_name}"

                model_data[namespaced_name] = item

        except Exception as e:
            logger.error(f"Error extracting Kubernetes replicasets for namespace {namespace}: {e}")

        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)

        return model_data

    @log_function_call
    def extract_statefulsets_for_namespace(self, namespace):
        model_type = SourceModelType.KUBERNETES_STATEFULSET
        model_data = {}

        try:
            command = f"get statefulsets -n {namespace} -o json"

            json_output = self.__kubectl_api_processor.execute_command(command)
            data = json.loads(json_output)

            for item in data.get('items', []):
                metadata = item.get('metadata', {})

                ss_name = metadata.get('name')
                ss_namespace = metadata.get('namespace', namespace)

                if not ss_name:
                    continue

                namespaced_name = f"{ss_namespace}/{ss_name}"

                model_data[namespaced_name] = item
        except Exception as e:
            logger.error(f"Error extracting Kubernetes statefulsets for namespace {namespace}: {e}")
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data