import logging
from core.integrations.source_metadata_extractor import SourceMetadataExtractor
from core.integrations.source_api_processors.gke_api_processor import GkeApiProcessor
from core.integrations.source_api_processors.gcp_api_processor import GcpApiProcessor
from core.protos.base_pb2 import Source, SourceModelType
from core.utils.logging_utils import log_function_call

logger = logging.getLogger(__name__)

class GkeSourceMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id: str, connector_name: str, project_id, service_account_json):
        self.__project_id = project_id
        self.__service_account_json = service_account_json
        self.gke_api_processor = GkeApiProcessor(self.__project_id, self.__service_account_json)
        self.gcp_api_processor = GcpApiProcessor(self.__project_id, self.__service_account_json)
        super().__init__(request_id, connector_name, Source.GKE)

    @staticmethod
    def _sanitize_metadata(value):
        try:
            from datetime import datetime, date
            if isinstance(value, (datetime, date)):
                return value.isoformat()
            if isinstance(value, dict):
                return {k: GkeSourceMetadataExtractor._sanitize_metadata(v) for k, v in value.items()}
            if isinstance(value, list):
                return [GkeSourceMetadataExtractor._sanitize_metadata(v) for v in value]
            if isinstance(value, tuple):
                return tuple(GkeSourceMetadataExtractor._sanitize_metadata(v) for v in value)
            return value
        except Exception:
            return value

    @log_function_call
    def extract_clusters(self):
        try:
            model_data = {}
            model_type = SourceModelType.GKE_CLUSTER
            
            clusters = self.gke_api_processor.list_clusters()
            if not clusters:
                return model_data
            
            # Group clusters by zone
            zone_clusters = {}
            for c in clusters:
                zone = c.get('zone', 'us-central1-a')  # Default zone if not available
                if zone not in zone_clusters:
                    zone_clusters[zone] = []
                zone_clusters[zone].append(c['name'])
            
            # Create metadata for each zone
            for zone, cluster_names in zone_clusters.items():
                if not cluster_names:
                    continue
                
                metadata = {
                    'zone': zone,
                    'clusters': cluster_names
                }
                
                sanitized = self._sanitize_metadata(metadata)
                model_data[zone] = sanitized
            
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
            return model_data
            
        except Exception as e:
            logger.error(f"Exception in GKE extract_clusters method: {str(e)}")
            raise

    @log_function_call
    def extract_namespaces(self):
        try:
            model_data = {}
            model_type = SourceModelType.GKE_NAMESPACE
            
            clusters = self.gke_api_processor.list_clusters()
            if not clusters:
                return model_data
            
            for cluster in clusters:
                zone = cluster.get('zone', 'us-central1-a')
                cluster_name = cluster.get('name')
                
                if not cluster_name:
                    continue
                
                try:
                    namespaces_response = self.gke_api_processor.list_namespaces(zone, cluster_name)
                    namespaces_dict = namespaces_response.to_dict()
                    
                    for item in namespaces_dict.get('items', []):
                        metadata = item.get('metadata', {})
                        namespace_name = metadata.get('name')
                        
                        if not namespace_name:
                            continue
                        
                        # Use zone/cluster/namespace as the unique identifier
                        namespaced_name = f"{zone}/{cluster_name}/{namespace_name}"
                        
                        # Add cluster and zone context to the item
                        item['gke_context'] = {
                            'zone': zone,
                            'cluster': cluster_name
                        }
                        
                        model_data[namespaced_name] = item
                        
                        sanitized_item = self._sanitize_metadata(item)
                        model_data[namespaced_name] = sanitized_item
                                
                except Exception as e:
                    logger.error(f"Error extracting namespaces for cluster {cluster_name} in zone {zone}: {str(e)}")
                    continue
            
            logger.info(f"Extracted {len(model_data)} namespaces from GKE clusters")
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
            return model_data
            
        except Exception as e:
            logger.error(f"Exception in GKE extract_namespaces method: {str(e)}")
            raise

    @log_function_call
    def extract_services(self, save_to_db=True):
        try:
            model_data = {}
            model_type = SourceModelType.GKE_SERVICE
            
            clusters = self.gke_api_processor.list_clusters()
            if not clusters:
                return model_data
            
            for cluster in clusters:
                zone = cluster.get('zone', 'us-central1-a')
                cluster_name = cluster.get('name')
                
                if not cluster_name:
                    continue
                
                try:
                    # Get namespaces first
                    namespaces_response = self.gke_api_processor.list_namespaces(zone, cluster_name)
                    namespaces_dict = namespaces_response.to_dict()
                    
                    for ns_item in namespaces_dict.get('items', []):
                        namespace_name = ns_item.get('metadata', {}).get('name')
                        if not namespace_name:
                            continue
                        
                        try:
                            services_response = self.gke_api_processor.list_services(zone, cluster_name, namespace_name)
                            services_dict = services_response.to_dict()
                            
                            for item in services_dict.get('items', []):
                                metadata = item.get('metadata', {})
                                service_name = metadata.get('name')
                                
                                if not service_name:
                                    continue
                                
                                # Use zone/cluster/namespace/service as the unique identifier
                                namespaced_name = f"{zone}/{cluster_name}/{namespace_name}/{service_name}"
                                
                                # Add cluster and zone context to the item
                                item['gke_context'] = {
                                    'zone': zone,
                                    'cluster': cluster_name
                                }
                                
                                model_data[namespaced_name] = item
                                
                                sanitized_item = self._sanitize_metadata(item)
                                model_data[namespaced_name] = sanitized_item
                                        
                        except Exception as e:
                            logger.error(f"Error extracting services for namespace {namespace_name} in cluster {cluster_name}: {str(e)}")
                            continue
                            
                except Exception as e:
                    logger.error(f"Error extracting services for cluster {cluster_name} in zone {zone}: {str(e)}")
                    continue
            
            logger.info(f"Extracted {len(model_data)} services from GKE clusters")
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
            return model_data
            
        except Exception as e:
            logger.error(f"Exception in GKE extract_services method: {str(e)}")
            raise

    @log_function_call
    def extract_deployments(self):
        try:
            model_data = {}
            model_type = SourceModelType.GKE_DEPLOYMENT
            
            clusters = self.gke_api_processor.list_clusters()
            if not clusters:
                return model_data
            
            for cluster in clusters:
                zone = cluster.get('zone', 'us-central1-a')
                cluster_name = cluster.get('name')
                
                if not cluster_name:
                    continue
                
                try:
                    # Get namespaces first
                    namespaces_response = self.gke_api_processor.list_namespaces(zone, cluster_name)
                    namespaces_dict = namespaces_response.to_dict()
                    
                    for ns_item in namespaces_dict.get('items', []):
                        namespace_name = ns_item.get('metadata', {}).get('name')
                        if not namespace_name:
                            continue
                        
                        try:
                            deployments_response = self.gke_api_processor.list_deployments(zone, cluster_name, namespace_name)
                            deployments_dict = deployments_response.to_dict()
                            
                            for item in deployments_dict.get('items', []):
                                metadata = item.get('metadata', {})
                                deployment_name = metadata.get('name')
                                
                                if not deployment_name:
                                    continue
                                
                                # Use zone/cluster/namespace/deployment as the unique identifier
                                namespaced_name = f"{zone}/{cluster_name}/{namespace_name}/{deployment_name}"
                                
                                # Add cluster and zone context to the item
                                item['gke_context'] = {
                                    'zone': zone,
                                    'cluster': cluster_name
                                }
                                
                                model_data[namespaced_name] = item
                                
                                sanitized_item = self._sanitize_metadata(item)
                                model_data[namespaced_name] = sanitized_item
                                        
                        except Exception as e:
                            logger.error(f"Error extracting deployments for namespace {namespace_name} in cluster {cluster_name}: {str(e)}")
                            continue
                            
                except Exception as e:
                    logger.error(f"Error extracting deployments for cluster {cluster_name} in zone {zone}: {str(e)}")
                    continue
            
            logger.info(f"Extracted {len(model_data)} deployments from GKE clusters")
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
            return model_data
            
        except Exception as e:
            logger.error(f"Exception in GKE extract_deployments method: {str(e)}")
            raise

    @log_function_call
    def extract_ingresses(self, save_to_db=True):
        try:
            model_data = {}
            model_type = SourceModelType.GKE_INGRESS
            
            clusters = self.gke_api_processor.list_clusters()
            if not clusters:
                return model_data
            
            for cluster in clusters:
                zone = cluster.get('zone', 'us-central1-a')
                cluster_name = cluster.get('name')
                
                if not cluster_name:
                    continue
                
                try:
                    # Get namespaces first
                    namespaces_response = self.gke_api_processor.list_namespaces(zone, cluster_name)
                    namespaces_dict = namespaces_response.to_dict()
                    
                    for ns_item in namespaces_dict.get('items', []):
                        namespace_name = ns_item.get('metadata', {}).get('name')
                        if not namespace_name:
                            continue
                        
                        try:
                            ingresses_response = self.gke_api_processor.list_ingresses(zone, cluster_name, namespace_name)
                            ingresses_dict = ingresses_response.to_dict()
                            
                            for item in ingresses_dict.get('items', []):
                                metadata = item.get('metadata', {})
                                ingress_name = metadata.get('name')
                                
                                if not ingress_name:
                                    continue
                                
                                # Use zone/cluster/namespace/ingress as the unique identifier
                                namespaced_name = f"{zone}/{cluster_name}/{namespace_name}/{ingress_name}"
                                
                                # Add cluster and zone context to the item
                                item['gke_context'] = {
                                    'zone': zone,
                                    'cluster': cluster_name
                                }
                                
                                model_data[namespaced_name] = item
                                
                                sanitized_item = self._sanitize_metadata(item)
                                model_data[namespaced_name] = sanitized_item
                                        
                        except Exception as e:
                            logger.error(f"Error extracting ingresses for namespace {namespace_name} in cluster {cluster_name}: {str(e)}")
                            continue
                            
                except Exception as e:
                    logger.error(f"Error extracting ingresses for cluster {cluster_name} in zone {zone}: {str(e)}")
                    continue
            
            logger.info(f"Extracted {len(model_data)} ingresses from GKE clusters")
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
            return model_data
            
        except Exception as e:
            logger.error(f"Exception in GKE extract_ingresses method: {str(e)}")
            raise

    @log_function_call
    def extract_network_policies(self, save_to_db=True):
        try:
            model_data = {}
            model_type = SourceModelType.GKE_NETWORK_POLICY
            
            clusters = self.gke_api_processor.list_clusters()
            if not clusters:
                return model_data
            
            for cluster in clusters:
                zone = cluster.get('zone', 'us-central1-a')
                cluster_name = cluster.get('name')
                
                if not cluster_name:
                    continue
                
                try:
                    # Get namespaces first
                    namespaces_response = self.gke_api_processor.list_namespaces(zone, cluster_name)
                    namespaces_dict = namespaces_response.to_dict()
                    
                    for ns_item in namespaces_dict.get('items', []):
                        namespace_name = ns_item.get('metadata', {}).get('name')
                        if not namespace_name:
                            continue
                        
                        try:
                            policies_response = self.gke_api_processor.list_network_policies(zone, cluster_name, namespace_name)
                            policies_dict = policies_response.to_dict()
                            
                            for item in policies_dict.get('items', []):
                                metadata = item.get('metadata', {})
                                policy_name = metadata.get('name')
                                
                                if not policy_name:
                                    continue
                                
                                # Use zone/cluster/namespace/policy as the unique identifier
                                namespaced_name = f"{zone}/{cluster_name}/{namespace_name}/{policy_name}"
                                
                                # Add cluster and zone context to the item
                                item['gke_context'] = {
                                    'zone': zone,
                                    'cluster': cluster_name
                                }
                                
                                model_data[namespaced_name] = item
                                
                                sanitized_item = self._sanitize_metadata(item)
                                model_data[namespaced_name] = sanitized_item
                                        
                        except Exception as e:
                            logger.error(f"Error extracting network policies for namespace {namespace_name} in cluster {cluster_name}: {str(e)}")
                            continue
                            
                except Exception as e:
                    logger.error(f"Error extracting network policies for cluster {cluster_name} in zone {zone}: {str(e)}")
                    continue
            
            logger.info(f"Extracted {len(model_data)} network policies from GKE clusters")
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
            return model_data
            
        except Exception as e:
            logger.error(f"Exception in GKE extract_network_policies method: {str(e)}")
            raise

    @log_function_call
    def extract_pod_autoscalers(self):
        try:
            model_data = {}
            model_type = SourceModelType.GKE_HPA
            
            clusters = self.gke_api_processor.list_clusters()
            if not clusters:
                return model_data
            
            for cluster in clusters:
                zone = cluster.get('zone', 'us-central1-a')
                cluster_name = cluster.get('name')
                
                if not cluster_name:
                    continue
                
                try:
                    # Get namespaces first
                    namespaces_response = self.gke_api_processor.list_namespaces(zone, cluster_name)
                    namespaces_dict = namespaces_response.to_dict()
                    
                    for ns_item in namespaces_dict.get('items', []):
                        namespace_name = ns_item.get('metadata', {}).get('name')
                        if not namespace_name:
                            continue
                        
                        try:
                            hpas_response = self.gke_api_processor.list_horizontal_pod_autoscalers(zone, cluster_name, namespace_name)
                            hpas_dict = hpas_response.to_dict()
                            
                            for item in hpas_dict.get('items', []):
                                metadata = item.get('metadata', {})
                                hpa_name = metadata.get('name')
                                
                                if not hpa_name:
                                    continue
                                
                                # Use zone/cluster/namespace/hpa as the unique identifier
                                namespaced_name = f"{zone}/{cluster_name}/{namespace_name}/{hpa_name}"
                                
                                # Add cluster and zone context to the item
                                item['gke_context'] = {
                                    'zone': zone,
                                    'cluster': cluster_name
                                }
                                
                                model_data[namespaced_name] = item
                                
                                sanitized_item = self._sanitize_metadata(item)
                                model_data[namespaced_name] = sanitized_item
                                        
                        except Exception as e:
                            logger.error(f"Error extracting HPAs for namespace {namespace_name} in cluster {cluster_name}: {str(e)}")
                            continue
                            
                except Exception as e:
                    logger.error(f"Error extracting HPAs for cluster {cluster_name} in zone {zone}: {str(e)}")
                    continue
            
            logger.info(f"Extracted {len(model_data)} HPAs from GKE clusters")
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
            return model_data
            
        except Exception as e:
            logger.error(f"Exception in GKE extract_pod_autoscalers method: {str(e)}")
            raise

    @log_function_call
    def extract_replicasets(self, save_to_db=True):
        try:
            model_data = {}
            model_type = SourceModelType.GKE_REPLICASET
            
            clusters = self.gke_api_processor.list_clusters()
            if not clusters:
                return model_data
            
            for cluster in clusters:
                zone = cluster.get('zone', 'us-central1-a')
                cluster_name = cluster.get('name')
                
                if not cluster_name:
                    continue
                
                try:
                    # Get namespaces first
                    namespaces_response = self.gke_api_processor.list_namespaces(zone, cluster_name)
                    namespaces_dict = namespaces_response.to_dict()
                    
                    for ns_item in namespaces_dict.get('items', []):
                        namespace_name = ns_item.get('metadata', {}).get('name')
                        if not namespace_name:
                            continue
                        
                        try:
                            replicasets_response = self.gke_api_processor.list_replicasets(zone, cluster_name, namespace_name)
                            replicasets_dict = replicasets_response.to_dict()
                            
                            for item in replicasets_dict.get('items', []):
                                metadata = item.get('metadata', {})
                                rs_name = metadata.get('name')
                                
                                if not rs_name:
                                    continue
                                
                                # Use zone/cluster/namespace/replicaset as the unique identifier
                                namespaced_name = f"{zone}/{cluster_name}/{namespace_name}/{rs_name}"
                                
                                # Add cluster and zone context to the item
                                item['gke_context'] = {
                                    'zone': zone,
                                    'cluster': cluster_name
                                }
                                
                                model_data[namespaced_name] = item
                                
                                sanitized_item = self._sanitize_metadata(item)
                                model_data[namespaced_name] = sanitized_item
                                        
                        except Exception as e:
                            logger.error(f"Error extracting replicasets for namespace {namespace_name} in cluster {cluster_name}: {str(e)}")
                            continue
                            
                except Exception as e:
                    logger.error(f"Error extracting replicasets for cluster {cluster_name} in zone {zone}: {str(e)}")
                    continue

            logger.info(f"Extracted {len(model_data)} replicasets from GKE clusters")
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
            return model_data
            
        except Exception as e:
            logger.error(f"Exception in GKE extract_replicasets method: {str(e)}")
            raise

    @log_function_call
    def extract_statefulsets(self, save_to_db=True):
        try:
            model_data = {}
            model_type = SourceModelType.GKE_STATEFULSET
            
            clusters = self.gke_api_processor.list_clusters()
            if not clusters:
                return model_data
            
            for cluster in clusters:
                zone = cluster.get('zone', 'us-central1-a')
                cluster_name = cluster.get('name')
                
                if not cluster_name:
                    continue
                
                try:
                    # Get namespaces first
                    namespaces_response = self.gke_api_processor.list_namespaces(zone, cluster_name)
                    namespaces_dict = namespaces_response.to_dict()
                    
                    for ns_item in namespaces_dict.get('items', []):
                        namespace_name = ns_item.get('metadata', {}).get('name')
                        if not namespace_name:
                            continue
                        
                        try:
                            statefulsets_response = self.gke_api_processor.list_statefulsets(zone, cluster_name, namespace_name)
                            statefulsets_dict = statefulsets_response.to_dict()
                            
                            for item in statefulsets_dict.get('items', []):
                                metadata = item.get('metadata', {})
                                ss_name = metadata.get('name')
                                
                                if not ss_name:
                                    continue
                                
                                # Use zone/cluster/namespace/statefulset as the unique identifier
                                namespaced_name = f"{zone}/{cluster_name}/{namespace_name}/{ss_name}"
                                
                                # Add cluster and zone context to the item
                                item['gke_context'] = {
                                    'zone': zone,
                                    'cluster': cluster_name
                                }
                                
                                model_data[namespaced_name] = item
                                
                                sanitized_item = self._sanitize_metadata(item)
                                model_data[namespaced_name] = sanitized_item
                                        
                        except Exception as e:
                            logger.error(f"Error extracting statefulsets for namespace {namespace_name} in cluster {cluster_name}: {str(e)}")
                            continue
                            
                except Exception as e:
                    logger.error(f"Error extracting statefulsets for cluster {cluster_name} in zone {zone}: {str(e)}")
                    continue
            
            logger.info(f"Extracted {len(model_data)} statefulsets from GKE clusters")
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
            return model_data
            
        except Exception as e:
            logger.error(f"Exception in GKE extract_statefulsets method: {str(e)}")
            raise
    
    @log_function_call
    def extract_deployments_for_namespace(self, zone, cluster_name, namespace):
        """Extract deployments for a specific namespace in a specific cluster"""
        model_data = {}
        model_type = SourceModelType.GKE_DEPLOYMENT

        try:
            deployments_response = self.gke_api_processor.list_deployments(zone, cluster_name, namespace)
            deployments_dict = deployments_response.to_dict()

            for item in deployments_dict.get('items', []):
                metadata = item.get('metadata', {})
                deployment_name = metadata.get('name')

                if not deployment_name:
                    continue

                # Use zone/cluster/namespace/deployment as the unique identifier
                namespaced_name = f"{zone}/{cluster_name}/{namespace}/{deployment_name}"

                # Add cluster and zone context to the item
                item['gke_context'] = {
                    'zone': zone,
                    'cluster': cluster_name
                }

                model_data[namespaced_name] = self._sanitize_metadata(item)

            logger.info(f"Extracted {len(model_data)} deployments from namespace {namespace} in cluster {cluster_name}")
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
            return model_data

        except Exception as e:
            logger.error(f"Error extracting deployments for namespace {namespace} in cluster {cluster_name}: {str(e)}")
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_services_for_namespace(self, zone, cluster_name, namespace):
        """Extract services for a specific namespace in a specific cluster"""
        model_data = {}
        model_type = SourceModelType.GKE_SERVICE

        try:
            services_response = self.gke_api_processor.list_services(zone, cluster_name, namespace)
            services_dict = services_response.to_dict()

            for item in services_dict.get('items', []):
                metadata = item.get('metadata', {})
                service_name = metadata.get('name')

                if not service_name:
                    continue

                # Use zone/cluster/namespace/service as the unique identifier
                namespaced_name = f"{zone}/{cluster_name}/{namespace}/{service_name}"

                # Add cluster and zone context to the item
                item['gke_context'] = {
                    'zone': zone,
                    'cluster': cluster_name
                }

                model_data[namespaced_name] = self._sanitize_metadata(item)

            logger.info(f"Extracted {len(model_data)} services from namespace {namespace} in cluster {cluster_name}")

        except Exception as e:
            logger.error(f"Error extracting services for namespace {namespace} in cluster {cluster_name}: {str(e)}")

        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_ingresses_for_namespace(self, zone, cluster_name, namespace):
        """Extract ingresses for a specific namespace in a specific cluster"""
        model_data = {}
        model_type = SourceModelType.GKE_INGRESS

        try:
            ingresses_response = self.gke_api_processor.list_ingresses(zone, cluster_name, namespace)
            ingresses_dict = ingresses_response.to_dict()

            for item in ingresses_dict.get('items', []):
                metadata = item.get('metadata', {})
                ingress_name = metadata.get('name')

                if not ingress_name:
                    continue

                # Use zone/cluster/namespace/ingress as the unique identifier
                namespaced_name = f"{zone}/{cluster_name}/{namespace}/{ingress_name}"

                # Add cluster and zone context to the item
                item['gke_context'] = {
                    'zone': zone,
                    'cluster': cluster_name
                }

                model_data[namespaced_name] = self._sanitize_metadata(item)

            logger.info(f"Extracted {len(model_data)} ingresses from namespace {namespace} in cluster {cluster_name}")

        except Exception as e:
            logger.error(f"Error extracting ingresses for namespace {namespace} in cluster {cluster_name}: {str(e)}")

        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_network_policies_for_namespace(self, zone, cluster_name, namespace):
        """Extract network policies for a specific namespace in a specific cluster"""
        model_data = {}
        model_type = SourceModelType.GKE_NETWORK_POLICY

        try:
            policies_response = self.gke_api_processor.list_network_policies(zone, cluster_name, namespace)
            policies_dict = policies_response.to_dict()

            for item in policies_dict.get('items', []):
                metadata = item.get('metadata', {})
                policy_name = metadata.get('name')

                if not policy_name:
                    continue

                # Use zone/cluster/namespace/policy as the unique identifier
                namespaced_name = f"{zone}/{cluster_name}/{namespace}/{policy_name}"

                # Add cluster and zone context to the item
                item['gke_context'] = {
                    'zone': zone,
                    'cluster': cluster_name
                }

                model_data[namespaced_name] = self._sanitize_metadata(item)

            logger.info(f"Extracted {len(model_data)} network policies from namespace {namespace} in cluster {cluster_name}")

        except Exception as e:
            logger.error(f"Error extracting network policies for namespace {namespace} in cluster {cluster_name}: {str(e)}")

        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_replicasets_for_namespace(self, zone, cluster_name, namespace):
        """Extract replicasets for a specific namespace in a specific cluster"""
        model_data = {}
        model_type = SourceModelType.GKE_REPLICASET

        try:
            replicasets_response = self.gke_api_processor.list_replicasets(zone, cluster_name, namespace)
            replicasets_dict = replicasets_response.to_dict()

            for item in replicasets_dict.get('items', []):
                metadata = item.get('metadata', {})
                rs_name = metadata.get('name')

                if not rs_name:
                    continue

                # Use zone/cluster/namespace/replicaset as the unique identifier
                namespaced_name = f"{zone}/{cluster_name}/{namespace}/{rs_name}"

                # Add cluster and zone context to the item
                item['gke_context'] = {
                    'zone': zone,
                    'cluster': cluster_name
                }

                model_data[namespaced_name] = self._sanitize_metadata(item)

            logger.info(f"Extracted {len(model_data)} replicasets from namespace {namespace} in cluster {cluster_name}")

        except Exception as e:
            logger.error(f"Error extracting replicasets for namespace {namespace} in cluster {cluster_name}: {str(e)}")

        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_statefulsets_for_namespace(self, zone, cluster_name, namespace):
        """Extract statefulsets for a specific namespace in a specific cluster"""
        model_data = {}
        model_type = SourceModelType.GKE_STATEFULSET

        try:
            statefulsets_response = self.gke_api_processor.list_statefulsets(zone, cluster_name, namespace)
            statefulsets_dict = statefulsets_response.to_dict()

            for item in statefulsets_dict.get('items', []):
                metadata = item.get('metadata', {})
                ss_name = metadata.get('name')

                if not ss_name:
                    continue

                # Use zone/cluster/namespace/statefulset as the unique identifier
                namespaced_name = f"{zone}/{cluster_name}/{namespace}/{ss_name}"

                # Add cluster and zone context to the item
                item['gke_context'] = {
                    'zone': zone,
                    'cluster': cluster_name
                }

                model_data[namespaced_name] = self._sanitize_metadata(item)

            logger.info(f"Extracted {len(model_data)} statefulsets from namespace {namespace} in cluster {cluster_name}")

        except Exception as e:
            logger.error(f"Error extracting statefulsets for namespace {namespace} in cluster {cluster_name}: {str(e)}")
        
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_pod_autoscalers_for_namespace(self, zone, cluster_name, namespace):
        """Extract HPAs for a specific namespace in a specific cluster"""
        model_data = {}
        model_type = SourceModelType.GKE_HPA

        try:
            hpas_response = self.gke_api_processor.list_horizontal_pod_autoscalers(zone, cluster_name, namespace)
            hpas_dict = hpas_response.to_dict()

            for item in hpas_dict.get('items', []):
                metadata = item.get('metadata', {})
                hpa_name = metadata.get('name')

                if not hpa_name:
                    continue

                # Use zone/cluster/namespace/hpa as the unique identifier
                namespaced_name = f"{zone}/{cluster_name}/{namespace}/{hpa_name}"

                # Add cluster and zone context to the item
                item['gke_context'] = {
                    'zone': zone,
                    'cluster': cluster_name
                }

                model_data[namespaced_name] = self._sanitize_metadata(item)

            logger.info(f"Extracted {len(model_data)} HPAs from namespace {namespace} in cluster {cluster_name}")

        except Exception as e:
            logger.error(f"Error extracting HPAs for namespace {namespace} in cluster {cluster_name}: {str(e)}")
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    # ==================== GCP Infrastructure Extraction Methods ====================

    @log_function_call
    def extract_compute_instances(self):
        """Extract GCP Compute Engine instances."""
        model_type = SourceModelType.GCP_COMPUTE_INSTANCE
        model_data = {}
        try:
            instances = self.gcp_api_processor.compute_list_instances()
            for instance in instances:
                instance_id = f"{instance.get('zone')}/{instance.get('name')}"
                instance['gcp_context'] = {'project_id': self.__project_id}
                model_data[instance_id] = self._sanitize_metadata(instance)
            logger.info(f"Extracted {len(model_data)} Compute Engine instances")
        except Exception as e:
            logger.error(f"Error extracting Compute Engine instances: {e}")
        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_instance_groups(self):
        """Extract GCP Instance Groups."""
        model_type = SourceModelType.GCP_INSTANCE_GROUP
        model_data = {}
        try:
            groups = self.gcp_api_processor.compute_list_instance_groups()
            for group in groups:
                group_id = f"{group.get('zone')}/{group.get('name')}"
                group['gcp_context'] = {'project_id': self.__project_id}
                model_data[group_id] = self._sanitize_metadata(group)
            logger.info(f"Extracted {len(model_data)} Instance Groups")
        except Exception as e:
            logger.error(f"Error extracting Instance Groups: {e}")
        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_storage_buckets(self):
        """Extract GCP Cloud Storage buckets."""
        model_type = SourceModelType.GCP_STORAGE_BUCKET
        model_data = {}
        try:
            buckets = self.gcp_api_processor.storage_list_buckets()
            for bucket in buckets:
                bucket['gcp_context'] = {'project_id': self.__project_id}
                model_data[bucket.get('name')] = self._sanitize_metadata(bucket)
            logger.info(f"Extracted {len(model_data)} Cloud Storage buckets")
        except Exception as e:
            logger.error(f"Error extracting Cloud Storage buckets: {e}")
        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_cloud_sql_instances(self):
        """Extract GCP Cloud SQL instances."""
        model_type = SourceModelType.GCP_CLOUD_SQL_INSTANCE
        model_data = {}
        try:
            instances = self.gcp_api_processor.sql_list_instances()
            for instance in instances:
                instance['gcp_context'] = {'project_id': self.__project_id}
                model_data[instance.get('name')] = self._sanitize_metadata(instance)
            logger.info(f"Extracted {len(model_data)} Cloud SQL instances")
        except Exception as e:
            logger.error(f"Error extracting Cloud SQL instances: {e}")
        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_cloud_sql_databases(self):
        """Extract GCP Cloud SQL databases."""
        model_type = SourceModelType.GCP_CLOUD_SQL_DATABASE
        model_data = {}
        try:
            instances = self.gcp_api_processor.sql_list_instances()
            for instance in instances:
                instance_name = instance.get('name')
                databases = self.gcp_api_processor.sql_list_databases(instance_name)
                for db in databases:
                    db_id = f"{instance_name}/{db.get('name')}"
                    db['gcp_context'] = {'project_id': self.__project_id, 'instance': instance_name}
                    model_data[db_id] = self._sanitize_metadata(db)
            logger.info(f"Extracted {len(model_data)} Cloud SQL databases")
        except Exception as e:
            logger.error(f"Error extracting Cloud SQL databases: {e}")
        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_memorystore_redis(self):
        """Extract GCP Memorystore Redis instances."""
        model_type = SourceModelType.GCP_MEMORYSTORE_REDIS
        model_data = {}
        try:
            instances = self.gcp_api_processor.redis_list_instances()
            for instance in instances:
                instance_id = f"{instance.get('location')}/{instance.get('name')}"
                instance['gcp_context'] = {'project_id': self.__project_id}
                model_data[instance_id] = self._sanitize_metadata(instance)
            logger.info(f"Extracted {len(model_data)} Memorystore Redis instances")
        except Exception as e:
            logger.error(f"Error extracting Memorystore Redis instances: {e}")
        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_alert_policies(self):
        """Extract GCP Monitoring alert policies."""
        model_type = SourceModelType.GCP_ALERT_POLICY
        model_data = {}
        try:
            policies = self.gcp_api_processor.monitoring_list_alert_policies()
            for policy in policies:
                policy['gcp_context'] = {'project_id': self.__project_id}
                model_data[policy.get('name')] = self._sanitize_metadata(policy)
            logger.info(f"Extracted {len(model_data)} alert policies")
        except Exception as e:
            logger.error(f"Error extracting alert policies: {e}")
        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_notification_channels(self):
        """Extract GCP notification channels."""
        model_type = SourceModelType.GCP_NOTIFICATION_CHANNEL
        model_data = {}
        try:
            channels = self.gcp_api_processor.monitoring_list_notification_channels()
            for channel in channels:
                channel['gcp_context'] = {'project_id': self.__project_id}
                model_data[channel.get('name')] = self._sanitize_metadata(channel)
            logger.info(f"Extracted {len(model_data)} notification channels")
        except Exception as e:
            logger.error(f"Error extracting notification channels: {e}")
        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_cloud_functions(self):
        """Extract GCP Cloud Functions."""
        model_type = SourceModelType.GCP_CLOUD_FUNCTION
        model_data = {}
        try:
            functions = self.gcp_api_processor.functions_list()
            for func in functions:
                func_id = f"{func.get('location')}/{func.get('name')}"
                func['gcp_context'] = {'project_id': self.__project_id}
                model_data[func_id] = self._sanitize_metadata(func)
            logger.info(f"Extracted {len(model_data)} Cloud Functions")
        except Exception as e:
            logger.error(f"Error extracting Cloud Functions: {e}")
        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_cloud_run_services(self):
        """Extract GCP Cloud Run services."""
        model_type = SourceModelType.GCP_CLOUD_RUN_SERVICE
        model_data = {}
        try:
            services = self.gcp_api_processor.cloudrun_list_services()
            for service in services:
                service_id = f"{service.get('location')}/{service.get('name')}"
                service['gcp_context'] = {'project_id': self.__project_id}
                model_data[service_id] = self._sanitize_metadata(service)
            logger.info(f"Extracted {len(model_data)} Cloud Run services")
        except Exception as e:
            logger.error(f"Error extracting Cloud Run services: {e}")
        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_pubsub_topics(self):
        """Extract GCP Pub/Sub topics."""
        model_type = SourceModelType.GCP_PUBSUB_TOPIC
        model_data = {}
        try:
            topics = self.gcp_api_processor.pubsub_list_topics()
            for topic in topics:
                topic['gcp_context'] = {'project_id': self.__project_id}
                model_data[topic.get('name')] = self._sanitize_metadata(topic)
            logger.info(f"Extracted {len(model_data)} Pub/Sub topics")
        except Exception as e:
            logger.error(f"Error extracting Pub/Sub topics: {e}")
        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_pubsub_subscriptions(self):
        """Extract GCP Pub/Sub subscriptions."""
        model_type = SourceModelType.GCP_PUBSUB_SUBSCRIPTION
        model_data = {}
        try:
            subscriptions = self.gcp_api_processor.pubsub_list_subscriptions()
            for sub in subscriptions:
                sub['gcp_context'] = {'project_id': self.__project_id}
                model_data[sub.get('name')] = self._sanitize_metadata(sub)
            logger.info(f"Extracted {len(model_data)} Pub/Sub subscriptions")
        except Exception as e:
            logger.error(f"Error extracting Pub/Sub subscriptions: {e}")
        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_bigquery_datasets(self):
        """Extract GCP BigQuery datasets."""
        model_type = SourceModelType.GCP_BIGQUERY_DATASET
        model_data = {}
        try:
            datasets = self.gcp_api_processor.bigquery_list_datasets()
            for dataset in datasets:
                dataset['gcp_context'] = {'project_id': self.__project_id}
                model_data[dataset.get('dataset_id')] = self._sanitize_metadata(dataset)
            logger.info(f"Extracted {len(model_data)} BigQuery datasets")
        except Exception as e:
            logger.error(f"Error extracting BigQuery datasets: {e}")
        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_bigquery_tables(self):
        """Extract GCP BigQuery tables."""
        model_type = SourceModelType.GCP_BIGQUERY_TABLE
        model_data = {}
        try:
            datasets = self.gcp_api_processor.bigquery_list_datasets()
            for dataset in datasets:
                dataset_id = dataset.get('dataset_id')
                tables = self.gcp_api_processor.bigquery_list_tables(dataset_id)
                for table in tables:
                    table_id = f"{dataset_id}/{table.get('table_id')}"
                    table['gcp_context'] = {'project_id': self.__project_id}
                    model_data[table_id] = self._sanitize_metadata(table)
            logger.info(f"Extracted {len(model_data)} BigQuery tables")
        except Exception as e:
            logger.error(f"Error extracting BigQuery tables: {e}")
        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_vpc_networks(self):
        """Extract GCP VPC networks."""
        model_type = SourceModelType.GCP_VPC_NETWORK
        model_data = {}
        try:
            networks = self.gcp_api_processor.network_list_networks()
            for network in networks:
                network['gcp_context'] = {'project_id': self.__project_id}
                model_data[network.get('name')] = self._sanitize_metadata(network)
            logger.info(f"Extracted {len(model_data)} VPC networks")
        except Exception as e:
            logger.error(f"Error extracting VPC networks: {e}")
        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_subnetworks(self):
        """Extract GCP subnetworks."""
        model_type = SourceModelType.GCP_SUBNETWORK
        model_data = {}
        try:
            subnetworks = self.gcp_api_processor.network_list_subnetworks()
            for subnet in subnetworks:
                subnet_id = f"{subnet.get('region')}/{subnet.get('name')}"
                subnet['gcp_context'] = {'project_id': self.__project_id}
                model_data[subnet_id] = self._sanitize_metadata(subnet)
            logger.info(f"Extracted {len(model_data)} subnetworks")
        except Exception as e:
            logger.error(f"Error extracting subnetworks: {e}")
        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_firewall_rules(self):
        """Extract GCP firewall rules."""
        model_type = SourceModelType.GCP_FIREWALL_RULE
        model_data = {}
        try:
            rules = self.gcp_api_processor.network_list_firewall_rules()
            for rule in rules:
                rule['gcp_context'] = {'project_id': self.__project_id}
                model_data[rule.get('name')] = self._sanitize_metadata(rule)
            logger.info(f"Extracted {len(model_data)} firewall rules")
        except Exception as e:
            logger.error(f"Error extracting firewall rules: {e}")
        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_load_balancers(self):
        """Extract GCP load balancers (forwarding rules)."""
        model_type = SourceModelType.GCP_LOAD_BALANCER
        model_data = {}
        try:
            rules = self.gcp_api_processor.network_list_forwarding_rules()
            for rule in rules:
                rule_id = f"{rule.get('region')}/{rule.get('name')}"
                rule['gcp_context'] = {'project_id': self.__project_id}
                model_data[rule_id] = self._sanitize_metadata(rule)
            logger.info(f"Extracted {len(model_data)} load balancers")
        except Exception as e:
            logger.error(f"Error extracting load balancers: {e}")
        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_secrets(self):
        """Extract GCP Secret Manager secrets."""
        model_type = SourceModelType.GCP_SECRET
        model_data = {}
        try:
            secrets = self.gcp_api_processor.secrets_list()
            for secret in secrets:
                secret['gcp_context'] = {'project_id': self.__project_id}
                model_data[secret.get('name')] = self._sanitize_metadata(secret)
            logger.info(f"Extracted {len(model_data)} secrets")
        except Exception as e:
            logger.error(f"Error extracting secrets: {e}")
        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_service_accounts(self):
        """Extract GCP service accounts."""
        model_type = SourceModelType.GCP_SERVICE_ACCOUNT
        model_data = {}
        try:
            accounts = self.gcp_api_processor.iam_list_service_accounts()
            for account in accounts:
                account['gcp_context'] = {'project_id': self.__project_id}
                model_data[account.get('email')] = self._sanitize_metadata(account)
            logger.info(f"Extracted {len(model_data)} service accounts")
        except Exception as e:
            logger.error(f"Error extracting service accounts: {e}")
        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_log_sinks(self):
        """Extract GCP log sinks."""
        model_type = SourceModelType.GCP_LOG_SINK
        model_data = {}
        try:
            sinks = self.gcp_api_processor.logging_list_sinks()
            for sink in sinks:
                sink['gcp_context'] = {'project_id': self.__project_id}
                model_data[sink.get('name')] = self._sanitize_metadata(sink)
            logger.info(f"Extracted {len(model_data)} log sinks")
        except Exception as e:
            logger.error(f"Error extracting log sinks: {e}")
        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_log_metrics(self):
        """Extract GCP log-based metrics."""
        model_type = SourceModelType.GCP_LOG_METRIC
        model_data = {}
        try:
            metrics = self.gcp_api_processor.logging_list_metrics()
            for metric in metrics:
                metric['gcp_context'] = {'project_id': self.__project_id}
                model_data[metric.get('name')] = self._sanitize_metadata(metric)
            logger.info(f"Extracted {len(model_data)} log metrics")
        except Exception as e:
            logger.error(f"Error extracting log metrics: {e}")
        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data