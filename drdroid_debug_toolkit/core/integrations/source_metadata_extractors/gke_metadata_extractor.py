import logging
from core.integrations.source_metadata_extractor import SourceMetadataExtractor
from core.integrations.source_api_processors.gke_api_processor import GkeApiProcessor
from core.protos.base_pb2 import Source, SourceModelType
from core.utils.logging_utils import log_function_call

logger = logging.getLogger(__name__)

class GkeSourceMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id: str, connector_name: str, project_id, service_account_json):
        self.__project_id = project_id
        self.__service_account_json = service_account_json
        self.gke_api_processor = GkeApiProcessor(self.__project_id, self.__service_account_json)
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