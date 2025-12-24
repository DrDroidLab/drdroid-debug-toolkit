import logging

from core.integrations.source_metadata_extractor import SourceMetadataExtractor
from core.integrations.source_api_processors.eks_api_processor import EKSApiProcessor
from core.protos.base_pb2 import Source, SourceModelType
from core.utils.logging_utils import log_function_call

logger = logging.getLogger(__name__)


class EksSourceMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id: str, connector_name: str, region: str, k8_role_arn: str, aws_access_key: str = None,
                 aws_secret_key: str = None, aws_assumed_role_arn: str = None, aws_drd_cloud_role_arn: str = None):
        self.__region = region
        self.__aws_access_key = aws_access_key
        self.__aws_secret_key = aws_secret_key
        self.__k8_role_arn = k8_role_arn
        self.__aws_assumed_role_arn = aws_assumed_role_arn
        self.eks_client = EKSApiProcessor(self.__region, self.__k8_role_arn, self.__aws_access_key,
                                          self.__aws_secret_key, self.__aws_assumed_role_arn, aws_drd_cloud_role_arn)
        super().__init__(request_id, connector_name, Source.EKS)

    @log_function_call
    def extract_clusters(self):
        model_data = {}
        model_type = SourceModelType.EKS_CLUSTER
        clusters = self.eks_client.eks_list_clusters()
        if not clusters:
            return model_data
        model_data[self.__region] = {'clusters': clusters}
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_namespaces(self):
        """Extract all namespaces from all EKS clusters"""
        model_data = {}
        model_type = SourceModelType.EKS_NAMESPACE

        try:
            clusters = self.eks_client.eks_list_clusters()
            if not clusters:
                return model_data

            for cluster_name in clusters:
                try:
                    api_instance = self.eks_client.eks_get_api_instance(cluster_name, client='api')
                    if not api_instance:
                        continue

                    namespaces_response = api_instance.list_namespace()

                    for item in namespaces_response.items:
                        namespace_name = item.metadata.name
                        if not namespace_name:
                            continue

                        # Use region/cluster/namespace as the unique identifier
                        namespaced_name = f"{self.__region}/{cluster_name}/{namespace_name}"

                        # Convert to dict-like structure
                        namespace_dict = {
                            'metadata': {
                                'name': namespace_name,
                                'uid': item.metadata.uid,
                                'creation_timestamp': item.metadata.creation_timestamp.isoformat() if item.metadata.creation_timestamp else None
                            },
                            'status': {
                                'phase': item.status.phase if item.status else None
                            },
                            'eks_context': {
                                'region': self.__region,
                                'cluster': cluster_name
                            }
                        }

                        model_data[namespaced_name] = namespace_dict
                except Exception as e:
                    logger.error(f"Error extracting namespaces for cluster {cluster_name}: {str(e)}")
        except Exception as e:
            logger.error(f"Error extracting namespaces: {str(e)}")
            return model_data

        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_deployments(self):
        """Extract all deployments from all EKS clusters and namespaces"""
        model_data = {}
        model_type = SourceModelType.EKS_DEPLOYMENT

        try:
            clusters = self.eks_client.eks_list_clusters()
            if not clusters:
                return model_data

            for cluster_name in clusters:
                try:
                    # Get API instances
                    api_instance = self.eks_client.eks_get_api_instance(cluster_name, client='api')
                    app_instance = self.eks_client.eks_get_api_instance(cluster_name, client='app')

                    if not api_instance or not app_instance:
                        continue

                    # Get namespaces first
                    namespaces_response = api_instance.list_namespace()

                    for ns_item in namespaces_response.items:
                        namespace_name = ns_item.metadata.name
                        if not namespace_name:
                            continue

                        try:
                            deployments_response = app_instance.list_namespaced_deployment(namespace_name)

                            for item in deployments_response.items:
                                deployment_name = item.metadata.name
                                if not deployment_name:
                                    continue

                                # Use region/cluster/namespace/deployment as the unique identifier
                                namespaced_name = f"{self.__region}/{cluster_name}/{namespace_name}/{deployment_name}"

                                # Convert to dict-like structure
                                deployment_dict = {
                                    'metadata': {
                                        'name': deployment_name,
                                        'namespace': namespace_name,
                                        'uid': item.metadata.uid,
                                        'creation_timestamp': item.metadata.creation_timestamp.isoformat() if item.metadata.creation_timestamp else None,
                                        'labels': item.metadata.labels if item.metadata.labels else {}
                                    },
                                    'spec': {
                                        'replicas': item.spec.replicas if item.spec else None
                                    },
                                    'status': {
                                        'available_replicas': item.status.available_replicas if item.status else None,
                                        'ready_replicas': item.status.ready_replicas if item.status else None
                                    },
                                    'eks_context': {
                                        'region': self.__region,
                                        'cluster': cluster_name
                                    }
                                }

                                model_data[namespaced_name] = deployment_dict

                        except Exception as e:
                            logger.error(f"Error extracting deployments for namespace {namespace_name} in cluster {cluster_name}: {str(e)}")
                            continue

                except Exception as e:
                    logger.error(f"Error extracting deployments for cluster {cluster_name}: {str(e)}")
                    continue

            logger.info(f"Extracted {len(model_data)} deployments from EKS clusters")
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
            return model_data

        except Exception as e:
            logger.error(f"Exception in EKS extract_deployments method: {str(e)}")
            raise

    @log_function_call
    def extract_services(self):
        """Extract all services from all EKS clusters and namespaces"""
        model_data = {}
        model_type = SourceModelType.EKS_SERVICE

        try:
            clusters = self.eks_client.eks_list_clusters()
            if not clusters:
                return model_data

            for cluster_name in clusters:
                try:
                    api_instance = self.eks_client.eks_get_api_instance(cluster_name, client='api')
                    if not api_instance:
                        continue

                    # Get namespaces first
                    namespaces_response = api_instance.list_namespace()

                    for ns_item in namespaces_response.items:
                        namespace_name = ns_item.metadata.name
                        if not namespace_name:
                            continue

                        try:
                            services_response = api_instance.list_namespaced_service(namespace_name)

                            for item in services_response.items:
                                service_name = item.metadata.name
                                if not service_name:
                                    continue

                                # Use region/cluster/namespace/service as the unique identifier
                                namespaced_name = f"{self.__region}/{cluster_name}/{namespace_name}/{service_name}"

                                # Convert to dict-like structure
                                service_dict = {
                                    'metadata': {
                                        'name': service_name,
                                        'namespace': namespace_name,
                                        'uid': item.metadata.uid,
                                        'creation_timestamp': item.metadata.creation_timestamp.isoformat() if item.metadata.creation_timestamp else None,
                                        'labels': item.metadata.labels if item.metadata.labels else {}
                                    },
                                    'spec': {
                                        'type': item.spec.type if item.spec else None,
                                        'cluster_ip': item.spec.cluster_ip if item.spec else None
                                    },
                                    'eks_context': {
                                        'region': self.__region,
                                        'cluster': cluster_name
                                    }
                                }

                                model_data[namespaced_name] = service_dict

                                model_data[namespaced_name] = service_dict

                        except Exception as e:
                            logger.error(f"Error extracting services for namespace {namespace_name} in cluster {cluster_name}: {str(e)}")
                            continue

                except Exception as e:
                    logger.error(f"Error extracting services for cluster {cluster_name}: {str(e)}")
                    continue

            logger.info(f"Extracted {len(model_data)} services from EKS clusters")
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
            return model_data

        except Exception as e:
            logger.error(f"Exception in EKS extract_services method: {str(e)}")
            raise

    @log_function_call
    def extract_ingresses(self):
        """Extract all ingresses from all EKS clusters and namespaces"""
        model_data = {}
        model_type = SourceModelType.EKS_INGRESS

        try:
            clusters = self.eks_client.eks_list_clusters()
            if not clusters:
                return model_data

            for cluster_name in clusters:
                try:
                    api_instance = self.eks_client.eks_get_api_instance(cluster_name, client='api')
                    if not api_instance:
                        continue

                    # Get networking API instance
                    networking_instance = self.eks_client.eks_get_api_instance(cluster_name, client='networking')
                    if not networking_instance:
                        continue

                    # Get namespaces first
                    namespaces_response = api_instance.list_namespace()

                    for ns_item in namespaces_response.items:
                        namespace_name = ns_item.metadata.name
                        if not namespace_name:
                            continue

                        try:
                            ingresses_response = networking_instance.list_namespaced_ingress(namespace_name)

                            for item in ingresses_response.items:
                                ingress_name = item.metadata.name
                                if not ingress_name:
                                    continue

                                # Use region/cluster/namespace/ingress as the unique identifier
                                namespaced_name = f"{self.__region}/{cluster_name}/{namespace_name}/{ingress_name}"

                                # Convert to dict-like structure
                                ingress_dict = {
                                    'metadata': {
                                        'name': ingress_name,
                                        'namespace': namespace_name,
                                        'uid': item.metadata.uid,
                                        'creation_timestamp': item.metadata.creation_timestamp.isoformat() if item.metadata.creation_timestamp else None,
                                        'labels': item.metadata.labels if item.metadata.labels else {}
                                    },
                                    'eks_context': {
                                        'region': self.__region,
                                        'cluster': cluster_name
                                    }
                                }

                                model_data[namespaced_name] = ingress_dict

                                model_data[namespaced_name] = ingress_dict

                        except Exception as e:
                            logger.error(f"Error extracting ingresses for namespace {namespace_name} in cluster {cluster_name}: {str(e)}")
                            continue

                except Exception as e:
                    logger.error(f"Error extracting ingresses for cluster {cluster_name}: {str(e)}")
                    continue

            logger.info(f"Extracted {len(model_data)} ingresses from EKS clusters")
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
            return model_data

        except Exception as e:
            logger.error(f"Exception in EKS extract_ingresses method: {str(e)}")
            raise

    @log_function_call
    def extract_network_policies(self):
        """Extract all network policies from all EKS clusters and namespaces"""
        model_data = {}
        model_type = SourceModelType.EKS_NETWORK_POLICY

        try:
            clusters = self.eks_client.eks_list_clusters()
            if not clusters:
                return model_data

            for cluster_name in clusters:
                try:
                    api_instance = self.eks_client.eks_get_api_instance(cluster_name, client='api')
                    if not api_instance:
                        continue

                    # Get networking API instance
                    networking_instance = self.eks_client.eks_get_api_instance(cluster_name, client='networking')
                    if not networking_instance:
                        continue

                    # Get namespaces first
                    namespaces_response = api_instance.list_namespace()

                    for ns_item in namespaces_response.items:
                        namespace_name = ns_item.metadata.name
                        if not namespace_name:
                            continue

                        try:
                            policies_response = networking_instance.list_namespaced_network_policy(namespace_name)

                            for item in policies_response.items:
                                policy_name = item.metadata.name
                                if not policy_name:
                                    continue

                                # Use region/cluster/namespace/policy as the unique identifier
                                namespaced_name = f"{self.__region}/{cluster_name}/{namespace_name}/{policy_name}"

                                # Convert to dict-like structure
                                policy_dict = {
                                    'metadata': {
                                        'name': policy_name,
                                        'namespace': namespace_name,
                                        'uid': item.metadata.uid,
                                        'creation_timestamp': item.metadata.creation_timestamp.isoformat() if item.metadata.creation_timestamp else None,
                                        'labels': item.metadata.labels if item.metadata.labels else {}
                                    },
                                    'eks_context': {
                                        'region': self.__region,
                                        'cluster': cluster_name
                                    }
                                }

                                model_data[namespaced_name] = policy_dict

                        except Exception as e:
                            logger.error(f"Error extracting network policies for namespace {namespace_name} in cluster {cluster_name}: {str(e)}")
                            continue

                except Exception as e:
                    logger.error(f"Error extracting network policies for cluster {cluster_name}: {str(e)}")
                    continue

            logger.info(f"Extracted {len(model_data)} network policies from EKS clusters")
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
            return model_data

        except Exception as e:
            logger.error(f"Exception in EKS extract_network_policies method: {str(e)}")
            raise

    @log_function_call
    def extract_replicasets(self):
        """Extract all replicasets from all EKS clusters and namespaces"""
        model_data = {}
        model_type = SourceModelType.EKS_REPLICASET

        try:
            clusters = self.eks_client.eks_list_clusters()
            if not clusters:
                return model_data

            for cluster_name in clusters:
                try:
                    api_instance = self.eks_client.eks_get_api_instance(cluster_name, client='api')
                    app_instance = self.eks_client.eks_get_api_instance(cluster_name, client='app')

                    if not api_instance or not app_instance:
                        continue

                    # Get namespaces first
                    namespaces_response = api_instance.list_namespace()

                    for ns_item in namespaces_response.items:
                        namespace_name = ns_item.metadata.name
                        if not namespace_name:
                            continue

                        try:
                            replicasets_response = app_instance.list_namespaced_replica_set(namespace_name)

                            for item in replicasets_response.items:
                                rs_name = item.metadata.name
                                if not rs_name:
                                    continue

                                # Use region/cluster/namespace/replicaset as the unique identifier
                                namespaced_name = f"{self.__region}/{cluster_name}/{namespace_name}/{rs_name}"

                                # Convert to dict-like structure
                                rs_dict = {
                                    'metadata': {
                                        'name': rs_name,
                                        'namespace': namespace_name,
                                        'uid': item.metadata.uid,
                                        'creation_timestamp': item.metadata.creation_timestamp.isoformat() if item.metadata.creation_timestamp else None,
                                        'labels': item.metadata.labels if item.metadata.labels else {}
                                    },
                                    'spec': {
                                        'replicas': item.spec.replicas if item.spec else None
                                    },
                                    'status': {
                                        'available_replicas': item.status.available_replicas if item.status else None,
                                        'ready_replicas': item.status.ready_replicas if item.status else None
                                    },
                                    'eks_context': {
                                        'region': self.__region,
                                        'cluster': cluster_name
                                    }
                                }

                                model_data[namespaced_name] = rs_dict

                        except Exception as e:
                            logger.error(f"Error extracting replicasets for namespace {namespace_name} in cluster {cluster_name}: {str(e)}")
                            continue

                except Exception as e:
                    logger.error(f"Error extracting replicasets for cluster {cluster_name}: {str(e)}")
                    continue

            logger.info(f"Extracted {len(model_data)} replicasets from EKS clusters")
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
            return model_data

        except Exception as e:
            logger.error(f"Exception in EKS extract_replicasets method: {str(e)}")
            raise

    @log_function_call
    def extract_statefulsets(self):
        """Extract all statefulsets from all EKS clusters and namespaces"""
        model_data = {}
        model_type = SourceModelType.EKS_STATEFULSET

        try:
            clusters = self.eks_client.eks_list_clusters()
            if not clusters:
                return model_data

            for cluster_name in clusters:
                try:
                    api_instance = self.eks_client.eks_get_api_instance(cluster_name, client='api')
                    app_instance = self.eks_client.eks_get_api_instance(cluster_name, client='app')

                    if not api_instance or not app_instance:
                        continue

                    # Get namespaces first
                    namespaces_response = api_instance.list_namespace()

                    for ns_item in namespaces_response.items:
                        namespace_name = ns_item.metadata.name
                        if not namespace_name:
                            continue

                        try:
                            statefulsets_response = app_instance.list_namespaced_stateful_set(namespace_name)

                            for item in statefulsets_response.items:
                                ss_name = item.metadata.name
                                if not ss_name:
                                    continue

                                # Use region/cluster/namespace/statefulset as the unique identifier
                                namespaced_name = f"{self.__region}/{cluster_name}/{namespace_name}/{ss_name}"

                                # Convert to dict-like structure
                                ss_dict = {
                                    'metadata': {
                                        'name': ss_name,
                                        'namespace': namespace_name,
                                        'uid': item.metadata.uid,
                                        'creation_timestamp': item.metadata.creation_timestamp.isoformat() if item.metadata.creation_timestamp else None,
                                        'labels': item.metadata.labels if item.metadata.labels else {}
                                    },
                                    'spec': {
                                        'replicas': item.spec.replicas if item.spec else None
                                    },
                                    'status': {
                                        'ready_replicas': item.status.ready_replicas if item.status else None
                                    },
                                    'eks_context': {
                                        'region': self.__region,
                                        'cluster': cluster_name
                                    }
                                }

                                model_data[namespaced_name] = ss_dict

                        except Exception as e:
                            logger.error(f"Error extracting statefulsets for namespace {namespace_name} in cluster {cluster_name}: {str(e)}")
                            continue

                except Exception as e:
                    logger.error(f"Error extracting statefulsets for cluster {cluster_name}: {str(e)}")
                    continue

            logger.info(f"Extracted {len(model_data)} statefulsets from EKS clusters")
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
            return model_data

        except Exception as e:
            logger.error(f"Exception in EKS extract_statefulsets method: {str(e)}")
            raise

    @log_function_call
    def extract_pod_autoscalers(self):
        """Extract all HPAs from all EKS clusters and namespaces"""
        model_data = {}
        model_type = SourceModelType.EKS_HPA

        try:
            clusters = self.eks_client.eks_list_clusters()
            if not clusters:
                return model_data

            for cluster_name in clusters:
                try:
                    api_instance = self.eks_client.eks_get_api_instance(cluster_name, client='api')
                    if not api_instance:
                        continue

                    # Get autoscaling API instance
                    autoscaling_instance = self.eks_client.eks_get_api_instance(cluster_name, client='autoscaling')
                    if not autoscaling_instance:
                        continue

                    # Get namespaces first
                    namespaces_response = api_instance.list_namespace()

                    for ns_item in namespaces_response.items:
                        namespace_name = ns_item.metadata.name
                        if not namespace_name:
                            continue

                        try:
                            hpas_response = autoscaling_instance.list_namespaced_horizontal_pod_autoscaler(namespace_name)

                            for item in hpas_response.items:
                                hpa_name = item.metadata.name
                                if not hpa_name:
                                    continue

                                # Use region/cluster/namespace/hpa as the unique identifier
                                namespaced_name = f"{self.__region}/{cluster_name}/{namespace_name}/{hpa_name}"

                                # Convert to dict-like structure
                                hpa_dict = {
                                    'metadata': {
                                        'name': hpa_name,
                                        'namespace': namespace_name,
                                        'uid': item.metadata.uid,
                                        'creation_timestamp': item.metadata.creation_timestamp.isoformat() if item.metadata.creation_timestamp else None,
                                        'labels': item.metadata.labels if item.metadata.labels else {}
                                    },
                                    'spec': {
                                        'min_replicas': item.spec.min_replicas if item.spec else None,
                                        'max_replicas': item.spec.max_replicas if item.spec else None
                                    },
                                    'eks_context': {
                                        'region': self.__region,
                                        'cluster': cluster_name
                                    }
                                }

                                model_data[namespaced_name] = hpa_dict

                        except Exception as e:
                            logger.error(f"Error extracting HPAs for namespace {namespace_name} in cluster {cluster_name}: {str(e)}")
                            continue

                except Exception as e:
                    logger.error(f"Error extracting HPAs for cluster {cluster_name}: {str(e)}")
                    continue

            logger.info(f"Extracted {len(model_data)} HPAs from EKS clusters")
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
            return model_data

        except Exception as e:
            logger.error(f"Exception in EKS extract_pod_autoscalers method: {str(e)}")
            raise

    # Namespace-specific extraction methods for single namespace refresh
    @log_function_call
    def extract_deployments_for_namespace(self, cluster_name, namespace):
        """Extract deployments for a specific namespace in a specific cluster"""
        model_data = {}
        model_type = SourceModelType.EKS_DEPLOYMENT

        try:
            app_instance = self.eks_client.eks_get_api_instance(cluster_name, client='app')
            if not app_instance:
                return model_data

            deployments_response = app_instance.list_namespaced_deployment(namespace)

            for item in deployments_response.items:
                deployment_name = item.metadata.name
                if not deployment_name:
                    continue

                # Use region/cluster/namespace/deployment as the unique identifier
                namespaced_name = f"{self.__region}/{cluster_name}/{namespace}/{deployment_name}"

                # Convert to dict-like structure
                deployment_dict = {
                    'metadata': {
                        'name': deployment_name,
                        'namespace': namespace,
                        'uid': item.metadata.uid,
                        'creation_timestamp': item.metadata.creation_timestamp.isoformat() if item.metadata.creation_timestamp else None,
                        'labels': item.metadata.labels if item.metadata.labels else {}
                    },
                    'spec': {
                        'replicas': item.spec.replicas if item.spec else None
                    },
                    'status': {
                        'available_replicas': item.status.available_replicas if item.status else None,
                        'ready_replicas': item.status.ready_replicas if item.status else None
                    },
                    'eks_context': {
                        'region': self.__region,
                        'cluster': cluster_name
                    }
                }

                model_data[namespaced_name] = deployment_dict

            logger.info(f"Extracted {len(model_data)} deployments from namespace {namespace} in cluster {cluster_name}")
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
            return model_data

        except Exception as e:
            logger.error(f"Error extracting deployments for namespace {namespace} in cluster {cluster_name}: {str(e)}")
            return model_data

    @log_function_call
    def extract_services_for_namespace(self, cluster_name, namespace):
        """Extract services for a specific namespace in a specific cluster"""
        model_data = {}
        model_type = SourceModelType.EKS_SERVICE

        try:
            api_instance = self.eks_client.eks_get_api_instance(cluster_name, client='api')
            if not api_instance:
                return model_data

            services_response = api_instance.list_namespaced_service(namespace)

            for item in services_response.items:
                service_name = item.metadata.name
                if not service_name:
                    continue

                # Use region/cluster/namespace/service as the unique identifier
                namespaced_name = f"{self.__region}/{cluster_name}/{namespace}/{service_name}"

                # Convert to dict-like structure
                service_dict = {
                    'metadata': {
                        'name': service_name,
                        'namespace': namespace,
                        'uid': item.metadata.uid,
                        'creation_timestamp': item.metadata.creation_timestamp.isoformat() if item.metadata.creation_timestamp else None,
                        'labels': item.metadata.labels if item.metadata.labels else {}
                    },
                    'spec': {
                        'type': item.spec.type if item.spec else None,
                        'cluster_ip': item.spec.cluster_ip if item.spec else None
                    },
                    'eks_context': {
                        'region': self.__region,
                        'cluster': cluster_name
                    }
                }

                model_data[namespaced_name] = service_dict
            logger.info(f"Extracted {len(model_data)} services from namespace {namespace} in cluster {cluster_name}")

        except Exception as e:
            logger.error(f"Error extracting services for namespace {namespace} in cluster {cluster_name}: {str(e)}")
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_ingresses_for_namespace(self, cluster_name, namespace):
        """Extract ingresses for a specific namespace in a specific cluster"""
        model_data = {}
        model_type = SourceModelType.EKS_INGRESS

        try:
            networking_instance = self.eks_client.eks_get_api_instance(cluster_name, client='networking')
            if not networking_instance:
                return model_data

            ingresses_response = networking_instance.list_namespaced_ingress(namespace)

            for item in ingresses_response.items:
                ingress_name = item.metadata.name
                if not ingress_name:
                    continue

                # Use region/cluster/namespace/ingress as the unique identifier
                namespaced_name = f"{self.__region}/{cluster_name}/{namespace}/{ingress_name}"

                # Convert to dict-like structure
                ingress_dict = {
                    'metadata': {
                        'name': ingress_name,
                        'namespace': namespace,
                        'uid': item.metadata.uid,
                        'creation_timestamp': item.metadata.creation_timestamp.isoformat() if item.metadata.creation_timestamp else None,
                        'labels': item.metadata.labels if item.metadata.labels else {}
                    },
                    'eks_context': {
                        'region': self.__region,
                        'cluster': cluster_name
                    }
                }

                model_data[namespaced_name] = ingress_dict

            logger.info(f"Extracted {len(model_data)} ingresses from namespace {namespace} in cluster {cluster_name}")
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
            return model_data

        except Exception as e:
            logger.error(f"Error extracting ingresses for namespace {namespace} in cluster {cluster_name}: {str(e)}")
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_network_policies_for_namespace(self, cluster_name, namespace):
        """Extract network policies for a specific namespace in a specific cluster"""
        model_data = {}
        model_type = SourceModelType.EKS_NETWORK_POLICY

        try:
            networking_instance = self.eks_client.eks_get_api_instance(cluster_name, client='networking')
            if not networking_instance:
                return model_data

            policies_response = networking_instance.list_namespaced_network_policy(namespace)

            for item in policies_response.items:
                policy_name = item.metadata.name
                if not policy_name:
                    continue

                # Use region/cluster/namespace/policy as the unique identifier
                namespaced_name = f"{self.__region}/{cluster_name}/{namespace}/{policy_name}"

                # Convert to dict-like structure
                policy_dict = {
                    'metadata': {
                        'name': policy_name,
                        'namespace': namespace,
                        'uid': item.metadata.uid,
                        'creation_timestamp': item.metadata.creation_timestamp.isoformat() if item.metadata.creation_timestamp else None,
                        'labels': item.metadata.labels if item.metadata.labels else {}
                    },
                    'eks_context': {
                        'region': self.__region,
                        'cluster': cluster_name
                    }
                }

                model_data[namespaced_name] = policy_dict

            logger.info(f"Extracted {len(model_data)} network policies from namespace {namespace} in cluster {cluster_name}")
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
            return model_data

        except Exception as e:
            logger.error(f"Error extracting network policies for namespace {namespace} in cluster {cluster_name}: {str(e)}")
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_replicasets_for_namespace(self, cluster_name, namespace):
        """Extract replicasets for a specific namespace in a specific cluster"""
        model_data = {}
        model_type = SourceModelType.EKS_REPLICASET

        try:
            app_instance = self.eks_client.eks_get_api_instance(cluster_name, client='app')
            if not app_instance:
                return model_data

            replicasets_response = app_instance.list_namespaced_replica_set(namespace)

            for item in replicasets_response.items:
                rs_name = item.metadata.name
                if not rs_name:
                    continue

                # Use region/cluster/namespace/replicaset as the unique identifier
                namespaced_name = f"{self.__region}/{cluster_name}/{namespace}/{rs_name}"

                # Convert to dict-like structure
                rs_dict = {
                    'metadata': {
                        'name': rs_name,
                        'namespace': namespace,
                        'uid': item.metadata.uid,
                        'creation_timestamp': item.metadata.creation_timestamp.isoformat() if item.metadata.creation_timestamp else None,
                        'labels': item.metadata.labels if item.metadata.labels else {}
                    },
                    'spec': {
                        'replicas': item.spec.replicas if item.spec else None
                    },
                    'status': {
                        'available_replicas': item.status.available_replicas if item.status else None,
                        'ready_replicas': item.status.ready_replicas if item.status else None
                    },
                    'eks_context': {
                        'region': self.__region,
                        'cluster': cluster_name
                    }
                }

                model_data[namespaced_name] = rs_dict

            logger.info(f"Extracted {len(model_data)} replicasets from namespace {namespace} in cluster {cluster_name}")
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
            return model_data

        except Exception as e:
            logger.error(f"Error extracting replicasets for namespace {namespace} in cluster {cluster_name}: {str(e)}")
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_statefulsets_for_namespace(self, cluster_name, namespace):
        """Extract statefulsets for a specific namespace in a specific cluster"""
        model_data = {}
        model_type = SourceModelType.EKS_STATEFULSET

        try:
            app_instance = self.eks_client.eks_get_api_instance(cluster_name, client='app')
            if not app_instance:
                return model_data

            statefulsets_response = app_instance.list_namespaced_stateful_set(namespace)

            for item in statefulsets_response.items:
                ss_name = item.metadata.name
                if not ss_name:
                    continue

                # Use region/cluster/namespace/statefulset as the unique identifier
                namespaced_name = f"{self.__region}/{cluster_name}/{namespace}/{ss_name}"

                # Convert to dict-like structure
                ss_dict = {
                    'metadata': {
                        'name': ss_name,
                        'namespace': namespace,
                        'uid': item.metadata.uid,
                        'creation_timestamp': item.metadata.creation_timestamp.isoformat() if item.metadata.creation_timestamp else None,
                        'labels': item.metadata.labels if item.metadata.labels else {}
                    },
                    'spec': {
                        'replicas': item.spec.replicas if item.spec else None
                    },
                    'status': {
                        'ready_replicas': item.status.ready_replicas if item.status else None
                    },
                    'eks_context': {
                        'region': self.__region,
                        'cluster': cluster_name
                    }
                }

                model_data[namespaced_name] = ss_dict

            logger.info(f"Extracted {len(model_data)} statefulsets from namespace {namespace} in cluster {cluster_name}")

        except Exception as e:
            logger.error(f"Error extracting statefulsets for namespace {namespace} in cluster {cluster_name}: {str(e)}")

        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_pod_autoscalers_for_namespace(self, cluster_name, namespace):
        """Extract HPAs for a specific namespace in a specific cluster"""
        model_data = {}
        model_type = SourceModelType.EKS_HPA

        try:
            autoscaling_instance = self.eks_client.eks_get_api_instance(cluster_name, client='autoscaling')
            if not autoscaling_instance:
                return model_data

            hpas_response = autoscaling_instance.list_namespaced_horizontal_pod_autoscaler(namespace)

            for item in hpas_response.items:
                hpa_name = item.metadata.name
                if not hpa_name:
                    continue

                # Use region/cluster/namespace/hpa as the unique identifier
                namespaced_name = f"{self.__region}/{cluster_name}/{namespace}/{hpa_name}"

                # Convert to dict-like structure
                hpa_dict = {
                    'metadata': {
                        'name': hpa_name,
                        'namespace': namespace,
                        'uid': item.metadata.uid,
                        'creation_timestamp': item.metadata.creation_timestamp.isoformat() if item.metadata.creation_timestamp else None,
                        'labels': item.metadata.labels if item.metadata.labels else {}
                    },
                    'spec': {
                        'min_replicas': item.spec.min_replicas if item.spec else None,
                        'max_replicas': item.spec.max_replicas if item.spec else None
                    },
                    'eks_context': {
                        'region': self.__region,
                        'cluster': cluster_name
                    }
                }

                model_data[namespaced_name] = hpa_dict

            logger.info(f"Extracted {len(model_data)} HPAs from namespace {namespace} in cluster {cluster_name}")

        except Exception as e:
            logger.error(f"Error extracting HPAs for namespace {namespace} in cluster {cluster_name}: {str(e)}")

        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data
