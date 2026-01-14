import logging
from datetime import datetime

from core.integrations.source_metadata_extractor import SourceMetadataExtractor
from core.integrations.source_api_processors.azure_api_processor import AzureApiProcessor
from core.protos.base_pb2 import Source, SourceModelType
from core.utils.logging_utils import log_function_call

logger = logging.getLogger(__name__)


def _to_isoformat(ts):
    """Convert datetime to ISO string like EKS does."""
    if ts is None:
        return None
    if hasattr(ts, 'isoformat'):
        return ts.isoformat()
    return str(ts)


class AzureConnectorMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id: str, connector_name: str, subscription_id: str, tenant_id: str, client_id: str,
                 client_secret: str):
        self.__subscription_id = subscription_id
        self.__client = AzureApiProcessor(subscription_id, tenant_id, client_id, client_secret)
        super().__init__(request_id, connector_name, Source.AZURE)

    # ==================== Core Azure Resources ====================

    @log_function_call
    def extract_workspaces(self):
        """Extract Azure Log Analytics workspaces."""
        model_type = SourceModelType.AZURE_WORKSPACE
        model_data = {}
        try:
            workspaces = self.__client.fetch_workspaces()
            if not workspaces:
                return model_data
            for w in workspaces:
                workspace_id = w.get('customer_id', '')
                model_data[workspace_id] = w
        except Exception as e:
            logger.error(f'Error fetching workspaces: {e}')

        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_resources(self):
        """Extract generic Azure resources with metrics."""
        model_type = SourceModelType.AZURE_RESOURCE
        model_data = {}
        try:
            resources = self.__client.fetch_resources()
            if not resources:
                return model_data
            for r in resources:
                name = r.get('name', '')
                model_data[name] = r
        except Exception as e:
            logger.error(f'Error fetching azure resources: {e}')

        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_resource_groups(self):
        """Extract Azure resource groups."""
        model_type = SourceModelType.AZURE_RESOURCE_GROUP
        model_data = {}
        try:
            groups = self.__client.list_resource_groups()
            if not groups:
                return model_data
            for rg in groups:
                rg_name = rg.get('name', '')
                model_data[rg_name] = {
                    'name': rg_name,
                    'location': rg.get('location'),
                    'tags': rg.get('tags', {}),
                    'provisioning_state': rg.get('properties', {}).get('provisioning_state'),
                    'subscription_id': self.__subscription_id
                }
        except Exception as e:
            logger.error(f'Error extracting resource groups: {e}')

        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    # ==================== AKS (Azure Kubernetes Service) ====================

    def _extract_resource_group_from_id(self, resource_id: str) -> str:
        """Extract resource group name from Azure resource ID."""
        if not resource_id:
            return ''
        parts = resource_id.split('/')
        if len(parts) > 4:
            return parts[4]
        return ''

    @log_function_call
    def extract_aks_clusters(self):
        """Extract all AKS clusters."""
        model_type = SourceModelType.AZURE_AKS_CLUSTER
        model_data = {}
        try:
            clusters = self.__client.aks_list_clusters()
            if not clusters:
                return model_data
            for cluster in clusters:
                cluster_name = cluster.get('name', '')
                resource_group = self._extract_resource_group_from_id(cluster.get('id', ''))

                model_data[cluster_name] = {
                    'name': cluster_name,
                    'resource_group': resource_group,
                    'location': cluster.get('location'),
                    'kubernetes_version': cluster.get('kubernetes_version'),
                    'provisioning_state': cluster.get('provisioning_state'),
                    'fqdn': cluster.get('fqdn'),
                    'agent_pool_profiles': cluster.get('agent_pool_profiles', []),
                    'network_profile': cluster.get('network_profile', {}),
                    'tags': cluster.get('tags', {}),
                    'subscription_id': self.__subscription_id,
                    'azure_context': {
                        'subscription_id': self.__subscription_id,
                        'resource_group': resource_group
                    }
                }
        except Exception as e:
            logger.error(f'Error extracting AKS clusters: {e}')

        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_aks_namespaces(self):
        """Extract namespaces from all AKS clusters."""
        model_type = SourceModelType.AZURE_AKS_NAMESPACE
        model_data = {}
        try:
            clusters = self.__client.aks_list_clusters()
            if not clusters:
                return model_data

            for cluster in clusters:
                cluster_name = cluster.get('name', '')
                resource_group = self._extract_resource_group_from_id(cluster.get('id', ''))

                try:
                    namespaces = self.__client.aks_list_namespaces(resource_group, cluster_name)
                    if not namespaces:
                        continue

                    for ns in namespaces:
                        metadata = ns.get('metadata', {})
                        ns_name = metadata.get('name', '')

                        model_data[f"{cluster_name}/{ns_name}"] = {
                            'metadata': {
                                'name': ns_name,
                                'uid': metadata.get('uid'),
                                'creation_timestamp': _to_isoformat(metadata.get('creation_timestamp'))
                            },
                            'status': ns.get('status', {}),
                            'aks_context': {
                                'subscription_id': self.__subscription_id,
                                'resource_group': resource_group,
                                'cluster': cluster_name
                            }
                        }
                except Exception as e:
                    logger.error(f"Error extracting namespaces from cluster {cluster_name}: {e}")
                    continue
        except Exception as e:
            logger.error(f'Error extracting AKS namespaces: {e}')

        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_aks_deployments(self):
        """Extract deployments from all AKS clusters."""
        model_type = SourceModelType.AZURE_AKS_DEPLOYMENT
        model_data = {}
        try:
            clusters = self.__client.aks_list_clusters()
            if not clusters:
                return model_data

            for cluster in clusters:
                cluster_name = cluster.get('name', '')
                resource_group = self._extract_resource_group_from_id(cluster.get('id', ''))

                try:
                    deployments = self.__client.aks_list_deployments(resource_group, cluster_name)
                    if not deployments:
                        continue

                    for dep in deployments:
                        metadata = dep.get('metadata', {})
                        dep_name = metadata.get('name', '')
                        namespace = metadata.get('namespace', '')

                        spec = dep.get('spec', {})
                        status = dep.get('status', {})

                        model_data[f"{cluster_name}/{namespace}/{dep_name}"] = {
                            'metadata': {
                                'name': dep_name,
                                'namespace': namespace,
                                'uid': metadata.get('uid'),
                                'creation_timestamp': _to_isoformat(metadata.get('creation_timestamp')),
                                'labels': metadata.get('labels', {})
                            },
                            'spec': {
                                'replicas': spec.get('replicas')
                            },
                            'status': {
                                'available_replicas': status.get('available_replicas'),
                                'ready_replicas': status.get('ready_replicas'),
                                'replicas': status.get('replicas')
                            },
                            'aks_context': {
                                'subscription_id': self.__subscription_id,
                                'resource_group': resource_group,
                                'cluster': cluster_name
                            }
                        }
                except Exception as e:
                    logger.error(f"Error extracting deployments from cluster {cluster_name}: {e}")
                    continue
        except Exception as e:
            logger.error(f'Error extracting AKS deployments: {e}')

        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_aks_services(self):
        """Extract services from all AKS clusters."""
        model_type = SourceModelType.AZURE_AKS_SERVICE
        model_data = {}
        try:
            clusters = self.__client.aks_list_clusters()
            if not clusters:
                return model_data

            for cluster in clusters:
                cluster_name = cluster.get('name', '')
                resource_group = self._extract_resource_group_from_id(cluster.get('id', ''))

                try:
                    services = self.__client.aks_list_services(resource_group, cluster_name)
                    if not services:
                        continue

                    for svc in services:
                        metadata = svc.get('metadata', {})
                        svc_name = metadata.get('name', '')
                        namespace = metadata.get('namespace', '')

                        spec = svc.get('spec', {})

                        model_data[f"{cluster_name}/{namespace}/{svc_name}"] = {
                            'metadata': {
                                'name': svc_name,
                                'namespace': namespace,
                                'uid': metadata.get('uid'),
                                'creation_timestamp': _to_isoformat(metadata.get('creation_timestamp')),
                                'labels': metadata.get('labels', {})
                            },
                            'spec': {
                                'type': spec.get('type'),
                                'cluster_ip': spec.get('cluster_ip'),
                                'ports': spec.get('ports', []),
                                'selector': spec.get('selector', {})
                            },
                            'aks_context': {
                                'subscription_id': self.__subscription_id,
                                'resource_group': resource_group,
                                'cluster': cluster_name
                            }
                        }
                except Exception as e:
                    logger.error(f"Error extracting services from cluster {cluster_name}: {e}")
                    continue
        except Exception as e:
            logger.error(f'Error extracting AKS services: {e}')

        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_aks_ingresses(self):
        """Extract ingresses from all AKS clusters."""
        model_type = SourceModelType.AZURE_AKS_INGRESS
        model_data = {}
        try:
            clusters = self.__client.aks_list_clusters()
            if not clusters:
                return model_data

            for cluster in clusters:
                cluster_name = cluster.get('name', '')
                resource_group = self._extract_resource_group_from_id(cluster.get('id', ''))

                try:
                    ingresses = self.__client.aks_list_ingresses(resource_group, cluster_name)
                    if not ingresses:
                        continue

                    for ing in ingresses:
                        metadata = ing.get('metadata', {})
                        ing_name = metadata.get('name', '')
                        namespace = metadata.get('namespace', '')

                        spec = ing.get('spec', {})

                        model_data[f"{cluster_name}/{namespace}/{ing_name}"] = {
                            'metadata': {
                                'name': ing_name,
                                'namespace': namespace,
                                'uid': metadata.get('uid'),
                                'creation_timestamp': _to_isoformat(metadata.get('creation_timestamp')),
                                'labels': metadata.get('labels', {})
                            },
                            'spec': {
                                'ingress_class_name': spec.get('ingress_class_name'),
                                'rules': spec.get('rules', []),
                                'tls': spec.get('tls', [])
                            },
                            'aks_context': {
                                'subscription_id': self.__subscription_id,
                                'resource_group': resource_group,
                                'cluster': cluster_name
                            }
                        }
                except Exception as e:
                    logger.error(f"Error extracting ingresses from cluster {cluster_name}: {e}")
                    continue
        except Exception as e:
            logger.error(f'Error extracting AKS ingresses: {e}')

        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_aks_hpas(self):
        """Extract horizontal pod autoscalers from all AKS clusters."""
        model_type = SourceModelType.AZURE_AKS_HPA
        model_data = {}
        try:
            clusters = self.__client.aks_list_clusters()
            if not clusters:
                return model_data

            for cluster in clusters:
                cluster_name = cluster.get('name', '')
                resource_group = self._extract_resource_group_from_id(cluster.get('id', ''))

                try:
                    hpas = self.__client.aks_list_hpas(resource_group, cluster_name)
                    if not hpas:
                        continue

                    for hpa in hpas:
                        metadata = hpa.get('metadata', {})
                        hpa_name = metadata.get('name', '')
                        namespace = metadata.get('namespace', '')

                        spec = hpa.get('spec', {})
                        status = hpa.get('status', {})

                        model_data[f"{cluster_name}/{namespace}/{hpa_name}"] = {
                            'metadata': {
                                'name': hpa_name,
                                'namespace': namespace,
                                'uid': metadata.get('uid'),
                                'creation_timestamp': _to_isoformat(metadata.get('creation_timestamp'))
                            },
                            'spec': {
                                'min_replicas': spec.get('min_replicas'),
                                'max_replicas': spec.get('max_replicas'),
                                'target_cpu_utilization_percentage': spec.get('target_cpu_utilization_percentage'),
                                'scale_target_ref': spec.get('scale_target_ref', {})
                            },
                            'status': {
                                'current_replicas': status.get('current_replicas'),
                                'desired_replicas': status.get('desired_replicas'),
                                'current_cpu_utilization_percentage': status.get('current_cpu_utilization_percentage')
                            },
                            'aks_context': {
                                'subscription_id': self.__subscription_id,
                                'resource_group': resource_group,
                                'cluster': cluster_name
                            }
                        }
                except Exception as e:
                    logger.error(f"Error extracting HPAs from cluster {cluster_name}: {e}")
                    continue
        except Exception as e:
            logger.error(f'Error extracting AKS HPAs: {e}')

        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_aks_statefulsets(self):
        """Extract stateful sets from all AKS clusters."""
        model_type = SourceModelType.AZURE_AKS_STATEFULSET
        model_data = {}
        try:
            clusters = self.__client.aks_list_clusters()
            if not clusters:
                return model_data

            for cluster in clusters:
                cluster_name = cluster.get('name', '')
                resource_group = self._extract_resource_group_from_id(cluster.get('id', ''))

                try:
                    statefulsets = self.__client.aks_list_statefulsets(resource_group, cluster_name)
                    if not statefulsets:
                        continue

                    for ss in statefulsets:
                        metadata = ss.get('metadata', {})
                        ss_name = metadata.get('name', '')
                        namespace = metadata.get('namespace', '')

                        spec = ss.get('spec', {})
                        status = ss.get('status', {})

                        model_data[f"{cluster_name}/{namespace}/{ss_name}"] = {
                            'metadata': {
                                'name': ss_name,
                                'namespace': namespace,
                                'uid': metadata.get('uid'),
                                'creation_timestamp': _to_isoformat(metadata.get('creation_timestamp')),
                                'labels': metadata.get('labels', {})
                            },
                            'spec': {
                                'replicas': spec.get('replicas'),
                                'service_name': spec.get('service_name')
                            },
                            'status': {
                                'replicas': status.get('replicas'),
                                'ready_replicas': status.get('ready_replicas'),
                                'current_replicas': status.get('current_replicas')
                            },
                            'aks_context': {
                                'subscription_id': self.__subscription_id,
                                'resource_group': resource_group,
                                'cluster': cluster_name
                            }
                        }
                except Exception as e:
                    logger.error(f"Error extracting StatefulSets from cluster {cluster_name}: {e}")
                    continue
        except Exception as e:
            logger.error(f'Error extracting AKS StatefulSets: {e}')

        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_aks_replicasets(self):
        """Extract replica sets from all AKS clusters."""
        model_type = SourceModelType.AZURE_AKS_REPLICASET
        model_data = {}
        try:
            clusters = self.__client.aks_list_clusters()
            if not clusters:
                return model_data

            for cluster in clusters:
                cluster_name = cluster.get('name', '')
                resource_group = self._extract_resource_group_from_id(cluster.get('id', ''))

                try:
                    replicasets = self.__client.aks_list_replicasets(resource_group, cluster_name)
                    if not replicasets:
                        continue

                    for rs in replicasets:
                        metadata = rs.get('metadata', {})
                        rs_name = metadata.get('name', '')
                        namespace = metadata.get('namespace', '')

                        spec = rs.get('spec', {})
                        status = rs.get('status', {})

                        model_data[f"{cluster_name}/{namespace}/{rs_name}"] = {
                            'metadata': {
                                'name': rs_name,
                                'namespace': namespace,
                                'uid': metadata.get('uid'),
                                'creation_timestamp': _to_isoformat(metadata.get('creation_timestamp')),
                                'labels': metadata.get('labels', {}),
                                'owner_references': metadata.get('owner_references', [])
                            },
                            'spec': {
                                'replicas': spec.get('replicas')
                            },
                            'status': {
                                'replicas': status.get('replicas'),
                                'ready_replicas': status.get('ready_replicas'),
                                'available_replicas': status.get('available_replicas')
                            },
                            'aks_context': {
                                'subscription_id': self.__subscription_id,
                                'resource_group': resource_group,
                                'cluster': cluster_name
                            }
                        }
                except Exception as e:
                    logger.error(f"Error extracting ReplicaSets from cluster {cluster_name}: {e}")
                    continue
        except Exception as e:
            logger.error(f'Error extracting AKS ReplicaSets: {e}')

        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_aks_network_policies(self):
        """Extract network policies from all AKS clusters."""
        model_type = SourceModelType.AZURE_AKS_NETWORK_POLICY
        model_data = {}
        try:
            clusters = self.__client.aks_list_clusters()
            if not clusters:
                return model_data

            for cluster in clusters:
                cluster_name = cluster.get('name', '')
                resource_group = self._extract_resource_group_from_id(cluster.get('id', ''))

                try:
                    policies = self.__client.aks_list_network_policies(resource_group, cluster_name)
                    if not policies:
                        continue

                    for policy in policies:
                        metadata = policy.get('metadata', {})
                        policy_name = metadata.get('name', '')
                        namespace = metadata.get('namespace', '')

                        spec = policy.get('spec', {})

                        model_data[f"{cluster_name}/{namespace}/{policy_name}"] = {
                            'metadata': {
                                'name': policy_name,
                                'namespace': namespace,
                                'uid': metadata.get('uid'),
                                'creation_timestamp': _to_isoformat(metadata.get('creation_timestamp'))
                            },
                            'spec': {
                                'pod_selector': spec.get('pod_selector', {}),
                                'ingress': spec.get('ingress', []),
                                'egress': spec.get('egress', []),
                                'policy_types': spec.get('policy_types', [])
                            },
                            'aks_context': {
                                'subscription_id': self.__subscription_id,
                                'resource_group': resource_group,
                                'cluster': cluster_name
                            }
                        }
                except Exception as e:
                    logger.error(f"Error extracting NetworkPolicies from cluster {cluster_name}: {e}")
                    continue
        except Exception as e:
            logger.error(f'Error extracting AKS NetworkPolicies: {e}')

        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    # ==================== Azure Compute (VMs) ====================

    @log_function_call
    def extract_virtual_machines(self):
        """Extract Azure Virtual Machines."""
        model_type = SourceModelType.AZURE_VIRTUAL_MACHINE
        model_data = {}
        try:
            vms = self.__client.compute_list_vms()
            if not vms:
                return model_data
            for vm in vms:
                vm_name = vm.get('name', '')
                resource_group = self._extract_resource_group_from_id(vm.get('id', ''))

                hardware_profile = vm.get('hardware_profile', {})
                storage_profile = vm.get('storage_profile', {})
                os_disk = storage_profile.get('os_disk', {})
                network_profile = vm.get('network_profile', {})

                model_data[vm_name] = {
                    'name': vm_name,
                    'resource_group': resource_group,
                    'location': vm.get('location'),
                    'vm_size': hardware_profile.get('vm_size'),
                    'os_type': os_disk.get('os_type'),
                    'provisioning_state': vm.get('provisioning_state'),
                    'network_interfaces': network_profile.get('network_interfaces', []),
                    'tags': vm.get('tags', {}),
                    'subscription_id': self.__subscription_id
                }
        except Exception as e:
            logger.error(f'Error extracting virtual machines: {e}')

        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_vmss(self):
        """Extract Azure VM Scale Sets."""
        model_type = SourceModelType.AZURE_VMSS
        model_data = {}
        try:
            vmss_list = self.__client.compute_list_vmss()
            if not vmss_list:
                return model_data
            for vmss in vmss_list:
                vmss_name = vmss.get('name', '')
                resource_group = self._extract_resource_group_from_id(vmss.get('id', ''))

                sku = vmss.get('sku', {})

                model_data[vmss_name] = {
                    'name': vmss_name,
                    'resource_group': resource_group,
                    'location': vmss.get('location'),
                    'sku_name': sku.get('name'),
                    'sku_tier': sku.get('tier'),
                    'capacity': sku.get('capacity'),
                    'provisioning_state': vmss.get('provisioning_state'),
                    'tags': vmss.get('tags', {}),
                    'subscription_id': self.__subscription_id
                }
        except Exception as e:
            logger.error(f'Error extracting VM scale sets: {e}')

        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    # ==================== Azure Storage ====================

    @log_function_call
    def extract_storage_accounts(self):
        """Extract Azure Storage Accounts."""
        model_type = SourceModelType.AZURE_STORAGE_ACCOUNT
        model_data = {}
        try:
            accounts = self.__client.storage_list_accounts()
            if not accounts:
                return model_data
            for account in accounts:
                account_name = account.get('name', '')
                resource_group = self._extract_resource_group_from_id(account.get('id', ''))

                sku = account.get('sku', {})
                primary_endpoints = account.get('primary_endpoints', {})

                model_data[account_name] = {
                    'name': account_name,
                    'resource_group': resource_group,
                    'location': account.get('location'),
                    'kind': account.get('kind'),
                    'sku_name': sku.get('name'),
                    'sku_tier': sku.get('tier'),
                    'access_tier': account.get('access_tier'),
                    'provisioning_state': account.get('provisioning_state'),
                    'primary_endpoints': {
                        'blob': primary_endpoints.get('blob'),
                        'file': primary_endpoints.get('file'),
                        'queue': primary_endpoints.get('queue'),
                        'table': primary_endpoints.get('table')
                    },
                    'tags': account.get('tags', {}),
                    'subscription_id': self.__subscription_id
                }
        except Exception as e:
            logger.error(f'Error extracting storage accounts: {e}')

        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_blob_containers(self):
        """Extract Azure Blob Containers from all storage accounts."""
        model_type = SourceModelType.AZURE_BLOB_CONTAINER
        model_data = {}
        try:
            # First get all storage accounts
            accounts = self.__client.storage_list_accounts()
            if not accounts:
                return model_data

            for account in accounts:
                account_name = account.get('name', '')
                resource_group = self._extract_resource_group_from_id(account.get('id', ''))

                try:
                    containers = self.__client.storage_list_blob_containers(resource_group, account_name)
                    if not containers:
                        continue

                    for container in containers:
                        container_name = container.get('name', '')

                        model_data[f"{account_name}/{container_name}"] = {
                            'name': container_name,
                            'storage_account': account_name,
                            'resource_group': resource_group,
                            'public_access': container.get('public_access'),
                            'lease_status': container.get('lease_status'),
                            'lease_state': container.get('lease_state'),
                            'last_modified': str(container.get('last_modified')) if container.get('last_modified') else None,
                            'etag': container.get('etag'),
                            'has_immutability_policy': container.get('has_immutability_policy'),
                            'has_legal_hold': container.get('has_legal_hold'),
                            'subscription_id': self.__subscription_id
                        }
                except Exception as e:
                    logger.error(f'Error extracting blob containers for account {account_name}: {e}')
                    continue

        except Exception as e:
            logger.error(f'Error extracting blob containers: {e}')

        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    # ==================== Azure SQL Database ====================

    @log_function_call
    def extract_sql_servers(self):
        """Extract Azure SQL Servers."""
        model_type = SourceModelType.AZURE_SQL_SERVER
        model_data = {}
        try:
            servers = self.__client.sql_list_servers()
            if not servers:
                return model_data
            for server in servers:
                server_name = server.get('name', '')
                resource_group = self._extract_resource_group_from_id(server.get('id', ''))

                model_data[server_name] = {
                    'name': server_name,
                    'resource_group': resource_group,
                    'location': server.get('location'),
                    'fully_qualified_domain_name': server.get('fully_qualified_domain_name'),
                    'administrator_login': server.get('administrator_login'),
                    'state': server.get('state'),
                    'version': server.get('version'),
                    'tags': server.get('tags', {}),
                    'subscription_id': self.__subscription_id
                }
        except Exception as e:
            logger.error(f'Error extracting SQL servers: {e}')

        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_sql_databases(self):
        """Extract Azure SQL Databases from all SQL servers."""
        model_type = SourceModelType.AZURE_SQL_DATABASE
        model_data = {}
        try:
            servers = self.__client.sql_list_servers()
            if not servers:
                return model_data

            for server in servers:
                server_name = server.get('name', '')
                resource_group = self._extract_resource_group_from_id(server.get('id', ''))

                try:
                    databases = self.__client.sql_list_databases(resource_group, server_name)
                    if not databases:
                        continue

                    for db in databases:
                        db_name = db.get('name', '')

                        sku = db.get('sku', {})

                        model_data[f"{server_name}/{db_name}"] = {
                            'name': db_name,
                            'server_name': server_name,
                            'resource_group': resource_group,
                            'location': db.get('location'),
                            'sku_name': sku.get('name'),
                            'sku_tier': sku.get('tier'),
                            'status': db.get('status'),
                            'max_size_bytes': db.get('max_size_bytes'),
                            'collation': db.get('collation'),
                            'tags': db.get('tags', {}),
                            'subscription_id': self.__subscription_id
                        }
                except Exception as e:
                    logger.error(f"Error extracting databases from SQL server {server_name}: {e}")
                    continue
        except Exception as e:
            logger.error(f'Error extracting SQL databases: {e}')

        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    # ==================== Azure Cosmos DB ====================

    @log_function_call
    def extract_cosmos_accounts(self):
        """Extract Azure Cosmos DB Accounts."""
        model_type = SourceModelType.AZURE_COSMOS_ACCOUNT
        model_data = {}
        try:
            accounts = self.__client.cosmos_list_accounts()
            if not accounts:
                return model_data
            for account in accounts:
                account_name = account.get('name', '')
                resource_group = self._extract_resource_group_from_id(account.get('id', ''))

                model_data[account_name] = {
                    'name': account_name,
                    'resource_group': resource_group,
                    'location': account.get('location'),
                    'kind': account.get('kind'),
                    'document_endpoint': account.get('document_endpoint'),
                    'provisioning_state': account.get('provisioning_state'),
                    'consistency_policy': account.get('consistency_policy', {}),
                    'capabilities': account.get('capabilities', []),
                    'tags': account.get('tags', {}),
                    'subscription_id': self.__subscription_id
                }
        except Exception as e:
            logger.error(f'Error extracting Cosmos DB accounts: {e}')

        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    # ==================== Azure Monitor ====================

    @log_function_call
    def extract_metric_alerts(self):
        """Extract Azure Monitor metric alerts."""
        model_type = SourceModelType.AZURE_METRIC_ALERT
        model_data = {}
        try:
            alerts = self.__client.monitor_list_metric_alerts()
            if not alerts:
                return model_data
            for alert in alerts:
                alert_name = alert.get('name', '')
                resource_group = self._extract_resource_group_from_id(alert.get('id', ''))

                model_data[alert_name] = {
                    'name': alert_name,
                    'resource_group': resource_group,
                    'location': alert.get('location'),
                    'description': alert.get('description'),
                    'severity': alert.get('severity'),
                    'enabled': alert.get('enabled'),
                    'scopes': alert.get('scopes', []),
                    'criteria': alert.get('criteria', {}),
                    'actions': alert.get('actions', []),
                    'tags': alert.get('tags', {}),
                    'subscription_id': self.__subscription_id
                }
        except Exception as e:
            logger.error(f'Error extracting metric alerts: {e}')

        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_action_groups(self):
        """Extract Azure Monitor action groups."""
        model_type = SourceModelType.AZURE_ACTION_GROUP
        model_data = {}
        try:
            groups = self.__client.monitor_list_action_groups()
            if not groups:
                return model_data
            for group in groups:
                group_name = group.get('name', '')
                resource_group = self._extract_resource_group_from_id(group.get('id', ''))

                model_data[group_name] = {
                    'name': group_name,
                    'resource_group': resource_group,
                    'location': group.get('location'),
                    'group_short_name': group.get('group_short_name'),
                    'enabled': group.get('enabled'),
                    'email_receivers': group.get('email_receivers', []),
                    'sms_receivers': group.get('sms_receivers', []),
                    'webhook_receivers': group.get('webhook_receivers', []),
                    'tags': group.get('tags', {}),
                    'subscription_id': self.__subscription_id
                }
        except Exception as e:
            logger.error(f'Error extracting action groups: {e}')

        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    # ==================== PostgreSQL Flexible Server ====================

    @log_function_call
    def extract_postgres_flexible_servers(self):
        """Extract Azure PostgreSQL Flexible Servers."""
        model_type = SourceModelType.AZURE_POSTGRES_SERVER
        model_data = {}
        try:
            servers = self.__client.postgres_flexible_list_servers()
            if not servers:
                return model_data
            for server in servers:
                server_name = server.get('name', '')
                resource_group = self._extract_resource_group_from_id(server.get('id', ''))

                sku = server.get('sku', {})
                storage = server.get('storage', {})

                model_data[server_name] = {
                    'name': server_name,
                    'resource_group': resource_group,
                    'location': server.get('location'),
                    'fully_qualified_domain_name': server.get('fully_qualified_domain_name'),
                    'administrator_login': server.get('administrator_login'),
                    'state': server.get('state'),
                    'version': server.get('version'),
                    'sku_name': sku.get('name'),
                    'sku_tier': sku.get('tier'),
                    'storage_size_gb': storage.get('storage_size_gb'),
                    'tags': server.get('tags', {}),
                    'subscription_id': self.__subscription_id
                }
        except Exception as e:
            logger.error(f'Error extracting PostgreSQL Flexible Servers: {e}')

        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_postgres_flexible_databases(self):
        """Extract databases from all PostgreSQL Flexible Servers."""
        model_type = SourceModelType.AZURE_POSTGRES_DATABASE
        model_data = {}
        try:
            servers = self.__client.postgres_flexible_list_servers()
            if not servers:
                return model_data

            for server in servers:
                server_name = server.get('name', '')
                resource_group = self._extract_resource_group_from_id(server.get('id', ''))

                try:
                    databases = self.__client.postgres_flexible_list_databases(resource_group, server_name)
                    if not databases:
                        continue

                    for db in databases:
                        db_name = db.get('name', '')

                        model_data[f"{server_name}/{db_name}"] = {
                            'name': db_name,
                            'server_name': server_name,
                            'resource_group': resource_group,
                            'charset': db.get('charset'),
                            'collation': db.get('collation'),
                            'subscription_id': self.__subscription_id
                        }
                except Exception as e:
                    logger.error(f"Error extracting databases from PostgreSQL server {server_name}: {e}")
                    continue
        except Exception as e:
            logger.error(f'Error extracting PostgreSQL databases: {e}')

        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    # ==================== Redis Cache ====================

    @log_function_call
    def extract_redis_caches(self):
        """Extract Azure Redis Caches."""
        model_type = SourceModelType.AZURE_REDIS_CACHE
        model_data = {}
        try:
            caches = self.__client.redis_list_caches()
            if not caches:
                return model_data
            for cache in caches:
                cache_name = cache.get('name', '')
                resource_group = self._extract_resource_group_from_id(cache.get('id', ''))

                sku = cache.get('sku', {})

                model_data[cache_name] = {
                    'name': cache_name,
                    'resource_group': resource_group,
                    'location': cache.get('location'),
                    'host_name': cache.get('host_name'),
                    'port': cache.get('port'),
                    'ssl_port': cache.get('ssl_port'),
                    'sku_name': sku.get('name'),
                    'sku_family': sku.get('family'),
                    'sku_capacity': sku.get('capacity'),
                    'redis_version': cache.get('redis_version'),
                    'provisioning_state': cache.get('provisioning_state'),
                    'enable_non_ssl_port': cache.get('enable_non_ssl_port'),
                    'tags': cache.get('tags', {}),
                    'subscription_id': self.__subscription_id
                }
        except Exception as e:
            logger.error(f'Error extracting Redis Caches: {e}')

        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data
