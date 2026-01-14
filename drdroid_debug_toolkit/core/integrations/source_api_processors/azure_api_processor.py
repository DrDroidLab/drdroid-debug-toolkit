import logging
import os
import tempfile
import base64
from datetime import timedelta, datetime, timezone

from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.mgmt.loganalytics import LogAnalyticsManagementClient
from azure.monitor.query import LogsQueryClient, MetricsQueryClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.containerservice import ContainerServiceClient
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.sql import SqlManagementClient
from azure.mgmt.cosmosdb import CosmosDBManagementClient
from azure.mgmt.monitor import MonitorManagementClient
from azure.mgmt.rdbms.postgresql_flexibleservers import PostgreSQLManagementClient
from azure.mgmt.redis import RedisManagementClient

from core.integrations.processor import Processor

logger = logging.getLogger(__name__)

# Optional kubernetes import - only needed for AKS K8s API calls
try:
    from kubernetes import client as k8s_client, config as k8s_config
    K8S_AVAILABLE = True
except ImportError:
    K8S_AVAILABLE = False
    logger.warning("kubernetes package not installed - AKS K8s API methods will not be available")


def query_response_to_dict(response):
    """
    Function to convert query response tables to dictionaries.
    """
    result = {}
    for table in response.tables:
        result[table.name] = []
        for row in table.rows:
            row_dict = {}
            for i, column in enumerate(table.columns):
                row_dict[column] = str(row[i])
            result[table.name].append(row_dict)
    return result


class AzureApiProcessor(Processor):
    client = None

    def __init__(self, subscription_id, tenant_id, client_id, client_secret):
        self.__subscription_id = subscription_id
        self.__client_id = client_id
        self.__client_secret = client_secret
        self.__tenant_id = tenant_id

        if not self.__client_id or not self.__client_secret or not self.__tenant_id:
            raise Exception("Azure Connection Error:: Missing client_id, client_secret, or tenant_id")

    def get_credentials(self):
        """
        Function to fetch Azure credentials using client_id, client_secret, and tenant_id.
        """
        os.environ['AZURE_CLIENT_ID'] = self.__client_id
        os.environ['AZURE_CLIENT_SECRET'] = self.__client_secret
        os.environ['AZURE_TENANT_ID'] = self.__tenant_id
        credential = DefaultAzureCredential()
        return credential 

    def test_connection(self):
        try:
            credentials = self.get_credentials()
            if not credentials:
                logger.error("Azure Connection Error:: Failed to get credentials")
                raise Exception("Azure Connection Error:: Failed to get credentials")
            log_analytics_client = LogAnalyticsManagementClient(credentials, self.__subscription_id)
            workspaces = log_analytics_client.workspaces.list()
            if not workspaces:
                raise Exception("Azure Connection Error:: No Workspaces Found")
            az_workspaces = []
            for workspace in workspaces:
                az_workspaces.append(workspace.as_dict())
            if len(az_workspaces) > 0:
                return True
            else:
                raise Exception("Azure Connection Error:: No Workspaces Found")
        except Exception as e:
            raise e

    def fetch_workspaces(self):
        try:
            credentials = self.get_credentials()
            if not credentials:
                logger.error("Azure Connection Error:: Failed to get credentials")
                return None
            log_analytics_client = LogAnalyticsManagementClient(credentials, self.__subscription_id)
            workspaces = log_analytics_client.workspaces.list()
            if not workspaces:
                logger.error("Azure Connection Error:: No Workspaces Found")
                return None
            return [workspace.as_dict() for workspace in workspaces]
        except Exception as e:
            logger.error(f"Failed to fetch workspaces with error: {e}")
            return None

    def fetch_resources(self):
        try:
            credentials = self.get_credentials()
            if not credentials:
                logger.error("Azure Connection Error:: Failed to get credentials")
                return None
            resource_client = ResourceManagementClient(credentials, self.__subscription_id)
            metrics_client = MetricsQueryClient(credentials)
            resources = resource_client.resources.list()
            if not resources:
                logger.error("Azure Connection Error:: No Resources Found")
                return None
            
            # Only include resources that actually produce metrics
            metric_producing_types = [
                "Microsoft.Compute/virtualMachines",
                "Microsoft.ContainerService/managedClusters",
                "Microsoft.Network/loadBalancers",
                "Microsoft.Network/applicationGateways",
                "Microsoft.Storage/storageAccounts",
                "Microsoft.Sql/servers/databases",
                "Microsoft.Web/sites",
                "Microsoft.Insights/components"
            ]
            # valid_resources = [
            #     resource.as_dict() for resource in resources
            #     if resource.type in metric_producing_types
            # ]
            valid_resources = []
            for resource in resources:
                if resource.type in metric_producing_types:
                    resource_dict = resource.as_dict()

                    # Get resource metrics
                    resource_id = resource.id
                    try:
                        metric_names = metrics_client.list_metric_definitions(resource_id)
                        metric_names = [metric.name for metric in metric_names]
                    except Exception as e:
                        logger.error(f"Failed to fetch metrics for resource {resource_id} with error: {e}")
                        metric_names = []

                    resource_dict["available_metrics"] = {"metric_names": metric_names}
                    valid_resources.append(resource_dict)
            return valid_resources
        except Exception as e:
            logger.error(f"Failed to fetch resources with error: {e}")
            return None

    def query_log_analytics(self, workspace_id, query, timespan=timedelta(hours=4)):
        try:
            credentials = self.get_credentials()
            if not credentials:
                logger.error("Azure Connection Error:: Failed to get credentials")
                return None
            client = LogsQueryClient(credentials)
            response = client.query_workspace(workspace_id, query, timespan=timespan)
            if not response:
                logger.error(f"Failed to query log analytics with error: {response.text}")
                return None
            results = query_response_to_dict(response)
            return results
        except Exception as e:
            logger.error(f"Failed to query log analytics with error: {e}")
            return None


    # Vidushee was here
    def query_metrics(self, resource_id, time_range, metric_names="Percentage CPU", aggregation="Average", granularity=300): # placeholer metric name
        """
        Function to fetch metrics from Azure Monitor.
        
        Parameters:
            resource_id (str): The Azure resource ID to fetch metrics for.
            metric_names (str): The names of the metric to retrieve.
            timespan (timedelta): The time range for fetching metrics.
            aggregation (str): The type of aggregation (e.g., 'Average', 'Total', 'Count', etc.).
        
        Returns:
            dict: A dictionary containing the retrieved metrics.
        """
        try:
            credentials = self.get_credentials()
            if not credentials:
                logger.error("Azure Connection Error:: Failed to get credentials")
                return None
    
            # Create Metrics Query Client
            client = MetricsQueryClient(credentials)
            # Convert Unix timestamps (in seconds) to datetime
            from_tr = datetime.fromtimestamp(time_range.time_geq, tz=timezone.utc)  # Convert start time to datetime
            to_tr = datetime.fromtimestamp(time_range.time_lt, tz=timezone.utc)  # Convert end time to datetime

            # total_seconds = (to_tr - from_tr).total_seconds()
            # Compute granularity dynamically (max 12 points)
            # granularity_seconds = total_seconds / 12  # Each interval should be total duration / 12
            # Convert to ISO 8601 duration string
            # if granularity_seconds < 60:
            #     granularity = f"PT{int(granularity_seconds)}S"  # Seconds
            # elif granularity_seconds < 3600:
            #     granularity = f"PT{int(granularity_seconds / 60)}M"  # Minutes
            # else:
            #     granularity = f"PT{int(granularity_seconds / 3600)}H"  # Hours

            # Ensure metric_names is always a list
            if isinstance(metric_names, str):
                metric_names = [m.strip() for m in metric_names.split(",")]  # Split by comma if needed

            # Fetch metrics
            response = client.query_resource(
                resource_uri=resource_id,
                metric_names=metric_names,
                timespan=(from_tr, to_tr),
                granularity=timedelta(seconds=granularity),
                aggregations=[aggregation],
            )
            
            # Convert response to a dictionary
            if not response:
                logger.error(f"Failed to fetch metrics with error: {response.text}")
                return None

            results = {}
            for metric in response.metrics:
                results[metric.name] = [
                    {
                        "timestamp": data.timestamp.isoformat(),
                        "value": data.total if aggregation == "Total" else data.average
                    }
                    for data in metric.timeseries[0].data
                ]

            return results

        except Exception as e:
            logger.error(f"Failed to fetch metrics with error: {e}")
            return None

    # ==================== AKS (Azure Kubernetes Service) Methods ====================

    def aks_list_clusters(self):
        """List all AKS clusters in the subscription."""
        try:
            credentials = self.get_credentials()
            if not credentials:
                logger.error("Azure Connection Error:: Failed to get credentials")
                return None
            container_client = ContainerServiceClient(credentials, self.__subscription_id)
            clusters = container_client.managed_clusters.list()
            return [cluster.as_dict() for cluster in clusters]
        except Exception as e:
            logger.error(f"Failed to list AKS clusters with error: {e}")
            return None

    def aks_get_cluster(self, resource_group: str, cluster_name: str):
        """Get details of a specific AKS cluster."""
        try:
            credentials = self.get_credentials()
            if not credentials:
                return None
            container_client = ContainerServiceClient(credentials, self.__subscription_id)
            cluster = container_client.managed_clusters.get(resource_group, cluster_name)
            return cluster.as_dict()
        except Exception as e:
            logger.error(f"Failed to get AKS cluster {cluster_name}: {e}")
            return None

    def aks_get_credentials(self, resource_group: str, cluster_name: str):
        """Get kubeconfig credentials for an AKS cluster."""
        try:
            credentials = self.get_credentials()
            if not credentials:
                return None
            container_client = ContainerServiceClient(credentials, self.__subscription_id)
            creds = container_client.managed_clusters.list_cluster_admin_credentials(
                resource_group, cluster_name)
            if creds.kubeconfigs:
                return creds.kubeconfigs[0].value
            return None
        except Exception as e:
            logger.error(f"Failed to get AKS credentials for {cluster_name}: {e}")
            return None

    def _get_k8s_client(self, kubeconfig_bytes):
        """Create Kubernetes API client from kubeconfig bytes."""
        if not K8S_AVAILABLE:
            logger.error("kubernetes package not installed")
            return None
        try:
            # Write kubeconfig to temp file
            with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.yaml') as f:
                f.write(kubeconfig_bytes)
                kubeconfig_path = f.name

            # Load config and create client
            k8s_config.load_kube_config(config_file=kubeconfig_path)
            api_client = k8s_client.ApiClient()

            # Clean up temp file
            os.unlink(kubeconfig_path)

            return api_client
        except Exception as e:
            logger.error(f"Error creating K8s client: {e}")
            return None

    def aks_list_namespaces(self, resource_group: str, cluster_name: str):
        """List namespaces in an AKS cluster."""
        try:
            kubeconfig = self.aks_get_credentials(resource_group, cluster_name)
            if not kubeconfig:
                return None
            api_client = self._get_k8s_client(kubeconfig)
            if not api_client:
                return None
            v1 = k8s_client.CoreV1Api(api_client)
            namespaces = v1.list_namespace()
            return [ns.to_dict() for ns in namespaces.items]
        except Exception as e:
            logger.error(f"Failed to list namespaces in AKS cluster {cluster_name}: {e}")
            return None

    def aks_list_deployments(self, resource_group: str, cluster_name: str, namespace: str = None):
        """List deployments in an AKS cluster."""
        try:
            kubeconfig = self.aks_get_credentials(resource_group, cluster_name)
            if not kubeconfig:
                return None
            api_client = self._get_k8s_client(kubeconfig)
            if not api_client:
                return None
            apps_v1 = k8s_client.AppsV1Api(api_client)
            if namespace:
                deployments = apps_v1.list_namespaced_deployment(namespace)
            else:
                deployments = apps_v1.list_deployment_for_all_namespaces()
            return [dep.to_dict() for dep in deployments.items]
        except Exception as e:
            logger.error(f"Failed to list deployments in AKS cluster {cluster_name}: {e}")
            return None

    def aks_list_services(self, resource_group: str, cluster_name: str, namespace: str = None):
        """List services in an AKS cluster."""
        try:
            kubeconfig = self.aks_get_credentials(resource_group, cluster_name)
            if not kubeconfig:
                return None
            api_client = self._get_k8s_client(kubeconfig)
            if not api_client:
                return None
            v1 = k8s_client.CoreV1Api(api_client)
            if namespace:
                services = v1.list_namespaced_service(namespace)
            else:
                services = v1.list_service_for_all_namespaces()
            return [svc.to_dict() for svc in services.items]
        except Exception as e:
            logger.error(f"Failed to list services in AKS cluster {cluster_name}: {e}")
            return None

    def aks_list_ingresses(self, resource_group: str, cluster_name: str, namespace: str = None):
        """List ingresses in an AKS cluster."""
        try:
            kubeconfig = self.aks_get_credentials(resource_group, cluster_name)
            if not kubeconfig:
                return None
            api_client = self._get_k8s_client(kubeconfig)
            if not api_client:
                return None
            networking_v1 = k8s_client.NetworkingV1Api(api_client)
            if namespace:
                ingresses = networking_v1.list_namespaced_ingress(namespace)
            else:
                ingresses = networking_v1.list_ingress_for_all_namespaces()
            return [ing.to_dict() for ing in ingresses.items]
        except Exception as e:
            logger.error(f"Failed to list ingresses in AKS cluster {cluster_name}: {e}")
            return None

    def aks_list_hpas(self, resource_group: str, cluster_name: str, namespace: str = None):
        """List horizontal pod autoscalers in an AKS cluster."""
        try:
            kubeconfig = self.aks_get_credentials(resource_group, cluster_name)
            if not kubeconfig:
                return None
            api_client = self._get_k8s_client(kubeconfig)
            if not api_client:
                return None
            autoscaling_v1 = k8s_client.AutoscalingV1Api(api_client)
            if namespace:
                hpas = autoscaling_v1.list_namespaced_horizontal_pod_autoscaler(namespace)
            else:
                hpas = autoscaling_v1.list_horizontal_pod_autoscaler_for_all_namespaces()
            return [hpa.to_dict() for hpa in hpas.items]
        except Exception as e:
            logger.error(f"Failed to list HPAs in AKS cluster {cluster_name}: {e}")
            return None

    def aks_list_statefulsets(self, resource_group: str, cluster_name: str, namespace: str = None):
        """List stateful sets in an AKS cluster."""
        try:
            kubeconfig = self.aks_get_credentials(resource_group, cluster_name)
            if not kubeconfig:
                return None
            api_client = self._get_k8s_client(kubeconfig)
            if not api_client:
                return None
            apps_v1 = k8s_client.AppsV1Api(api_client)
            if namespace:
                statefulsets = apps_v1.list_namespaced_stateful_set(namespace)
            else:
                statefulsets = apps_v1.list_stateful_set_for_all_namespaces()
            return [ss.to_dict() for ss in statefulsets.items]
        except Exception as e:
            logger.error(f"Failed to list StatefulSets in AKS cluster {cluster_name}: {e}")
            return None

    def aks_list_replicasets(self, resource_group: str, cluster_name: str, namespace: str = None):
        """List replica sets in an AKS cluster."""
        try:
            kubeconfig = self.aks_get_credentials(resource_group, cluster_name)
            if not kubeconfig:
                return None
            api_client = self._get_k8s_client(kubeconfig)
            if not api_client:
                return None
            apps_v1 = k8s_client.AppsV1Api(api_client)
            if namespace:
                replicasets = apps_v1.list_namespaced_replica_set(namespace)
            else:
                replicasets = apps_v1.list_replica_set_for_all_namespaces()
            return [rs.to_dict() for rs in replicasets.items]
        except Exception as e:
            logger.error(f"Failed to list ReplicaSets in AKS cluster {cluster_name}: {e}")
            return None

    def aks_list_network_policies(self, resource_group: str, cluster_name: str, namespace: str = None):
        """List network policies in an AKS cluster."""
        try:
            kubeconfig = self.aks_get_credentials(resource_group, cluster_name)
            if not kubeconfig:
                return None
            api_client = self._get_k8s_client(kubeconfig)
            if not api_client:
                return None
            networking_v1 = k8s_client.NetworkingV1Api(api_client)
            if namespace:
                policies = networking_v1.list_namespaced_network_policy(namespace)
            else:
                policies = networking_v1.list_network_policy_for_all_namespaces()
            return [policy.to_dict() for policy in policies.items]
        except Exception as e:
            logger.error(f"Failed to list NetworkPolicies in AKS cluster {cluster_name}: {e}")
            return None

    # ==================== Azure Compute Methods ====================

    def compute_list_vms(self, resource_group: str = None):
        """List all virtual machines in the subscription or resource group."""
        try:
            credentials = self.get_credentials()
            if not credentials:
                return None
            compute_client = ComputeManagementClient(credentials, self.__subscription_id)
            if resource_group:
                vms = compute_client.virtual_machines.list(resource_group)
            else:
                vms = compute_client.virtual_machines.list_all()
            return [vm.as_dict() for vm in vms]
        except Exception as e:
            logger.error(f"Failed to list VMs: {e}")
            return None

    def compute_get_vm_instance_view(self, resource_group: str, vm_name: str):
        """Get VM instance view (running state, etc.)."""
        try:
            credentials = self.get_credentials()
            if not credentials:
                return None
            compute_client = ComputeManagementClient(credentials, self.__subscription_id)
            instance_view = compute_client.virtual_machines.instance_view(resource_group, vm_name)
            return instance_view.as_dict()
        except Exception as e:
            logger.error(f"Failed to get VM instance view for {vm_name}: {e}")
            return None

    def compute_list_vmss(self, resource_group: str = None):
        """List all VM scale sets in the subscription or resource group."""
        try:
            credentials = self.get_credentials()
            if not credentials:
                return None
            compute_client = ComputeManagementClient(credentials, self.__subscription_id)
            if resource_group:
                vmss_list = compute_client.virtual_machine_scale_sets.list(resource_group)
            else:
                vmss_list = compute_client.virtual_machine_scale_sets.list_all()
            return [vmss.as_dict() for vmss in vmss_list]
        except Exception as e:
            logger.error(f"Failed to list VMSS: {e}")
            return None

    # ==================== Azure Storage Methods ====================

    def storage_list_accounts(self, resource_group: str = None):
        """List all storage accounts in the subscription or resource group."""
        try:
            credentials = self.get_credentials()
            if not credentials:
                return None
            storage_client = StorageManagementClient(credentials, self.__subscription_id)
            if resource_group:
                accounts = storage_client.storage_accounts.list_by_resource_group(resource_group)
            else:
                accounts = storage_client.storage_accounts.list()
            return [account.as_dict() for account in accounts]
        except Exception as e:
            logger.error(f"Failed to list storage accounts: {e}")
            return None

    def storage_list_blob_containers(self, resource_group: str, account_name: str):
        """List blob containers in a storage account."""
        try:
            credentials = self.get_credentials()
            if not credentials:
                return None
            storage_client = StorageManagementClient(credentials, self.__subscription_id)
            containers = storage_client.blob_containers.list(resource_group, account_name)
            return [container.as_dict() for container in containers]
        except Exception as e:
            logger.error(f"Failed to list blob containers for {account_name}: {e}")
            return None

    # ==================== Azure SQL Database Methods ====================

    def sql_list_servers(self, resource_group: str = None):
        """List all SQL servers in the subscription or resource group."""
        try:
            credentials = self.get_credentials()
            if not credentials:
                return None
            sql_client = SqlManagementClient(credentials, self.__subscription_id)
            if resource_group:
                servers = sql_client.servers.list_by_resource_group(resource_group)
            else:
                servers = sql_client.servers.list()
            return [server.as_dict() for server in servers]
        except Exception as e:
            logger.error(f"Failed to list SQL servers: {e}")
            return None

    def sql_list_databases(self, resource_group: str, server_name: str):
        """List all databases in a SQL server."""
        try:
            credentials = self.get_credentials()
            if not credentials:
                return None
            sql_client = SqlManagementClient(credentials, self.__subscription_id)
            databases = sql_client.databases.list_by_server(resource_group, server_name)
            return [db.as_dict() for db in databases]
        except Exception as e:
            logger.error(f"Failed to list SQL databases for {server_name}: {e}")
            return None

    # ==================== Azure Cosmos DB Methods ====================

    def cosmos_list_accounts(self, resource_group: str = None):
        """List all Cosmos DB accounts in the subscription or resource group."""
        try:
            credentials = self.get_credentials()
            if not credentials:
                return None
            cosmos_client = CosmosDBManagementClient(credentials, self.__subscription_id)
            if resource_group:
                accounts = cosmos_client.database_accounts.list_by_resource_group(resource_group)
            else:
                accounts = cosmos_client.database_accounts.list()
            return [account.as_dict() for account in accounts]
        except Exception as e:
            logger.error(f"Failed to list Cosmos DB accounts: {e}")
            return None

    # ==================== Azure Monitor Methods ====================

    def monitor_list_metric_alerts(self, resource_group: str = None):
        """List all metric alerts in the subscription or resource group."""
        try:
            credentials = self.get_credentials()
            if not credentials:
                return None
            monitor_client = MonitorManagementClient(credentials, self.__subscription_id)
            if resource_group:
                alerts = monitor_client.metric_alerts.list_by_resource_group(resource_group)
            else:
                alerts = monitor_client.metric_alerts.list_by_subscription()
            return [alert.as_dict() for alert in alerts]
        except Exception as e:
            logger.error(f"Failed to list metric alerts: {e}")
            return None

    def monitor_list_action_groups(self, resource_group: str = None):
        """List all action groups in the subscription or resource group."""
        try:
            credentials = self.get_credentials()
            if not credentials:
                return None
            monitor_client = MonitorManagementClient(credentials, self.__subscription_id)
            if resource_group:
                groups = monitor_client.action_groups.list_by_resource_group(resource_group)
            else:
                groups = monitor_client.action_groups.list_by_subscription_id()
            return [group.as_dict() for group in groups]
        except Exception as e:
            logger.error(f"Failed to list action groups: {e}")
            return None

    def list_resource_groups(self):
        """List all resource groups in the subscription."""
        try:
            credentials = self.get_credentials()
            if not credentials:
                return None
            resource_client = ResourceManagementClient(credentials, self.__subscription_id)
            groups = resource_client.resource_groups.list()
            return [rg.as_dict() for rg in groups]
        except Exception as e:
            logger.error(f"Failed to list resource groups: {e}")
            return None

    # ==================== PostgreSQL Flexible Server Methods ====================

    def postgres_flexible_list_servers(self, resource_group: str = None):
        """List all PostgreSQL Flexible Servers in the subscription or resource group."""
        try:
            credentials = self.get_credentials()
            if not credentials:
                return None
            postgres_client = PostgreSQLManagementClient(credentials, self.__subscription_id)
            if resource_group:
                servers = postgres_client.servers.list_by_resource_group(resource_group)
            else:
                servers = postgres_client.servers.list()
            return [server.as_dict() for server in servers]
        except Exception as e:
            logger.error(f"Failed to list PostgreSQL Flexible Servers: {e}")
            return None

    def postgres_flexible_list_databases(self, resource_group: str, server_name: str):
        """List all databases in a PostgreSQL Flexible Server."""
        try:
            credentials = self.get_credentials()
            if not credentials:
                return None
            postgres_client = PostgreSQLManagementClient(credentials, self.__subscription_id)
            databases = postgres_client.databases.list_by_server(resource_group, server_name)
            return [db.as_dict() for db in databases]
        except Exception as e:
            logger.error(f"Failed to list databases for PostgreSQL server {server_name}: {e}")
            return None

    # ==================== Redis Cache Methods ====================

    def redis_list_caches(self, resource_group: str = None):
        """List all Redis Caches in the subscription or resource group."""
        try:
            credentials = self.get_credentials()
            if not credentials:
                return None
            redis_client = RedisManagementClient(credentials, self.__subscription_id)
            if resource_group:
                caches = redis_client.redis.list_by_resource_group(resource_group)
            else:
                caches = redis_client.redis.list_by_subscription()
            return [cache.as_dict() for cache in caches]
        except Exception as e:
            logger.error(f"Failed to list Redis Caches: {e}")
            return None