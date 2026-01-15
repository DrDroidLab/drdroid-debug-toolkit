"""
Azure API Processor using az CLI instead of Azure SDK.

Reduces dependency overhead by using Azure CLI for all Azure operations
while keeping the kubernetes package for AKS K8s API calls.
"""
import logging
import os
import tempfile
from datetime import timedelta, datetime, timezone
from typing import List, Dict, Any, Optional

from core.integrations.processor import Processor
from core.integrations.utils.cli_executor import (
    AzureCLIExecutor,
    CLIExecutionError,
    CLINotFoundError,
    CLITimeoutError
)

logger = logging.getLogger(__name__)

# Optional kubernetes import - only needed for AKS K8s API calls
try:
    from kubernetes import client as k8s_client, config as k8s_config
    K8S_AVAILABLE = True
except ImportError:
    K8S_AVAILABLE = False
    logger.warning("kubernetes package not installed - AKS K8s API methods will not be available")


class AzureApiProcessor(Processor):
    """Azure API processor using az CLI instead of SDK."""

    def __init__(self, subscription_id, tenant_id, client_id, client_secret):
        self.__subscription_id = subscription_id
        self.__client_id = client_id
        self.__client_secret = client_secret
        self.__tenant_id = tenant_id

        if not self.__client_id or not self.__client_secret or not self.__tenant_id:
            raise Exception("Azure Connection Error:: Missing client_id, client_secret, or tenant_id")

        self._cli = AzureCLIExecutor(
            subscription_id=subscription_id,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )

    def test_connection(self):
        try:
            workspaces = self.fetch_workspaces()
            if workspaces and len(workspaces) > 0:
                return True
            else:
                raise Exception("Azure Connection Error:: No Workspaces Found")
        except Exception as e:
            raise e

    def fetch_workspaces(self) -> Optional[List[Dict[str, Any]]]:
        """Fetch all Log Analytics workspaces."""
        try:
            result = self._cli.execute_az_command([
                "monitor", "log-analytics", "workspace", "list",
                "--subscription", self.__subscription_id
            ])
            return result if isinstance(result, list) else []
        except CLIExecutionError as e:
            logger.error(f"Failed to fetch workspaces: {e.stderr}")
            return None
        except Exception as e:
            logger.error(f"Failed to fetch workspaces with error: {e}")
            return None

    def fetch_resources(self) -> Optional[List[Dict[str, Any]]]:
        """Fetch resources that produce metrics."""
        try:
            result = self._cli.execute_az_command([
                "resource", "list",
                "--subscription", self.__subscription_id
            ])

            if not isinstance(result, list):
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

            valid_resources = []
            for resource in result:
                if resource.get('type') in metric_producing_types:
                    resource_id = resource.get('id')
                    # Get metric definitions for the resource
                    try:
                        metrics_result = self._cli.execute_az_command([
                            "monitor", "metrics", "list-definitions",
                            "--resource", resource_id
                        ])
                        metric_names = [m.get('name', {}).get('value') for m in metrics_result] if isinstance(metrics_result, list) else []
                    except Exception as e:
                        logger.error(f"Failed to fetch metrics for resource {resource_id}: {e}")
                        metric_names = []

                    resource["available_metrics"] = {"metric_names": metric_names}
                    valid_resources.append(resource)

            return valid_resources
        except CLIExecutionError as e:
            logger.error(f"Failed to fetch resources: {e.stderr}")
            return None
        except Exception as e:
            logger.error(f"Failed to fetch resources with error: {e}")
            return None

    def query_log_analytics(self, workspace_id: str, query: str, timespan=timedelta(hours=4)) -> Optional[Dict[str, Any]]:
        """Query Log Analytics workspace."""
        try:
            # Convert timespan to ISO 8601 duration format
            total_seconds = int(timespan.total_seconds())
            hours = total_seconds // 3600
            timespan_str = f"PT{hours}H" if hours > 0 else f"PT{total_seconds}S"

            result = self._cli.execute_az_command([
                "monitor", "log-analytics", "query",
                "--workspace", workspace_id,
                "--analytics-query", query,
                "--timespan", timespan_str
            ])

            # Convert to expected format
            if isinstance(result, list):
                return {"PrimaryResult": result}
            return result
        except CLIExecutionError as e:
            logger.error(f"Failed to query log analytics: {e.stderr}")
            return None
        except Exception as e:
            logger.error(f"Failed to query log analytics with error: {e}")
            return None

    def query_metrics(self, resource_id: str, time_range, metric_names="Percentage CPU",
                      aggregation="Average", granularity=300) -> Optional[Dict[str, Any]]:
        """Fetch metrics from Azure Monitor."""
        try:
            # Convert Unix timestamps to ISO format
            from_tr = datetime.fromtimestamp(time_range.time_geq, tz=timezone.utc)
            to_tr = datetime.fromtimestamp(time_range.time_lt, tz=timezone.utc)

            # Ensure metric_names is a list
            if isinstance(metric_names, str):
                metric_names = [m.strip() for m in metric_names.split(",")]

            # Convert granularity to ISO 8601 duration
            if granularity < 60:
                interval = f"PT{granularity}S"
            elif granularity < 3600:
                interval = f"PT{granularity // 60}M"
            else:
                interval = f"PT{granularity // 3600}H"

            result = self._cli.execute_az_command([
                "monitor", "metrics", "list",
                "--resource", resource_id,
                "--metrics", ",".join(metric_names),
                "--start-time", from_tr.isoformat(),
                "--end-time", to_tr.isoformat(),
                "--interval", interval,
                "--aggregation", aggregation
            ])

            if not result:
                return None

            # Convert to expected format
            results = {}
            metrics_data = result.get('value', []) if isinstance(result, dict) else result
            for metric in metrics_data:
                metric_name = metric.get('name', {}).get('value', 'Unknown')
                timeseries = metric.get('timeseries', [])
                if timeseries:
                    data_points = timeseries[0].get('data', [])
                    results[metric_name] = [
                        {
                            "timestamp": dp.get('timeStamp'),
                            "value": dp.get('total') if aggregation == "Total" else dp.get('average')
                        }
                        for dp in data_points
                    ]

            return results
        except CLIExecutionError as e:
            logger.error(f"Failed to fetch metrics: {e.stderr}")
            return None
        except Exception as e:
            logger.error(f"Failed to fetch metrics with error: {e}")
            return None

    # ==================== AKS (Azure Kubernetes Service) Methods ====================

    def aks_list_clusters(self) -> Optional[List[Dict[str, Any]]]:
        """List all AKS clusters in the subscription."""
        try:
            result = self._cli.execute_az_command([
                "aks", "list",
                "--subscription", self.__subscription_id
            ])
            return result if isinstance(result, list) else []
        except CLIExecutionError as e:
            logger.error(f"Failed to list AKS clusters: {e.stderr}")
            return None
        except Exception as e:
            logger.error(f"Failed to list AKS clusters with error: {e}")
            return None

    def aks_get_cluster(self, resource_group: str, cluster_name: str) -> Optional[Dict[str, Any]]:
        """Get details of a specific AKS cluster."""
        try:
            result = self._cli.execute_az_command([
                "aks", "show",
                "--resource-group", resource_group,
                "--name", cluster_name
            ])
            return result if isinstance(result, dict) else None
        except CLIExecutionError as e:
            logger.error(f"Failed to get AKS cluster {cluster_name}: {e.stderr}")
            return None
        except Exception as e:
            logger.error(f"Failed to get AKS cluster {cluster_name}: {e}")
            return None

    def aks_get_credentials(self, resource_group: str, cluster_name: str) -> Optional[bytes]:
        """Get kubeconfig credentials for an AKS cluster."""
        try:
            # Create a temp file to store kubeconfig
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.yaml') as f:
                kubeconfig_path = f.name

            # Get credentials and save to temp file
            self._cli.execute_az_command([
                "aks", "get-credentials",
                "--resource-group", resource_group,
                "--name", cluster_name,
                "--file", kubeconfig_path,
                "--admin",
                "--overwrite-existing"
            ], parse_json=False)

            # Read the kubeconfig file
            with open(kubeconfig_path, 'rb') as f:
                kubeconfig_bytes = f.read()

            # Clean up
            os.unlink(kubeconfig_path)

            return kubeconfig_bytes
        except CLIExecutionError as e:
            logger.error(f"Failed to get AKS credentials for {cluster_name}: {e.stderr}")
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

    def aks_list_namespaces(self, resource_group: str, cluster_name: str) -> Optional[List[Dict[str, Any]]]:
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

    def aks_list_deployments(self, resource_group: str, cluster_name: str, namespace: str = None) -> Optional[List[Dict[str, Any]]]:
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

    def aks_list_services(self, resource_group: str, cluster_name: str, namespace: str = None) -> Optional[List[Dict[str, Any]]]:
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

    def aks_list_ingresses(self, resource_group: str, cluster_name: str, namespace: str = None) -> Optional[List[Dict[str, Any]]]:
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

    def aks_list_hpas(self, resource_group: str, cluster_name: str, namespace: str = None) -> Optional[List[Dict[str, Any]]]:
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

    def aks_list_statefulsets(self, resource_group: str, cluster_name: str, namespace: str = None) -> Optional[List[Dict[str, Any]]]:
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

    def aks_list_replicasets(self, resource_group: str, cluster_name: str, namespace: str = None) -> Optional[List[Dict[str, Any]]]:
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

    def aks_list_network_policies(self, resource_group: str, cluster_name: str, namespace: str = None) -> Optional[List[Dict[str, Any]]]:
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

    def compute_list_vms(self, resource_group: str = None) -> Optional[List[Dict[str, Any]]]:
        """List all virtual machines in the subscription or resource group."""
        try:
            if resource_group:
                result = self._cli.execute_az_command([
                    "vm", "list",
                    "--resource-group", resource_group
                ])
            else:
                result = self._cli.execute_az_command(["vm", "list"])
            return result if isinstance(result, list) else []
        except CLIExecutionError as e:
            logger.error(f"Failed to list VMs: {e.stderr}")
            return None
        except Exception as e:
            logger.error(f"Failed to list VMs: {e}")
            return None

    def compute_get_vm_instance_view(self, resource_group: str, vm_name: str) -> Optional[Dict[str, Any]]:
        """Get VM instance view (running state, etc.)."""
        try:
            result = self._cli.execute_az_command([
                "vm", "get-instance-view",
                "--resource-group", resource_group,
                "--name", vm_name
            ])
            return result if isinstance(result, dict) else None
        except CLIExecutionError as e:
            logger.error(f"Failed to get VM instance view for {vm_name}: {e.stderr}")
            return None
        except Exception as e:
            logger.error(f"Failed to get VM instance view for {vm_name}: {e}")
            return None

    def compute_list_vmss(self, resource_group: str = None) -> Optional[List[Dict[str, Any]]]:
        """List all VM scale sets in the subscription or resource group."""
        try:
            if resource_group:
                result = self._cli.execute_az_command([
                    "vmss", "list",
                    "--resource-group", resource_group
                ])
            else:
                result = self._cli.execute_az_command(["vmss", "list"])
            return result if isinstance(result, list) else []
        except CLIExecutionError as e:
            logger.error(f"Failed to list VMSS: {e.stderr}")
            return None
        except Exception as e:
            logger.error(f"Failed to list VMSS: {e}")
            return None

    # ==================== Azure Storage Methods ====================

    def storage_list_accounts(self, resource_group: str = None) -> Optional[List[Dict[str, Any]]]:
        """List all storage accounts in the subscription or resource group."""
        try:
            if resource_group:
                result = self._cli.execute_az_command([
                    "storage", "account", "list",
                    "--resource-group", resource_group
                ])
            else:
                result = self._cli.execute_az_command(["storage", "account", "list"])
            return result if isinstance(result, list) else []
        except CLIExecutionError as e:
            logger.error(f"Failed to list storage accounts: {e.stderr}")
            return None
        except Exception as e:
            logger.error(f"Failed to list storage accounts: {e}")
            return None

    def storage_list_blob_containers(self, resource_group: str, account_name: str) -> Optional[List[Dict[str, Any]]]:
        """List blob containers in a storage account."""
        try:
            result = self._cli.execute_az_command([
                "storage", "container", "list",
                "--account-name", account_name,
                "--auth-mode", "login"
            ])
            return result if isinstance(result, list) else []
        except CLIExecutionError as e:
            logger.error(f"Failed to list blob containers for {account_name}: {e.stderr}")
            return None
        except Exception as e:
            logger.error(f"Failed to list blob containers for {account_name}: {e}")
            return None

    # ==================== Azure SQL Database Methods ====================

    def sql_list_servers(self, resource_group: str = None) -> Optional[List[Dict[str, Any]]]:
        """List all SQL servers in the subscription or resource group."""
        try:
            if resource_group:
                result = self._cli.execute_az_command([
                    "sql", "server", "list",
                    "--resource-group", resource_group
                ])
            else:
                result = self._cli.execute_az_command(["sql", "server", "list"])
            return result if isinstance(result, list) else []
        except CLIExecutionError as e:
            logger.error(f"Failed to list SQL servers: {e.stderr}")
            return None
        except Exception as e:
            logger.error(f"Failed to list SQL servers: {e}")
            return None

    def sql_list_databases(self, resource_group: str, server_name: str) -> Optional[List[Dict[str, Any]]]:
        """List all databases in a SQL server."""
        try:
            result = self._cli.execute_az_command([
                "sql", "db", "list",
                "--resource-group", resource_group,
                "--server", server_name
            ])
            return result if isinstance(result, list) else []
        except CLIExecutionError as e:
            logger.error(f"Failed to list SQL databases for {server_name}: {e.stderr}")
            return None
        except Exception as e:
            logger.error(f"Failed to list SQL databases for {server_name}: {e}")
            return None

    # ==================== Azure Cosmos DB Methods ====================

    def cosmos_list_accounts(self, resource_group: str = None) -> Optional[List[Dict[str, Any]]]:
        """List all Cosmos DB accounts in the subscription or resource group."""
        try:
            if resource_group:
                result = self._cli.execute_az_command([
                    "cosmosdb", "list",
                    "--resource-group", resource_group
                ])
            else:
                result = self._cli.execute_az_command(["cosmosdb", "list"])
            return result if isinstance(result, list) else []
        except CLIExecutionError as e:
            logger.error(f"Failed to list Cosmos DB accounts: {e.stderr}")
            return None
        except Exception as e:
            logger.error(f"Failed to list Cosmos DB accounts: {e}")
            return None

    # ==================== Azure Monitor Methods ====================

    def monitor_list_metric_alerts(self, resource_group: str = None) -> Optional[List[Dict[str, Any]]]:
        """List all metric alerts in the subscription or resource group."""
        try:
            if resource_group:
                result = self._cli.execute_az_command([
                    "monitor", "metrics", "alert", "list",
                    "--resource-group", resource_group
                ])
            else:
                result = self._cli.execute_az_command(["monitor", "metrics", "alert", "list"])
            return result if isinstance(result, list) else []
        except CLIExecutionError as e:
            logger.error(f"Failed to list metric alerts: {e.stderr}")
            return None
        except Exception as e:
            logger.error(f"Failed to list metric alerts: {e}")
            return None

    def monitor_list_action_groups(self, resource_group: str = None) -> Optional[List[Dict[str, Any]]]:
        """List all action groups in the subscription or resource group."""
        try:
            if resource_group:
                result = self._cli.execute_az_command([
                    "monitor", "action-group", "list",
                    "--resource-group", resource_group
                ])
            else:
                result = self._cli.execute_az_command(["monitor", "action-group", "list"])
            return result if isinstance(result, list) else []
        except CLIExecutionError as e:
            logger.error(f"Failed to list action groups: {e.stderr}")
            return None
        except Exception as e:
            logger.error(f"Failed to list action groups: {e}")
            return None

    def list_resource_groups(self) -> Optional[List[Dict[str, Any]]]:
        """List all resource groups in the subscription."""
        try:
            result = self._cli.execute_az_command(["group", "list"])
            return result if isinstance(result, list) else []
        except CLIExecutionError as e:
            logger.error(f"Failed to list resource groups: {e.stderr}")
            return None
        except Exception as e:
            logger.error(f"Failed to list resource groups: {e}")
            return None

    # ==================== PostgreSQL Flexible Server Methods ====================

    def postgres_flexible_list_servers(self, resource_group: str = None) -> Optional[List[Dict[str, Any]]]:
        """List all PostgreSQL Flexible Servers in the subscription or resource group."""
        try:
            if resource_group:
                result = self._cli.execute_az_command([
                    "postgres", "flexible-server", "list",
                    "--resource-group", resource_group
                ])
            else:
                result = self._cli.execute_az_command(["postgres", "flexible-server", "list"])
            return result if isinstance(result, list) else []
        except CLIExecutionError as e:
            logger.error(f"Failed to list PostgreSQL Flexible Servers: {e.stderr}")
            return None
        except Exception as e:
            logger.error(f"Failed to list PostgreSQL Flexible Servers: {e}")
            return None

    def postgres_flexible_list_databases(self, resource_group: str, server_name: str) -> Optional[List[Dict[str, Any]]]:
        """List all databases in a PostgreSQL Flexible Server."""
        try:
            result = self._cli.execute_az_command([
                "postgres", "flexible-server", "db", "list",
                "--resource-group", resource_group,
                "--server-name", server_name
            ])
            return result if isinstance(result, list) else []
        except CLIExecutionError as e:
            logger.error(f"Failed to list databases for PostgreSQL server {server_name}: {e.stderr}")
            return None
        except Exception as e:
            logger.error(f"Failed to list databases for PostgreSQL server {server_name}: {e}")
            return None

    # ==================== Redis Cache Methods ====================

    def redis_list_caches(self, resource_group: str = None) -> Optional[List[Dict[str, Any]]]:
        """List all Redis Caches in the subscription or resource group."""
        try:
            if resource_group:
                result = self._cli.execute_az_command([
                    "redis", "list",
                    "--resource-group", resource_group
                ])
            else:
                result = self._cli.execute_az_command(["redis", "list"])
            return result if isinstance(result, list) else []
        except CLIExecutionError as e:
            logger.error(f"Failed to list Redis Caches: {e.stderr}")
            return None
        except Exception as e:
            logger.error(f"Failed to list Redis Caches: {e}")
            return None
