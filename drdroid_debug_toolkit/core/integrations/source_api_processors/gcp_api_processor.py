"""
GCP API Processor for non-GKE Google Cloud Platform services.

Handles: Compute Engine, Cloud Storage, Cloud SQL, Memorystore Redis,
Cloud Functions, Cloud Run, Pub/Sub, BigQuery, Monitoring, Networking,
Secret Manager, IAM, and Logging services.
"""
import json
import logging
from typing import List, Dict, Any, Optional

from google.oauth2 import service_account
from google.cloud import compute_v1
from google.cloud import storage
from google.cloud import redis_v1
from google.cloud import monitoring_v3
from google.cloud import functions_v1
from google.cloud import run_v2
from google.cloud import pubsub_v1
from google.cloud import bigquery
from google.cloud import secretmanager
from google.cloud import logging as cloud_logging
from googleapiclient.discovery import build

from core.integrations.processor import Processor

logger = logging.getLogger(__name__)


def get_gcp_credentials(service_account_json_str: str):
    """Get GCP credentials from service account JSON string."""
    service_account_json = json.loads(service_account_json_str)
    scopes = [
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/compute.readonly",
        "https://www.googleapis.com/auth/devstorage.read_only",
        "https://www.googleapis.com/auth/sqlservice.admin",
        "https://www.googleapis.com/auth/monitoring.read",
        "https://www.googleapis.com/auth/pubsub",
        "https://www.googleapis.com/auth/bigquery.readonly",
        "https://www.googleapis.com/auth/logging.read",
    ]
    credentials = service_account.Credentials.from_service_account_info(
        service_account_json, scopes=scopes
    )
    return credentials


class GcpApiProcessor(Processor):
    """API Processor for GCP non-GKE services."""

    def __init__(self, project_id: str, service_account_json: str):
        self.__project_id = project_id
        self.__service_account_json = service_account_json
        self.__credentials = get_gcp_credentials(service_account_json)

    def test_connection(self) -> bool:
        """Test connection to GCP."""
        try:
            # Try to list compute zones to verify connectivity
            compute_client = compute_v1.ZonesClient(credentials=self.__credentials)
            zones = list(compute_client.list(project=self.__project_id))
            return len(zones) > 0
        except Exception as e:
            logger.error(f"GCP connection test failed: {e}")
            raise e

    # ==================== Compute Engine ====================

    def compute_list_instances(self, zone: str = None) -> List[Dict[str, Any]]:
        """List all Compute Engine instances in the project."""
        instances = []
        try:
            compute_client = compute_v1.InstancesClient(credentials=self.__credentials)

            if zone:
                # List instances in specific zone
                request = compute_v1.ListInstancesRequest(
                    project=self.__project_id,
                    zone=zone
                )
                for instance in compute_client.list(request=request):
                    instances.append(self._instance_to_dict(instance, zone))
            else:
                # List instances across all zones
                agg_client = compute_v1.InstancesClient(credentials=self.__credentials)
                agg_request = compute_v1.AggregatedListInstancesRequest(
                    project=self.__project_id
                )
                for zone_name, response in agg_client.aggregated_list(request=agg_request):
                    if response.instances:
                        for instance in response.instances:
                            z = zone_name.replace('zones/', '')
                            instances.append(self._instance_to_dict(instance, z))
        except Exception as e:
            logger.error(f"Error listing compute instances: {e}")
        return instances

    def _instance_to_dict(self, instance, zone: str) -> Dict[str, Any]:
        """Convert Compute Engine instance to dictionary."""
        return {
            'name': instance.name,
            'id': str(instance.id),
            'zone': zone,
            'machine_type': instance.machine_type.split('/')[-1] if instance.machine_type else None,
            'status': instance.status,
            'creation_timestamp': instance.creation_timestamp,
            'network_interfaces': [
                {
                    'network': ni.network.split('/')[-1] if ni.network else None,
                    'subnetwork': ni.subnetwork.split('/')[-1] if ni.subnetwork else None,
                    'internal_ip': ni.network_i_p,
                    'external_ip': ni.access_configs[0].nat_i_p if ni.access_configs else None,
                }
                for ni in instance.network_interfaces
            ] if instance.network_interfaces else [],
            'disks': [
                {
                    'source': disk.source.split('/')[-1] if disk.source else None,
                    'boot': disk.boot,
                    'auto_delete': disk.auto_delete,
                }
                for disk in instance.disks
            ] if instance.disks else [],
            'labels': dict(instance.labels) if instance.labels else {},
            'tags': list(instance.tags.items) if instance.tags else [],
        }

    def compute_list_instance_groups(self, zone: str = None) -> List[Dict[str, Any]]:
        """List all Instance Groups in the project."""
        groups = []
        try:
            ig_client = compute_v1.InstanceGroupsClient(credentials=self.__credentials)

            if zone:
                request = compute_v1.ListInstanceGroupsRequest(
                    project=self.__project_id,
                    zone=zone
                )
                for group in ig_client.list(request=request):
                    groups.append(self._instance_group_to_dict(group, zone))
            else:
                agg_request = compute_v1.AggregatedListInstanceGroupsRequest(
                    project=self.__project_id
                )
                for zone_name, response in ig_client.aggregated_list(request=agg_request):
                    if response.instance_groups:
                        for group in response.instance_groups:
                            z = zone_name.replace('zones/', '')
                            groups.append(self._instance_group_to_dict(group, z))
        except Exception as e:
            logger.error(f"Error listing instance groups: {e}")
        return groups

    def _instance_group_to_dict(self, group, zone: str) -> Dict[str, Any]:
        """Convert Instance Group to dictionary."""
        return {
            'name': group.name,
            'id': str(group.id),
            'zone': zone,
            'description': group.description,
            'size': group.size,
            'creation_timestamp': group.creation_timestamp,
            'network': group.network.split('/')[-1] if group.network else None,
            'subnetwork': group.subnetwork.split('/')[-1] if group.subnetwork else None,
        }

    # ==================== Cloud Storage ====================

    def storage_list_buckets(self) -> List[Dict[str, Any]]:
        """List all Cloud Storage buckets in the project."""
        buckets = []
        try:
            storage_client = storage.Client(
                project=self.__project_id,
                credentials=self.__credentials
            )
            for bucket in storage_client.list_buckets():
                buckets.append({
                    'name': bucket.name,
                    'id': bucket.id,
                    'location': bucket.location,
                    'location_type': bucket.location_type,
                    'storage_class': bucket.storage_class,
                    'creation_time': bucket.time_created.isoformat() if bucket.time_created else None,
                    'versioning_enabled': bucket.versioning_enabled,
                    'labels': dict(bucket.labels) if bucket.labels else {},
                    'lifecycle_rules': [
                        {'action': rule.get('action'), 'condition': rule.get('condition')}
                        for rule in bucket.lifecycle_rules
                    ] if bucket.lifecycle_rules else [],
                    'cors': bucket.cors,
                    'default_event_based_hold': bucket.default_event_based_hold,
                    'requester_pays': bucket.requester_pays,
                })
        except Exception as e:
            logger.error(f"Error listing storage buckets: {e}")
        return buckets

    # ==================== Cloud SQL ====================

    def sql_list_instances(self) -> List[Dict[str, Any]]:
        """List all Cloud SQL instances in the project."""
        instances = []
        try:
            service = build('sqladmin', 'v1beta4', credentials=self.__credentials)
            request = service.instances().list(project=self.__project_id)
            response = request.execute()

            for instance in response.get('items', []):
                instances.append({
                    'name': instance.get('name'),
                    'database_version': instance.get('databaseVersion'),
                    'region': instance.get('region'),
                    'state': instance.get('state'),
                    'tier': instance.get('settings', {}).get('tier'),
                    'storage_auto_resize': instance.get('settings', {}).get('storageAutoResize'),
                    'data_disk_size_gb': instance.get('settings', {}).get('dataDiskSizeGb'),
                    'data_disk_type': instance.get('settings', {}).get('dataDiskType'),
                    'availability_type': instance.get('settings', {}).get('availabilityType'),
                    'backup_enabled': instance.get('settings', {}).get('backupConfiguration', {}).get('enabled'),
                    'ip_addresses': [
                        {'type': ip.get('type'), 'ip_address': ip.get('ipAddress')}
                        for ip in instance.get('ipAddresses', [])
                    ],
                    'connection_name': instance.get('connectionName'),
                    'gce_zone': instance.get('gceZone'),
                    'secondary_gce_zone': instance.get('secondaryGceZone'),
                    'creation_time': instance.get('createTime'),
                    'labels': instance.get('settings', {}).get('userLabels', {}),
                })
        except Exception as e:
            logger.error(f"Error listing Cloud SQL instances: {e}")
        return instances

    def sql_list_databases(self, instance_name: str) -> List[Dict[str, Any]]:
        """List all databases in a Cloud SQL instance."""
        databases = []
        try:
            service = build('sqladmin', 'v1beta4', credentials=self.__credentials)
            request = service.databases().list(project=self.__project_id, instance=instance_name)
            response = request.execute()

            for db in response.get('items', []):
                databases.append({
                    'name': db.get('name'),
                    'instance': instance_name,
                    'charset': db.get('charset'),
                    'collation': db.get('collation'),
                    'self_link': db.get('selfLink'),
                })
        except Exception as e:
            logger.error(f"Error listing databases for {instance_name}: {e}")
        return databases

    # ==================== Memorystore Redis ====================

    def redis_list_instances(self) -> List[Dict[str, Any]]:
        """List all Memorystore Redis instances in the project."""
        instances = []
        try:
            redis_client = redis_v1.CloudRedisClient(credentials=self.__credentials)
            parent = f"projects/{self.__project_id}/locations/-"

            for instance in redis_client.list_instances(parent=parent):
                instances.append({
                    'name': instance.name.split('/')[-1],
                    'display_name': instance.display_name,
                    'location': instance.location_id,
                    'state': instance.state.name,
                    'tier': instance.tier.name,
                    'memory_size_gb': instance.memory_size_gb,
                    'redis_version': instance.redis_version,
                    'host': instance.host,
                    'port': instance.port,
                    'current_location_id': instance.current_location_id,
                    'create_time': instance.create_time.isoformat() if instance.create_time else None,
                    'authorized_network': instance.authorized_network,
                    'persistence_mode': instance.persistence_config.persistence_mode.name if instance.persistence_config else None,
                    'labels': dict(instance.labels) if instance.labels else {},
                    'replica_count': instance.replica_count,
                    'read_replicas_mode': instance.read_replicas_mode.name if instance.read_replicas_mode else None,
                })
        except Exception as e:
            logger.error(f"Error listing Redis instances: {e}")
        return instances

    # ==================== Cloud Monitoring ====================

    def monitoring_list_alert_policies(self) -> List[Dict[str, Any]]:
        """List all alert policies in the project."""
        policies = []
        try:
            client = monitoring_v3.AlertPolicyServiceClient(credentials=self.__credentials)
            name = f"projects/{self.__project_id}"

            for policy in client.list_alert_policies(name=name):
                policies.append({
                    'name': policy.name.split('/')[-1],
                    'display_name': policy.display_name,
                    'enabled': policy.enabled.value if policy.enabled else True,
                    'combiner': policy.combiner.name,
                    'conditions': [
                        {
                            'name': c.name,
                            'display_name': c.display_name,
                        }
                        for c in policy.conditions
                    ],
                    'notification_channels': list(policy.notification_channels),
                    'documentation': policy.documentation.content if policy.documentation else None,
                    'labels': dict(policy.user_labels) if policy.user_labels else {},
                    'creation_time': policy.creation_record.mutate_time.isoformat() if policy.creation_record else None,
                })
        except Exception as e:
            logger.error(f"Error listing alert policies: {e}")
        return policies

    def monitoring_list_notification_channels(self) -> List[Dict[str, Any]]:
        """List all notification channels in the project."""
        channels = []
        try:
            client = monitoring_v3.NotificationChannelServiceClient(credentials=self.__credentials)
            name = f"projects/{self.__project_id}"

            for channel in client.list_notification_channels(name=name):
                channels.append({
                    'name': channel.name.split('/')[-1],
                    'display_name': channel.display_name,
                    'type': channel.type_,
                    'enabled': channel.enabled.value if channel.enabled else True,
                    'labels': dict(channel.labels) if channel.labels else {},
                    'description': channel.description,
                    'verification_status': channel.verification_status.name,
                    'creation_time': channel.creation_record.mutate_time.isoformat() if channel.creation_record else None,
                })
        except Exception as e:
            logger.error(f"Error listing notification channels: {e}")
        return channels

    # ==================== Cloud Functions ====================

    def functions_list(self) -> List[Dict[str, Any]]:
        """List all Cloud Functions in the project."""
        functions = []
        try:
            client = functions_v1.CloudFunctionsServiceClient(credentials=self.__credentials)
            parent = f"projects/{self.__project_id}/locations/-"

            for func in client.list_functions(parent=parent):
                functions.append({
                    'name': func.name.split('/')[-1],
                    'location': func.name.split('/')[3] if '/' in func.name else None,
                    'status': func.status.name,
                    'runtime': func.runtime,
                    'entry_point': func.entry_point,
                    'available_memory_mb': func.available_memory_mb,
                    'timeout': func.timeout.seconds if func.timeout else None,
                    'max_instances': func.max_instances,
                    'min_instances': func.min_instances,
                    'vpc_connector': func.vpc_connector,
                    'trigger_http': bool(func.https_trigger),
                    'trigger_event': func.event_trigger.event_type if func.event_trigger else None,
                    'update_time': func.update_time.isoformat() if func.update_time else None,
                    'labels': dict(func.labels) if func.labels else {},
                    'environment_variables': dict(func.environment_variables) if func.environment_variables else {},
                })
        except Exception as e:
            logger.error(f"Error listing Cloud Functions: {e}")
        return functions

    # ==================== Cloud Run ====================

    def cloudrun_list_services(self) -> List[Dict[str, Any]]:
        """List all Cloud Run services in the project."""
        services = []
        try:
            client = run_v2.ServicesClient(credentials=self.__credentials)
            parent = f"projects/{self.__project_id}/locations/-"

            for service in client.list_services(parent=parent):
                services.append({
                    'name': service.name.split('/')[-1],
                    'location': service.name.split('/')[3] if '/' in service.name else None,
                    'uri': service.uri,
                    'generation': service.generation,
                    'labels': dict(service.labels) if service.labels else {},
                    'annotations': dict(service.annotations) if service.annotations else {},
                    'creator': service.creator,
                    'last_modifier': service.last_modifier,
                    'create_time': service.create_time.isoformat() if service.create_time else None,
                    'update_time': service.update_time.isoformat() if service.update_time else None,
                    'ingress': service.ingress.name if service.ingress else None,
                    'launch_stage': service.launch_stage.name if service.launch_stage else None,
                    'latest_ready_revision': service.latest_ready_revision,
                    'latest_created_revision': service.latest_created_revision,
                    'traffic_statuses': [
                        {
                            'revision': t.revision,
                            'percent': t.percent,
                            'type': t.type_.name if t.type_ else None,
                        }
                        for t in service.traffic_statuses
                    ] if service.traffic_statuses else [],
                })
        except Exception as e:
            logger.error(f"Error listing Cloud Run services: {e}")
        return services

    # ==================== Pub/Sub ====================

    def pubsub_list_topics(self) -> List[Dict[str, Any]]:
        """List all Pub/Sub topics in the project."""
        topics = []
        try:
            publisher = pubsub_v1.PublisherClient(credentials=self.__credentials)
            project_path = f"projects/{self.__project_id}"

            for topic in publisher.list_topics(request={"project": project_path}):
                topics.append({
                    'name': topic.name.split('/')[-1],
                    'full_name': topic.name,
                    'labels': dict(topic.labels) if topic.labels else {},
                    'kms_key_name': topic.kms_key_name,
                    'message_retention_duration': topic.message_retention_duration.seconds if topic.message_retention_duration else None,
                })
        except Exception as e:
            logger.error(f"Error listing Pub/Sub topics: {e}")
        return topics

    def pubsub_list_subscriptions(self) -> List[Dict[str, Any]]:
        """List all Pub/Sub subscriptions in the project."""
        subscriptions = []
        try:
            subscriber = pubsub_v1.SubscriberClient(credentials=self.__credentials)
            project_path = f"projects/{self.__project_id}"

            for sub in subscriber.list_subscriptions(request={"project": project_path}):
                subscriptions.append({
                    'name': sub.name.split('/')[-1],
                    'full_name': sub.name,
                    'topic': sub.topic.split('/')[-1] if sub.topic else None,
                    'topic_full': sub.topic,
                    'push_endpoint': sub.push_config.push_endpoint if sub.push_config else None,
                    'ack_deadline_seconds': sub.ack_deadline_seconds,
                    'retain_acked_messages': sub.retain_acked_messages,
                    'message_retention_duration': sub.message_retention_duration.seconds if sub.message_retention_duration else None,
                    'labels': dict(sub.labels) if sub.labels else {},
                    'enable_message_ordering': sub.enable_message_ordering,
                    'expiration_ttl': sub.expiration_policy.ttl.seconds if sub.expiration_policy and sub.expiration_policy.ttl else None,
                    'filter': sub.filter,
                    'dead_letter_topic': sub.dead_letter_policy.dead_letter_topic if sub.dead_letter_policy else None,
                    'max_delivery_attempts': sub.dead_letter_policy.max_delivery_attempts if sub.dead_letter_policy else None,
                })
        except Exception as e:
            logger.error(f"Error listing Pub/Sub subscriptions: {e}")
        return subscriptions

    # ==================== BigQuery ====================

    def bigquery_list_datasets(self) -> List[Dict[str, Any]]:
        """List all BigQuery datasets in the project."""
        datasets = []
        try:
            bq_client = bigquery.Client(
                project=self.__project_id,
                credentials=self.__credentials
            )
            for dataset in bq_client.list_datasets():
                full_dataset = bq_client.get_dataset(dataset.dataset_id)
                datasets.append({
                    'dataset_id': full_dataset.dataset_id,
                    'full_dataset_id': full_dataset.full_dataset_id,
                    'location': full_dataset.location,
                    'description': full_dataset.description,
                    'creation_time': full_dataset.created.isoformat() if full_dataset.created else None,
                    'modified_time': full_dataset.modified.isoformat() if full_dataset.modified else None,
                    'default_table_expiration_ms': full_dataset.default_table_expiration_ms,
                    'labels': dict(full_dataset.labels) if full_dataset.labels else {},
                })
        except Exception as e:
            logger.error(f"Error listing BigQuery datasets: {e}")
        return datasets

    def bigquery_list_tables(self, dataset_id: str) -> List[Dict[str, Any]]:
        """List all tables in a BigQuery dataset."""
        tables = []
        try:
            bq_client = bigquery.Client(
                project=self.__project_id,
                credentials=self.__credentials
            )
            for table in bq_client.list_tables(dataset_id):
                full_table = bq_client.get_table(table)
                tables.append({
                    'table_id': full_table.table_id,
                    'full_table_id': full_table.full_table_id,
                    'dataset_id': dataset_id,
                    'table_type': full_table.table_type,
                    'description': full_table.description,
                    'num_rows': full_table.num_rows,
                    'num_bytes': full_table.num_bytes,
                    'creation_time': full_table.created.isoformat() if full_table.created else None,
                    'modified_time': full_table.modified.isoformat() if full_table.modified else None,
                    'schema_fields': [
                        {'name': f.name, 'type': f.field_type, 'mode': f.mode}
                        for f in full_table.schema
                    ] if full_table.schema else [],
                    'labels': dict(full_table.labels) if full_table.labels else {},
                    'partitioning_type': full_table.partitioning_type,
                    'clustering_fields': list(full_table.clustering_fields) if full_table.clustering_fields else [],
                })
        except Exception as e:
            logger.error(f"Error listing BigQuery tables for {dataset_id}: {e}")
        return tables

    # ==================== VPC Networking ====================

    def network_list_networks(self) -> List[Dict[str, Any]]:
        """List all VPC networks in the project."""
        networks = []
        try:
            network_client = compute_v1.NetworksClient(credentials=self.__credentials)
            request = compute_v1.ListNetworksRequest(project=self.__project_id)

            for network in network_client.list(request=request):
                networks.append({
                    'name': network.name,
                    'id': str(network.id),
                    'description': network.description,
                    'auto_create_subnetworks': network.auto_create_subnetworks,
                    'routing_mode': network.routing_config.routing_mode if network.routing_config else None,
                    'mtu': network.mtu,
                    'creation_timestamp': network.creation_timestamp,
                    'subnetworks': [s.split('/')[-1] for s in network.subnetworks] if network.subnetworks else [],
                    'peerings': [
                        {
                            'name': p.name,
                            'network': p.network.split('/')[-1] if p.network else None,
                            'state': p.state,
                        }
                        for p in network.peerings
                    ] if network.peerings else [],
                })
        except Exception as e:
            logger.error(f"Error listing VPC networks: {e}")
        return networks

    def network_list_subnetworks(self, region: str = None) -> List[Dict[str, Any]]:
        """List all subnetworks in the project."""
        subnetworks = []
        try:
            subnet_client = compute_v1.SubnetworksClient(credentials=self.__credentials)

            if region:
                request = compute_v1.ListSubnetworksRequest(
                    project=self.__project_id,
                    region=region
                )
                for subnet in subnet_client.list(request=request):
                    subnetworks.append(self._subnetwork_to_dict(subnet, region))
            else:
                agg_request = compute_v1.AggregatedListSubnetworksRequest(
                    project=self.__project_id
                )
                for region_name, response in subnet_client.aggregated_list(request=agg_request):
                    if response.subnetworks:
                        r = region_name.replace('regions/', '')
                        for subnet in response.subnetworks:
                            subnetworks.append(self._subnetwork_to_dict(subnet, r))
        except Exception as e:
            logger.error(f"Error listing subnetworks: {e}")
        return subnetworks

    def _subnetwork_to_dict(self, subnet, region: str) -> Dict[str, Any]:
        """Convert subnetwork to dictionary."""
        return {
            'name': subnet.name,
            'id': str(subnet.id),
            'region': region,
            'network': subnet.network.split('/')[-1] if subnet.network else None,
            'ip_cidr_range': subnet.ip_cidr_range,
            'gateway_address': subnet.gateway_address,
            'private_ip_google_access': subnet.private_ip_google_access,
            'purpose': subnet.purpose,
            'stack_type': subnet.stack_type,
            'creation_timestamp': subnet.creation_timestamp,
            'secondary_ip_ranges': [
                {'name': r.range_name, 'cidr': r.ip_cidr_range}
                for r in subnet.secondary_ip_ranges
            ] if subnet.secondary_ip_ranges else [],
        }

    def network_list_firewall_rules(self) -> List[Dict[str, Any]]:
        """List all firewall rules in the project."""
        rules = []
        try:
            fw_client = compute_v1.FirewallsClient(credentials=self.__credentials)
            request = compute_v1.ListFirewallsRequest(project=self.__project_id)

            for rule in fw_client.list(request=request):
                rules.append({
                    'name': rule.name,
                    'id': str(rule.id),
                    'description': rule.description,
                    'network': rule.network.split('/')[-1] if rule.network else None,
                    'priority': rule.priority,
                    'direction': rule.direction,
                    'disabled': rule.disabled,
                    'source_ranges': list(rule.source_ranges) if rule.source_ranges else [],
                    'destination_ranges': list(rule.destination_ranges) if rule.destination_ranges else [],
                    'source_tags': list(rule.source_tags) if rule.source_tags else [],
                    'target_tags': list(rule.target_tags) if rule.target_tags else [],
                    'allowed': [
                        {'protocol': a.I_p_protocol, 'ports': list(a.ports) if a.ports else []}
                        for a in rule.allowed
                    ] if rule.allowed else [],
                    'denied': [
                        {'protocol': d.I_p_protocol, 'ports': list(d.ports) if d.ports else []}
                        for d in rule.denied
                    ] if rule.denied else [],
                    'creation_timestamp': rule.creation_timestamp,
                })
        except Exception as e:
            logger.error(f"Error listing firewall rules: {e}")
        return rules

    def network_list_forwarding_rules(self) -> List[Dict[str, Any]]:
        """List all forwarding rules (load balancers) in the project."""
        rules = []
        try:
            fr_client = compute_v1.ForwardingRulesClient(credentials=self.__credentials)
            agg_request = compute_v1.AggregatedListForwardingRulesRequest(
                project=self.__project_id
            )

            for region_name, response in fr_client.aggregated_list(request=agg_request):
                if response.forwarding_rules:
                    r = region_name.replace('regions/', '')
                    for rule in response.forwarding_rules:
                        rules.append({
                            'name': rule.name,
                            'id': str(rule.id),
                            'region': r,
                            'ip_address': rule.I_p_address,
                            'ip_protocol': rule.I_p_protocol,
                            'port_range': rule.port_range,
                            'ports': list(rule.ports) if rule.ports else [],
                            'target': rule.target.split('/')[-1] if rule.target else None,
                            'load_balancing_scheme': rule.load_balancing_scheme,
                            'network': rule.network.split('/')[-1] if rule.network else None,
                            'subnetwork': rule.subnetwork.split('/')[-1] if rule.subnetwork else None,
                            'creation_timestamp': rule.creation_timestamp,
                            'labels': dict(rule.labels) if rule.labels else {},
                        })
        except Exception as e:
            logger.error(f"Error listing forwarding rules: {e}")
        return rules

    # ==================== Secret Manager ====================

    def secrets_list(self) -> List[Dict[str, Any]]:
        """List all secrets in Secret Manager."""
        secrets = []
        try:
            client = secretmanager.SecretManagerServiceClient(credentials=self.__credentials)
            parent = f"projects/{self.__project_id}"

            for secret in client.list_secrets(request={"parent": parent}):
                secrets.append({
                    'name': secret.name.split('/')[-1],
                    'create_time': secret.create_time.isoformat() if secret.create_time else None,
                    'labels': dict(secret.labels) if secret.labels else {},
                    'replication': secret.replication.automatic.customer_managed_encryption.kms_key_name if secret.replication.automatic and secret.replication.automatic.customer_managed_encryption else 'automatic',
                    'topics': [t.name for t in secret.topics] if secret.topics else [],
                    'expire_time': secret.expire_time.isoformat() if secret.expire_time else None,
                    'version_aliases': dict(secret.version_aliases) if secret.version_aliases else {},
                })
        except Exception as e:
            logger.error(f"Error listing secrets: {e}")
        return secrets

    # ==================== IAM ====================

    def iam_list_service_accounts(self) -> List[Dict[str, Any]]:
        """List all service accounts in the project."""
        accounts = []
        try:
            service = build('iam', 'v1', credentials=self.__credentials)
            name = f"projects/{self.__project_id}"
            request = service.projects().serviceAccounts().list(name=name)

            while request is not None:
                response = request.execute()
                for account in response.get('accounts', []):
                    accounts.append({
                        'name': account.get('name', '').split('/')[-1],
                        'email': account.get('email'),
                        'unique_id': account.get('uniqueId'),
                        'display_name': account.get('displayName'),
                        'description': account.get('description'),
                        'disabled': account.get('disabled', False),
                    })
                request = service.projects().serviceAccounts().list_next(
                    previous_request=request,
                    previous_response=response
                )
        except Exception as e:
            logger.error(f"Error listing service accounts: {e}")
        return accounts

    # ==================== Cloud Logging ====================

    def logging_list_sinks(self) -> List[Dict[str, Any]]:
        """List all log sinks in the project."""
        sinks = []
        try:
            client = cloud_logging.Client(
                project=self.__project_id,
                credentials=self.__credentials
            )
            for sink in client.list_sinks():
                sinks.append({
                    'name': sink.name,
                    'destination': sink.destination,
                    'filter': sink.filter_,
                    'writer_identity': sink.writer_identity,
                    'include_children': sink.include_children,
                    'disabled': sink.disabled,
                })
        except Exception as e:
            logger.error(f"Error listing log sinks: {e}")
        return sinks

    def logging_list_metrics(self) -> List[Dict[str, Any]]:
        """List all log-based metrics in the project."""
        metrics = []
        try:
            client = cloud_logging.Client(
                project=self.__project_id,
                credentials=self.__credentials
            )
            for metric in client.list_metrics():
                metrics.append({
                    'name': metric.name,
                    'description': metric.description,
                    'filter': metric.filter_,
                })
        except Exception as e:
            logger.error(f"Error listing log metrics: {e}")
        return metrics
