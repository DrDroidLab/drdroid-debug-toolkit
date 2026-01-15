"""
GCP API Processor for non-GKE Google Cloud Platform services.

Uses google-api-python-client (unified SDK) for all GCP services instead of
individual google-cloud-* packages, reducing dependency overhead.

Handles: Compute Engine, Cloud Storage, Cloud SQL, Memorystore Redis,
Cloud Functions, Cloud Run, Pub/Sub, BigQuery, Monitoring, Networking,
Secret Manager, IAM, and Logging services.
"""
import json
import logging
from typing import List, Dict, Any, Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

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
    """API Processor for GCP non-GKE services using google-api-python-client."""

    def __init__(self, project_id: str, service_account_json: str):
        self.__project_id = project_id
        self.__service_account_json = service_account_json
        self.__credentials = get_gcp_credentials(service_account_json)
        # Cache for service clients
        self._service_cache = {}

    def _get_service(self, service_name: str, version: str):
        """Get or create a cached service client."""
        cache_key = f"{service_name}_{version}"
        if cache_key not in self._service_cache:
            self._service_cache[cache_key] = build(
                service_name, version, credentials=self.__credentials, cache_discovery=False
            )
        return self._service_cache[cache_key]

    def test_connection(self) -> bool:
        """Test connection to GCP using Cloud Resource Manager API (commonly enabled by default)."""
        try:
            # Use Cloud Resource Manager API which is typically enabled by default
            service = self._get_service('cloudresourcemanager', 'v1')
            request = service.projects().get(projectId=self.__project_id)
            response = request.execute()
            return response.get('projectId') == self.__project_id
        except Exception as e:
            logger.error(f"GCP connection test failed: {e}")
            raise e

    # ==================== Compute Engine ====================

    def compute_list_instances(self, zone: str = None) -> List[Dict[str, Any]]:
        """List all Compute Engine instances in the project."""
        instances = []
        try:
            service = self._get_service('compute', 'v1')

            if zone:
                request = service.instances().list(project=self.__project_id, zone=zone)
                while request is not None:
                    response = request.execute()
                    for instance in response.get('items', []):
                        instances.append(self._instance_to_dict(instance, zone))
                    request = service.instances().list_next(previous_request=request, previous_response=response)
            else:
                request = service.instances().aggregatedList(project=self.__project_id)
                while request is not None:
                    response = request.execute()
                    for zone_name, zone_data in response.get('items', {}).items():
                        if 'instances' in zone_data:
                            z = zone_name.replace('zones/', '')
                            for instance in zone_data['instances']:
                                instances.append(self._instance_to_dict(instance, z))
                    request = service.instances().aggregatedList_next(previous_request=request, previous_response=response)
        except HttpError as e:
            logger.error(f"Error listing compute instances: {e}")
        except Exception as e:
            logger.error(f"Error listing compute instances: {e}")
        return instances

    def _instance_to_dict(self, instance: Dict, zone: str) -> Dict[str, Any]:
        """Convert Compute Engine instance to dictionary."""
        network_interfaces = []
        for ni in instance.get('networkInterfaces', []):
            access_configs = ni.get('accessConfigs', [])
            network_interfaces.append({
                'network': ni.get('network', '').split('/')[-1] if ni.get('network') else None,
                'subnetwork': ni.get('subnetwork', '').split('/')[-1] if ni.get('subnetwork') else None,
                'internal_ip': ni.get('networkIP'),
                'external_ip': access_configs[0].get('natIP') if access_configs else None,
            })

        disks = []
        for disk in instance.get('disks', []):
            disks.append({
                'source': disk.get('source', '').split('/')[-1] if disk.get('source') else None,
                'boot': disk.get('boot', False),
                'auto_delete': disk.get('autoDelete', False),
            })

        return {
            'name': instance.get('name'),
            'id': instance.get('id'),
            'zone': zone,
            'machine_type': instance.get('machineType', '').split('/')[-1] if instance.get('machineType') else None,
            'status': instance.get('status'),
            'creation_timestamp': instance.get('creationTimestamp'),
            'network_interfaces': network_interfaces,
            'disks': disks,
            'labels': instance.get('labels', {}),
            'tags': instance.get('tags', {}).get('items', []),
        }

    def compute_list_instance_groups(self, zone: str = None) -> List[Dict[str, Any]]:
        """List all Instance Groups in the project."""
        groups = []
        try:
            service = self._get_service('compute', 'v1')

            if zone:
                request = service.instanceGroups().list(project=self.__project_id, zone=zone)
                while request is not None:
                    response = request.execute()
                    for group in response.get('items', []):
                        groups.append(self._instance_group_to_dict(group, zone))
                    request = service.instanceGroups().list_next(previous_request=request, previous_response=response)
            else:
                request = service.instanceGroups().aggregatedList(project=self.__project_id)
                while request is not None:
                    response = request.execute()
                    for zone_name, zone_data in response.get('items', {}).items():
                        if 'instanceGroups' in zone_data:
                            z = zone_name.replace('zones/', '')
                            for group in zone_data['instanceGroups']:
                                groups.append(self._instance_group_to_dict(group, z))
                    request = service.instanceGroups().aggregatedList_next(previous_request=request, previous_response=response)
        except HttpError as e:
            logger.error(f"Error listing instance groups: {e}")
        except Exception as e:
            logger.error(f"Error listing instance groups: {e}")
        return groups

    def _instance_group_to_dict(self, group: Dict, zone: str) -> Dict[str, Any]:
        """Convert Instance Group to dictionary."""
        return {
            'name': group.get('name'),
            'id': group.get('id'),
            'zone': zone,
            'description': group.get('description'),
            'size': group.get('size'),
            'creation_timestamp': group.get('creationTimestamp'),
            'network': group.get('network', '').split('/')[-1] if group.get('network') else None,
            'subnetwork': group.get('subnetwork', '').split('/')[-1] if group.get('subnetwork') else None,
        }

    # ==================== Cloud Storage ====================

    def storage_list_buckets(self) -> List[Dict[str, Any]]:
        """List all Cloud Storage buckets in the project."""
        buckets = []
        try:
            service = self._get_service('storage', 'v1')
            request = service.buckets().list(project=self.__project_id)

            while request is not None:
                response = request.execute()
                for bucket in response.get('items', []):
                    lifecycle_rules = []
                    if bucket.get('lifecycle', {}).get('rule'):
                        for rule in bucket['lifecycle']['rule']:
                            lifecycle_rules.append({
                                'action': rule.get('action'),
                                'condition': rule.get('condition'),
                            })

                    buckets.append({
                        'name': bucket.get('name'),
                        'id': bucket.get('id'),
                        'location': bucket.get('location'),
                        'location_type': bucket.get('locationType'),
                        'storage_class': bucket.get('storageClass'),
                        'creation_time': bucket.get('timeCreated'),
                        'versioning_enabled': bucket.get('versioning', {}).get('enabled', False),
                        'labels': bucket.get('labels', {}),
                        'lifecycle_rules': lifecycle_rules,
                        'cors': bucket.get('cors', []),
                        'default_event_based_hold': bucket.get('defaultEventBasedHold', False),
                        'requester_pays': bucket.get('billing', {}).get('requesterPays', False),
                    })
                request = service.buckets().list_next(previous_request=request, previous_response=response)
        except HttpError as e:
            logger.error(f"Error listing storage buckets: {e}")
        except Exception as e:
            logger.error(f"Error listing storage buckets: {e}")
        return buckets

    # ==================== Cloud SQL ====================

    def sql_list_instances(self) -> List[Dict[str, Any]]:
        """List all Cloud SQL instances in the project."""
        instances = []
        try:
            service = self._get_service('sqladmin', 'v1beta4')
            request = service.instances().list(project=self.__project_id)
            response = request.execute()

            for instance in response.get('items', []):
                settings = instance.get('settings', {})
                backup_config = settings.get('backupConfiguration', {})
                instances.append({
                    'name': instance.get('name'),
                    'database_version': instance.get('databaseVersion'),
                    'region': instance.get('region'),
                    'state': instance.get('state'),
                    'tier': settings.get('tier'),
                    'storage_auto_resize': settings.get('storageAutoResize'),
                    'data_disk_size_gb': settings.get('dataDiskSizeGb'),
                    'data_disk_type': settings.get('dataDiskType'),
                    'availability_type': settings.get('availabilityType'),
                    'backup_enabled': backup_config.get('enabled'),
                    'ip_addresses': [
                        {'type': ip.get('type'), 'ip_address': ip.get('ipAddress')}
                        for ip in instance.get('ipAddresses', [])
                    ],
                    'connection_name': instance.get('connectionName'),
                    'gce_zone': instance.get('gceZone'),
                    'secondary_gce_zone': instance.get('secondaryGceZone'),
                    'creation_time': instance.get('createTime'),
                    'labels': settings.get('userLabels', {}),
                })
        except HttpError as e:
            logger.error(f"Error listing Cloud SQL instances: {e}")
        except Exception as e:
            logger.error(f"Error listing Cloud SQL instances: {e}")
        return instances

    def sql_list_databases(self, instance_name: str) -> List[Dict[str, Any]]:
        """List all databases in a Cloud SQL instance."""
        databases = []
        try:
            service = self._get_service('sqladmin', 'v1beta4')
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
        except HttpError as e:
            logger.error(f"Error listing databases for {instance_name}: {e}")
        except Exception as e:
            logger.error(f"Error listing databases for {instance_name}: {e}")
        return databases

    # ==================== Memorystore Redis ====================

    def redis_list_instances(self) -> List[Dict[str, Any]]:
        """List all Memorystore Redis instances in the project."""
        instances = []
        try:
            service = self._get_service('redis', 'v1')
            parent = f"projects/{self.__project_id}/locations/-"
            request = service.projects().locations().instances().list(parent=parent)

            while request is not None:
                response = request.execute()
                for instance in response.get('instances', []):
                    persistence_config = instance.get('persistenceConfig', {})
                    instances.append({
                        'name': instance.get('name', '').split('/')[-1],
                        'display_name': instance.get('displayName'),
                        'location': instance.get('locationId'),
                        'state': instance.get('state'),
                        'tier': instance.get('tier'),
                        'memory_size_gb': instance.get('memorySizeGb'),
                        'redis_version': instance.get('redisVersion'),
                        'host': instance.get('host'),
                        'port': instance.get('port'),
                        'current_location_id': instance.get('currentLocationId'),
                        'create_time': instance.get('createTime'),
                        'authorized_network': instance.get('authorizedNetwork'),
                        'persistence_mode': persistence_config.get('persistenceMode'),
                        'labels': instance.get('labels', {}),
                        'replica_count': instance.get('replicaCount'),
                        'read_replicas_mode': instance.get('readReplicasMode'),
                    })
                request = service.projects().locations().instances().list_next(
                    previous_request=request, previous_response=response
                )
        except HttpError as e:
            logger.error(f"Error listing Redis instances: {e}")
        except Exception as e:
            logger.error(f"Error listing Redis instances: {e}")
        return instances

    # ==================== Cloud Monitoring ====================

    def monitoring_list_alert_policies(self) -> List[Dict[str, Any]]:
        """List all alert policies in the project."""
        policies = []
        try:
            service = self._get_service('monitoring', 'v3')
            name = f"projects/{self.__project_id}"
            request = service.projects().alertPolicies().list(name=name)

            while request is not None:
                response = request.execute()
                for policy in response.get('alertPolicies', []):
                    conditions = []
                    for c in policy.get('conditions', []):
                        conditions.append({
                            'name': c.get('name'),
                            'display_name': c.get('displayName'),
                        })

                    creation_record = policy.get('creationRecord', {})
                    policies.append({
                        'name': policy.get('name', '').split('/')[-1],
                        'display_name': policy.get('displayName'),
                        'enabled': policy.get('enabled', True),
                        'combiner': policy.get('combiner'),
                        'conditions': conditions,
                        'notification_channels': policy.get('notificationChannels', []),
                        'documentation': policy.get('documentation', {}).get('content'),
                        'labels': policy.get('userLabels', {}),
                        'creation_time': creation_record.get('mutateTime'),
                    })
                request = service.projects().alertPolicies().list_next(
                    previous_request=request, previous_response=response
                )
        except HttpError as e:
            logger.error(f"Error listing alert policies: {e}")
        except Exception as e:
            logger.error(f"Error listing alert policies: {e}")
        return policies

    def monitoring_list_notification_channels(self) -> List[Dict[str, Any]]:
        """List all notification channels in the project."""
        channels = []
        try:
            service = self._get_service('monitoring', 'v3')
            name = f"projects/{self.__project_id}"
            request = service.projects().notificationChannels().list(name=name)

            while request is not None:
                response = request.execute()
                for channel in response.get('notificationChannels', []):
                    creation_record = channel.get('creationRecord', {})
                    channels.append({
                        'name': channel.get('name', '').split('/')[-1],
                        'display_name': channel.get('displayName'),
                        'type': channel.get('type'),
                        'enabled': channel.get('enabled', True),
                        'labels': channel.get('labels', {}),
                        'description': channel.get('description'),
                        'verification_status': channel.get('verificationStatus'),
                        'creation_time': creation_record.get('mutateTime'),
                    })
                request = service.projects().notificationChannels().list_next(
                    previous_request=request, previous_response=response
                )
        except HttpError as e:
            logger.error(f"Error listing notification channels: {e}")
        except Exception as e:
            logger.error(f"Error listing notification channels: {e}")
        return channels

    # ==================== Cloud Functions ====================

    def functions_list(self) -> List[Dict[str, Any]]:
        """List all Cloud Functions in the project."""
        functions = []
        try:
            service = self._get_service('cloudfunctions', 'v1')
            parent = f"projects/{self.__project_id}/locations/-"
            request = service.projects().locations().functions().list(parent=parent)

            while request is not None:
                response = request.execute()
                for func in response.get('functions', []):
                    name_parts = func.get('name', '').split('/')
                    location = name_parts[3] if len(name_parts) > 3 else None
                    func_name = name_parts[-1] if name_parts else None

                    timeout_str = func.get('timeout', '0s')
                    timeout_seconds = int(timeout_str.replace('s', '')) if timeout_str else None

                    functions.append({
                        'name': func_name,
                        'location': location,
                        'status': func.get('status'),
                        'runtime': func.get('runtime'),
                        'entry_point': func.get('entryPoint'),
                        'available_memory_mb': func.get('availableMemoryMb'),
                        'timeout': timeout_seconds,
                        'max_instances': func.get('maxInstances'),
                        'min_instances': func.get('minInstances'),
                        'vpc_connector': func.get('vpcConnector'),
                        'trigger_http': 'httpsTrigger' in func,
                        'trigger_event': func.get('eventTrigger', {}).get('eventType'),
                        'update_time': func.get('updateTime'),
                        'labels': func.get('labels', {}),
                        'environment_variables': func.get('environmentVariables', {}),
                    })
                request = service.projects().locations().functions().list_next(
                    previous_request=request, previous_response=response
                )
        except HttpError as e:
            logger.error(f"Error listing Cloud Functions: {e}")
        except Exception as e:
            logger.error(f"Error listing Cloud Functions: {e}")
        return functions

    # ==================== Cloud Run ====================

    def cloudrun_list_services(self) -> List[Dict[str, Any]]:
        """List all Cloud Run services in the project."""
        services = []
        try:
            service = self._get_service('run', 'v2')
            parent = f"projects/{self.__project_id}/locations/-"
            request = service.projects().locations().services().list(parent=parent)

            while request is not None:
                response = request.execute()
                for svc in response.get('services', []):
                    name_parts = svc.get('name', '').split('/')
                    location = name_parts[3] if len(name_parts) > 3 else None
                    svc_name = name_parts[-1] if name_parts else None

                    traffic_statuses = []
                    for t in svc.get('trafficStatuses', []):
                        traffic_statuses.append({
                            'revision': t.get('revision'),
                            'percent': t.get('percent'),
                            'type': t.get('type'),
                        })

                    services.append({
                        'name': svc_name,
                        'location': location,
                        'uri': svc.get('uri'),
                        'generation': svc.get('generation'),
                        'labels': svc.get('labels', {}),
                        'annotations': svc.get('annotations', {}),
                        'creator': svc.get('creator'),
                        'last_modifier': svc.get('lastModifier'),
                        'create_time': svc.get('createTime'),
                        'update_time': svc.get('updateTime'),
                        'ingress': svc.get('ingress'),
                        'launch_stage': svc.get('launchStage'),
                        'latest_ready_revision': svc.get('latestReadyRevision'),
                        'latest_created_revision': svc.get('latestCreatedRevision'),
                        'traffic_statuses': traffic_statuses,
                    })
                request = service.projects().locations().services().list_next(
                    previous_request=request, previous_response=response
                )
        except HttpError as e:
            logger.error(f"Error listing Cloud Run services: {e}")
        except Exception as e:
            logger.error(f"Error listing Cloud Run services: {e}")
        return services

    # ==================== Pub/Sub ====================

    def pubsub_list_topics(self) -> List[Dict[str, Any]]:
        """List all Pub/Sub topics in the project."""
        topics = []
        try:
            service = self._get_service('pubsub', 'v1')
            project_path = f"projects/{self.__project_id}"
            request = service.projects().topics().list(project=project_path)

            while request is not None:
                response = request.execute()
                for topic in response.get('topics', []):
                    retention_duration = topic.get('messageRetentionDuration')
                    retention_seconds = None
                    if retention_duration:
                        retention_seconds = int(retention_duration.replace('s', ''))

                    topics.append({
                        'name': topic.get('name', '').split('/')[-1],
                        'full_name': topic.get('name'),
                        'labels': topic.get('labels', {}),
                        'kms_key_name': topic.get('kmsKeyName'),
                        'message_retention_duration': retention_seconds,
                    })
                request = service.projects().topics().list_next(
                    previous_request=request, previous_response=response
                )
        except HttpError as e:
            logger.error(f"Error listing Pub/Sub topics: {e}")
        except Exception as e:
            logger.error(f"Error listing Pub/Sub topics: {e}")
        return topics

    def pubsub_list_subscriptions(self) -> List[Dict[str, Any]]:
        """List all Pub/Sub subscriptions in the project."""
        subscriptions = []
        try:
            service = self._get_service('pubsub', 'v1')
            project_path = f"projects/{self.__project_id}"
            request = service.projects().subscriptions().list(project=project_path)

            while request is not None:
                response = request.execute()
                for sub in response.get('subscriptions', []):
                    push_config = sub.get('pushConfig', {})
                    dead_letter_policy = sub.get('deadLetterPolicy', {})
                    expiration_policy = sub.get('expirationPolicy', {})

                    retention_duration = sub.get('messageRetentionDuration')
                    retention_seconds = None
                    if retention_duration:
                        retention_seconds = int(retention_duration.replace('s', ''))

                    expiration_ttl = None
                    if expiration_policy.get('ttl'):
                        expiration_ttl = int(expiration_policy['ttl'].replace('s', ''))

                    subscriptions.append({
                        'name': sub.get('name', '').split('/')[-1],
                        'full_name': sub.get('name'),
                        'topic': sub.get('topic', '').split('/')[-1] if sub.get('topic') else None,
                        'topic_full': sub.get('topic'),
                        'push_endpoint': push_config.get('pushEndpoint'),
                        'ack_deadline_seconds': sub.get('ackDeadlineSeconds'),
                        'retain_acked_messages': sub.get('retainAckedMessages', False),
                        'message_retention_duration': retention_seconds,
                        'labels': sub.get('labels', {}),
                        'enable_message_ordering': sub.get('enableMessageOrdering', False),
                        'expiration_ttl': expiration_ttl,
                        'filter': sub.get('filter'),
                        'dead_letter_topic': dead_letter_policy.get('deadLetterTopic'),
                        'max_delivery_attempts': dead_letter_policy.get('maxDeliveryAttempts'),
                    })
                request = service.projects().subscriptions().list_next(
                    previous_request=request, previous_response=response
                )
        except HttpError as e:
            logger.error(f"Error listing Pub/Sub subscriptions: {e}")
        except Exception as e:
            logger.error(f"Error listing Pub/Sub subscriptions: {e}")
        return subscriptions

    # ==================== BigQuery ====================

    def bigquery_list_datasets(self) -> List[Dict[str, Any]]:
        """List all BigQuery datasets in the project."""
        datasets = []
        try:
            service = self._get_service('bigquery', 'v2')
            request = service.datasets().list(projectId=self.__project_id)

            while request is not None:
                response = request.execute()
                for dataset_ref in response.get('datasets', []):
                    dataset_id = dataset_ref.get('datasetReference', {}).get('datasetId')
                    if dataset_id:
                        # Get full dataset details
                        try:
                            full_dataset = service.datasets().get(
                                projectId=self.__project_id, datasetId=dataset_id
                            ).execute()
                            datasets.append({
                                'dataset_id': full_dataset.get('datasetReference', {}).get('datasetId'),
                                'full_dataset_id': f"{self.__project_id}:{dataset_id}",
                                'location': full_dataset.get('location'),
                                'description': full_dataset.get('description'),
                                'creation_time': full_dataset.get('creationTime'),
                                'modified_time': full_dataset.get('lastModifiedTime'),
                                'default_table_expiration_ms': full_dataset.get('defaultTableExpirationMs'),
                                'labels': full_dataset.get('labels', {}),
                            })
                        except HttpError as e:
                            logger.warning(f"Could not get details for dataset {dataset_id}: {e}")
                request = service.datasets().list_next(
                    previous_request=request, previous_response=response
                )
        except HttpError as e:
            logger.error(f"Error listing BigQuery datasets: {e}")
        except Exception as e:
            logger.error(f"Error listing BigQuery datasets: {e}")
        return datasets

    def bigquery_list_tables(self, dataset_id: str) -> List[Dict[str, Any]]:
        """List all tables in a BigQuery dataset."""
        tables = []
        try:
            service = self._get_service('bigquery', 'v2')
            request = service.tables().list(projectId=self.__project_id, datasetId=dataset_id)

            while request is not None:
                response = request.execute()
                for table_ref in response.get('tables', []):
                    table_id = table_ref.get('tableReference', {}).get('tableId')
                    if table_id:
                        try:
                            full_table = service.tables().get(
                                projectId=self.__project_id, datasetId=dataset_id, tableId=table_id
                            ).execute()

                            schema_fields = []
                            for f in full_table.get('schema', {}).get('fields', []):
                                schema_fields.append({
                                    'name': f.get('name'),
                                    'type': f.get('type'),
                                    'mode': f.get('mode'),
                                })

                            tables.append({
                                'table_id': table_id,
                                'full_table_id': f"{self.__project_id}:{dataset_id}.{table_id}",
                                'dataset_id': dataset_id,
                                'table_type': full_table.get('type'),
                                'description': full_table.get('description'),
                                'num_rows': full_table.get('numRows'),
                                'num_bytes': full_table.get('numBytes'),
                                'creation_time': full_table.get('creationTime'),
                                'modified_time': full_table.get('lastModifiedTime'),
                                'schema_fields': schema_fields,
                                'labels': full_table.get('labels', {}),
                                'partitioning_type': full_table.get('timePartitioning', {}).get('type'),
                                'clustering_fields': full_table.get('clustering', {}).get('fields', []),
                            })
                        except HttpError as e:
                            logger.warning(f"Could not get details for table {table_id}: {e}")
                request = service.tables().list_next(
                    previous_request=request, previous_response=response
                )
        except HttpError as e:
            logger.error(f"Error listing BigQuery tables for {dataset_id}: {e}")
        except Exception as e:
            logger.error(f"Error listing BigQuery tables for {dataset_id}: {e}")
        return tables

    # ==================== VPC Networking ====================

    def network_list_networks(self) -> List[Dict[str, Any]]:
        """List all VPC networks in the project."""
        networks = []
        try:
            service = self._get_service('compute', 'v1')
            request = service.networks().list(project=self.__project_id)

            while request is not None:
                response = request.execute()
                for network in response.get('items', []):
                    peerings = []
                    for p in network.get('peerings', []):
                        peerings.append({
                            'name': p.get('name'),
                            'network': p.get('network', '').split('/')[-1] if p.get('network') else None,
                            'state': p.get('state'),
                        })

                    networks.append({
                        'name': network.get('name'),
                        'id': network.get('id'),
                        'description': network.get('description'),
                        'auto_create_subnetworks': network.get('autoCreateSubnetworks', False),
                        'routing_mode': network.get('routingConfig', {}).get('routingMode'),
                        'mtu': network.get('mtu'),
                        'creation_timestamp': network.get('creationTimestamp'),
                        'subnetworks': [s.split('/')[-1] for s in network.get('subnetworks', [])],
                        'peerings': peerings,
                    })
                request = service.networks().list_next(previous_request=request, previous_response=response)
        except HttpError as e:
            logger.error(f"Error listing VPC networks: {e}")
        except Exception as e:
            logger.error(f"Error listing VPC networks: {e}")
        return networks

    def network_list_subnetworks(self, region: str = None) -> List[Dict[str, Any]]:
        """List all subnetworks in the project."""
        subnetworks = []
        try:
            service = self._get_service('compute', 'v1')

            if region:
                request = service.subnetworks().list(project=self.__project_id, region=region)
                while request is not None:
                    response = request.execute()
                    for subnet in response.get('items', []):
                        subnetworks.append(self._subnetwork_to_dict(subnet, region))
                    request = service.subnetworks().list_next(previous_request=request, previous_response=response)
            else:
                request = service.subnetworks().aggregatedList(project=self.__project_id)
                while request is not None:
                    response = request.execute()
                    for region_name, region_data in response.get('items', {}).items():
                        if 'subnetworks' in region_data:
                            r = region_name.replace('regions/', '')
                            for subnet in region_data['subnetworks']:
                                subnetworks.append(self._subnetwork_to_dict(subnet, r))
                    request = service.subnetworks().aggregatedList_next(previous_request=request, previous_response=response)
        except HttpError as e:
            logger.error(f"Error listing subnetworks: {e}")
        except Exception as e:
            logger.error(f"Error listing subnetworks: {e}")
        return subnetworks

    def _subnetwork_to_dict(self, subnet: Dict, region: str) -> Dict[str, Any]:
        """Convert subnetwork to dictionary."""
        secondary_ranges = []
        for r in subnet.get('secondaryIpRanges', []):
            secondary_ranges.append({
                'name': r.get('rangeName'),
                'cidr': r.get('ipCidrRange'),
            })

        return {
            'name': subnet.get('name'),
            'id': subnet.get('id'),
            'region': region,
            'network': subnet.get('network', '').split('/')[-1] if subnet.get('network') else None,
            'ip_cidr_range': subnet.get('ipCidrRange'),
            'gateway_address': subnet.get('gatewayAddress'),
            'private_ip_google_access': subnet.get('privateIpGoogleAccess', False),
            'purpose': subnet.get('purpose'),
            'stack_type': subnet.get('stackType'),
            'creation_timestamp': subnet.get('creationTimestamp'),
            'secondary_ip_ranges': secondary_ranges,
        }

    def network_list_firewall_rules(self) -> List[Dict[str, Any]]:
        """List all firewall rules in the project."""
        rules = []
        try:
            service = self._get_service('compute', 'v1')
            request = service.firewalls().list(project=self.__project_id)

            while request is not None:
                response = request.execute()
                for rule in response.get('items', []):
                    allowed = []
                    for a in rule.get('allowed', []):
                        allowed.append({
                            'protocol': a.get('IPProtocol'),
                            'ports': a.get('ports', []),
                        })

                    denied = []
                    for d in rule.get('denied', []):
                        denied.append({
                            'protocol': d.get('IPProtocol'),
                            'ports': d.get('ports', []),
                        })

                    rules.append({
                        'name': rule.get('name'),
                        'id': rule.get('id'),
                        'description': rule.get('description'),
                        'network': rule.get('network', '').split('/')[-1] if rule.get('network') else None,
                        'priority': rule.get('priority'),
                        'direction': rule.get('direction'),
                        'disabled': rule.get('disabled', False),
                        'source_ranges': rule.get('sourceRanges', []),
                        'destination_ranges': rule.get('destinationRanges', []),
                        'source_tags': rule.get('sourceTags', []),
                        'target_tags': rule.get('targetTags', []),
                        'allowed': allowed,
                        'denied': denied,
                        'creation_timestamp': rule.get('creationTimestamp'),
                    })
                request = service.firewalls().list_next(previous_request=request, previous_response=response)
        except HttpError as e:
            logger.error(f"Error listing firewall rules: {e}")
        except Exception as e:
            logger.error(f"Error listing firewall rules: {e}")
        return rules

    def network_list_forwarding_rules(self) -> List[Dict[str, Any]]:
        """List all forwarding rules (load balancers) in the project."""
        rules = []
        try:
            service = self._get_service('compute', 'v1')
            request = service.forwardingRules().aggregatedList(project=self.__project_id)

            while request is not None:
                response = request.execute()
                for region_name, region_data in response.get('items', {}).items():
                    if 'forwardingRules' in region_data:
                        r = region_name.replace('regions/', '')
                        for rule in region_data['forwardingRules']:
                            rules.append({
                                'name': rule.get('name'),
                                'id': rule.get('id'),
                                'region': r,
                                'ip_address': rule.get('IPAddress'),
                                'ip_protocol': rule.get('IPProtocol'),
                                'port_range': rule.get('portRange'),
                                'ports': rule.get('ports', []),
                                'target': rule.get('target', '').split('/')[-1] if rule.get('target') else None,
                                'load_balancing_scheme': rule.get('loadBalancingScheme'),
                                'network': rule.get('network', '').split('/')[-1] if rule.get('network') else None,
                                'subnetwork': rule.get('subnetwork', '').split('/')[-1] if rule.get('subnetwork') else None,
                                'creation_timestamp': rule.get('creationTimestamp'),
                                'labels': rule.get('labels', {}),
                            })
                request = service.forwardingRules().aggregatedList_next(previous_request=request, previous_response=response)
        except HttpError as e:
            logger.error(f"Error listing forwarding rules: {e}")
        except Exception as e:
            logger.error(f"Error listing forwarding rules: {e}")
        return rules

    # ==================== Secret Manager ====================

    def secrets_list(self) -> List[Dict[str, Any]]:
        """List all secrets in Secret Manager."""
        secrets = []
        try:
            service = self._get_service('secretmanager', 'v1')
            parent = f"projects/{self.__project_id}"
            request = service.projects().secrets().list(parent=parent)

            while request is not None:
                response = request.execute()
                for secret in response.get('secrets', []):
                    replication = secret.get('replication', {})
                    replication_type = 'automatic' if 'automatic' in replication else 'user_managed'

                    secrets.append({
                        'name': secret.get('name', '').split('/')[-1],
                        'create_time': secret.get('createTime'),
                        'labels': secret.get('labels', {}),
                        'replication': replication_type,
                        'topics': [t.get('name') for t in secret.get('topics', [])],
                        'expire_time': secret.get('expireTime'),
                        'version_aliases': secret.get('versionAliases', {}),
                    })
                request = service.projects().secrets().list_next(
                    previous_request=request, previous_response=response
                )
        except HttpError as e:
            logger.error(f"Error listing secrets: {e}")
        except Exception as e:
            logger.error(f"Error listing secrets: {e}")
        return secrets

    # ==================== IAM ====================

    def iam_list_service_accounts(self) -> List[Dict[str, Any]]:
        """List all service accounts in the project."""
        accounts = []
        try:
            service = self._get_service('iam', 'v1')
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
        except HttpError as e:
            logger.error(f"Error listing service accounts: {e}")
        except Exception as e:
            logger.error(f"Error listing service accounts: {e}")
        return accounts

    # ==================== Cloud Logging ====================

    def logging_list_sinks(self) -> List[Dict[str, Any]]:
        """List all log sinks in the project."""
        sinks = []
        try:
            service = self._get_service('logging', 'v2')
            parent = f"projects/{self.__project_id}"
            request = service.projects().sinks().list(parent=parent)

            while request is not None:
                response = request.execute()
                for sink in response.get('sinks', []):
                    sinks.append({
                        'name': sink.get('name'),
                        'destination': sink.get('destination'),
                        'filter': sink.get('filter'),
                        'writer_identity': sink.get('writerIdentity'),
                        'include_children': sink.get('includeChildren', False),
                        'disabled': sink.get('disabled', False),
                    })
                request = service.projects().sinks().list_next(
                    previous_request=request, previous_response=response
                )
        except HttpError as e:
            logger.error(f"Error listing log sinks: {e}")
        except Exception as e:
            logger.error(f"Error listing log sinks: {e}")
        return sinks

    def logging_list_metrics(self) -> List[Dict[str, Any]]:
        """List all log-based metrics in the project."""
        metrics = []
        try:
            service = self._get_service('logging', 'v2')
            parent = f"projects/{self.__project_id}"
            request = service.projects().metrics().list(parent=parent)

            while request is not None:
                response = request.execute()
                for metric in response.get('metrics', []):
                    metrics.append({
                        'name': metric.get('name'),
                        'description': metric.get('description'),
                        'filter': metric.get('filter'),
                    })
                request = service.projects().metrics().list_next(
                    previous_request=request, previous_response=response
                )
        except HttpError as e:
            logger.error(f"Error listing log metrics: {e}")
        except Exception as e:
            logger.error(f"Error listing log metrics: {e}")
        return metrics
