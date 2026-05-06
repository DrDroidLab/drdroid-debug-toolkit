import logging
from datetime import datetime, date

from core.integrations.source_metadata_extractor import SourceMetadataExtractor
from core.integrations.source_api_processors.gcm_api_processor import GcmApiProcessor
from core.integrations.source_api_processors.gcp_api_processor import GcpApiProcessor
from core.protos.base_pb2 import Source, SourceModelType
from core.utils.logging_utils import log_function_call
from core.utils.static_mappings import GCM_SERVICE_DASHBOARD_QUERIES

logger = logging.getLogger(__name__)


class GcmSourceMetadataExtractor(SourceMetadataExtractor):
    """
    GCM (Google Cloud Monitoring) connector extractor.

    Beyond the core monitoring assets (metrics, dashboards, cloud-run-service
    dashboards), this also covers broader GCP infrastructure entities (compute,
    storage, sql, pubsub, bigquery, networking, secrets, IAM, logging) under
    SourceModelType.GCP_* — surfaced through the same connector so we don't
    fragment Google Cloud coverage across multiple connector types. Each
    infra extractor is best-effort: if the connector's service account lacks
    a given role, that method logs and yields zero rows while the rest of
    the sync continues.
    """

    def __init__(self, request_id: str, connector_name: str, project_id, service_account_json):
        self.__project_id = project_id
        self.__service_account_json = service_account_json
        # GCP-wide API client used by the infra-coverage methods. Reuses the
        # same service-account JSON the GCM monitoring client uses; the SA
        # just needs the additional read roles to populate non-monitoring assets.
        self.gcp_api_processor = GcpApiProcessor(self.__project_id, self.__service_account_json)
        super().__init__(request_id, connector_name, Source.GCM)

    @staticmethod
    def _sanitize_metadata(value):
        # Why: GCP API responses can contain datetime/date objects that don't
        # round-trip through JSON; coerce them recursively before persisting.
        try:
            if isinstance(value, (datetime, date)):
                return value.isoformat()
            if isinstance(value, dict):
                return {k: GcmSourceMetadataExtractor._sanitize_metadata(v) for k, v in value.items()}
            if isinstance(value, list):
                return [GcmSourceMetadataExtractor._sanitize_metadata(v) for v in value]
            if isinstance(value, tuple):
                return tuple(GcmSourceMetadataExtractor._sanitize_metadata(v) for v in value)
            return value
        except Exception:
            return value

    # ==================== GCM Monitoring Extraction Methods ====================

    @log_function_call
    def extract_metric_descriptors(self):
        model_type = SourceModelType.GCM_METRIC
        model_data = {}
        gcm_api_processor = GcmApiProcessor(self.__project_id, self.__service_account_json)
        try:
            all_metric_descriptors = gcm_api_processor.fetch_metrics_list()
            for descriptor in all_metric_descriptors:
                try:
                    metric_type = descriptor['type']
                    model_data[metric_type] = {
                        'metric_type': metric_type
                    }
                except Exception as e:
                    logger.error(f'Error processing metric descriptor: {e}')
        except Exception as e:
            logger.error(f'Error extracting metric descriptors: {e}')
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)

    @log_function_call
    def extract_dashboard_entities(self):
        model_type = SourceModelType.GCM_DASHBOARD
        model_data = {}
        try:
            # Get all dashboards from the GCM API
            gcm_api_processor = GcmApiProcessor(self.__project_id, self.__service_account_json)
            all_dashboards = gcm_api_processor.fetch_dashboards_list()

            for dashboard in all_dashboards:
                try:
                    # Extract dashboard ID from the full resource name
                    dashboard_name = dashboard.get('name', '')
                    dashboard_id = dashboard_name.split('/')[-1] if dashboard_name else ''

                    if not dashboard_id:
                        continue

                    # Fetch detailed dashboard info with widgets
                    dashboard_detail = gcm_api_processor.fetch_dashboard(dashboard_id)

                    # Extract widgets information
                    widgets = []
                    if 'gridLayout' in dashboard_detail:
                        widgets = dashboard_detail.get('gridLayout', {}).get('widgets', [])
                    elif 'mosaicLayout' in dashboard_detail:
                        widgets = dashboard_detail.get('mosaicLayout', {}).get('widgets', [])

                    # Clean up widget data to only include necessary information
                    for widget in widgets:
                        # Remove any large or unnecessary fields
                        if 'plotData' in widget:
                            del widget['plotData']

                    # Prepare model data
                    model_data[dashboard_id] = {
                        'displayName': dashboard.get('displayName', ''),
                        'etag': dashboard.get('etag', ''),
                        'name': dashboard_name,
                        'widgets': widgets
                    }


                except Exception as e:
                    logger.error(f'Error extracting gcm dashboard entity: {e}')

        except Exception as e:
            logger.error(f'Error extracting gcm dashboard entities: {e}')

        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)

    @log_function_call
    def extract_cloud_run_service_dashboards(self):
        """
        Extract Cloud Run services + regions for the cloud_run_service_dashboard
        task type (model_type GCM_CLOUD_RUN_SERVICE_DASHBOARD = 1703).

        Distinct from extract_cloud_run_services below, which populates the
        infra-side GCP_CLOUD_RUN_SERVICE (1319) — both are kept so dashboards
        and the underlying service entities are discoverable independently.
        """
        model_type = SourceModelType.GCM_CLOUD_RUN_SERVICE_DASHBOARD
        model_data = {}

        try:
            # Use the GcmApiProcessor to fetch Cloud Run services
            gcm_api_processor = GcmApiProcessor(self.__project_id, self.__service_account_json)
            cloud_run_services = gcm_api_processor.fetch_cloud_run_services()

            for service_item in cloud_run_services:
                service_name = service_item.get('service_name')
                region = service_item.get('region')

                # Create a unique ID for this service+region combination
                model_uid = f"{service_name}:{region}"

                # Build metrics metadata using the static mappings
                metrics_data = []
                for metric_name, aggregations in GCM_SERVICE_DASHBOARD_QUERIES.items():
                    metrics_data.append({
                        'metric_name': metric_name,
                        'aggregations': list(aggregations.keys())
                    })

                # Prepare the metadata
                service_metadata = {
                    'service_name': service_name,
                    'region': region,
                    'project_id': self.__project_id,
                    'metrics': metrics_data,
                    'url': f"https://console.cloud.google.com/run/detail/{region}/{service_name}/metrics?project={self.__project_id}"
                }

                model_data[model_uid] = service_metadata


        except Exception as e:
            logger.error(f'Error extracting Cloud Run services: {e}')

        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)

    # ==================== GCP Infrastructure Extraction Methods ====================
    # Why: model_types 1310-1331 cover broader GCP infra (compute, storage, sql,
    # pubsub, bigquery, networking, secrets, IAM, logging). Surfaced via the GCM
    # connector instead of a separate Source.GCP so we don't fragment GCP setup.
    # Each method is independently best-effort — missing IAM scopes on the SA
    # produce 403s that are logged and skipped without breaking the rest of the
    # sync.

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
        """Extract GCP Cloud Run services (infra entity, model_type GCP_CLOUD_RUN_SERVICE = 1319)."""
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
