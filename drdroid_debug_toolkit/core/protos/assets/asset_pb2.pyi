from core.protos import base_pb2 as _base_pb2
from google.protobuf import wrappers_pb2 as _wrappers_pb2
from google.protobuf import struct_pb2 as _struct_pb2
from core.protos.connectors import connector_pb2 as _connector_pb2
from core.protos.assets import clickhouse_asset_pb2 as _clickhouse_asset_pb2
from core.protos.assets import grafana_asset_pb2 as _grafana_asset_pb2
from core.protos.assets import slack_asset_pb2 as _slack_asset_pb2
from core.protos.assets import newrelic_asset_pb2 as _newrelic_asset_pb2
from core.protos.assets import datadog_asset_pb2 as _datadog_asset_pb2
from core.protos.assets import postgres_asset_pb2 as _postgres_asset_pb2
from core.protos.assets import eks_asset_pb2 as _eks_asset_pb2
from core.protos.assets import azure_asset_pb2 as _azure_asset_pb2
from core.protos.assets import bash_asset_pb2 as _bash_asset_pb2
from core.protos.assets import gke_asset_pb2 as _gke_asset_pb2
from core.protos.assets import elastic_search_asset_pb2 as _elastic_search_asset_pb2
from core.protos.assets import gcm_asset_pb2 as _gcm_asset_pb2
from core.protos.assets import cloudwatch_asset_pb2 as _cloudwatch_asset_pb2
from core.protos.assets import asana_asset_pb2 as _asana_asset_pb2
from core.protos.assets import open_search_asset_pb2 as _open_search_asset_pb2
from core.protos.assets import github_asset_pb2 as _github_asset_pb2
from core.protos.assets import jira_asset_pb2 as _jira_asset_pb2
from core.protos.assets import argocd_asset_pb2 as _argocd_asset_pb2
from core.protos.assets import jenkins_asset_pb2 as _jenkins_asset_pb2
from core.protos.assets import posthog_asset_pb2 as _posthog_asset_pb2
from core.protos.assets import mongodb_asset_pb2 as _mongodb_asset_pb2
from core.protos.assets import sql_database_asset_pb2 as _sql_database_asset_pb2
from core.protos.assets import signoz_asset_pb2 as _signoz_asset_pb2
from core.protos.assets import kubectl_asset_pb2 as _kubectl_asset_pb2
from core.protos.assets import coralogix_asset_pb2 as _coralogix_asset_pb2
from core.protos.assets import sentry_asset_pb2 as _sentry_asset_pb2
from core.protos.assets import victoria_logs_asset_pb2 as _victoria_logs_asset_pb2
from core.protos.assets import linear_asset_pb2 as _linear_asset_pb2
from core.protos.assets import metabase_asset_pb2 as _metabase_asset_pb2
from core.protos.assets import mcp_asset_pb2 as _mcp_asset_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ConnectorModelTypeOptions(_message.Message):
    __slots__ = ("model_type", "cloudwatch_log_group_model_options", "cloudwatch_metric_model_options", "grafana_target_metric_promql_model_options", "clickhouse_database_model_options", "slack_channel_model_options", "new_relic_entity_application_model_options", "new_relic_entity_dashboard_model_options", "datadog_service_model_options", "postgres_database_model_options", "eks_cluster_model_options", "ssh_server_model_options", "azure_workspace_model_options", "grafana_prometheus_datasource_model_options", "gke_cluster_model_options", "elastic_search_index_model_options", "open_search_index_model_options", "gcm_metric_model_options", "datadog_oauth_service_model_options", "cloudwatch_log_group_query_model_options", "asana_project_model_options", "grafana_alert_rule_model_options", "cloudwatch_alarm_model_options", "datadog_dashboard_model_options", "datadog_oauth_dashboard_model_options", "github_repository_model_options", "github_member_model_options", "jira_user_model_options", "jira_project_model_options", "sentry_project_model_options", "argocd_apps_model_options", "azure_resource_model_options", "jenkins_apps_model_options", "mongodb_database_model_options", "mongodb_collection_model_options", "posthog_property_model_options", "ecs_cluster_model_options", "ecs_task_model_options", "ecs_service_model_options", "gcm_dashboard_model_options", "gcm_cloud_run_service_model_options", "signoz_dashboard_model_options", "signoz_alert_model_options", "kubernetes_namespace_model_options", "kubernetes_service_model_options", "kubernetes_deployment_model_options", "kubernetes_ingress_model_options", "kubernetes_network_policy_model_options", "kubernetes_hpa_model_options", "kubernetes_replicaset_model_options", "kubernetes_statefulset_model_options", "kubernetes_network_map_model_options", "elastic_search_dashboard_model_options", "elastic_search_index_pattern_model_options", "elastic_search_service_model_options", "grafana_loki_datasource_model_options", "signoz_service_model_options", "gke_namespace_model_filters", "gke_service_model_filters", "gke_deployment_model_filters", "gke_ingress_model_filters", "gke_network_policy_model_filters", "gke_hpa_model_filters", "gke_replicaset_model_filters", "gke_statefulset_model_filters", "coralogix_index_mapping_model_options", "signoz_log_attributes_model_options", "victoria_logs_field_model_options", "linear_team_model_options", "linear_user_model_options", "linear_project_model_options", "posthog_dashboard_model_options", "posthog_project_model_options", "metabase_dashboard_model_options", "metabase_card_model_options", "metabase_database_model_options", "metabase_collection_model_options", "metabase_subscription_model_options", "grafana_tempo_datasource_model_options", "grafana_tempo_service_model_options", "mcp_asset_model_options")
    MODEL_TYPE_FIELD_NUMBER: _ClassVar[int]
    CLOUDWATCH_LOG_GROUP_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    CLOUDWATCH_METRIC_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    GRAFANA_TARGET_METRIC_PROMQL_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    CLICKHOUSE_DATABASE_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    SLACK_CHANNEL_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    NEW_RELIC_ENTITY_APPLICATION_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    NEW_RELIC_ENTITY_DASHBOARD_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    DATADOG_SERVICE_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    POSTGRES_DATABASE_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    EKS_CLUSTER_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    SSH_SERVER_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    AZURE_WORKSPACE_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    GRAFANA_PROMETHEUS_DATASOURCE_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    GKE_CLUSTER_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    ELASTIC_SEARCH_INDEX_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    OPEN_SEARCH_INDEX_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    GCM_METRIC_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    DATADOG_OAUTH_SERVICE_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    CLOUDWATCH_LOG_GROUP_QUERY_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    ASANA_PROJECT_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    GRAFANA_ALERT_RULE_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    CLOUDWATCH_ALARM_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    DATADOG_DASHBOARD_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    DATADOG_OAUTH_DASHBOARD_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    GITHUB_REPOSITORY_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    GITHUB_MEMBER_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    JIRA_USER_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    JIRA_PROJECT_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    SENTRY_PROJECT_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    ARGOCD_APPS_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    AZURE_RESOURCE_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    JENKINS_APPS_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    MONGODB_DATABASE_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    MONGODB_COLLECTION_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    POSTHOG_PROPERTY_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    ECS_CLUSTER_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    ECS_TASK_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    ECS_SERVICE_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    GCM_DASHBOARD_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    GCM_CLOUD_RUN_SERVICE_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    SIGNOZ_DASHBOARD_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    SIGNOZ_ALERT_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    KUBERNETES_NAMESPACE_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    KUBERNETES_SERVICE_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    KUBERNETES_DEPLOYMENT_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    KUBERNETES_INGRESS_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    KUBERNETES_NETWORK_POLICY_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    KUBERNETES_HPA_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    KUBERNETES_REPLICASET_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    KUBERNETES_STATEFULSET_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    KUBERNETES_NETWORK_MAP_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    ELASTIC_SEARCH_DASHBOARD_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    ELASTIC_SEARCH_INDEX_PATTERN_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    ELASTIC_SEARCH_SERVICE_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    GRAFANA_LOKI_DATASOURCE_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    SIGNOZ_SERVICE_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    GKE_NAMESPACE_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    GKE_SERVICE_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    GKE_DEPLOYMENT_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    GKE_INGRESS_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    GKE_NETWORK_POLICY_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    GKE_HPA_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    GKE_REPLICASET_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    GKE_STATEFULSET_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    CORALOGIX_INDEX_MAPPING_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    SIGNOZ_LOG_ATTRIBUTES_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    VICTORIA_LOGS_FIELD_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    LINEAR_TEAM_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    LINEAR_USER_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    LINEAR_PROJECT_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    POSTHOG_DASHBOARD_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    POSTHOG_PROJECT_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    METABASE_DASHBOARD_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    METABASE_CARD_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    METABASE_DATABASE_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    METABASE_COLLECTION_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    METABASE_SUBSCRIPTION_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    GRAFANA_TEMPO_DATASOURCE_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    GRAFANA_TEMPO_SERVICE_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    MCP_ASSET_MODEL_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    model_type: _base_pb2.SourceModelType
    cloudwatch_log_group_model_options: _cloudwatch_asset_pb2.CloudwatchLogGroupAssetOptions
    cloudwatch_metric_model_options: _cloudwatch_asset_pb2.CloudwatchMetricAssetOptions
    grafana_target_metric_promql_model_options: _grafana_asset_pb2.GrafanaTargetMetricPromQlAssetOptions
    clickhouse_database_model_options: _clickhouse_asset_pb2.ClickhouseDatabaseAssetOptions
    slack_channel_model_options: _slack_asset_pb2.SlackChannelAssetOptions
    new_relic_entity_application_model_options: _newrelic_asset_pb2.NewRelicApplicationEntityAssetOptions
    new_relic_entity_dashboard_model_options: _newrelic_asset_pb2.NewRelicDashboardEntityAssetOptions
    datadog_service_model_options: _datadog_asset_pb2.DatadogServiceAssetOptions
    postgres_database_model_options: _postgres_asset_pb2.PostgresDatabaseAssetOptions
    eks_cluster_model_options: _eks_asset_pb2.EksClusterAssetOptions
    ssh_server_model_options: _bash_asset_pb2.BashSshServerAssetOptions
    azure_workspace_model_options: _azure_asset_pb2.AzureWorkspaceAssetOptions
    grafana_prometheus_datasource_model_options: _grafana_asset_pb2.GrafanaDatasourceAssetOptions
    gke_cluster_model_options: _gke_asset_pb2.GkeClusterAssetOptions
    elastic_search_index_model_options: _elastic_search_asset_pb2.ElasticSearchIndexAssetOptions
    open_search_index_model_options: _open_search_asset_pb2.OpenSearchIndexAssetOptions
    gcm_metric_model_options: _gcm_asset_pb2.GcmMetricAssetOptions
    datadog_oauth_service_model_options: _datadog_asset_pb2.DatadogServiceAssetOptions
    cloudwatch_log_group_query_model_options: _cloudwatch_asset_pb2.CloudwatchLogGroupQueryAssetOptions
    asana_project_model_options: _asana_asset_pb2.AsanaProjectAssetOptions
    grafana_alert_rule_model_options: _grafana_asset_pb2.GrafanaAlertRuleAssetOptions
    cloudwatch_alarm_model_options: _cloudwatch_asset_pb2.CloudwatchAlarmAssetOptions
    datadog_dashboard_model_options: _datadog_asset_pb2.DatadogDashboardAssetOptions
    datadog_oauth_dashboard_model_options: _datadog_asset_pb2.DatadogDashboardAssetOptions
    github_repository_model_options: _github_asset_pb2.GithubRepositoryAssetOptions
    github_member_model_options: _github_asset_pb2.GithubMemberAssetOptions
    jira_user_model_options: _jira_asset_pb2.JiraUserAssetOptions
    jira_project_model_options: _jira_asset_pb2.JiraProjectAssetOptions
    sentry_project_model_options: _sentry_asset_pb2.SentryProjectAssetOptions
    argocd_apps_model_options: _argocd_asset_pb2.ArgoCDAppsAssetOptions
    azure_resource_model_options: _azure_asset_pb2.AzureResourceAssetOptions
    jenkins_apps_model_options: _jenkins_asset_pb2.JenkinsAppsAssetOptions
    mongodb_database_model_options: _mongodb_asset_pb2.MongoDBDatabaseAssetOptions
    mongodb_collection_model_options: _mongodb_asset_pb2.MongoDBCollectionAssetOptions
    posthog_property_model_options: _posthog_asset_pb2.PosthogPropertyAssetOptions
    ecs_cluster_model_options: _cloudwatch_asset_pb2.EcsClusterAssetOptions
    ecs_task_model_options: _cloudwatch_asset_pb2.EcsTaskAssetOptions
    ecs_service_model_options: _cloudwatch_asset_pb2.EcsServiceAssetOptions
    gcm_dashboard_model_options: _gcm_asset_pb2.GcmDashboardEntityAssetOptions
    gcm_cloud_run_service_model_options: _gcm_asset_pb2.GcmCloudRunServiceAssetOptions
    signoz_dashboard_model_options: _signoz_asset_pb2.SignozDashboardAssetOptions
    signoz_alert_model_options: _signoz_asset_pb2.SignozAlertAssetOptions
    kubernetes_namespace_model_options: _kubectl_asset_pb2.KubernetesNamespaceAssetOptions
    kubernetes_service_model_options: _kubectl_asset_pb2.KubernetesServiceAssetOptions
    kubernetes_deployment_model_options: _kubectl_asset_pb2.KubernetesDeploymentAssetOptions
    kubernetes_ingress_model_options: _kubectl_asset_pb2.KubernetesIngressAssetOptions
    kubernetes_network_policy_model_options: _kubectl_asset_pb2.KubernetesNetworkPolicyAssetOptions
    kubernetes_hpa_model_options: _kubectl_asset_pb2.KubernetesHpaAssetOptions
    kubernetes_replicaset_model_options: _kubectl_asset_pb2.KubernetesReplicasetAssetOptions
    kubernetes_statefulset_model_options: _kubectl_asset_pb2.KubernetesStatefulsetAssetOptions
    kubernetes_network_map_model_options: _kubectl_asset_pb2.KubernetesNetworkMapAssetOptions
    elastic_search_dashboard_model_options: _elastic_search_asset_pb2.ElasticSearchDashboardAssetOptions
    elastic_search_index_pattern_model_options: _elastic_search_asset_pb2.ElasticSearchIndexPatternAssetOptions
    elastic_search_service_model_options: _elastic_search_asset_pb2.ElasticSearchServiceAssetOptions
    grafana_loki_datasource_model_options: _grafana_asset_pb2.GrafanaDatasourceAssetOptions
    signoz_service_model_options: _signoz_asset_pb2.SignozServiceAssetOptions
    gke_namespace_model_filters: _gke_asset_pb2.GkeNamespaceAssetOptions
    gke_service_model_filters: _gke_asset_pb2.GkeServiceOption
    gke_deployment_model_filters: _gke_asset_pb2.GkeDeploymentOption
    gke_ingress_model_filters: _gke_asset_pb2.GkeIngressAssetOptions
    gke_network_policy_model_filters: _gke_asset_pb2.GkeNetworkPolicyAssetOptions
    gke_hpa_model_filters: _gke_asset_pb2.GkeHpaAssetOptions
    gke_replicaset_model_filters: _gke_asset_pb2.GkeReplicasetAssetOptions
    gke_statefulset_model_filters: _gke_asset_pb2.GkeStatefulsetAssetOptions
    coralogix_index_mapping_model_options: _coralogix_asset_pb2.CoralogixIndexMappingAssetOptions
    signoz_log_attributes_model_options: _signoz_asset_pb2.SignozLogAttributesAssetOptions
    victoria_logs_field_model_options: _victoria_logs_asset_pb2.VictoriaLogsFieldAssetOptions
    linear_team_model_options: _linear_asset_pb2.LinearTeamAssetOptions
    linear_user_model_options: _linear_asset_pb2.LinearUserAssetOptions
    linear_project_model_options: _linear_asset_pb2.LinearProjectAssetOptions
    posthog_dashboard_model_options: _posthog_asset_pb2.PosthogDashboardAssetOptions
    posthog_project_model_options: _posthog_asset_pb2.PosthogProjectAssetOptions
    metabase_dashboard_model_options: _metabase_asset_pb2.MetabaseDashboardAssetOptions
    metabase_card_model_options: _metabase_asset_pb2.MetabaseCardAssetOptions
    metabase_database_model_options: _metabase_asset_pb2.MetabaseDatabaseAssetOptions
    metabase_collection_model_options: _metabase_asset_pb2.MetabaseCollectionAssetOptions
    metabase_subscription_model_options: _metabase_asset_pb2.MetabaseSubscriptionAssetOptions
    grafana_tempo_datasource_model_options: _grafana_asset_pb2.GrafanaDatasourceAssetOptions
    grafana_tempo_service_model_options: _grafana_asset_pb2.GrafanaTempoServiceAssetOptions
    mcp_asset_model_options: _mcp_asset_pb2.McpAssetOptions
    def __init__(self, model_type: _Optional[_Union[_base_pb2.SourceModelType, str]] = ..., cloudwatch_log_group_model_options: _Optional[_Union[_cloudwatch_asset_pb2.CloudwatchLogGroupAssetOptions, _Mapping]] = ..., cloudwatch_metric_model_options: _Optional[_Union[_cloudwatch_asset_pb2.CloudwatchMetricAssetOptions, _Mapping]] = ..., grafana_target_metric_promql_model_options: _Optional[_Union[_grafana_asset_pb2.GrafanaTargetMetricPromQlAssetOptions, _Mapping]] = ..., clickhouse_database_model_options: _Optional[_Union[_clickhouse_asset_pb2.ClickhouseDatabaseAssetOptions, _Mapping]] = ..., slack_channel_model_options: _Optional[_Union[_slack_asset_pb2.SlackChannelAssetOptions, _Mapping]] = ..., new_relic_entity_application_model_options: _Optional[_Union[_newrelic_asset_pb2.NewRelicApplicationEntityAssetOptions, _Mapping]] = ..., new_relic_entity_dashboard_model_options: _Optional[_Union[_newrelic_asset_pb2.NewRelicDashboardEntityAssetOptions, _Mapping]] = ..., datadog_service_model_options: _Optional[_Union[_datadog_asset_pb2.DatadogServiceAssetOptions, _Mapping]] = ..., postgres_database_model_options: _Optional[_Union[_postgres_asset_pb2.PostgresDatabaseAssetOptions, _Mapping]] = ..., eks_cluster_model_options: _Optional[_Union[_eks_asset_pb2.EksClusterAssetOptions, _Mapping]] = ..., ssh_server_model_options: _Optional[_Union[_bash_asset_pb2.BashSshServerAssetOptions, _Mapping]] = ..., azure_workspace_model_options: _Optional[_Union[_azure_asset_pb2.AzureWorkspaceAssetOptions, _Mapping]] = ..., grafana_prometheus_datasource_model_options: _Optional[_Union[_grafana_asset_pb2.GrafanaDatasourceAssetOptions, _Mapping]] = ..., gke_cluster_model_options: _Optional[_Union[_gke_asset_pb2.GkeClusterAssetOptions, _Mapping]] = ..., elastic_search_index_model_options: _Optional[_Union[_elastic_search_asset_pb2.ElasticSearchIndexAssetOptions, _Mapping]] = ..., open_search_index_model_options: _Optional[_Union[_open_search_asset_pb2.OpenSearchIndexAssetOptions, _Mapping]] = ..., gcm_metric_model_options: _Optional[_Union[_gcm_asset_pb2.GcmMetricAssetOptions, _Mapping]] = ..., datadog_oauth_service_model_options: _Optional[_Union[_datadog_asset_pb2.DatadogServiceAssetOptions, _Mapping]] = ..., cloudwatch_log_group_query_model_options: _Optional[_Union[_cloudwatch_asset_pb2.CloudwatchLogGroupQueryAssetOptions, _Mapping]] = ..., asana_project_model_options: _Optional[_Union[_asana_asset_pb2.AsanaProjectAssetOptions, _Mapping]] = ..., grafana_alert_rule_model_options: _Optional[_Union[_grafana_asset_pb2.GrafanaAlertRuleAssetOptions, _Mapping]] = ..., cloudwatch_alarm_model_options: _Optional[_Union[_cloudwatch_asset_pb2.CloudwatchAlarmAssetOptions, _Mapping]] = ..., datadog_dashboard_model_options: _Optional[_Union[_datadog_asset_pb2.DatadogDashboardAssetOptions, _Mapping]] = ..., datadog_oauth_dashboard_model_options: _Optional[_Union[_datadog_asset_pb2.DatadogDashboardAssetOptions, _Mapping]] = ..., github_repository_model_options: _Optional[_Union[_github_asset_pb2.GithubRepositoryAssetOptions, _Mapping]] = ..., github_member_model_options: _Optional[_Union[_github_asset_pb2.GithubMemberAssetOptions, _Mapping]] = ..., jira_user_model_options: _Optional[_Union[_jira_asset_pb2.JiraUserAssetOptions, _Mapping]] = ..., jira_project_model_options: _Optional[_Union[_jira_asset_pb2.JiraProjectAssetOptions, _Mapping]] = ..., sentry_project_model_options: _Optional[_Union[_sentry_asset_pb2.SentryProjectAssetOptions, _Mapping]] = ..., argocd_apps_model_options: _Optional[_Union[_argocd_asset_pb2.ArgoCDAppsAssetOptions, _Mapping]] = ..., azure_resource_model_options: _Optional[_Union[_azure_asset_pb2.AzureResourceAssetOptions, _Mapping]] = ..., jenkins_apps_model_options: _Optional[_Union[_jenkins_asset_pb2.JenkinsAppsAssetOptions, _Mapping]] = ..., mongodb_database_model_options: _Optional[_Union[_mongodb_asset_pb2.MongoDBDatabaseAssetOptions, _Mapping]] = ..., mongodb_collection_model_options: _Optional[_Union[_mongodb_asset_pb2.MongoDBCollectionAssetOptions, _Mapping]] = ..., posthog_property_model_options: _Optional[_Union[_posthog_asset_pb2.PosthogPropertyAssetOptions, _Mapping]] = ..., ecs_cluster_model_options: _Optional[_Union[_cloudwatch_asset_pb2.EcsClusterAssetOptions, _Mapping]] = ..., ecs_task_model_options: _Optional[_Union[_cloudwatch_asset_pb2.EcsTaskAssetOptions, _Mapping]] = ..., ecs_service_model_options: _Optional[_Union[_cloudwatch_asset_pb2.EcsServiceAssetOptions, _Mapping]] = ..., gcm_dashboard_model_options: _Optional[_Union[_gcm_asset_pb2.GcmDashboardEntityAssetOptions, _Mapping]] = ..., gcm_cloud_run_service_model_options: _Optional[_Union[_gcm_asset_pb2.GcmCloudRunServiceAssetOptions, _Mapping]] = ..., signoz_dashboard_model_options: _Optional[_Union[_signoz_asset_pb2.SignozDashboardAssetOptions, _Mapping]] = ..., signoz_alert_model_options: _Optional[_Union[_signoz_asset_pb2.SignozAlertAssetOptions, _Mapping]] = ..., kubernetes_namespace_model_options: _Optional[_Union[_kubectl_asset_pb2.KubernetesNamespaceAssetOptions, _Mapping]] = ..., kubernetes_service_model_options: _Optional[_Union[_kubectl_asset_pb2.KubernetesServiceAssetOptions, _Mapping]] = ..., kubernetes_deployment_model_options: _Optional[_Union[_kubectl_asset_pb2.KubernetesDeploymentAssetOptions, _Mapping]] = ..., kubernetes_ingress_model_options: _Optional[_Union[_kubectl_asset_pb2.KubernetesIngressAssetOptions, _Mapping]] = ..., kubernetes_network_policy_model_options: _Optional[_Union[_kubectl_asset_pb2.KubernetesNetworkPolicyAssetOptions, _Mapping]] = ..., kubernetes_hpa_model_options: _Optional[_Union[_kubectl_asset_pb2.KubernetesHpaAssetOptions, _Mapping]] = ..., kubernetes_replicaset_model_options: _Optional[_Union[_kubectl_asset_pb2.KubernetesReplicasetAssetOptions, _Mapping]] = ..., kubernetes_statefulset_model_options: _Optional[_Union[_kubectl_asset_pb2.KubernetesStatefulsetAssetOptions, _Mapping]] = ..., kubernetes_network_map_model_options: _Optional[_Union[_kubectl_asset_pb2.KubernetesNetworkMapAssetOptions, _Mapping]] = ..., elastic_search_dashboard_model_options: _Optional[_Union[_elastic_search_asset_pb2.ElasticSearchDashboardAssetOptions, _Mapping]] = ..., elastic_search_index_pattern_model_options: _Optional[_Union[_elastic_search_asset_pb2.ElasticSearchIndexPatternAssetOptions, _Mapping]] = ..., elastic_search_service_model_options: _Optional[_Union[_elastic_search_asset_pb2.ElasticSearchServiceAssetOptions, _Mapping]] = ..., grafana_loki_datasource_model_options: _Optional[_Union[_grafana_asset_pb2.GrafanaDatasourceAssetOptions, _Mapping]] = ..., signoz_service_model_options: _Optional[_Union[_signoz_asset_pb2.SignozServiceAssetOptions, _Mapping]] = ..., gke_namespace_model_filters: _Optional[_Union[_gke_asset_pb2.GkeNamespaceAssetOptions, _Mapping]] = ..., gke_service_model_filters: _Optional[_Union[_gke_asset_pb2.GkeServiceOption, _Mapping]] = ..., gke_deployment_model_filters: _Optional[_Union[_gke_asset_pb2.GkeDeploymentOption, _Mapping]] = ..., gke_ingress_model_filters: _Optional[_Union[_gke_asset_pb2.GkeIngressAssetOptions, _Mapping]] = ..., gke_network_policy_model_filters: _Optional[_Union[_gke_asset_pb2.GkeNetworkPolicyAssetOptions, _Mapping]] = ..., gke_hpa_model_filters: _Optional[_Union[_gke_asset_pb2.GkeHpaAssetOptions, _Mapping]] = ..., gke_replicaset_model_filters: _Optional[_Union[_gke_asset_pb2.GkeReplicasetAssetOptions, _Mapping]] = ..., gke_statefulset_model_filters: _Optional[_Union[_gke_asset_pb2.GkeStatefulsetAssetOptions, _Mapping]] = ..., coralogix_index_mapping_model_options: _Optional[_Union[_coralogix_asset_pb2.CoralogixIndexMappingAssetOptions, _Mapping]] = ..., signoz_log_attributes_model_options: _Optional[_Union[_signoz_asset_pb2.SignozLogAttributesAssetOptions, _Mapping]] = ..., victoria_logs_field_model_options: _Optional[_Union[_victoria_logs_asset_pb2.VictoriaLogsFieldAssetOptions, _Mapping]] = ..., linear_team_model_options: _Optional[_Union[_linear_asset_pb2.LinearTeamAssetOptions, _Mapping]] = ..., linear_user_model_options: _Optional[_Union[_linear_asset_pb2.LinearUserAssetOptions, _Mapping]] = ..., linear_project_model_options: _Optional[_Union[_linear_asset_pb2.LinearProjectAssetOptions, _Mapping]] = ..., posthog_dashboard_model_options: _Optional[_Union[_posthog_asset_pb2.PosthogDashboardAssetOptions, _Mapping]] = ..., posthog_project_model_options: _Optional[_Union[_posthog_asset_pb2.PosthogProjectAssetOptions, _Mapping]] = ..., metabase_dashboard_model_options: _Optional[_Union[_metabase_asset_pb2.MetabaseDashboardAssetOptions, _Mapping]] = ..., metabase_card_model_options: _Optional[_Union[_metabase_asset_pb2.MetabaseCardAssetOptions, _Mapping]] = ..., metabase_database_model_options: _Optional[_Union[_metabase_asset_pb2.MetabaseDatabaseAssetOptions, _Mapping]] = ..., metabase_collection_model_options: _Optional[_Union[_metabase_asset_pb2.MetabaseCollectionAssetOptions, _Mapping]] = ..., metabase_subscription_model_options: _Optional[_Union[_metabase_asset_pb2.MetabaseSubscriptionAssetOptions, _Mapping]] = ..., grafana_tempo_datasource_model_options: _Optional[_Union[_grafana_asset_pb2.GrafanaDatasourceAssetOptions, _Mapping]] = ..., grafana_tempo_service_model_options: _Optional[_Union[_grafana_asset_pb2.GrafanaTempoServiceAssetOptions, _Mapping]] = ..., mcp_asset_model_options: _Optional[_Union[_mcp_asset_pb2.McpAssetOptions, _Mapping]] = ...) -> None: ...

class AccountConnectorAssetsModelOptions(_message.Message):
    __slots__ = ("connector_type", "connector", "model_types_options")
    CONNECTOR_TYPE_FIELD_NUMBER: _ClassVar[int]
    CONNECTOR_FIELD_NUMBER: _ClassVar[int]
    MODEL_TYPES_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    connector_type: _base_pb2.Source
    connector: _connector_pb2.Connector
    model_types_options: _containers.RepeatedCompositeFieldContainer[ConnectorModelTypeOptions]
    def __init__(self, connector_type: _Optional[_Union[_base_pb2.Source, str]] = ..., connector: _Optional[_Union[_connector_pb2.Connector, _Mapping]] = ..., model_types_options: _Optional[_Iterable[_Union[ConnectorModelTypeOptions, _Mapping]]] = ...) -> None: ...

class AccountConnectorAssetsModelFilters(_message.Message):
    __slots__ = ("cloudwatch_log_group_model_filters", "cloudwatch_metric_model_filters", "grafana_target_metric_promql_model_filters", "clickhouse_database_model_filters", "slack_channel_model_filters", "new_relic_entity_application_model_filters", "new_relic_entity_dashboard_model_filters", "datadog_service_model_filters", "postgres_database_model_filters", "eks_cluster_model_filters", "ssh_server_model_filters", "azure_workspace_model_filters", "grafana_prometheus_datasource_model_filters", "gke_cluster_model_filters", "elastic_search_index_model_filters", "open_search_index_model_filters", "gcm_metric_model_filters", "datadog_oauth_service_model_filters", "cloudwatch_log_group_query_model_filters", "asana_project_model_filters", "grafana_alert_rule_model_filters", "cloudwatch_alarm_model_filters", "datadog_dashboard_model_filters", "datadog_oauth_dashboard_model_filters", "github_repository_model_filters", "github_member_model_filters", "jira_user_model_filters", "jira_project_model_filters", "sentry_project_model_filters", "argocd_apps_model_filters", "azure_resource_model_filters", "grafana_dashboard_model_filters", "jenkins_apps_model_filters", "mongodb_database_model_filters", "mongodb_collection_model_filters", "posthog_property_model_filters", "sql_table_model_filters", "ecs_cluster_model_filters", "ecs_task_model_filters", "ecs_service_model_filters", "gcm_dashboard_model_filters", "gcm_cloud_run_service_model_filters", "cloudwatch_dashboard_model_filters", "signoz_dashboard_model_filters", "signoz_alert_model_filters", "kubernetes_namespace_model_filters", "kubernetes_service_model_filters", "kubernetes_deployment_model_filters", "kubernetes_ingress_model_filters", "kubernetes_network_policy_model_filters", "kubernetes_hpa_model_filters", "kubernetes_replicaset_model_filters", "kubernetes_statefulset_model_filters", "kubernetes_network_map_model_filters", "elastic_search_service_model_filters", "elastic_search_dashboard_model_filters", "elastic_search_index_pattern_model_filters", "coralogix_dashboard_model_filters", "grafana_loki_datasource_model_filters", "signoz_service_model_filters", "gke_namespace_model_filters", "gke_service_model_filters", "gke_deployment_model_filters", "gke_ingress_model_filters", "gke_network_policy_model_filters", "gke_hpa_model_filters", "gke_replicaset_model_filters", "gke_statefulset_model_filters", "coralogix_index_mapping_model_filters", "signoz_log_attributes_model_filters", "victoria_logs_field_model_filters", "linear_team_model_filters", "linear_user_model_filters", "linear_project_model_filters", "posthog_dashboard_model_filters", "posthog_project_model_filters", "metabase_dashboard_model_filters", "metabase_card_model_filters", "metabase_database_model_filters", "metabase_collection_model_filters", "metabase_subscription_model_filters", "grafana_tempo_datasource_model_filters", "grafana_tempo_service_model_filters", "mcp_asset_model_filters")
    CLOUDWATCH_LOG_GROUP_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    CLOUDWATCH_METRIC_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    GRAFANA_TARGET_METRIC_PROMQL_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    CLICKHOUSE_DATABASE_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    SLACK_CHANNEL_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    NEW_RELIC_ENTITY_APPLICATION_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    NEW_RELIC_ENTITY_DASHBOARD_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    DATADOG_SERVICE_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    POSTGRES_DATABASE_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    EKS_CLUSTER_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    SSH_SERVER_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    AZURE_WORKSPACE_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    GRAFANA_PROMETHEUS_DATASOURCE_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    GKE_CLUSTER_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    ELASTIC_SEARCH_INDEX_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    OPEN_SEARCH_INDEX_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    GCM_METRIC_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    DATADOG_OAUTH_SERVICE_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    CLOUDWATCH_LOG_GROUP_QUERY_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    ASANA_PROJECT_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    GRAFANA_ALERT_RULE_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    CLOUDWATCH_ALARM_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    DATADOG_DASHBOARD_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    DATADOG_OAUTH_DASHBOARD_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    GITHUB_REPOSITORY_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    GITHUB_MEMBER_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    JIRA_USER_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    JIRA_PROJECT_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    SENTRY_PROJECT_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    ARGOCD_APPS_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    AZURE_RESOURCE_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    GRAFANA_DASHBOARD_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    JENKINS_APPS_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    MONGODB_DATABASE_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    MONGODB_COLLECTION_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    POSTHOG_PROPERTY_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    SQL_TABLE_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    ECS_CLUSTER_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    ECS_TASK_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    ECS_SERVICE_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    GCM_DASHBOARD_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    GCM_CLOUD_RUN_SERVICE_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    CLOUDWATCH_DASHBOARD_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    SIGNOZ_DASHBOARD_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    SIGNOZ_ALERT_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    KUBERNETES_NAMESPACE_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    KUBERNETES_SERVICE_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    KUBERNETES_DEPLOYMENT_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    KUBERNETES_INGRESS_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    KUBERNETES_NETWORK_POLICY_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    KUBERNETES_HPA_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    KUBERNETES_REPLICASET_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    KUBERNETES_STATEFULSET_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    KUBERNETES_NETWORK_MAP_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    ELASTIC_SEARCH_SERVICE_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    ELASTIC_SEARCH_DASHBOARD_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    ELASTIC_SEARCH_INDEX_PATTERN_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    CORALOGIX_DASHBOARD_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    GRAFANA_LOKI_DATASOURCE_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    SIGNOZ_SERVICE_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    GKE_NAMESPACE_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    GKE_SERVICE_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    GKE_DEPLOYMENT_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    GKE_INGRESS_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    GKE_NETWORK_POLICY_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    GKE_HPA_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    GKE_REPLICASET_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    GKE_STATEFULSET_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    CORALOGIX_INDEX_MAPPING_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    SIGNOZ_LOG_ATTRIBUTES_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    VICTORIA_LOGS_FIELD_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    LINEAR_TEAM_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    LINEAR_USER_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    LINEAR_PROJECT_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    POSTHOG_DASHBOARD_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    POSTHOG_PROJECT_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    METABASE_DASHBOARD_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    METABASE_CARD_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    METABASE_DATABASE_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    METABASE_COLLECTION_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    METABASE_SUBSCRIPTION_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    GRAFANA_TEMPO_DATASOURCE_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    GRAFANA_TEMPO_SERVICE_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    MCP_ASSET_MODEL_FILTERS_FIELD_NUMBER: _ClassVar[int]
    cloudwatch_log_group_model_filters: _cloudwatch_asset_pb2.CloudwatchLogGroupAssetOptions
    cloudwatch_metric_model_filters: _cloudwatch_asset_pb2.CloudwatchMetricAssetOptions
    grafana_target_metric_promql_model_filters: _grafana_asset_pb2.GrafanaTargetMetricPromQlAssetOptions
    clickhouse_database_model_filters: _clickhouse_asset_pb2.ClickhouseDatabaseAssetOptions
    slack_channel_model_filters: _slack_asset_pb2.SlackChannelAssetOptions
    new_relic_entity_application_model_filters: _newrelic_asset_pb2.NewRelicApplicationEntityAssetOptions
    new_relic_entity_dashboard_model_filters: _newrelic_asset_pb2.NewRelicDashboardEntityAssetOptions
    datadog_service_model_filters: _datadog_asset_pb2.DatadogServiceAssetOptions
    postgres_database_model_filters: _postgres_asset_pb2.PostgresDatabaseAssetOptions
    eks_cluster_model_filters: _eks_asset_pb2.EksClusterAssetOptions
    ssh_server_model_filters: _bash_asset_pb2.BashSshServerAssetOptions
    azure_workspace_model_filters: _azure_asset_pb2.AzureWorkspaceAssetOptions
    grafana_prometheus_datasource_model_filters: _grafana_asset_pb2.GrafanaDatasourceAssetOptions
    gke_cluster_model_filters: _gke_asset_pb2.GkeClusterAssetOptions
    elastic_search_index_model_filters: _elastic_search_asset_pb2.ElasticSearchIndexAssetOptions
    open_search_index_model_filters: _open_search_asset_pb2.OpenSearchIndexAssetOptions
    gcm_metric_model_filters: _gcm_asset_pb2.GcmMetricAssetOptions
    datadog_oauth_service_model_filters: _datadog_asset_pb2.DatadogServiceAssetOptions
    cloudwatch_log_group_query_model_filters: _cloudwatch_asset_pb2.CloudwatchLogGroupQueryAssetOptions
    asana_project_model_filters: _asana_asset_pb2.AsanaProjectAssetOptions
    grafana_alert_rule_model_filters: _grafana_asset_pb2.GrafanaAlertRuleAssetOptions
    cloudwatch_alarm_model_filters: _cloudwatch_asset_pb2.CloudwatchAlarmAssetOptions
    datadog_dashboard_model_filters: _datadog_asset_pb2.DatadogDashboardAssetOptions
    datadog_oauth_dashboard_model_filters: _datadog_asset_pb2.DatadogDashboardAssetOptions
    github_repository_model_filters: _github_asset_pb2.GithubRepositoryAssetOptions
    github_member_model_filters: _github_asset_pb2.GithubMemberAssetOptions
    jira_user_model_filters: _jira_asset_pb2.JiraUserAssetOptions
    jira_project_model_filters: _jira_asset_pb2.JiraProjectAssetOptions
    sentry_project_model_filters: _sentry_asset_pb2.SentryProjectAssetOptions
    argocd_apps_model_filters: _argocd_asset_pb2.ArgoCDAppsAssetOptions
    azure_resource_model_filters: _azure_asset_pb2.AzureResourceAssetOptions
    grafana_dashboard_model_filters: _grafana_asset_pb2.GrafanaDashboardAssetOptions
    jenkins_apps_model_filters: _jenkins_asset_pb2.JenkinsAppsAssetOptions
    mongodb_database_model_filters: _mongodb_asset_pb2.MongoDBDatabaseAssetOptions
    mongodb_collection_model_filters: _mongodb_asset_pb2.MongoDBCollectionAssetOptions
    posthog_property_model_filters: _posthog_asset_pb2.PosthogPropertyAssetOptions
    sql_table_model_filters: _sql_database_asset_pb2.SqlTableAssetOptions
    ecs_cluster_model_filters: _cloudwatch_asset_pb2.EcsClusterAssetOptions
    ecs_task_model_filters: _cloudwatch_asset_pb2.EcsTaskAssetOptions
    ecs_service_model_filters: _cloudwatch_asset_pb2.EcsServiceAssetOptions
    gcm_dashboard_model_filters: _gcm_asset_pb2.GcmDashboardEntityAssetOptions
    gcm_cloud_run_service_model_filters: _gcm_asset_pb2.GcmCloudRunServiceAssetOptions
    cloudwatch_dashboard_model_filters: _cloudwatch_asset_pb2.CloudwatchDashboardAssetOptions
    signoz_dashboard_model_filters: _signoz_asset_pb2.SignozDashboardAssetOptions
    signoz_alert_model_filters: _signoz_asset_pb2.SignozAlertAssetOptions
    kubernetes_namespace_model_filters: _kubectl_asset_pb2.KubernetesNamespaceAssetOptions
    kubernetes_service_model_filters: _kubectl_asset_pb2.KubernetesServiceAssetOptions
    kubernetes_deployment_model_filters: _kubectl_asset_pb2.KubernetesDeploymentAssetOptions
    kubernetes_ingress_model_filters: _kubectl_asset_pb2.KubernetesIngressAssetOptions
    kubernetes_network_policy_model_filters: _kubectl_asset_pb2.KubernetesNetworkPolicyAssetOptions
    kubernetes_hpa_model_filters: _kubectl_asset_pb2.KubernetesHpaAssetOptions
    kubernetes_replicaset_model_filters: _kubectl_asset_pb2.KubernetesReplicasetAssetOptions
    kubernetes_statefulset_model_filters: _kubectl_asset_pb2.KubernetesStatefulsetAssetOptions
    kubernetes_network_map_model_filters: _kubectl_asset_pb2.KubernetesNetworkMapAssetOptions
    elastic_search_service_model_filters: _elastic_search_asset_pb2.ElasticSearchServiceAssetOptions
    elastic_search_dashboard_model_filters: _elastic_search_asset_pb2.ElasticSearchDashboardAssetOptions
    elastic_search_index_pattern_model_filters: _elastic_search_asset_pb2.ElasticSearchIndexPatternAssetOptions
    coralogix_dashboard_model_filters: _coralogix_asset_pb2.CoralogixDashboardAssetOptions
    grafana_loki_datasource_model_filters: _grafana_asset_pb2.GrafanaDatasourceAssetOptions
    signoz_service_model_filters: _signoz_asset_pb2.SignozServiceAssetOptions
    gke_namespace_model_filters: _gke_asset_pb2.GkeNamespaceAssetOptions
    gke_service_model_filters: _gke_asset_pb2.GkeServiceOption
    gke_deployment_model_filters: _gke_asset_pb2.GkeDeploymentOption
    gke_ingress_model_filters: _gke_asset_pb2.GkeIngressAssetOptions
    gke_network_policy_model_filters: _gke_asset_pb2.GkeNetworkPolicyAssetOptions
    gke_hpa_model_filters: _gke_asset_pb2.GkeHpaAssetOptions
    gke_replicaset_model_filters: _gke_asset_pb2.GkeReplicasetAssetOptions
    gke_statefulset_model_filters: _gke_asset_pb2.GkeStatefulsetAssetOptions
    coralogix_index_mapping_model_filters: _coralogix_asset_pb2.CoralogixIndexMappingAssetOptions
    signoz_log_attributes_model_filters: _signoz_asset_pb2.SignozLogAttributesAssetOptions
    victoria_logs_field_model_filters: _victoria_logs_asset_pb2.VictoriaLogsFieldAssetOptions
    linear_team_model_filters: _linear_asset_pb2.LinearTeamAssetOptions
    linear_user_model_filters: _linear_asset_pb2.LinearUserAssetOptions
    linear_project_model_filters: _linear_asset_pb2.LinearProjectAssetOptions
    posthog_dashboard_model_filters: _posthog_asset_pb2.PosthogDashboardAssetOptions
    posthog_project_model_filters: _posthog_asset_pb2.PosthogProjectAssetOptions
    metabase_dashboard_model_filters: _metabase_asset_pb2.MetabaseDashboardAssetOptions
    metabase_card_model_filters: _metabase_asset_pb2.MetabaseCardAssetOptions
    metabase_database_model_filters: _metabase_asset_pb2.MetabaseDatabaseAssetOptions
    metabase_collection_model_filters: _metabase_asset_pb2.MetabaseCollectionAssetOptions
    metabase_subscription_model_filters: _metabase_asset_pb2.MetabaseSubscriptionAssetOptions
    grafana_tempo_datasource_model_filters: _grafana_asset_pb2.GrafanaDatasourceAssetOptions
    grafana_tempo_service_model_filters: _grafana_asset_pb2.GrafanaTempoServiceAssetOptions
    mcp_asset_model_filters: _mcp_asset_pb2.McpAssetOptions
    def __init__(self, cloudwatch_log_group_model_filters: _Optional[_Union[_cloudwatch_asset_pb2.CloudwatchLogGroupAssetOptions, _Mapping]] = ..., cloudwatch_metric_model_filters: _Optional[_Union[_cloudwatch_asset_pb2.CloudwatchMetricAssetOptions, _Mapping]] = ..., grafana_target_metric_promql_model_filters: _Optional[_Union[_grafana_asset_pb2.GrafanaTargetMetricPromQlAssetOptions, _Mapping]] = ..., clickhouse_database_model_filters: _Optional[_Union[_clickhouse_asset_pb2.ClickhouseDatabaseAssetOptions, _Mapping]] = ..., slack_channel_model_filters: _Optional[_Union[_slack_asset_pb2.SlackChannelAssetOptions, _Mapping]] = ..., new_relic_entity_application_model_filters: _Optional[_Union[_newrelic_asset_pb2.NewRelicApplicationEntityAssetOptions, _Mapping]] = ..., new_relic_entity_dashboard_model_filters: _Optional[_Union[_newrelic_asset_pb2.NewRelicDashboardEntityAssetOptions, _Mapping]] = ..., datadog_service_model_filters: _Optional[_Union[_datadog_asset_pb2.DatadogServiceAssetOptions, _Mapping]] = ..., postgres_database_model_filters: _Optional[_Union[_postgres_asset_pb2.PostgresDatabaseAssetOptions, _Mapping]] = ..., eks_cluster_model_filters: _Optional[_Union[_eks_asset_pb2.EksClusterAssetOptions, _Mapping]] = ..., ssh_server_model_filters: _Optional[_Union[_bash_asset_pb2.BashSshServerAssetOptions, _Mapping]] = ..., azure_workspace_model_filters: _Optional[_Union[_azure_asset_pb2.AzureWorkspaceAssetOptions, _Mapping]] = ..., grafana_prometheus_datasource_model_filters: _Optional[_Union[_grafana_asset_pb2.GrafanaDatasourceAssetOptions, _Mapping]] = ..., gke_cluster_model_filters: _Optional[_Union[_gke_asset_pb2.GkeClusterAssetOptions, _Mapping]] = ..., elastic_search_index_model_filters: _Optional[_Union[_elastic_search_asset_pb2.ElasticSearchIndexAssetOptions, _Mapping]] = ..., open_search_index_model_filters: _Optional[_Union[_open_search_asset_pb2.OpenSearchIndexAssetOptions, _Mapping]] = ..., gcm_metric_model_filters: _Optional[_Union[_gcm_asset_pb2.GcmMetricAssetOptions, _Mapping]] = ..., datadog_oauth_service_model_filters: _Optional[_Union[_datadog_asset_pb2.DatadogServiceAssetOptions, _Mapping]] = ..., cloudwatch_log_group_query_model_filters: _Optional[_Union[_cloudwatch_asset_pb2.CloudwatchLogGroupQueryAssetOptions, _Mapping]] = ..., asana_project_model_filters: _Optional[_Union[_asana_asset_pb2.AsanaProjectAssetOptions, _Mapping]] = ..., grafana_alert_rule_model_filters: _Optional[_Union[_grafana_asset_pb2.GrafanaAlertRuleAssetOptions, _Mapping]] = ..., cloudwatch_alarm_model_filters: _Optional[_Union[_cloudwatch_asset_pb2.CloudwatchAlarmAssetOptions, _Mapping]] = ..., datadog_dashboard_model_filters: _Optional[_Union[_datadog_asset_pb2.DatadogDashboardAssetOptions, _Mapping]] = ..., datadog_oauth_dashboard_model_filters: _Optional[_Union[_datadog_asset_pb2.DatadogDashboardAssetOptions, _Mapping]] = ..., github_repository_model_filters: _Optional[_Union[_github_asset_pb2.GithubRepositoryAssetOptions, _Mapping]] = ..., github_member_model_filters: _Optional[_Union[_github_asset_pb2.GithubMemberAssetOptions, _Mapping]] = ..., jira_user_model_filters: _Optional[_Union[_jira_asset_pb2.JiraUserAssetOptions, _Mapping]] = ..., jira_project_model_filters: _Optional[_Union[_jira_asset_pb2.JiraProjectAssetOptions, _Mapping]] = ..., sentry_project_model_filters: _Optional[_Union[_sentry_asset_pb2.SentryProjectAssetOptions, _Mapping]] = ..., argocd_apps_model_filters: _Optional[_Union[_argocd_asset_pb2.ArgoCDAppsAssetOptions, _Mapping]] = ..., azure_resource_model_filters: _Optional[_Union[_azure_asset_pb2.AzureResourceAssetOptions, _Mapping]] = ..., grafana_dashboard_model_filters: _Optional[_Union[_grafana_asset_pb2.GrafanaDashboardAssetOptions, _Mapping]] = ..., jenkins_apps_model_filters: _Optional[_Union[_jenkins_asset_pb2.JenkinsAppsAssetOptions, _Mapping]] = ..., mongodb_database_model_filters: _Optional[_Union[_mongodb_asset_pb2.MongoDBDatabaseAssetOptions, _Mapping]] = ..., mongodb_collection_model_filters: _Optional[_Union[_mongodb_asset_pb2.MongoDBCollectionAssetOptions, _Mapping]] = ..., posthog_property_model_filters: _Optional[_Union[_posthog_asset_pb2.PosthogPropertyAssetOptions, _Mapping]] = ..., sql_table_model_filters: _Optional[_Union[_sql_database_asset_pb2.SqlTableAssetOptions, _Mapping]] = ..., ecs_cluster_model_filters: _Optional[_Union[_cloudwatch_asset_pb2.EcsClusterAssetOptions, _Mapping]] = ..., ecs_task_model_filters: _Optional[_Union[_cloudwatch_asset_pb2.EcsTaskAssetOptions, _Mapping]] = ..., ecs_service_model_filters: _Optional[_Union[_cloudwatch_asset_pb2.EcsServiceAssetOptions, _Mapping]] = ..., gcm_dashboard_model_filters: _Optional[_Union[_gcm_asset_pb2.GcmDashboardEntityAssetOptions, _Mapping]] = ..., gcm_cloud_run_service_model_filters: _Optional[_Union[_gcm_asset_pb2.GcmCloudRunServiceAssetOptions, _Mapping]] = ..., cloudwatch_dashboard_model_filters: _Optional[_Union[_cloudwatch_asset_pb2.CloudwatchDashboardAssetOptions, _Mapping]] = ..., signoz_dashboard_model_filters: _Optional[_Union[_signoz_asset_pb2.SignozDashboardAssetOptions, _Mapping]] = ..., signoz_alert_model_filters: _Optional[_Union[_signoz_asset_pb2.SignozAlertAssetOptions, _Mapping]] = ..., kubernetes_namespace_model_filters: _Optional[_Union[_kubectl_asset_pb2.KubernetesNamespaceAssetOptions, _Mapping]] = ..., kubernetes_service_model_filters: _Optional[_Union[_kubectl_asset_pb2.KubernetesServiceAssetOptions, _Mapping]] = ..., kubernetes_deployment_model_filters: _Optional[_Union[_kubectl_asset_pb2.KubernetesDeploymentAssetOptions, _Mapping]] = ..., kubernetes_ingress_model_filters: _Optional[_Union[_kubectl_asset_pb2.KubernetesIngressAssetOptions, _Mapping]] = ..., kubernetes_network_policy_model_filters: _Optional[_Union[_kubectl_asset_pb2.KubernetesNetworkPolicyAssetOptions, _Mapping]] = ..., kubernetes_hpa_model_filters: _Optional[_Union[_kubectl_asset_pb2.KubernetesHpaAssetOptions, _Mapping]] = ..., kubernetes_replicaset_model_filters: _Optional[_Union[_kubectl_asset_pb2.KubernetesReplicasetAssetOptions, _Mapping]] = ..., kubernetes_statefulset_model_filters: _Optional[_Union[_kubectl_asset_pb2.KubernetesStatefulsetAssetOptions, _Mapping]] = ..., kubernetes_network_map_model_filters: _Optional[_Union[_kubectl_asset_pb2.KubernetesNetworkMapAssetOptions, _Mapping]] = ..., elastic_search_service_model_filters: _Optional[_Union[_elastic_search_asset_pb2.ElasticSearchServiceAssetOptions, _Mapping]] = ..., elastic_search_dashboard_model_filters: _Optional[_Union[_elastic_search_asset_pb2.ElasticSearchDashboardAssetOptions, _Mapping]] = ..., elastic_search_index_pattern_model_filters: _Optional[_Union[_elastic_search_asset_pb2.ElasticSearchIndexPatternAssetOptions, _Mapping]] = ..., coralogix_dashboard_model_filters: _Optional[_Union[_coralogix_asset_pb2.CoralogixDashboardAssetOptions, _Mapping]] = ..., grafana_loki_datasource_model_filters: _Optional[_Union[_grafana_asset_pb2.GrafanaDatasourceAssetOptions, _Mapping]] = ..., signoz_service_model_filters: _Optional[_Union[_signoz_asset_pb2.SignozServiceAssetOptions, _Mapping]] = ..., gke_namespace_model_filters: _Optional[_Union[_gke_asset_pb2.GkeNamespaceAssetOptions, _Mapping]] = ..., gke_service_model_filters: _Optional[_Union[_gke_asset_pb2.GkeServiceOption, _Mapping]] = ..., gke_deployment_model_filters: _Optional[_Union[_gke_asset_pb2.GkeDeploymentOption, _Mapping]] = ..., gke_ingress_model_filters: _Optional[_Union[_gke_asset_pb2.GkeIngressAssetOptions, _Mapping]] = ..., gke_network_policy_model_filters: _Optional[_Union[_gke_asset_pb2.GkeNetworkPolicyAssetOptions, _Mapping]] = ..., gke_hpa_model_filters: _Optional[_Union[_gke_asset_pb2.GkeHpaAssetOptions, _Mapping]] = ..., gke_replicaset_model_filters: _Optional[_Union[_gke_asset_pb2.GkeReplicasetAssetOptions, _Mapping]] = ..., gke_statefulset_model_filters: _Optional[_Union[_gke_asset_pb2.GkeStatefulsetAssetOptions, _Mapping]] = ..., coralogix_index_mapping_model_filters: _Optional[_Union[_coralogix_asset_pb2.CoralogixIndexMappingAssetOptions, _Mapping]] = ..., signoz_log_attributes_model_filters: _Optional[_Union[_signoz_asset_pb2.SignozLogAttributesAssetOptions, _Mapping]] = ..., victoria_logs_field_model_filters: _Optional[_Union[_victoria_logs_asset_pb2.VictoriaLogsFieldAssetOptions, _Mapping]] = ..., linear_team_model_filters: _Optional[_Union[_linear_asset_pb2.LinearTeamAssetOptions, _Mapping]] = ..., linear_user_model_filters: _Optional[_Union[_linear_asset_pb2.LinearUserAssetOptions, _Mapping]] = ..., linear_project_model_filters: _Optional[_Union[_linear_asset_pb2.LinearProjectAssetOptions, _Mapping]] = ..., posthog_dashboard_model_filters: _Optional[_Union[_posthog_asset_pb2.PosthogDashboardAssetOptions, _Mapping]] = ..., posthog_project_model_filters: _Optional[_Union[_posthog_asset_pb2.PosthogProjectAssetOptions, _Mapping]] = ..., metabase_dashboard_model_filters: _Optional[_Union[_metabase_asset_pb2.MetabaseDashboardAssetOptions, _Mapping]] = ..., metabase_card_model_filters: _Optional[_Union[_metabase_asset_pb2.MetabaseCardAssetOptions, _Mapping]] = ..., metabase_database_model_filters: _Optional[_Union[_metabase_asset_pb2.MetabaseDatabaseAssetOptions, _Mapping]] = ..., metabase_collection_model_filters: _Optional[_Union[_metabase_asset_pb2.MetabaseCollectionAssetOptions, _Mapping]] = ..., metabase_subscription_model_filters: _Optional[_Union[_metabase_asset_pb2.MetabaseSubscriptionAssetOptions, _Mapping]] = ..., grafana_tempo_datasource_model_filters: _Optional[_Union[_grafana_asset_pb2.GrafanaDatasourceAssetOptions, _Mapping]] = ..., grafana_tempo_service_model_filters: _Optional[_Union[_grafana_asset_pb2.GrafanaTempoServiceAssetOptions, _Mapping]] = ..., mcp_asset_model_filters: _Optional[_Union[_mcp_asset_pb2.McpAssetOptions, _Mapping]] = ...) -> None: ...

class AccountConnectorAssets(_message.Message):
    __slots__ = ("connector", "cloudwatch", "grafana", "clickhouse", "slack", "new_relic", "datadog", "postgres", "eks", "bash", "azure", "gke", "elastic_search", "gcm", "datadog_oauth", "open_search", "asana", "github", "jira_cloud", "sentry", "argocd", "jenkins", "mongodb", "posthog", "sql", "signoz", "kubernetes", "coralogix", "victoria_logs", "linear", "metabase", "mcp_server")
    CONNECTOR_FIELD_NUMBER: _ClassVar[int]
    CLOUDWATCH_FIELD_NUMBER: _ClassVar[int]
    GRAFANA_FIELD_NUMBER: _ClassVar[int]
    CLICKHOUSE_FIELD_NUMBER: _ClassVar[int]
    SLACK_FIELD_NUMBER: _ClassVar[int]
    NEW_RELIC_FIELD_NUMBER: _ClassVar[int]
    DATADOG_FIELD_NUMBER: _ClassVar[int]
    POSTGRES_FIELD_NUMBER: _ClassVar[int]
    EKS_FIELD_NUMBER: _ClassVar[int]
    BASH_FIELD_NUMBER: _ClassVar[int]
    AZURE_FIELD_NUMBER: _ClassVar[int]
    GKE_FIELD_NUMBER: _ClassVar[int]
    ELASTIC_SEARCH_FIELD_NUMBER: _ClassVar[int]
    GCM_FIELD_NUMBER: _ClassVar[int]
    DATADOG_OAUTH_FIELD_NUMBER: _ClassVar[int]
    OPEN_SEARCH_FIELD_NUMBER: _ClassVar[int]
    ASANA_FIELD_NUMBER: _ClassVar[int]
    GITHUB_FIELD_NUMBER: _ClassVar[int]
    JIRA_CLOUD_FIELD_NUMBER: _ClassVar[int]
    SENTRY_FIELD_NUMBER: _ClassVar[int]
    ARGOCD_FIELD_NUMBER: _ClassVar[int]
    JENKINS_FIELD_NUMBER: _ClassVar[int]
    MONGODB_FIELD_NUMBER: _ClassVar[int]
    POSTHOG_FIELD_NUMBER: _ClassVar[int]
    SQL_FIELD_NUMBER: _ClassVar[int]
    SIGNOZ_FIELD_NUMBER: _ClassVar[int]
    KUBERNETES_FIELD_NUMBER: _ClassVar[int]
    CORALOGIX_FIELD_NUMBER: _ClassVar[int]
    VICTORIA_LOGS_FIELD_NUMBER: _ClassVar[int]
    LINEAR_FIELD_NUMBER: _ClassVar[int]
    METABASE_FIELD_NUMBER: _ClassVar[int]
    MCP_SERVER_FIELD_NUMBER: _ClassVar[int]
    connector: _connector_pb2.Connector
    cloudwatch: _cloudwatch_asset_pb2.CloudwatchAssets
    grafana: _grafana_asset_pb2.GrafanaAssets
    clickhouse: _clickhouse_asset_pb2.ClickhouseAssets
    slack: _slack_asset_pb2.SlackAssets
    new_relic: _newrelic_asset_pb2.NewRelicAssets
    datadog: _datadog_asset_pb2.DatadogAssets
    postgres: _postgres_asset_pb2.PostgresAssets
    eks: _eks_asset_pb2.EksAssets
    bash: _bash_asset_pb2.BashAssets
    azure: _azure_asset_pb2.AzureAssets
    gke: _gke_asset_pb2.GkeAssets
    elastic_search: _elastic_search_asset_pb2.ElasticSearchAssets
    gcm: _gcm_asset_pb2.GcmAssets
    datadog_oauth: _datadog_asset_pb2.DatadogAssets
    open_search: _open_search_asset_pb2.OpenSearchAssets
    asana: _asana_asset_pb2.AsanaAssets
    github: _github_asset_pb2.GithubAssets
    jira_cloud: _jira_asset_pb2.JiraAssets
    sentry: _sentry_asset_pb2.SentryAssets
    argocd: _argocd_asset_pb2.ArgoCDAssets
    jenkins: _jenkins_asset_pb2.JenkinsAssets
    mongodb: _mongodb_asset_pb2.MongoDBAssets
    posthog: _posthog_asset_pb2.PosthogAssets
    sql: _sql_database_asset_pb2.SqlAssets
    signoz: _signoz_asset_pb2.SignozAssets
    kubernetes: _kubectl_asset_pb2.KubernetesAssets
    coralogix: _coralogix_asset_pb2.CoralogixAssets
    victoria_logs: _victoria_logs_asset_pb2.VictoriaLogsAssets
    linear: _linear_asset_pb2.LinearAssets
    metabase: _metabase_asset_pb2.MetabaseAssets
    mcp_server: _mcp_asset_pb2.McpAssets
    def __init__(self, connector: _Optional[_Union[_connector_pb2.Connector, _Mapping]] = ..., cloudwatch: _Optional[_Union[_cloudwatch_asset_pb2.CloudwatchAssets, _Mapping]] = ..., grafana: _Optional[_Union[_grafana_asset_pb2.GrafanaAssets, _Mapping]] = ..., clickhouse: _Optional[_Union[_clickhouse_asset_pb2.ClickhouseAssets, _Mapping]] = ..., slack: _Optional[_Union[_slack_asset_pb2.SlackAssets, _Mapping]] = ..., new_relic: _Optional[_Union[_newrelic_asset_pb2.NewRelicAssets, _Mapping]] = ..., datadog: _Optional[_Union[_datadog_asset_pb2.DatadogAssets, _Mapping]] = ..., postgres: _Optional[_Union[_postgres_asset_pb2.PostgresAssets, _Mapping]] = ..., eks: _Optional[_Union[_eks_asset_pb2.EksAssets, _Mapping]] = ..., bash: _Optional[_Union[_bash_asset_pb2.BashAssets, _Mapping]] = ..., azure: _Optional[_Union[_azure_asset_pb2.AzureAssets, _Mapping]] = ..., gke: _Optional[_Union[_gke_asset_pb2.GkeAssets, _Mapping]] = ..., elastic_search: _Optional[_Union[_elastic_search_asset_pb2.ElasticSearchAssets, _Mapping]] = ..., gcm: _Optional[_Union[_gcm_asset_pb2.GcmAssets, _Mapping]] = ..., datadog_oauth: _Optional[_Union[_datadog_asset_pb2.DatadogAssets, _Mapping]] = ..., open_search: _Optional[_Union[_open_search_asset_pb2.OpenSearchAssets, _Mapping]] = ..., asana: _Optional[_Union[_asana_asset_pb2.AsanaAssets, _Mapping]] = ..., github: _Optional[_Union[_github_asset_pb2.GithubAssets, _Mapping]] = ..., jira_cloud: _Optional[_Union[_jira_asset_pb2.JiraAssets, _Mapping]] = ..., sentry: _Optional[_Union[_sentry_asset_pb2.SentryAssets, _Mapping]] = ..., argocd: _Optional[_Union[_argocd_asset_pb2.ArgoCDAssets, _Mapping]] = ..., jenkins: _Optional[_Union[_jenkins_asset_pb2.JenkinsAssets, _Mapping]] = ..., mongodb: _Optional[_Union[_mongodb_asset_pb2.MongoDBAssets, _Mapping]] = ..., posthog: _Optional[_Union[_posthog_asset_pb2.PosthogAssets, _Mapping]] = ..., sql: _Optional[_Union[_sql_database_asset_pb2.SqlAssets, _Mapping]] = ..., signoz: _Optional[_Union[_signoz_asset_pb2.SignozAssets, _Mapping]] = ..., kubernetes: _Optional[_Union[_kubectl_asset_pb2.KubernetesAssets, _Mapping]] = ..., coralogix: _Optional[_Union[_coralogix_asset_pb2.CoralogixAssets, _Mapping]] = ..., victoria_logs: _Optional[_Union[_victoria_logs_asset_pb2.VictoriaLogsAssets, _Mapping]] = ..., linear: _Optional[_Union[_linear_asset_pb2.LinearAssets, _Mapping]] = ..., metabase: _Optional[_Union[_metabase_asset_pb2.MetabaseAssets, _Mapping]] = ..., mcp_server: _Optional[_Union[_mcp_asset_pb2.McpAssets, _Mapping]] = ...) -> None: ...

class AssetMetadataModel(_message.Message):
    __slots__ = ("model_uid", "model_type", "metadata")
    MODEL_UID_FIELD_NUMBER: _ClassVar[int]
    MODEL_TYPE_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    model_uid: _wrappers_pb2.StringValue
    model_type: _base_pb2.SourceModelType
    metadata: _struct_pb2.Struct
    def __init__(self, model_uid: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., model_type: _Optional[_Union[_base_pb2.SourceModelType, str]] = ..., metadata: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...) -> None: ...
