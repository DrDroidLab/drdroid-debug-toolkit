import logging

from google.protobuf.wrappers_pb2 import StringValue

from core.integrations.source_manager import SourceManager
from core.integrations.source_managers.api_source_manager import ApiSourceManager
from core.integrations.source_managers.azure_source_manager import AzureSourceManager
from core.integrations.source_managers.bash_source_manager import BashSourceManager
from core.integrations.source_managers.big_query_source_manager import BigQuerySourceManager
from core.integrations.source_managers.clickhouse_source_manager import ClickhouseSourceManager
from core.integrations.source_managers.cloudwatch_source_manager import CloudwatchSourceManager
from core.integrations.source_managers.datadog_source_manager import DatadogSourceManager
from core.integrations.source_managers.datadog_oauth_soruce_manager import DatadogSourceManager as DatadogOauthSourceManager
from core.integrations.source_managers.documentation_source_manager import DocumentationSourceManager
from core.integrations.source_managers.eks_source_manager import EksSourceManager
from core.integrations.source_managers.elastic_search_source_manager import ElasticSearchSourceManager
from core.integrations.source_managers.gcm_source_manager import GcmSourceManager
from core.integrations.source_managers.github_source_manager import GithubSourceManager
from core.integrations.source_managers.gke_source_manager import GkeSourceManager
from core.integrations.source_managers.grafana_loki_source_manager import GrafanaLokiSourceManager
from core.integrations.source_managers.grafana_source_manager import GrafanaSourceManager
from core.integrations.source_managers.jenkins_source_manager import JenkinsSourceManager
from core.integrations.source_managers.kubernetes_source_manager import KubernetesSourceManager
from core.integrations.source_managers.mimir_source_manager import MimirSourceManager
from core.integrations.source_managers.mongodb_source_manager import MongoDBSourceManager
from core.integrations.source_managers.newrelic_source_manager import NewRelicSourceManager
from core.integrations.source_managers.open_search_source_manager import OpenSearchSourceManager
from core.integrations.source_managers.postgres_source_manager import PostgresSourceManager
from core.integrations.source_managers.rootly_source_manager import RootlySourceManager
from core.integrations.source_managers.slack_source_manager import SlackSourceManager
from core.integrations.source_managers.smtp_source_manager import SMTPSourceManager
from core.integrations.source_managers.sql_database_connection_source_manager import SqlDatabaseConnectionSourceManager
from core.integrations.source_managers.zenduty_source_manager import ZendutySourceManager
from core.integrations.source_managers.argocd_source_manager import ArgoCDSourceManager
from core.integrations.source_managers.jira_source_manager import JiraSourceManager
from core.integrations.source_managers.posthog_source_manager import PosthogSourceManager
from core.integrations.source_managers.signoz_source_manager import SignozSourceManager
from core.integrations.source_managers.sentry_source_manager import SentrySourceManager
from core.integrations.source_managers.github_actions_source_manager import GithubActionsSourceManager
from core.integrations.source_managers.coralogix_source_manager import CoralogixSourceManager
from core.integrations.source_managers.render_source_manager import RenderSourceManager
from core.integrations.source_managers.victoria_logs_source_manager import VictoriaLogsSourceManager
from core.integrations.source_managers.metabase_source_manager import MetabaseSourceManager
from core.integrations.source_managers.bitbucket_source_manager import BitbucketSourceManager
from core.protos.base_pb2 import Source
from core.protos.connectors.connector_pb2 import Connector as ConnectorProto
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResult

from core.protos.playbooks.playbook_pb2 import PlaybookTask

logger = logging.getLogger(__name__)


class SourceFacade:

    def __init__(self):
        self._map = {}

    def register(self, source: Source, manager: SourceManager):
        self._map[source] = manager

    def get_source_manager(self, source: Source):
        if source not in self._map:
            raise ValueError(f'No executor found for source: {source}')
        return self._map.get(source)

    def execute_task(self, time_range, global_variable_set, task: PlaybookTask):
        source = task.source
        if source not in self._map:
            raise ValueError(f'No executor found for source: {source}')
        manager = self._map[source]
        try:
            return manager.execute_task(time_range, global_variable_set, task)
        except Exception as e:
            logger.error(f'Error while executing task: {str(e)}')
            return PlaybookTaskResult(error=StringValue(value=str(e)))

    def test_source_connection(self, source_connection: ConnectorProto):
        source = source_connection.type
        if source not in self._map:
            return False, f'No executor found for source: {source}'
        manager = self._map[source]
        try:
            return manager.test_connector_processor(source_connection), None
        except Exception as e:
            logger.error(f'Error while testing source connection: {str(e)}')
            return False, str(e)

    def get_task_types(self, connector_type: Source):
        playbook_source_manager = self.get_source_manager(connector_type)
        task_protos = playbook_source_manager.task_proto
        return task_protos

    def check_required_connector_keys(self, connector: ConnectorProto):
        playbook_source_manager = self.get_source_manager(connector.type)
        return playbook_source_manager.check_required_connector_keys(connector)

    def get_required_connector_key_types(self, connector_type: Source):
        playbook_source_manager = self.get_source_manager(connector_type)
        return playbook_source_manager.get_required_connector_key_types()

    def get_connector_keys_display_name_map(self, connector_type: Source):
        playbook_source_manager = self.get_source_manager(connector_type)
        return playbook_source_manager.get_connector_keys_display_name_map()

    def get_connector_type_details(self, connector_type: Source):
        playbook_source_manager = self.get_source_manager(connector_type)
        return playbook_source_manager.get_connector_type_details()

    def get_connector_required_keys(self, connector_type: Source):
        playbook_source_manager = self.get_source_manager(connector_type)
        return playbook_source_manager.get_connector_required_keys()

    def get_all_available_connector_integrations(self):
        return self._map.keys()

    def get_connector_masked_keys(self, connector_type: Source):
        playbook_source_manager = self.get_source_manager(connector_type)
        return playbook_source_manager.get_connector_masked_keys()


source_facade = SourceFacade()
source_facade.register(Source.CLOUDWATCH, CloudwatchSourceManager())
source_facade.register(Source.EKS, EksSourceManager())
source_facade.register(Source.DATADOG, DatadogSourceManager())
source_facade.register(Source.DATADOG_OAUTH, DatadogOauthSourceManager())
source_facade.register(Source.NEW_RELIC, NewRelicSourceManager())
source_facade.register(Source.GRAFANA, GrafanaSourceManager())
source_facade.register(Source.GRAFANA_MIMIR, MimirSourceManager())
source_facade.register(Source.AZURE, AzureSourceManager())
source_facade.register(Source.GKE, GkeSourceManager())
source_facade.register(Source.GCM, GcmSourceManager())
source_facade.register(Source.GRAFANA_LOKI, GrafanaLokiSourceManager())

source_facade.register(Source.POSTGRES, PostgresSourceManager())
source_facade.register(Source.CLICKHOUSE, ClickhouseSourceManager())
source_facade.register(Source.SQL_DATABASE_CONNECTION, SqlDatabaseConnectionSourceManager())
source_facade.register(Source.ELASTIC_SEARCH, ElasticSearchSourceManager())
source_facade.register(Source.BIG_QUERY, BigQuerySourceManager())
source_facade.register(Source.MONGODB, MongoDBSourceManager())
source_facade.register(Source.OPEN_SEARCH, OpenSearchSourceManager())

source_facade.register(Source.API, ApiSourceManager())
source_facade.register(Source.BASH, BashSourceManager())
source_facade.register(Source.KUBERNETES, KubernetesSourceManager())
source_facade.register(Source.SMTP, SMTPSourceManager())
source_facade.register(Source.SLACK, SlackSourceManager())

source_facade.register(Source.DOCUMENTATION, DocumentationSourceManager())
source_facade.register(Source.ROOTLY, RootlySourceManager())
source_facade.register(Source.ZENDUTY, ZendutySourceManager())

source_facade.register(Source.GITHUB, GithubSourceManager())
source_facade.register(Source.ARGOCD, ArgoCDSourceManager())
source_facade.register(Source.JIRA_CLOUD, JiraSourceManager())
source_facade.register(Source.JENKINS, JenkinsSourceManager())

source_facade.register(Source.POSTHOG, PosthogSourceManager())
source_facade.register(Source.SIGNOZ, SignozSourceManager())
source_facade.register(Source.SENTRY, SentrySourceManager())
source_facade.register(Source.GITHUB_ACTIONS, GithubActionsSourceManager())
source_facade.register(Source.CORALOGIX, CoralogixSourceManager())
source_facade.register(Source.RENDER, RenderSourceManager())
source_facade.register(Source.VICTORIA_LOGS, VictoriaLogsSourceManager())
source_facade.register(Source.METABASE, MetabaseSourceManager())
source_facade.register(Source.BITBUCKET, BitbucketSourceManager())