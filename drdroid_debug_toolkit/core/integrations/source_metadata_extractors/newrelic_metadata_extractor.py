from core.integrations.source_metadata_extractor import SourceMetadataExtractor
from core.integrations.source_api_processors.new_relic_graph_ql_processor import NewRelicGraphQlConnector
from core.protos.base_pb2 import Source, SourceModelType
from core.utils.logging_utils import log_function_call
from core.utils.static_mappings import NEWRELIC_APM_QUERIES

class NewrelicSourceMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id: str, connector_name: str, nr_api_key, nr_app_id, nr_api_domain='api.newrelic.com'):
        self.__gql_processor = NewRelicGraphQlConnector(nr_api_key, nr_app_id, nr_api_domain)

        super().__init__(request_id, connector_name, Source.NEW_RELIC)

    @log_function_call
    def extract_policy(self):
        model_type = SourceModelType.NEW_RELIC_POLICY
        model_data = self._collect_policies_data()
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    def _collect_policies_data(self):
        """Collect policies data - used by both extract_policy and get_policies_data."""
        cursor = 'null'
        policies = []
        policies_search = self.__gql_processor.get_all_policies(cursor)
        if 'policies' not in policies_search:
            return {}
        results = policies_search['policies']
        policies.extend(results)
        if 'nextCursor' in policies_search:
            cursor = policies_search['nextCursor']
        while cursor and cursor != 'null':
            policies_search = self.__gql_processor.get_all_policies(cursor)
            if 'policies' in policies_search:
                results = policies_search['policies']
                policies.extend(results)
                if 'nextCursor' in policies_search:
                    cursor = policies_search['nextCursor']
                else:
                    cursor = None
            else:
                break
        model_data = {}
        for policy in policies:
            policy_id = policy['id']
            model_data[policy_id] = policy
        return model_data

    def get_policies_data(self):
        """Get policies data directly without storing to database."""
        return self._collect_policies_data()

    def extract_entity(self):
        model_type = SourceModelType.NEW_RELIC_ENTITY
        model_data = self._collect_entities_data()
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    def _collect_entities_data(self):
        """Collect entities data - used by both extract_entity and get_entities_data."""
        cursor = 'null'
        types = ['HOST', 'MONITOR', 'WORKLOAD']
        entity_search = self.__gql_processor.get_all_entities(cursor, types)
        if 'results' not in entity_search:
            return {}
        results = entity_search['results']
        entities = []
        if 'entities' in results:
            entities.extend(results['entities'])
        if 'nextCursor' in results:
            cursor = results['nextCursor']
        while cursor and cursor != 'null':
            entity_search = self.__gql_processor.get_all_entities(cursor, types)
            if 'results' in entity_search:
                results = entity_search['results']
                if 'entities' in results:
                    entities.extend(results['entities'])
                if 'nextCursor' in results:
                    cursor = results['nextCursor']
                else:
                    cursor = None
            else:
                break
        model_data = {}
        for entity in entities:
            entity_guid = entity['guid']
            model_data[entity_guid] = entity
        return model_data

    def get_entities_data(self):
        """Get entities data directly without storing to database."""
        return self._collect_entities_data()

    def extract_condition(self):
        model_type = SourceModelType.NEW_RELIC_CONDITION
        model_data = self._collect_conditions_data()
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    def _collect_conditions_data(self):
        """Collect conditions data - used by both extract_condition and get_conditions_data."""
        cursor = 'null'
        conditions = []
        conditions_search = self.__gql_processor.get_all_conditions(cursor)
        if 'nrqlConditions' not in conditions_search:
            return {}
        results = conditions_search['nrqlConditions']
        conditions.extend(results)
        if 'nextCursor' in conditions_search:
            cursor = conditions_search['nextCursor']
        while cursor and cursor != 'null':
            conditions_search = self.__gql_processor.get_all_conditions(cursor)
            if 'nrqlConditions' in conditions_search:
                results = conditions_search['nrqlConditions']
                conditions.extend(results)
                if 'nextCursor' in conditions_search:
                    cursor = conditions_search['nextCursor']
                else:
                    cursor = None
            else:
                break
        model_data = {}
        for condition in conditions:
            condition_id = condition['id']
            model_data[condition_id] = condition
        return model_data

    def get_conditions_data(self):
        """Get conditions data directly without storing to database."""
        return self._collect_conditions_data()

    def extract_dashboard_entity(self):
        model_type = SourceModelType.NEW_RELIC_ENTITY_DASHBOARD
        model_data = self._collect_dashboard_entities_data()
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    def _collect_dashboard_entities_data(self):
        """Collect dashboard entities data - used by both extract_dashboard_entity and get_dashboard_entities_data."""
        cursor = 'null'
        types = ['DASHBOARD']
        entity_search = self.__gql_processor.get_all_entities(cursor, types)
        if 'results' not in entity_search:
            return {}
        results = entity_search['results']
        entities = []
        if 'entities' in results:
            entities.extend(results['entities'])
        if 'nextCursor' in results:
            cursor = results['nextCursor']
        while cursor and cursor != 'null':
            entity_search = self.__gql_processor.get_all_entities(cursor, types)
            if 'results' in entity_search:
                results = entity_search['results']
                if 'entities' in results:
                    entities.extend(results['entities'])
                if 'nextCursor' in results:
                    cursor = results['nextCursor']
                else:
                    cursor = None
            else:
                break
        dashboard_entity_guid = [entity['guid'] for entity in entities]
        model_data = {}
        for i in range(0, len(dashboard_entity_guid), 25):
            dashboard_entity_search = self.__gql_processor.get_all_dashboard_entities(dashboard_entity_guid[i:i + 25])
            if not dashboard_entity_search or len(dashboard_entity_search) == 0:
                continue
            for entity in dashboard_entity_search:
                entity_id = entity['guid']
                model_data[entity_id] = entity
        return model_data

    def get_dashboard_entities_data(self):
        """Get dashboard entities data directly without storing to database."""
        return self._collect_dashboard_entities_data()

    def extract_application_entity(self):
        model_type = SourceModelType.NEW_RELIC_ENTITY_APPLICATION
        model_data = self._collect_application_entities_data()
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    def _collect_application_entities_data(self):
        """Collect application entities data - used by both extract_application_entity and get_application_entities_data."""
        cursor = 'null'
        types = ['APPLICATION']
        entity_search = self.__gql_processor.get_all_entities(cursor, types)
        if 'results' not in entity_search:
            return {}
        results = entity_search['results']
        entities = []
        if 'entities' in results:
            entities.extend(results['entities'])
        if 'nextCursor' in results:
            cursor = results['nextCursor']
        while cursor and cursor != 'null':
            entity_search = self.__gql_processor.get_all_entities(cursor, types)
            if 'results' in entity_search:
                results = entity_search['results']
                if 'entities' in results:
                    entities.extend(results['entities'])
                if 'nextCursor' in results:
                    cursor = results['nextCursor']
                else:
                    cursor = None
            else:
                break
        model_data = {}
        for entity in entities:
            entity_guid = entity['guid']
            entity['apm_summary'] = []
            for metric_name, query in NEWRELIC_APM_QUERIES.items():
                formatted_query = query.replace('{}', f"'{entity_guid}'")
                apm_metric = {
                    'name': metric_name,
                    'unit': '',
                    'query': formatted_query
                }
                entity['apm_summary'].append(apm_metric)
            
            model_data[entity_guid] = entity
        return model_data

    def get_application_entities_data(self):
        """Get application entities data directly without storing to database."""
        return self._collect_application_entities_data()

    def extract_dashboard_entity_v2(self):
        model_type = SourceModelType.NEW_RELIC_ENTITY_DASHBOARD_V2  
        model_data = self._collect_dashboard_entities_v2_data()
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    def _collect_dashboard_entities_v2_data(self):
        """Collect dashboard entities v2 data - used by both extract_dashboard_entity_v2 and get_dashboard_entities_v2_data."""
        cursor = 'null'
        types = ['DASHBOARD']
        entity_search = self.__gql_processor.get_all_entities(cursor, types)
        if 'results' not in entity_search:
            return {}
        results = entity_search['results']
        entities = []
        if 'entities' in results:
            entities.extend(results['entities'])
        if 'nextCursor' in results:
            cursor = results['nextCursor']
        while cursor and cursor != 'null':
            entity_search = self.__gql_processor.get_all_entities(cursor, types)
            if 'results' in entity_search:
                results = entity_search['results']
                if 'entities' in results:
                    entities.extend(results['entities'])
                if 'nextCursor' in results:
                    cursor = results['nextCursor']
                else:
                    cursor = None
            else:
                break
        dashboard_entity_guid = [entity['guid'] for entity in entities]
        model_data = {}
        for i in range(0, len(dashboard_entity_guid), 25):
            dashboard_entity_search = self.__gql_processor.get_all_dashboard_entities(dashboard_entity_guid[i:i + 25])
            if not dashboard_entity_search or len(dashboard_entity_search) == 0:
                continue
            for entity in dashboard_entity_search:
                entity_id = entity['guid']
                model_data[entity_id] = entity
        return model_data

    def get_dashboard_entities_v2_data(self):
        """Get dashboard entities v2 data directly without storing to database."""
        return self._collect_dashboard_entities_v2_data()
