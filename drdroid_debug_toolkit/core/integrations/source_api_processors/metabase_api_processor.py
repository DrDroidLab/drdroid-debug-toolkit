import copy
import logging

import requests

from core.integrations.processor import Processor
from core.settings import EXTERNAL_CALL_TIMEOUT

logger = logging.getLogger(__name__)


class MetabaseApiProcessor(Processor):
    def __init__(self, metabase_url, metabase_api_key):
        self.__host = metabase_url.rstrip('/')
        self.__api_key = metabase_api_key
        self.headers = {
            "x-api-key": self.__api_key,
            "Content-Type": "application/json"
        }

    def test_connection(self):
        """
        Test connection to Metabase by calling the /api/user/current endpoint.
        This endpoint returns the current user info and validates API key authentication.

        Returns:
            bool: True if connection is successful
        Raises:
            Exception: If connection fails
        """
        try:
            url = f"{self.__host}/api/user/current"
            response = requests.get(url, headers=self.headers, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return True
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error while testing Metabase connection: {e}")
            raise Exception(f"Metabase connection failed: {e}")
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error while testing Metabase connection: {e}")
            raise Exception(f"Could not connect to Metabase at {self.__host}: {e}")
        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout while testing Metabase connection: {e}")
            raise Exception(f"Metabase connection timed out: {e}")
        except Exception as e:
            logger.error(f"Exception occurred while testing Metabase connection: {e}")
            raise e

    # Alert endpoints

    def list_alerts(self):
        try:
            url = f"{self.__host}/api/alert"
            response = requests.get(url, headers=self.headers, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.list_alerts:: Error listing alerts: {e}")
            raise

    def get_alert(self, alert_id):
        try:
            url = f"{self.__host}/api/alert/{alert_id}"
            response = requests.get(url, headers=self.headers, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.get_alert:: Error getting alert {alert_id}: {e}")
            raise

    def create_alert(self, payload):
        try:
            url = f"{self.__host}/api/alert"
            response = requests.post(url, headers=self.headers, json=payload, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.create_alert:: Error creating alert: {e}")
            raise

    def update_alert(self, alert_id, payload):
        try:
            url = f"{self.__host}/api/alert/{alert_id}"
            response = requests.put(url, headers=self.headers, json=payload, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.update_alert:: Error updating alert {alert_id}: {e}")
            raise

    def delete_alert(self, alert_id):
        try:
            url = f"{self.__host}/api/alert/{alert_id}"
            response = requests.delete(url, headers=self.headers, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.delete_alert:: Error deleting alert {alert_id}: {e}")
            raise

    # Pulse endpoints

    def list_pulses(self):
        try:
            url = f"{self.__host}/api/pulse"
            response = requests.get(url, headers=self.headers, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.list_pulses:: Error listing pulses: {e}")
            raise

    def get_pulse(self, pulse_id):
        try:
            url = f"{self.__host}/api/pulse/{pulse_id}"
            response = requests.get(url, headers=self.headers, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.get_pulse:: Error getting pulse {pulse_id}: {e}")
            raise

    def create_pulse(self, payload):
        try:
            url = f"{self.__host}/api/pulse"
            response = requests.post(url, headers=self.headers, json=payload, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.create_pulse:: Error creating pulse: {e}")
            raise

    def update_pulse(self, pulse_id, payload):
        try:
            url = f"{self.__host}/api/pulse/{pulse_id}"
            response = requests.put(url, headers=self.headers, json=payload, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.update_pulse:: Error updating pulse {pulse_id}: {e}")
            raise

    def delete_pulse(self, pulse_id):
        try:
            url = f"{self.__host}/api/pulse/{pulse_id}"
            response = requests.delete(url, headers=self.headers, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.delete_pulse:: Error deleting pulse {pulse_id}: {e}")
            raise

    # Dashboard endpoints (https://www.metabase.com/docs/latest/api/dashboard)

    def list_dashboards(self):
        try:
            url = f"{self.__host}/api/dashboard/"
            response = requests.get(url, headers=self.headers, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.list_dashboards:: Error listing dashboards: {e}")
            raise

    def create_dashboard(self, payload):
        try:
            url = f"{self.__host}/api/dashboard/"
            response = requests.post(url, headers=self.headers, json=payload, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.create_dashboard:: Error creating dashboard: {e}")
            raise

    def get_dashboard(self, dashboard_id):
        try:
            url = f"{self.__host}/api/dashboard/{dashboard_id}"
            response = requests.get(url, headers=self.headers, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.get_dashboard:: Error getting dashboard {dashboard_id}: {e}")
            raise

    def update_dashboard(self, dashboard_id, payload):
        try:
            url = f"{self.__host}/api/dashboard/{dashboard_id}"
            response = requests.put(url, headers=self.headers, json=payload, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.update_dashboard:: Error updating dashboard {dashboard_id}: {e}")
            raise

    def get_dashboard_cards(self, dashboard_id):
        """Get all cards (dashcards) in a dashboard. GET /api/dashboard/{id}/items"""
        try:
            url = f"{self.__host}/api/dashboard/{dashboard_id}/items"
            response = requests.get(url, headers=self.headers, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.get_dashboard_cards:: Error getting dashboard cards {dashboard_id}: {e}")
            raise

    def update_dashboard_cards(self, dashboard_id, cards_payload):
        """Replace/update cards on a dashboard. PUT /api/dashboard/{id}/cards. Body is array of dashcards."""
        try:
            url = f"{self.__host}/api/dashboard/{dashboard_id}/cards"
            response = requests.put(url, headers=self.headers, json=cards_payload, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.update_dashboard_cards:: Error updating dashboard cards {dashboard_id}: {e}")
            raise

    # Card/Question endpoints (https://www.metabase.com/docs/latest/api/card)

    def list_cards(self):
        try:
            url = f"{self.__host}/api/card/"
            response = requests.get(url, headers=self.headers, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.list_cards:: Error listing cards: {e}")
            raise

    def create_card(self, payload):
        try:
            url = f"{self.__host}/api/card/"
            response = requests.post(url, headers=self.headers, json=payload, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.create_card:: Error creating card: {e}")
            raise

    def get_card(self, card_id):
        try:
            url = f"{self.__host}/api/card/{card_id}"
            response = requests.get(url, headers=self.headers, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.get_card:: Error getting card {card_id}: {e}")
            raise

    def _normalize_dataset_query_for_update(self, existing_dq, new_dq):
        """
        Ensure dataset_query is in the legacy API format (type, native, database) that
        Metabase PUT /api/card/{id} accepts and persists. Do NOT send internal keys
        like "lib/type" or "stages" - they can cause the API to reject or clear the
        query and break the card in the UI and on execute.
        If existing has full "stages" structure (GET returned it), preserve it and
        only update the native SQL; otherwise send only legacy format.
        """
        if not isinstance(new_dq, dict) or new_dq.get("type") != "native":
            return new_dq
        native_block = new_dq.get("native")
        if not isinstance(native_block, dict) or "query" not in native_block:
            return new_dq
        database_id = new_dq.get("database")
        # Ensure native has template-tags (API expects it for native questions)
        native = dict(native_block)
        if "template-tags" not in native:
            native["template-tags"] = {}
        legacy_dq = {
            "type": "native",
            "database": database_id,
            "native": native,
        }
        # If existing has full stages structure (rare - GET often returns {}), reuse it
        # and only set the native SQL so we don't lose any extra keys Metabase stores
        if (
            isinstance(existing_dq, dict)
            and existing_dq
            and "stages" in existing_dq
            and isinstance(existing_dq["stages"], list)
            and len(existing_dq["stages"]) > 0
            and isinstance(existing_dq["stages"][0], dict)
        ):
            merged_dq = copy.deepcopy(existing_dq)
            merged_dq["database"] = database_id or merged_dq.get("database")
            merged_dq["type"] = "native"
            merged_dq["native"] = native
            merged_dq["stages"][0]["native"] = native_block["query"]
            merged_dq["stages"][0]["template-tags"] = native.get("template-tags", {})
            return merged_dq
        return legacy_dq

    def update_card(self, card_id, payload):
        """
        Update a card by ID. Compliant with Metabase API PUT /api/card/{id}.
        See: https://www.metabase.com/docs/latest/api#tag/apicard/put/api/card/{id}

        Fetches the full card first (GET /api/card/{id}), merges payload into it,
        then PUTs the full object. Sending a partial body can cause Metabase to clear
        fields like dataset_query; merging avoids that so execute works after update.
        Auth: x-api-key header (API key). Response may redact dataset_query; the
        update is still persisted when the request body is valid.
        """
        try:
            existing = self.get_card(card_id)
            # Merge payload into existing so we never send a partial card
            if isinstance(existing, dict) and isinstance(payload, dict):
                merged = dict(existing)
                for key, value in payload.items():
                    if value is not None:
                        if key == "dataset_query":
                            value = self._normalize_dataset_query_for_update(
                                merged.get(key) or {}, value
                            )
                        merged[key] = value
            else:
                merged = payload
            url = f"{self.__host}/api/card/{card_id}"
            response = requests.put(url, headers=self.headers, json=merged, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.update_card:: Error updating card {card_id}: {e}")
            raise

    def execute_card(self, card_id, parameters=None):
        """Execute a question/card. POST /api/card/{card-id}/query. parameters: list of param values."""
        try:
            url = f"{self.__host}/api/card/{card_id}/query"
            body = {}
            if parameters is not None:
                body["parameters"] = parameters if isinstance(parameters, list) else []
            response = requests.post(url, headers=self.headers, json=body, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.execute_card:: Error executing card {card_id}: {e}")
            raise

    # Database endpoints (https://www.metabase.com/docs/latest/api/database)

    def list_databases(self):
        try:
            url = f"{self.__host}/api/database/"
            response = requests.get(url, headers=self.headers, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.list_databases:: Error listing databases: {e}")
            raise

    def get_database_metadata(self, database_id):
        """Get database metadata (schemas, tables). GET /api/database/{id}/metadata"""
        try:
            url = f"{self.__host}/api/database/{database_id}/metadata"
            response = requests.get(url, headers=self.headers, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.get_database_metadata:: Error getting metadata for db {database_id}: {e}")
            raise

    def get_database_schemas(self, database_id):
        """GET /api/database/{id}/schemas"""
        try:
            url = f"{self.__host}/api/database/{database_id}/schemas"
            response = requests.get(url, headers=self.headers, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.get_database_schemas:: Error getting schemas for db {database_id}: {e}")
            raise

    # Dataset endpoint for native SQL (https://www.metabase.com/docs/latest/api/dataset)

    def execute_native_query(self, database_id, query):
        """Execute raw SQL. POST /api/dataset/native. Body: database, type: native, native: { query }."""
        try:
            url = f"{self.__host}/api/dataset/native"
            body = {
                "database": database_id,
                "type": "native",
                "native": {
                    "query": query,
                }
            }
            response = requests.post(url, headers=self.headers, json=body, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.execute_native_query:: Error executing SQL: {e}")
            raise

    # Collection endpoints (https://www.metabase.com/docs/latest/api/collection)

    def list_collections(self):
        try:
            url = f"{self.__host}/api/collection/"
            response = requests.get(url, headers=self.headers, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.list_collections:: Error listing collections: {e}")
            raise

    # Search (https://www.metabase.com/docs/latest/api/search)

    def search(self, q):
        """Search across Metabase content. GET /api/search/?q=..."""
        try:
            url = f"{self.__host}/api/search/"
            response = requests.get(url, headers=self.headers, params={"q": q}, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.search:: Error searching: {e}")
            raise
