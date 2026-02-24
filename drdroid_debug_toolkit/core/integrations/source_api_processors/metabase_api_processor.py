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

    @staticmethod
    def _response_error_message(response):
        """
        Extract a readable error string from a non-2xx response so we can pass
        the API error back to the caller instead of a generic status line.
        """
        try:
            text = (response.text or "").strip()
            if response.headers.get("content-type", "").startswith("application/json") and text:
                data = response.json()
                if isinstance(data, dict):
                    msg = data.get("message") or data.get("msg") or data.get("error")
                    if msg:
                        return str(msg)
                return text
            return text or response.reason or f"HTTP {response.status_code}"
        except Exception:
            return response.text or response.reason or f"HTTP {response.status_code}"

    def _raise_for_status_with_body(self, response, context="Metabase API"):
        """If response is not ok, log and raise HTTPError with API body in the message."""
        if response.ok:
            return
        msg = self._response_error_message(response)
        full = f"{context} ({response.status_code}): {msg}"
        logger.error("%s", full)
        raise requests.exceptions.HTTPError(full, response=response)

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

    def add_card_to_dashboard(self, dashboard_id, dashcard_payload):
        """
        Add a card to a dashboard via POST /api/dashboard/{id}/cards.

        Expects dashcard_payload in snake_case from callers:
        card_id, row, col, size_x, size_y, parameter_mappings, series, visualization_settings.
        """
        if not isinstance(dashcard_payload, dict):
            dashcard_payload = {}
        card_id = dashcard_payload.get("card_id")
        if card_id is None:
            raise ValueError("card_id is required to add a card to a dashboard")
        row = dashcard_payload.get("row", 0)
        col = dashcard_payload.get("col", 0)
        size_x = dashcard_payload.get("size_x", 4)
        size_y = dashcard_payload.get("size_y", 4)
        series = dashcard_payload.get("series", [])
        parameter_mappings = dashcard_payload.get("parameter_mappings", [])
        visualization_settings = dashcard_payload.get("visualization_settings", {})

        body = {
            "cardId": card_id,
            "dashboardId": dashboard_id,
            "row": row,
            "col": col,
            "sizeX": size_x,
            "sizeY": size_y,
            "series": series,
            "parameter_mappings": parameter_mappings,
            "visualization_settings": visualization_settings,
        }
        try:
            url = f"{self.__host}/api/dashboard/{dashboard_id}/cards"
            response = requests.post(url, headers=self.headers, json=body, timeout=EXTERNAL_CALL_TIMEOUT)
            self._raise_for_status_with_body(
                response,
                context=f"MetabaseApiProcessor.add_card_to_dashboard (dashboard {dashboard_id}, card {card_id})",
            )
            return response.json()
        except requests.exceptions.HTTPError:
            raise
        except Exception as e:
            logger.error(
                "MetabaseApiProcessor.add_card_to_dashboard:: Error adding card %s to dashboard %s: %s",
                card_id,
                dashboard_id,
                e,
            )
            raise

    def update_dashboard(self, dashboard_id, payload):
        try:
            url = f"{self.__host}/api/dashboard/{dashboard_id}"
            response = requests.put(url, headers=self.headers, json=payload, timeout=EXTERNAL_CALL_TIMEOUT)
            self._raise_for_status_with_body(
                response,
                context=f"MetabaseApiProcessor.update_dashboard (dashboard {dashboard_id})",
            )
            return response.json()
        except requests.exceptions.HTTPError:
            raise
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.update_dashboard:: Error updating dashboard {dashboard_id}: {e}")
            raise

    def get_dashboard_cards(self, dashboard_id):
        """
        Get all dashcards (cards on the dashboard). Uses GET /api/dashboard/{id}
        and returns the dashcards array so it matches what the Metabase UI shows.
        In Metabase, a 'question' is a card (type=question); dashcards reference
        card_id, so questions on a dashboard are returned here.
        """
        try:
            url = f"{self.__host}/api/dashboard/{dashboard_id}"
            response = requests.get(url, headers=self.headers, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            if isinstance(data, list):
                return data
            for key in ("dashcards", "ordered_cards", "cards", "data"):
                if key in data and isinstance(data[key], list):
                    return data[key]
            return []
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.get_dashboard_cards:: Error getting dashboard cards {dashboard_id}: {e}")
            raise

    def update_dashboard_cards(self, dashboard_id, cards_payload):
        """Replace/update cards on a dashboard. PUT /api/dashboard/{id}/cards. Body is array of dashcards."""
        try:
            url = f"{self.__host}/api/dashboard/{dashboard_id}/cards"
            response = requests.put(url, headers=self.headers, json=cards_payload, timeout=EXTERNAL_CALL_TIMEOUT)
            self._raise_for_status_with_body(
                response,
                context=f"MetabaseApiProcessor.update_dashboard_cards (dashboard {dashboard_id})",
            )
            return response.json()
        except requests.exceptions.HTTPError:
            raise
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

    def _normalize_native_dataset_query(self, dataset_query):
        """
        Return dataset_query in the format Metabase API accepts for native questions:
        type, database, native { query, template-tags }.
        """
        if not isinstance(dataset_query, dict) or dataset_query.get("type") != "native":
            return dataset_query
        native = dataset_query.get("native")
        if not isinstance(native, dict) or "query" not in native:
            return dataset_query
        out_native = dict(native)
        if "template-tags" not in out_native:
            out_native["template-tags"] = {}
        db = dataset_query.get("database")
        if db is not None and not isinstance(db, int):
            try:
                db = int(db)
            except (TypeError, ValueError):
                pass
        return {
            "type": "native",
            "database": db,
            "native": out_native,
        }

    def create_card(self, payload):
        """
        Create a card. POST /api/card/. Normalizes dataset_query and sets safe defaults
        for required fields (name, display, visualization_settings) so Metabase doesn't 500.
        """
        if not isinstance(payload, dict):
            payload = {}
        body = dict(payload)
        if "dataset_query" in body and body["dataset_query"] is not None:
            body["dataset_query"] = self._normalize_native_dataset_query(body["dataset_query"])
        if body.get("name") is None or body.get("name") == "":
            body["name"] = "New question"
        if body.get("display") is None or body.get("display") == "":
            body["display"] = "table"
        if "visualization_settings" not in body or body["visualization_settings"] is None:
            body["visualization_settings"] = {}
        if "collection_id" not in body:
            body["collection_id"] = None
        url = f"{self.__host}/api/card/"
        response = requests.post(url, headers=self.headers, json=body, timeout=EXTERNAL_CALL_TIMEOUT)
        self._raise_for_status_with_body(response, context="MetabaseApiProcessor.create_card")
        return response.json()

    def get_card(self, card_id):
        try:
            url = f"{self.__host}/api/card/{card_id}"
            response = requests.get(url, headers=self.headers, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.get_card:: Error getting card {card_id}: {e}")
            raise

    def update_card(self, card_id, payload):
        """
        Update a card. PUT /api/card/{id}. Normalizes dataset_query so Metabase persists it.
        """
        if not isinstance(payload, dict):
            payload = {}
        body = dict(payload)
        if "dataset_query" in body and body["dataset_query"] is not None:
            body["dataset_query"] = self._normalize_native_dataset_query(body["dataset_query"])
        url = f"{self.__host}/api/card/{card_id}"
        response = requests.put(url, headers=self.headers, json=body, timeout=EXTERNAL_CALL_TIMEOUT)
        self._raise_for_status_with_body(
            response,
            context=f"MetabaseApiProcessor.update_card (card {card_id})",
        )
        return response.json()

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
