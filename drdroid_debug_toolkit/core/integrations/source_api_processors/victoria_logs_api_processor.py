import json
from typing import Any, Dict, Optional, List

import requests


class VictoriaLogsApiProcessor:

    def __init__(self, **kwargs):
        # VictoriaLogs-specific connector keys
        self._protocol = kwargs.get('VICTORIA_LOGS_PROTOCOL', 'http') or 'http'
        self._host = kwargs.get('VICTORIA_LOGS_HOST')
        self._port = kwargs.get('VICTORIA_LOGS_PORT', 9428)
        self._ssl_verify = kwargs.get('SSL_VERIFY', True)

        # Optional custom headers (e.g., Authorization, tenant headers)
        # Expect JSON string in MCP_SERVER_AUTH_HEADERS
        raw_headers = kwargs.get('MCP_SERVER_AUTH_HEADERS')
        self._headers: Dict[str, str] = {}
        if isinstance(raw_headers, str):
            try:
                self._headers = json.loads(raw_headers)
            except Exception:
                # Fallback: treat as bearer token string
                self._headers = {'Authorization': f'Bearer {raw_headers}'}
        elif isinstance(raw_headers, dict):
            self._headers = raw_headers

        # Normalize boolean flag if it comes as string
        if isinstance(self._ssl_verify, str):
            self._ssl_verify = self._ssl_verify.lower() in ['true', '1', 'yes']

        if not self._host:
            raise ValueError('VictoriaLogsApiProcessor requires host (VICTORIA_LOGS_HOST).')

        self._base_url = f"{self._protocol}://{self._host}:{self._port}"

    def _http_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self._base_url}{path}"
        resp = requests.get(url, params=params or {}, headers=self._headers, timeout=60, verify=self._ssl_verify)
        resp.raise_for_status()
        if not resp.text:
            return {}
        try:
            return resp.json()
        except Exception:
            # Some endpoints return plain text; wrap it
            return {'text': resp.text}

    def _http_post_form(self, path: str, data: Dict[str, Any]) -> requests.Response:
        """
        Send application/x-www-form-urlencoded POST and return raw response.
        """
        url = f"{self._base_url}{path}"
        headers = {**self._headers}  # requests sets proper form content-type for dict data
        resp = requests.post(url, data=data, headers=headers, timeout=60, verify=self._ssl_verify)
        resp.raise_for_status()
        return resp

    def query_logsql(self, query: str, *, limit: Optional[int] = None, start: Optional[str] = None,
                     end: Optional[str] = None, timeout: Optional[str] = None) -> Dict[str, Any]:
        """
        Execute a LogsQL query via /select/logsql/query, returning a normalized dict:
        {"data": [ {field: value, ...}, ... ]}

        The endpoint streams JSON Lines; we parse all lines into a list.
        """
        payload: Dict[str, Any] = {"query": query}
        if limit is not None:
            payload["limit"] = int(limit)
        if start is not None:
            payload["start"] = start
        if end is not None:
            payload["end"] = end
        if timeout is not None:
            payload["timeout"] = timeout

        resp = self._http_post_form('/select/logsql/query', payload)
        lines = resp.text.splitlines()
        items: List[Dict[str, Any]] = []
        for line in lines:
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    items.append(obj)
                else:
                    items.append({"value": obj})
            except Exception:
                # Non-JSON line; include as text for visibility
                items.append({"text": line})
        return {"data": items}

    def fetch_field_values(self, field_name: str, time_filter: Optional[str] = None, limit: int = 100) -> Dict[str, Any]:
        """
        Fetch distinct values for a field using LogsQL `field_values` pipe.

        Example query: `_time:1h | field_values level | limit 100`
        """
        time_clause = time_filter or '_time:1h'
        q = f"{time_clause} | field_values {field_name} | limit {int(limit)}"
        return self.query_logsql(q)

    def fetch_field_names(self, time_filter: Optional[str] = None, limit: int = 1000) -> Dict[str, Any]:
        """
        Fetch field names using LogsQL `field_names` pipe.
        """
        time_clause = time_filter or '_time:1h'
        # Add limit to be safe on large datasets
        q = f"{time_clause} | field_names | limit {int(limit)}"
        return self.query_logsql(q)

    def test_connection(self) -> None:
        """
        Basic connectivity check used by source manager. Raises on failure.

        Performs a lightweight LogsQL request to validate reachability and auth.
        """
        # Use a tiny time window and hard limit to keep it cheap.
        # Prefer passing limit as a dedicated arg per VictoriaLogs semantics
        self.query_logsql('* | limit 1', limit=1)

