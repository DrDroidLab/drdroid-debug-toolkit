import logging
import requests
from requests.auth import HTTPBasicAuth

from core.integrations.processor import Processor

logger = logging.getLogger(__name__)


class OpenSearchApiProcessor(Processor):
    def __init__(self, protocol: str, host: str, username: str, password: str, verify_certs: bool = False,
                 port: str = None):
        self.base_url = f"{protocol}://{host}"
        if port:
            self.base_url = f"{self.base_url}:{port}"
        self.auth = HTTPBasicAuth(username, password)
        self.verify_certs = verify_certs

    def _make_request(self, method, endpoint, data=None, params=None):
        url = f"{self.base_url}/{endpoint}"
        try:
            response = requests.request(method, url, auth=self.auth, verify=self.verify_certs, json=data, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"OpenSearchApiProcessor._make_request:: Error making request to OpenSearch: {e}")
            raise

    def test_connection(self):
        try:
            indices = self._make_request("GET", "_cat/indices?format=json")
            if len(indices) > 0:
                return True
            else:
                logger.error(f"OpenSearchApiProcessor.test_connection:: Connection Error. No indices found for host: "
                             f"{self.base_url}")
                raise Exception(f"OpenSearchApiProcessor.test_connection:: Connection Error. No indices found for host:"
                                f" {self.base_url}")
        except Exception as e:
            logger.error(f"OpenSearchApiProcessor.test_connection:: Connection Error for host: {self.base_url} "
                         f"error: {e}")
            raise e

    def query(self, index, query):
        try:
            result = self._make_request("POST", f"{index}/_search", data=query)
            return result
        except Exception as e:
            logger.error(f"OpenSearchApiProcessor.fetch_indices:: Exception occurred while executing query: {query} on "
                         f"index: {index} for host: {self.base_url} with error: {e}")
            raise e

    def get_document(self, index, doc_id):
        try:
            result = self._make_request("GET", f"{index}/_doc/{doc_id}", params={"preference": "_primary_first"})
            return result
        except Exception as e:
            logger.error(f"OpenSearchApiProcessor.get_document:: Exception occurred while fetching document from "
                         f"index: {index}, doc id: {doc_id} for host: {self.base_url} with error: {e}")
            raise e

    # management APIs
    def get_node_stats(self):
        try:
            result = self._make_request("GET", "_nodes/stats")
            return result
        except Exception as e:
            logger.error(f"OpenSearchApiProcessor.get_node_stats:: Exception occurred while fetching node stats for "
                         f"host: {self.base_url} with error: {e}")
            raise e

    def get_index_stats(self):
        try:
            result = self._make_request("GET", "_stats")
            return result
        except Exception as e:
            logger.error(f"OpenSearchApiProcessor.get_index_stats:: Exception occurred while fetching indices stats "
                         f"for host: {self.base_url} with error: {e}")
            raise e

    def fetch_indices(self):
        try:
            indices = self._make_request("GET", "_alias")
            return list(indices.keys())
        except Exception as e:
            logger.error(f"OpenSearchApiProcessor.fetch_indices:: Exception occurred while fetching indices for host: "
                         f"{self.base_url} with error: {e}")
            raise e

    # changes state of the instance, use carefully
    def delete_index(self, index):
        try:
            result = self._make_request("DELETE", f"{index}")
            return result
        except Exception as e:
            logger.error(f"OpenSearchApiProcessor.delete_index:: Exception occurred while deleting index: {index} from "
                         f"host: {self.base_url} with error: {e}")
            raise e

    # cluster health
    def get_cluster_health(self):
        try:
            result = self._make_request("GET", "_cluster/health")
            return result
        except Exception as e:
            logger.error(f"OpenSearchApiProcessor.get_cluster_health:: Exception occurred while fetching cluster health "
                         f"for host: {self.base_url} with error: {e}")
            raise e
        
    # cluster settings
    def get_cluster_settings(self):
        try:
            result = self._make_request("GET", "_cluster/settings")
            return result
        except Exception as e:
            logger.error(f"OpenSearchApiProcessor.get_cluster_settings:: Exception occurred while fetching cluster settings "
                         f"for host: {self.base_url} with error: {e}")
            raise e
        
    # cluster stats
    def get_cluster_stats(self):
        try:
            result = self._make_request("GET", "_cluster/stats")
            return result
        except Exception as e:
            logger.error(f"OpenSearchApiProcessor.get_cluster_stats:: Exception occurred while fetching cluster stats "
                         f"for host: {self.base_url} with error: {e}")
            raise e
        
    # list index and shard recoveries
    def get_index_and_shard_recoveries(self):
        try:
            result = self._make_request("GET", "_recovery")
            return result
        except Exception as e:
            logger.error(f"OpenSearchApiProcessor.get_index_and_shard_recoveries:: Exception occurred while fetching index and shard recoveries "
                         f"for host: {self.base_url} with error: {e}")
            raise e
    
    # list indices
    def get_indices(self):
        try:
            result = self._make_request("GET", "_cat/indices?format=json")
            response = {"api_response": result}
            return response
        except Exception as e:
            logger.error(f"OpenSearchApiProcessor.get_indices:: Exception occurred while fetching indices for host: "
                         f"{self.base_url} with error: {e}")
            raise e
        
    # list pending tasks
    def get_pending_tasks(self):
        try:
            result = self._make_request("GET", "_cluster/pending_tasks")
            return result
        except Exception as e:
            logger.error(f"OpenSearchApiProcessor.get_pending_tasks:: Exception occurred while fetching pending tasks for "
                         f"host: {self.base_url} with error: {e}")
            raise e
        
    # list shards
    def get_shards(self):
        try:
            result = self._make_request("GET", "_cat/shards?format=json")
            response = {"api_response": result}
            return response
        except Exception as e:
            logger.error(f"OpenSearchApiProcessor.get_shards:: Exception occurred while fetching shards for host: "
                         f"{self.base_url} with error: {e}")
            raise e
        
    # list tasks
    def get_tasks(self):
        try:
            result = self._make_request("GET", "_tasks")
            return result
        except Exception as e:
            logger.error(f"OpenSearchApiProcessor.get_tasks:: Exception occurred while fetching tasks for "
                         f"host: {self.base_url} with error: {e}")
            raise e