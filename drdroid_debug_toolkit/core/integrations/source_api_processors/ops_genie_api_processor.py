import opsgenie_sdk
import requests

from core.integrations.processor import Processor
from core.settings import EXTERNAL_CALL_TIMEOUT


class OpsGenieApiProcessor(Processor):
    client = None

    def __init__(self, ops_genie_api_key):
        self.__api_key = ops_genie_api_key
        self.base_url = 'https://api.opsgenie.com/'
        self.base_headers = {
            'Authorization': 'GenieKey ' + self.__api_key,
        }

    def test_connection(self):
        api = self.base_url + 'v2/escalations'
        headers = self.base_headers
        try:
            response = requests.get(api, headers=headers, timeout=EXTERNAL_CALL_TIMEOUT)
            if response.status_code == 200:
                api = self.base_url + 'v2/teams'
                response = requests.get(api, headers=headers, timeout=EXTERNAL_CALL_TIMEOUT)
                if response.status_code == 200:
                    return True
                else:
                    raise Exception(
                        "Ops Genie API request failed: " + str(response.status_code) + " - " + response.text)
            else:
                raise Exception("Ops Genie API request failed: " + str(response.status_code) + " - " + response.text)
        except requests.exceptions.RequestException as e:
            raise Exception("Ops Genie API request failed: " + str(e))

    def fetch_escalation_policies(self):
        api = self.base_url + 'v2/escalations'
        headers = self.base_headers
        try:
            response = requests.get(api, headers=headers, timeout=EXTERNAL_CALL_TIMEOUT)
            if response.status_code == 200:
                return response.json().get('data', [])
            else:
                raise Exception("Ops Genie API request failed: " + str(response.status_code))
        except requests.exceptions.RequestException as e:
            raise Exception("Ops Genie API request failed: " + str(e))

    def fetch_teams(self):
        api = self.base_url + 'v2/teams'
        headers = self.base_headers
        try:
            response = requests.get(api, headers=headers, timeout=EXTERNAL_CALL_TIMEOUT)
            if response.status_code == 200:
                return response.json().get('data', [])
            else:
                raise Exception("Ops Genie API request failed: " + str(response.status_code))
        except requests.exceptions.RequestException as e:
            raise Exception("Ops Genie API request failed: " + str(e))

    def fetch_alerts(self, query=None, limit=100, offset=0):
        try:
            configuration = opsgenie_sdk.Configuration()
            configuration.api_key['Authorization'] = self.__api_key
            client = opsgenie_sdk.AlertApi(opsgenie_sdk.ApiClient(configuration))

            all_data = []
            response = client.list_alerts(query=query, limit=limit, offset=offset)
            data = response.data
            for d in data:
                d_dict = d.to_dict()
                all_data.append(d_dict)
            return all_data
        except Exception as e:
            print(f"Exception occurred while fetching alerts with error: {e}")
        return None
