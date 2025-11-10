import opsgenie_sdk
import requests
import logging

from core.integrations.processor import Processor
from core.settings import EXTERNAL_CALL_TIMEOUT

logger = logging.getLogger(__name__)

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
            response = requests.get(api, headers=headers)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            raise Exception("Ops Genie API request failed: " + str(e))
        except Exception as e:
            raise Exception("Ops Genie API request failed: " + str(e))

    def fetch_escalation_policies(self):
        api = self.base_url + 'v2/escalations'
        headers = self.base_headers
        try:
            response = requests.get(api, headers=headers)
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
            response = requests.get(api, headers=headers)
            if response.status_code == 200:
                return response.json().get('data', [])
            else:
                raise Exception("Ops Genie API request failed: " + str(response.status_code))
        except requests.exceptions.RequestException as e:
            raise Exception("Ops Genie API request failed: " + str(e))

    def fetch_alerts(self, query="", limit=100, offset=0):
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

    def get_alert(self, opsgenie_alert_id):
        try:
            configuration = opsgenie_sdk.Configuration()
            configuration.api_key['Authorization'] = self.__api_key
            client = opsgenie_sdk.AlertApi(opsgenie_sdk.ApiClient(configuration))
            response = client.get_alert(identifier=opsgenie_alert_id)
            data = response.data
            return data.to_dict()
        except Exception as e:
            print(f"Exception occurred while fetching single alert info with error: {e}")
        return None

    def create_alert(self, message, alias=None, description=None, responders=None, 
                     visible_to=None, actions=None, tags=None, details=None, 
                     entity=None, priority=None, source=None, user=None, note=None):
        """
        Create a new alert in OpsGenie
        
        Args:
            message (str): Alert message (required)
            alias (str): Alert alias
            description (str): Alert description
            responders (list): List of responders (teams/users)
            visible_to (list): List of teams/users who can see the alert
            actions (list): List of custom actions
            tags (list): List of tags
            details (dict): Custom properties
            entity (str): Entity name
            priority (str): Alert priority (P1, P2, P3, P4, P5)
            source (str): Source of the alert
            user (str): User creating the alert
            note (str): Note for the alert
            
        Returns:
            dict: Created alert data or None if failed
        """
        try:
            configuration = opsgenie_sdk.Configuration()
            configuration.api_key['Authorization'] = self.__api_key
            client = opsgenie_sdk.AlertApi(opsgenie_sdk.ApiClient(configuration))
            
            # Build the alert payload
            alert_payload = opsgenie_sdk.CreateAlertPayload(
                message=message,
                alias=alias,
                description=description,
                responders=responders or [],
                visible_to=visible_to or [],
                actions=actions or [],
                tags=tags or [],
                details=details or {},
                entity=entity,
                priority=priority,
                source=source,
                user=user,
                note=note
            )
            
            response = client.create_alert(create_alert_payload=alert_payload)
            return response.to_dict()
        except Exception as e:
            print(f"Exception occurred while creating alert with error: {e}")
        return None

    def update_alert(self, alert_id, message=None, description=None, priority=None):
        """
        Update an existing alert in OpsGenie
        
        Args:
            alert_id (str): Alert identifier (ID or alias)
            message (str): New alert message
            description (str): New alert description
            priority (str): New alert priority (P1, P2, P3, P4, P5)
            user (str): User updating the alert
            note (str): Note for the update
            source (str): Source of the update
            
        Returns:
            dict: Updated alert data or None if failed
        """
        try:
            configuration = opsgenie_sdk.Configuration()
            configuration.api_key['Authorization'] = self.__api_key
            client = opsgenie_sdk.AlertApi(opsgenie_sdk.ApiClient(configuration))
            
            # Update message if provided
            if message:
                message_payload = opsgenie_sdk.UpdateAlertMessagePayload(
                    message=message,
                )
                response = client.update_alert_message(
                    identifier=alert_id, 
                    update_alert_message_payload=message_payload
                )
            
            # Update description if provided
            if description:
                description_payload = opsgenie_sdk.UpdateAlertDescriptionPayload(
                    description=description,
                )
                response = client.update_alert_description(
                    identifier=alert_id,
                    update_alert_description_payload=description_payload
                )
            
            # Update priority if provided
            if priority:
                priority_payload = opsgenie_sdk.UpdateAlertPriorityPayload(
                    priority=priority,
                )
                response = client.update_alert_priority(
                    identifier=alert_id,
                    update_alert_priority_payload=priority_payload
                )
            return response.to_dict()
        except Exception as e:
            print(f"Exception occurred while updating alert with error: {e}")
        return None

    def acknowledge_alert(self, alert_id, user=None, note=None, source=None):
        """
        Acknowledge an alert in OpsGenie
        
        Args:
            alert_id (str): Alert identifier (ID or alias)
            user (str): User acknowledging the alert
            note (str): Note for the acknowledgment
            source (str): Source of the acknowledgment
            
        Returns:
            dict: Acknowledgment response or None if failed
        """
        try:
            configuration = opsgenie_sdk.Configuration()
            configuration.api_key['Authorization'] = self.__api_key
            client = opsgenie_sdk.AlertApi(opsgenie_sdk.ApiClient(configuration))
            
            acknowledge_payload = opsgenie_sdk.AcknowledgeAlertPayload(
                note=note,
                source=source
            )
            
            response = client.acknowledge_alert(
                identifier=alert_id,
                acknowledge_alert_payload=acknowledge_payload
            )
            return response.to_dict()
        except Exception as e:
            print(f"Exception occurred while acknowledging alert with error: {e}")
        return None

    def close_alert(self, alert_id, user=None, note=None, source=None):
        """
        Close an alert in OpsGenie
        
        Args:
            alert_id (str): Alert identifier (ID or alias)
            user (str): User closing the alert
            note (str): Note for closing the alert
            source (str): Source of the close action
            
        Returns:
            dict: Close response or None if failed
        """
        try:
            configuration = opsgenie_sdk.Configuration()
            configuration.api_key['Authorization'] = self.__api_key
            client = opsgenie_sdk.AlertApi(opsgenie_sdk.ApiClient(configuration))
            
            close_payload = opsgenie_sdk.CloseAlertPayload(
                note=note,
                source=source
            )
            
            response = client.close_alert(
                identifier=alert_id,
                close_alert_payload=close_payload
            )
            return response.to_dict()
        except Exception as e:
            print(f"Exception occurred while closing alert with error: {e}")
        return None

    def create_incident(self, message, description=None, responders=None, priority=None, 
                       tags=None, details=None, note=None, status_page_entry=None, 
                       notify_stakeholders=None):
        """
        Create a new incident in OpsGenie
        
        Args:
            message (str): Incident message (required)
            description (str): Incident description
            responders (list): Teams/users that the incident is routed to via notifications
            priority (str): Incident priority (P1, P2, P3, P4, P5)
            tags (list): List of tags
            details (dict): Custom properties
            note (str): Note for the incident
            status_page_entry (dict): Status page entry fields
            notify_stakeholders (bool): Whether stakeholders are notified
            
        Returns:
            dict: Created incident data or None if failed
        """
        try:
            configuration = opsgenie_sdk.Configuration()
            configuration.api_key['Authorization'] = self.__api_key
            client = opsgenie_sdk.IncidentApi(opsgenie_sdk.ApiClient(configuration))
            
            # Build the incident payload with correct parameters
            incident_payload = opsgenie_sdk.CreateIncidentPayload(
                message=message,
                description=description,
                responders=responders or [],
                priority=priority,
                tags=tags or [],
                details=details or {},
                note=note,
                status_page_entry=status_page_entry,
                notify_stakeholders=notify_stakeholders
            )
            
            response = client.create_incident(create_incident_payload=incident_payload)
            return response.to_dict()
        except Exception as e:
            logger.error(f"Exception occurred while creating incident with error: {e}")
            raise Exception(f"Failed to create incident: {str(e)}")

    def get_incident(self, incident_id):
        """
        Get a specific incident from OpsGenie
        
        Args:
            incident_id (str): Incident identifier
            
        Returns:
            dict: Incident data or None if failed
        """
        try:
            configuration = opsgenie_sdk.Configuration()
            configuration.api_key['Authorization'] = self.__api_key
            client = opsgenie_sdk.IncidentApi(opsgenie_sdk.ApiClient(configuration))
            
            response = client.get_incident(identifier=incident_id)
            return response.to_dict()
        except Exception as e:
            logger.error(f"Exception occurred while fetching incident {incident_id} with error: {e}")
            raise Exception(f"Failed to fetch incident {incident_id}: {str(e)}")

    def list_incidents(self, query="", limit=100, offset=0):
        """
        List incidents from OpsGenie
        
        Args:
            query (str): Query to filter incidents
            limit (int): Maximum number of incidents to return
            offset (int): Number of incidents to skip
            
        Returns:
            list: List of incident data or None if failed
        """
        try:
            configuration = opsgenie_sdk.Configuration()
            configuration.api_key['Authorization'] = self.__api_key
            client = opsgenie_sdk.IncidentApi(opsgenie_sdk.ApiClient(configuration))
            
            response = client.list_incidents(query=query, limit=limit, offset=offset)
            incidents_data = []
            if hasattr(response, 'data') and response.data:
                for incident in response.data:
                    incidents_data.append(incident.to_dict())
            return incidents_data
        except Exception as e:
            logger.error(f"Exception occurred while listing incidents with error: {e}")
            raise Exception(f"Failed to list incidents: {str(e)}")

    def close_incident(self, incident_id, note=None):
        """
        Close an incident in OpsGenie
        
        Args:
            incident_id (str): Incident identifier
            note (str): Note for closing the incident
            
        Returns:
            dict: Close response or None if failed
        """
        try:
            configuration = opsgenie_sdk.Configuration()
            configuration.api_key['Authorization'] = self.__api_key
            client = opsgenie_sdk.IncidentApi(opsgenie_sdk.ApiClient(configuration))
            
            close_payload = opsgenie_sdk.CloseIncidentPayload(
                note=note
            )
            
            response = client.close_incident(
                identifier=incident_id,
                close_incident_payload=close_payload
            )
            return response.to_dict()
        except Exception as e:
            logger.error(f"Exception occurred while closing incident {incident_id} with error: {e}")
            raise Exception(f"Failed to close incident {incident_id}: {str(e)}")

    def get_request_status(self, request_id):
        """
        Get the status of an incident request
        
        Args:
            request_id (str): Request identifier
            
        Returns:
            dict: Request status data or None if failed
        """
        try:
            api_url = f"{self.base_url}v2/alerts/requests/{request_id}"
            headers = self.base_headers
            
            response = requests.get(api_url, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Exception occurred while getting request status {request_id} with error: {e}")
            raise Exception(f"Failed to get request status {request_id}: {str(e)}")
        except Exception as e:
            logger.error(f"Exception occurred while getting request status {request_id} with error: {e}")
            raise Exception(f"Failed to get request status {request_id}: {str(e)}")

    def update_incident(self, incident_id, message=None, description=None, priority=None):
        """
        Update an incident in OpsGenie using direct API calls
        
        Args:
            incident_id (str): Incident identifier
            message (str): New incident message
            description (str): New incident description
            priority (str): New incident priority (P1, P2, P3, P4, P5)
            
        Returns:
            dict: Update response data
        """
        try:
            results = {}
            
            # Update message if provided
            if message is not None:
                api_url = f"{self.base_url}v1/incidents/{incident_id}/message"
                headers = {
                    **self.base_headers,
                    'Content-Type': 'application/json'
                }
                payload = {"message": message}
                
                response = requests.post(api_url, headers=headers, json=payload)
                response.raise_for_status()
                results['message_update'] = response.json()
            
            # Update description if provided
            if description is not None:
                api_url = f"{self.base_url}v1/incidents/{incident_id}/description"
                headers = {
                    **self.base_headers,
                    'Content-Type': 'application/json'
                }
                payload = {"description": description}
                
                response = requests.put(api_url, headers=headers, json=payload)
                response.raise_for_status()
                results['description_update'] = response.json()
            
            # Update priority if provided
            if priority is not None:
                api_url = f"{self.base_url}v1/incidents/{incident_id}/priority"
                headers = {
                    **self.base_headers,
                    'Content-Type': 'application/json'
                }
                payload = {"priority": priority}
                
                response = requests.put(api_url, headers=headers, json=payload)
                response.raise_for_status()
                results['priority_update'] = response.json()
            
            return results
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Exception occurred while updating incident {incident_id} with error: {e}")
            raise Exception(f"Failed to update incident {incident_id}: {str(e)}")
        except Exception as e:
            logger.error(f"Exception occurred while updating incident {incident_id} with error: {e}")
            raise Exception(f"Failed to update incident {incident_id}: {str(e)}")

    def resolve_incident(self, incident_id, note=None):
        """
        Resolve an incident in OpsGenie
        
        Args:
            incident_id (str): Incident identifier
            note (str): Note for resolving the incident
            
        Returns:
            dict: Resolve response data
        """
        try:
            api_url = f"{self.base_url}v1/incidents/{incident_id}/resolve?identifierType=id"
            headers = {
                **self.base_headers,
                'Content-Type': 'application/json'
            }
            payload = {}
            if note:
                payload["note"] = note
            
            response = requests.post(api_url, headers=headers, json=payload)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Exception occurred while resolving incident {incident_id} with error: {e}")
            raise Exception(f"Failed to resolve incident {incident_id}: {str(e)}")
        except Exception as e:
            logger.error(f"Exception occurred while resolving incident {incident_id} with error: {e}")
            raise Exception(f"Failed to resolve incident {incident_id}: {str(e)}")

    def reopen_incident(self, incident_id, note=None):
        """
        Reopen an incident in OpsGenie
        
        Args:
            incident_id (str): Incident identifier
            note (str): Note for reopening the incident
            
        Returns:
            dict: Reopen response data
        """
        try:
            api_url = f"{self.base_url}v1/incidents/{incident_id}/reopen?identifierType=id"
            headers = {
                **self.base_headers,
                'Content-Type': 'application/json'
            }
            payload = {}
            if note:
                payload["note"] = note
            
            response = requests.post(api_url, headers=headers, json=payload)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Exception occurred while reopening incident {incident_id} with error: {e}")
            raise Exception(f"Failed to reopen incident {incident_id}: {str(e)}")
        except Exception as e:
            logger.error(f"Exception occurred while reopening incident {incident_id} with error: {e}")
            raise Exception(f"Failed to reopen incident {incident_id}: {str(e)}")

    def add_incident_note(self, incident_id, note):
        """
        Add a note to an incident in OpsGenie
        
        Args:
            incident_id (str): Incident identifier
            note (str): Note to add to the incident
            
        Returns:
            dict: Add note response data
        """
        try:
            api_url = f"{self.base_url}v1/incidents/{incident_id}/notes?identifierType=id"
            headers = {
                **self.base_headers,
                'Content-Type': 'application/json'
            }
            payload = {"note": note}
            
            response = requests.post(api_url, headers=headers, json=payload)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Exception occurred while adding note to incident {incident_id} with error: {e}")
            raise Exception(f"Failed to add note to incident {incident_id}: {str(e)}")
        except Exception as e:
            logger.error(f"Exception occurred while adding note to incident {incident_id} with error: {e}")
            raise Exception(f"Failed to add note to incident {incident_id}: {str(e)}")

    def list_incident_notes(self, incident_id, offset=None, direction=None, limit=10, order=None):
        """
        List notes for an incident in OpsGenie
        
        Args:
            incident_id (str): Incident identifier
            offset (str): Optional offset for pagination
            direction (str): Direction for pagination (prev, next)
            limit (int): Maximum number of notes to return
            order (str): Order of notes (asc, desc)
            
        Returns:
            list: List of note data or None if failed
        """
        try:
            api_url = f"{self.base_url}v1/incidents/{incident_id}/notes?identifierType=id"
            headers = self.base_headers
            
            # Build query parameters
            params = {}
            if offset:
                params['offset'] = offset
            if direction:
                params['direction'] = direction
            if limit:
                params['limit'] = limit
            if order:
                params['order'] = order
            
            response = requests.get(api_url, headers=headers, params=params)
            response.raise_for_status()
            
            response_data = response.json()
            # Extract notes from the response
            notes_data = response_data.get('data', [])
            return notes_data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Exception occurred while listing notes for incident {incident_id} with error: {e}")
            raise Exception(f"Failed to list notes for incident {incident_id}: {str(e)}")
        except Exception as e:
            logger.error(f"Exception occurred while listing notes for incident {incident_id} with error: {e}")
            raise Exception(f"Failed to list notes for incident {incident_id}: {str(e)}")
