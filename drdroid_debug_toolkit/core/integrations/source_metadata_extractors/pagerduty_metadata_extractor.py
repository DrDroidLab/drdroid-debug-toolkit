import logging
from core.integrations.source_metadata_extractor import SourceMetadataExtractor
from core.integrations.source_api_processors.pd_api_processor import PdApiProcessor
from core.protos.base_pb2 import Source, SourceModelType
from datetime import datetime, timedelta

from core.utils.logging_utils import log_function_call


logger = logging.getLogger(__name__)


class PagerDutyConnectorMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id: str, connector_name: str, api_token, configured_email):
        self.__client = PdApiProcessor(api_token, configured_email)
        super().__init__(request_id, connector_name, Source.PAGER_DUTY)

    @log_function_call
    def extract_alerts(self):
        model_type = SourceModelType.PAGERDUTY_INCIDENT
        try:
            since_time = (datetime.utcnow() - timedelta(days=7)).isoformat() + 'Z'
            incidents = self.__client.fetch_incidents()
            if not incidents:
                logger.error('No pagerduty incidents found')
                return
            recent_alerts = {}
            for incident in incidents:
                incident_id = incident.get('id', '')
                if not incident_id:
                    continue
                alerts = self.__client.fetch_alerts(incident_id)
                filtered_alerts = [alert for alert in alerts if alert.get('created_at') >= since_time]
                if filtered_alerts:
                    recent_alerts[incident_id] = filtered_alerts
        except Exception as e:
            logger.error(f'Error fetching pagerduty incidents: {e}')
            return
        if not recent_alerts:
            return
        model_data = {}
        for incident_id, alerts in recent_alerts.items():
            try:
                detailed_alerts = []
                for alert in alerts:
                    alert_id = alert.get('id', '')
                    details = alert.get('body', {}).get('details', {})
                    detailed_alert = {
                        'alert': alert,
                        'details': details
                    }
                    detailed_alerts.append(detailed_alert)
                model_data[incident_id] = detailed_alerts
            except Exception as e:
                logger.error(f'Error processing alerts for pagerduty incident {incident_id}: {e}')
                continue
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data
