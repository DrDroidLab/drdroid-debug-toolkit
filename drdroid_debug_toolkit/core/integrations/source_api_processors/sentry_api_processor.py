import logging
import re
import time

import requests

from core.integrations.processor import Processor
from core.settings import EXTERNAL_CALL_TIMEOUT

logger = logging.getLogger(__name__)


class SentryApiProcessor(Processor):
    client = None

    def __init__(self, api_key, org_slug):
        self.__api_key = api_key
        self.org_slug = org_slug

    def test_connection(self, timeout=None):
        try:
            url = f'https://sentry.io/api/0/organizations/{self.org_slug}/projects/'
            headers = {
                'Authorization': f'Bearer {self.__api_key}'
            }
            response = requests.request("GET", url, headers=headers)
            response.raise_for_status()
        except Exception as e:
            logger.error(f"Exception occurred while testing postgres connection with error: {e}")
            raise e

    def fetch_issue_details(self, issue_id):
        try:
            url = f"https://sentry.io/api/0/issues/{issue_id}/"
            payload = {}
            headers = {
                'Authorization': f'Bearer {self.__api_key}'
            }
            response = requests.request("GET", url, headers=headers, data=payload)
            if response:
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429 or response.status_code == 403:
                    reset_in_epoch_seconds = int(response.headers.get('X-Sentry-Rate-Limit-Reset', None))
                    if reset_in_epoch_seconds:
                        time.sleep(reset_in_epoch_seconds - int(time.time()))
                        return self.fetch_issue_details(issue_id)
                    else:
                        time.sleep(100)
                        return self.fetch_issue_details(issue_id)
                else:
                    logger.error(f"Error occurred while fetching sentry issue details for issue_id: {issue_id} "
                                 f"with status_code: {response.status_code} and response: {response.text}")
        except Exception as e:
            logger.error(f"Exception occurred while fetching sentry issue details "
                         f"for issue_id: {issue_id} with error: {e}")
        return None
    
    def fetch_issue_last_event(self, issue_id):
        try:
            url = f'https://sentry.io/api/0/organizations/{self.org_slug}/issues/{issue_id}/hashes/'
            payload = {}
            headers = {
                'Authorization': f'Bearer {self.__api_key}'
            }
            response = requests.get(url, headers=headers, data=payload)
            if response:
                if response.status_code == 200:
                    events = response.json()
                    if events:
                        latest_event = events[0]
                        return latest_event
                    else:
                        print("No events found for the specified issue.")
                elif response.status_code == 429 or response.status_code == 403:
                    reset_in_epoch_seconds = int(response.headers.get('X-Sentry-Rate-Limit-Reset', None))
                    if reset_in_epoch_seconds:
                        time.sleep(reset_in_epoch_seconds - int(time.time()))
                        return self.fetch_issue_details(issue_id)
                    else:
                        time.sleep(100)
                        return self.fetch_issue_details(issue_id)
                else:
                    logger.error(f"Error occurred while fetching sentry issue details for issue_id: {issue_id} "
                                 f"with status_code: {response.status_code} and response: {response.text}")
        except Exception as e:
            logger.error(f"Exception occurred while fetching sentry issue details "
                         f"for issue_id: {issue_id} with error: {e}")
            return None
        return None

    def fetch_issues_with_query(self, project_slug, query, start_time=None, end_time=None, stats_period=None):
        """
        Fetch Sentry issues with query, handling pagination automatically.
        
        Args:
            project_slug: Project slug
            query: Optional query string
            start_time: Optional start time (ISO format)
            end_time: Optional end time (ISO format)
            stats_period: Optional stats period (e.g., "24h", "14d")
            
        Returns:
            List of all issues matching the query (all pages fetched automatically)
        """
        try:
            if query:
                url = f'https://sentry.io/api/0/projects/{self.org_slug}/{project_slug}/issues/?query={query}'
            else:
                url = f'https://sentry.io/api/0/projects/{self.org_slug}/{project_slug}/issues/'

            headers = {
                'Authorization': f'Bearer {self.__api_key}'
            }
            
            params = {}
            if start_time:
                params['start'] = start_time
            if end_time:
                params['end'] = end_time
            if stats_period:
                params['statsPeriod'] = stats_period
        
            all_issues = []
            cursor = None
            max_pages = 50  # Limit to prevent excessive API calls
            page_count = 0
            
            while page_count < max_pages:
                page_params = params.copy()
                if cursor:
                    page_params['cursor'] = cursor
                
                response = requests.get(url, headers=headers, params=page_params, timeout=EXTERNAL_CALL_TIMEOUT)
                
                if not response:
                    break
                    
                if response.status_code == 200:
                    page_data = response.json()
                    
                    # Sentry API returns a list of issues
                    if isinstance(page_data, list):
                        if len(page_data) == 0:
                            # No more results
                            break
                            
                        all_issues.extend(page_data)
                        page_count += 1
                        
                        # Check for pagination cursor in Link header
                        link_header = response.headers.get('Link', '')
                        if 'rel="next"' in link_header:
                            # Extract cursor from Link header
                            # Format: <url>; rel="next"; cursor="..." or cursor in query params
                            # Try to extract cursor from Link header
                            cursor_match = re.search(r'cursor=([^&;"]+)', link_header)
                            if cursor_match:
                                cursor = cursor_match.group(1)
                            else:
                                # Check if cursor is in quotes
                                cursor_match = re.search(r'cursor="([^"]+)"', link_header)
                                if cursor_match:
                                    cursor = cursor_match.group(1)
                                else:
                                    # No cursor found, assume no more pages
                                    break
                        else:
                            # No "next" link means we're done
                            break
                    else:
                        # If response is not a list, return as-is (might be error or different format)
                        if page_count == 0:
                            return page_data
                        else:
                            # We already got some results, return what we have
                            break
                        
                elif response.status_code in (429, 403):
                    reset_in_epoch_seconds = int(response.headers.get('X-Sentry-Rate-Limit-Reset', 0))
                    if reset_in_epoch_seconds:
                        sleep_time = max(0, reset_in_epoch_seconds - int(time.time()))
                        time.sleep(sleep_time)
                        continue  # Retry this page
                    else:
                        time.sleep(100)
                        continue  # Retry this page
                else:
                    logger.error(f"Error occurred while fetching issues with query status_code: {response.status_code} "
                                 f"and response: {response.text}")
                    # If we have some results, return them; otherwise return None
                    break
                
                # If no cursor for next page, we're done
                if not cursor:
                    break
            
            return all_issues if all_issues else None
            
        except Exception as e:
            logger.error(f"Exception occurred while fetching issues with query: {e}")
            raise e
    
    def fetch_events_inside_issue(self, issue_id, project_slug, start_time=None, end_time=None):
        try:
            url = f'https://sentry.io/api/0/organizations/{self.org_slug}/issues/{issue_id}/events/'
            headers = {
                'Authorization': f'Bearer {self.__api_key}'
            }
            params = {}
            if start_time:
                params['start'] = start_time
            if end_time:
                params['end'] = end_time

            response = requests.get(url, headers=headers, params=params)

            if response:
                if response.status_code == 200:
                    return response.json()
                elif response.status_code in (429, 403):
                    reset_in_epoch_seconds = int(response.headers.get('X-Sentry-Rate-Limit-Reset', None))
                    if reset_in_epoch_seconds:
                        time.sleep(reset_in_epoch_seconds - int(time.time()))
                        return self.fetch_events_inside_issue(issue_id, project_slug, start_time, end_time)
                    else:
                        time.sleep(100)
                        return self.fetch_events_inside_issue(issue_id, project_slug, start_time, end_time)
                else:
                    logger.error(f"Error occurred while fetching recent errors status_code: {response.status_code} "
                                 f"and response: {response.text}")
        except Exception as e:
            logger.error(f"Exception occurred while fetching recent errors with error: {e}")
            raise e
    
    def fetch_event_details(self, event_id, project_slug=None):
        """
        Fetch detailed information about a specific Sentry event.
        """
        try:
            if project_slug:
                url = f'https://sentry.io/api/0/projects/{self.org_slug}/{project_slug}/events/{event_id}/'
            else:
                url = f'https://sentry.io/api/0/events/{self.org_slug}/{event_id}/'

            headers = {
                'Authorization': f'Bearer {self.__api_key}'
            }

            response = requests.get(url, headers=headers)

            if response:
                if response.status_code == 200:
                    return response.json()
                elif response.status_code in (429, 403):
                    reset_in_epoch_seconds = int(response.headers.get('X-Sentry-Rate-Limit-Reset', None))
                    if reset_in_epoch_seconds:
                        time.sleep(reset_in_epoch_seconds - int(time.time()))
                        return self.fetch_event_details(event_id, project_slug)
                    else:
                        time.sleep(100)
                        return self.fetch_event_details(event_id, project_slug)
                else:
                    logger.error(f"Error occurred while fetching event details for event_id: {event_id} "
                                 f"with status_code: {response.status_code} and response: {response.text}")
        except Exception as e:
            logger.error(f"Exception occurred while fetching event details "
                         f"for event_id: {event_id} with error: {e}")
            raise e

        return None

    def fetch_projects(self):
        """
        Fetch all projects in the organization for metadata extraction.
        Returns the raw API response that will be processed by the metadata extractor.
        """
        try:
            url = f'https://sentry.io/api/0/organizations/{self.org_slug}/projects/'
            headers = {
                'Authorization': f'Bearer {self.__api_key}'
            }
            response = requests.get(url, headers=headers)

            if response:
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429 or response.status_code == 403:
                    reset_in_epoch_seconds = int(response.headers.get('X-Sentry-Rate-Limit-Reset', None))
                    if reset_in_epoch_seconds:
                        time.sleep(reset_in_epoch_seconds - int(time.time()))
                        return self.fetch_projects()
                    else:
                        time.sleep(100)
                        return self.fetch_projects()
                else:
                    logger.error(
                        f"Error occurred while fetching sentry projects with status_code: {response.status_code} "
                        f"and response: {response.text}")

        except Exception as e:
            logger.error(f"Exception occurred while fetching sentry projects with error: {e}")
            raise e

        return None
