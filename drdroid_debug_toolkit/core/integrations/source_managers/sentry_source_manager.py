import logging
import random
import urllib.parse

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from functools import partial

from google.protobuf.struct_pb2 import Struct
from google.protobuf.wrappers_pb2 import Int64Value, StringValue, UInt64Value

from core.integrations.source_api_processors.sentry_api_processor import SentryApiProcessor
from core.integrations.source_manager import SourceManager
from core.protos.base_pb2 import TimeRange, Source, SourceKeyType
from core.protos.connectors.connector_pb2 import Connector as ConnectorProto
from core.protos.literal_pb2 import Literal, LiteralType
from core.protos.playbooks.playbook_commons_pb2 import ApiResponseResult, PlaybookTaskResult, PlaybookTaskResultType, TableResult, TextResult
from core.protos.playbooks.source_task_definitions.sentry_task_pb2 import Sentry
from core.protos.ui_definition_pb2 import FormField, FormFieldType
from core.utils.credentilal_utils import generate_credentials_dict, get_connector_key_type_string, DISPLAY_NAME, CATEGORY, APPLICATION_MONITORING
from core.utils.proto_utils import dict_to_proto

logger = logging.getLogger(__name__)


def buildSentryUrl(org_slug: str, task_type: str, params: dict = None) -> str:
    """
    Build Sentry URLs for different task types.
    
    Args:
        org_slug: Sentry organization slug from connector
        task_type: Type of task ("issue", "event", "project", "project_issues")
        params: Dictionary containing task-specific parameters
    
    Returns:
        Complete Sentry URL for the specific task type
    """
    if not org_slug:
        return ""
    
    base_url = "https://sentry.io"
    
    if params is None:
        params = {}
    
    if task_type == "issue":
        if 'issue_id' in params:
            return f"{base_url.replace('sentry.io', f'{org_slug}.sentry.io')}/issues/{params['issue_id']}/"
        else:
            return f"{base_url.replace('sentry.io', f'{org_slug}.sentry.io')}/issues/"
    
    elif task_type == "event":
        if 'event_id' in params and 'issue_id' in params:
            return f"{base_url.replace('sentry.io', f'{org_slug}.sentry.io')}/issues/{params['issue_id']}/events/{params['event_id']}/"
        elif 'event_id' in params:
            # Fallback: if no issue_id, we can't construct the proper event URL
            return f"{base_url.replace('sentry.io', f'{org_slug}.sentry.io')}/issues/"
        else:
            return f"{base_url.replace('sentry.io', f'{org_slug}.sentry.io')}/issues/"
    
    elif task_type == "project":
        if 'project_slug' in params:
            return f"{base_url.replace('sentry.io', f'{org_slug}.sentry.io')}/projects/{params['project_slug']}/"
        else:
            return f"{base_url.replace('sentry.io', f'{org_slug}.sentry.io')}/projects/"
    
    elif task_type == "project_issues":
        if 'project_id' in params or 'project_slug' in params:
            url_params = []
            
            # Use project ID if available, otherwise fall back to project slug
            if 'project_id' in params:
                url_params.append(f"project={params['project_id']}")
            elif 'project_slug' in params:
                # For project slug, we need to construct the full path
                url_params.append(f"project={params['project_slug']}")
            
            # Add query parameter if provided
            if 'query' in params:
                url_params.append(f"query={urllib.parse.quote(params['query'])}")
            
            # Add standard Sentry UI parameters
            url_params.append("referrer=issue-list")
            url_params.append("sort=date")
            
            # Add time range parameters - prefer start/end for custom ranges, statsPeriod for predefined periods
            if 'start' in params and 'end' in params:
                # Custom time range - use start/end with utc=true
                url_params.append(f"start={urllib.parse.quote(params['start'])}")
                url_params.append(f"end={urllib.parse.quote(params['end'])}")
                url_params.append("utc=true")
            elif 'stats_period' in params:
                # Predefined period (1h, 24h, etc.)
                url_params.append(f"statsPeriod={params['stats_period']}")
            else:
                # Default to 1 hour if no time range specified
                url_params.append("statsPeriod=1h")
            
            query_string = "&".join(url_params)
            # Use the simpler URL structure: https://{org}.sentry.io/issues/
            base_issue_url = f"{base_url.replace('sentry.io', f'{org_slug}.sentry.io')}/issues/"
            return f"{base_issue_url}?{query_string}" if query_string else base_issue_url
        else:
            return f"{base_url.replace('sentry.io', f'{org_slug}.sentry.io')}/issues/"
    
    else:
        logger.warning(f"Unsupported Sentry task type: {task_type}")
        return f"{base_url.replace('sentry.io', f'{org_slug}.sentry.io')}/"


class SentrySourceManager(SourceManager):

    def __init__(self):
        self.source = Source.SENTRY
        self.task_proto = Sentry
        self.task_type_callable_map = {
            Sentry.TaskType.FETCH_ISSUE_INFO_BY_ID: {
                'executor': self.fetch_issue_info_by_id,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Fetch Sentry Issue Related info',
                'category': 'Error',
                'form_fields': [
                    FormField(key_name=StringValue(value="issue_id"),
                              display_name=StringValue(value="Issue ID"),
                              description=StringValue(value='Enter Issue ID'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                ]
            },

            Sentry.TaskType.FETCH_EVENT_INFO_BY_ID: {
                'executor': self.fetch_event_info_by_id,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Fetch Sentry Event Info by ID',
                'category': 'Error',
                'form_fields': [
                    FormField(key_name=StringValue(value="event_id"),
                              display_name=StringValue(value="Event ID"),
                              description=StringValue(value='Enter Event ID'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="project_slug"),
                              display_name=StringValue(value="Project Slug"),
                              description=StringValue(value='Enter Project Slug'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                ]
            },
            Sentry.TaskType.FETCH_LIST_OF_RECENT_EVENTS_WITH_SEARCH_QUERY: {
                'executor': self.fetch_list_of_recent_events_with_search_query,
                'model_types': [],
                'result_type': PlaybookTaskResultType.LOGS,
                'display_name': 'Fetch List of Recent Events with Search Query',
                'category': 'Error',
                'form_fields': [
                    FormField(key_name=StringValue(value="project_slug"),
                              display_name=StringValue(value="Project Slug"),
                              description=StringValue(value='Enter Project Slug'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="query"),
                              display_name=StringValue(value="Query"),
                              description=StringValue(value='Enter Query'),
                              default_value=Literal(type=LiteralType.STRING, string=StringValue(value="is:unresolved")),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="max_events_to_analyse"),
                              display_name=StringValue(value="Max Events to Analyse"),
                              description=StringValue(value='Enter Max Events to Analyse'),
                              default_value=Literal(type=LiteralType.LONG, long=Int64Value(value=10)),
                              data_type=LiteralType.LONG,
                              form_field_type=FormFieldType.TEXT_FT),
                ]
            },
            Sentry.TaskType.FETCH_PROJECTS: {
                'executor': self.fetch_projects,
                'model_types': [],
                'result_type': PlaybookTaskResultType.TABLE,
                'display_name': 'Fetch Sentry Projects',
                'category': 'Projects',
                'form_fields': []
            }
        }
        self.connector_form_configs = [
            {
                "name": StringValue(value="Sentry API Key Connection"),
                "description": StringValue(value="Connect to Sentry using an API Key and Organization Slug."),
                "form_fields": {
                    SourceKeyType.SENTRY_API_KEY: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.SENTRY_API_KEY)),
                        display_name=StringValue(value="API Key"),
                        helper_text=StringValue(value="Enter your Sentry API Key."),
                        description=StringValue(value='e.g. "1234567890abcdefghijklmnopqrstuvwxyz"'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                        is_sensitive=True
                    ),
                    SourceKeyType.SENTRY_ORG_SLUG: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.SENTRY_ORG_SLUG)),
                        display_name=StringValue(value="Organization Slug"),
                        helper_text=StringValue(value="Enter your Sentry Organization Slug"),
                        description=StringValue(value='e.g. "your-organization-slug"'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False
                    )
                }
            }
        ]
        self.connector_type_details = {
            DISPLAY_NAME: "SENTRY",
            CATEGORY: APPLICATION_MONITORING,
        }

    def get_connector_processor(self, sentry_connector, **kwargs):
        generated_credentials = generate_credentials_dict(sentry_connector.type, sentry_connector.keys)
        return SentryApiProcessor(**generated_credentials)

    def _extract_org_slug_from_connector(self, sentry_connector: ConnectorProto) -> str:
        """Extract the organization slug from the Sentry connector."""
        if not sentry_connector or not sentry_connector.keys:
            return ""
        
        for key in sentry_connector.keys:
            if key.key_type == SourceKeyType.SENTRY_ORG_SLUG and key.key.value:
                return key.key.value
        
        return ""

    def _create_metadata_with_sentry_url(self, org_slug: str, task_type: str, params: dict = None) -> Struct:
        """Create metadata struct with Sentry URL."""
        sentry_url = buildSentryUrl(org_slug, task_type, params)
        metadata_dict = {
            "link": sentry_url
        }
        return dict_to_proto(metadata_dict, Struct)

    def _get_sentry_time_params(self, time_range: TimeRange) -> dict:
        """Get properly formatted time parameters for Sentry URLs."""
        start_time = datetime.fromtimestamp(time_range.time_geq, tz=timezone.utc).isoformat()
        end_time = datetime.fromtimestamp(time_range.time_lt, tz=timezone.utc).isoformat()
        return {
            "start": start_time,
            "end": end_time
        }

    def _get_project_id_from_slug(self, sentry_processor, project_slug: str) -> str:
        """Get project ID from project slug by fetching projects list."""
        try:
            projects = sentry_processor.fetch_projects()
            if projects:
                for project in projects:
                    if project.get('slug') == project_slug:
                        return str(project.get('id', ''))
        except Exception as e:
            logger.warning(f"Failed to fetch project ID for slug {project_slug}: {e}")
        return ""

    def fetch_issue_info_by_id(self, time_range: TimeRange, sentry_task: Sentry,
                         sentry_connector: ConnectorProto):
        try:
            if not sentry_connector:
                raise Exception("Task execution Failed:: No Sentry source found")
            task = sentry_task.fetch_issue_info_by_id
            issue_id = task.issue_id.value

            sentry_processor = self.get_connector_processor(sentry_connector)

            issue_details = sentry_processor.fetch_issue_details(issue_id)
            if not issue_details:
                # Extract org slug and create metadata with Sentry URL
                org_slug = self._extract_org_slug_from_connector(sentry_connector)
                metadata = self._create_metadata_with_sentry_url(org_slug, "issue", {
                    "issue_id": issue_id
                })
                
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"No issue found with ID: {issue_id}")),
                    source=self.source,
                    metadata=metadata
                )

            issue_hash_0 = sentry_processor.fetch_issue_last_event(issue_id)
            if not issue_hash_0:
                # Still return issue details even if no events are found
                first_seen = issue_details['firstSeen']
                last_seen = issue_details['lastSeen']
                slug = issue_details.get("project", {}).get("slug", "")
                isUnhandled = issue_details.get("isUnhandled", False)
                users_impacted = len(issue_details['seenBy'])
                
                response_dict = {
                    'first_seen': first_seen, 
                    'last_seen': last_seen, 
                    'users_impacted': users_impacted,
                    'exception_counts': 0, 
                    'count_exception_entries': 0,
                    'stack_trace': {}, 
                    'culprit': issue_details.get('culprit', ''), 
                    'tags': issue_details.get('tags', []), 
                    'url': None,
                    'project_slug': slug, 
                    'isUnhandled': isUnhandled,
                    'additional_data': {
                        'issue_type': issue_details.get('issueType', ''),
                        'issue_category': issue_details.get('issueCategory', ''),
                        'priority': issue_details.get('priority', ''),
                        'status': issue_details.get('status', ''),
                        'count': issue_details.get('count', '0'),
                        'user_count': issue_details.get('userCount', 0),
                        'title': issue_details.get('title', ''),
                        'metadata': issue_details.get('metadata', {}),
                        'permalink': issue_details.get('permalink', '')
                    },
                    'message': 'No events found for the specified issue, but issue details are available.'
                }
                
                # Extract org slug and create metadata with Sentry URL
                org_slug = self._extract_org_slug_from_connector(sentry_connector)
                metadata = self._create_metadata_with_sentry_url(org_slug, "issue", {
                    "issue_id": issue_id
                })
                
                response_struct = dict_to_proto(response_dict, Struct)
                sentry_issue_output = ApiResponseResult(response_body=response_struct)
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.API_RESPONSE,
                    source=self.source,
                    api_response=sentry_issue_output,
                    metadata=metadata
                )
            
            first_seen = issue_details['firstSeen']
            last_seen = issue_details['lastSeen']
            slug = issue_details.get("project", {}).get("slug", "")
            isUnhandled = issue_details.get("isUnhandled", False)
            users_impacted = len(issue_details['seenBy'])
            exception_entries = list(filter(lambda x: x['type'] == 'exception', issue_hash_0['latestEvent']['entries']))
            count_exception_entries = len(exception_entries)
            tags = issue_hash_0['latestEvent']['tags']
            if count_exception_entries > 0:
                exception_counts = len(exception_entries[0]['data']['values'])
                stack_trace = issue_hash_0['latestEvent']['metadata']
                culprit = issue_hash_0['latestEvent']['culprit']
            else:
                exception_counts = 0
                stack_trace = {}
                culprit = issue_hash_0['latestEvent']['culprit']
            request_entries = list(filter(lambda x: x['type'] == 'request', issue_hash_0['latestEvent']['entries']))
            count_request_entries = len(request_entries)
            if count_request_entries > 0:
                url = request_entries[0]['data']['url']
            else:
                url = None
            response_dict = {'first_seen': first_seen, 'last_seen': last_seen, 'users_impacted': users_impacted,
                             'exception_counts': exception_counts, 'count_exception_entries': count_exception_entries,
                             'stack_trace': stack_trace, 'culprit': culprit, 'tags': tags, 'url': url,
                             'project_slug': slug, 'isUnhandled': isUnhandled}  # VG: Added project_slug
            
            # Add additional data from the issue details
            additional_data = {}
            if 'latestEvent' in issue_hash_0:
                latest_event = issue_hash_0['latestEvent']
                # Get additional data from the event
                if 'contexts' in latest_event:
                    additional_data['contexts'] = latest_event['contexts']
                if 'packages' in latest_event:
                    additional_data['packages'] = latest_event['packages']
                if 'extra' in latest_event:
                    additional_data['extra'] = latest_event['extra']
                if 'user' in latest_event:
                    additional_data['user'] = latest_event['user']
                if 'sdk' in latest_event:
                    additional_data['sdk'] = latest_event['sdk']
                if 'breadcrumbs' in latest_event:
                    additional_data['breadcrumbs'] = latest_event['breadcrumbs']
                
                # Get HTTP request data if available
                if count_request_entries > 0:
                    request_data = request_entries[0]['data']
                    additional_data['http_request'] = {
                        'url': request_data.get('url'),
                        'method': request_data.get('method'),
                        'query_string': request_data.get('query_string'),
                        'headers': request_data.get('headers'),
                        'data': request_data.get('data')
                    }

            # Add additional data to response dict
            response_dict['additional_data'] = additional_data
            
            # Extract org slug and create metadata with Sentry URL
            org_slug = self._extract_org_slug_from_connector(sentry_connector)
            metadata = self._create_metadata_with_sentry_url(org_slug, "issue", {
                "issue_id": issue_id
            })
            
            response_struct = dict_to_proto(response_dict, Struct)
            sentry_issue_output = ApiResponseResult(response_body=response_struct)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=sentry_issue_output,
                metadata=metadata
            )

        except Exception as e:
            logger.error(f"Exception occurred while Sentry fetch_issue_info_by_id details with error: {e}")
            
            # Extract org slug and create metadata with Sentry URL for error case
            org_slug = self._extract_org_slug_from_connector(sentry_connector)
            metadata = self._create_metadata_with_sentry_url(org_slug, "issue", {
                "issue_id": issue_id
            })
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=f"Error while executing Sentry task: {e}")),
                source=self.source,
                metadata=metadata
            )

    def fetch_event_info_by_id(self, time_range: TimeRange, sentry_task: Sentry,
                            sentry_connector: ConnectorProto):
        try:
            if not sentry_connector:
                raise Exception("Task execution Failed:: No Sentry source found")

            task = sentry_task.fetch_event_info_by_id
            event_id = task.event_id.value
            project_slug = task.project_slug.value if task.HasField('project_slug') else None

            sentry_processor = self.get_connector_processor(sentry_connector)
            event_details = sentry_processor.fetch_event_details(event_id, project_slug)

            if not event_details:
                # Extract org slug and create metadata with Sentry URL
                org_slug = self._extract_org_slug_from_connector(sentry_connector)
                metadata = self._create_metadata_with_sentry_url(org_slug, "event", {
                    "event_id": event_id,
                    "project_slug": project_slug
                })
                
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"No event found with ID: {event_id}")),
                    source=self.source,
                    metadata=metadata
                )

            # Convert the event details to a structured format
            response_dict = {
                'event_id': event_details.get('eventID', ''),
                'issue_id': event_details.get('issueID', '') or event_details.get('groupID', ''),
                'project': event_details.get('project', {}).get('slug', ''),
                'timestamp': event_details.get('dateCreated', ''),
                'message': event_details.get('message', ''),
                'title': event_details.get('title', ''),
                'tags': event_details.get('tags', []),
                'contexts': event_details.get('contexts', {}),
                'entries': [
                    {
                        'type': entry.get('type', ''),
                        'data': entry.get('data', {})
                    }
                    for entry in event_details.get('entries', [])
                ],
                'metadata': event_details.get('metadata', {}),
                'user': event_details.get('user', {}),
                'sdk': event_details.get('sdk', {}),
                'url': f"https://sentry.io/organizations/{sentry_processor.org_slug}/issues/{event_id}/"
            }

            # Extract org slug and create metadata with Sentry URL
            org_slug = self._extract_org_slug_from_connector(sentry_connector)
            # Get issue_id from event details for proper URL construction
            issue_id = event_details.get('issueID', '') or event_details.get('groupID', '')
            metadata = self._create_metadata_with_sentry_url(org_slug, "event", {
                "event_id": event_id,
                "issue_id": issue_id,
                "project_slug": project_slug
            })
            
            response_struct = dict_to_proto(response_dict, Struct)
            event_output = ApiResponseResult(response_body=response_struct)

            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=event_output,
                metadata=metadata
            )

        except Exception as e:
            logger.error(f"Exception occurred while fetching Sentry event details with error: {e}")
            
            # Extract org slug and create metadata with Sentry URL for error case
            org_slug = self._extract_org_slug_from_connector(sentry_connector)
            metadata = self._create_metadata_with_sentry_url(org_slug, "event", {
                "event_id": event_id,
                "project_slug": project_slug
            })
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=f"Error while executing Sentry event task: {e}")),
                source=self.source,
                metadata=metadata
            )

    def fetch_list_of_recent_events_with_search_query(self, time_range: TimeRange, sentry_task: Sentry,
                            sentry_connector: ConnectorProto):
        try:
            if not sentry_connector:
                raise Exception("Task execution Failed:: No Sentry source found")

            task = sentry_task.fetch_list_of_recent_events_with_search_query
            project_slug = task.project_slug.value
            query = task.query.value
            max_events_to_analyse = task.max_events_to_analyse.value

            sentry_processor = self.get_connector_processor(sentry_connector)
            
            start_time = datetime.fromtimestamp(time_range.time_geq, tz=timezone.utc).isoformat()
            end_time = datetime.fromtimestamp(time_range.time_lt, tz=timezone.utc).isoformat()
            
            # Get project ID for proper URL construction
            project_id = self._get_project_id_from_slug(sentry_processor, project_slug)
            
            # Get issues with the specified tag
            issues = sentry_processor.fetch_issues_with_query(project_slug, query, start_time, end_time)
            if not issues:
                # Extract org slug and create metadata with Sentry URL
                org_slug = self._extract_org_slug_from_connector(sentry_connector)
                metadata = self._create_metadata_with_sentry_url(org_slug, "project_issues", {
                    "project_id": project_id,
                    "project_slug": project_slug,
                    "query": query,
                    "start": start_time,
                    "end": end_time
                })
                
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"No issues found with {query} for project: {project_slug}")),
                    source=self.source,
                    metadata=metadata
                )
                        
            # filter issues where last_seen is not between start and end time. last seen is in this format -- '2025-03-20T18:19:36.613837Z' . 
            issues = [issue for issue in issues if issue.get('lastSeen', '') and datetime.fromisoformat(start_time) <= datetime.fromisoformat(issue.get('lastSeen', '').replace("Z", "+00:00"))]
            # each issue is a dictionary with keys like 'id', 'lastSeen'. sort in descending by 'last_seen'
            issues = sorted(issues, key=lambda x: x.get('lastSeen', 0), reverse=True)

            if not issues:
                # Extract org slug and create metadata with Sentry URL
                org_slug = self._extract_org_slug_from_connector(sentry_connector)
                metadata = self._create_metadata_with_sentry_url(org_slug, "project_issues", {
                    "project_id": project_id,
                    "project_slug": project_slug,
                    "query": query,
                    "start": start_time,
                    "end": end_time
                })
                
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"No issues found with {query} for project: {project_slug}")),
                    source=self.source,
                    metadata=metadata
                )

            all_events = []
            with ThreadPoolExecutor(max_workers=10) as executor:
                process = partial(self.process_issue, time_range=time_range, sentry_processor=sentry_processor, project_slug=project_slug)
                futures = [executor.submit(process, issue) for issue in issues]
                for future in as_completed(futures):
                    all_events.extend(future.result())

            # Select 10 random events from the filtered list
            if len(all_events) > max_events_to_analyse:
                top_events = random.sample(all_events, max_events_to_analyse)
            else:
                top_events = all_events
                
            # Sort the selected events by timestamp in descending order
            top_events = sorted(top_events, key=lambda x: x.get('dateCreated', ''), reverse=True)

            # Create table rows for each event
            table_rows: [TableResult.TableRow] = []

            ## fetch the entire payload of every event, then add the issue_id and respective issue_count in the event.
            events_list = []
            for event in top_events:
                # Get event-specific URL
                event_id = event.get('eventID', '')
                # Get detailed event information
                event_details = sentry_processor.fetch_event_details(event_id, project_slug)
                event_details['issue_id'] = event.get('issue_id', '')
                event_details['issue_count'] = len([e for e in all_events if e.get('issue_id') == event.get('issue_id')])

                # Extract stack trace if available
                if event_details:
                    # Look for exception entries
                    exception_entries = [entry for entry in event_details.get('entries', []) if entry.get('type') == 'exception']
                    if exception_entries and 'data' in exception_entries[0] and 'values' in exception_entries[0]['data']:
                        exception_value = exception_entries[0]['data']['values'][0]
                        stacktrace = exception_value.get('stacktrace')
                        if stacktrace and isinstance(stacktrace, dict):
                            frames = stacktrace.get('frames', [])
                            if frames:
                                # Iterate over all frames and format them
                                stack_traces = [
                                    f"{frame.get('filename', 'Unknown')}:{frame.get('lineno', '?')} in {frame.get('function', 'Unknown')}"
                                    for frame in frames
                                ]
                                # Join all formatted frames into a single string (separated by newline or any delimiter)
                                full_stack_trace = "\n".join(stack_traces)
                                event_details['stack_trace'] = full_stack_trace
                events_list.append(event_details)

            # translate the events_list to a table assuming whatever keys in it as the default columns
            for event in events_list:
                table_columns: [TableResult.TableColumn] = []
                for key, value in event.items():
                    table_column = TableResult.TableColumn(
                        name=StringValue(value=key),
                        value=StringValue(value=str(value))
                    )
                    table_columns.append(table_column)
                table_row = TableResult.TableRow(columns=table_columns)
                table_rows.append(table_row)
            
            # Create the table result
            result = TableResult(
                raw_query=StringValue(
                    value=f"Project: {project_slug}, Query: {query}, Total Events: {len(all_events)}, Showing: {len(top_events)} random events since {start_time}"
                ),
                rows=table_rows,
                total_count=UInt64Value(value=len(table_rows))
            )
            
            # Extract org slug and create metadata with Sentry URL
            org_slug = self._extract_org_slug_from_connector(sentry_connector)
            metadata = self._create_metadata_with_sentry_url(org_slug, "project_issues", {
                "project_id": project_id,
                "project_slug": project_slug,
                "query": query,
                "start": start_time,
                "end": end_time
            })
            
            # Return the logs result
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.LOGS,
                logs=result,
                source=self.source,
                metadata=metadata
            )
            
        except Exception as e:
            logger.error(f"Exception occurred while fetching Sentry event details with tag with error: {e}")
            
            # Extract org slug and create metadata with Sentry URL for error case
            org_slug = self._extract_org_slug_from_connector(sentry_connector)
            project_id = self._get_project_id_from_slug(sentry_processor, project_slug)
            metadata = self._create_metadata_with_sentry_url(org_slug, "project_issues", {
                "project_id": project_id,
                "project_slug": project_slug,
                "query": query,
                "start": start_time,
                "end": end_time
            })
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=f"Error while executing Sentry events search task: {e}")),
                source=self.source,
                metadata=metadata
            )

    def process_issue(self, issue, time_range, sentry_processor, project_slug):
        issue_id = issue.get('id')
        start_time_dt = datetime.fromtimestamp(time_range.time_geq, tz=timezone.utc)
        end_time_dt = datetime.fromtimestamp(time_range.time_lt, tz=timezone.utc)

        try:
            events = sentry_processor.fetch_events_inside_issue(issue_id, project_slug, start_time=start_time_dt, end_time=end_time_dt)
            filtered = [
                event for event in events
                if start_time_dt <= datetime.fromisoformat(event.get('dateCreated', '').replace('Z', '+00:00')) <= end_time_dt
            ]
            for event in filtered:
                event['issue_id'] = issue_id
            return filtered
        except Exception as e:
            logger.error(f"Failed to process issue {issue_id}: {e}")
            return []

    def fetch_projects(self, time_range: TimeRange, sentry_task: Sentry,
                       sentry_connector: ConnectorProto):
        try:
            if not sentry_connector:
                raise Exception("Task execution Failed:: No Sentry source found")

            sentry_processor = self.get_connector_processor(sentry_connector)
            projects = sentry_processor.fetch_projects()

            if not projects:
                # Extract org slug and create metadata with Sentry URL
                org_slug = self._extract_org_slug_from_connector(sentry_connector)
                metadata = self._create_metadata_with_sentry_url(org_slug, "project", {})
                
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value="No projects found for the given Sentry account.")),
                    source=self.source,
                    metadata=metadata
                )

            table_rows: [TableResult.TableRow] = []
            
            project_keys = ['id', 'name', 'slug', 'platform', 'dateCreated', 'status']

            for project in projects:
                table_columns: [TableResult.TableColumn] = []
                for key in project_keys:
                    table_column = TableResult.TableColumn(
                        name=StringValue(value=key.replace('_', ' ').title()),
                        value=StringValue(value=str(project.get(key, '')))
                    )
                    table_columns.append(table_column)
                table_row = TableResult.TableRow(columns=table_columns)
                table_rows.append(table_row)
            
            # Extract org slug and create metadata with Sentry URL
            org_slug = self._extract_org_slug_from_connector(sentry_connector)
            metadata = self._create_metadata_with_sentry_url(org_slug, "project", {})
            
            result = TableResult(
                raw_query=StringValue(value="Sentry Projects"),
                rows=table_rows,
                total_count=UInt64Value(value=len(table_rows))
            )

            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TABLE,
                table=result,
                source=self.source,
                metadata=metadata
            )

        except Exception as e:
            logger.error(f"Exception occurred while fetching Sentry projects: {e}")
            
            # Extract org slug and create metadata with Sentry URL for error case
            org_slug = self._extract_org_slug_from_connector(sentry_connector)
            metadata = self._create_metadata_with_sentry_url(org_slug, "project", {})
            
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=f"Error while executing Sentry projects task: {e}")),
                source=self.source,
                metadata=metadata
            )