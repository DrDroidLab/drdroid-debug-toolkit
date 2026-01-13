import logging

from google.protobuf.struct_pb2 import Struct

from core.utils.credentilal_utils import generate_credentials_dict, get_connector_key_type_string, DISPLAY_NAME, CATEGORY, CODE_REPOSITORY
from core.utils.proto_utils import dict_to_proto
from core.utils.time_utils import format_to_github_timestamp

from google.protobuf.wrappers_pb2 import StringValue

from core.integrations.source_api_processors.github_api_processor import GithubAPIProcessor
from core.integrations.source_manager import SourceManager
from core.protos.base_pb2 import TimeRange, Source, SourceModelType, SourceKeyType
from core.protos.connectors.connector_pb2 import Connector as ConnectorProto
from core.protos.literal_pb2 import LiteralType, Literal
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResult, PlaybookTaskResultType, ApiResponseResult, \
    TextResult
from core.protos.playbooks.source_task_definitions.github_task_pb2 import Github
from core.protos.ui_definition_pb2 import FormField, FormFieldType

logger = logging.getLogger(__name__)


class TimeoutException(Exception):
    pass


class GithubSourceManager(SourceManager):
    def __init__(self):
        self.source = Source.GITHUB
        self.task_proto = Github
        self.task_type_callable_map = {
            Github.TaskType.FETCH_RELATED_COMMITS: {
                'executor': self.fetch_related_commits,
                'model_types': [SourceModelType.GITHUB_REPOSITORY],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Fetch Related Commits for a function',
                'category': 'Code Repository',
                'form_fields': [
                    FormField(key_name=StringValue(value="repo"),
                              display_name=StringValue(value="Repo"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT),
                    FormField(key_name=StringValue(value="file_path"),
                              display_name=StringValue(value="Enter File Path"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="function_name"),
                              display_name=StringValue(value="Enter Function name"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="branch"),
                              display_name=StringValue(value="Enter Branch name. Defaults to main"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              default_value=Literal(type=LiteralType.STRING, string=StringValue(value='main'))),
                ]
            },
            Github.TaskType.FETCH_FILE: {
                'executor': self.fetch_file,
                'model_types': [SourceModelType.GITHUB_REPOSITORY],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Fetch Files',
                'category': 'Code Repository',
                'form_fields': [
                    FormField(key_name=StringValue(value="repo"),
                              display_name=StringValue(value="Enter Repository name"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT),
                    FormField(key_name=StringValue(value="file_path"),
                              display_name=StringValue(value="Enter File Name"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT)
                ]
            },
            Github.TaskType.UPDATE_FILE: {
                'executor': self.update_file,
                'model_types': [SourceModelType.GITHUB_REPOSITORY, SourceModelType.GITHUB_MEMBER],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Update Files',
                'category': 'Code Repository',
                'form_fields': [
                    FormField(key_name=StringValue(value="repo"),
                              display_name=StringValue(value="Enter Repository name"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT),
                    FormField(key_name=StringValue(value="file_path"),
                              display_name=StringValue(value="Enter File Path"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="file_fetch_button"),
                              display_name=StringValue(value="Fetch File"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.BUTTON_FT),
                    FormField(key_name=StringValue(value="committer_name"),
                              display_name=StringValue(value="Enter Name for Commit message"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="committer_email"),
                              display_name=StringValue(value="Enter Email for Commit message"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="content"),
                              display_name=StringValue(value="Modified Content"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.CODE_EDITOR_FT),
                    FormField(key_name=StringValue(value="sha"),
                              display_name=StringValue(value="Enter Sha of previous commit"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="branch"),
                              display_name=StringValue(value="Enter Branch name. Defaults to main"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              default_value=Literal(type=LiteralType.STRING, string=StringValue(value='main')),
                              is_optional=True),
                ]
            },
            Github.TaskType.FETCH_RECENT_COMMITS: {
                'executor': self.fetch_recent_commits,
                'model_types': [SourceModelType.GITHUB_REPOSITORY],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Fetch Recent Commits for a branch',
                'category': 'Code Repository',
                'form_fields': [
                    FormField(key_name=StringValue(value="repo"),
                              display_name=StringValue(value="Repo"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT),
                    FormField(key_name=StringValue(value="branch"),
                              display_name=StringValue(value="Enter Branch Name. Defaults to main"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              default_value=Literal(type=LiteralType.STRING, string=StringValue(value='main'))),
                    FormField(key_name=StringValue(value="author"),
                              display_name=StringValue(value="Commit Author (optional)"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True)
                ]
            },
            Github.TaskType.FETCH_RECENT_MERGES: {
                'executor': self.fetch_recent_merges,
                'model_types': [SourceModelType.GITHUB_REPOSITORY],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Fetch Recent Merges for a branch',
                'category': 'Code Repository',
                'form_fields': [
                    FormField(key_name=StringValue(value="repo"),
                              display_name=StringValue(value="Repo"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT),
                    FormField(key_name=StringValue(value="branch"),
                              display_name=StringValue(value="Enter Branch Name. Defaults to main"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              default_value=Literal(type=LiteralType.STRING, string=StringValue(value='main')))
                ]
            },
            Github.TaskType.CREATE_PULL_REQUEST: {
                'executor': self.create_pull_request,
                'model_types': [SourceModelType.GITHUB_REPOSITORY],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Create Pull Request',
                'category': 'Code Repository',
                'form_fields': [
                    FormField(key_name=StringValue(value="repo"),
                              display_name=StringValue(value="Repository"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT),
                    FormField(key_name=StringValue(value="title"),
                              display_name=StringValue(value="PR Title"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="body"),
                              display_name=StringValue(value="PR Description"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.MULTILINE_FT),
                    FormField(key_name=StringValue(value="head_branch"),
                              display_name=StringValue(value="Source Branch"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="base_branch"),
                              display_name=StringValue(value="Target Branch (defaults to main branch)"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="committer_name"),
                              display_name=StringValue(value="Committer Name"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="committer_email"),
                              display_name=StringValue(value="Committer Email"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="commit_message"),
                              display_name=StringValue(value="Commit Message"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="files"),
                              display_name=StringValue(value="Files to Update (JSON array of {path, content} objects)"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.CODE_EDITOR_FT,
                              is_optional=True,
                              default_value=Literal(type=LiteralType.STRING, string=StringValue(
                                  value='[{"path": "path/to/file1.js", "content": "content of file1"},{"path": "path/to/file2.js", "content": "content of file2"}]')))
                ]
            },
            Github.TaskType.ANALYZE_SENTRY_CREATE_PR: {
                'display_name': 'Analyze Sentry Alerts and Create PR',
                'model_types': [SourceModelType.GITHUB_REPOSITORY]
            }
        }

        self.connector_form_configs = [
            {
                "name": StringValue(value="GitHub Authentication"),
                "description": StringValue(value="Connect to GitHub using a Personal Access Token (PAT) and specify the Organization. Ensure the PAT has the necessary scopes (e.g., `repo` for private repositories, `org:read` for organization details)."),
                "form_fields": {
                    SourceKeyType.GITHUB_TOKEN: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.GITHUB_TOKEN)),
                        display_name=StringValue(value="GitHub Token"),
                        description=StringValue(value='e.g. "ghp_1234567890abcdefghijklmnopqrstuvwxyz"'),
                        helper_text=StringValue(value="Enter your GitHub Personal Access Token with 'repo' and 'org:read' scopes from Developer Settings > Personal Access Tokens"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                        is_sensitive=True
                    ),
                    SourceKeyType.GITHUB_ORG: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.GITHUB_ORG)),
                        display_name=StringValue(value="GitHub Organization"),
                        description=StringValue(value='e.g. "my-org", "acme-corp", "awesome-team"'),
                        helper_text=StringValue(value="Enter your GitHub organization name (found in the URL: github.com/org-name)"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False
                    )
                }
            },
            {
                "name": StringValue(value="GitHub App Authentication"),
                "description": StringValue(value="Connect to GitHub using a GitHub App. This provides more granular permissions and is recommended for production environments. Note: The GitHub App Private Key is configured as a service-level environment variable (GITHUB_APP_PRIVATE_KEY) and is not stored per connector."),
                "form_fields": {
                    SourceKeyType.GITHUB_APP_ID: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.GITHUB_APP_ID)),
                        display_name=StringValue(value="GitHub App ID"),
                        description=StringValue(value='e.g. "123456"'),
                        helper_text=StringValue(value="Enter your GitHub App ID (found in your GitHub App settings)"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False
                    ),
                    # Note: GITHUB_APP_PRIVATE_KEY is NOT shown in the form
                    # It is retrieved from service settings (GITHUB_APP_PRIVATE_KEY) when needed
                    # This is a service-level secret, same for all installations
                    SourceKeyType.GITHUB_APP_INSTALLATION_ID: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.GITHUB_APP_INSTALLATION_ID)),
                        display_name=StringValue(value="GitHub App Installation ID"),
                        description=StringValue(value='e.g. "12345678"'),
                        helper_text=StringValue(value="Enter the Installation ID for your GitHub App (obtained after installing the app on an organization/account)"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False
                    ),
                    SourceKeyType.GITHUB_ORG: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.GITHUB_ORG)),
                        display_name=StringValue(value="GitHub Organization (Optional)"),
                        description=StringValue(value='e.g. "my-org", "acme-corp"'),
                        helper_text=StringValue(value="Enter the GitHub organization name if the app is installed on an organization. Leave empty if installed on a user account."),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True
                    )
                }
            }
        ]
        self.connector_type_details = {
            DISPLAY_NAME: "GITHUB",
            CATEGORY: CODE_REPOSITORY,
        }

    def get_connector_processor(self, github_connector, **kwargs):
        generated_credentials = generate_credentials_dict(github_connector.type, github_connector.keys)
        
        # Check if using GitHub App authentication
        # Note: We don't check for GITHUB_APP_PRIVATE_KEY as it's not stored in DB
        has_app_keys = any(
            key.key_type in [
                SourceKeyType.GITHUB_APP_ID,
                SourceKeyType.GITHUB_APP_INSTALLATION_ID
            ]
            for key in github_connector.keys
        )
        
        if has_app_keys:
            # Import GitHub App API Processor
            from core.integrations.source_api_processors.github_app_api_processor import GithubAppAPIProcessor
            return GithubAppAPIProcessor(**generated_credentials)
        else:
            # Traditional PAT-based processor
            from core.integrations.source_api_processors.github_api_processor import GithubAPIProcessor
            return GithubAPIProcessor(**generated_credentials)

    def fetch_related_commits(self, time_range: TimeRange, github_task: Github,
                              github_connector: ConnectorProto):
        try:
            task = github_task.fetch_related_commits
            repo = task.repo.value
            file_path = task.file_path.value
            function_name = task.function_name.value
            branch = task.branch.value
            commits = self.get_connector_processor(github_connector).get_file_commits(repo, file_path, branch)
            if not commits:
                return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT, text=TextResult(output=StringValue(
                    value=f"No commits returned from Github for repo: {repo}, file: {file_path}, branch: {branch} and "
                          f"function: {function_name}")), source=self.source)

            # Loop through the commits and get the diff for each one
            related_commits = []
            for commit in commits:
                commit_sha = commit['sha']
                commit_details = self.get_connector_processor(github_connector).get_commit_sha(repo, commit_sha)
                # Get the diff for the specific file
                for file in commit_details['files']:
                    if file['filename'] == file_path:
                        patch = file.get('patch', '')
                        if function_name in patch:
                            related_commit = {'commit_url': commit['html_url'], 'commit_sha': commit_sha,
                                              'author_name': commit['commit']['author']['name'],
                                              'author_email': commit['commit']['author']['email'],
                                              'commit_date': commit['commit']['author']['date'], 'patch': patch}
                            related_commits.append(related_commit)

            response_struct = dict_to_proto({"all_commits": related_commits}, Struct)
            commit_output = ApiResponseResult(response_body=response_struct)
            return PlaybookTaskResult(type=PlaybookTaskResultType.API_RESPONSE, source=self.source,
                                      api_response=commit_output)
        except Exception as e:
            raise Exception(f"Error while executing Github fetch_related_commits task: {e}")

    def fetch_file(self, time_range: TimeRange, github_task: Github,
                   github_connector: ConnectorProto):
        try:
            task = github_task.fetch_file
            repo = task.repo.value
            file_path = task.file_path.value
            timestamp = task.timestamp if task.timestamp != 0 else None
            processor = self.get_connector_processor(github_connector)
            all_repos = processor.list_all_repos()
            
            # Validate repo exists (handle None/empty list from list_all_repos)
            # If validation fails, still try the API call - repo might be accessible but not in list
            if all_repos:
                all_repo_names = [r.get('name', '') for r in all_repos if r and r.get('name')]
                all_repo_full_names = [r.get('full_name', '') for r in all_repos if r and r.get('full_name')]
                
                # Check both name and full_name
                repo_found = repo in all_repo_names or repo in all_repo_full_names
                if not repo_found:
                    # Also check if repo matches the end of any full_name (e.g., "repo" matches "owner/repo")
                    for full_name in all_repo_full_names:
                        if full_name and full_name.endswith(f'/{repo}'):
                            repo_found = True
                            break
                    
                    if not repo_found:
                        # Log warning but don't fail - repo might still be accessible
                        logger.warning(f"Repository {repo} not found in accessible repos list ({len(all_repos)} repos), attempting fetch anyway")
            # If all_repos is None or empty, skip validation and let the API call handle it
            
            # Validate file_path is provided
            if not file_path or not file_path.strip():
                raise Exception(f"File path is required. Please provide a valid file path.")
            
            file_details = processor.fetch_file(repo, file_path, timestamp=timestamp)
            if not file_details:
                raise Exception(f"Failed to fetch file {file_path} from repository {repo}. Repository may not exist or may not be accessible. Check if the repository name is correct and the GitHub App has access to it.")
            
            # Check if response is a list (directory contents) instead of a dict (file)
            if isinstance(file_details, list):
                raise Exception(f"Path '{file_path}' is a directory, not a file. Please provide a specific file path. Directory contains: {', '.join([item.get('name', 'unknown') for item in file_details[:10]])}")
            
            # Ensure file_details is a dict
            if not isinstance(file_details, dict):
                raise Exception(f"Unexpected response format from GitHub API. Expected a file object, got: {type(file_details)}")
            
            response_struct = dict_to_proto(file_details, Struct)
            file_output = ApiResponseResult(response_body=response_struct)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=file_output
            )
        except Exception as e:
            raise Exception(f"Error while executing Github fetch_file task: {e}")

    def update_file(self, time_range: TimeRange, github_task: Github,
                    github_connector: ConnectorProto):
        try:
            task = github_task.update_file
            repo = task.repo.value
            file_path = task.file_path.value
            content = task.content.value
            sha = task.sha.value
            committer_name = task.committer_name.value
            committer_email = task.committer_email.value
            branch = task.branch.value if task.branch.value else None
            processor = self.get_connector_processor(github_connector)
            all_repos = processor.list_all_repos()
            if all_repos:
                all_repo_names = [r.get('name', '') for r in all_repos if r and r.get('name')]
                if repo not in all_repo_names:
                    raise Exception(f"Repository {repo} not found")
            file_details = processor.update_file(repo=repo, file_path=file_path, content=content, sha=sha,
                                                 committer_name=committer_name, committer_email=committer_email,
                                                 branch_name=branch)
            response_struct = dict_to_proto(file_details, Struct)
            file_output = ApiResponseResult(response_body=response_struct)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=file_output
            )
        except Exception as e:
            raise Exception(f"Error while executing Github update_file task: {e}")

    def fetch_recent_commits(self, time_range: TimeRange, github_task: Github,
                             github_connector: ConnectorProto):
        try:
            task = github_task.fetch_recent_commits
            repo = task.repo.value
            branch = task.branch.value
            # Only include time filters if they're actually set (non-zero)
            time_since = format_to_github_timestamp(time_range.time_geq) if time_range.time_geq > 0 else None
            time_until = format_to_github_timestamp(time_range.time_lt) if time_range.time_lt > 0 else None
            author = task.author.value if task.author.value else None
            
            logger.info(f"Fetching recent commits for repo: {repo}, branch: {branch}, time_since: {time_since}, time_until: {time_until}, author: {author}")
            
            recent_commits = self.get_connector_processor(github_connector).get_branch_commits(repo, branch, time_since,
                                                                                               time_until, author)
            
            if recent_commits is None:
                return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT, text=TextResult(output=StringValue(
                    value=f"Failed to fetch commits from Github for repo: {repo}, branch: {branch}. The repository may not exist, the branch may not exist, or there may be an authentication issue.")), source=self.source)
            
            if not recent_commits or len(recent_commits) == 0:
                time_filter_msg = ""
                if time_since or time_until:
                    time_filter_msg = f" within the specified time range"
                return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT, text=TextResult(output=StringValue(
                    value=f"No commits found for repo: {repo}, branch: {branch}{time_filter_msg}. Try removing time filters or checking if the branch has any commits.")), source=self.source)
            
            response_struct = dict_to_proto({"recent_commits": recent_commits}, Struct)
            commit_output = ApiResponseResult(response_body=response_struct)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=commit_output
            )
        except Exception as e:
            raise Exception(f"Error while executing Github fetch_recent_commits task: {e}")

    def create_pull_request(self, time_range: TimeRange, github_task: Github,
                            github_connector: ConnectorProto):
        try:
            task = github_task.create_pull_request
            repo = task.repo.value
            title = task.title.value
            body = task.body.value
            head_branch = task.head_branch.value
            base_branch = task.base_branch.value if task.HasField('base_branch') else None

            # Extract new fields
            files_to_update = []
            if task.files:
                for file_update in task.files:
                    files_to_update.append({
                        'path': file_update.path.value,
                        'content': file_update.content.value
                    })

            committer_name = task.committer_name.value if task.HasField('committer_name') else None
            committer_email = task.committer_email.value if task.HasField('committer_email') else None
            commit_message = task.commit_message.value if task.HasField('commit_message') else None

            processor = self.get_connector_processor(github_connector)
            all_repos = processor.list_all_repos()
            if all_repos:
                all_repo_names = [r.get('name', '') for r in all_repos if r and r.get('name')]
                if repo not in all_repo_names:
                    raise Exception(f"Repository {repo} not found")

            pr_details = processor.create_pull_request(repo=repo, title=title, body=body, head=head_branch,
                                                       base=base_branch,
                                                       files_to_update=files_to_update if files_to_update else None,
                                                       commit_message=commit_message, committer_name=committer_name,
                                                       committer_email=committer_email)
            if not pr_details:
                return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT, text=TextResult(
                    output=StringValue(value=f"Failed to create PR in repository {repo}")), source=self.source)

            # Check for error message
            if 'error' in pr_details:
                error_msg = pr_details['error']
                # Make error message more user-friendly
                if "No commits between" in error_msg:
                    error_msg = "Branch exists but has no commits. Add commits before creating PR"
                return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT,
                                          text=TextResult(output=StringValue(value=error_msg)), source=self.source)

            response_struct = dict_to_proto({"pr_number": pr_details['number'], "pr_url": pr_details['html_url']},
                                            Struct)
            pr_output = ApiResponseResult(response_body=response_struct)
            return PlaybookTaskResult(type=PlaybookTaskResultType.API_RESPONSE, source=self.source,
                                      api_response=pr_output)
        except Exception as e:
            raise Exception(f"Error while executing Github create_pull_request task: {e}")

    def fetch_recent_merges(self, time_range: TimeRange, github_task: Github,
                            github_connector: ConnectorProto):
        try:
            task = github_task.fetch_recent_merges
            repo = task.repo.value
            branch = task.branch.value
            recent_merges = self.get_connector_processor(github_connector).get_recent_merges(repo, branch)
            response_struct = dict_to_proto({"recent_merges": recent_merges}, Struct)
            commit_output = ApiResponseResult(response_body=response_struct)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=commit_output
            )
        except Exception as e:
            raise Exception(f"Error while executing Github fetch_recent_merges task: {e}")

    @staticmethod
    def validate_connector(connector: ConnectorProto) -> bool:
        from core.integrations.source_facade import source_facade as playbook_source_facade
        if connector.is_proxy_enabled.value:
            return True
        keys = connector.keys
        all_ck_types = [ck.key_type for ck in keys if ck.key.value]
        all_ck_types = list(set(all_ck_types))
        
        # Check for GitHub App keys
        # Note: We don't check for GITHUB_APP_PRIVATE_KEY as it's not stored in DB
        has_app_keys = any(
            key_type in [
                SourceKeyType.GITHUB_APP_ID,
                SourceKeyType.GITHUB_APP_INSTALLATION_ID
            ]
            for key_type in all_ck_types
        )
        
        if has_app_keys:
            # Validate GitHub App keys
            # Note: GITHUB_APP_PRIVATE_KEY is NOT stored in DB - retrieved from settings
            required_app_keys = [
                SourceKeyType.GITHUB_APP_ID,
                SourceKeyType.GITHUB_APP_INSTALLATION_ID
            ]
            return all(key_type in all_ck_types for key_type in required_app_keys)
        else:
            # Validate PAT keys
            required_key_types = playbook_source_facade.get_connector_required_keys(connector.type)
            all_keys_found = False
            for rkt in required_key_types:
                if set(rkt) <= set(all_ck_types):
                    all_keys_found = True
                    break
            return all_keys_found
