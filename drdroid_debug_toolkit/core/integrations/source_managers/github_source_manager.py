import logging

from google.protobuf.struct_pb2 import Struct

from core.utils.credentilal_utils import generate_credentials_dict, get_connector_key_type_string, DISPLAY_NAME, CATEGORY, CODE_REPOSITORY
from core.utils.proto_utils import dict_to_proto
from core.utils.time_utils import format_to_github_timestamp

from google.protobuf.wrappers_pb2 import StringValue, Int64Value

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
from core.integrations.utils.task_result_utils import build_task_failure_result, build_task_empty_result


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
            },
            Github.TaskType.CREATE_PROJECT_DRAFT_ISSUE: {
                'executor': self.create_project_draft_issue,
                'model_types': [SourceModelType.GITHUB_PROJECT],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Create Project Draft Issue',
                'category': 'Projects',
                'form_fields': [
                    FormField(key_name=StringValue(value="project_number"),
                              display_name=StringValue(value="Project Number"),
                              data_type=LiteralType.LONG,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="title"),
                              display_name=StringValue(value="Title"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="body"),
                              display_name=StringValue(value="Body"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.MULTILINE_FT,
                              is_optional=True),
                ]
            },
            Github.TaskType.FETCH_ORG_PROJECTS: {
                'executor': self.fetch_org_projects,
                'model_types': [SourceModelType.GITHUB_PROJECT],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Fetch Org Projects',
                'category': 'Projects',
                'form_fields': [
                    FormField(key_name=StringValue(value="org"),
                              display_name=StringValue(value="Organization (optional override)"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                ]
            },
            Github.TaskType.FETCH_PROJECT: {
                'executor': self.fetch_project,
                'model_types': [SourceModelType.GITHUB_PROJECT],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Fetch Project Details',
                'category': 'Projects',
                'form_fields': [
                    FormField(key_name=StringValue(value="project_number"),
                              display_name=StringValue(value="Project Number"),
                              data_type=LiteralType.LONG,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="org"),
                              display_name=StringValue(value="Organization (optional override)"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                ]
            },
            Github.TaskType.FETCH_PROJECT_ITEMS: {
                'executor': self.fetch_project_items,
                'model_types': [SourceModelType.GITHUB_PROJECT],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Fetch Project Items',
                'category': 'Projects',
                'form_fields': [
                    FormField(key_name=StringValue(value="project_number"),
                              display_name=StringValue(value="Project Number"),
                              data_type=LiteralType.LONG,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="org"),
                              display_name=StringValue(value="Organization (optional override)"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="per_page"),
                              display_name=StringValue(value="Per page (max 100)"),
                              data_type=LiteralType.LONG,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True,
                              default_value=Literal(type=LiteralType.LONG, long=Int64Value(value=50))),
                ]
            },
            Github.TaskType.CREATE_PROJECT_SUB_ISSUE: {
                'executor': self.create_project_sub_issue,
                'model_types': [SourceModelType.GITHUB_PROJECT, SourceModelType.GITHUB_REPOSITORY],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Create Project Sub-Issue',
                'category': 'Projects',
                'form_fields': [
                    FormField(key_name=StringValue(value="project_number"),
                              display_name=StringValue(value="Project Number"),
                              data_type=LiteralType.LONG,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="title"),
                              display_name=StringValue(value="Title"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="body"),
                              display_name=StringValue(value="Body"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.MULTILINE_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="parent_repo"),
                              display_name=StringValue(value="Parent Repo (owner/repo)"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="parent_issue_number"),
                              display_name=StringValue(value="Parent Issue Number"),
                              data_type=LiteralType.LONG,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="parent_issue_id"),
                              display_name=StringValue(value="Parent Issue ID (optional)"),
                              data_type=LiteralType.LONG,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                ]
            },
        }

        self.connector_form_configs = [
            {
                "name": StringValue(value="GitHub Personal Access Token"),
                "description": StringValue(
                    value=(
                        "Connect to GitHub using a Personal Access Token (PAT) and specify the Organization. "
                        "Fine-grained permissions: Contents (R/W), Metadata (R), Pull requests (R/W), "
                        "and Projects (R/W) for creating draft issues on org project boards. "
                        "Classic tokens need `repo` and `org:read` (plus project access as applicable)."
                    )
                ),
                "form_fields": {
                    SourceKeyType.GITHUB_TOKEN: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.GITHUB_TOKEN)),
                        display_name=StringValue(value="GitHub Token"),
                        description=StringValue(value='Your GitHub Personal Access Token (classic or fine-grained)'),
                        helper_text=StringValue(
                            value=(
                                "Fine-grained: Contents R/W, Metadata R, Pull requests R/W, Projects R/W. "
                                "Classic: repo + org:read. Projects access is required to create draft issues on boards."
                            )
                        ),
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
                    ),
                    SourceKeyType.GITHUB_API_URL: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.GITHUB_API_URL)),
                        display_name=StringValue(value="GitHub API URL (Optional)"),
                        description=StringValue(value='e.g. "https://github.mycompany.com/api/v3"'),
                        helper_text=StringValue(value="Leave empty for GitHub.com. For GitHub Enterprise Server, enter the API URL: https://<hostname>/api/v3"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True
                    )
                }
            },
            {
                "name": StringValue(value="GitHub App"),
                "description": StringValue(
                    value=(
                        "Connect to GitHub using a GitHub App installation. "
                        "App ID and Installation ID are stored on the connector; "
                        "the App private key is configured server-side and is not shown here. "
                        "The App must have Projects: Read and write (plus Contents/PRs as needed). "
                        "After changing App permissions, re-accept the installation on the org, "
                        "then refresh connector assets so projects appear in pickers."
                    )
                ),
                "form_fields": {
                    SourceKeyType.GITHUB_APP_ID: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.GITHUB_APP_ID)),
                        display_name=StringValue(value="GitHub App ID"),
                        description=StringValue(value="Numeric GitHub App ID for this installation"),
                        helper_text=StringValue(
                            value="Set automatically when you install the Doctor Droid GitHub App"
                        ),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                    ),
                    SourceKeyType.GITHUB_APP_INSTALLATION_ID: FormField(
                        key_name=StringValue(
                            value=get_connector_key_type_string(SourceKeyType.GITHUB_APP_INSTALLATION_ID)
                        ),
                        display_name=StringValue(value="GitHub App Installation ID"),
                        description=StringValue(value="Installation ID for the App on your org/account"),
                        helper_text=StringValue(
                            value=(
                                "Set automatically on install. If Projects APIs fail with 403, "
                                "update the App's Projects permission and re-accept this installation."
                            )
                        ),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                    ),
                    SourceKeyType.GITHUB_ORG: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.GITHUB_ORG)),
                        display_name=StringValue(value="GitHub Organization"),
                        description=StringValue(value='e.g. "my-org", "acme-corp", "awesome-team"'),
                        helper_text=StringValue(
                            value="Organization that owns this GitHub App installation (required for org project boards)"
                        ),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True,
                    ),
                }
            },
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
                return build_task_failure_result(self.source, f"No commits returned from Github for repo: {repo}, file: {file_path}, branch: {branch} and "
                          f"function: {function_name}")

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
                return build_task_failure_result(self.source, f"Failed to fetch commits from Github for repo: {repo}, branch: {branch}. The repository may not exist, the branch may not exist, or there may be an authentication issue.")
            
            if not recent_commits or len(recent_commits) == 0:
                time_filter_msg = ""
                if time_since or time_until:
                    time_filter_msg = f" within the specified time range"
                # "No commits" means the query ran but the branch/time-range had none — that's an
                # empty result, NOT an operational fault. Emitting it as empty_query_result (instead
                # of a 500) stops benign "no commits" outcomes counting as tool-call failures.
                branch_label = branch or "(default branch)"
                return build_task_empty_result(self.source, f"No commits found for repo: {repo}, branch: {branch_label}{time_filter_msg}. Try removing time filters or checking if the branch has any commits.")
            
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
                return build_task_failure_result(self.source, f"Failed to create PR in repository {repo}")

            # Check for error message
            if 'error' in pr_details:
                error_msg = pr_details['error']
                # Make error message more user-friendly
                if "No commits between" in error_msg:
                    error_msg = "Branch exists but has no commits. Add commits before creating PR"
                return build_task_failure_result(self.source, error_msg)

            response_struct = dict_to_proto({"pr_number": pr_details['number'], "pr_url": pr_details['html_url']},
                                            Struct)
            pr_output = ApiResponseResult(response_body=response_struct)
            return PlaybookTaskResult(type=PlaybookTaskResultType.API_RESPONSE, source=self.source,
                                      api_response=pr_output)
        except Exception as e:
            raise Exception(f"Error while executing Github create_pull_request task: {e}")

    def create_project_draft_issue(self, time_range: TimeRange, github_task: Github,
                                   github_connector: ConnectorProto):
        try:
            task = github_task.create_project_draft_issue
            project_number = task.project_number.value
            title = task.title.value
            body = task.body.value if task.HasField('body') else ''

            processor = self.get_connector_processor(github_connector)
            org = getattr(processor, 'org', None)
            if not org:
                return build_task_failure_result(
                    self.source, "GitHub connector has no org configured"
                )

            result = processor.create_project_draft_issue(
                org=org,
                project_number=project_number,
                title=title,
                body=body,
            )
            if not result:
                return build_task_failure_result(
                    self.source, f"Failed to create draft issue on project #{project_number}"
                )
            if isinstance(result, dict) and result.get('error'):
                return build_task_failure_result(self.source, result['error'])

            response_body = {
                'item_id': result.get('item_id'),
                'project_number': result.get('project_number'),
                'title': result.get('title'),
                'project_url': result.get('project_url'),
                'status': result.get('status', 'Created'),
            }
            response_struct = dict_to_proto(response_body, Struct)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=ApiResponseResult(response_body=response_struct),
            )
        except Exception as e:
            raise Exception(f"Error while executing Github create_project_draft_issue task: {e}")

    def _resolve_github_org(self, processor, task) -> str:
        override = task.org.value.strip() if task.HasField('org') and task.org.value else ''
        org = override or getattr(processor, 'org', None)
        return org or ''

    def fetch_org_projects(self, time_range: TimeRange, github_task: Github,
                           github_connector: ConnectorProto):
        try:
            task = github_task.fetch_org_projects
            processor = self.get_connector_processor(github_connector)
            org = self._resolve_github_org(processor, task)
            if not org:
                return build_task_failure_result(
                    self.source, "GitHub connector has no org configured"
                )

            projects = processor.list_org_projects(org=org) or []
            simplified = []
            for project in projects:
                simplified.append({
                    'number': project.get('number'),
                    'title': project.get('title') or project.get('name'),
                    'id': project.get('id'),
                    'url': project.get('url') or project.get('html_url'),
                    'state': project.get('state'),
                    'public': project.get('public'),
                    'short_description': project.get('short_description'),
                    'created_at': project.get('created_at'),
                    'updated_at': project.get('updated_at'),
                })
            response_struct = dict_to_proto(
                {'org': org, 'count': len(simplified), 'projects': simplified},
                Struct,
            )
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=ApiResponseResult(response_body=response_struct),
            )
        except Exception as e:
            raise Exception(f"Error while executing Github fetch_org_projects task: {e}")

    def fetch_project(self, time_range: TimeRange, github_task: Github,
                      github_connector: ConnectorProto):
        try:
            task = github_task.fetch_project
            project_number = task.project_number.value
            processor = self.get_connector_processor(github_connector)
            org = self._resolve_github_org(processor, task)
            if not org:
                return build_task_failure_result(
                    self.source, "GitHub connector has no org configured"
                )

            project = processor.get_org_project(org=org, project_number=project_number) or {}
            response_body = {
                'org': org,
                'project_number': int(project_number),
                'project': {
                    'number': project.get('number'),
                    'title': project.get('title') or project.get('name'),
                    'id': project.get('id'),
                    'url': project.get('url') or project.get('html_url'),
                    'state': project.get('state'),
                    'public': project.get('public'),
                    'short_description': project.get('short_description'),
                    'readme': project.get('readme'),
                    'created_at': project.get('created_at'),
                    'updated_at': project.get('updated_at'),
                    'closed_at': project.get('closed_at'),
                    'deleted_at': project.get('deleted_at'),
                    'owner': project.get('owner'),
                },
            }
            response_struct = dict_to_proto(response_body, Struct)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=ApiResponseResult(response_body=response_struct),
            )
        except Exception as e:
            raise Exception(f"Error while executing Github fetch_project task: {e}")

    def fetch_project_items(self, time_range: TimeRange, github_task: Github,
                            github_connector: ConnectorProto):
        try:
            task = github_task.fetch_project_items
            project_number = task.project_number.value
            per_page = task.per_page.value if task.HasField('per_page') else 50
            processor = self.get_connector_processor(github_connector)
            org = self._resolve_github_org(processor, task)
            if not org:
                return build_task_failure_result(
                    self.source, "GitHub connector has no org configured"
                )

            items = processor.list_org_project_items(
                org=org, project_number=project_number, per_page=per_page
            ) or []
            simplified = []
            for item in items:
                content = item.get('content') or {}
                content_type = (
                    item.get('content_type')
                    or item.get('type')
                    or content.get('type')
                    or ''
                )
                url = content.get('html_url') or content.get('url') or ''
                repo_full_name = self._repo_from_issue_url(url)
                repository = content.get('repository') or {}
                if not repo_full_name and isinstance(repository, dict):
                    repo_full_name = repository.get('full_name') or ''
                    if not repo_full_name and repository.get('name'):
                        owner = (repository.get('owner') or {}).get('login') or org
                        repo_full_name = f"{owner}/{repository.get('name')}"
                simplified.append({
                    'id': item.get('id'),
                    'content_type': content_type,
                    'created_at': item.get('created_at'),
                    'updated_at': item.get('updated_at'),
                    'title': content.get('title') or item.get('title'),
                    'body': content.get('body') or item.get('body'),
                    'number': content.get('number'),
                    'issue_id': content.get('id'),
                    'state': content.get('state'),
                    'url': url,
                    'repo': repo_full_name,
                    'is_issue': str(content_type).lower() in ('issue', 'issues'),
                })
            issues = [i for i in simplified if i.get('is_issue') and i.get('number')]
            response_struct = dict_to_proto(
                {
                    'org': org,
                    'project_number': int(project_number),
                    'count': len(simplified),
                    'issue_count': len(issues),
                    'items': simplified,
                    'issues': issues,
                },
                Struct,
            )
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=ApiResponseResult(response_body=response_struct),
            )
        except Exception as e:
            raise Exception(f"Error while executing Github fetch_project_items task: {e}")

    @staticmethod
    def _repo_from_issue_url(url: str) -> str:
        if not url:
            return ''
        # https://github.com/{owner}/{repo}/issues/{n}
        try:
            parts = url.split('github.com/', 1)[-1].strip('/').split('/')
            if len(parts) >= 2:
                return f"{parts[0]}/{parts[1]}"
        except Exception:
            return ''
        return ''

    def create_project_sub_issue(self, time_range: TimeRange, github_task: Github,
                                 github_connector: ConnectorProto):
        try:
            task = github_task.create_project_sub_issue
            project_number = task.project_number.value
            title = task.title.value
            body = task.body.value if task.HasField('body') else ''
            parent_repo = task.parent_repo.value
            parent_issue_number = task.parent_issue_number.value
            parent_issue_id = (
                task.parent_issue_id.value if task.HasField('parent_issue_id') else None
            )

            processor = self.get_connector_processor(github_connector)
            org = getattr(processor, 'org', None)
            if not org:
                return build_task_failure_result(
                    self.source, "GitHub connector has no org configured"
                )

            result = processor.create_project_sub_issue(
                org=org,
                project_number=project_number,
                title=title,
                body=body,
                parent_repo=parent_repo,
                parent_issue_number=parent_issue_number,
                parent_issue_id=parent_issue_id,
            )
            if not result:
                return build_task_failure_result(
                    self.source, f"Failed to create sub-issue on project #{project_number}"
                )
            if isinstance(result, dict) and result.get('error'):
                return build_task_failure_result(self.source, result['error'])

            response_body = {
                'item_id': result.get('item_id'),
                'issue_id': result.get('issue_id'),
                'issue_number': result.get('issue_number'),
                'issue_url': result.get('issue_url'),
                'parent_repo': result.get('parent_repo'),
                'parent_issue_number': result.get('parent_issue_number'),
                'parent_issue_id': result.get('parent_issue_id'),
                'project_number': result.get('project_number'),
                'title': result.get('title'),
                'project_url': result.get('project_url'),
                'status': result.get('status', 'Created'),
            }
            response_struct = dict_to_proto(response_body, Struct)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=ApiResponseResult(response_body=response_struct),
            )
        except Exception as e:
            raise Exception(f"Error while executing Github create_project_sub_issue task: {e}")

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
