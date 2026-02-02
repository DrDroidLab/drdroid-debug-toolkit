import logging

from google.protobuf.struct_pb2 import Struct
from google.protobuf.wrappers_pb2 import StringValue

from core.utils.credentilal_utils import generate_credentials_dict, get_connector_key_type_string, DISPLAY_NAME, CATEGORY, CODE_REPOSITORY
from core.utils.proto_utils import dict_to_proto

from core.integrations.source_api_processors.bitbucket_api_processor import BitbucketAPIProcessor
from core.integrations.source_manager import SourceManager
from core.protos.base_pb2 import TimeRange, Source, SourceKeyType
from core.protos.connectors.connector_pb2 import Connector as ConnectorProto
from core.protos.literal_pb2 import LiteralType, Literal
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResult, PlaybookTaskResultType, ApiResponseResult, \
    TextResult
from core.protos.playbooks.source_task_definitions.bitbucket_task_pb2 import Bitbucket
from core.protos.ui_definition_pb2 import FormField, FormFieldType

logger = logging.getLogger(__name__)


class BitbucketSourceManager(SourceManager):
    def __init__(self):
        self.source = Source.BITBUCKET
        self.task_proto = Bitbucket
        self.task_type_callable_map = {
            Bitbucket.TaskType.FETCH_FILE: {
                'executor': self.fetch_file,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Fetch File',
                'category': 'Code Repository',
                'form_fields': [
                    FormField(key_name=StringValue(value="repo"),
                              display_name=StringValue(value="Repository Slug"),
                              description=StringValue(value="Repository name/slug"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="file_path"),
                              display_name=StringValue(value="File Path"),
                              description=StringValue(value="Path to the file in the repository"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="branch"),
                              display_name=StringValue(value="Branch"),
                              description=StringValue(value="Branch name (defaults to main)"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              default_value=Literal(type=LiteralType.STRING, string=StringValue(value='main')),
                              is_optional=True),
                ],
            },
            Bitbucket.TaskType.UPDATE_FILE: {
                'executor': self.update_file,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Update File',
                'category': 'Code Repository',
                'form_fields': [
                    FormField(key_name=StringValue(value="repo"),
                              display_name=StringValue(value="Repository Slug"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="file_path"),
                              display_name=StringValue(value="File Path"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="content"),
                              display_name=StringValue(value="File Content"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.CODE_EDITOR_FT),
                    FormField(key_name=StringValue(value="commit_message"),
                              display_name=StringValue(value="Commit Message"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="branch"),
                              display_name=StringValue(value="Branch"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              default_value=Literal(type=LiteralType.STRING, string=StringValue(value='main')),
                              is_optional=True),
                    FormField(key_name=StringValue(value="author"),
                              display_name=StringValue(value="Author (optional)"),
                              description=StringValue(value="Format: Name <email>"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                ],
            },
            Bitbucket.TaskType.FETCH_COMMITS: {
                'executor': self.fetch_commits,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Fetch Commits',
                'category': 'Code Repository',
                'form_fields': [
                    FormField(key_name=StringValue(value="repo"),
                              display_name=StringValue(value="Repository Slug"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="branch"),
                              display_name=StringValue(value="Branch"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              default_value=Literal(type=LiteralType.STRING, string=StringValue(value='main'))),
                    FormField(key_name=StringValue(value="author"),
                              display_name=StringValue(value="Author Filter (optional)"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                ],
            },
            Bitbucket.TaskType.FETCH_RELATED_COMMITS: {
                'executor': self.fetch_related_commits,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Fetch Related Commits for a File',
                'category': 'Code Repository',
                'form_fields': [
                    FormField(key_name=StringValue(value="repo"),
                              display_name=StringValue(value="Repository Slug"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="file_path"),
                              display_name=StringValue(value="File Path"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="function_name"),
                              display_name=StringValue(value="Function Name"),
                              description=StringValue(value="Search for commits containing this function name in diff"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="branch"),
                              display_name=StringValue(value="Branch"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              default_value=Literal(type=LiteralType.STRING, string=StringValue(value='main')),
                              is_optional=True),
                ],
            },
            Bitbucket.TaskType.FETCH_RECENT_MERGES: {
                'executor': self.fetch_recent_merges,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Fetch Recent Merges',
                'category': 'Code Repository',
                'form_fields': [
                    FormField(key_name=StringValue(value="repo"),
                              display_name=StringValue(value="Repository Slug"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="branch"),
                              display_name=StringValue(value="Destination Branch"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              default_value=Literal(type=LiteralType.STRING, string=StringValue(value='main'))),
                ],
            },
            Bitbucket.TaskType.CREATE_PULL_REQUEST: {
                'executor': self.create_pull_request,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Create Pull Request',
                'category': 'Code Repository',
                'form_fields': [
                    FormField(key_name=StringValue(value="repo"),
                              display_name=StringValue(value="Repository Slug"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="title"),
                              display_name=StringValue(value="PR Title"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="description"),
                              display_name=StringValue(value="PR Description"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.MULTILINE_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="source_branch"),
                              display_name=StringValue(value="Source Branch"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="destination_branch"),
                              display_name=StringValue(value="Destination Branch"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              default_value=Literal(type=LiteralType.STRING, string=StringValue(value='main')),
                              is_optional=True),
                    FormField(key_name=StringValue(value="close_source_branch"),
                              display_name=StringValue(value="Close Source Branch After Merge"),
                              data_type=LiteralType.BOOL,
                              form_field_type=FormFieldType.CHECKBOX_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="commit_message"),
                              display_name=StringValue(value="Commit Message (for file updates)"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="files"),
                              display_name=StringValue(value="Files to Update (JSON array)"),
                              description=StringValue(value='[{"path": "file.txt", "content": "content"}]'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.CODE_EDITOR_FT,
                              is_optional=True),
                ]
            },
            Bitbucket.TaskType.FETCH_PULL_REQUEST_DIFF: {
                'executor': self.fetch_pull_request_diff,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Fetch Pull Request Diff',
                'category': 'Code Repository',
                'form_fields': [
                    FormField(key_name=StringValue(value="repo"),
                              display_name=StringValue(value="Repository Slug"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="pr_id"),
                              display_name=StringValue(value="Pull Request ID"),
                              data_type=LiteralType.LONG,
                              form_field_type=FormFieldType.TEXT_FT),
                ]
            },
        }

        self.connector_form_configs = [
            {
                "name": StringValue(value="Bitbucket Repository Access Token"),
                "description": StringValue(value="Connect to Bitbucket using a Repository Access Token. Create one in Repository Settings > Access tokens."),
                "form_fields": {
                    SourceKeyType.BITBUCKET_API_KEY: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.BITBUCKET_API_KEY)),
                        display_name=StringValue(value="Repository Access Token"),
                        description=StringValue(value='Repository Access Token from Bitbucket'),
                        helper_text=StringValue(value="Create in Repository Settings > Access tokens"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                        is_sensitive=True
                    ),
                    SourceKeyType.BITBUCKET_WORKSPACE: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.BITBUCKET_WORKSPACE)),
                        display_name=StringValue(value="Workspace"),
                        description=StringValue(value='e.g. "my-workspace"'),
                        helper_text=StringValue(value="Bitbucket workspace slug (found in URL: bitbucket.org/workspace-slug)"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False
                    ),
                    SourceKeyType.BITBUCKET_REPO: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.BITBUCKET_REPO)),
                        display_name=StringValue(value="Repository (Optional)"),
                        description=StringValue(value='e.g. "my-repo"'),
                        helper_text=StringValue(value="Default repository slug. Can be overridden in tasks."),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=True
                    )
                }
            }
        ]

        self.connector_type_details = {
            DISPLAY_NAME: "BITBUCKET",
            CATEGORY: CODE_REPOSITORY,
        }

    def get_connector_processor(self, bitbucket_connector, **kwargs):
        generated_credentials = generate_credentials_dict(bitbucket_connector.type, bitbucket_connector.keys)
        return BitbucketAPIProcessor(**generated_credentials)

    def fetch_file(self, time_range: TimeRange, bitbucket_task: Bitbucket,
                   bitbucket_connector: ConnectorProto):
        try:
            task = bitbucket_task.fetch_file
            repo = task.repo.value if task.HasField('repo') else None
            file_path = task.file_path.value
            branch = task.branch.value if task.HasField('branch') and task.branch.value else "main"
            timestamp = task.timestamp if task.timestamp != 0 else None

            processor = self.get_connector_processor(bitbucket_connector)

            # Use repo from task or fall back to connector default
            if not repo:
                repo = processor.repo
            if not repo:
                raise Exception("Repository is required. Specify in task or connector configuration.")

            if not file_path or not file_path.strip():
                raise Exception("File path is required.")

            file_details = processor.fetch_file(repo, file_path, branch, timestamp)
            if not file_details:
                raise Exception(f"Failed to fetch file {file_path} from repository {repo}")

            response_struct = dict_to_proto(file_details, Struct)
            file_output = ApiResponseResult(response_body=response_struct)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=file_output
            )
        except Exception as e:
            raise Exception(f"Error while executing Bitbucket fetch_file task: {e}")

    def update_file(self, time_range: TimeRange, bitbucket_task: Bitbucket,
                    bitbucket_connector: ConnectorProto):
        try:
            task = bitbucket_task.update_file
            repo = task.repo.value if task.HasField('repo') else None
            file_path = task.file_path.value
            content = task.content.value
            commit_message = task.commit_message.value
            branch = task.branch.value if task.HasField('branch') and task.branch.value else "main"
            author = task.author.value if task.HasField('author') and task.author.value else None

            processor = self.get_connector_processor(bitbucket_connector)

            if not repo:
                repo = processor.repo
            if not repo:
                raise Exception("Repository is required.")

            result = processor.update_file(repo, file_path, content, commit_message, branch, author)
            if not result:
                raise Exception(f"Failed to update file {file_path}")

            response_struct = dict_to_proto(result, Struct)
            file_output = ApiResponseResult(response_body=response_struct)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=file_output
            )
        except Exception as e:
            raise Exception(f"Error while executing Bitbucket update_file task: {e}")

    def fetch_commits(self, time_range: TimeRange, bitbucket_task: Bitbucket,
                      bitbucket_connector: ConnectorProto):
        try:
            task = bitbucket_task.fetch_commits
            repo = task.repo.value if task.HasField('repo') else None
            branch = task.branch.value if task.HasField('branch') and task.branch.value else "main"
            author = task.author.value if task.HasField('author') and task.author.value else None

            processor = self.get_connector_processor(bitbucket_connector)

            if not repo:
                repo = processor.repo
            if not repo:
                raise Exception("Repository is required.")

            commits = processor.get_commits(repo, branch, author)

            if not commits:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"No commits found for repo: {repo}, branch: {branch}")),
                    source=self.source
                )

            # Format commits for response
            formatted_commits = []
            for commit in commits[:50]:  # Limit to 50 commits
                formatted_commits.append({
                    "hash": commit.get("hash"),
                    "message": commit.get("message", "").strip(),
                    "author": commit.get("author", {}).get("raw", ""),
                    "date": commit.get("date"),
                    "url": commit.get("links", {}).get("html", {}).get("href", ""),
                })

            response_struct = dict_to_proto({"commits": formatted_commits}, Struct)
            commit_output = ApiResponseResult(response_body=response_struct)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=commit_output
            )
        except Exception as e:
            raise Exception(f"Error while executing Bitbucket fetch_commits task: {e}")

    def fetch_related_commits(self, time_range: TimeRange, bitbucket_task: Bitbucket,
                              bitbucket_connector: ConnectorProto):
        try:
            task = bitbucket_task.fetch_related_commits
            repo = task.repo.value if task.HasField('repo') else None
            file_path = task.file_path.value
            function_name = task.function_name.value
            branch = task.branch.value if task.HasField('branch') and task.branch.value else "main"

            processor = self.get_connector_processor(bitbucket_connector)

            if not repo:
                repo = processor.repo
            if not repo:
                raise Exception("Repository is required.")

            # Get file history (commits affecting this file)
            file_commits = processor.get_file_history(repo, file_path, branch)

            if not file_commits:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(
                        value=f"No commits found for file: {file_path} in repo: {repo}, branch: {branch}")),
                    source=self.source
                )

            # Filter commits that contain the function name in the diff
            related_commits = []
            for commit_info in file_commits[:20]:  # Check last 20 commits
                commit_hash = commit_info.get("commit", {}).get("hash")
                if not commit_hash:
                    continue

                # Get the diff for this commit
                diff = processor.get_commit_diff(repo, commit_hash)
                if diff and function_name in diff:
                    related_commits.append({
                        "commit_hash": commit_hash,
                        "author": commit_info.get("commit", {}).get("author", {}).get("raw", ""),
                        "date": commit_info.get("commit", {}).get("date"),
                        "message": commit_info.get("commit", {}).get("message", "").strip(),
                        "url": commit_info.get("commit", {}).get("links", {}).get("html", {}).get("href", ""),
                    })

            response_struct = dict_to_proto({"related_commits": related_commits}, Struct)
            commit_output = ApiResponseResult(response_body=response_struct)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=commit_output
            )
        except Exception as e:
            raise Exception(f"Error while executing Bitbucket fetch_related_commits task: {e}")

    def fetch_recent_merges(self, time_range: TimeRange, bitbucket_task: Bitbucket,
                            bitbucket_connector: ConnectorProto):
        try:
            task = bitbucket_task.fetch_recent_merges
            repo = task.repo.value if task.HasField('repo') else None
            branch = task.branch.value if task.HasField('branch') and task.branch.value else "main"

            processor = self.get_connector_processor(bitbucket_connector)

            if not repo:
                repo = processor.repo
            if not repo:
                raise Exception("Repository is required.")

            recent_merges = processor.get_recent_merges(repo, branch)

            response_struct = dict_to_proto({"recent_merges": recent_merges}, Struct)
            merge_output = ApiResponseResult(response_body=response_struct)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=merge_output
            )
        except Exception as e:
            raise Exception(f"Error while executing Bitbucket fetch_recent_merges task: {e}")

    def create_pull_request(self, time_range: TimeRange, bitbucket_task: Bitbucket,
                            bitbucket_connector: ConnectorProto):
        try:
            task = bitbucket_task.create_pull_request
            repo = task.repo.value if task.HasField('repo') else None
            title = task.title.value
            description = task.description.value if task.HasField('description') else ""
            source_branch = task.source_branch.value
            destination_branch = task.destination_branch.value if task.HasField('destination_branch') and task.destination_branch.value else "main"
            close_source_branch = task.close_source_branch.value if task.HasField('close_source_branch') else False
            commit_message = task.commit_message.value if task.HasField('commit_message') else None

            # Parse files to update
            files_to_update = []
            if task.files:
                for file_update in task.files:
                    files_to_update.append({
                        'path': file_update.path.value,
                        'content': file_update.content.value
                    })

            processor = self.get_connector_processor(bitbucket_connector)

            if not repo:
                repo = processor.repo
            if not repo:
                raise Exception("Repository is required.")

            pr_result = processor.create_pull_request(
                repo=repo,
                title=title,
                source_branch=source_branch,
                destination_branch=destination_branch,
                description=description,
                close_source_branch=close_source_branch,
                files_to_update=files_to_update if files_to_update else None,
                commit_message=commit_message
            )

            if not pr_result:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=f"Failed to create PR in repository {repo}")),
                    source=self.source
                )

            response_struct = dict_to_proto({
                "pr_id": pr_result.get("id"),
                "pr_url": pr_result.get("links", {}).get("html", {}).get("href", ""),
                "title": pr_result.get("title"),
                "state": pr_result.get("state"),
            }, Struct)
            pr_output = ApiResponseResult(response_body=response_struct)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=pr_output
            )
        except Exception as e:
            raise Exception(f"Error while executing Bitbucket create_pull_request task: {e}")

    def fetch_pull_request_diff(self, time_range: TimeRange, bitbucket_task: Bitbucket,
                                bitbucket_connector: ConnectorProto):
        try:
            task = bitbucket_task.fetch_pull_request_diff
            repo = task.repo.value if task.HasField('repo') else None
            pr_id = task.pr_id.value

            processor = self.get_connector_processor(bitbucket_connector)

            if not repo:
                repo = processor.repo
            if not repo:
                raise Exception("Repository is required.")

            # Get PR diff (files changed)
            diffstat = processor.get_pr_diffstat(repo, pr_id)

            response_struct = dict_to_proto({"files": diffstat, "pr_id": pr_id}, Struct)
            diff_output = ApiResponseResult(response_body=response_struct)
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=diff_output
            )
        except Exception as e:
            raise Exception(f"Error while executing Bitbucket fetch_pull_request_diff task: {e}")

    @staticmethod
    def validate_connector(connector: ConnectorProto) -> bool:
        if connector.is_proxy_enabled.value:
            return True

        keys = connector.keys
        all_ck_types = [ck.key_type for ck in keys if ck.key.value]
        all_ck_types = list(set(all_ck_types))

        # Required keys for Bitbucket
        required_keys = [SourceKeyType.BITBUCKET_API_KEY, SourceKeyType.BITBUCKET_WORKSPACE]
        return all(key_type in all_ck_types for key_type in required_keys)
