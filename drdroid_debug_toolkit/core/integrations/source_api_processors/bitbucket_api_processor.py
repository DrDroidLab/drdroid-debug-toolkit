import logging
import requests

from datetime import datetime, timezone

from core.integrations.processor import Processor
from core.settings import EXTERNAL_CALL_TIMEOUT

logger = logging.getLogger(__name__)


class BitbucketAPIProcessor(Processor):
    """
    Bitbucket Cloud API Processor using Repository Access Token (Bearer auth).

    Base URL: https://api.bitbucket.org/2.0
    Auth: Bearer token (Repository Access Token)
    """

    BASE_URL = "https://api.bitbucket.org/2.0"

    def __init__(self, api_key, workspace, repo=None):
        self.__api_key = api_key
        self.workspace = workspace
        self.repo = repo

    def _get_headers(self):
        return {
            "Authorization": f"Bearer {self.__api_key}",
            "Accept": "application/json",
        }

    def _paginate(self, url, params=None):
        """Handle Bitbucket pagination using 'next' URL."""
        results = []
        headers = self._get_headers()

        while url:
            try:
                response = requests.get(url, headers=headers, params=params, timeout=EXTERNAL_CALL_TIMEOUT)
                response.raise_for_status()
                data = response.json()
                results.extend(data.get("values", []))
                url = data.get("next")
                params = None  # Only use params for first request
            except Exception as e:
                logger.error(f"BitbucketAPIProcessor._paginate:: Error during pagination: {e}")
                break

        return results

    def test_connection(self):
        """Test connection by fetching user info."""
        try:
            url = f"{self.BASE_URL}/user"
            response = requests.get(url, headers=self._get_headers(), timeout=EXTERNAL_CALL_TIMEOUT)

            if response.status_code == 200:
                return True
            elif response.status_code == 401:
                raise Exception("Authentication failed: Invalid or expired token")
            else:
                raise Exception(f"Bitbucket connection failed: {response.status_code}, {response.text}")
        except Exception as e:
            logger.error(f"BitbucketAPIProcessor.test_connection:: Exception occurred: {e}")
            raise e

    def get_repository_info(self, repo=None):
        """Get repository details."""
        repo = repo or self.repo
        if not repo:
            raise ValueError("Repository name is required")

        try:
            url = f"{self.BASE_URL}/repositories/{self.workspace}/{repo}"
            response = requests.get(url, headers=self._get_headers(), timeout=EXTERNAL_CALL_TIMEOUT)

            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"BitbucketAPIProcessor.get_repository_info:: Error: {response.status_code}, {response.text}")
                return None
        except Exception as e:
            logger.error(f"BitbucketAPIProcessor.get_repository_info:: Exception: {e}")
            return None

    def fetch_file(self, repo, file_path, branch="main", timestamp=None):
        """
        Fetch file content from repository.

        Note: Bitbucket returns raw file content, not base64 encoded like GitHub.
        """
        try:
            # If timestamp provided, get the commit at that time first
            if timestamp:
                commit_sha = self._get_commit_before_timestamp(repo, file_path, timestamp)
                if commit_sha:
                    branch = commit_sha

            url = f"{self.BASE_URL}/repositories/{self.workspace}/{repo}/src/{branch}/{file_path}"
            response = requests.get(url, headers=self._get_headers(), timeout=EXTERNAL_CALL_TIMEOUT)

            if response.status_code == 200:
                # Bitbucket returns raw content, wrap it in a dict for consistency
                return {
                    "content": response.text,
                    "path": file_path,
                    "type": "file",
                }
            else:
                logger.error(f"BitbucketAPIProcessor.fetch_file:: Error fetching {file_path}: {response.status_code}, {response.text}")
                return None
        except Exception as e:
            logger.error(f"BitbucketAPIProcessor.fetch_file:: Exception fetching {file_path}: {e}")
            return None

    def fetch_readme_content(self, repo):
        """Fetch README file content from repository root."""
        readme_file_names = ['README.md', 'README', 'README.txt', 'README.rst', 'readme.md', 'readme']

        for readme_file in readme_file_names:
            try:
                file_data = self.fetch_file(repo, readme_file)
                if file_data and file_data.get('content'):
                    return file_data['content']
            except Exception as e:
                logger.debug(f'Could not fetch {readme_file} for repo {repo}: {e}')
                continue

        return None

    def _get_commit_before_timestamp(self, repo, file_path, timestamp):
        """Find the latest commit affecting the file before the given timestamp."""
        try:
            commits = self.get_file_history(repo, file_path)
            if not commits:
                return None

            commit_search_datetime = datetime.fromtimestamp(timestamp, tz=timezone.utc)

            for commit in commits:
                commit_date_str = commit.get('date', '')
                if commit_date_str:
                    # Bitbucket date format: 2024-01-15T10:30:00+00:00
                    commit_time = datetime.fromisoformat(commit_date_str.replace('Z', '+00:00'))
                    if commit_time <= commit_search_datetime:
                        return commit.get('hash')

            return None
        except Exception as e:
            logger.error(f"BitbucketAPIProcessor._get_commit_before_timestamp:: Error: {e}")
            return None

    def get_file_history(self, repo, file_path, branch="main"):
        """Get commit history for a specific file."""
        try:
            url = f"{self.BASE_URL}/repositories/{self.workspace}/{repo}/filehistory/{branch}/{file_path}"
            return self._paginate(url)
        except Exception as e:
            logger.error(f"BitbucketAPIProcessor.get_file_history:: Exception: {e}")
            return []

    def update_file(self, repo, file_path, content, commit_message, branch="main", author=None):
        """
        Update or create a file in the repository.

        Note: Bitbucket uses multipart/form-data for file updates, not JSON.
        """
        try:
            url = f"{self.BASE_URL}/repositories/{self.workspace}/{repo}/src"

            # Bitbucket uses multipart form data
            files = {
                file_path: (file_path, content),
            }
            data = {
                "message": commit_message,
                "branch": branch,
            }
            if author:
                data["author"] = author

            headers = {"Authorization": f"Bearer {self.__api_key}"}  # No Content-Type for multipart

            response = requests.post(url, headers=headers, files=files, data=data, timeout=EXTERNAL_CALL_TIMEOUT)

            if response.status_code in [200, 201]:
                return {"success": True, "message": "File updated successfully"}
            else:
                logger.error(f"BitbucketAPIProcessor.update_file:: Error: {response.status_code}, {response.text}")
                return None
        except Exception as e:
            logger.error(f"BitbucketAPIProcessor.update_file:: Exception: {e}")
            return None

    def get_commits(self, repo, branch="main", author=None):
        """Get commits for a branch."""
        try:
            url = f"{self.BASE_URL}/repositories/{self.workspace}/{repo}/commits/{branch}"
            params = {}

            commits = self._paginate(url, params)

            # Filter by author if specified
            if author and commits:
                commits = [c for c in commits if author.lower() in c.get('author', {}).get('raw', '').lower()]

            return commits
        except Exception as e:
            logger.error(f"BitbucketAPIProcessor.get_commits:: Exception: {e}")
            return []

    def get_commit(self, repo, commit_sha):
        """Get details of a specific commit."""
        try:
            url = f"{self.BASE_URL}/repositories/{self.workspace}/{repo}/commit/{commit_sha}"
            response = requests.get(url, headers=self._get_headers(), timeout=EXTERNAL_CALL_TIMEOUT)

            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"BitbucketAPIProcessor.get_commit:: Error: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"BitbucketAPIProcessor.get_commit:: Exception: {e}")
            return None

    def get_commit_diff(self, repo, commit_sha):
        """Get the diff for a commit."""
        try:
            url = f"{self.BASE_URL}/repositories/{self.workspace}/{repo}/diff/{commit_sha}"
            response = requests.get(url, headers=self._get_headers(), timeout=EXTERNAL_CALL_TIMEOUT)

            if response.status_code == 200:
                return response.text  # Diff is returned as plain text
            else:
                logger.error(f"BitbucketAPIProcessor.get_commit_diff:: Error: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"BitbucketAPIProcessor.get_commit_diff:: Exception: {e}")
            return None

    def get_commit_diffstat(self, repo, commit_sha):
        """Get the diffstat (changed files) for a commit."""
        try:
            url = f"{self.BASE_URL}/repositories/{self.workspace}/{repo}/diffstat/{commit_sha}"
            diffstat = self._paginate(url)

            # Normalize to GitHub-like format
            files = []
            for item in diffstat:
                files.append({
                    "filename": item.get("new", {}).get("path") or item.get("old", {}).get("path"),
                    "status": item.get("status"),
                    "lines_added": item.get("lines_added", 0),
                    "lines_removed": item.get("lines_removed", 0),
                })
            return files
        except Exception as e:
            logger.error(f"BitbucketAPIProcessor.get_commit_diffstat:: Exception: {e}")
            return []

    def list_branches(self, repo):
        """List all branches in the repository."""
        try:
            url = f"{self.BASE_URL}/repositories/{self.workspace}/{repo}/refs/branches"
            return self._paginate(url)
        except Exception as e:
            logger.error(f"BitbucketAPIProcessor.list_branches:: Exception: {e}")
            return []

    def get_branch(self, repo, branch_name):
        """Get details of a specific branch."""
        try:
            url = f"{self.BASE_URL}/repositories/{self.workspace}/{repo}/refs/branches/{branch_name}"
            response = requests.get(url, headers=self._get_headers(), timeout=EXTERNAL_CALL_TIMEOUT)

            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"BitbucketAPIProcessor.get_branch:: Error: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"BitbucketAPIProcessor.get_branch:: Exception: {e}")
            return None

    def create_branch(self, repo, branch_name, target_branch="main"):
        """Create a new branch from target branch."""
        try:
            # Get the target branch to find its commit hash
            target = self.get_branch(repo, target_branch)
            if not target:
                raise Exception(f"Target branch {target_branch} not found")

            target_hash = target.get("target", {}).get("hash")
            if not target_hash:
                raise Exception(f"Could not get hash for branch {target_branch}")

            url = f"{self.BASE_URL}/repositories/{self.workspace}/{repo}/refs/branches"
            payload = {
                "name": branch_name,
                "target": {"hash": target_hash}
            }

            headers = self._get_headers()
            headers["Content-Type"] = "application/json"

            response = requests.post(url, headers=headers, json=payload, timeout=EXTERNAL_CALL_TIMEOUT)

            if response.status_code in [200, 201]:
                return response.json()
            else:
                logger.error(f"BitbucketAPIProcessor.create_branch:: Error: {response.status_code}, {response.text}")
                return None
        except Exception as e:
            logger.error(f"BitbucketAPIProcessor.create_branch:: Exception: {e}")
            return None

    def list_pull_requests(self, repo, state="OPEN"):
        """
        List pull requests.

        state: OPEN, MERGED, DECLINED, SUPERSEDED
        """
        try:
            url = f"{self.BASE_URL}/repositories/{self.workspace}/{repo}/pullrequests"
            params = {"state": state}
            return self._paginate(url, params)
        except Exception as e:
            logger.error(f"BitbucketAPIProcessor.list_pull_requests:: Exception: {e}")
            return []

    def get_pull_request(self, repo, pr_id):
        """Get details of a specific pull request."""
        try:
            url = f"{self.BASE_URL}/repositories/{self.workspace}/{repo}/pullrequests/{pr_id}"
            response = requests.get(url, headers=self._get_headers(), timeout=EXTERNAL_CALL_TIMEOUT)

            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"BitbucketAPIProcessor.get_pull_request:: Error: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"BitbucketAPIProcessor.get_pull_request:: Exception: {e}")
            return None

    def get_pr_diffstat(self, repo, pr_id):
        """Get the diffstat (changed files) for a pull request."""
        try:
            url = f"{self.BASE_URL}/repositories/{self.workspace}/{repo}/pullrequests/{pr_id}/diffstat"
            diffstat = self._paginate(url)

            # Normalize to GitHub-like format
            files = []
            for item in diffstat:
                files.append({
                    "filename": item.get("new", {}).get("path") or item.get("old", {}).get("path"),
                    "status": item.get("status"),
                    "lines_added": item.get("lines_added", 0),
                    "lines_removed": item.get("lines_removed", 0),
                })
            return files
        except Exception as e:
            logger.error(f"BitbucketAPIProcessor.get_pr_diffstat:: Exception: {e}")
            return []

    def get_pr_comments(self, repo, pr_id):
        """Get comments on a pull request."""
        try:
            url = f"{self.BASE_URL}/repositories/{self.workspace}/{repo}/pullrequests/{pr_id}/comments"
            return self._paginate(url)
        except Exception as e:
            logger.error(f"BitbucketAPIProcessor.get_pr_comments:: Exception: {e}")
            return []

    def create_pull_request(self, repo, title, source_branch, destination_branch, description="",
                           close_source_branch=False, files_to_update=None, commit_message=None):
        """
        Create a pull request.

        If files_to_update is provided, commits those files first.
        """
        try:
            # If files to update, commit them first
            if files_to_update and commit_message:
                for file_update in files_to_update:
                    result = self.update_file(
                        repo=repo,
                        file_path=file_update.get('path'),
                        content=file_update.get('content'),
                        commit_message=commit_message,
                        branch=source_branch
                    )
                    if not result:
                        raise Exception(f"Failed to update file: {file_update.get('path')}")

            url = f"{self.BASE_URL}/repositories/{self.workspace}/{repo}/pullrequests"
            payload = {
                "title": title,
                "description": description,
                "source": {
                    "branch": {"name": source_branch}
                },
                "destination": {
                    "branch": {"name": destination_branch}
                },
                "close_source_branch": close_source_branch
            }

            headers = self._get_headers()
            headers["Content-Type"] = "application/json"

            response = requests.post(url, headers=headers, json=payload, timeout=EXTERNAL_CALL_TIMEOUT)

            if response.status_code in [200, 201]:
                return response.json()
            else:
                logger.error(f"BitbucketAPIProcessor.create_pull_request:: Error: {response.status_code}, {response.text}")
                return None
        except Exception as e:
            logger.error(f"BitbucketAPIProcessor.create_pull_request:: Exception: {e}")
            return None

    def merge_pull_request(self, repo, pr_id, merge_strategy="merge_commit"):
        """
        Merge a pull request.

        merge_strategy: merge_commit, squash, fast_forward
        """
        try:
            url = f"{self.BASE_URL}/repositories/{self.workspace}/{repo}/pullrequests/{pr_id}/merge"
            payload = {
                "merge_strategy": merge_strategy
            }

            headers = self._get_headers()
            headers["Content-Type"] = "application/json"

            response = requests.post(url, headers=headers, json=payload, timeout=EXTERNAL_CALL_TIMEOUT)

            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"BitbucketAPIProcessor.merge_pull_request:: Error: {response.status_code}, {response.text}")
                return None
        except Exception as e:
            logger.error(f"BitbucketAPIProcessor.merge_pull_request:: Exception: {e}")
            return None

    def get_recent_merges(self, repo, branch="main"):
        """Get recently merged pull requests to a branch."""
        try:
            prs = self.list_pull_requests(repo, state="MERGED")

            # Filter by destination branch and sort by merge time
            merged_prs = []
            for pr in prs:
                dest_branch = pr.get("destination", {}).get("branch", {}).get("name")
                if dest_branch == branch:
                    merged_prs.append({
                        "id": pr.get("id"),
                        "title": pr.get("title"),
                        "author": pr.get("author", {}).get("display_name"),
                        "source_branch": pr.get("source", {}).get("branch", {}).get("name"),
                        "destination_branch": dest_branch,
                        "merged_at": pr.get("updated_on"),
                        "url": pr.get("links", {}).get("html", {}).get("href"),
                    })

            # Sort by merge time (most recent first)
            merged_prs.sort(key=lambda x: x.get("merged_at", ""), reverse=True)

            return merged_prs
        except Exception as e:
            logger.error(f"BitbucketAPIProcessor.get_recent_merges:: Exception: {e}")
            return []

    def list_tags(self, repo):
        """List all tags in the repository."""
        try:
            url = f"{self.BASE_URL}/repositories/{self.workspace}/{repo}/refs/tags"
            return self._paginate(url)
        except Exception as e:
            logger.error(f"BitbucketAPIProcessor.list_tags:: Exception: {e}")
            return []

    def list_repositories(self):
        """List all repositories in the workspace."""
        try:
            url = f"{self.BASE_URL}/repositories/{self.workspace}"
            return self._paginate(url)
        except Exception as e:
            logger.error(f"BitbucketAPIProcessor.list_repositories:: Exception: {e}")
            return []

    def list_workspace_members(self):
        """List all members in the workspace."""
        try:
            url = f"{self.BASE_URL}/workspaces/{self.workspace}/members"
            return self._paginate(url)
        except Exception as e:
            logger.error(f"BitbucketAPIProcessor.list_workspace_members:: Exception: {e}")
            return []

    def get_workspace_info(self):
        """Get workspace information."""
        try:
            url = f"{self.BASE_URL}/workspaces/{self.workspace}"
            response = requests.get(url, headers=self._get_headers(), timeout=EXTERNAL_CALL_TIMEOUT)

            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"BitbucketAPIProcessor.get_workspace_info:: Error: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"BitbucketAPIProcessor.get_workspace_info:: Exception: {e}")
            return None
