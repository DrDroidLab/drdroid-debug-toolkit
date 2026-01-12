import logging
from datetime import datetime, timedelta, timezone
from sqlite3 import ProgrammingError

import boto3
import requests
from botocore.exceptions import ClientError
from django.conf import settings

from core.integrations.processor import Processor
from core.utils.time_utils import current_milli_time
from core.settings import EXTERNAL_CALL_TIMEOUT

logger = logging.getLogger(__name__)


def generate_aws_access_secret_session_key(aws_assumed_role_arn, aws_drd_cloud_role_arn):
    aws_access_key_id = getattr(settings, 'AWS_ACCESS_KEY_ID', None)
    aws_secret_access_key = getattr(settings, 'AWS_SECRET_ACCESS_KEY', None)
    aws_region = getattr(settings, 'AWS_REGION', None)
    default_session = boto3.setup_default_session(aws_access_key_id=aws_access_key_id,
                                                  aws_secret_access_key=aws_secret_access_key,
                                                  region_name=aws_region)
    uuid = str(current_milli_time())
    role_session_name = "drd_session" + uuid
    sts_client = boto3.client('sts', aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key, region_name=aws_region)

    assumed_role = sts_client.assume_role(
        RoleArn=aws_drd_cloud_role_arn,
        RoleSessionName=role_session_name
    )

    aws_access_key_id = assumed_role['Credentials']['AccessKeyId']
    aws_secret_access_key = assumed_role['Credentials']['SecretAccessKey']
    aws_session_token = assumed_role['Credentials']['SessionToken']
    region_name = 'us-west-2'

    assumed_client = boto3.client('sts', aws_access_key_id=aws_access_key_id,
                                  aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token,
                                  region_name=region_name)

    # Assume the Prodigal role
    assumed_role_2 = assumed_client.assume_role(
        RoleArn=aws_assumed_role_arn,
        RoleSessionName="client_session"
    )

    return {'aws_access_key': assumed_role_2['Credentials']['AccessKeyId'],
            'aws_secret_key': assumed_role_2['Credentials']['SecretAccessKey'],
            'aws_session_token': assumed_role_2['Credentials']['SessionToken']}


class AWSBoto3ApiProcessor(Processor):
    def __init__(self, client_type, region, aws_access_key=None, aws_secret_key=None, aws_assumed_role_arn=None,
                 aws_drd_cloud_role_arn=None):

        if (not aws_access_key or not aws_secret_key) and not (aws_assumed_role_arn or not aws_drd_cloud_role_arn):
            raise Exception("Received invalid AWS Credentials")

        self.client_type = client_type
        self.__aws_access_key = aws_access_key
        self.__aws_secret_key = aws_secret_key
        self.region = region
        self.__aws_session_token = None
        if aws_assumed_role_arn:
            credentials = generate_aws_access_secret_session_key(aws_assumed_role_arn, aws_drd_cloud_role_arn)
            self.__aws_access_key = credentials['aws_access_key']
            self.__aws_secret_key = credentials['aws_secret_key']
            self.__aws_session_token = credentials['aws_session_token']

    def get_connection(self):
        try:
            client = boto3.client(self.client_type, aws_access_key_id=self.__aws_access_key,
                                  aws_secret_access_key=self.__aws_secret_key, region_name=self.region,
                                  aws_session_token=self.__aws_session_token)
            return client
        except Exception as e:
            logger.error(f"Exception occurred while creating boto3 client with error: {e}")
            raise e

    def test_cloudwatch_list_metrics_permission(self):
        """Tests permission to list CloudWatch metrics."""
        try:
            client = self.get_connection()
            # Basic list_metrics call for permission check
            client.list_metrics()
            return True
        except Exception as e:
            logger.warning(f"CloudWatch list_metrics permission check failed: {e}")
            raise e

    def test_logs_describe_log_groups_permission(self):
        """Tests permission to describe CloudWatch log groups."""
        try:
            logger.info(f"Testing CloudWatch Logs describe_log_groups permission for region: {self.region}")
            client = self.get_connection()
            client.describe_log_groups(limit=1)
            return True
        except Exception as e:
            logger.warning(f"CloudWatch Logs describe_log_groups permission check failed: {e}")
            raise e

    def test_logs_start_query_permission(self):
        """Tests permission to start a CloudWatch Logs Insights query (logs:StartQuery)."""
        try:
            logger.info(f"Testing CloudWatch Logs start_query permission for region: {self.region}")
            client = self.get_connection()

            # Use a non-existent log group and a simple query to avoid reading real data.
            end_time = int(datetime.now(timezone.utc).timestamp())
            start_time = end_time - 300  # last 5 minutes

            try:
                start_query_response = client.start_query(
                    logGroupName='test-log-group',
                    startTime=start_time,
                    endTime=end_time,
                    queryString='fields @timestamp | limit 1',
                )

                # Best-effort: immediately stop the query if it was started successfully.
                logger.info(f"CloudWatch Logs StartQuery permission check passed: {start_query_response}")
                query_id = start_query_response.get('queryId')
                if query_id:
                    try:
                        client.stop_query(queryId=query_id)
                    except Exception:
                        raise e

                return True

            except client.exceptions.ClientError as ce:
                error_code = ce.response.get('Error', {}).get('Code')
                http_status_code = ce.response.get('ResponseMetadata', {}).get('HTTPStatusCode')
                message_lower = str(ce).lower()

                # Resource not found means permissions are likely fine but the test resource doesn't exist.
                if (
                    error_code in ['ResourceNotFoundException', 'ResourceNotFound']
                    or http_status_code == 404
                    or 'resource not found' in message_lower
                ):
                    logger.info(
                        "CloudWatch Logs StartQuery permission check passed "
                        "(ignoring resource not found: "
                        f"{error_code or http_status_code} - "
                        f"{ce.response.get('Error', {}).get('Message', str(ce))[:100]}...)"
                    )
                    return True

                # Explicit access denied errors should fail the permission check.
                if error_code in ['AccessDeniedException', 'AccessDenied'] or 'accessdenied' in message_lower or 'forbidden' in message_lower:
                    logger.warning(f"CloudWatch Logs StartQuery permission check failed due to access denied/forbidden: {ce}")
                    raise ce

                # Any other client error is unexpected, surface it.
                logger.warning(f"CloudWatch Logs StartQuery permission check failed with unexpected ClientError: {ce}")
                raise ce

        except Exception as e:
            logger.warning(f"CloudWatch Logs StartQuery permission check failed with unexpected error: {e}")
            raise e

    def test_ecs_list_clusters_permission(self):
        """Tests permission to list ECS clusters."""
        try:
            client = self.get_connection()
            # list_clusters doesn't have a simple 'limit', use maxResults
            client.list_clusters(maxResults=1)
            return True
        except Exception as e:
            logger.warning(f"ECS list_clusters permission check failed: {e}")
            raise e

    def test_cloudwatch_list_dashboards_permission(self):
        """Tests permission to list CloudWatch dashboards."""
        try:
            client = self.get_connection()
            client.list_dashboards()
            return True
        except Exception as e:
            logger.warning(f"CloudWatch list_dashboards permission check failed: {e}")
            raise e

    def test_s3_get_object_permission(self):
        """Tests permission to get objects from S3 (s3:GetObject)."""
        try:
            # Use head_object as a lightweight way to check GetObject permission
            client = self.get_connection()
            client.head_object(Bucket='drd-permission-test-bucket-non-existent', Key='permission-test-key')

            return True
        except client.exceptions.ClientError as ce:
            error_code = ce.response.get('Error', {}).get('Code')
            http_status_code = ce.response.get('ResponseMetadata', {}).get('HTTPStatusCode')

            # this means we couldn't test the permission because the object isn't there,
            # not that the permission itself is denied.
            if error_code in ['NoSuchBucket', 'NoSuchKey'] or http_status_code == 404:
                logger.info(
                    f"S3 GetObject permission check passed (ignoring resource not found: "
                    f"{error_code or 'HTTP 404'} - "
                    f"{ce.response.get('Error', {}).get('Message', str(ce))[:100]}...)"
                )
                return True

            # Check for explicit 'AccessDenied' error code or messages containing 'AccessDenied'/'Forbidden'.
            elif error_code == 'AccessDenied' or \
                    'AccessDenied' in str(ce) or \
                    'Forbidden' in str(ce):
                logger.warning(f"S3 GetObject permission check failed due to AccessDenied/Forbidden: {ce}")
                raise ce
            else:
                # Log and re-raise other unexpected client errors.
                logger.warning(f"S3 GetObject permission check failed with unexpected ClientError: {ce}")
                raise ce
        except Exception as e:
            # Catch any other unexpected errors during client operation.
            logger.warning(f"S3 GetObject permission check failed with unexpected error: {e}")
            raise e

    def test_pi_describe_dimension_keys_permission(self):
        """Tests permission to describe dimension keys in Performance Insights (pi:DescribeDimensionKeys)."""
        try:
            client = self.get_connection()
            end_time = datetime.now()
            start_time = end_time - timedelta(minutes=5)

            # Use a mock resource uri format - for permission check only
            mock_resource_uri = "db-FAIHNTYBKTGAUSUZQYPDS2GW4A"

            # Make a lightweight call with minimal parameters
            client.describe_dimension_keys(
                ServiceType='RDS',
                Identifier=mock_resource_uri,
                StartTime=start_time,
                EndTime=end_time,
                Metric='db.load.avg',
                GroupBy={'Group': 'db'}
            )
            return True
        except client.exceptions.ClientError as ce:
            error_code = ce.response.get('Error', {}).get('Code')
            error_message = str(ce).lower()

            # If we get a ValidationException, that means our credentials were accepted
            # but the resource format is invalid, which is expected with our mock ARN
            if error_code == 'ValidationException' or 'validation' in error_message:
                logger.info(
                    "PI DescribeDimensionKeys permission check passed (validation error is expected with mock ARN)")
                return True

            # If the error is ResourceNotFound, the permission is granted but the resource doesn't exist
            if error_code == 'InvalidIdentifier' or 'ResourceNotFound' in error_message:
                logger.info("PI DescribeDimensionKeys permission check passed (resource not found is expected)")
                return True

            # Check for explicit access denied errors
            elif error_code == 'AccessDeniedException' or 'access denied' in error_message or 'forbidden' in error_message:
                logger.warning(f"PI DescribeDimensionKeys permission check failed due to access denied: {ce}")
                raise ce

            else:
                # Log and re-raise other unexpected client errors
                logger.warning(f"PI DescribeDimensionKeys permission check failed with unexpected ClientError: {ce}")
                raise ce
        except Exception as e:
            # Catch any other unexpected errors during client operation
            logger.warning(f"PI DescribeDimensionKeys permission check failed with unexpected error: {e}")
            raise e

    def cloudwatch_describe_alarms(self, alarm_names):
        try:
            client = self.get_connection()
            response = client.describe_alarms(AlarmNames=alarm_names)
            if response['ResponseMetadata']['HTTPStatusCode'] == 200 and response['MetricAlarms']:
                return response['MetricAlarms']
            else:
                print(f"No alarms found for '{alarm_names}'")
        except Exception as e:
            logger.error(
                f"Exception occurred while fetching cloudwatch alarms for alarm_names: {alarm_names} with error: {e}")
            raise e

    def cloudwatch_describe_all_alarms(self):
        try:
            client = self.get_connection()
            response = client.describe_alarms()

            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                return {
                    'MetricAlarms': response.get('MetricAlarms', []),
                    'CompositeAlarms': response.get('CompositeAlarms', [])
                }
            else:
                logger.warning(
                    f"Unexpected response when fetching all CloudWatch alarms. Status code: {response['ResponseMetadata']['HTTPStatusCode']}")
                return {'MetricAlarms': [], 'CompositeAlarms': []}
        except Exception as e:
            logger.error(f"Exception occurred while fetching all CloudWatch alarms. Error: {e}")
            raise e

    def cloudwatch_list_metrics(self, namespace, token=None):
        try:
            client = self.get_connection()
            if token:
                metrics = client.list_metrics(NextToken=token, Namespace=namespace)
            else:
                metrics = client.list_metrics(Namespace=namespace)
            return metrics
        except Exception as e:
            logger.error(f"Exception occurred while fetching cloudwatch metrics with error: {e}")
            raise e

    def cloudwatch_get_metric_statistics(self, namespace, metric, start_time, end_time, period, statistics, dimensions):
        try:
            client = self.get_connection()
            response = client.get_metric_statistics(
                Namespace=namespace,
                MetricName=metric,
                StartTime=start_time,
                EndTime=end_time,
                Period=period,
                Statistics=statistics,
                Dimensions=dimensions
            )
            return response
        except Exception as e:
            logger.error(
                f"Exception occurred while fetching cloudwatch metric statistics for metric: {metric} with error: {e}")
            raise e

    def logs_describe_log_groups(self):
        try:
            client = self.get_connection()
            paginator = client.get_paginator('describe_log_groups')
            log_groups = []
            for page in paginator.paginate():
                for log_group in page['logGroups']:
                    log_groups.append(log_group['logGroupName'])
            return log_groups
        except Exception as e:
            logger.error(f"Exception occurred while fetching log groups with error: {e}")
            raise e

    def logs_describe_log_group_queries(self):
        try:
            client = self.get_connection()
            paginator = client.get_paginator('describe_queries')
            all_queries = []

            for page in paginator.paginate():
                all_queries.extend(page['queries'])

            return all_queries
        except Exception as e:
            logger.error(f"Exception occurred while fetching log queries with error: {e}")
            raise e

    def logs_filter_events(self, log_group, query_pattern, start_time, end_time):
        try:
            client = self.get_connection()
            start_query_response = client.start_query(
                logGroupName=log_group,
                startTime=start_time,
                endTime=end_time,
                queryString=query_pattern,
            )

            query_id = start_query_response['queryId']

            status = 'Running'
            query_start_time = current_milli_time()
            while status == 'Running' or status == 'Scheduled':
                print("Waiting for query to complete...")
                response = client.get_query_results(queryId=query_id)
                status = response['status']
                if status == 'Complete':
                    return response['results']
                elif current_milli_time() - query_start_time > 60000:
                    print("Query took too long to complete. Aborting...")
                    stop_query_response = client.stop_query(queryId=query_id)
                    print(f"Query stopped with response: {stop_query_response}")
                    return None
            return None
        except Exception as e:
            logger.error(f"Exception occurred while fetching logs for log_group: {log_group} with error: {e}")
            raise e

    def rds_describe_instances(self, db_instance_identifier=None):
        try:
            client = self.get_connection()
            if db_instance_identifier:
                rds_instances = client.describe_db_instances(DBInstanceIdentifier=db_instance_identifier)
            else:
                rds_instances = client.describe_db_instances()
            return rds_instances
        except Exception as e:
            logger.error(f"Exception occurred while fetching RDS instances with error: {e}")
            raise e

    def pi_describe_db_dimension_keys(self, resource_uri, performance_insights_metric='db.load.avg'):
        end_time = datetime.now()
        start_time = end_time - timedelta(days=1)
        client = self.get_connection()
        response = client.describe_dimension_keys(
            ServiceType='RDS',
            Identifier=resource_uri,
            Metric=performance_insights_metric,
            StartTime=start_time,
            EndTime=end_time,
            GroupBy={
                'Group': 'db'
            }
        )
        keys = response['Keys']
        all_db_names = []
        for k in keys:
            if 'Dimensions' in k:
                for key, value in k['Dimensions'].items():
                    if key == 'db.name':
                        all_db_names.append(value)
        return list(set(all_db_names))

    def pi_get_long_running_queries(self, resource_uri, query_end_time, start_minutes_ago=60, service_type='RDS'):
        end_time = query_end_time if query_end_time else datetime.now()
        start_time = end_time - timedelta(minutes=start_minutes_ago)
        client = self.get_connection()
        try:
            response = client.describe_dimension_keys(
                ServiceType=service_type,
                Identifier=resource_uri,
                StartTime=start_time,
                EndTime=end_time,
                Metric='db.load.avg',
                PeriodInSeconds=300,
                GroupBy={
                    "Dimensions": ["db.sql_tokenized.id", "db.sql_tokenized.statement"],
                    "Group": "db.sql_tokenized",
                },
                AdditionalMetrics=[
                    "db.sql_tokenized.stats.sum_timer_wait_per_call.avg",
                    "db.sql_tokenized.stats.sum_rows_examined_per_call.avg",
                ],
            )
            query_data = []
            for dimension in response.get('Keys', []):
                sql = dimension.get('Dimensions', {}).get('db.sql_tokenized.statement', 'Unknown')
                additional_metrics = dimension.get('AdditionalMetrics', {})
                total_rows_examined = additional_metrics.get('db.sql_tokenized.stats.sum_rows_examined_per_call.avg', 0)
                total_wait_time_ms = additional_metrics.get('db.sql_tokenized.stats.sum_timer_wait_per_call.avg', 0)
                query_data.append({
                    'sql': sql,
                    'total_query_time_ms': total_wait_time_ms,
                    'total_rows_examined': total_rows_examined,
                })
            query_data.sort(key=lambda x: x['total_query_time_ms'], reverse=True)
            return query_data
        except Exception as e:
            print(f"Error fetching long-running queries: {e}")
        return []

    def pi_list_dimension_details(self, resource_uri, metric_type='db.sql_tokenized.stats'):
        try:
            client = self.get_connection()
            response = client.list_available_resource_metrics(
                ServiceType='RDS',
                Identifier=resource_uri,
                MetricTypes=[
                    metric_type,
                ],
            )
            return response
        except Exception as e:
            logger.error(f"Exception occurred while fetching PI dimension details with error: {e}")
            raise e

    # CloudWatch Dashboard Methods
    def cloudwatch_list_dashboards(self):
        """List all CloudWatch dashboards in the configured region."""
        try:
            client = self.get_connection()
            dashboards = []
            paginator = client.get_paginator('list_dashboards')

            for page in paginator.paginate():
                if 'DashboardEntries' in page:
                    dashboards.extend(page['DashboardEntries'])

            return dashboards
        except Exception as e:
            logger.error(f"Exception occurred while listing CloudWatch dashboards with error: {e}")
            return []

    def cloudwatch_get_dashboard(self, dashboard_name: str):
        """Get details of a specific CloudWatch dashboard by name."""
        try:
            client = self.get_connection()
            response = client.get_dashboard(DashboardName=dashboard_name)
            return response
        except Exception as e:
            logger.error(f"Exception occurred while getting CloudWatch dashboard '{dashboard_name}': {e}")
            return None

    # ECS Methods
    def list_all_clusters(self):
        """List all ECS clusters in the account."""
        try:
            client = self.get_connection()
            clusters = []
            paginator = client.get_paginator('list_clusters')

            for page in paginator.paginate():
                if 'clusterArns' in page:
                    clusters.extend(page['clusterArns'])

            # Get detailed information for each cluster
            if clusters:
                response = client.describe_clusters(clusters=clusters)
                return response.get('clusters', [])

            return []
        except Exception as e:
            logger.error(f"Exception occurred while listing ECS clusters with error: {e}")
            return None

    def list_tasks_in_cluster(self, cluster_arn):
        """List all tasks in a specific ECS cluster, including both running and stopped tasks."""
        try:
            client = self.get_connection()
            all_tasks = []

            # Get RUNNING tasks
            running_tasks = []
            paginator = client.get_paginator('list_tasks')
            for page in paginator.paginate(cluster=cluster_arn, desiredStatus='RUNNING'):
                if 'taskArns' in page and page['taskArns']:
                    running_tasks.extend(page['taskArns'])

            # Get STOPPED tasks
            stopped_tasks = []
            for page in paginator.paginate(cluster=cluster_arn, desiredStatus='STOPPED'):
                if 'taskArns' in page and page['taskArns']:
                    stopped_tasks.extend(page['taskArns'])

            # Combine all task ARNs
            all_tasks = running_tasks + stopped_tasks

            # Get detailed information for each task
            if all_tasks:
                # ECS API limits the number of tasks in a single describe_tasks call
                # Process in batches of 100
                task_details = []
                for i in range(0, len(all_tasks), 100):
                    batch = all_tasks[i:i + 100]
                    response = client.describe_tasks(
                        cluster=cluster_arn,
                        tasks=batch
                    )
                    task_details.extend(response.get('tasks', []))

                return task_details

            return []
        except Exception as e:
            logger.error(f"Exception occurred while listing tasks in ECS cluster: {cluster_arn} with error: {e}")
            return None

    def get_task_definition(self, task_definition_arn):
        """Get details of a specific task definition."""
        try:
            client = self.get_connection()
            response = client.describe_task_definition(
                taskDefinition=task_definition_arn
            )
            return response.get('taskDefinition')
        except Exception as e:
            logger.error(f"Exception occurred while getting ECS task definition: {task_definition_arn} with error: {e}")
            return None

    def list_services_in_cluster(self, cluster_arn):
        """List all services in a specific ECS cluster."""
        try:
            client = self.get_connection()
            services = []
            paginator = client.get_paginator('list_services')

            for page in paginator.paginate(cluster=cluster_arn):
                if 'serviceArns' in page and page['serviceArns']:
                    service_arns = page['serviceArns']
                    # Get detailed information for each service
                    response = client.describe_services(
                        cluster=cluster_arn,
                        services=service_arns
                    )
                    services.extend(response.get('services', []))

            return services
        except Exception as e:
            logger.error(f"Exception occurred while listing services in ECS cluster: {cluster_arn} with error: {e}")
            return None

    def get_task_definitions_map(self, cluster_name):
        """
        Get a mapping of task IDs to their task definitions for a specific cluster.
        
        Args:
            cluster_name (str): The name of the ECS cluster
            
        Returns:
            dict: A dictionary mapping task IDs to their task definition names
        """
        try:
            client = self.get_connection()
            task_def_map = {}

            # Get all task ARNs in the cluster (both running and stopped)
            running_task_arns = client.list_tasks(cluster=cluster_name, desiredStatus="RUNNING").get("taskArns", [])
            stopped_tasks_arns = client.list_tasks(cluster=cluster_name, desiredStatus="STOPPED").get("taskArns", [])
            task_arns = running_task_arns + stopped_tasks_arns

            if not task_arns:
                return {}

            # Process tasks in batches of 100 (AWS API limit)
            for i in range(0, len(task_arns), 100):
                batch = task_arns[i:i + 100]
                task_details = client.describe_tasks(cluster=cluster_name, tasks=batch)

                for task_info in task_details.get("tasks", []):
                    task_id = task_info.get("taskArn").split("/")[-1]
                    task_def_arn = task_info.get("taskDefinitionArn")
                    # Extract task definition name and revision (e.g., "Vidushee-Experiment:4")
                    task_def_name = task_def_arn.split("/")[-1] if task_def_arn else "unknown"
                    task_def_map[task_id] = task_def_name

            return task_def_map

        except Exception as e:
            logger.error(
                f"Exception occurred while getting task definitions map for cluster: {cluster_name} with error: {e}")
            return {}

    def get_task_logs(self, cluster_name, max_lines=100):
        """
        Fetch logs for all running tasks in an ECS cluster.
        
        Args:
            cluster_name (str): ECS cluster name.
            task_definition_name (str): Task definition (format: "name:revision").
            max_lines (int, optional): Number of log lines to return (default: 100).
        
        Returns:
            dict: Container-wise logs for all tasks.
        """
        try:
            client = self.get_connection()
            logs_client = boto3.client('logs',
                                       aws_access_key_id=self.__aws_access_key,
                                       aws_secret_access_key=self.__aws_secret_key,
                                       region_name=self.region,
                                       aws_session_token=self.__aws_session_token)

            # Get all running and stopped tasks in the cluster
            running_task_arns = client.list_tasks(cluster=cluster_name, desiredStatus="RUNNING").get("taskArns", [])
            stopped_tasks_arns = client.list_tasks(cluster=cluster_name, desiredStatus="STOPPED").get("taskArns", [])
            task_arns = running_task_arns + stopped_tasks_arns

            if not task_arns:
                return {"error": f"No running tasks found in cluster: {cluster_name}"}

            # Step 2: Get details of all running tasks
            task_details = client.describe_tasks(cluster=cluster_name, tasks=task_arns)
            if not task_details.get("tasks"):
                return {"error": "Could not retrieve task details."}

            logs_by_task = {}

            # Step 3: Iterate through each running task
            for task_info in task_details["tasks"]:
                task_arn = task_info.get("taskArn")
                task_id = task_arn.split("/")[-1]
                task_definition_arn = task_info.get("taskDefinitionArn")

                # Get task definition to fetch log configuration
                task_def_response = client.describe_task_definition(taskDefinition=task_definition_arn)
                container_definitions = task_def_response["taskDefinition"]["containerDefinitions"]

                logs_by_task[task_id] = {}

                for container_def in container_definitions:
                    container_name = container_def["name"]
                    log_config = container_def.get("logConfiguration", {})

                    if not log_config or log_config.get("logDriver") != "awslogs":
                        logs_by_task[task_id][container_name] = {
                            "error": "Logs not configured to use CloudWatch. Check ECS task definition."
                        }
                        continue

                    # Extract CloudWatch log configuration
                    log_group = log_config["options"].get("awslogs-group")
                    log_stream_prefix = log_config["options"].get("awslogs-stream-prefix")

                    if not log_group or not log_stream_prefix:
                        logs_by_task[task_id][container_name] = {"error": "Missing log group or stream prefix."}
                        continue

                    # Construct log stream name variations
                    log_stream_variations = [
                        f"{log_stream_prefix}/{container_name}/{task_id}",
                        f"{log_stream_prefix}/{task_id}/{container_name}",
                        f"{container_name}/{task_id}"
                    ]

                    # Fetch logs from CloudWatch
                    for stream_name in log_stream_variations:
                        try:
                            # Calculate timestamps for the last 2 weeks
                            end_time = int(datetime.now().timestamp() * 1000)
                            start_time = int((datetime.now() - timedelta(weeks=2)).timestamp() * 1000)

                            log_events_response = logs_client.get_log_events(
                                logGroupName=log_group,
                                logStreamName=stream_name,
                                limit=max_lines,
                                startTime=start_time,
                                endTime=end_time
                            )
                            log_messages = [event["message"] for event in log_events_response.get("events", [])]

                            if log_messages:
                                logs_by_task[task_id][container_name] = {
                                    "logGroup": log_group,
                                    "logStream": stream_name,
                                    "logs": log_messages
                                }
                                break  # Stop searching if logs are found
                        except Exception:
                            continue  # Try next log stream variation

            return logs_by_task

        except Exception as e:
            logger.error(f"Exception occurred while getting ECS task logs: {e}")
            return {"error": f"Failed to fetch logs: {str(e)}"}

    # S3 Methods
    def download_file_contents_from_s3(self, bucket_name, object_key, expiration=3600):
        """
        Generates a pre-signed URL and returns the text content of the file from S3.

        :param bucket_name: str - S3 bucket name
        :param object_key: str - Key (path) to the file in the bucket
        :param expiration: int - Pre-signed URL expiry time in seconds
        :return: str or None - File content as text if successful, None otherwise
        """
        s3_client = boto3.client('s3',
                                 aws_access_key_id=self.__aws_access_key,
                                 aws_secret_access_key=self.__aws_secret_key,
                                 region_name=self.region,
                                 aws_session_token=self.__aws_session_token)

        try:
            url = s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket_name, 'Key': object_key},
                ExpiresIn=expiration
            )
        except ClientError as e:
            print(f"[ERROR] Failed to generate pre-signed URL: {e}")
            return None

        try:
            response = requests.get(url)
            response.raise_for_status()

            # Try decoding the content as UTF-8
            try:
                content = response.content.decode('utf-8')
            except UnicodeDecodeError:
                print("[WARNING] File is not in UTF-8 format or is binary.")
                return None

            return content

        except requests.RequestException as e:
            print(f"[ERROR] Failed to download file: {e}")
            return None

    def test_cost_explorer_permission(self):
        """Tests permission to access AWS Cost Explorer (ce:GetCostAndUsage)."""
        try:
            logger.info(f"Testing Cost Explorer permissions for region: {self.region}")
            client = self.get_connection()

            # Use a minimal test query to check permissions
            from datetime import datetime, timedelta
            end_date = datetime.now()
            start_date = end_date - timedelta(days=1)

            try:
                response = client.get_cost_and_usage(
                    TimePeriod={
                        'Start': start_date.strftime('%Y-%m-%d'),
                        'End': end_date.strftime('%Y-%m-%d')
                    },
                    Granularity='DAILY',
                    Metrics=['UnblendedCost'],
                    GroupBy=[{'Type': 'DIMENSION', 'Key': 'SERVICE'}]
                )

                logger.info("Cost Explorer permission check passed")
                return True

            except client.exceptions.ClientError as ce:
                error_code = ce.response.get('Error', {}).get('Code')
                message_lower = str(ce).lower()

                # Check for explicit access denied errors
                if error_code in ['AccessDeniedException', 'UnauthorizedOperation'] or 'access denied' in message_lower or 'unauthorized' in message_lower:
                    logger.warning(f"Cost Explorer permission check failed due to access denied: {ce}")
                    raise ce

                # Other errors might be due to account configuration but permissions are likely OK
                logger.info(f"Cost Explorer permission check passed (ignoring non-permission error: {error_code})")
                return True

        except Exception as e:
            logger.warning(f"Cost Explorer permission check failed: {e}")
            raise e

    def aws_cost_get_cost_analysis(self, start_date, end_date, group_by_dimension,
                                  group_by_tag_key=None, filter_dimension=None, filter_values=None,
                                  filter_tag_key=None, filter_tag_values=None, granularity='DAILY',
                                  metrics=['UnblendedCost']):
        """
        Analyze AWS costs by any dimension with flexible filtering.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            group_by_dimension: Dimension to group by ('SERVICE', 'USAGE_TYPE', 'INSTANCE_TYPE', 'AZ', 'TAG', etc.)
            group_by_tag_key: Required when group_by_dimension='TAG'
            filter_dimension: Optional dimension to filter by
            filter_values: Optional list of values to filter by
            filter_tag_key: Required when filter_dimension='TAG'
            filter_tag_values: Optional list of tag values to filter by
            granularity: Time granularity ('DAILY', 'MONTHLY', 'HOURLY')
            metrics: List of metrics to analyze (['UnblendedCost'], ['BlendedCost'], etc.)

        Returns:
            Dict with cost analysis results
        """
        try:
            from collections import defaultdict

            client = self.get_connection()

            # Build GroupBy parameter
            if group_by_dimension == 'TAG':
                if not group_by_tag_key:
                    return {'error': 'group_by_tag_key is required when group_by_dimension="TAG"'}
                group_by = [{'Type': 'TAG', 'Key': group_by_tag_key}]
            else:
                group_by = [{'Type': 'DIMENSION', 'Key': group_by_dimension}]

            # Build request parameters
            params = {
                'TimePeriod': {'Start': start_date, 'End': end_date},
                'Granularity': granularity,
                'Metrics': metrics,
                'GroupBy': group_by
            }

            # Add filter if specified
            if filter_dimension and filter_values:
                if filter_dimension == 'TAG':
                    if not filter_tag_key:
                        return {'error': 'filter_tag_key is required when filter_dimension="TAG"'}
                    params['Filter'] = {
                        'Tags': {
                            'Key': filter_tag_key,
                            'Values': filter_tag_values if filter_tag_values else filter_values
                        }
                    }
                else:
                    params['Filter'] = {
                        'Dimensions': {
                            'Key': filter_dimension,
                            'Values': filter_values if isinstance(filter_values, list) else [filter_values]
                        }
                    }

            # Execute the query
            response = client.get_cost_and_usage(**params)

            # Process and aggregate results
            aggregated_costs = defaultdict(float)
            daily_costs = []

            for result in response.get('ResultsByTime', []):
                date = result['TimePeriod']['Start']
                daily_total = 0

                for group in result.get('Groups', []):
                    key = group['Keys'][0] if len(group['Keys']) == 1 else ' | '.join(group['Keys'])
                    cost = float(group['Metrics'][metrics[0]]['Amount'])
                    aggregated_costs[key] += cost
                    daily_total += cost

                daily_costs.append({'date': date, 'cost': daily_total})

            # Sort by cost descending
            sorted_costs = dict(sorted(aggregated_costs.items(), key=lambda x: x[1], reverse=True))
            total_cost = sum(aggregated_costs.values())

            return {
                'success': True,
                'period': {'start': start_date, 'end': end_date},
                'group_by': group_by_dimension,
                'group_by_tag_key': group_by_tag_key,
                'filter_applied': bool(filter_dimension and filter_values),
                'filter_dimension': filter_dimension,
                'filter_values': filter_values,
                'total_cost': round(total_cost, 2),
                'cost_breakdown': {k: round(v, 2) for k, v in sorted_costs.items()},
                'daily_costs': daily_costs,
                'metrics_used': metrics,
                'granularity': granularity
            }

        except Exception as e:
            logger.error(f"Error in AWS cost analysis: {str(e)}")
            return {'error': f"Error analyzing AWS costs: {str(e)}"}

    def aws_cost_discover_dimensions(self, start_date, end_date,
                                   dimensions_to_check=None, max_values_per_dimension=50,
                                   min_cost_threshold=0.01, include_tags=True, sample_only=False,
                                   filter_by_service=None, region_filter=None):
        """
        Discover available AWS cost dimensions and their values.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            dimensions_to_check: List of dimensions to check (None = all major ones)
            max_values_per_dimension: Max values to return per dimension
            min_cost_threshold: Minimum cost to include a value
            include_tags: Whether to discover tag keys/values
            sample_only: Return only top 10 values per dimension
            filter_by_service: Pre-filter to specific services
            region_filter: Pre-filter to specific regions

        Returns:
            Dict with discovered dimensions and their values
        """
        try:
            from collections import defaultdict

            client = self.get_connection()

            # Default dimensions to check
            if not dimensions_to_check:
                dimensions_to_check = ['SERVICE', 'USAGE_TYPE', 'INSTANCE_TYPE', 'AZ', 'LINKED_ACCOUNT', 'REGION']

            if sample_only:
                max_values_per_dimension = 10

            result = {
                'success': True,
                'period': {'start': start_date, 'end': end_date},
                'dimensions': {},
                'tags': {},
                'discovery_settings': {
                    'max_values_per_dimension': max_values_per_dimension,
                    'min_cost_threshold': min_cost_threshold,
                    'sample_only': sample_only
                }
            }

            # Discover each dimension
            for dimension in dimensions_to_check:
                if dimension == 'TAG':
                    continue  # Handle tags separately

                try:
                    # Build base parameters
                    params = {
                        'TimePeriod': {'Start': start_date, 'End': end_date},
                        'Granularity': 'DAILY',
                        'Metrics': ['UnblendedCost'],
                        'GroupBy': [{'Type': 'DIMENSION', 'Key': dimension}]
                    }

                    # Add filters if specified
                    if filter_by_service and dimension != 'SERVICE':
                        params['Filter'] = {
                            'Dimensions': {'Key': 'SERVICE', 'Values': filter_by_service}
                        }
                    elif region_filter and dimension not in ['REGION', 'AZ']:
                        params['Filter'] = {
                            'Dimensions': {'Key': 'REGION', 'Values': region_filter}
                        }

                    response = client.get_cost_and_usage(**params)

                    # Aggregate dimension values
                    dimension_costs = defaultdict(float)
                    for time_result in response.get('ResultsByTime', []):
                        for group in time_result.get('Groups', []):
                            key = group['Keys'][0]
                            if key and key.strip():
                                cost = float(group['Metrics']['UnblendedCost']['Amount'])
                                dimension_costs[key] += cost

                    # Filter by cost threshold and limit
                    filtered_costs = {k: v for k, v in dimension_costs.items()
                                    if v >= min_cost_threshold}
                    sorted_costs = dict(sorted(filtered_costs.items(),
                                             key=lambda x: x[1], reverse=True))

                    # Limit results
                    limited_costs = dict(list(sorted_costs.items())[:max_values_per_dimension])

                    result['dimensions'][dimension] = {
                        'values': list(limited_costs.keys()),
                        'costs': {k: round(v, 2) for k, v in limited_costs.items()},
                        'count': len(limited_costs),
                        'total_cost': round(sum(limited_costs.values()), 2),
                        'truncated': len(sorted_costs) > max_values_per_dimension
                    }

                except Exception as e:
                    logger.warning(f"Could not discover dimension {dimension}: {str(e)}")
                    result['dimensions'][dimension] = {'error': str(e)}

            # Discover tags if requested
            if include_tags:
                try:
                    # Get available tag keys using get_tags
                    tags_response = client.get_tags(
                        TimePeriod={'Start': start_date, 'End': end_date}
                    )

                    discovered_tags = {}
                    for tag_item in tags_response.get('Tags', []):
                        if isinstance(tag_item, str):
                            # Simple tag key
                            tag_key = tag_item
                            discovered_tags[tag_key] = []
                        elif isinstance(tag_item, dict):
                            # Tag with key and values
                            tag_key = tag_item.get('Key', '')
                            tag_values = tag_item.get('Values', [])
                            if tag_key:
                                discovered_tags[tag_key] = tag_values

                    # Test each tag for cost data (sample the top few)
                    tag_costs = {}
                    for tag_key in list(discovered_tags.keys())[:5]:  # Test top 5 tags
                        try:
                            response = client.get_cost_and_usage(
                                TimePeriod={'Start': start_date, 'End': end_date},
                                Granularity='DAILY',
                                Metrics=['UnblendedCost'],
                                GroupBy=[{'Type': 'TAG', 'Key': tag_key}]
                            )

                            total_cost = 0
                            unique_values = set()
                            for time_result in response.get('ResultsByTime', []):
                                for group in time_result.get('Groups', []):
                                    tag_value = group['Keys'][0]
                                    if tag_value and tag_value.strip():
                                        cost = float(group['Metrics']['UnblendedCost']['Amount'])
                                        total_cost += cost
                                        unique_values.add(tag_value)

                            if total_cost >= min_cost_threshold:
                                tag_costs[tag_key] = {
                                    'total_cost': round(total_cost, 2),
                                    'unique_values': list(unique_values)[:10],  # Sample values
                                    'value_count': len(unique_values)
                                }

                        except Exception as e:
                            logger.warning(f"Could not test tag {tag_key}: {str(e)}")

                    result['tags'] = {
                        'available_tag_keys': list(discovered_tags.keys()),
                        'tag_keys_with_cost_data': tag_costs,
                        'total_tag_keys_discovered': len(discovered_tags)
                    }

                except Exception as e:
                    logger.warning(f"Could not discover tags: {str(e)}")
                    result['tags'] = {'error': str(e)}

            return result

        except Exception as e:
            logger.error(f"Error in AWS cost dimension discovery: {str(e)}")
            return {'error': f"Error discovering AWS cost dimensions: {str(e)}"}
