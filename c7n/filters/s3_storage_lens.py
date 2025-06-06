import csv
from datetime import datetime, timedelta, timezone
import boto3
import re
import botocore
import botocore.exceptions
from c7n.filters import Filter
from c7n.utils import type_schema

class StorageLensMetricsFilter(Filter):
    """
    Filter S3 buckets using metrics from S3 Storage Lens reports (not CloudWatch).
    Also provides a method to discover which buckets are configured to receive Storage Lens CSV reports.
    """
    schema = type_schema(
        'storage-lens-metrics',
        metric={'type': 'string'},
        op={'type': 'string'},
        value={'type': 'number'},
        days={'type': 'integer'},
    )

    def process(self, resources, event=None):
        try:
            session = self.manager.session_factory()
            account_id = session.client('sts').get_caller_identity()['Account']
            region = session.region_name
            report_buckets = self.get_all_report_buckets(account_id, region)
            # self.log.info(f"Discovered Storage Lens report buckets: {report_buckets}")

            days = self.data.get('days', 1)

            report_files = self.list_recent_report_files(session, report_buckets, days)
            self.log.info(f"Recent report files: {report_files}")
            print(f"Recent report files: {report_files}")

        except Exception as e:
            self.log.error(f"Error in getting Storage Lens report buckets or reading metrics: {e}")

        return resources

    @staticmethod
    def get_all_report_buckets(account_id, region_name=None):
        """
        Discover S3 buckets configured to receive Storage Lens CSV reports in this account.
        Returns a list of bucket names.
        """
        try:
            s3control = boto3.client('s3control', region_name=region_name)
            buckets = set()
            try:
                resp = s3control.list_storage_lens_configurations(AccountId=account_id)
            except botocore.exceptions.ClientError as e:
                print(f"Error listing Storage Lens configurations: {e}")
                return []
            except (botocore.exceptions.NoCredentialsError, botocore.exceptions.PartialCredentialsError) as e:
                print(f"AWS credentials error: {e}")
                return []

            for sl_config in resp.get('StorageLensConfigurationList', []):
                config_id = sl_config['Id']
                try:
                    config = s3control.get_storage_lens_configuration(
                        ConfigId=config_id,
                        AccountId=account_id
                    )
                except botocore.exceptions.ClientError as e:
                    print(f"Error getting Storage Lens configuration {config_id}: {e}")
                    continue
                dest = (config.get('StorageLensConfiguration', {})
                        .get('DataExport', {})
                        .get('S3BucketDestination', {}))
                bucket_arn = dest.get('Arn')
                if bucket_arn:
                    # ARN format: arn:aws:s3:::bucket-name
                    match = re.match(r"arn:aws:s3:::([a-zA-Z0-9._-]+)", bucket_arn)
                    if match:
                        buckets.add(match.group(1))
            return list(buckets)
        except (botocore.exceptions.NoCredentialsError, botocore.exceptions.PartialCredentialsError) as e:
            print(f"AWS credentials error: {e}")
            return []
        except Exception as e:
            print(f"Unexpected error in get_all_report_buckets: {e}")
            return []

    @staticmethod
    def list_recent_report_files(session, buckets, days):
        """
        List all Storage Lens CSV report files in the specified buckets created in the last N days.
        Returns: {bucket_name: [list of s3 keys]}
        """
        result = {}
        s3 = session.client('s3')
        now = datetime.now(timezone.utc)
        for bucket in buckets:
            result[bucket] = []
            paginator = s3.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket):
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    if key.endswith('.csv'):
                        last_modified = obj['LastModified']
                        if (now - last_modified).days < days:
                            result[bucket].append(key)
        return result
