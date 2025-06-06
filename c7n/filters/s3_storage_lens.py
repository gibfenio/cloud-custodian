import csv
import json
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
            days = self.data.get('days', 1)

            configs_buckets = self.get_all_report_buckets_with_config(account_id, region)
            metrics_info = []
            for config_id, buckets in configs_buckets:
                # Only one bucket per config
                bucket = buckets[0] if buckets else None
                csv_list = self.list_recent_report_files(session, [bucket], days).get(bucket, []) if bucket else []
                metrics_info.append({
                    "config_name": config_id,
                    "buckets": bucket,
                    "csv_list": csv_list
                })
            analyzer = StorageLensMetricsAnalyzer(metrics_info)
            result = {
                "metrics_info": metrics_info,
                "all_csvs": analyzer.get_all_csvs(),
                "summary": analyzer.summary()
            }
            # self.log.info(f"Metrics info: {json.dumps(result, indent=2)}")
            print("=============")
            print(json.dumps(result, indent=2))
        except Exception as e:
            self.log.error(f"Error in getting Storage Lens report files: {e}")
        return resources

    @staticmethod
    def get_all_report_buckets_with_config(account_id, region_name=None):
        """
        Returns a list of tuples: (config_id, [bucket_names])
        """
        try:
            s3control = boto3.client('s3control', region_name=region_name)
            configs = []
            resp = s3control.list_storage_lens_configurations(AccountId=account_id)
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
                    match = re.match(r"arn:aws:s3:::([a-zA-Z0-9._-]+)", bucket_arn)
                    if match:
                        bucket_name = match.group(1)
                        configs.append((config_id, [bucket_name]))
            return configs
        except Exception as e:
            print(f"Unexpected error in get_all_report_buckets_with_config: {e}")
            return []

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

class StorageLensMetricsAnalyzer:
    def __init__(self, metrics_info):
        self.metrics_info = metrics_info

    def get_all_csvs(self):
        """Return all CSV file paths across all configs/buckets."""
        all_csvs = []
        for entry in self.metrics_info:
            all_csvs.extend(entry.get('csv_list', []))
        return all_csvs

    def summary(self):
        """Return a summary of configs and CSV counts."""
        return [
            {
                'config_name': entry['config_name'],
                'bucket': entry['buckets'],
                'csv_count': len(entry.get('csv_list', []))
            }
            for entry in self.metrics_info
        ]
