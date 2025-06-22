import csv
import pandas as pd
import io
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
        op={'type': 'string'},
        metric={'type': 'string'},
        value={'type': 'number'},
        days={'type': 'integer'},
        metrics={'type': 'array', 'items': {'type': 'string'}},
        threshold={'type': 'number'},
        statistic={'type': 'string'},
    )

    def process(self, resources, event=None):
        try:
            session = self.manager.session_factory()
            account_id = session.client('sts').get_caller_identity()['Account']
            region = session.region_name
            days = self.data.get('days', 1)

            configs_buckets = self.get_all_report_buckets_with_config(account_id, region)
            metrics_info = []
            all_csvs = []
            for config_id, buckets in configs_buckets:
                bucket = buckets[0] if buckets else None
                csv_list = self.list_recent_report_files(session, [bucket], days).get(bucket, []) if bucket else []
                metrics_info.append({
                    "config_name": config_id,
                    "buckets": bucket,
                    "csv_list": csv_list
                })
                all_csvs.extend([f's3://{bucket}/{key}' for key in csv_list])
            analyzer = StorageLensMetricsAnalyzer(metrics_info)
            result = {
                "metrics_info": metrics_info,
                "all_csvs": analyzer.get_all_csvs(),
                "summary": analyzer.summary()
            }
            print("All Storage Lens CSVs:")
            for csv_path in all_csvs:
                print(csv_path)

            csv_tuples = [(path.split('/')[2], '/'.join(path.split('/')[3:])) for path in all_csvs]
            # Instead of combining, check each CSV individually
            return self.filter_buckets_by_metrics_per_csv(session, csv_tuples, region, resources)
        except Exception as e:
            self.log.error(f"Error in getting Storage Lens report files: {e}")
        return resources

    def to_df_from_csv(self, session, csv_tuples, region):
        all_dfs = []
        print("\nReading CSV contents into DataFrame:")
        for bucket, key in csv_tuples:
            s3_path = f's3://{bucket}/{key}'
            print(f'--- Reading {s3_path} ---')
            try:
                obj = session.client('s3', region_name=region).get_object(Bucket=bucket, Key=key)
                content = obj['Body'].read().decode('utf-8')
                df = pd.read_csv(io.StringIO(content))
                df['source_file'] = s3_path  # Add source column
                all_dfs.append(df)
            except Exception as e:
                print(f'Error reading {s3_path}: {e}')

        if all_dfs:
            combined_df = pd.concat(all_dfs, ignore_index=True)
        else:
            combined_df = pd.DataFrame()  # Return empty if no files

        return combined_df

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

    def detect_sum_exceeds_threshold(self, metrics_df, metrics, threshold):
        """
        For each metric in metrics, computes the sum of metric_value grouped by metric_name.
        If the sum is >= threshold, returns a list of dicts with metric_name, sum, and csv sources.
        """
        print(">>>>>>>>>>>>>>>>>>>>>>>>>")
        print(metrics)
        print(">>>>>>>>>>>>>>>>>>>>>>>>>")
        
        result = []
        for metric in metrics:
            df_metric = metrics_df[metrics_df['metric_name'] == metric]
            if df_metric.empty:
                continue
            total = df_metric['metric_value'].sum()
            if total >= threshold:
                csv_metric_values = {
                    src: int(val)
                    for src, val in df_metric.groupby('source_file')['metric_value'].sum().items()
                }
                result.append({
                    'metric_name': metric,
                    'sum': int(total),
                    'csv_metric_values': csv_metric_values
                })
        return result

    def filter_buckets_by_metrics(self, metrics_df, resources):
        """
        Filters resources based on aggregated metrics from metrics_df and filter config.
        Attaches all relevant metrics data as 'metrics_df' to each matching resource.
        """
        # DEBUG: print columns
        print("[DEBUG] metrics_df columns:", metrics_df.columns.tolist())
        if not metrics_df.empty:
            print("==== metrics_df (first 2 rows, full columns) ====")
            print(metrics_df.head(2).to_string(max_cols=None, line_width=1000))
            print("=================================================")
        if metrics_df.empty:
            return []

        # Use correct bucket column name
        bucket_col = 'bucket_name' if 'bucket_name' in metrics_df.columns else 'Bucket'
        print(f"[DEBUG] Using bucket column: {bucket_col}")

        metrics = self.data.get('metrics', [])
        statistic = self.data.get('statistic', 'sum')
        op = self.data.get('op', 'ge')
        threshold = self.data.get('threshold', 0)

        # Only keep relevant metrics columns
        filtered = metrics_df[metrics_df.columns.intersection([bucket_col] + metrics)]
        grouped = filtered.groupby(bucket_col).agg(statistic)

        import operator as opmap
        ops = {
            'ge': opmap.ge,
            'gt': opmap.gt,
            'le': opmap.le,
            'lt': opmap.lt,
            'eq': opmap.eq,
            'ne': opmap.ne,
        }
        if statistic == 'sum' and op == 'ge':
            result = self.detect_sum_exceeds_threshold(metrics_df, metrics, threshold)
            print("=== Metrics sum >= threshold results ===")
            for entry in result:
                print(entry)
            print("========================================")

            # Attach results to resources by bucket name
            matched = []
            for r in resources:
                bucket_name = r.get('Name') or r.get('Bucket') or r.get('name')
                # Check if this bucket appears in any csv_sources for any metric
                bucket_metrics = [
                    entry for entry in result
                    if any(bucket_name in src for src in entry['csv_metric_values'].keys())
                ]
                if bucket_metrics:
                    r['storage_lens_metrics'] = bucket_metrics
                    matched.append(r)
            return matched
        
        return []

    def filter_buckets_by_metrics_per_csv(self, session, csv_tuples, region, resources):
        """
        For each CSV, check all metrics in the 'metrics' list.
        If any metric in the list meets the threshold in a CSV, that bucket is matched.
        """
        import pandas as pd
        metrics = self.data.get('metrics', [])
        threshold = self.data.get('threshold')
        op = self.data.get('op', 'greater-than')
        operator_map = {
            'greater-than': lambda x, y: x > y,
            'gt': lambda x, y: x > y,
            'greater-than-equal': lambda x, y: x >= y,
            'ge': lambda x, y: x >= y,
            'less-than': lambda x, y: x < y,
            'lt': lambda x, y: x < y,
            'less-than-equal': lambda x, y: x <= y,
            'le': lambda x, y: x <= y,
            'equal': lambda x, y: x == y,
            'eq': lambda x, y: x == y
        }
        operator = operator_map[op]
        matched_buckets = set()
        for bucket, key in csv_tuples:
            s3_path = f's3://{bucket}/{key}'
            try:
                obj = session.client('s3', region_name=region).get_object(Bucket=bucket, Key=key)
                content = obj['Body'].read().decode('utf-8')
                df = pd.read_csv(io.StringIO(content))
                # print(f'==== {s3_path} ====')
                # print(df)
                # print('--- Metric Sums (per metric_name) ---')
                if 'metric_name' in df.columns and 'metric_value' in df.columns:
                    df['metric_value'] = pd.to_numeric(df['metric_value'], errors='coerce').fillna(0)
                    sums = df.groupby('metric_name')['metric_value'].sum()
                    for metric, total in sums.items():
                        # print(f'Metric: {metric} | Sum: {total}')
                        if metric in metrics and operator(total, threshold):
                            print(f'[DEBUG] Matched: bucket={bucket}, metric={metric}, sum={total}, threshold={threshold}, op={op}')
                            matched_buckets.add(bucket)
                            print(f'[DEBUG] matched_buckets so far: {matched_buckets}')
                else:
                    print('CSV missing required columns.')
                print('==============================')
            except Exception as e:
                print(f'Error reading {s3_path}: {e}')
        matched = []
        for r in resources:
            bucket_name = r.get('Name') or r.get('Bucket') or r.get('name')
            if bucket_name in matched_buckets:
                matched.append(r)
        print("+++++++++++++++++++++++++")
        print(matched)
        return matched

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
