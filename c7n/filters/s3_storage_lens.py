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

    @staticmethod
    def sum_metric_exceeds_threshold(metric_rows, operator, threshold, s3_path, report_date, metric, bucket):
        metric_sum = float(metric_rows['metric_value'].sum()) if not pd.isna(metric_rows['metric_value'].sum()) else 0.0
        if not operator(metric_sum, threshold):
            return False, None
        buckets = [
            {'bucket_name': row['bucket_name'], 'value': float(row['metric_value'])}
            for _, row in metric_rows.iterrows()
            if 'bucket_name' in row and pd.notnull(row['bucket_name']) and float(row['metric_value']) > 0
        ]
        comment = f"aggregated sum exceeded threshold {threshold}"
        detail = {
            'csv': s3_path,
            'report_date': report_date,
            'metric_name': metric,
            'sum': metric_sum,
            'buckets': buckets,
            'comment': comment
        }
        return True, detail

    @staticmethod
    def bucket_metric_exceeds_threshold(metric_rows, operator, threshold, s3_path, report_date, metric, threshold_val):
        details = []
        matched_buckets = set()
        for _, row in metric_rows.iterrows():
            bucket_name = row.get('bucket_name')
            metric_value = float(row['metric_value']) if not pd.isna(row['metric_value']) else 0.0
            if bucket_name and operator(metric_value, threshold):
                matched_buckets.add(bucket_name)
                details.append({
                    'csv': s3_path,
                    'report_date': report_date,
                    'metric_name': metric,
                    'bucket_name': bucket_name,
                    'value': int(metric_value),
                    'comment': f"bucket value exceeded threshold {threshold_val}"
                })
        return matched_buckets, details

    @staticmethod
    def operator_map(operator):
        tmp_operator_map = {
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
        if operator not in tmp_operator_map:
            raise ValueError(f"Unsupported operator: {operator}. Supported operators: {list(tmp_operator_map.keys())}")
        return tmp_operator_map[operator]

    def filter_buckets_by_metrics_per_csv(self, session, csv_tuples, region, resources):
        """
        For each CSV, check all metrics in the 'metrics' list.
        If any metric in the list meets the threshold in a CSV, that bucket is matched.
        """
        statistic = self.data.get('statistic', 'sum')
        metrics = self.data.get('metrics', [])
        threshold = self.data.get('threshold')
        op = self.data.get('op', 'greater-than')
        operator = self.operator_map(op)
        matched_buckets = set()
        detailed_stats = []

        for bucket, key in csv_tuples:
            s3_path = f's3://{bucket}/{key}'
            try:
                obj = session.client('s3', region_name=region).get_object(Bucket=bucket, Key=key)
                content = obj['Body'].read().decode('utf-8')
                df = pd.read_csv(io.StringIO(content))
                if 'metric_name' in df.columns and 'metric_value' in df.columns:
                    df['metric_value'] = pd.to_numeric(df['metric_value'], errors='coerce').fillna(0)
                    report_date = df['report_date'].iloc[0] if 'report_date' in df.columns else None
                    for metric in metrics:
                        metric_rows = df[df['metric_name'] == metric]
                        if metric_rows.empty:
                            continue
                        if statistic == 'sum':
                            exceeded, detail = self.sum_metric_exceeds_threshold(
                                metric_rows, operator, threshold, s3_path, report_date, metric, bucket)
                            if not exceeded:
                                continue
                            matched_buckets.add(bucket)
                            detailed_stats.append(detail)
                        elif statistic == 'per-bucket-value':
                            matched_set, details = self.bucket_metric_exceeds_threshold(
                                metric_rows, operator, threshold, s3_path, report_date, metric, threshold)
                            matched_buckets.update(matched_set)
                            detailed_stats.extend(details)
                else:
                    detailed_stats.append({'csv': s3_path, 'error': 'CSV missing required columns.'})
            except Exception as e:
                detailed_stats.append({'csv': s3_path, 'error': str(e)})
        matched = []
        for r in resources:
            bucket_name = r.get('Name') or r.get('Bucket') or r.get('name')
            if bucket_name in matched_buckets:
                matched.append(r)
            # Attach detailed stats to all resources for traceability
            r['storage_lens_metric_details'] = detailed_stats
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
