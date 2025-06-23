import pandas as pd
import io
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
    def bucket_metric_exceeds_threshold(metric_rows, operator, threshold, s3_path, report_date, metric, threshold_val):
        details = []
        matched_buckets = set()
        for _, row in metric_rows.iterrows():
            bucket_name = row.get('bucket_name')
            # Skip if bucket_name is missing, NaN, not a string, or empty
            if pd.isna(bucket_name) or not isinstance(bucket_name, str) or not bucket_name.strip():
                continue
            metric_value = float(row['metric_value']) if not pd.isna(row['metric_value']) else 0.0
            if operator(metric_value, threshold):
                matched_buckets.add(bucket_name)
                details.append({
                    's3_len_lens_csv_report': s3_path,
                    'report_date': report_date,
                    'metric_name': metric,
                    'bucket_name': bucket_name,
                    'value': int(metric_value),
                    'comment': f"{bucket_name} metric {metric} value {int(metric_value)} "+
                               f"exceeded expected threshold {int(threshold_val)}"
                })
        return matched_buckets, details

    @staticmethod
    def get_operator(operator):
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

    @staticmethod
    def group_by_csv_date_descending_then_metric_type_dict(bucket_details):
        """
        Groups all bucket metric details by CSV report (descending by date), then by metric name (ascending).
        Returns a list of dicts, each with csv_file, report_date, and metrics_info (dict of metric_name -> list of bucket dicts).
        """
        grouped = {}
        for detail in bucket_details:
            csv_report = detail['s3_len_lens_csv_report']
            report_date = detail['report_date']
            metric_name = detail['metric_name']
            if csv_report not in grouped:
                grouped[csv_report] = {'report_date': report_date, 'metrics': {}}
            if metric_name not in grouped[csv_report]['metrics']:
                grouped[csv_report]['metrics'][metric_name] = []
            grouped[csv_report]['metrics'][metric_name].append({
                k: v for k, v in detail.items() if k not in ['s3_len_lens_csv_report', 'report_date', 'metric_name']
            })
        # Sort by date descending
        def date_key(item):
            try:
                return datetime.strptime(item[1]['report_date'], "%Y-%m-%d")
            except Exception:
                return item[1]['report_date']
        sorted_grouped = sorted(grouped.items(), key=date_key, reverse=True)
        output = []
        for csv_report, data in sorted_grouped:
            # Sort metric names ascending and build a dict
            metrics_info = {metric_name: data['metrics'][metric_name]
                            for metric_name in sorted(data['metrics'].keys())}
            output.append({
                'csv_file': csv_report,
                'report_date': data['report_date'],
                'metrics_info': metrics_info
            })
        return output

    def filter_buckets_by_metrics_per_csv(self, session, csv_tuples, region, resources):
        """
        For each CSV, check all metrics in the 'metrics' list.
        If any metric in the list meets the threshold in a CSV, that bucket is matched.
        """
        statistic = self.data.get('statistic', 'sum')
        metrics = self.data.get('metrics', [])
        threshold = self.data.get('threshold')
        op = self.data.get('op', 'greater-than')
        operator = self.get_operator(op)
        matched_buckets = set()
        detailed_stats = []
        matched = []

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
                        if statistic == 'value':
                            matched_set, details = self.bucket_metric_exceeds_threshold(
                                metric_rows, operator, threshold, s3_path, report_date, metric, threshold)
                            if not matched_set or not details:
                                continue
                            matched_buckets.update(matched_set)
                            detailed_stats.extend(details)
                else:
                    detailed_stats.append({'s3_len_lens_csv_report': s3_path, 'error': 'CSV missing required columns.'})
            except Exception as e:
                detailed_stats.append({'s3_len_lens_csv_report': s3_path, 'error': str(e)})

        # Instead of grouping per bucket, collect all details across all buckets
        all_details = []
        for r in resources:
            bucket_name = r.get('Name')
            bucket_details = [d for d in detailed_stats if d.get('bucket_name') == bucket_name]
            all_details.extend(bucket_details)
        if all_details:
            output = self.group_by_csv_date_descending_then_metric_type_dict(all_details)
            matched = [{"s3_lens_metrics_info_list": output}]
        else:
            matched = []
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
