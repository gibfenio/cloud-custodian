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
        bucket={'type': 'string'},   # optional: S3 bucket for Storage Lens reports
        prefix={'type': 'string'},   # optional: S3 prefix for reports
    )

    def process(self, resources, event=None):
        try:
            session = self.manager.session_factory()
            account_id = session.client('sts').get_caller_identity()['Account']
            region = session.region_name
            report_buckets = self.get_all_report_buckets(account_id, region)
            self.log.info(f"Discovered Storage Lens report buckets: {report_buckets}")
            print(report_buckets)
        except Exception as e:
            self.log.error(f"Error in getting Storage Lens report buckets: {e}")

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
