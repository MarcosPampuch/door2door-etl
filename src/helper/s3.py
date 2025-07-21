import boto3
from botocore import UNSIGNED
from botocore.client import Config
from helper.logger import logger
import json
import sys
import os


class S3:
    def __init__(self, access_key=None, secret_access_key=None, host=None, anonymous=False):
        """
        Initialize the S3 client for either authenticated or anonymous access.
        
        Args:
            access_key (str, optional): AWS access key ID.
            secret_access_key (str, optional): AWS secret access key.
            host (str, optional): S3 endpoint host.
            anonymous (bool, optional): If True, use unsigned (anonymous) access.
        """
        if anonymous:
            self.s3_client = boto3.client(
                's3',
                config=Config(signature_version=UNSIGNED)
                # region_name=getenv('AWS_REGION', 'us-east-1')
            )
        else:
            self.s3_client = boto3.client(
                's3',
                endpoint_url=f'http://{host}',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_access_key
            )

    def get_hour_files_from_bucket(self, bucket_name, timestamp):
        """
        List and download JSON files from the specified S3 bucket for a given hour (UTC).
        
        Args:
            bucket_name (str): Name of the S3 bucket.
            timestamp (datetime): The hour to filter files by (UTC).
        
        Returns:
            tuple: (list of JSON records, number of files fetched)
        """
        logger.info(f"Connecting to S3 bucket: {bucket_name}")
        response = self.s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix='data/'
        )
        files_fetched = 0
        grouped_json = []
        for obj in response.get('Contents', []):
            key = obj['Key']
            last_modified = obj['LastModified']
            if last_modified.hour == timestamp.hour and key.endswith('.json'):
                # logger.info(f"Downloading file: {key}")
                files_fetched += 1
                file_obj = self.s3_client.get_object(Bucket=bucket_name, Key=key)
                file_content = file_obj['Body'].read().decode('utf-8')
                for line in file_content.splitlines():
                    if line.strip():  # skip empty lines
                        try:
                            record_dict = json.loads(line)
                            record_dict['original_s3_file_path'] = f'{bucket_name}/{key}'
                            grouped_json.append(record_dict)
                        except Exception as e:
                            logger.warning(f"Failed to load a line from {key} as JSON: {e}")
                logger.info(f"Loaded JSON records from {key}")
        return grouped_json, files_fetched
    
    def upload_file_to_bucket(self, local_file, bucket):
        """
        Upload a local file to the specified S3 bucket and delete the local file after successful upload.
        
        Args:
            local_file (str): Path to the local file to upload.
            bucket (str): Name of the S3 bucket.
        """
        logger.info(f"Uploading result to S3 bucket: {bucket} as {local_file}")
        
        try:
            self.s3_client.upload_file(local_file, bucket, local_file)
            logger.info(f"Upload to S3 completed.")
            
            if os.path.exists(local_file):
                os.remove(local_file)
                
        except Exception as e:
            logger.error(f"Failed to upload {local_file} to S3 bucket {bucket} as {local_file}: {e}")
    
    def bucket_exists(self, bucket_name):
        """
        Check if a bucket exists in S3. Exits the program if the bucket does not exist.
        
        Args:
            bucket_name (str): Name of the S3 bucket.
        """
        try:
            response = self.s3_client.list_buckets()
            for bucket in response.get('Buckets', []):
                if bucket['Name'] == bucket_name:
                    return
            logger.error(f"S3 bucket {bucket_name} does not exist.")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Error checking if bucket {bucket_name} exists: {e}")
            sys.exit(1)
    
    def fetch_file_from_bucket(self, s3_path):
        """
        Fetch the content of a file from S3 given a full S3 path (e.g., 's3://bucket/key').
        
        Args:
            s3_path (str): Full S3 path to the file.
        
        Returns:
            dict or list: Parsed JSON content of the file.
        """
        
        bucket, path  = s3_path.replace('s3://', '').split('/', 1)
        response = self.s3_client.get_object(Bucket=bucket, Key=path)
        content = response['Body'].read().decode('utf-8')
        logger.info(f"Fetched file from S3: {s3_path}")
        
        json_content = json.loads(content)
        return json_content
    
    def close(self):
        """
        Close the S3 client connection.
        """
        self.s3_client.close()