import boto3
from botocore.exceptions import ClientError
import os

class S3Helper:
    """Helper class for S3 operations"""
    
    def __init__(self, bucket_name, region_name='us-east-1'):
        """
        Initialize S3Helper
        
        Args:
            bucket_name (str): Name of the S3 bucket
            region_name (str): AWS region (default: us-east-1)
        """
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3', region_name=region_name)
        print(f"✓ S3Helper initialized for bucket: {bucket_name}")
    
    def upload_file(self, local_path, s3_key):
        """Upload a file to S3"""
        try:
            self.s3_client.upload_file(local_path, self.bucket_name, s3_key)
            print(f"✓ Uploaded: {local_path} → s3://{self.bucket_name}/{s3_key}")
            return True
        except ClientError as e:
            print(f"✗ Upload failed: {e}")
            return False
    
    def download_file(self, s3_key, local_path):
        """Download a file from S3"""
        try:
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
            print(f"✓ Downloaded: s3://{self.bucket_name}/{s3_key} → {local_path}")
            return True
        except ClientError as e:
            print(f"✗ Download failed: {e}")
            return False
    
    def list_files(self, prefix=""):
        """List files in bucket"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix)
        
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    files.append(obj['Key'])
        
            print(f"✓ Found {len(files)} files in s3://{self.bucket_name}/{prefix}")
            return files
        
        except ClientError as e:
            print(f"✗ List failed: {e}")
            return []
    
    def file_exists(self, s3_key):
        """Check if file exists"""
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404' or error_code == '403':
                return False
            else:
                print(f"✗ Error checking file: {e}")
                return False
    
    def delete_file(self, s3_key):
        """Delete a file from S3"""
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)
            print(f"✓ Deleted: s3://{self.bucket_name}/{s3_key}")
            return True
        except ClientError as e:
            print(f"✗ Delete failed {e}")
            return False
