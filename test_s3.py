import boto3, os
from dotenv import load_dotenv

load_dotenv()

region = os.getenv('AWS_REGION')
bucket_name = os.getenv('BRONZE_BUCKET')

client = boto3.client('s3', region_name=region)

response = client.list_buckets()

print("\n🪣 My S3 Buckets:")
print("-" * 50)
for bucket in response['Buckets']:
    name = bucket['Name']
    created = bucket['CreationDate'].strftime('%Y-%m-%d')
    print(f"  ✓ {name}")
    print(f"    Created: {created}")
    print()
