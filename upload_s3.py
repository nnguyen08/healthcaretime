import glob
from src.s3_helper import S3Helper
from dotenv import load_dotenv
import os

load_dotenv()
bronze_bucket = os.getenv('BRONZE_BUCKET')

s3 = S3Helper(bronze_bucket, quiet=True)

partition = glob.glob('data/generated/year=*/month=*/day=*/appointments.csv')
partition.sort()

print(f"Found {len(partition)} files")
print(f"📍 Uploading to Destination: s3://{bronze_bucket}/appointments/")

response = input("\nDo you want to continue? Y|N: ")
if response.lower() != "y":
    exit()

print(f"\n📤 Uploading {len(partition)} partitions to S3...")
uploaded = 0
failed = 0
for local_path in partition:
    path = local_path.replace('data/generated/', '')
    s3_key = f'appointments/{path}'
    success = s3.upload_file(local_path, s3_key)
    
    if success:
        uploaded+=1
    else:
        failed+=1
    
    if uploaded % 100 == 0:
        print(f"   ✓ {uploaded}/{len(partition)} uploaded...")

print(":\n✅ Upload complete!")
print(f"    Total Files Successfully Uploaded: {uploaded}")   
print(f"    Total Files Failed to Upload: {failed}") 


