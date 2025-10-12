from src.s3_helper import S3Helper
from dotenv import load_dotenv
import os

# Load environment
load_dotenv()
bucket = os.getenv('BRONZE_BUCKET')

# Initialize S3Helper
s3 = S3Helper(bucket)

# Test list all files
print("\n📋 Listing all files in bucket:")
all_files = s3.list_files()
for f in all_files:
    print(f"  - {f}")

# Test list files in test/ folder only
print("\n📋 Listing files in 'test/' folder:")
test_files = s3.list_files('test/')
for f in test_files:
    print(f"  - {f}")

print(f"\n✅ List test passed! Found {len(all_files)} total files, {len(test_files)} in test/")
