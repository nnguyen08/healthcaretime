from src.s3_helper import S3Helper
from dotenv import load_dotenv
import os

# Load environment
load_dotenv()
bucket = os.getenv('BRONZE_BUCKET')

# Initialize S3Helper
s3 = S3Helper(bucket)

# Test 1: Check file exists before delete
print("\n🔍 Checking if test file exists...")
exists_before = s3.file_exists('test/upload_test.csv')
print(f"Before delete: {exists_before}")

# Test 2: Delete the file
print("\n🗑️  Deleting test file...")
result = s3.delete_file('test/upload_test.csv')
print(f"Delete result: {result}")

# Test 3: Check file no longer exists
print("\n🔍 Checking if file still exists...")
exists_after = s3.file_exists('test/upload_test.csv')
print(f"After delete: {exists_after}")

# Verify
if exists_before and not exists_after and result:
    print("\n✅ All delete tests passed!")
else:
    print("\n❌ Delete test failed!")
