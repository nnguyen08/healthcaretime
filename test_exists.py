from src.s3_helper import S3Helper
from dotenv import load_dotenv
import os

# Load environment
load_dotenv()
bucket = os.getenv('BRONZE_BUCKET')

# Initialize S3Helper
s3 = S3Helper(bucket)

# Test 1: Check file that EXISTS
print("\n🔍 Checking if 'test/upload_test.csv' exists...")
exists = s3.file_exists('test/upload_test.csv')
print(f"Result: {exists}")
assert exists == True, "File should exist!"
print("✅ Test 1 passed!")

# Test 2: Check file that DOESN'T exist
print("\n🔍 Checking if 'fake/nonexistent.csv' exists...")
exists = s3.file_exists('fake/nonexistent.csv')
print(f"Result: {exists}")
assert exists == False, "File should NOT exist!"
print("✅ Test 2 passed!")

print("\n✅ All file_exists tests passed!")
