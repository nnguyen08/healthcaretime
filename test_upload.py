from src.s3_helper import S3Helper
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
bucket = os.getenv('BRONZE_BUCKET')

# Initialize S3Helper
s3 = S3Helper(bucket)

# Test upload
print("\n📤 Testing upload...")
result = s3.upload_file('test_data.csv', 'test/upload_test.csv')

if result:
    print("\n✅ Upload test passed!")
else:
    print("\n❌ Upload test failed!")
