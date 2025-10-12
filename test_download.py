from src.s3_helper import S3Helper
from dotenv import load_dotenv
import os

# Load environment
load_dotenv()
bucket = os.getenv('BRONZE_BUCKET')

# Initialize S3Helper
s3 = S3Helper(bucket)

# Test download
print("\n📥 Testing download...")
result = s3.download_file('test/upload_test.csv', 'downloaded_test.csv')

if result:
    print("\n✅ Download test passed!")
    # Verify file exists locally
    if os.path.exists('downloaded_test.csv'):
        print("✓ File exists locally")
        # Show contents
        with open('downloaded_test.csv', 'r') as f:
            print("\n📄 File contents:")
            print(f.read())
    else:
        print("✗ File not found locally")
else:
    print("\n❌ Download test failed!")
