# HealthFlow S3Helper

A Python library for simplified AWS S3 operations, built as part of the HealthFlow healthcare data pipeline project.

## Overview

This project provides a clean, reusable `S3Helper` class that wraps boto3 for common S3 operations with proper error handling and user-friendly output.

## Features

- ✅ Upload files to S3
- ✅ Download files from S3
- ✅ List files with prefix filtering
- ✅ Check file existence
- ✅ Delete files
- ✅ Comprehensive error handling
- ✅ Clean, intuitive API

## Installation
```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/healthcaretime.git
cd healthcaretime

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure environment variables
cp .env.example .env
# Edit .env with your AWS credentials and bucket names
