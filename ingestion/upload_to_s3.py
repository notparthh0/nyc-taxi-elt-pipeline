import boto3
import os

# Your S3 bucket name
BUCKET_NAME = "nyc-taxi-de-parth-2026"

# Connect to S3 (uses credentials from aws configure automatically)
s3 = boto3.client("s3", region_name="us-east-1")

# Upload all files from data/cleaned/ to S3
local_folder = "data/cleaned/"
s3_folder = "cleaned/"

print(f"Uploading files to s3://{BUCKET_NAME}/{s3_folder}")

for filename in os.listdir(local_folder):
    local_path = os.path.join(local_folder, filename)
    s3_path = s3_folder + filename
    
    if os.path.isfile(local_path):
        print(f"Uploading {filename}...")
        s3.upload_file(local_path, BUCKET_NAME, s3_path)
        print(f"Done: {filename}")

print("All files uploaded to S3 successfully!")
