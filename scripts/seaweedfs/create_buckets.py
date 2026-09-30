import os
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import time
import socket

# ------------------------
# Config & Endpoint Check
# ------------------------
endpoint = os.getenv("SEAWEED_S3_ENDPOINT")
if not endpoint:
    raise RuntimeError("SEAWEED_S3_ENDPOINT is not set")

bucket_names = [b.strip() for b in os.getenv("SEAWEED_S3_BUCKETS", "").split(",") if b.strip()]
if not bucket_names:
    raise RuntimeError("No buckets defined in SEAWEED_S3_BUCKETS")

print("SEAWEED_S3_ENDPOINT =", endpoint)
print("Buckets to create:", bucket_names)

# ------------------------
# Wait for S3 endpoint
# ------------------------
def wait_for_s3(host, port, timeout=60):
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=2):
                print("S3 endpoint is reachable")
                return
        except OSError:
            print("Waiting for S3...")
            time.sleep(2)
    raise TimeoutError("S3 endpoint not reachable")

host = endpoint.replace("http://", "").split(":")[0]
port = int(endpoint.split(":")[-1])
wait_for_s3(host, port)

# ------------------------
# Initialize S3 client
# ------------------------
s3 = boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}),
    region_name="us-east-1",
)

# ------------------------
# Helper
# ------------------------
def bucket_exists(s3, name):
    try:
        s3.head_bucket(Bucket=name)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
            return False
        raise

# ------------------------
# Create buckets if missing
# ------------------------
for bucket in bucket_names:
    for attempt in range(3):  # retry up to 3 times
        try:
            if bucket_exists(s3, bucket):
                print(f"Bucket already exists: {bucket}")
            else:
                s3.create_bucket(Bucket=bucket)
                print(f"Bucket created: {bucket}")
            break
        except Exception as e:
            print(f"Error creating bucket {bucket}: {e}")
            if attempt < 2:
                print("Retrying in 3 seconds...")
                time.sleep(3)
            else:
                raise

print("Bucket initialization complete")


