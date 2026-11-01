"""
Initializes Lakehouse S3 Buckets in MinIO.
"""
import boto3
import time

def setup_buckets():
    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadminpassword",
        region_name="us-east-1",
    )

    buckets = [
        "lakehouse-bronze",
        "lakehouse-silver",
        "lakehouse-gold",
        "spark-checkpoints"
    ]

    print("Verifying MinIO connection and buckets...")
    for bucket in buckets:
        try:
            s3.create_bucket(Bucket=bucket)
            print(f"[+] Bucket created: {bucket}")
        except s3.exceptions.BucketAlreadyOwnedByYou:
            print(f"[i] Bucket already exists: {bucket}")
        except Exception as e:
            print(f"[-] Error creating {bucket}: {e}")

if __name__ == "__main__":
    time.sleep(2)  # Short pause to ensure MinIO port is accepting connections
    setup_buckets()
