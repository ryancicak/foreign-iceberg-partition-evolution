import boto3
import os
import uuid
import time

def setup_v2_resources():
    # Load env
    env_path = '/Users/ryan.cicak/Documents/foreign-iceberg-partition-evolution/.env'
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'): continue
                if '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value.strip('"').strip("'")

    session = boto3.Session(
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_DEFAULT_REGION")
    )
    s3 = session.client('s3')
    glue = session.client('glue')
    athena = session.client('athena')

    # 1. New Bucket
    bucket_name = f"uc-federation-demo-v2-{uuid.uuid4().hex[:8]}"
    print(f"Creating V2 bucket: {bucket_name}")
    if os.environ.get("AWS_DEFAULT_REGION") == 'us-east-1':
        s3.create_bucket(Bucket=bucket_name)
    else:
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': os.environ.get("AWS_DEFAULT_REGION")}
        )

    # 2. New Glue Database
    db_name = "glue_uc_demo_db_v2"
    print(f"Creating V2 Database: {db_name}")
    try:
        glue.create_database(
            DatabaseInput={'Name': db_name, 'Description': 'V2 Demo DB'}
        )
    except glue.exceptions.AlreadyExistsException:
        print(f"Database {db_name} exists.")

    return bucket_name, db_name

if __name__ == "__main__":
    b, d = setup_v2_resources()
    print(f"V2_SETUP: {b} {d}")

