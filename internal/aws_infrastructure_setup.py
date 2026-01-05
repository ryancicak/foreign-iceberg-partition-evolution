"""
AWS Infrastructure Setup for Iceberg Metadata Surgeon.

Creates the required AWS resources:
1. S3 bucket for Iceberg table storage
2. Glue database for table metadata
3. Lake Formation permissions
"""

import boto3
import uuid
from typing import Tuple
from config import get_aws_config, load_env_file


class AWSInfrastructureSetup:
    """Manages AWS infrastructure for Iceberg demos."""
    
    def __init__(self):
        load_env_file()
        aws_config = get_aws_config()
        
        if not aws_config.validate():
            raise ValueError("AWS configuration is incomplete")
        
        self.session = boto3.Session(
            aws_access_key_id=aws_config.access_key_id,
            aws_secret_access_key=aws_config.secret_access_key,
            region_name=aws_config.region
        )
        self.s3 = self.session.client('s3')
        self.glue = self.session.client('glue')
        self.region = aws_config.region
    
    def create_bucket(self, bucket_name: str = None) -> str:
        """Create an S3 bucket for Iceberg storage."""
        if bucket_name is None:
            bucket_name = f"iceberg-demo-{uuid.uuid4().hex[:8]}"
        
        print(f"Creating S3 bucket: {bucket_name}")
        
        try:
            if self.region == 'us-east-1':
                self.s3.create_bucket(Bucket=bucket_name)
            else:
                self.s3.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': self.region}
                )
            print(f"✓ Bucket created: {bucket_name}")
            return bucket_name
        except self.s3.exceptions.BucketAlreadyOwnedByYou:
            print(f"✓ Bucket already exists: {bucket_name}")
            return bucket_name
        except Exception as e:
            print(f"✗ Failed to create bucket: {e}")
            raise
    
    def create_database(self, database_name: str = None) -> str:
        """Create a Glue database."""
        if database_name is None:
            database_name = f"iceberg_demo_db_{uuid.uuid4().hex[:8]}"
        
        print(f"Creating Glue database: {database_name}")
        
        try:
            self.glue.create_database(
                DatabaseInput={
                    'Name': database_name,
                    'Description': 'Demo database for Iceberg partition evolution'
                }
            )
            print(f"✓ Database created: {database_name}")
            return database_name
        except self.glue.exceptions.AlreadyExistsException:
            print(f"✓ Database already exists: {database_name}")
            return database_name
        except Exception as e:
            print(f"✗ Failed to create database: {e}")
            raise
    
    def delete_bucket(self, bucket_name: str) -> bool:
        """Delete an S3 bucket and all its contents."""
        print(f"Deleting S3 bucket: {bucket_name}")
        
        try:
            # First, delete all objects
            s3_resource = self.session.resource('s3')
            bucket = s3_resource.Bucket(bucket_name)
            bucket.objects.all().delete()
            
            # Then delete the bucket
            self.s3.delete_bucket(Bucket=bucket_name)
            print(f"✓ Bucket deleted: {bucket_name}")
            return True
        except Exception as e:
            print(f"✗ Failed to delete bucket: {e}")
            return False
    
    def delete_database(self, database_name: str) -> bool:
        """Delete a Glue database and all its tables."""
        print(f"Deleting Glue database: {database_name}")
        
        try:
            # First, delete all tables
            tables = self.glue.get_tables(DatabaseName=database_name).get('TableList', [])
            for table in tables:
                self.glue.delete_table(DatabaseName=database_name, Name=table['Name'])
                print(f"  ✓ Deleted table: {table['Name']}")
            
            # Then delete the database
            self.glue.delete_database(Name=database_name)
            print(f"✓ Database deleted: {database_name}")
            return True
        except Exception as e:
            print(f"✗ Failed to delete database: {e}")
            return False
    
    def setup_demo_infrastructure(self, prefix: str = None) -> Tuple[str, str]:
        """Set up complete infrastructure for a demo."""
        if prefix is None:
            prefix = uuid.uuid4().hex[:8]
        
        bucket_name = f"iceberg-demo-{prefix}"
        database_name = f"iceberg_demo_db_{prefix}"
        
        print("\n" + "=" * 60)
        print("AWS INFRASTRUCTURE SETUP")
        print("=" * 60)
        
        bucket = self.create_bucket(bucket_name)
        database = self.create_database(database_name)
        
        print("\n" + "=" * 60)
        print("✅ INFRASTRUCTURE READY")
        print("=" * 60)
        print(f"S3 Bucket: {bucket}")
        print(f"Glue Database: {database}")
        print(f"Warehouse Path: s3://{bucket}/warehouse/")
        
        return bucket, database
    
    def cleanup_demo_infrastructure(self, bucket_name: str, database_name: str) -> bool:
        """Clean up demo infrastructure."""
        print("\n" + "=" * 60)
        print("CLEANUP")
        print("=" * 60)
        
        success = True
        success &= self.delete_database(database_name)
        success &= self.delete_bucket(bucket_name)
        
        if success:
            print("\n✅ Cleanup complete")
        else:
            print("\n⚠️ Cleanup completed with errors")
        
        return success


def main():
    """Run infrastructure setup interactively."""
    import argparse
    
    parser = argparse.ArgumentParser(description="AWS Infrastructure Setup")
    parser.add_argument("--create", action="store_true", help="Create demo infrastructure")
    parser.add_argument("--cleanup", action="store_true", help="Clean up infrastructure")
    parser.add_argument("--bucket", help="Bucket name (for cleanup)")
    parser.add_argument("--database", help="Database name (for cleanup)")
    parser.add_argument("--prefix", help="Prefix for resource names")
    
    args = parser.parse_args()
    
    setup = AWSInfrastructureSetup()
    
    if args.cleanup:
        if not args.bucket or not args.database:
            print("Error: --bucket and --database required for cleanup")
            return
        setup.cleanup_demo_infrastructure(args.bucket, args.database)
    else:
        setup.setup_demo_infrastructure(args.prefix)


if __name__ == "__main__":
    main()
