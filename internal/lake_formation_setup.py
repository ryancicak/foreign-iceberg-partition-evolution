"""
Lake Formation Permission Setup for Iceberg Metadata Surgeon.

This module handles the complex Lake Formation permissions required for:
1. EMR to create/modify tables in Glue
2. Databricks Unity Catalog to read tables via federation
"""

import boto3
from typing import Optional
from config import get_aws_config, load_env_file


class LakeFormationSetup:
    """Manages Lake Formation permissions for Iceberg federation."""
    
    def __init__(self):
        load_env_file()
        aws_config = get_aws_config()
        
        self.session = boto3.Session(
            aws_access_key_id=aws_config.access_key_id,
            aws_secret_access_key=aws_config.secret_access_key,
            region_name=aws_config.region
        )
        self.lf = self.session.client('lakeformation')
        self.iam = self.session.client('iam')
        self.glue = self.session.client('glue')
        self.region = aws_config.region
    
    def get_account_id(self) -> str:
        """Get AWS account ID."""
        sts = self.session.client('sts')
        return sts.get_caller_identity()["Account"]
    
    def grant_database_permissions(
        self, 
        database_name: str, 
        principal_arn: str,
        permissions: list = None
    ) -> bool:
        """Grant Lake Formation permissions on a database."""
        if permissions is None:
            permissions = ["ALL"]
        
        print(f"  Granting {permissions} on database '{database_name}' to {principal_arn}...")
        
        try:
            self.lf.grant_permissions(
                Principal={'DataLakePrincipalIdentifier': principal_arn},
                Resource={'Database': {'Name': database_name}},
                Permissions=permissions,
            )
            print(f"  ✓ Database permissions granted")
            return True
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"  ✓ Permissions already exist")
                return True
            print(f"  ⚠ Failed: {e}")
            return False
    
    def grant_table_permissions(
        self,
        database_name: str,
        table_name: str,
        principal_arn: str,
        permissions: list = None
    ) -> bool:
        """Grant Lake Formation permissions on a table."""
        if permissions is None:
            permissions = ["ALL", "SELECT", "DESCRIBE"]
        
        print(f"  Granting {permissions} on table '{database_name}.{table_name}' to {principal_arn}...")
        
        try:
            self.lf.grant_permissions(
                Principal={'DataLakePrincipalIdentifier': principal_arn},
                Resource={'Table': {'DatabaseName': database_name, 'Name': table_name}},
                Permissions=permissions,
            )
            print(f"  ✓ Table permissions granted")
            return True
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"  ✓ Permissions already exist")
                return True
            print(f"  ⚠ Failed: {e}")
            return False
    
    def grant_catalog_permissions(self, principal_arn: str) -> bool:
        """Grant Lake Formation catalog-level permissions."""
        print(f"  Granting catalog DESCRIBE to {principal_arn}...")
        
        try:
            self.lf.grant_permissions(
                Principal={'DataLakePrincipalIdentifier': principal_arn},
                Resource={'Catalog': {}},
                Permissions=["DESCRIBE"],
            )
            print(f"  ✓ Catalog permissions granted")
            return True
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"  ✓ Permissions already exist")
                return True
            print(f"  ⚠ Failed: {e}")
            return False
    
    def opt_out_database(self, database_name: str) -> bool:
        """Opt a database out of Lake Formation (grant to IAM_ALLOWED_PRINCIPALS)."""
        print(f"  Opting database '{database_name}' out of Lake Formation...")
        
        try:
            self.lf.grant_permissions(
                Principal={'DataLakePrincipalIdentifier': 'IAM_ALLOWED_PRINCIPALS'},
                Resource={'Database': {'Name': database_name}},
                Permissions=["ALL"],
            )
            print(f"  ✓ Database opted out of Lake Formation")
            return True
        except Exception as e:
            print(f"  ⚠ Failed: {e}")
            return False
    
    def opt_out_table(self, database_name: str, table_name: str) -> bool:
        """Opt a table out of Lake Formation (grant to IAM_ALLOWED_PRINCIPALS)."""
        print(f"  Opting table '{database_name}.{table_name}' out of Lake Formation...")
        
        try:
            self.lf.grant_permissions(
                Principal={'DataLakePrincipalIdentifier': 'IAM_ALLOWED_PRINCIPALS'},
                Resource={'Table': {'DatabaseName': database_name, 'Name': table_name}},
                Permissions=["ALL"],
            )
            print(f"  ✓ Table opted out of Lake Formation")
            return True
        except Exception as e:
            print(f"  ⚠ Failed: {e}")
            return False
    
    def add_iam_policy_for_lakeformation(self, role_name: str) -> bool:
        """Add Lake Formation permissions to an IAM role."""
        print(f"  Adding Lake Formation policy to role '{role_name}'...")
        
        policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["lakeformation:*"],
                    "Resource": "*"
                }
            ]
        }
        
        try:
            self.iam.put_role_policy(
                RoleName=role_name,
                PolicyName='LakeFormationAccess',
                PolicyDocument=str(policy_document).replace("'", '"')
            )
            print(f"  ✓ IAM policy added")
            return True
        except Exception as e:
            print(f"  ⚠ Failed: {e}")
            return False
    
    def add_s3_policy_to_role(self, role_name: str, bucket_name: str) -> bool:
        """Add S3 permissions for a bucket to an IAM role."""
        print(f"  Adding S3 access for bucket '{bucket_name}' to role '{role_name}'...")
        
        policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject",
                        "s3:ListBucket",
                        "s3:GetBucketLocation"
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{bucket_name}",
                        f"arn:aws:s3:::{bucket_name}/*"
                    ]
                }
            ]
        }
        
        try:
            self.iam.put_role_policy(
                RoleName=role_name,
                PolicyName=f'S3Access-{bucket_name[:20]}',
                PolicyDocument=str(policy_document).replace("'", '"')
            )
            print(f"  ✓ S3 policy added")
            return True
        except Exception as e:
            print(f"  ⚠ Failed: {e}")
            return False
    
    def add_user_as_lf_admin(self, user_arn: str) -> bool:
        """Add an IAM user/role as a Lake Formation admin."""
        print(f"  Adding {user_arn} as Lake Formation admin...")
        
        try:
            # Get current settings
            current = self.lf.get_data_lake_settings()
            admins = current.get('DataLakeSettings', {}).get('DataLakeAdmins', [])
            admin_arns = [a['DataLakePrincipalIdentifier'] for a in admins]
            
            if user_arn in admin_arns:
                print(f"  ✓ Already a Lake Formation admin")
                return True
            
            # Add new admin
            admins.append({'DataLakePrincipalIdentifier': user_arn})
            
            self.lf.put_data_lake_settings(
                DataLakeSettings={
                    'DataLakeAdmins': admins,
                    'CreateDatabaseDefaultPermissions': [],
                    'CreateTableDefaultPermissions': []
                }
            )
            print(f"  ✓ Added as Lake Formation admin")
            return True
        except Exception as e:
            print(f"  ⚠ Failed: {e}")
            return False
    
    def setup_for_emr(self, emr_role_name: str, database_name: str, bucket_name: str) -> bool:
        """Set up all Lake Formation permissions for EMR access."""
        print("\n" + "=" * 60)
        print("LAKE FORMATION SETUP FOR EMR")
        print("=" * 60)
        
        account_id = self.get_account_id()
        emr_role_arn = f"arn:aws:iam::{account_id}:role/{emr_role_name}"
        
        success = True
        success &= self.grant_database_permissions(database_name, emr_role_arn, ["ALL"])
        success &= self.add_s3_policy_to_role(emr_role_name, bucket_name)
        success &= self.add_iam_policy_for_lakeformation(emr_role_name)
        
        return success
    
    def setup_for_databricks(
        self, 
        uc_role_name: str, 
        database_name: str, 
        table_name: Optional[str] = None,
        bucket_name: Optional[str] = None
    ) -> bool:
        """Set up all Lake Formation permissions for Databricks UC access."""
        print("\n" + "=" * 60)
        print("LAKE FORMATION SETUP FOR DATABRICKS")
        print("=" * 60)
        
        account_id = self.get_account_id()
        uc_role_arn = f"arn:aws:iam::{account_id}:role/{uc_role_name}"
        
        success = True
        
        # Catalog level
        success &= self.grant_catalog_permissions(uc_role_arn)
        
        # Database level
        success &= self.grant_database_permissions(database_name, uc_role_arn)
        
        # Also grant on 'default' database (Databricks checks this)
        success &= self.grant_database_permissions("default", uc_role_arn, ["DESCRIBE"])
        
        # Table level (if specified)
        if table_name:
            success &= self.grant_table_permissions(database_name, table_name, uc_role_arn)
        
        # S3 access (if specified)
        if bucket_name:
            success &= self.add_s3_policy_to_role(uc_role_name, bucket_name)
        
        return success


def main():
    """Run Lake Formation setup interactively."""
    import os
    
    lf = LakeFormationSetup()
    
    database = os.getenv("GLUE_DATABASE")
    table = os.getenv("GLUE_TABLE")
    bucket = os.getenv("S3_BUCKET")
    emr_role = os.getenv("EMR_ROLE_NAME")
    uc_role = os.getenv("UC_IAM_ROLE_NAME")
    
    if not database:
        print("Error: GLUE_DATABASE environment variable required")
        return
    
    if emr_role:
        lf.setup_for_emr(emr_role, database, bucket)
    
    if uc_role:
        lf.setup_for_databricks(uc_role, database, table, bucket)
    
    print("\n✅ Lake Formation setup complete!")


if __name__ == "__main__":
    main()

