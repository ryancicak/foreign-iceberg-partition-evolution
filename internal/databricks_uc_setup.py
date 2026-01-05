"""
Databricks Unity Catalog Federation Setup for Iceberg Metadata Surgeon.

This module creates all necessary Databricks resources for Glue federation:
1. Storage Credential (for S3 access)
2. Service Credential (for Glue connection - CRITICAL: must be SERVICE type!)
3. External Location (for S3 bucket access)
4. Glue Connection
5. Federated Catalog
"""

import os
import requests
from typing import Optional
from config import get_aws_config, get_databricks_config, get_aws_account_id, load_env_file


class DatabricksUCSetup:
    """Manages Databricks Unity Catalog setup for Glue federation."""
    
    def __init__(self):
        load_env_file()
        db_config = get_databricks_config()
        
        if not db_config.validate():
            raise ValueError("Databricks configuration is incomplete")
        
        self.host = db_config.host.rstrip("/")
        self.token = db_config.token
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        
        aws_config = get_aws_config()
        self.aws_region = aws_config.region
        self.aws_account_id = get_aws_account_id()
    
    def _api_call(self, method: str, endpoint: str, json_data: dict = None) -> dict:
        """Make an API call to Databricks."""
        url = f"{self.host}/api/2.1/unity-catalog/{endpoint}"
        
        if method == "GET":
            resp = requests.get(url, headers=self.headers)
        elif method == "POST":
            resp = requests.post(url, headers=self.headers, json=json_data)
        elif method == "PATCH":
            resp = requests.patch(url, headers=self.headers, json=json_data)
        elif method == "DELETE":
            resp = requests.delete(url, headers=self.headers)
        else:
            raise ValueError(f"Unknown method: {method}")
        
        return {"status": resp.status_code, "data": resp.json() if resp.text else {}}
    
    def create_storage_credential(
        self,
        name: str,
        iam_role_arn: str,
        comment: str = ""
    ) -> bool:
        """Create a storage credential for S3 access."""
        print(f"\n  Creating storage credential: {name}")
        
        # Delete if exists
        self._api_call("DELETE", f"storage-credentials/{name}?force=true")
        
        resp = self._api_call("POST", "storage-credentials", {
            "name": name,
            "aws_iam_role": {"role_arn": iam_role_arn},
            "comment": comment
        })
        
        if resp["status"] in [200, 201]:
            print(f"  ✓ Storage credential created")
            return True
        else:
            print(f"  ⚠ Failed: {resp['data'].get('message', resp['data'])}")
            return False
    
    def create_service_credential(
        self,
        name: str,
        iam_role_arn: str,
        comment: str = ""
    ) -> bool:
        """
        Create a SERVICE credential for Glue connection.
        
        CRITICAL: Glue connections require SERVICE credentials, not STORAGE credentials!
        """
        print(f"\n  Creating service credential: {name}")
        
        # Delete if exists
        self._api_call("DELETE", f"credentials/{name}?force=true")
        
        resp = self._api_call("POST", "credentials", {
            "name": name,
            "purpose": "SERVICE",
            "aws_iam_role": {"role_arn": iam_role_arn},
            "comment": comment
        })
        
        if resp["status"] in [200, 201]:
            print(f"  ✓ Service credential created")
            return True
        else:
            print(f"  ⚠ Failed: {resp['data'].get('message', resp['data'])}")
            return False
    
    def create_external_location(
        self,
        name: str,
        s3_url: str,
        credential_name: str,
        comment: str = ""
    ) -> bool:
        """Create an external location for S3 access."""
        print(f"\n  Creating external location: {name} -> {s3_url}")
        
        # Delete if exists
        self._api_call("DELETE", f"external-locations/{name}?force=true")
        
        resp = self._api_call("POST", "external-locations", {
            "name": name,
            "url": s3_url,
            "credential_name": credential_name,
            "skip_validation": True,
            "comment": comment
        })
        
        if resp["status"] in [200, 201]:
            print(f"  ✓ External location created")
            return True
        else:
            print(f"  ⚠ Failed: {resp['data'].get('message', resp['data'])}")
            return False
    
    def create_glue_connection(
        self,
        name: str,
        service_credential_name: str,
        comment: str = ""
    ) -> bool:
        """
        Create a Glue connection using a SERVICE credential.
        
        CRITICAL: Must use a SERVICE credential, not a storage credential!
        """
        print(f"\n  Creating Glue connection: {name}")
        
        # Delete if exists
        self._api_call("DELETE", f"connections/{name}")
        
        resp = self._api_call("POST", "connections", {
            "name": name,
            "connection_type": "GLUE",
            "options": {
                "aws_region": self.aws_region,
                "aws_account_id": self.aws_account_id,
                "credential": service_credential_name
            },
            "comment": comment
        })
        
        if resp["status"] in [200, 201]:
            print(f"  ✓ Glue connection created")
            return True
        else:
            print(f"  ⚠ Failed: {resp['data'].get('message', resp['data'])}")
            return False
    
    def create_federated_catalog(
        self,
        name: str,
        connection_name: str,
        authorized_paths: str,
        storage_root: Optional[str] = None,
        comment: str = ""
    ) -> bool:
        """Create a federated catalog pointing to Glue."""
        print(f"\n  Creating federated catalog: {name}")
        
        # Delete if exists
        self._api_call("DELETE", f"catalogs/{name}?force=true")
        
        catalog_def = {
            "name": name,
            "connection_name": connection_name,
            "options": {"authorized_paths": authorized_paths},
            "comment": comment
        }
        
        resp = self._api_call("POST", "catalogs", catalog_def)
        
        if resp["status"] in [200, 201]:
            print(f"  ✓ Federated catalog created")
            
            # Set storage_root if provided
            if storage_root:
                self._api_call("PATCH", f"catalogs/{name}", {
                    "storage_root": storage_root
                })
                print(f"  ✓ Storage root set to {storage_root}")
            
            return True
        else:
            print(f"  ⚠ Failed: {resp['data'].get('message', resp['data'])}")
            return False
    
    def grant_permissions(
        self,
        securable_type: str,
        securable_name: str,
        principal: str,
        permissions: list
    ) -> bool:
        """Grant permissions on a Unity Catalog securable."""
        print(f"\n  Granting {permissions} on {securable_type}/{securable_name} to {principal}")
        
        resp = self._api_call(
            "PATCH",
            f"permissions/{securable_type}/{securable_name}",
            {
                "changes": [{
                    "principal": principal,
                    "add": permissions
                }]
            }
        )
        
        if resp["status"] == 200:
            print(f"  ✓ Permissions granted")
            return True
        else:
            print(f"  ⚠ Failed: {resp['data'].get('message', resp['data'])}")
            return False
    
    def setup_complete_federation(
        self,
        bucket_name: str,
        iam_role_arn: str,
        prefix: str = "iceberg_demo"
    ) -> dict:
        """
        Set up complete Glue federation with all required resources.
        
        Returns dict with created resource names.
        """
        print("\n" + "=" * 60)
        print("DATABRICKS UNITY CATALOG FEDERATION SETUP")
        print("=" * 60)
        print(f"Bucket: {bucket_name}")
        print(f"IAM Role: {iam_role_arn}")
        print(f"AWS Account: {self.aws_account_id}")
        print(f"Region: {self.aws_region}")
        
        # Resource names
        storage_cred_name = f"{prefix}_storage_cred"
        service_cred_name = f"{prefix}_service_cred"
        ext_loc_name = f"{prefix}_ext_loc"
        conn_name = f"{prefix}_glue_conn"
        catalog_name = f"{prefix}_catalog"
        s3_url = f"s3://{bucket_name}/"
        
        success = True
        
        # 1. Storage credential (for S3 access via external location)
        success &= self.create_storage_credential(
            storage_cred_name,
            iam_role_arn,
            "Storage credential for Iceberg demo"
        )
        
        # 2. Service credential (for Glue connection)
        success &= self.create_service_credential(
            service_cred_name,
            iam_role_arn,
            "Service credential for Glue connection"
        )
        
        # 3. External location
        success &= self.create_external_location(
            ext_loc_name,
            s3_url,
            storage_cred_name,
            "External location for Iceberg demo"
        )
        
        # 4. Glue connection
        success &= self.create_glue_connection(
            conn_name,
            service_cred_name,
            "Glue connection for Iceberg partition evolution demo"
        )
        
        # 5. Federated catalog
        success &= self.create_federated_catalog(
            catalog_name,
            conn_name,
            s3_url,
            storage_root=s3_url,
            comment="Federated catalog for Iceberg partition evolution demo"
        )
        
        # 6. Grant permissions to all users
        self.grant_permissions("storage_credential", storage_cred_name, "account users", 
                              ["CREATE_EXTERNAL_LOCATION", "READ_FILES"])
        self.grant_permissions("external_location", ext_loc_name, "account users", 
                              ["READ_FILES"])
        
        print("\n" + "=" * 60)
        if success:
            print("✅ FEDERATION SETUP COMPLETE")
        else:
            print("⚠️ FEDERATION SETUP COMPLETED WITH WARNINGS")
        print("=" * 60)
        
        return {
            "storage_credential": storage_cred_name,
            "service_credential": service_cred_name,
            "external_location": ext_loc_name,
            "connection": conn_name,
            "catalog": catalog_name,
            "bucket": bucket_name,
            "success": success
        }


def main():
    """Run Databricks UC setup interactively."""
    bucket = os.getenv("S3_BUCKET")
    iam_role = os.getenv("UC_IAM_ROLE_ARN")
    prefix = os.getenv("RESOURCE_PREFIX", "iceberg_demo")
    
    if not bucket or not iam_role:
        print("Error: S3_BUCKET and UC_IAM_ROLE_ARN environment variables required")
        print("\nExample:")
        print("  export S3_BUCKET=my-iceberg-bucket")
        print("  export UC_IAM_ROLE_ARN=arn:aws:iam::123456789012:role/my-uc-role")
        return
    
    setup = DatabricksUCSetup()
    result = setup.setup_complete_federation(bucket, iam_role, prefix)
    
    print(f"\nCatalog name: {result['catalog']}")
    print(f"Query: SELECT * FROM {result['catalog']}.<database>.<table>;")


if __name__ == "__main__":
    main()
