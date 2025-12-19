import os
import boto3
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog

def setup_v2_databricks():
    env_path = '/Users/ryan.cicak/Documents/foreign-iceberg-partition-evolution/.env'
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'): continue
                if '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value.strip('"').strip("'")

    w = WorkspaceClient()
    
    # V2 Config
    bucket_name = "uc-federation-demo-v2-5ff618be"
    db_name = "glue_uc_demo_db_v2"
    
    cred_name = "aws_glue_credential_v2" 
    conn_name = "aws_glue_connection_v2" 
    ext_loc_name = "glue_demo_ext_loc_v2"
    cat_name = "glue_federation_catalog_v2"

    # 0. Update Connection to US-WEST-2
    print(f"Updating Connection {conn_name} to us-west-2...")
    try:
        w.connections.update(
            name=conn_name,
            options={
                "aws_region": "us-west-2",
                "aws_account_id": boto3.client('sts').get_caller_identity()["Account"],
                "credential": cred_name
            }
        )
    except Exception as e:
        print(f"Connection update failed: {e}")
        try:
             w.connections.create(
                name=conn_name,
                connection_type=catalog.ConnectionType.GLUE,
                options={
                    "aws_region": "us-west-2",
                    "aws_account_id": boto3.client('sts').get_caller_identity()["Account"],
                    "credential": cred_name
                },
                comment="Connection V2 US-WEST-2"
            )
        except Exception as ce:
            print(f"Connection creation failed: {ce}")

    # 1. External Location V2
    print(f"Creating External Location: {ext_loc_name}")
    try:
        try:
             w.external_locations.delete(name=ext_loc_name, force=True)
             print(f"Deleted existing External Location {ext_loc_name}")
        except Exception:
             pass
             
        w.external_locations.create(
             name=ext_loc_name,
             url=f"s3://{bucket_name}/",
             credential_name=cred_name,
             skip_validation=True,
             comment="Ext Loc V2"
        )
        print(f"Created External Location {ext_loc_name}")
    except Exception as e:
        print(f"Failed to create ext loc: {e}")

    # 2. Catalog V2
    print(f"Creating Catalog: {cat_name}")
    try:
        try:
            w.catalogs.delete(name=cat_name, force=True)
            print(f"Deleted existing Catalog {cat_name}")
        except Exception:
            pass

        w.catalogs.create(
            name=cat_name,
            connection_name=conn_name,
            options={
                "authorized_paths": f"s3://{bucket_name}/"
            },
            comment="Foreign Catalog V2"
        )
        print(f"Created Catalog {cat_name}")
    except Exception as e:
        print(f"Failed to create catalog: {e}")

    # 3. Grant Permissions
    user_email = "ryan.cicak@databricks.com"
    print(f"Granting permissions to {user_email}...")
    try:
        # Catalog
        w.grants.update(
            securable_type="catalog",
            full_name=cat_name,
            changes=[
                catalog.PermissionsChange(
                    principal=user_email,
                    add=[catalog.Privilege.USE_CATALOG, catalog.Privilege.USE_SCHEMA, catalog.Privilege.SELECT]
                )
            ]
        )
        # External Location
        w.grants.update(
            securable_type="external_location",
            full_name=ext_loc_name,
            changes=[
                catalog.PermissionsChange(
                    principal=user_email,
                    add=[catalog.Privilege.READ_FILES]
                )
            ]
        )
        print("Permissions granted.")
    except Exception as e:
        print(f"Grant permissions failed: {e}")

if __name__ == "__main__":
    setup_v2_databricks()

