import boto3
import json
import uuid
import os
import re
from urllib.parse import urlparse

# ==============================================================================
# ICEBERG METADATA SURGERY
# ------------------------------------------------------------------------------
# Purpose: Manually sanitize Iceberg metadata to remove partition evolution
#          history. This fixes the Databricks UC Federation error:
#          ICEBERG_UNDERGONE_PARTITION_EVOLUTION
# ==============================================================================

# AWS Configuration - Using environment variables set by the bash script
REGION = os.getenv("AWS_REGION", "us-west-2")

# Initialize Clients
session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=REGION
)
s3 = session.client('s3')
glue = session.client('glue')

def perform_surgery():
    database = os.getenv("GLUE_DATABASE")
    table_name = os.getenv("GLUE_TABLE")

    if not database or not table_name:
        print("Error: GLUE_DATABASE and GLUE_TABLE environment variables must be set.")
        return

    print(f"\n--- Starting Metadata Surgery on {database}.{table_name} ---")
    
    # 1. Get current table definition from Glue to find metadata location
    try:
        table_resp = glue.get_table(DatabaseName=database, Name=table_name)
        table_info = table_resp['Table']
        metadata_location = table_info['Parameters'].get('metadata_location')
        
        if not metadata_location:
            print(f"Error: Table {database}.{table_name} does not have a metadata_location parameter.")
            return
            
        print(f"Current metadata location: {metadata_location}")
        
        # Parse S3 path
        parsed_url = urlparse(metadata_location)
        bucket = parsed_url.netloc
        current_metadata_key = parsed_url.path.lstrip('/')
        prefix = os.path.dirname(current_metadata_key) + '/'
        
    except Exception as e:
        print(f"Error fetching table from Glue: {e}")
        return

    # 2. Download latest metadata
    try:
        print(f"Downloading metadata: {current_metadata_key}")
        obj = s3.get_object(Bucket=bucket, Key=current_metadata_key)
        metadata = json.loads(obj['Body'].read().decode('utf-8'))
    except Exception as e:
        print(f"Error downloading metadata from S3: {e}")
        return

    # 3. Determine next version number
    latest_filename = os.path.basename(current_metadata_key)
    version_match = re.match(r'^(\d+)-', latest_filename)
    if version_match:
        current_version = int(version_match.group(1))
        next_version = current_version + 1
    else:
        next_version = 1

    # 4. PERFORM THE SURGERY
    current_specs = metadata.get('partition-specs', [])
    default_spec_id = metadata.get('default-spec-id')
    
    print(f"Metadata check: Found {len(current_specs)} partition specs in history.")
    
    if len(current_specs) <= 1:
        print("Table is already clean (1 or 0 specs). No surgery needed.")
        return

    # Locate the active spec that we want to keep
    active_spec = next((s for s in current_specs if s['spec-id'] == default_spec_id), None)

    if not active_spec:
        print(f"Error: Could not find spec matching default-spec-id {default_spec_id}")
        return

    # REMOVE ALL HISTORY
    metadata['partition-specs'] = [active_spec]
    print(f"Surgery: Deleted {len(current_specs)-1} old specs. Only Spec ID {default_spec_id} remains.")

    # 5. Upload the Sanitized Metadata to S3
    new_filename = f"{next_version:05d}-{uuid.uuid4()}.metadata.json"
    new_metadata_key = f"{prefix}{new_filename}"
    print(f"Uploading sanitized metadata as: {new_filename}")

    try:
        s3.put_object(
            Bucket=bucket,
            Key=new_metadata_key,
            Body=json.dumps(metadata, indent=2),
            ContentType='application/json'
        )
    except Exception as e:
        print(f"Error uploading sanitized metadata: {e}")
        return

    # 6. Update Glue Catalog
    print(f"Updating Glue Table to point to new metadata...")
    
    # Prepare table input for update
    table_input = table_info
    fields_to_remove = [
        'DatabaseName', 'CreateTime', 'UpdateTime', 'CreatedBy', 
        'IsRegisteredWithLakeFormation', 'CatalogId', 'FederatedTable',
        'VersionId', 'IsMultiDialectView'
    ]
    for field in fields_to_remove:
        table_input.pop(field, None)

    table_input['Parameters']['metadata_location'] = f"s3://{bucket}/{new_metadata_key}"

    try:
        glue.update_table(
            DatabaseName=database,
            TableInput=table_input
        )
        print(f"\nSUCCESS: Table {database}.{table_name} is now clean for Databricks.")
        print(f"Rollback: Change Glue metadata_location back to {metadata_location}")
    except Exception as e:
        print(f"Error updating Glue table: {e}")

if __name__ == "__main__":
    perform_surgery()
