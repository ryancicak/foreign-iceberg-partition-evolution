"""
Iceberg Metadata Surgery - Core Fix for Partition Evolution Error.

This script surgically removes old partition specs from Iceberg metadata.json
to fix the Databricks ICEBERG_UNDERGONE_PARTITION_EVOLUTION error.

IMPORTANT: Run rewrite_data_files() BEFORE running this surgery to ensure
all physical data files use the latest partition spec.
"""

import boto3
import json
import uuid
import os
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from typing import Optional
from config import get_aws_config, get_glue_config, load_env_file


class MetadataSurgery:
    """Performs metadata surgery on Iceberg tables to remove partition evolution history."""
    
    def __init__(self):
        load_env_file()
        aws_config = get_aws_config()
        
        self.session = boto3.Session(
            aws_access_key_id=aws_config.access_key_id,
            aws_secret_access_key=aws_config.secret_access_key,
            region_name=aws_config.region
        )
        self.s3 = self.session.client('s3')
        self.glue = self.session.client('glue')
        
        # Rollback info storage
        self.rollback_dir = Path(__file__).parent.parent / "rollback"
        self.rollback_dir.mkdir(exist_ok=True)
    
    def analyze_table(self, database: str, table_name: str) -> dict:
        """Analyze an Iceberg table and return its partition spec info."""
        print(f"\n📊 Analyzing {database}.{table_name}...")
        
        try:
            table_resp = self.glue.get_table(DatabaseName=database, Name=table_name)
            table_info = table_resp['Table']
            metadata_location = table_info['Parameters'].get('metadata_location')
            
            if not metadata_location:
                return {"error": "No metadata_location found - is this an Iceberg table?"}
            
            # Parse S3 path
            parsed_url = urlparse(metadata_location)
            bucket = parsed_url.netloc
            metadata_key = parsed_url.path.lstrip('/')
            
            # Download metadata
            obj = self.s3.get_object(Bucket=bucket, Key=metadata_key)
            metadata = json.loads(obj['Body'].read().decode('utf-8'))
            
            specs = metadata.get('partition-specs', [])
            default_spec_id = metadata.get('default-spec-id')
            
            return {
                "database": database,
                "table": table_name,
                "metadata_location": metadata_location,
                "bucket": bucket,
                "metadata_key": metadata_key,
                "partition_specs": specs,
                "default_spec_id": default_spec_id,
                "num_specs": len(specs),
                "needs_surgery": len(specs) > 1,
                "metadata": metadata,
                "table_info": table_info
            }
            
        except Exception as e:
            return {"error": str(e)}
    
    def perform_surgery(
        self, 
        database: str, 
        table_name: str, 
        dry_run: bool = False
    ) -> bool:
        """
        Perform metadata surgery to remove old partition specs.
        
        Args:
            database: Glue database name
            table_name: Glue table name
            dry_run: If True, only show what would be done without making changes
        
        Returns:
            True if successful, False otherwise
        """
        print("\n" + "=" * 60)
        print("🔪 ICEBERG METADATA SURGERY")
        print("=" * 60)
        print(f"Table: {database}.{table_name}")
        print(f"Mode: {'DRY RUN (no changes)' if dry_run else 'LIVE'}")
        
        # Analyze table
        analysis = self.analyze_table(database, table_name)
        
        if "error" in analysis:
            print(f"\n❌ Error: {analysis['error']}")
            return False
        
        print(f"\nCurrent metadata location: {analysis['metadata_location']}")
        print(f"Partition specs found: {analysis['num_specs']}")
        
        for spec in analysis['partition_specs']:
            fields = [f['name'] for f in spec.get('fields', [])]
            marker = " ← ACTIVE" if spec['spec-id'] == analysis['default_spec_id'] else " ← GHOST"
            print(f"  - Spec ID {spec['spec-id']}: {fields}{marker}")
        
        if not analysis['needs_surgery']:
            print("\n✅ Table is already clean (1 or 0 specs). No surgery needed.")
            return True
        
        # Find active spec
        active_spec = next(
            (s for s in analysis['partition_specs'] if s['spec-id'] == analysis['default_spec_id']), 
            None
        )
        
        if not active_spec:
            print(f"\n❌ Error: Could not find spec matching default-spec-id {analysis['default_spec_id']}")
            return False
        
        # Prepare surgery
        metadata = analysis['metadata']
        old_specs_count = len(analysis['partition_specs']) - 1
        
        print(f"\n🔪 Surgery plan:")
        print(f"   - Remove {old_specs_count} old partition spec(s)")
        print(f"   - Keep only Spec ID {analysis['default_spec_id']}")
        
        if dry_run:
            print("\n🔍 DRY RUN - No changes made.")
            print("   Run without --dry-run to apply changes.")
            return True
        
        # Perform surgery
        metadata['partition-specs'] = [active_spec]
        
        # Determine new metadata filename
        latest_filename = os.path.basename(analysis['metadata_key'])
        version_match = re.match(r'^(\d+)-', latest_filename)
        current_version = int(version_match.group(1)) if version_match else 0
        next_version = current_version + 1
        
        new_filename = f"{next_version:05d}-{uuid.uuid4()}.metadata.json"
        prefix = os.path.dirname(analysis['metadata_key']) + '/'
        new_metadata_key = f"{prefix}{new_filename}"
        new_metadata_location = f"s3://{analysis['bucket']}/{new_metadata_key}"
        
        # Save rollback info
        rollback_file = self.rollback_dir / f"{database}_{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        rollback_info = {
            "database": database,
            "table": table_name,
            "original_metadata_location": analysis['metadata_location'],
            "new_metadata_location": new_metadata_location,
            "surgery_timestamp": datetime.now().isoformat(),
            "specs_removed": old_specs_count,
            "rollback_command": f"aws glue update-table --database-name {database} --table-input '{{\"Name\": \"{table_name}\", \"Parameters\": {{\"metadata_location\": \"{analysis['metadata_location']}\"}}}}'"
        }
        
        with open(rollback_file, 'w') as f:
            json.dump(rollback_info, f, indent=2)
        print(f"\n📝 Rollback info saved to: {rollback_file}")
        
        # Upload new metadata
        print(f"\n📤 Uploading sanitized metadata: {new_filename}")
        try:
            self.s3.put_object(
                Bucket=analysis['bucket'],
                Key=new_metadata_key,
                Body=json.dumps(metadata, indent=2),
                ContentType='application/json'
            )
        except Exception as e:
            print(f"❌ Error uploading metadata: {e}")
            return False
        
        # Update Glue catalog
        print(f"📝 Updating Glue catalog to point to new metadata...")
        try:
            table_input = analysis['table_info']
            # Remove read-only fields
            fields_to_remove = [
                'DatabaseName', 'CreateTime', 'UpdateTime', 'CreatedBy',
                'IsRegisteredWithLakeFormation', 'CatalogId', 'FederatedTable',
                'VersionId', 'IsMultiDialectView'
            ]
            for field in fields_to_remove:
                table_input.pop(field, None)
            
            table_input['Parameters']['metadata_location'] = new_metadata_location
            
            self.glue.update_table(
                DatabaseName=database,
                TableInput=table_input
            )
        except Exception as e:
            print(f"❌ Error updating Glue table: {e}")
            print(f"   You may need to manually update metadata_location to: {new_metadata_location}")
            return False
        
        print("\n" + "=" * 60)
        print("✅ SURGERY COMPLETE!")
        print("=" * 60)
        print(f"Removed {old_specs_count} old partition spec(s).")
        print(f"New metadata: {new_metadata_location}")
        print(f"\n🔄 To rollback, run:")
        print(f"   python metadata_surgery.py --rollback {rollback_file}")
        
        return True
    
    def rollback(self, rollback_file: str) -> bool:
        """Rollback a previous surgery using the rollback file."""
        print("\n" + "=" * 60)
        print("🔄 ROLLBACK SURGERY")
        print("=" * 60)
        
        try:
            with open(rollback_file) as f:
                rollback_info = json.load(f)
        except Exception as e:
            print(f"❌ Error reading rollback file: {e}")
            return False
        
        database = rollback_info['database']
        table_name = rollback_info['table']
        original_location = rollback_info['original_metadata_location']
        
        print(f"Table: {database}.{table_name}")
        print(f"Restoring to: {original_location}")
        
        try:
            table_resp = self.glue.get_table(DatabaseName=database, Name=table_name)
            table_input = table_resp['Table']
            
            fields_to_remove = [
                'DatabaseName', 'CreateTime', 'UpdateTime', 'CreatedBy',
                'IsRegisteredWithLakeFormation', 'CatalogId', 'FederatedTable',
                'VersionId', 'IsMultiDialectView'
            ]
            for field in fields_to_remove:
                table_input.pop(field, None)
            
            table_input['Parameters']['metadata_location'] = original_location
            
            self.glue.update_table(
                DatabaseName=database,
                TableInput=table_input
            )
            
            print("\n✅ Rollback complete!")
            return True
            
        except Exception as e:
            print(f"❌ Error during rollback: {e}")
            return False


def main():
    """Run metadata surgery from command line."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Iceberg Metadata Surgery - Fix partition evolution errors"
    )
    parser.add_argument("--database", "-d", help="Glue database name")
    parser.add_argument("--table", "-t", help="Glue table name")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")
    parser.add_argument("--rollback", help="Rollback file path to restore previous metadata")
    parser.add_argument("--analyze", action="store_true", help="Only analyze the table, don't perform surgery")
    
    args = parser.parse_args()
    
    surgery = MetadataSurgery()
    
    if args.rollback:
        surgery.rollback(args.rollback)
        return
    
    # Get database and table from args or environment
    database = args.database or os.getenv("GLUE_DATABASE")
    table = args.table or os.getenv("GLUE_TABLE")
    
    if not database or not table:
        print("Error: Database and table required.")
        print("  Use --database and --table arguments, or set GLUE_DATABASE and GLUE_TABLE env vars")
        parser.print_help()
        return
    
    if args.analyze:
        analysis = surgery.analyze_table(database, table)
        if "error" in analysis:
            print(f"Error: {analysis['error']}")
        else:
            print(f"\nTable: {database}.{table}")
            print(f"Metadata: {analysis['metadata_location']}")
            print(f"Partition specs: {analysis['num_specs']}")
            for spec in analysis['partition_specs']:
                fields = [f['name'] for f in spec.get('fields', [])]
                print(f"  - Spec ID {spec['spec-id']}: {fields}")
            print(f"Needs surgery: {'Yes' if analysis['needs_surgery'] else 'No'}")
        return
    
    surgery.perform_surgery(database, table, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
