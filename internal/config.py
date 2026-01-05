"""
Shared configuration loader for the Iceberg Metadata Surgeon.
Loads environment variables from .env file and provides validated config.
"""

import os
from pathlib import Path
from typing import Optional
from dataclasses import dataclass


@dataclass
class AWSConfig:
    access_key_id: str
    secret_access_key: str
    region: str
    
    def validate(self) -> bool:
        if not self.access_key_id or not self.secret_access_key:
            print("❌ Error: AWS credentials not configured.")
            print("   Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.")
            return False
        return True


@dataclass
class DatabricksConfig:
    host: str
    token: str
    
    def validate(self) -> bool:
        if not self.host or not self.token:
            print("❌ Error: Databricks credentials not configured.")
            print("   Set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables.")
            return False
        return True


@dataclass
class EMRConfig:
    cluster_id: str
    pem_path: str
    
    def validate(self) -> bool:
        if not self.cluster_id:
            print("❌ Error: EMR cluster ID not configured.")
            print("   Set EMR_CLUSTER_ID environment variable.")
            return False
        if self.pem_path and not Path(self.pem_path).exists():
            print(f"❌ Error: PEM file not found: {self.pem_path}")
            return False
        return True


@dataclass
class GlueConfig:
    database: str
    table: str
    
    def validate(self) -> bool:
        if not self.database or not self.table:
            print("❌ Error: Glue table not configured.")
            print("   Set GLUE_DATABASE and GLUE_TABLE environment variables.")
            return False
        return True


def load_env_file(env_path: Optional[str] = None) -> None:
    """Load environment variables from .env file."""
    if env_path is None:
        # Look for .env in project root
        project_root = Path(__file__).parent.parent
        env_path = project_root / ".env"
    else:
        env_path = Path(env_path)
    
    if not env_path.exists():
        return
    
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                # Only set if not already set (allow env override)
                if key not in os.environ:
                    os.environ[key] = value


def get_aws_config() -> AWSConfig:
    """Get AWS configuration from environment."""
    load_env_file()
    return AWSConfig(
        access_key_id=os.getenv("AWS_ACCESS_KEY_ID", ""),
        secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        region=os.getenv("AWS_DEFAULT_REGION", os.getenv("AWS_REGION", "us-west-2"))
    )


def get_databricks_config() -> DatabricksConfig:
    """Get Databricks configuration from environment."""
    load_env_file()
    return DatabricksConfig(
        host=os.getenv("DATABRICKS_HOST", ""),
        token=os.getenv("DATABRICKS_TOKEN", "")
    )


def get_emr_config() -> EMRConfig:
    """Get EMR configuration from environment."""
    load_env_file()
    pem_path = os.getenv("EMR_PEM_PATH", "")
    # Expand ~ to home directory
    if pem_path.startswith("~"):
        pem_path = str(Path.home() / pem_path[2:])
    return EMRConfig(
        cluster_id=os.getenv("EMR_CLUSTER_ID", ""),
        pem_path=pem_path
    )


def get_glue_config() -> GlueConfig:
    """Get Glue table configuration from environment."""
    load_env_file()
    return GlueConfig(
        database=os.getenv("GLUE_DATABASE", ""),
        table=os.getenv("GLUE_TABLE", "")
    )


def get_aws_account_id() -> str:
    """Get AWS account ID from STS."""
    import boto3
    aws_config = get_aws_config()
    sts = boto3.client(
        'sts',
        aws_access_key_id=aws_config.access_key_id,
        aws_secret_access_key=aws_config.secret_access_key,
        region_name=aws_config.region
    )
    return sts.get_caller_identity()["Account"]


def print_config_summary():
    """Print a summary of current configuration."""
    load_env_file()
    
    print("\n" + "=" * 60)
    print("CONFIGURATION SUMMARY")
    print("=" * 60)
    
    aws = get_aws_config()
    print(f"\nAWS:")
    print(f"  Region: {aws.region}")
    print(f"  Access Key: {aws.access_key_id[:8]}..." if aws.access_key_id else "  Access Key: NOT SET")
    
    db = get_databricks_config()
    print(f"\nDatabricks:")
    print(f"  Host: {db.host}" if db.host else "  Host: NOT SET")
    print(f"  Token: {db.token[:10]}..." if db.token else "  Token: NOT SET")
    
    emr = get_emr_config()
    print(f"\nEMR:")
    print(f"  Cluster ID: {emr.cluster_id}" if emr.cluster_id else "  Cluster ID: NOT SET")
    print(f"  PEM Path: {emr.pem_path}" if emr.pem_path else "  PEM Path: NOT SET")
    
    glue = get_glue_config()
    print(f"\nGlue:")
    print(f"  Database: {glue.database}" if glue.database else "  Database: NOT SET")
    print(f"  Table: {glue.table}" if glue.table else "  Table: NOT SET")
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    print_config_summary()

