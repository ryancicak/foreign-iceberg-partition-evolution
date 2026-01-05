"""
EMR Partition Evolution Reproduction Script.

Submits a Spark SQL job to an EMR cluster to create an Iceberg table
that triggers the ICEBERG_UNDERGONE_PARTITION_EVOLUTION error in Databricks.
"""

import boto3
import time
import os
from config import get_aws_config, get_emr_config, get_glue_config, load_env_file


class EMREvolutionReproducer:
    """Reproduces partition evolution on EMR."""
    
    def __init__(self):
        load_env_file()
        aws_config = get_aws_config()
        emr_config = get_emr_config()
        
        if not aws_config.validate():
            raise ValueError("AWS configuration is incomplete")
        
        self.session = boto3.Session(
            aws_access_key_id=aws_config.access_key_id,
            aws_secret_access_key=aws_config.secret_access_key,
            region_name=aws_config.region
        )
        self.emr = self.session.client('emr')
        self.region = aws_config.region
        self.cluster_id = emr_config.cluster_id
    
    def get_master_dns(self) -> str:
        """Get the EMR master node DNS."""
        resp = self.emr.describe_cluster(ClusterId=self.cluster_id)
        return resp['Cluster']['MasterPublicDnsName']
    
    def submit_evolution_step(
        self,
        database: str,
        table: str,
        warehouse_path: str
    ) -> str:
        """Submit a Spark SQL step to create an evolved table."""
        
        spark_sql_command = f"""
        DROP TABLE IF EXISTS glue_catalog.{database}.{table};

        CREATE TABLE glue_catalog.{database}.{table} (
            id INT,
            category STRING,
            product STRING,
            ts TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (category)
        TBLPROPERTIES ('format-version'='2');

        INSERT INTO glue_catalog.{database}.{table} 
        VALUES 
            (1, 'Electronics', 'Laptop', CURRENT_TIMESTAMP()),
            (2, 'Electronics', 'Phone', CURRENT_TIMESTAMP());

        -- EVOLVE THE PARTITION SPEC
        ALTER TABLE glue_catalog.{database}.{table} 
        ADD PARTITION FIELD product;

        INSERT INTO glue_catalog.{database}.{table} 
        VALUES 
            (3, 'Furniture', 'Chair', CURRENT_TIMESTAMP()),
            (4, 'Furniture', 'Desk', CURRENT_TIMESTAMP());
        """

        print(f"Submitting evolution step to EMR cluster: {self.cluster_id}")
        
        response = self.emr.add_job_flow_steps(
            JobFlowId=self.cluster_id,
            Steps=[
                {
                    'Name': 'Iceberg Partition Evolution Reproduction',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': [
                            'spark-sql',
                            '--conf', 'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
                            '--conf', 'spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog',
                            '--conf', 'spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog',
                            '--conf', 'spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO',
                            '--conf', f'spark.sql.catalog.glue_catalog.warehouse={warehouse_path}',
                            '-e', spark_sql_command
                        ]
                    }
                }
            ]
        )

        step_id = response['StepIds'][0]
        print(f"Step submitted (ID: {step_id})")
        return step_id
    
    def wait_for_step(self, step_id: str, timeout_seconds: int = 600) -> bool:
        """Wait for a step to complete."""
        print("Waiting for step completion...")
        
        start_time = time.time()
        while True:
            if time.time() - start_time > timeout_seconds:
                print("Timeout waiting for step")
                return False
            
            resp = self.emr.describe_step(ClusterId=self.cluster_id, StepId=step_id)
            status = resp['Step']['Status']['State']
            
            if status == 'COMPLETED':
                print(f"✓ Step completed successfully")
                return True
            elif status in ['FAILED', 'CANCELLED']:
                reason = resp['Step']['Status'].get('FailureDetails', {}).get('Reason', 'Unknown')
                print(f"✗ Step failed: {reason}")
                return False
            
            print(f"  Status: {status}")
            time.sleep(10)
    
    def run_rewrite_data_files(
        self,
        database: str,
        table: str,
        warehouse_path: str
    ) -> bool:
        """Run rewrite_data_files to unify physical data."""
        
        spark_sql_command = f"""
        CALL glue_catalog.system.rewrite_data_files(
            table => '{database}.{table}',
            options => map('rewrite-all', 'true')
        );
        """

        print(f"Submitting rewrite_data_files step...")
        
        response = self.emr.add_job_flow_steps(
            JobFlowId=self.cluster_id,
            Steps=[
                {
                    'Name': 'Iceberg Rewrite Data Files',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': [
                            'spark-sql',
                            '--conf', 'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
                            '--conf', 'spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog',
                            '--conf', 'spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog',
                            '--conf', 'spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO',
                            '--conf', f'spark.sql.catalog.glue_catalog.warehouse={warehouse_path}',
                            '-e', spark_sql_command
                        ]
                    }
                }
            ]
        )

        step_id = response['StepIds'][0]
        return self.wait_for_step(step_id)


def main():
    """Run EMR evolution reproduction."""
    glue_config = get_glue_config()
    
    database = glue_config.database or os.getenv("GLUE_DATABASE")
    table = glue_config.table or os.getenv("GLUE_TABLE", "partition_evolution_test")
    bucket = os.getenv("S3_BUCKET")
    
    if not database or not bucket:
        print("Error: GLUE_DATABASE and S3_BUCKET environment variables required")
        return
    
    warehouse_path = f"s3://{bucket}/warehouse/"
    
    reproducer = EMREvolutionReproducer()
    
    print("\n" + "=" * 60)
    print("EMR PARTITION EVOLUTION REPRODUCTION")
    print("=" * 60)
    print(f"Cluster: {reproducer.cluster_id}")
    print(f"Database: {database}")
    print(f"Table: {table}")
    print(f"Warehouse: {warehouse_path}")
    
    step_id = reproducer.submit_evolution_step(database, table, warehouse_path)
    success = reproducer.wait_for_step(step_id)
    
    if success:
        print("\n" + "=" * 60)
        print("✅ TABLE CREATED WITH PARTITION EVOLUTION")
        print("=" * 60)
        print(f"\nNow go to Databricks and try to query:")
        print(f"  SELECT * FROM <catalog>.{database}.{table};")
        print("\nYou should see: ICEBERG_UNDERGONE_PARTITION_EVOLUTION error")


if __name__ == "__main__":
    main()
