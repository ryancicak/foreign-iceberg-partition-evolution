import boto3
import time

# ==============================================================================
# EMR PARTITION EVOLUTION REPRODUCTION
# ------------------------------------------------------------------------------
# This script automatically submits a Spark SQL job to your EMR cluster to:
# 1. Create an Iceberg table (Partitioned by category)
# 2. Insert data
# 3. Evolve the partition spec (Add product)
# 4. Insert more data
# ==============================================================================

# Configuration
CLUSTER_ID = "j-2A9YG8FYJ0GTV"
REGION = "us-west-2"
DATABASE = "glue_uc_demo_db_v2"
TABLE = "partition_evolution_test"
WAREHOUSE_PATH = "s3://uc-federation-demo-v2-5ff618be/emr_warehouse/"

emr = boto3.client('emr', region_name=REGION)

def run_emr_step():
    spark_sql_command = f"""
    DROP TABLE IF EXISTS glue_catalog.{DATABASE}.{TABLE};

    CREATE TABLE glue_catalog.{DATABASE}.{TABLE} (
        id INT,
        category STRING,
        product STRING,
        ts TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (category)
    TBLPROPERTIES ('format-version'='2');

    INSERT INTO glue_catalog.{DATABASE}.{TABLE} 
    VALUES (1, 'Electronics', 'Laptop', CURRENT_TIMESTAMP());

    -- EVOLVE THE SPEC
    ALTER TABLE glue_catalog.{DATABASE}.{TABLE} 
    ADD PARTITION FIELD product;

    INSERT INTO glue_catalog.{DATABASE}.{TABLE} 
    VALUES (2, 'Furniture', 'Chair', CURRENT_TIMESTAMP());
    """

    print(f"Submitting Evolution Step to EMR Cluster: {CLUSTER_ID}")
    
    response = emr.add_job_flow_steps(
        JobFlowId=CLUSTER_ID,
        Steps=[
            {
                'Name': 'Iceberg Partition Evolution Repro',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-sql',
                        '--conf', 'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
                        '--conf', 'spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog',
                        '--conf', 'spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog',
                        '--conf', 'spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO',
                        '--conf', f'spark.sql.catalog.glue_catalog.warehouse={WAREHOUSE_PATH}',
                        '-e', spark_sql_command
                    ]
                }
            }
        ]
    )

    step_id = response['StepIds'][0]
    print(f"Step Submitted (ID: {step_id}). Waiting for completion...")

    while True:
        resp = emr.describe_step(ClusterId=CLUSTER_ID, StepId=step_id)
        status = resp['Step']['Status']['State']
        if status in ['COMPLETED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(5)

    if status == 'COMPLETED':
        print(f"\nSUCCESS: Table '{DATABASE}.{TABLE}' has been evolved on EMR.")
        print("Now go to Databricks and try to query it to see the ICEBERG_UNDERGONE_PARTITION_EVOLUTION error!")
    else:
        reason = resp['Step']['Status'].get('FailureDetails', {}).get('Reason', 'Unknown')
        print(f"\nFAILED: EMR step failed with status {status}. Reason: {reason}")

if __name__ == "__main__":
    run_emr_step()

