#!/bin/bash

# ==============================================================================
# METADATA SURGERY AUTOMATION SCRIPT
# ------------------------------------------------------------------------------
# Purpose: Reproduce and Fix Iceberg Partition Evolution errors in Databricks
#          UC Federation without moving 120+ trillion rows.
# ==============================================================================

set -e

echo "----------------------------------------------------------------"
echo "WELCOME TO THE ICEBERG METADATA SURGEON"
echo "----------------------------------------------------------------"

# 1. GATHER ALL REQUIRED INFORMATION
read -p "Enter Databricks Workspace URL (e.g. https://dbc-xxx.cloud.databricks.com): " DB_HOST
read -sp "Enter Databricks PAT (Token): " DB_TOKEN
echo ""
read -p "Enter AWS Access Key ID: " AWS_ACCESS_KEY
read -sp "Enter AWS Secret Access Key: " AWS_SECRET_KEY
echo ""
read -p "Enter AWS Region (e.g. us-west-2): " AWS_REGION
AWS_REGION=${AWS_REGION:-us-west-2}

read -p "Enter EMR Cluster ID (e.g. j-XXXXX): " EMR_ID
read -p "Enter Path to EMR .pem SSH Key (e.g. ~/Downloads/key.pem): " PEM_PATH
PEM_PATH="${PEM_PATH/#\~/$HOME}" # Expand tilde if used

# NEW: Prompt for Full Table Name
echo ""
echo "OPTIONAL: Provide a specific table to fix, or press [Enter] for the full demo."
read -p "Full Table Name in Glue (e.g. my_database.my_table): " TARGET_TABLE

# Export credentials for Python surgery script
export AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY
export AWS_SECRET_ACCESS_KEY=$AWS_SECRET_KEY
export AWS_DEFAULT_REGION=$AWS_REGION
export AWS_REGION=$AWS_REGION
export DATABRICKS_HOST=$DB_HOST
export DATABRICKS_TOKEN=$DB_TOKEN

# Define location of internal scripts
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ -n "$TARGET_TABLE" ]; then
    # TARGETED FIX MODE
    GLUE_DATABASE=$(echo $TARGET_TABLE | cut -d'.' -f1)
    GLUE_TABLE=$(echo $TARGET_TABLE | cut -d'.' -f2)
    
    export GLUE_DATABASE=$GLUE_DATABASE
    export GLUE_TABLE=$GLUE_TABLE

    echo -e "\n----------------------------------------------------------------"
    echo "🛠️  TARGETED FIX MODE: $TARGET_TABLE"
    echo "----------------------------------------------------------------"
    echo "Performing Metadata Surgery on your existing table..."
    python3 "$SCRIPT_DIR/internal/metadata_surgery.py"
    
    echo -e "\n----------------------------------------------------------------"
    echo "COMPLETED!"
    echo "If you have already unified the data via 'rewrite_data_files', "
    echo "you can now query $TARGET_TABLE in Databricks successfully."
    echo "----------------------------------------------------------------"
    exit 0
fi

# FULL DEMO MODE ("TRY IT" PIECE)
echo -e "\n----------------------------------------------------------------"
echo "🚀 FULL DEMO MODE: REPRODUCING AND FIXING"
echo "----------------------------------------------------------------"

# Default Demo Config
export GLUE_DATABASE="glue_uc_demo_db_v2"
export GLUE_TABLE="partition_evolution_test"
CATALOG="glue_federation_catalog_v2"
BUCKET="uc-federation-demo-v2-5ff618be"
WAREHOUSE_PATH="s3://$BUCKET/emr_warehouse/"

# Configuration string for Spark SQL
ICEBERG_CONFS="--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
--conf spark.sql.catalog.glue_catalog.warehouse=$WAREHOUSE_PATH"

echo -e "\n----------------------------------------------------------------"
echo "PHASE 1: REPRODUCING THE ERROR"
echo "----------------------------------------------------------------"
echo "Creating an evolved Iceberg table on EMR..."

MASTER_DNS=$(aws emr describe-cluster --cluster-id $EMR_ID --region $AWS_REGION --query "Cluster.MasterPublicDnsName" --output text)
chmod 600 "$PEM_PATH"

ssh -i "$PEM_PATH" -o StrictHostKeyChecking=no hadoop@$MASTER_DNS "spark-sql $ICEBERG_CONFS -e \"
DROP TABLE IF EXISTS glue_catalog.$GLUE_DATABASE.$GLUE_TABLE;
CREATE TABLE glue_catalog.$GLUE_DATABASE.$GLUE_TABLE (id INT, category STRING, product STRING, ts TIMESTAMP) 
USING iceberg PARTITIONED BY (category) TBLPROPERTIES ('format-version'='2');
INSERT INTO glue_catalog.$GLUE_DATABASE.$GLUE_TABLE VALUES (1, 'Electronics', 'Laptop', CURRENT_TIMESTAMP());
ALTER TABLE glue_catalog.$GLUE_DATABASE.$GLUE_TABLE ADD PARTITION FIELD product;
INSERT INTO glue_catalog.$GLUE_DATABASE.$GLUE_TABLE VALUES (2, 'Furniture', 'Chair', CURRENT_TIMESTAMP());
\""

echo -e "\n🛑 REPRODUCED!"
echo "----------------------------------------------------------------"
echo "ACTION REQUIRED: Go to Databricks and run this query:"
echo -e "\033[1;33mSELECT * FROM $CATALOG.$GLUE_DATABASE.$GLUE_TABLE;\033[0m"
echo "Confirm you see the ICEBERG_UNDERGONE_PARTITION_EVOLUTION error."
read -p "Press [Enter] once you have confirmed the error..."

echo -e "\n----------------------------------------------------------------"
echo "PHASE 2: THE ENGINEERING WORKAROUND (STEP 1)"
echo "----------------------------------------------------------------"
echo "Running 'rewrite_data_files' on EMR to unify physical files..."

ssh -i "$PEM_PATH" -o StrictHostKeyChecking=no hadoop@$MASTER_DNS "spark-sql $ICEBERG_CONFS -e \"
CALL glue_catalog.system.rewrite_data_files(
  table => '$GLUE_DATABASE.$GLUE_TABLE',
  where => 'ts <= current_timestamp()',
  options => map('rewrite-all', 'true')
);
\""

echo -e "\n🤔 STEP 1 COMPLETE!"
echo "----------------------------------------------------------------"
echo "ACTION REQUIRED: Go back to Databricks and RE-RUN the query."
echo "You will see that the error STILL EXISTS because the metadata history is tainted."
read -p "Press [Enter] once you have confirmed the error is still there..."

echo -e "\n----------------------------------------------------------------"
echo "PHASE 3: THE METADATA SURGERY (STEP 2)"
echo "----------------------------------------------------------------"
echo "Performing Metadata Surgery..."

python3 "$SCRIPT_DIR/internal/metadata_surgery.py"

echo -e "\n✨ SURGERY COMPLETE!"
echo "----------------------------------------------------------------"
echo "FINAL ACTION: Go to Databricks and run the query ONE LAST TIME."
echo -e "\033[1;32mSELECT * FROM $CATALOG.$GLUE_DATABASE.$GLUE_TABLE;\033[0m"
echo "The error is gone! The data is unified, and the metadata is clean."
echo "----------------------------------------------------------------"
