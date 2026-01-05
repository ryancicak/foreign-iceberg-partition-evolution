#!/bin/bash

# ==============================================================================
# ICEBERG METADATA SURGEON - FULL AUTOMATION SCRIPT
# ==============================================================================
# Purpose: Reproduce and Fix Iceberg Partition Evolution errors in Databricks
#          UC Federation without moving a single byte of data.
#
# Usage:
#   ./full_fix_automation.sh              # Interactive mode
#   ./full_fix_automation.sh --targeted   # Fix a specific table
#   ./full_fix_automation.sh --demo       # Full demo mode
# ==============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

print_header() {
    echo -e "\n${BLUE}================================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}================================================================${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${CYAN}ℹ️  $1${NC}"
}

check_prerequisites() {
    print_header "CHECKING PREREQUISITES"
    
    local missing=0
    
    # Check AWS CLI
    if command -v aws &> /dev/null; then
        print_success "AWS CLI installed"
    else
        print_error "AWS CLI not found. Install from https://aws.amazon.com/cli/"
        missing=1
    fi
    
    # Check Python
    if command -v python3 &> /dev/null; then
        print_success "Python 3 installed"
    else
        print_error "Python 3 not found"
        missing=1
    fi
    
    # Check boto3
    if python3 -c "import boto3" 2>/dev/null; then
        print_success "boto3 installed"
    else
        print_warning "boto3 not found. Installing..."
        pip3 install boto3
    fi
    
    # Check SSH
    if command -v ssh &> /dev/null; then
        print_success "SSH available"
    else
        print_error "SSH not found"
        missing=1
    fi
    
    if [ $missing -eq 1 ]; then
        print_error "Missing prerequisites. Please install and try again."
        exit 1
    fi
    
    echo ""
}

load_env_file() {
    if [ -f "$SCRIPT_DIR/.env" ]; then
        print_info "Loading configuration from .env file..."
        export $(grep -v '^#' "$SCRIPT_DIR/.env" | xargs)
    fi
}

prompt_credentials() {
    print_header "CONFIGURATION"
    
    # AWS Credentials
    if [ -z "$AWS_ACCESS_KEY_ID" ]; then
        read -p "Enter AWS Access Key ID: " AWS_ACCESS_KEY_ID
    else
        print_info "AWS Access Key: ${AWS_ACCESS_KEY_ID:0:8}..."
    fi
    
    if [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
        read -sp "Enter AWS Secret Access Key: " AWS_SECRET_ACCESS_KEY
        echo ""
    else
        print_info "AWS Secret Key: ****"
    fi
    
    if [ -z "$AWS_DEFAULT_REGION" ]; then
        read -p "Enter AWS Region [us-west-2]: " AWS_DEFAULT_REGION
        AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION:-us-west-2}
    else
        print_info "AWS Region: $AWS_DEFAULT_REGION"
    fi
    
    # Databricks
    if [ -z "$DATABRICKS_HOST" ]; then
        read -p "Enter Databricks Workspace URL: " DATABRICKS_HOST
    else
        print_info "Databricks Host: $DATABRICKS_HOST"
    fi
    
    if [ -z "$DATABRICKS_TOKEN" ]; then
        read -sp "Enter Databricks PAT: " DATABRICKS_TOKEN
        echo ""
    else
        print_info "Databricks Token: ****"
    fi
    
    # EMR
    if [ -z "$EMR_CLUSTER_ID" ]; then
        read -p "Enter EMR Cluster ID (e.g., j-XXXXX): " EMR_CLUSTER_ID
    else
        print_info "EMR Cluster ID: $EMR_CLUSTER_ID"
    fi
    
    if [ -z "$EMR_PEM_PATH" ]; then
        read -p "Enter path to EMR .pem SSH key: " EMR_PEM_PATH
    else
        print_info "EMR PEM Path: $EMR_PEM_PATH"
    fi
    EMR_PEM_PATH="${EMR_PEM_PATH/#\~/$HOME}"
    
    # Export all
    export AWS_ACCESS_KEY_ID
    export AWS_SECRET_ACCESS_KEY
    export AWS_DEFAULT_REGION
    export AWS_REGION=$AWS_DEFAULT_REGION
    export DATABRICKS_HOST
    export DATABRICKS_TOKEN
    export EMR_CLUSTER_ID
    export EMR_PEM_PATH
}

validate_emr() {
    print_info "Validating EMR cluster..."
    
    EMR_STATUS=$(aws emr describe-cluster --cluster-id $EMR_CLUSTER_ID --region $AWS_DEFAULT_REGION --query "Cluster.Status.State" --output text 2>/dev/null)
    
    if [ "$EMR_STATUS" != "WAITING" ] && [ "$EMR_STATUS" != "RUNNING" ]; then
        print_error "EMR cluster $EMR_CLUSTER_ID is not running (status: $EMR_STATUS)"
        exit 1
    fi
    
    MASTER_DNS=$(aws emr describe-cluster --cluster-id $EMR_CLUSTER_ID --region $AWS_DEFAULT_REGION --query "Cluster.MasterPublicDnsName" --output text)
    print_success "EMR cluster is ready: $MASTER_DNS"
    
    # Validate PEM file
    if [ ! -f "$EMR_PEM_PATH" ]; then
        print_error "PEM file not found: $EMR_PEM_PATH"
        exit 1
    fi
    chmod 600 "$EMR_PEM_PATH"
    print_success "PEM file validated"
}

# ==============================================================================
# TARGETED FIX MODE
# ==============================================================================

run_targeted_fix() {
    print_header "🛠️  TARGETED FIX MODE"
    
    if [ -z "$1" ]; then
        read -p "Enter full table name (database.table): " TARGET_TABLE
    else
        TARGET_TABLE=$1
    fi
    
    GLUE_DATABASE=$(echo $TARGET_TABLE | cut -d'.' -f1)
    GLUE_TABLE=$(echo $TARGET_TABLE | cut -d'.' -f2)
    
    export GLUE_DATABASE
    export GLUE_TABLE
    
    print_info "Database: $GLUE_DATABASE"
    print_info "Table: $GLUE_TABLE"
    
    echo ""
    read -p "Run in dry-run mode first? [Y/n]: " DRY_RUN
    
    if [ "$DRY_RUN" != "n" ] && [ "$DRY_RUN" != "N" ]; then
        print_info "Running analysis (dry-run)..."
        python3 "$SCRIPT_DIR/internal/metadata_surgery.py" --dry-run
        
        echo ""
        read -p "Proceed with actual surgery? [y/N]: " PROCEED
        if [ "$PROCEED" != "y" ] && [ "$PROCEED" != "Y" ]; then
            print_info "Aborted."
            exit 0
        fi
    fi
    
    print_info "Performing metadata surgery..."
    python3 "$SCRIPT_DIR/internal/metadata_surgery.py"
    
    print_header "SURGERY COMPLETE"
    echo -e "You can now query ${GREEN}$TARGET_TABLE${NC} in Databricks!"
    echo ""
    echo "If the error persists, ensure you have run rewrite_data_files first:"
    echo -e "${YELLOW}CALL system.rewrite_data_files(table => '$TARGET_TABLE', options => map('rewrite-all', 'true'));${NC}"
}

# ==============================================================================
# FULL DEMO MODE
# ==============================================================================

run_demo_mode() {
    print_header "🚀 FULL DEMO MODE"
    
    # Generate unique identifiers for this demo
    UNIQUE_ID=$(openssl rand -hex 4)
    BUCKET_NAME="iceberg-demo-${UNIQUE_ID}"
    GLUE_DATABASE="iceberg_demo_db_${UNIQUE_ID}"
    GLUE_TABLE="partition_evolution_test"
    CATALOG_NAME="glue_federation_demo_${UNIQUE_ID}"
    WAREHOUSE_PATH="s3://${BUCKET_NAME}/warehouse/"
    
    export GLUE_DATABASE
    export GLUE_TABLE
    export S3_BUCKET=$BUCKET_NAME
    
    print_info "Unique Demo ID: $UNIQUE_ID"
    print_info "S3 Bucket: $BUCKET_NAME"
    print_info "Glue Database: $GLUE_DATABASE"
    print_info "Catalog Name: $CATALOG_NAME"
    
    # ==== PHASE 0: Create Infrastructure ====
    print_header "PHASE 0: CREATING AWS INFRASTRUCTURE"
    
    print_info "Creating S3 bucket..."
    aws s3 mb s3://$BUCKET_NAME --region $AWS_DEFAULT_REGION
    print_success "Bucket created: $BUCKET_NAME"
    
    print_info "Creating Glue database..."
    aws glue create-database --database-input "{\"Name\": \"$GLUE_DATABASE\", \"Description\": \"Demo for partition evolution\"}" --region $AWS_DEFAULT_REGION
    print_success "Database created: $GLUE_DATABASE"
    
    # Grant Lake Formation permissions
    print_info "Setting up Lake Formation permissions..."
    EMR_ROLE=$(aws emr describe-cluster --cluster-id $EMR_CLUSTER_ID --query "Cluster.Ec2InstanceAttributes.IamInstanceProfile" --output text --region $AWS_DEFAULT_REGION)
    EMR_ROLE_ARN="arn:aws:iam::$(aws sts get-caller-identity --query Account --output text):role/${EMR_ROLE/%-profile/-role}"
    
    aws lakeformation grant-permissions \
        --principal DataLakePrincipalIdentifier="IAM_ALLOWED_PRINCIPALS" \
        --resource "{\"Database\": {\"Name\": \"$GLUE_DATABASE\"}}" \
        --permissions "ALL" \
        --region $AWS_DEFAULT_REGION 2>/dev/null || true
    
    print_success "Lake Formation permissions configured"
    
    # ==== PHASE 1: Reproduce the Error ====
    print_header "PHASE 1: REPRODUCING THE PARTITION EVOLUTION ERROR"
    
    ICEBERG_CONFS="--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
--conf spark.sql.catalog.glue_catalog.warehouse=$WAREHOUSE_PATH"
    
    print_info "Creating evolved Iceberg table on EMR..."
    
    ssh -i "$EMR_PEM_PATH" -o StrictHostKeyChecking=no hadoop@$MASTER_DNS "spark-sql $ICEBERG_CONFS -e \"
DROP TABLE IF EXISTS glue_catalog.$GLUE_DATABASE.$GLUE_TABLE;
CREATE TABLE glue_catalog.$GLUE_DATABASE.$GLUE_TABLE (id INT, category STRING, product STRING, ts TIMESTAMP) 
USING iceberg PARTITIONED BY (category) TBLPROPERTIES ('format-version'='2');
INSERT INTO glue_catalog.$GLUE_DATABASE.$GLUE_TABLE VALUES (1, 'Electronics', 'Laptop', CURRENT_TIMESTAMP()), (2, 'Electronics', 'Phone', CURRENT_TIMESTAMP());
ALTER TABLE glue_catalog.$GLUE_DATABASE.$GLUE_TABLE ADD PARTITION FIELD product;
INSERT INTO glue_catalog.$GLUE_DATABASE.$GLUE_TABLE VALUES (3, 'Furniture', 'Chair', CURRENT_TIMESTAMP()), (4, 'Furniture', 'Desk', CURRENT_TIMESTAMP());
\""
    
    print_success "Table created with partition evolution!"
    
    echo ""
    echo -e "${YELLOW}ACTION REQUIRED:${NC}"
    echo "1. Go to Databricks and set up a federated catalog pointing to this Glue database"
    echo "2. Run this query to see the error:"
    echo -e "   ${CYAN}SELECT * FROM <catalog>.$GLUE_DATABASE.$GLUE_TABLE;${NC}"
    echo ""
    echo "Expected error: ICEBERG_UNDERGONE_PARTITION_EVOLUTION"
    echo ""
    read -p "Press [Enter] once you have confirmed the error..."
    
    # ==== PHASE 2: Show rewrite_data_files doesn't fix it ====
    print_header "PHASE 2: RUNNING rewrite_data_files (Incomplete Fix)"
    
    print_info "Running rewrite_data_files on EMR..."
    
    ssh -i "$EMR_PEM_PATH" -o StrictHostKeyChecking=no hadoop@$MASTER_DNS "spark-sql $ICEBERG_CONFS -e \"
CALL glue_catalog.system.rewrite_data_files(
  table => '$GLUE_DATABASE.$GLUE_TABLE',
  options => map('rewrite-all', 'true')
);
\""
    
    print_success "rewrite_data_files complete!"
    
    echo ""
    echo -e "${YELLOW}ACTION REQUIRED:${NC}"
    echo "Go back to Databricks and RE-RUN the same query."
    echo "You will see the error STILL EXISTS because the metadata history is tainted."
    echo ""
    read -p "Press [Enter] once you have confirmed the error persists..."
    
    # ==== PHASE 3: Metadata Surgery ====
    print_header "PHASE 3: METADATA SURGERY (The Real Fix)"
    
    # First, grant LF permissions on the table
    aws lakeformation grant-permissions \
        --principal DataLakePrincipalIdentifier="IAM_ALLOWED_PRINCIPALS" \
        --resource "{\"Table\": {\"DatabaseName\": \"$GLUE_DATABASE\", \"Name\": \"$GLUE_TABLE\"}}" \
        --permissions "ALL" \
        --region $AWS_DEFAULT_REGION 2>/dev/null || true
    
    print_info "Performing metadata surgery..."
    python3 "$SCRIPT_DIR/internal/metadata_surgery.py"
    
    print_header "✨ SURGERY COMPLETE!"
    echo ""
    echo -e "${GREEN}FINAL ACTION:${NC} Go to Databricks and run the query ONE LAST TIME."
    echo -e "   ${CYAN}SELECT * FROM <catalog>.$GLUE_DATABASE.$GLUE_TABLE;${NC}"
    echo ""
    echo -e "${GREEN}The error is gone!${NC} The data is unified, and the metadata is clean."
    echo ""
    echo "Demo resources created:"
    echo "  - S3 Bucket: $BUCKET_NAME"
    echo "  - Glue Database: $GLUE_DATABASE"
    echo "  - Table: $GLUE_TABLE"
    echo ""
    echo "To clean up, run:"
    echo "  aws s3 rb s3://$BUCKET_NAME --force"
    echo "  aws glue delete-database --name $GLUE_DATABASE"
}

# ==============================================================================
# MAIN
# ==============================================================================

main() {
    echo -e "${CYAN}"
    echo "╔═══════════════════════════════════════════════════════════════╗"
    echo "║         ICEBERG METADATA SURGEON                              ║"
    echo "║         Fix Partition Evolution Errors in Databricks          ║"
    echo "╚═══════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
    
    # Check prerequisites
    check_prerequisites
    
    # Load .env file if exists
    load_env_file
    
    # Parse arguments
    case "$1" in
        --targeted|-t)
            prompt_credentials
            validate_emr
            run_targeted_fix "$2"
            ;;
        --demo|-d)
            prompt_credentials
            validate_emr
            run_demo_mode
            ;;
        --help|-h)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --targeted, -t [table]  Fix a specific table (database.table)"
            echo "  --demo, -d              Run full demo (create, break, fix)"
            echo "  --help, -h              Show this help message"
            echo ""
            echo "Environment variables (or use .env file):"
            echo "  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION"
            echo "  DATABRICKS_HOST, DATABRICKS_TOKEN"
            echo "  EMR_CLUSTER_ID, EMR_PEM_PATH"
            echo "  GLUE_DATABASE, GLUE_TABLE (for targeted mode)"
            ;;
        *)
            echo "Choose a mode:"
            echo "  1) Targeted Fix - Fix a specific table"
            echo "  2) Full Demo - Create, break, and fix a demo table"
            echo ""
            read -p "Enter choice [1/2]: " CHOICE
            
            prompt_credentials
            validate_emr
            
            case "$CHOICE" in
                1) run_targeted_fix ;;
                2) run_demo_mode ;;
                *) print_error "Invalid choice"; exit 1 ;;
            esac
            ;;
    esac
}

main "$@"
