"""
Partition Evolution SQL Reference.

This file contains the SQL commands to reproduce partition evolution on EMR.
Use these as a reference or copy/paste into spark-sql.

For automated execution, use reproduce_evolution_on_emr.py instead.
"""

# ==============================================================================
# SQL COMMANDS TO RUN ON EMR (SPARK-SQL)
# ==============================================================================
#
# Replace <DATABASE> and <TABLE> with your actual values.
#
# Prerequisites:
#   1. EMR cluster with Iceberg configured
#   2. Glue Catalog integration enabled
#   3. S3 bucket for warehouse storage
#
# ==============================================================================

SQL_DROP_TABLE = """
DROP TABLE IF EXISTS glue_catalog.<DATABASE>.<TABLE>;
"""

SQL_CREATE_TABLE = """
CREATE TABLE glue_catalog.<DATABASE>.<TABLE> (
    id INT,
    category STRING,
    product STRING,
    ts TIMESTAMP
)
USING iceberg
PARTITIONED BY (category)
TBLPROPERTIES ('format-version'='2');
"""

SQL_INSERT_INITIAL = """
-- Insert data using initial partition spec (category only)
INSERT INTO glue_catalog.<DATABASE>.<TABLE> 
VALUES 
    (1, 'Electronics', 'Laptop', CURRENT_TIMESTAMP()),
    (2, 'Electronics', 'Phone', CURRENT_TIMESTAMP());
"""

SQL_EVOLVE_SPEC = """
-- EVOLVE THE PARTITION SPEC
-- This adds 'product' as a new partition field
ALTER TABLE glue_catalog.<DATABASE>.<TABLE> 
ADD PARTITION FIELD product;
"""

SQL_INSERT_EVOLVED = """
-- Insert data using evolved partition spec (category + product)
INSERT INTO glue_catalog.<DATABASE>.<TABLE> 
VALUES 
    (3, 'Furniture', 'Chair', CURRENT_TIMESTAMP()),
    (4, 'Furniture', 'Desk', CURRENT_TIMESTAMP());
"""

SQL_REWRITE_DATA_FILES = """
-- Step 1 of the fix: Unify physical data files
-- This rewrites all files using the latest partition spec
CALL glue_catalog.system.rewrite_data_files(
    table => '<DATABASE>.<TABLE>',
    options => map('rewrite-all', 'true')
);
"""

SQL_REWRITE_MANIFESTS = """
-- Optional: Also rewrite manifests for cleaner metadata
CALL glue_catalog.system.rewrite_manifests(
    table => '<DATABASE>.<TABLE>'
);
"""

# ==============================================================================
# FULL REPRODUCTION SCRIPT
# ==============================================================================

FULL_REPRODUCTION_SCRIPT = """
-- ============================================================================
-- FULL PARTITION EVOLUTION REPRODUCTION SCRIPT
-- ============================================================================
-- Run this on EMR to create a table that will fail in Databricks UC Federation
-- ============================================================================

-- 1. Create table with initial partition spec (category only)
DROP TABLE IF EXISTS glue_catalog.<DATABASE>.<TABLE>;

CREATE TABLE glue_catalog.<DATABASE>.<TABLE> (
    id INT,
    category STRING,
    product STRING,
    ts TIMESTAMP
)
USING iceberg
PARTITIONED BY (category)
TBLPROPERTIES ('format-version'='2');

-- 2. Insert initial data (uses Spec ID 0)
INSERT INTO glue_catalog.<DATABASE>.<TABLE> 
VALUES 
    (1, 'Electronics', 'Laptop', CURRENT_TIMESTAMP()),
    (2, 'Electronics', 'Phone', CURRENT_TIMESTAMP());

-- 3. EVOLVE THE PARTITION SPEC (creates Spec ID 1)
ALTER TABLE glue_catalog.<DATABASE>.<TABLE> 
ADD PARTITION FIELD product;

-- 4. Insert data using new spec (uses Spec ID 1)
INSERT INTO glue_catalog.<DATABASE>.<TABLE> 
VALUES 
    (3, 'Furniture', 'Chair', CURRENT_TIMESTAMP()),
    (4, 'Furniture', 'Desk', CURRENT_TIMESTAMP());

-- ============================================================================
-- At this point, the table will fail in Databricks UC Federation with:
-- ICEBERG_UNDERGONE_PARTITION_EVOLUTION
-- ============================================================================
"""


def print_sql(database: str = "<DATABASE>", table: str = "<TABLE>"):
    """Print the full reproduction script with actual values."""
    script = FULL_REPRODUCTION_SCRIPT.replace("<DATABASE>", database).replace("<TABLE>", table)
    print(script)


if __name__ == "__main__":
    import os
    from config import get_glue_config, load_env_file
    
    load_env_file()
    glue_config = get_glue_config()
    
    database = glue_config.database or os.getenv("GLUE_DATABASE", "<DATABASE>")
    table = glue_config.table or os.getenv("GLUE_TABLE", "<TABLE>")
    
    print_sql(database, table)
