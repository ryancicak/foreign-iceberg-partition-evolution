# ==============================================================================
# PARTITION EVOLUTION REPRODUCTION SCRIPT
# ------------------------------------------------------------------------------
# Run this on an EMR cluster to create an Iceberg table that will trigger
# the Databricks 'ICEBERG_UNDERGONE_PARTITION_EVOLUTION' error.
# ==============================================================================

"""
-- SQL COMMANDS TO RUN ON EMR (SPARK-SQL):

-- 1. Create table with initial partition spec (category)
DROP TABLE IF EXISTS glue_catalog.glue_uc_demo_db_v2.partition_evolution_test;

CREATE TABLE glue_catalog.glue_uc_demo_db_v2.partition_evolution_test (
    id INT,
    category STRING,
    product STRING,
    ts TIMESTAMP
)
USING iceberg
PARTITIONED BY (category)
TBLPROPERTIES ('format-version'='2');

-- 2. Insert initial data
INSERT INTO glue_catalog.glue_uc_demo_db_v2.partition_evolution_test 
VALUES (1, 'Electronics', 'Laptop', CURRENT_TIMESTAMP());

-- 3. EVOLVE THE PARTITION SPEC
ALTER TABLE glue_catalog.glue_uc_demo_db_v2.partition_evolution_test 
ADD PARTITION FIELD product;

-- 4. Insert data into evolved spec
INSERT INTO glue_catalog.glue_uc_demo_db_v2.partition_evolution_test 
VALUES (2, 'Furniture', 'Chair', CURRENT_TIMESTAMP());

-- At this point, the table will fail in Databricks UC Federation.
"""

