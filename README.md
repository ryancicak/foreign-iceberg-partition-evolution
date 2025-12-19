# Iceberg Metadata Surgeon

Bypassing the `ICEBERG_UNDERGONE_PARTITION_EVOLUTION` error in Databricks UC Federation without moving a single byte of data.

## The Problem
When using Databricks Unity Catalog (UC) Federation with an AWS Glue Iceberg catalog, any table that has undergone Partition Evolution (e.g., adding a new partition field) will fail to query or clone with the following error:

`[DELTA_CLONE_INCOMPATIBLE_SOURCE.ICEBERG_UNDERGONE_PARTITION_EVOLUTION] The clone source has valid format, but has unsupported feature with Delta. Source iceberg table has undergone partition evolution.`

### Why Engineering's Advice Fails
The standard advice is to run `CALL system.rewrite_data_files(...)`. While this physically unifies the data, it does not clean the metadata history. Databricks performs a fail-fast check on the Iceberg `metadata.json` file. If the `partition-specs` array contains more than one entry (even if unused), Databricks blocks the query. For customers with 100+ trillion rows, a full CTAS/migration is not an option.

### Physical vs Logical Unification

```text
INITIAL STATE (EVOLVED TABLE)
--------------------------------------------------------------------------------
METADATA LAYER (.metadata.json)             PHYSICAL LAYER (S3 Parquet Files)
-------------------------------             ---------------------------------
[partition-specs]                           [data/ folder]
|                                           |
|-- Spec ID 0: (category)                   |-- file_A.parquet (Linked to ID 0)
|-- Spec ID 1: (category, product)          |-- file_B.parquet (Linked to ID 1)
                                            
Result: Databricks sees 2 specs -> Error.


AFTER RUNNING: CALL system.rewrite_data_files(...)
--------------------------------------------------------------------------------
METADATA LAYER (.metadata.json)             PHYSICAL LAYER (S3 Parquet Files)
-------------------------------             ---------------------------------
[partition-specs]                           [data/ folder]
|                                           |
|-- Spec ID 0: (category) <--- GHOST        |-- file_A_NEW.parquet (Linked to ID 1)
|-- Spec ID 1: (category, product)          |-- file_B.parquet (Linked to ID 1)

Result: 
1. The physical files are now unified (All use Spec ID 1).
2. The Metadata JSON still contains the "Ghost" of Spec ID 0 in its history.
3. Databricks still sees 2 specs in the JSON -> Error.


THE FINAL FIX: METADATA SURGERY
--------------------------------------------------------------------------------
METADATA LAYER (.metadata.json)             PHYSICAL LAYER (S3 Parquet Files)
-------------------------------             ---------------------------------
[partition-specs]                           [data/ folder]
|                                           |
|-- Spec ID 1: (category, product)          |-- file_A_NEW.parquet (Linked to ID 1)
                                            |-- file_B.parquet (Linked to ID 1)

Result: 
1. The surgery physically deletes Spec ID 0 from the JSON array.
2. Databricks reads the JSON, sees exactly 1 spec -> SUCCESS.
```

---

## The Solution: A Two-Step Workaround

This project provides a Metadata Surgery technique that unifies the Databricks reader by sanitizing the Iceberg metadata history.

### Step 1: Physical Data Unification
Run on EMR/Spark. This ensures every data file in the current snapshot adheres to the latest partition spec.
```sql
CALL system.rewrite_data_files(table => 'db.table', options => map('rewrite-all', 'true'));
CALL system.rewrite_manifests(table => 'db.table');
```

### Step 2: Metadata Surgery
The `internal/metadata_surgery.py` script included in this repo:
1. Downloads the latest Iceberg `.metadata.json` from S3.
2. Edits the JSON to delete the history of old partition specs.
3. Wipes Spec 0 (and any others) so the `partition-specs` array has exactly one entry.
4. Uploads the sanitized JSON and redirects the Glue Catalog to point to it.

---

## How to use this Repo

### 1. Requirements
* An active EMR Cluster with Iceberg/Glue configured.
* AWS CLI credentials with Glue/S3 permissions.
* Databricks PAT and Workspace URL.

### 2. Run the Automation Tool
The included bash script provides two modes:
1. **Targeted Fix**: Provide a specific table name (e.g., `database.table`) to surgically fix an existing table.
2. **Demo Mode**: Press [Enter] to run the full end-to-end lifecycle (Repro -> Step 1 Failure -> Step 2 Success).

```bash
chmod +x full_fix_automation.sh
./full_fix_automation.sh
```

---

## Risks & Safety
- Time Travel: This surgery removes references to old partition layouts, effectively breaking time-travel to snapshots created before the evolution.
- Rollback: The script never deletes old metadata. To rollback, simply update the Glue table's `metadata_location` parameter to point back to the original JSON file.

## Contribution & Feedback
This is an unofficial workaround developed for large-scale customers where data movement is physically or financially impossible.

Author: Ryan Cicak (ryan.cicak@databricks.com)
