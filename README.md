# Iceberg Metadata Surgeon 🔪

Bypassing the `ICEBERG_UNDERGONE_PARTITION_EVOLUTION` error in Databricks UC Federation without moving a single byte of data.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## The Problem

When using Databricks Unity Catalog (UC) Federation with an AWS Glue Iceberg catalog, any table that has undergone Partition Evolution (e.g., adding a new partition field) will fail to query with:

```
[DELTA_CLONE_INCOMPATIBLE_SOURCE.ICEBERG_UNDERGONE_PARTITION_EVOLUTION] 
The clone source has valid format, but has unsupported feature with Delta. 
Source iceberg table has undergone partition evolution.
```

### Why This Happens

Databricks performs a fail-fast check on the Iceberg `metadata.json` file. If the `partition-specs` array contains more than one entry, it blocks the query - even if all current data files use the same partition spec.

The standard fix of running `CALL system.rewrite_data_files(...)` **physically unifies the data** but **doesn't clean the metadata history**. The old partition spec remains as a "ghost" in the JSON.

### Physical vs Logical Unification

```
INITIAL STATE (EVOLVED TABLE)
────────────────────────────────────────────────────────────────────────────────
METADATA LAYER (.metadata.json)             PHYSICAL LAYER (S3 Parquet Files)
───────────────────────────────             ─────────────────────────────────
[partition-specs]                           [data/ folder]
│                                           │
├── Spec ID 0: (category)                   ├── file_A.parquet (Linked to ID 0)
└── Spec ID 1: (category, product)          └── file_B.parquet (Linked to ID 1)
                                            
Result: Databricks sees 2 specs → ERROR


AFTER: CALL system.rewrite_data_files(...)
────────────────────────────────────────────────────────────────────────────────
METADATA LAYER (.metadata.json)             PHYSICAL LAYER (S3 Parquet Files)
───────────────────────────────             ─────────────────────────────────
[partition-specs]                           [data/ folder]
│                                           │
├── Spec ID 0: (category) ← GHOST           ├── file_A_NEW.parquet (ID 1)
└── Spec ID 1: (category, product)          └── file_B.parquet (ID 1)

Result: Physical files unified, but metadata still has 2 specs → STILL ERROR


AFTER: METADATA SURGERY
────────────────────────────────────────────────────────────────────────────────
METADATA LAYER (.metadata.json)             PHYSICAL LAYER (S3 Parquet Files)
───────────────────────────────             ─────────────────────────────────
[partition-specs]                           [data/ folder]
│                                           │
└── Spec ID 1: (category, product)          ├── file_A_NEW.parquet (ID 1)
                                            └── file_B.parquet (ID 1)

Result: Only 1 spec in metadata → SUCCESS! ✅
```

---

## Quick Start

### Prerequisites

- Python 3.8+
- AWS CLI configured
- Active EMR Cluster with Iceberg/Glue
- Databricks workspace with Unity Catalog

### Installation

```bash
git clone https://github.com/ryancicak/foreign-iceberg-partition-evolution.git
cd foreign-iceberg-partition-evolution
pip install -r requirements.txt
```

### Configuration

Copy the example environment file and fill in your values:

```bash
cp env.example .env
# Edit .env with your credentials
```

### Usage

#### Option 1: Fix a Specific Table

```bash
# Analyze a table (dry-run)
python internal/metadata_surgery.py --database my_db --table my_table --dry-run

# Perform the surgery
python internal/metadata_surgery.py --database my_db --table my_table
```

#### Option 2: Full Interactive Demo

```bash
chmod +x full_fix_automation.sh
./full_fix_automation.sh
```

This will:
1. Create a new Iceberg table with partition evolution
2. Show the error in Databricks
3. Run `rewrite_data_files` (show it doesn't fix the error)
4. Run metadata surgery (show the fix works!)

---

## The Solution: Two-Step Workaround

### Step 1: Physical Data Unification (on EMR/Spark)

Run this to rewrite all data files using the latest partition spec:

```sql
CALL system.rewrite_data_files(
  table => 'database.table', 
  options => map('rewrite-all', 'true')
);
CALL system.rewrite_manifests(table => 'database.table');
```

### Step 2: Metadata Surgery

The `metadata_surgery.py` script:
1. Downloads the latest Iceberg `metadata.json` from S3
2. Removes all old partition specs from the `partition-specs` array
3. Uploads a new sanitized metadata file
4. Updates the Glue Catalog to point to the new metadata

```bash
python internal/metadata_surgery.py -d my_database -t my_table
```

---

## Project Structure

```
foreign-iceberg-partition-evolution/
├── full_fix_automation.sh      # Main automation script
├── requirements.txt            # Python dependencies
├── env.example                 # Environment template
├── README.md
├── LICENSE
└── internal/
    ├── config.py               # Shared configuration loader
    ├── metadata_surgery.py     # Core surgery script
    ├── databricks_uc_setup.py  # Databricks UC federation setup
    ├── lake_formation_setup.py # AWS Lake Formation permissions
    └── ...
```

---

## Command Reference

### Metadata Surgery

```bash
# Analyze table without making changes
python internal/metadata_surgery.py -d DATABASE -t TABLE --analyze

# Dry run - show what would happen
python internal/metadata_surgery.py -d DATABASE -t TABLE --dry-run

# Perform surgery
python internal/metadata_surgery.py -d DATABASE -t TABLE

# Rollback to previous metadata
python internal/metadata_surgery.py --rollback rollback/DATABASE_TABLE_TIMESTAMP.json
```

### Automation Script

```bash
# Interactive mode
./full_fix_automation.sh

# Targeted fix for specific table
./full_fix_automation.sh --targeted database.table

# Full demo mode
./full_fix_automation.sh --demo

# Show help
./full_fix_automation.sh --help
```

---

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `AWS_ACCESS_KEY_ID` | AWS access key | Yes |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | Yes |
| `AWS_DEFAULT_REGION` | AWS region (e.g., us-west-2) | Yes |
| `DATABRICKS_HOST` | Databricks workspace URL | Yes |
| `DATABRICKS_TOKEN` | Databricks PAT | Yes |
| `EMR_CLUSTER_ID` | EMR cluster ID (e.g., j-XXXXX) | For demo |
| `EMR_PEM_PATH` | Path to EMR SSH key | For demo |
| `GLUE_DATABASE` | Glue database name | For targeted fix |
| `GLUE_TABLE` | Glue table name | For targeted fix |

---

## Risks & Safety

### ⚠️ Time Travel Impact
This surgery removes references to old partition layouts, effectively breaking time-travel to snapshots created before the evolution.

### 🔄 Rollback
The script automatically saves rollback information. To restore:

```bash
python internal/metadata_surgery.py --rollback rollback/DATABASE_TABLE_TIMESTAMP.json
```

Or manually update the Glue table's `metadata_location` parameter to point to the original metadata file.

### 📋 Best Practices
1. **Always run `rewrite_data_files` first** to ensure all data uses the latest spec
2. **Use `--dry-run` first** to see what changes will be made
3. **Test in non-production** before applying to critical tables
4. **Keep rollback files** until you've verified the fix works

---

## Troubleshooting

### Lake Formation Permissions
If you see `AccessDeniedException` errors, you may need to:
1. Add your IAM user/role as a Lake Formation admin
2. Grant permissions on the database and table
3. Use the `lake_formation_setup.py` helper

### Glue Connection Issues
Databricks Glue connections require **SERVICE** credentials, not storage credentials. Use `databricks_uc_setup.py` to create properly configured connections.

### EMR SSH Issues
Ensure:
1. The PEM file has correct permissions (`chmod 600`)
2. The EMR cluster security group allows SSH (port 22)
3. The cluster is in WAITING or RUNNING state

---

## Contributing

This is an unofficial workaround developed for large-scale customers where data movement is physically or financially impossible.

Issues and PRs welcome!

---

## Author

**Ryan Cicak** - ryan.cicak@databricks.com

## License

MIT License - see [LICENSE](LICENSE) for details.
