# Foreign Iceberg Table Fixes for Databricks

Workarounds for common issues when reading foreign Iceberg tables in Databricks Unity Catalog Federation.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## Issue 1: Partition Evolution

**Error:** `ICEBERG_UNDERGONE_PARTITION_EVOLUTION`

### Fix: Compaction + Spark Flag

```sql
-- On EMR/Spark
CALL system.rewrite_data_files(
  table => 'your_db.your_table',
  options => map('rewrite-all', 'true')
);
```

Then add to Databricks Classic Cluster config:
```
spark.databricks.delta.convert.iceberg.partitionEvolution.enabled true
```

Query from that cluster first, then Serverless works too.

---

## Issue 2: V2 Merge-on-Read Delete Files

**Error:** `requirement failed` at `DeleteFileWrapper`

Databricks can't read V2 Iceberg tables with Merge-on-Read delete files.

### Fix: Upgrade to V3 + Compact

**CRITICAL: Must use EMR 7.12+ (Iceberg 1.10+)**

| EMR Version | Iceberg | V3 Support |
|-------------|---------|------------|
| < 7.12 | < 1.10 | [BROKEN] Missing `next-row-id` |
| **7.12+** | **1.10+** | [OK] Full V3 support |

```sql
-- Step 1: Upgrade to V3 (EMR 7.12+ ONLY!)
ALTER TABLE glue_catalog.your_db.your_table 
SET TBLPROPERTIES ('format-version' = '3');

-- Step 2: Compact with delete file removal
CALL glue_catalog.system.rewrite_data_files(
  table => 'your_db.your_table',
  options => map('rewrite-all', 'true', 'delete-file-threshold', '1')
);

-- Step 3: Expire old snapshots
CALL glue_catalog.system.expire_snapshots(
  table => 'your_db.your_table',
  older_than => TIMESTAMP '2030-01-01 00:00:00',
  retain_last => 1
);
```

### Warning: V3 Upgrade on Old EMR Breaks Tables!

If you upgrade to V3 on EMR < 7.12:
- Table becomes **unreadable** by both Databricks AND EMR 7.12+
- Missing `next-row-id` field in metadata
- **Fix requires metadata surgery** (see below)

---

## Issue 3: V3 Table Missing `next-row-id`

**Error:** `Cannot parse missing long: next-row-id`

This happens when V3 upgrade was done on EMR < 7.12.

### Fix: Metadata Surgery

```bash
# Download metadata
aws s3 cp s3://bucket/warehouse/db.db/table/metadata/LATEST.metadata.json /tmp/meta.json

# Add next-row-id (Python)
python3 << 'EOF'
import json
with open('/tmp/meta.json', 'r') as f:
    data = json.load(f)
data['next-row-id'] = 0
with open('/tmp/fixed.json', 'w') as f:
    json.dump(data, f)
EOF

# Upload fixed metadata
NEW_FILE="00999-$(uuidgen).metadata.json"
aws s3 cp /tmp/fixed.json "s3://bucket/warehouse/db.db/table/metadata/$NEW_FILE"

# Update Glue catalog
aws glue update-table --database-name your_db --table-input '{
  "Name": "your_table",
  "Parameters": {
    "table_type": "ICEBERG",
    "metadata_location": "s3://bucket/warehouse/db.db/table/metadata/'"$NEW_FILE"'"
  }
}'
```

Then run compaction on EMR 7.12+ (see Issue 2).

---

## Quick Reference

| Problem | Solution |
|---------|----------|
| Partition evolution error | Compact + Spark flag |
| V2 MoR delete files | Upgrade to V3 on EMR 7.12+ |
| V3 missing `next-row-id` | Metadata surgery + compact |

---

## Environment Variables

```bash
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_DEFAULT_REGION="us-west-2"
export DATABRICKS_HOST="https://xxx.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
export EMR_CLUSTER_ID="j-..."
export EMR_PEM_PATH="/path/to/key.pem"
```

---

## Scripts

| Script | Purpose |
|--------|---------|
| `full_fix_automation.sh` | Interactive demo of partition evolution fix |
| `internal/metadata_surgery.py` | Remove old partition specs from metadata |
| `internal/lake_formation_setup.py` | Grant Lake Formation permissions |
| `internal/databricks_uc_setup.py` | Setup Databricks Glue connection |

---

## Author

**Ryan Cicak** - ryan.cicak@databricks.com

## License

MIT License - see [LICENSE](LICENSE)
