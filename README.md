# RDS Backup Tools

Tools for exporting RDS snapshots to S3 and converting to SQLite databases.

## Setup

```bash
cd ~/tmp/rds-export
uv sync
```

Create a `.env` file with your AWS configuration (see `.env.example`):

```bash
cp .env.example .env
# Edit .env with your values
```

## Scripts

### 1. Create RDS Snapshot

Creates a manual RDS snapshot for one of the configured databases:

- `aisl` - `ai-shipping-labs`, snapshot names like `aisl-YYYY-MM-DD`
- `cmp` - `course-management-manual`, snapshot names like `cmp-YYYY-MM-DD`

```bash
# Interactive mode
uv run create_snapshot.py

# Select a database directly
uv run create_snapshot.py --db aisl
uv run create_snapshot.py --db cmp

# Start the snapshot and exit immediately
uv run create_snapshot.py --db cmp --no-wait
```

By default, the script polls AWS until the snapshot is available.

### 2. Export RDS Snapshot to Zip

Exports a selected RDS snapshot to S3 as Parquet files, then downloads and creates a zip archive.

```bash
# Interactive mode: choose from available manual snapshots created in the last 7 days
uv run rds_export.py

# Export a specific snapshot
uv run rds_export.py --snapshot-id cmp-2026-06-19

# Continue after a failed download/zip step
uv run rds_export.py --resume-export-id rds-export-1781876924
```

**Options:**
- `--snapshot-id <name>` - Export a specific snapshot by name
- `--recent-days <days>` - Number of days to show in the interactive snapshot list
- `--resume-export-id <id>` - Continue from a completed export task
- `--cleanup-s3` - Delete exported files from S3 after creating zip
- `--keep-s3` - Keep exported files in S3 (no prompt)

**Output:** `rds-backup-YYYYMMDD-HHMMSS.zip` in `/tmp/rds-export/`

### 3. Convert Parquet to SQLite

Converts Parquet files from the zip archive to a SQLite database.

```bash
# Interactive mode (select schema)
uv run parquet_to_sqlite.py

# Specify schema directly
uv run parquet_to_sqlite.py --schema prod

# List available schemas
uv run parquet_to_sqlite.py --list

# Custom output path
uv run parquet_to_sqlite.py --schema prod --output ~/my-database.db
```

**Available schemas:** `dev`, `prod`, `test_prod`

**Output:** `rds-<schema>-YYYYMMDD-HHMMSS.db` in `/tmp/rds-export/`

## Viewing Data

### Top 5 Biggest Tables

```python
import sqlite3

conn = sqlite3.connect("/tmp/rds-export/rds-prod.db")
cursor = conn.cursor()

cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [r[0] for r in cursor.fetchall()]

stats = []
for t in tables:
    cursor.execute(f'SELECT COUNT(*) FROM "{t}"')
    stats.append((t, cursor.fetchone()[0]))

for table, count in sorted(stats, key=lambda x: x[1], reverse=True)[:5]:
    print(f"{table:40} {count:>12,} rows")
```

### Query a Table

```python
import sqlite3

conn = sqlite3.connect("/tmp/rds-export/rds-prod.db")

# Sample query
df = pd.read_sql_query("SELECT * FROM courses_answer LIMIT 10", conn)
print(df)
```

## Configuration

Configuration is loaded from a `.env` file:

- `S3_BUCKET` - Target S3 bucket for exports
- `KMS_KEY_ARN` - KMS key for encryption
- `IAM_ROLE_ARN` - IAM role for RDS export
- `CLUSTER_ID` - RDS cluster identifier

## AWS Setup

The Terraform configuration in `~/git/infra-terraform/db_backup.tf` includes:

- S3 bucket for RDS backups
- KMS key for snapshot exports
- IAM role with S3/KMS permissions

To apply:

```bash
cd ~/git/infra-terraform
terraform plan
terraform apply
```
