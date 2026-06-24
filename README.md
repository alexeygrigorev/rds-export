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

For unattended runs, the `.env` file must include AWS credentials with permission
to create RDS snapshots, start RDS export tasks, use the configured export role,
read/write the backup bucket, and use the export KMS key.

## Scripts

### Full Pipeline

Runs snapshot creation, export, SQLite conversion, and SQLite upload to S3.

```bash
# One database
uv run run_pipeline.py --db cmp --schema prod
uv run run_pipeline.py --db aisl --schema aisl_prod

# Both databases
uv run run_pipeline.py --db all
```

By default, the pipeline keeps the Parquet export files in S3. Use `--cleanup-export-s3` to delete them after the local zip is created.
If `--schema` is omitted, the pipeline uses the default schema for each database:
`prod` for CMP and `aisl_prod` for AI Shipping Labs.

### Hetzner Cron

The Hetzner host uses the SSH alias `hetzner`. The deployed checkout lives at:

```bash
~/rds-export
```

Initial setup:

```bash
ssh hetzner 'git clone https://github.com/alexeygrigorev/rds-export.git ~/rds-export'
scp .env hetzner:~/rds-export/.env
ssh hetzner 'cd ~/rds-export && uv sync'
```

Crontab entries:

```cron
0 1 * * * cd /home/alexey/rds-export && /home/alexey/.local/bin/uv run run_pipeline.py --db cmp --schema prod >> /home/alexey/rds-export/logs/cmp.log 2>&1
0 2 * * * cd /home/alexey/rds-export && /home/alexey/.local/bin/uv run run_pipeline.py --db aisl --schema aisl_prod >> /home/alexey/rds-export/logs/aisl.log 2>&1
```

Create `~/rds-export/logs` before enabling cron. The server timezone is CEST, so these run at 01:00 and 02:00 server time.

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

# Upload generated SQLite database to S3
uv run parquet_to_sqlite.py --schema prod --upload-s3
```

Use `--list` to see available schemas/databases in the selected zip.

**Output:** `rds-<schema>-YYYYMMDD-HHMMSS.db` in `/tmp/rds-export/`

**Upload output:** `s3://<S3_BUCKET>/sqlite/rds-<schema>-YYYYMMDD-HHMMSS.db`

### 4. Clean Up Old Manual Snapshots

Lists manual RDS snapshots older than a retention window and optionally deletes
them. The command is a dry run unless `--delete` is passed.

```bash
# Dry run for AI Shipping Labs instance snapshots older than 7 days
uv run cleanup_snapshots.py \
  --db-instance-id ai-shipping-labs \
  --snapshot-prefix aisl- \
  --retention-days 7

# Delete the snapshots shown by the same filter
uv run cleanup_snapshots.py \
  --db-instance-id ai-shipping-labs \
  --snapshot-prefix aisl- \
  --retention-days 7 \
  --delete

# Dry run for CMP cluster snapshots
uv run cleanup_snapshots.py \
  --db-cluster-id course-management-manual \
  --snapshot-prefix cmp- \
  --retention-days 30
```

The cleanup command only queries snapshots with `SnapshotType=manual`; automated
RDS backups are not selected.

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
