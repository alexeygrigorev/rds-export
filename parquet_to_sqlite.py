#!/usr/bin/env -S uv run --python 3.11 python
"""
Convert Parquet RDS export to SQLite database.

Reads Parquet files from an RDS snapshot export and creates a SQLite database.
Supports multiple PostgreSQL schemas (dev, prod, test_prod).
"""

import argparse
import os
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

# Configuration
load_dotenv()
LOCAL_TMP = "/tmp/rds-export"
S3_BUCKET = os.getenv("S3_BUCKET")


class Colors:
    """ANSI color codes for terminal output."""
    GREEN = "\033[0;32m"
    YELLOW = "\033[1;33m"
    RED = "\033[0;31m"
    BOLD = "\033[1m"
    NC = "\033[0m"  # No Color


def log_info(msg: str) -> None:
    """Print an info message."""
    print(f"{Colors.GREEN}[INFO]{Colors.NC} {msg}")


def log_warn(msg: str) -> None:
    """Print a warning message."""
    print(f"{Colors.YELLOW}[WARN]{Colors.NC} {msg}")


def log_error(msg: str) -> None:
    """Print an error message."""
    print(f"{Colors.RED}[ERROR]{Colors.NC} {msg}")


def upload_sqlite_to_s3(db_path: Path, bucket: str, prefix: str) -> str:
    """Upload a SQLite database file to S3.

    Returns:
        The S3 URI of the uploaded file.
    """
    normalized_prefix = prefix.strip("/")
    s3_key = f"{normalized_prefix}/{db_path.name}" if normalized_prefix else db_path.name

    log_info(f"Uploading SQLite database to s3://{bucket}/{s3_key}")
    boto3.client("s3").upload_file(str(db_path), bucket, s3_key)

    return f"s3://{bucket}/{s3_key}"


def find_zip_files(base_dir: str) -> list[Path]:
    """Find all zip files in the specified directory."""
    base_path = Path(base_dir)
    if not base_path.exists():
        log_error(f"Directory not found: {base_dir}")
        return []

    zip_files = list(base_path.glob("rds-backup-*.zip"))
    return sorted(zip_files, key=lambda p: p.stat().st_mtime, reverse=True)


def list_schemas(zip_path: Path) -> list[str]:
    """List available schemas in the zip file."""
    import zipfile

    schemas = set()

    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            if not name.endswith(".parquet"):
                continue

            parts = Path(name).parts
            for index, part in enumerate(parts):
                if "." in part:
                    if index > 0:
                        schemas.add(parts[index - 1])
                    else:
                        schemas.add(part.split(".", 1)[0])
                    break

    return sorted(schemas)


def extract_schema(zip_path: Path, schema: str, extract_dir: Path) -> Path:
    """Extract only the specified schema from the zip file."""
    import zipfile

    schema_dir = extract_dir / schema
    schema_dir.mkdir(parents=True, exist_ok=True)

    log_info(f"Extracting {schema} schema...")

    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            parts = Path(name).parts
            if schema in parts:
                schema_idx = parts.index(schema)
                relative_path = Path(*parts[schema_idx:])
                target_path = extract_dir / relative_path
            else:
                table_idx = next(
                    (
                        index
                        for index, part in enumerate(parts)
                        if part.startswith(f"{schema}.")
                    ),
                    None,
                )
                if table_idx is None:
                    continue

                relative_path = Path(schema, *parts[table_idx:])
                target_path = extract_dir / relative_path

            target_path.parent.mkdir(parents=True, exist_ok=True)

            with open(target_path, "wb") as f:
                f.write(zf.read(name))

    return schema_dir


def find_parquet_files(schema_dir: Path) -> dict[str, Path]:
    """Find all Parquet files and map table names to files.

    Returns:
        Dict mapping table names to their Parquet file paths.
    """
    parquet_files = {}

    for parquet_path in schema_dir.rglob("*.parquet"):
        # Path relative to schema_dir: schema/schema.table_name/N/part-*.parquet
        # or schema.table_name/N/part-*.parquet
        rel_path = parquet_path.relative_to(schema_dir)
        parts = rel_path.parts

        # The first part should be "schema.table_name" or just "table_name"
        if parts:
            full_table_name = parts[0]
            table_name = full_table_name.split(".", 1)[-1] if "." in full_table_name else full_table_name
            parquet_files[table_name] = parquet_path

    return parquet_files


def arrow_type_to_sqlite(arrow_type: pa.DataType) -> str:
    """Convert PyArrow type to SQLite type."""
    if pa.types.is_int8(arrow_type) or pa.types.is_int16(arrow_type) or \
       pa.types.is_int32(arrow_type) or pa.types.is_int64(arrow_type) or \
       pa.types.is_uint8(arrow_type) or pa.types.is_uint16(arrow_type) or \
       pa.types.is_uint32(arrow_type) or pa.types.is_uint64(arrow_type):
        return "INTEGER"
    elif pa.types.is_float32(arrow_type) or pa.types.is_float64(arrow_type):
        return "REAL"
    elif pa.types.is_boolean(arrow_type):
        return "INTEGER"
    elif pa.types.is_temporal(arrow_type):
        return "TEXT"
    elif pa.types.is_binary(arrow_type):
        return "BLOB"
    else:
        return "TEXT"


def create_table_from_arrow(
    conn: sqlite3.Connection,
    table_name: str,
    table: pa.Table,
) -> None:
    """Create a SQLite table based on Arrow schema."""
    columns = []
    for field in table.schema:
        sqlite_type = arrow_type_to_sqlite(field.type)
        columns.append(f'"{field.name}" {sqlite_type}')

    create_sql = f'CREATE TABLE "{table_name}" ({", ".join(columns)});'

    cursor = conn.cursor()
    cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    cursor.execute(create_sql)
    conn.commit()


def convert_arrow_value(value):
    """Convert Arrow Python objects to SQLite-compatible types."""
    if value is None:
        return None
    # Handle Arrow's special types
    if hasattr(value, 'as_py'):
        return value.as_py()
    return value


def import_parquet_to_sqlite(
    parquet_path: Path,
    conn: sqlite3.Connection,
    table_name: str,
) -> int:
    """Import a Parquet file into SQLite.

    Returns:
        Number of rows imported.
    """
    log_info(f"  Importing {table_name}...")

    try:
        # Read Parquet file with PyArrow
        table = pq.read_table(parquet_path)

        if table.num_rows == 0:
            log_warn(f"    No data in {table_name}")
            return 0

        # Create table schema
        create_table_from_arrow(conn, table_name, table)

        # Convert to Python and insert in batches
        cursor = conn.cursor()
        columns = [f'"{field.name}"' for field in table.schema]
        placeholders = ", ".join(["?" for _ in columns])
        insert_sql = f'INSERT INTO "{table_name}" VALUES ({placeholders})'

        # Convert to Python and insert in batches
        batch_size = 10000
        total_rows = 0

        for batch in table.to_batches(max_chunksize=batch_size):
            # Convert batch to Python dict
            batch_dict = batch.to_pydict()

            # Create rows as tuples
            rows = list(zip(*(batch_dict[col] for col in table.schema.names)))

            # Insert batch
            cursor.executemany(insert_sql, rows)
            conn.commit()
            total_rows += len(rows)

        return total_rows

    except Exception as e:
        log_error(f"    Failed to import {table_name}: {e}")
        import traceback
        traceback.print_exc()
        return 0


def select_interactive(options: list[str], prompt: str, default: int = 0) -> str:
    """Interactive selection from a list of options."""
    print(f"\n{Colors.BOLD}{prompt}{Colors.NC}")

    for i, option in enumerate(options):
        marker = "→" if i == default else " "
        print(f"  {marker} [{i}] {option}")

    while True:
        try:
            response = input(f"\nSelect [0-{len(options)-1}] (default: {default}): ").strip()
            if not response:
                return options[default]
            idx = int(response)
            if 0 <= idx < len(options):
                return options[idx]
            log_warn(f"Please enter a number between 0 and {len(options)-1}")
        except ValueError:
            log_warn("Please enter a valid number")
        except KeyboardInterrupt:
            print()
            sys.exit(1)


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Convert Parquet RDS export to SQLite database"
    )
    parser.add_argument(
        "--zip",
        type=str,
        help="Path to zip file (default: most recent in tmp dir)",
    )
    parser.add_argument(
        "--schema",
        type=str,
        help="Schema to import (default: prompt)",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output SQLite database path (default: auto-generated)",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available schemas and exit",
    )
    parser.add_argument(
        "--upload-s3",
        action="store_true",
        help="Upload the generated SQLite database to S3",
    )
    parser.add_argument(
        "--s3-bucket",
        default=S3_BUCKET,
        help="S3 bucket for SQLite upload (default: S3_BUCKET from .env)",
    )
    parser.add_argument(
        "--s3-prefix",
        default="sqlite",
        help="S3 prefix for SQLite upload (default: sqlite)",
    )
    args = parser.parse_args()
    if args.upload_s3 and not args.s3_bucket:
        log_error("--upload-s3 requires --s3-bucket or S3_BUCKET in .env")
        return 1

    # Find zip file
    if args.zip:
        zip_path = Path(args.zip)
        if not zip_path.exists():
            log_error(f"Zip file not found: {args.zip}")
            return 1
    else:
        zip_files = find_zip_files(LOCAL_TMP)
        if not zip_files:
            log_error(f"No zip files found in {LOCAL_TMP}")
            log_info("Run rds_export.py first to create a backup")
            return 1
        zip_path = zip_files[0]
        log_info(f"Using most recent zip: {zip_path.name}")

    # List schemas
    schemas = list_schemas(zip_path)
    if not schemas:
        log_error("No schemas found in zip file")
        return 1

    if args.list:
        print(f"\nAvailable schemas in {zip_path.name}:")
        for schema in schemas:
            print(f"  - {schema}")
        return 0

    print(f"\n{Colors.BOLD}Available schemas:{Colors.NC}")
    for schema in schemas:
        print(f"  • {schema}")

    # Select schema
    if args.schema:
        selected_schema = args.schema
    else:
        selected_schema = select_interactive(
            schemas,
            "Select a schema to import:",
        )

    log_info(f"Selected schema: {selected_schema}")

    # Extract schema
    extract_dir = Path(LOCAL_TMP) / "extracted"
    schema_dir = extract_schema(zip_path, selected_schema, extract_dir)

    # Find Parquet files
    parquet_files = find_parquet_files(schema_dir)

    if not parquet_files:
        log_error(f"No Parquet files found for schema: {selected_schema}")
        return 1

    log_info(f"Found {len(parquet_files)} tables")

    # Create output database
    if args.output:
        db_path = Path(args.output)
    else:
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        db_path = Path(LOCAL_TMP) / f"rds-{selected_schema}-{timestamp}.db"

    # Create SQLite database
    log_info(f"Creating SQLite database: {db_path}")

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = OFF")

    total_rows = 0
    imported_tables = 0

    for table_name, parquet_path in sorted(parquet_files.items()):
        rows = import_parquet_to_sqlite(parquet_path, conn, table_name)
        if rows > 0:
            total_rows += rows
            imported_tables += 1
            log_info(f"    {rows:,} rows")

    conn.close()

    # Cleanup extracted files
    shutil.rmtree(schema_dir)

    s3_uri = None
    if args.upload_s3:
        s3_uri = upload_sqlite_to_s3(db_path, args.s3_bucket, args.s3_prefix)

    # Summary
    print()
    log_info("=" * 40)
    log_info("IMPORT SUMMARY")
    log_info("=" * 40)
    log_info(f"Schema:        {selected_schema}")
    log_info(f"Tables:        {imported_tables}/{len(parquet_files)}")
    log_info(f"Total rows:    {total_rows:,}")
    log_info(f"Database:      {db_path}")
    log_info(f"Database size: {Path(db_path).stat().st_size / 1024 / 1024:.1f} MB")
    if s3_uri:
        log_info(f"S3 upload:     {s3_uri}")
    log_info("=" * 40)

    return 0


if __name__ == "__main__":
    sys.exit(main())
