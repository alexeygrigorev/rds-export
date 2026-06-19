#!/usr/bin/env -S uv run --python 3.13 python
"""
Run the full RDS backup pipeline.

Steps:
1. Create a manual RDS snapshot.
2. Export the snapshot to S3 and download a zip archive.
3. Convert the selected schema to SQLite.
4. Upload the SQLite database back to S3.
"""

import argparse
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Database:
    key: str
    name: str
    snapshot_type: str
    source_id: str


DATABASES = {
    "aisl": Database(
        key="aisl",
        name="AI Shipping Labs",
        snapshot_type="instance",
        source_id="ai-shipping-labs",
    ),
    "cmp": Database(
        key="cmp",
        name="Course Management",
        snapshot_type="cluster",
        source_id="course-management-manual",
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the full RDS backup pipeline.")
    parser.add_argument("--db", choices=["aisl", "cmp", "all"], required=True)
    parser.add_argument("--schema", default="prod", help="Schema/database to convert to SQLite.")
    parser.add_argument("--region", default="eu-west-1", help="AWS region.")
    parser.add_argument("--poll-interval", type=int, default=30, help="Seconds between snapshot status checks.")
    parser.add_argument(
        "--cleanup-export-s3",
        action="store_true",
        help="Delete the Parquet export files from S3 after creating the local zip.",
    )
    return parser.parse_args()


def run_step(command: list[str]) -> None:
    subprocess.run(command, check=True)


def snapshot_exists(rds_client, db: Database, snapshot_id: str) -> bool:
    try:
        if db.snapshot_type == "cluster":
            rds_client.describe_db_cluster_snapshots(
                DBClusterSnapshotIdentifier=snapshot_id
            )
        else:
            rds_client.describe_db_snapshots(
                DBSnapshotIdentifier=snapshot_id
            )
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in {"DBSnapshotNotFound", "DBClusterSnapshotNotFoundFault"}:
            return False
        raise

    return True


def snapshot_status(rds_client, db: Database, snapshot_id: str) -> str:
    if db.snapshot_type == "cluster":
        response = rds_client.describe_db_cluster_snapshots(
            DBClusterSnapshotIdentifier=snapshot_id
        )
        return response["DBClusterSnapshots"][0]["Status"].lower()

    response = rds_client.describe_db_snapshots(
        DBSnapshotIdentifier=snapshot_id
    )
    return response["DBSnapshots"][0]["Status"].lower()


def create_snapshot(rds_client, db: Database, poll_interval: int) -> str:
    snapshot_id = f"{db.key}-{datetime.now().strftime('%Y-%m-%d')}"
    if snapshot_exists(rds_client, db, snapshot_id):
        snapshot_id = f"{snapshot_id}-{datetime.now().strftime('%H%M%S')}"

    print()
    print(f"=== {db.name}: create snapshot ===")
    print(f"Snapshot: {snapshot_id}")

    if db.snapshot_type == "cluster":
        rds_client.create_db_cluster_snapshot(
            DBClusterIdentifier=db.source_id,
            DBClusterSnapshotIdentifier=snapshot_id,
        )
    else:
        rds_client.create_db_snapshot(
            DBInstanceIdentifier=db.source_id,
            DBSnapshotIdentifier=snapshot_id,
        )

    while True:
        status = snapshot_status(rds_client, db, snapshot_id)
        print(f"{datetime.now().strftime('%H:%M:%S')} status: {status}")
        if status == "available":
            return snapshot_id
        if status in {"deleted", "deleting", "failed"}:
            raise RuntimeError(f"Snapshot ended with unexpected status: {status}")
        time.sleep(poll_interval)


def export_snapshot(db: Database, snapshot_id: str, cleanup_export_s3: bool) -> None:
    print()
    print(f"=== {db.name}: export snapshot ===")

    command = [
        sys.executable,
        "rds_export.py",
        "--snapshot-id",
        snapshot_id,
        "--snapshot-type",
        db.snapshot_type,
        "--source-id",
        db.source_id,
    ]
    if cleanup_export_s3:
        command.append("--cleanup-s3")
    else:
        command.append("--keep-s3")

    run_step(command)


def convert_and_upload_sqlite(db: Database, schema: str) -> None:
    print()
    print(f"=== {db.name}: convert {schema} to SQLite and upload ===")
    run_step([
        sys.executable,
        "parquet_to_sqlite.py",
        "--schema",
        schema,
        "--upload-s3",
    ])


def run_pipeline(db: Database, args: argparse.Namespace) -> None:
    rds_client = boto3.client("rds", region_name=args.region)
    snapshot_id = create_snapshot(rds_client, db, args.poll_interval)
    export_snapshot(db, snapshot_id, args.cleanup_export_s3)
    convert_and_upload_sqlite(db, args.schema)


def main() -> int:
    args = parse_args()
    if args.poll_interval < 1:
        print("--poll-interval must be at least 1 second.")
        return 1

    selected_dbs = DATABASES.values() if args.db == "all" else [DATABASES[args.db]]

    try:
        for db in selected_dbs:
            run_pipeline(db, args)
    except subprocess.CalledProcessError as exc:
        return exc.returncode
    except KeyboardInterrupt:
        print("\nCancelled.")
        return 130

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
