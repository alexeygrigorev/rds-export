#!/usr/bin/env python3
"""
RDS Snapshot Export to S3

Exports the latest RDS snapshot to S3, downloads, and creates a zip archive.
"""

import argparse
import os
import shutil
import sys
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from dotenv import load_dotenv


class Colors:
    """ANSI color codes for terminal output."""
    GREEN = "\033[0;32m"
    YELLOW = "\033[1;33m"
    RED = "\033[0;31m"
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


# Load environment variables from .env file
load_dotenv()

# Configuration
S3_BUCKET = os.getenv("S3_BUCKET")
KMS_KEY_ARN = os.getenv("KMS_KEY_ARN")
IAM_ROLE_ARN = os.getenv("IAM_ROLE_ARN")
CLUSTER_ID = os.getenv("CLUSTER_ID")
LOCAL_TMP = "/tmp/rds-export"

def validate_config(require_cluster_id: bool = True) -> None:
    """Validate required environment variables."""
    required_vars = {
        "S3_BUCKET": S3_BUCKET,
        "KMS_KEY_ARN": KMS_KEY_ARN,
        "IAM_ROLE_ARN": IAM_ROLE_ARN,
    }
    if require_cluster_id:
        required_vars["CLUSTER_ID"] = CLUSTER_ID

    missing_vars = [name for name, value in required_vars.items() if not value]
    if missing_vars:
        log_error(f"Missing required environment variables: {', '.join(missing_vars)}")
        log_error("Create a .env file with these values or set them in your environment.")
        sys.exit(1)


def format_snapshot_time(snapshot: dict) -> str:
    """Format an RDS snapshot creation time for display."""
    return snapshot["SnapshotCreateTime"].strftime("%Y-%m-%d %H:%M:%S UTC")


def get_snapshots(
    rds_client,
    source_id: str,
    snapshot_type: str,
) -> list[dict]:
    """Get manual RDS snapshots for the configured source."""
    if snapshot_type == "cluster":
        paginator_name = "describe_db_cluster_snapshots"
        paginate_args = {
            "DBClusterIdentifier": source_id,
            "SnapshotType": "manual",
        }
        response_key = "DBClusterSnapshots"
    else:
        paginator_name = "describe_db_snapshots"
        paginate_args = {
            "DBInstanceIdentifier": source_id,
            "SnapshotType": "manual",
        }
        response_key = "DBSnapshots"

    paginator = rds_client.get_paginator(paginator_name)
    snapshots = []

    for page in paginator.paginate(**paginate_args):
        snapshots.extend(page[response_key])

    return sorted(
        snapshots,
        key=lambda snapshot: snapshot["SnapshotCreateTime"],
        reverse=True,
    )


def snapshot_arn_key(snapshot_type: str) -> str:
    return "DBClusterSnapshotArn" if snapshot_type == "cluster" else "DBSnapshotArn"


def snapshot_id_key(snapshot_type: str) -> str:
    return "DBClusterSnapshotIdentifier" if snapshot_type == "cluster" else "DBSnapshotIdentifier"


def get_snapshot_by_id(
    rds_client,
    snapshot_id: str,
    snapshot_type: str,
) -> tuple[str, str, str]:
    """Get a snapshot by ID.

    Returns:
        Tuple of (snapshot_arn, snapshot_id, creation_time)
    """
    log_info(f"Finding snapshot: {snapshot_id}")

    if snapshot_type == "cluster":
        response = rds_client.describe_db_cluster_snapshots(
            DBClusterSnapshotIdentifier=snapshot_id
        )
        response_key = "DBClusterSnapshots"
    else:
        response = rds_client.describe_db_snapshots(
            DBSnapshotIdentifier=snapshot_id
        )
        response_key = "DBSnapshots"

    snapshots = response[response_key]
    if not snapshots:
        log_error(f"Snapshot not found: {snapshot_id}")
        sys.exit(1)

    snapshot = snapshots[0]
    if snapshot["Status"] != "available":
        log_error(f"Snapshot is not available yet: {snapshot_id} ({snapshot['Status']})")
        sys.exit(1)

    creation_time = format_snapshot_time(snapshot)
    log_info(f"Snapshot: {snapshot_id}")
    log_info(f"Created: {creation_time}")

    return snapshot[snapshot_arn_key(snapshot_type)], snapshot_id, creation_time


def select_recent_snapshot(
    rds_client,
    source_id: str,
    snapshot_type: str,
    recent_days: int,
) -> tuple[str, str, str]:
    """Select an available snapshot from recent snapshots or by name."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=recent_days)
    snapshots = [
        snapshot
        for snapshot in get_snapshots(rds_client, source_id, snapshot_type)
        if snapshot["SnapshotCreateTime"] >= cutoff
    ]

    available_snapshots = [
        snapshot for snapshot in snapshots if snapshot["Status"] == "available"
    ]

    print()
    if available_snapshots:
        print(f"Available snapshots from the last {recent_days} days:")
        for index, snapshot in enumerate(available_snapshots, start=1):
            snapshot_id = snapshot[snapshot_id_key(snapshot_type)]
            created = format_snapshot_time(snapshot)
            print(f"  {index}. {snapshot_id} ({created})")
        print("  m. Enter snapshot name manually")

        while True:
            choice = input("Select snapshot: ").strip().lower()
            if choice == "m":
                break

            if choice.isdigit() and 1 <= int(choice) <= len(available_snapshots):
                snapshot = available_snapshots[int(choice) - 1]
                snapshot_id = snapshot[snapshot_id_key(snapshot_type)]
                creation_time = format_snapshot_time(snapshot)
                return snapshot[snapshot_arn_key(snapshot_type)], snapshot_id, creation_time

            print(f"Invalid selection. Use 1-{len(available_snapshots)} or m.")
    else:
        log_warn(f"No available snapshots found from the last {recent_days} days.")

    snapshot_id = input("Enter snapshot name: ").strip()
    if not snapshot_id:
        log_error("Snapshot name is required.")
        sys.exit(1)

    return get_snapshot_by_id(rds_client, snapshot_id, snapshot_type)


def start_export_task(
    rds_client,
    snapshot_arn: str,
    s3_bucket: str,
    kms_key_arn: str,
    iam_role_arn: str,
) -> str:
    """Start the RDS snapshot export task.

    Returns:
        The export task identifier.
    """
    export_id = f"rds-export-{int(time.time())}"
    log_info(f"Starting export task: {export_id}")

    rds_client.start_export_task(
        SourceArn=snapshot_arn,
        ExportTaskIdentifier=export_id,
        IamRoleArn=iam_role_arn,
        KmsKeyId=kms_key_arn,
        S3BucketName=s3_bucket,
    )

    log_info("Export task started...")
    return export_id


def wait_for_export_completion(rds_client, export_id: str) -> None:
    """Wait for the export task to complete, showing progress."""
    log_info("Waiting for export to complete (this can take a while)...")

    while True:
        response = rds_client.describe_export_tasks(
            ExportTaskIdentifier=export_id
        )

        if not response["ExportTasks"]:
            log_error(f"Export task not found: {export_id}")
            sys.exit(1)

        task = response["ExportTasks"][0]
        status = task["Status"]
        percent = task.get("PercentProgress", 0)

        match status:
            case "STARTING" | "IN_PROGRESS":
                print(f"\rProgress: {percent}%", end="", flush=True)
            case "COMPLETE":
                print()  # New line after progress
                log_info("Export complete!")
                return
            case "FAILED" | "CANCELLING" | "CANCELLED":
                print()  # New line after progress
                log_error(f"Export {status.lower()}")
                failure_cause = task.get("FailureCause")
                if failure_cause:
                    log_error(f"Reason: {failure_cause}")
                sys.exit(1)
            case _:
                log_warn(f"Unknown status: {status}")

        time.sleep(10)


def download_from_s3(s3_client, s3_bucket: str, export_id: str, local_dir: str) -> str:
    """Download exported files from S3.

    Returns:
        The size of downloaded data as a human-readable string.
    """
    log_info("Downloading files from S3...")

    export_dir = os.path.join(local_dir, export_id)
    os.makedirs(export_dir, exist_ok=True)

    bucket = s3_client.Bucket(s3_bucket)
    downloaded_files = 0
    for obj in bucket.objects.filter(Prefix=f"{export_id}/"):
        relative_key = os.path.relpath(obj.key, export_id)
        if relative_key == ".":
            continue

        local_path = os.path.join(export_dir, relative_key)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        bucket.download_file(obj.key, local_path)
        downloaded_files += 1

    if downloaded_files == 0:
        raise RuntimeError(f"No files found in s3://{s3_bucket}/{export_id}/")

    # Calculate total size
    total_size = sum(
        os.path.getsize(os.path.join(root, file))
        for root, _, files in os.walk(export_dir)
        for file in files
    )

    # Convert to human-readable format
    for unit in ["B", "KB", "MB", "GB"]:
        if total_size < 1024:
            size_str = f"{total_size:.1f} {unit}"
            break
        total_size /= 1024
    else:
        size_str = f"{total_size:.1f} TB"

    log_info(f"Downloaded: {size_str}")
    return size_str


def create_zip_archive(local_dir: str, export_id: str) -> tuple[str, str]:
    """Create a zip archive of the exported files.

    Returns:
        Tuple of (zip_filename, zip_size)
    """
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    zip_filename = f"rds-backup-{timestamp}.zip"
    zip_path = os.path.join(local_dir, zip_filename)

    log_info(f"Creating zip archive: {zip_filename}")

    export_dir = os.path.join(local_dir, export_id)

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(export_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, local_dir)
                zipf.write(file_path, arcname)

    # Get zip size
    zip_size_bytes = os.path.getsize(zip_path)
    zip_size = zip_size_bytes
    for unit in ["B", "KB", "MB", "GB"]:
        if zip_size < 1024:
            size_str = f"{zip_size:.1f} {unit}"
            break
        zip_size /= 1024
    else:
        size_str = f"{zip_size:.1f} TB"

    log_info(f"Archive created: {size_str}")
    return zip_filename, size_str


def delete_parquet_files(local_dir: str, export_id: str) -> None:
    """Delete the original Parquet files."""
    log_info("Deleting original parquet files...")
    export_dir = os.path.join(local_dir, export_id)
    shutil.rmtree(export_dir)
    log_info("Original files deleted")


def cleanup_s3(s3_client, s3_bucket: str, export_id: str) -> None:
    """Delete exported files from S3."""
    log_info("Deleting files from S3...")
    bucket = s3_client.Bucket(s3_bucket)
    bucket.objects.filter(Prefix=f"{export_id}/").delete()
    log_info("S3 files deleted")


def snapshot_id_from_arn(snapshot_arn: str) -> str:
    """Extract a snapshot identifier from an RDS snapshot ARN."""
    return snapshot_arn.rsplit(":", 1)[-1]


def get_export_task(rds_client, export_id: str) -> dict:
    """Get a single RDS export task."""
    response = rds_client.describe_export_tasks(
        ExportTaskIdentifier=export_id
    )

    if not response["ExportTasks"]:
        raise RuntimeError(f"Export task not found: {export_id}")

    return response["ExportTasks"][0]


def finish_export(
    rds_client,
    s3_client,
    args: argparse.Namespace,
    export_id: str,
    snapshot_id: str | None,
    snapshot_time: str | None,
    snapshot_type: str,
) -> tuple[str, str]:
    """Download, zip, and optionally clean up a completed export."""
    task = get_export_task(rds_client, export_id)
    status = task["Status"]
    if status != "COMPLETE":
        raise RuntimeError(f"Export task is not complete: {export_id} ({status})")

    if not snapshot_id:
        snapshot_id = snapshot_id_from_arn(task["SourceArn"])
    if not snapshot_time:
        try:
            _, _, snapshot_time = get_snapshot_by_id(
                rds_client,
                snapshot_id,
                snapshot_type,
            )
        except Exception:
            snapshot_time = "unknown"

    # Download from S3
    download_size = download_from_s3(s3_client, S3_BUCKET, export_id, LOCAL_TMP)

    # Create zip archive
    zip_filename, zip_size = create_zip_archive(LOCAL_TMP, export_id)

    # Delete original parquet files
    delete_parquet_files(LOCAL_TMP, export_id)

    # Cleanup S3 (optional)
    if args.cleanup_s3:
        cleanup_s3(s3_client, S3_BUCKET, export_id)
    elif not args.keep_s3:
        response = input("Delete exported files from S3? (y/N): ")
        if response.lower() == "y":
            cleanup_s3(s3_client, S3_BUCKET, export_id)

    # Summary
    print()
    log_info("=" * 30)
    log_info("SUMMARY")
    log_info("=" * 30)
    log_info(f"Snapshot: {snapshot_id}")
    log_info(f"Created:  {snapshot_time}")
    log_info(f"Export ID: {export_id}")
    log_info(f"Downloaded: {download_size}")
    log_info(f"Local zip: {Path(LOCAL_TMP, zip_filename)} ({zip_size})")
    log_info("=" * 30)

    return zip_filename, zip_size


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Export RDS snapshot to S3 and create a zip archive"
    )
    parser.add_argument(
        "--cleanup-s3",
        action="store_true",
        help="Delete exported files from S3 after creating zip",
    )
    parser.add_argument(
        "--keep-s3",
        action="store_true",
        help="Keep exported files in S3 (default: prompt)",
    )
    parser.add_argument(
        "--snapshot-id",
        help="Snapshot identifier to export. If omitted, choose from a list.",
    )
    parser.add_argument(
        "--snapshot-type",
        choices=["cluster", "instance"],
        default="cluster",
        help="Snapshot type to export.",
    )
    parser.add_argument(
        "--source-id",
        help="DB cluster or DB instance identifier for the interactive snapshot list.",
    )
    parser.add_argument(
        "--recent-days",
        type=int,
        default=7,
        help="How many days of snapshots to show in the interactive list.",
    )
    parser.add_argument(
        "--resume-export-id",
        help="Continue from a completed export task by downloading and zipping its S3 files.",
    )
    args = parser.parse_args()
    if args.recent_days < 1:
        log_error("--recent-days must be at least 1.")
        return 1
    validate_config(require_cluster_id=not args.source_id)

    # Initialize boto3 clients
    rds = boto3.client("rds", region_name="eu-west-1")
    s3 = boto3.resource("s3", region_name="eu-west-1")
    source_id = args.source_id or CLUSTER_ID
    if not source_id:
        log_error("--source-id is required when CLUSTER_ID is not set.")
        return 1

    # Create temp directory
    os.makedirs(LOCAL_TMP, exist_ok=True)
    os.chdir(LOCAL_TMP)

    try:
        if args.resume_export_id:
            finish_export(
                rds,
                s3,
                args,
                args.resume_export_id,
                snapshot_id=None,
                snapshot_time=None,
                snapshot_type=args.snapshot_type,
            )
            return 0

        # 1. Select snapshot
        if args.snapshot_id:
            snapshot_arn, snapshot_id, snapshot_time = get_snapshot_by_id(
                rds,
                args.snapshot_id,
                args.snapshot_type,
            )
        else:
            snapshot_arn, snapshot_id, snapshot_time = select_recent_snapshot(
                rds,
                source_id,
                args.snapshot_type,
                args.recent_days,
            )

        # 2. Start export task
        export_id = start_export_task(
            rds,
            snapshot_arn,
            S3_BUCKET,
            KMS_KEY_ARN,
            IAM_ROLE_ARN,
        )

        # 3. Wait for completion
        wait_for_export_completion(rds, export_id)

        # 4. Download, zip, and optionally clean up
        finish_export(
            rds,
            s3,
            args,
            export_id,
            snapshot_id=snapshot_id,
            snapshot_time=snapshot_time,
            snapshot_type=args.snapshot_type,
        )

        return 0

    except KeyboardInterrupt:
        log_warn("\nExport interrupted by user")
        return 1
    except Exception as e:
        log_error(f"Export failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
