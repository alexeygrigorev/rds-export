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
from datetime import datetime
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

# Validate required environment variables
required_vars = {
    "S3_BUCKET": S3_BUCKET,
    "KMS_KEY_ARN": KMS_KEY_ARN,
    "IAM_ROLE_ARN": IAM_ROLE_ARN,
    "CLUSTER_ID": CLUSTER_ID,
}
missing_vars = [name for name, value in required_vars.items() if not value]
if missing_vars:
    log_error(f"Missing required environment variables: {', '.join(missing_vars)}")
    log_error("Create a .env file with these values or set them in your environment.")
    sys.exit(1)


def get_latest_snapshot(rds_client, cluster_id: str) -> tuple[str, str, str]:
    """Get the latest RDS cluster snapshot ARN, ID, and creation time.

    Returns:
        Tuple of (snapshot_arn, snapshot_id, creation_time)
    """
    log_info("Finding latest RDS snapshot...")

    response = rds_client.describe_db_cluster_snapshots(
        DBClusterIdentifier=cluster_id
    )

    snapshots = response["DBClusterSnapshots"]
    if not snapshots:
        log_error("No snapshots found for cluster: %s", cluster_id)
        sys.exit(1)

    # Sort by creation time and get the latest
    latest = max(snapshots, key=lambda s: s["SnapshotCreateTime"])

    snapshot_arn = latest["DBClusterSnapshotArn"]
    snapshot_id = latest["DBClusterSnapshotIdentifier"]
    creation_time = latest["SnapshotCreateTime"].strftime("%Y-%m-%d %H:%M:%S UTC")

    log_info(f"Latest snapshot: {snapshot_id}")
    log_info(f"Created: {creation_time}")

    return snapshot_arn, snapshot_id, creation_time


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
    bucket.objects.filter(Prefix=f"{export_id}/").download_file(export_dir)

    # Calculate total size
    total_size = sum(
        f.stat().st_size
        for f in os.scandir(export_dir)
        if f.is_file()
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
    args = parser.parse_args()

    # Initialize boto3 clients
    rds = boto3.client("rds", region_name="eu-west-1")
    s3 = boto3.resource("s3", region_name="eu-west-1")

    # Create temp directory
    os.makedirs(LOCAL_TMP, exist_ok=True)
    os.chdir(LOCAL_TMP)

    try:
        # 1. Get latest snapshot
        snapshot_arn, snapshot_id, snapshot_time = get_latest_snapshot(rds, CLUSTER_ID)

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

        # 4. Download from S3
        download_size = download_from_s3(s3, S3_BUCKET, export_id, LOCAL_TMP)

        # 5. Create zip archive
        zip_filename, zip_size = create_zip_archive(LOCAL_TMP, export_id)

        # 6. Delete original parquet files
        delete_parquet_files(LOCAL_TMP, export_id)

        # 7. Cleanup S3 (optional)
        if args.cleanup_s3:
            cleanup_s3(s3, S3_BUCKET, export_id)
        elif not args.keep_s3:
            response = input("Delete exported files from S3? (y/N): ")
            if response.lower() == "y":
                cleanup_s3(s3, S3_BUCKET, export_id)

        # Summary
        print()
        log_info("=" * 30)
        log_info("SUMMARY")
        log_info("=" * 30)
        log_info(f"Snapshot: {snapshot_id}")
        log_info(f"Created:  {snapshot_time}")
        log_info(f"Export ID: {export_id}")
        log_info(f"Local zip: {os.path.join(LOCAL_TMP, zip_filename)} ({zip_size})")
        log_info("=" * 30)

        return 0

    except KeyboardInterrupt:
        log_warn("\nExport interrupted by user")
        return 1
    except Exception as e:
        log_error(f"Export failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
