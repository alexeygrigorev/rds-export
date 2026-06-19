#!/usr/bin/env -S uv run --python 3.13 python
"""
Create manual RDS snapshots.

This is the first step before exporting the latest snapshot to S3.
"""

import argparse
import time
from dataclasses import dataclass
from datetime import datetime

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv


load_dotenv()


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


@dataclass(frozen=True)
class Database:
    key: str
    name: str
    snapshot_type: str
    identifier: str


DATABASES = (
    Database(
        key="aisl",
        name="AI Shipping Labs",
        snapshot_type="instance",
        identifier="ai-shipping-labs",
    ),
    Database(
        key="cmp",
        name="Course Management",
        snapshot_type="cluster",
        identifier="course-management-manual",
    ),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a manual AWS RDS snapshot.")
    parser.add_argument("--region", default="eu-west-1", help="AWS region.")
    parser.add_argument("--profile", help="AWS profile name.")
    parser.add_argument("--db", choices=[db.key for db in DATABASES], help="DB to snapshot.")
    parser.add_argument("--snapshot-id", help="Override the generated snapshot id.")
    parser.add_argument("--yes", action="store_true", help="Create the snapshot without confirmation.")
    parser.add_argument("--no-wait", action="store_true", help="Start the snapshot and exit immediately.")
    parser.add_argument("--poll-interval", type=int, default=30, help="Seconds between status checks.")
    return parser.parse_args()


def create_aws_session(args: argparse.Namespace) -> boto3.Session:
    session_kwargs = {"region_name": args.region}
    if args.profile:
        session_kwargs["profile_name"] = args.profile

    return boto3.Session(**session_kwargs)


def verify_aws(session: boto3.Session) -> None:
    try:
        session.client("sts").get_caller_identity()
    except NoCredentialsError as exc:
        raise SystemExit("AWS credentials were not found.") from exc
    except ClientError as exc:
        raise SystemExit(f"AWS credentials are not working: {exc}") from exc


def select_database(selected_key: str | None) -> Database:
    if selected_key:
        return next(db for db in DATABASES if db.key == selected_key)

    print()
    print("Select DB to snapshot:")
    for index, db in enumerate(DATABASES, start=1):
        print(f"  {index}. {db.name} ({db.identifier}) -> {db.key}-YYYY-MM-DD")

    while True:
        choice = input("Enter 1 or 2: ").strip().lower()
        if choice.isdigit() and 1 <= int(choice) <= len(DATABASES):
            return DATABASES[int(choice) - 1]

        for db in DATABASES:
            if choice == db.key:
                return db

        print("Invalid selection. Use 1, 2, aisl, or cmp.")


def generate_snapshot_id(db: Database) -> str:
    return f"{db.key}-{datetime.now().strftime('%Y-%m-%d')}"


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


def resolve_snapshot_id(rds_client, args: argparse.Namespace, db: Database) -> str:
    snapshot_id = args.snapshot_id or generate_snapshot_id(db)

    if not snapshot_exists(rds_client, db, snapshot_id):
        return snapshot_id

    suggested = f"{snapshot_id}-{datetime.now().strftime('%H%M%S')}"
    log_warn(f"Snapshot '{snapshot_id}' already exists.")

    if args.yes:
        log_info(f"Using '{suggested}' instead.")
        return suggested

    custom_snapshot_id = input(
        f"Enter another snapshot id or press Enter for '{suggested}': "
    ).strip()
    return custom_snapshot_id or suggested


def create_snapshot(rds_client, db: Database, snapshot_id: str) -> None:
    log_info(f"Creating snapshot: {snapshot_id}")

    if db.snapshot_type == "cluster":
        rds_client.create_db_cluster_snapshot(
            DBClusterIdentifier=db.identifier,
            DBClusterSnapshotIdentifier=snapshot_id,
        )
    else:
        rds_client.create_db_snapshot(
            DBInstanceIdentifier=db.identifier,
            DBSnapshotIdentifier=snapshot_id,
        )


def get_snapshot_status(rds_client, db: Database, snapshot_id: str) -> str:
    if db.snapshot_type == "cluster":
        response = rds_client.describe_db_cluster_snapshots(
            DBClusterSnapshotIdentifier=snapshot_id
        )
        return response["DBClusterSnapshots"][0]["Status"].lower()

    response = rds_client.describe_db_snapshots(
        DBSnapshotIdentifier=snapshot_id
    )
    return response["DBSnapshots"][0]["Status"].lower()


def wait_for_snapshot(rds_client, db: Database, snapshot_id: str, poll_interval: int) -> None:
    log_info("Waiting for snapshot to become available...")

    while True:
        status = get_snapshot_status(rds_client, db, snapshot_id)
        print(f"{datetime.now().strftime('%H:%M:%S')} status: {status}")

        if status == "available":
            return

        if status in {"deleted", "deleting", "failed"}:
            raise SystemExit(f"Snapshot ended with unexpected status: {status}")

        time.sleep(poll_interval)


def main() -> int:
    args = parse_args()
    if args.poll_interval < 1:
        log_error("--poll-interval must be at least 1 second.")
        return 1

    session = create_aws_session(args)
    verify_aws(session)
    rds_client = session.client("rds")

    db = select_database(args.db)
    snapshot_id = resolve_snapshot_id(rds_client, args, db)

    print()
    print(f"DB:       {db.name}")
    print(f"Target:   {db.identifier}")
    print(f"Region:   {args.region}")
    print(f"Snapshot: {snapshot_id}")

    if not args.yes:
        confirm = input("Create this snapshot? [y/N] ").strip().lower()
        if confirm not in {"y", "yes"}:
            print("Cancelled.")
            return 0

    try:
        create_snapshot(rds_client, db, snapshot_id)
        log_info("Snapshot started.")

        if not args.no_wait:
            wait_for_snapshot(rds_client, db, snapshot_id, args.poll_interval)
            log_info("Snapshot is available.")
        else:
            log_info("Run the script without --no-wait to poll until it is available.")
    except ClientError as exc:
        log_error(str(exc))
        return 1

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nCancelled.")
        raise SystemExit(130)
