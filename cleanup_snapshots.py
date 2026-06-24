#!/usr/bin/env -S uv run --python 3.13 python
"""
List and delete old manual RDS snapshots.

Dry-run is the default. Pass --delete to actually remove matching snapshots.
"""

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv


load_dotenv()


class Colors:
    """ANSI color codes for terminal output."""
    GREEN = "\033[0;32m"
    YELLOW = "\033[1;33m"
    RED = "\033[0;31m"
    NC = "\033[0m"


def log_info(msg: str) -> None:
    print(f"{Colors.GREEN}[INFO]{Colors.NC} {msg}")


def log_warn(msg: str) -> None:
    print(f"{Colors.YELLOW}[WARN]{Colors.NC} {msg}")


def log_error(msg: str) -> None:
    print(f"{Colors.RED}[ERROR]{Colors.NC} {msg}")


@dataclass(frozen=True)
class Snapshot:
    kind: str
    identifier: str
    source_id: str
    created_at: datetime
    age_days: int
    size_gb: int | None
    status: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="List and optionally delete old manual RDS snapshots."
    )
    parser.add_argument("--region", default="eu-west-1", help="AWS region.")
    parser.add_argument("--profile", help="AWS profile name.")
    parser.add_argument(
        "--db-instance-id",
        help="Limit to manual DB instance snapshots for this DB instance.",
    )
    parser.add_argument(
        "--db-cluster-id",
        help="Limit to manual DB cluster snapshots for this DB cluster.",
    )
    parser.add_argument(
        "--snapshot-prefix",
        help="Limit to snapshot identifiers starting with this prefix.",
    )
    parser.add_argument(
        "--snapshot-kind",
        choices=["instance", "cluster", "both"],
        default="both",
        help="Snapshot kind to scan when no DB identifier is specified.",
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=30,
        help="Keep snapshots newer than this many days.",
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Delete matching snapshots. Without this, the command is a dry run.",
    )
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


def validate_args(args: argparse.Namespace) -> None:
    if args.retention_days < 1:
        raise SystemExit("--retention-days must be at least 1.")

    if args.db_instance_id and args.db_cluster_id:
        raise SystemExit("Use only one of --db-instance-id or --db-cluster-id.")

    if not (args.db_instance_id or args.db_cluster_id or args.snapshot_prefix):
        raise SystemExit(
            "Provide at least one filter: --db-instance-id, --db-cluster-id, "
            "or --snapshot-prefix."
        )


def snapshot_age_days(created_at: datetime, now: datetime) -> int:
    created_utc = created_at.astimezone(timezone.utc)
    return (now - created_utc).days


def normalize_instance_snapshot(snapshot: dict, now: datetime) -> Snapshot:
    created_at = snapshot["SnapshotCreateTime"]
    return Snapshot(
        kind="instance",
        identifier=snapshot["DBSnapshotIdentifier"],
        source_id=snapshot.get("DBInstanceIdentifier", ""),
        created_at=created_at,
        age_days=snapshot_age_days(created_at, now),
        size_gb=snapshot.get("AllocatedStorage"),
        status=snapshot["Status"],
    )


def normalize_cluster_snapshot(snapshot: dict, now: datetime) -> Snapshot:
    created_at = snapshot["SnapshotCreateTime"]
    return Snapshot(
        kind="cluster",
        identifier=snapshot["DBClusterSnapshotIdentifier"],
        source_id=snapshot.get("DBClusterIdentifier", ""),
        created_at=created_at,
        age_days=snapshot_age_days(created_at, now),
        size_gb=snapshot.get("AllocatedStorage"),
        status=snapshot["Status"],
    )


def list_instance_snapshots(
    rds_client,
    args: argparse.Namespace,
    now: datetime,
) -> list[Snapshot]:
    paginate_args = {"SnapshotType": "manual"}
    if args.db_instance_id:
        paginate_args["DBInstanceIdentifier"] = args.db_instance_id

    snapshots = []
    paginator = rds_client.get_paginator("describe_db_snapshots")
    for page in paginator.paginate(**paginate_args):
        snapshots.extend(
            normalize_instance_snapshot(snapshot, now)
            for snapshot in page["DBSnapshots"]
        )

    return snapshots


def list_cluster_snapshots(
    rds_client,
    args: argparse.Namespace,
    now: datetime,
) -> list[Snapshot]:
    paginate_args = {"SnapshotType": "manual"}
    if args.db_cluster_id:
        paginate_args["DBClusterIdentifier"] = args.db_cluster_id

    snapshots = []
    paginator = rds_client.get_paginator("describe_db_cluster_snapshots")
    for page in paginator.paginate(**paginate_args):
        snapshots.extend(
            normalize_cluster_snapshot(snapshot, now)
            for snapshot in page["DBClusterSnapshots"]
        )

    return snapshots


def list_manual_snapshots(rds_client, args: argparse.Namespace) -> list[Snapshot]:
    now = datetime.now(timezone.utc)
    snapshots = []

    if args.db_instance_id:
        snapshots.extend(list_instance_snapshots(rds_client, args, now))
    elif args.db_cluster_id:
        snapshots.extend(list_cluster_snapshots(rds_client, args, now))
    else:
        if args.snapshot_kind in {"instance", "both"}:
            snapshots.extend(list_instance_snapshots(rds_client, args, now))
        if args.snapshot_kind in {"cluster", "both"}:
            snapshots.extend(list_cluster_snapshots(rds_client, args, now))

    if args.snapshot_prefix:
        snapshots = [
            snapshot
            for snapshot in snapshots
            if snapshot.identifier.startswith(args.snapshot_prefix)
        ]

    return sorted(snapshots, key=lambda snapshot: snapshot.created_at)


def select_expired_snapshots(
    snapshots: list[Snapshot],
    retention_days: int,
) -> list[Snapshot]:
    return [
        snapshot
        for snapshot in snapshots
        if snapshot.age_days >= retention_days
    ]


def format_size(size_gb: int | None) -> str:
    if size_gb is None:
        return "-"
    return f"{size_gb} GB"


def print_snapshots(snapshots: list[Snapshot]) -> None:
    if not snapshots:
        log_info("No matching old manual snapshots found.")
        return

    print()
    print(
        f"{'KIND':<8} {'SNAPSHOT':<36} {'SOURCE':<28} "
        f"{'CREATED':<20} {'AGE':>5} {'SIZE':>8} {'STATUS':<12}"
    )
    print("-" * 124)
    for snapshot in snapshots:
        created = snapshot.created_at.strftime("%Y-%m-%d %H:%M UTC")
        print(
            f"{snapshot.kind:<8} {snapshot.identifier:<36} {snapshot.source_id:<28} "
            f"{created:<20} {snapshot.age_days:>4}d {format_size(snapshot.size_gb):>8} "
            f"{snapshot.status:<12}"
        )


def delete_snapshot(rds_client, snapshot: Snapshot) -> None:
    if snapshot.kind == "cluster":
        rds_client.delete_db_cluster_snapshot(
            DBClusterSnapshotIdentifier=snapshot.identifier
        )
    else:
        rds_client.delete_db_snapshot(
            DBSnapshotIdentifier=snapshot.identifier
        )


def delete_snapshots(rds_client, snapshots: list[Snapshot]) -> None:
    for snapshot in snapshots:
        if snapshot.status != "available":
            log_warn(
                f"Skipping {snapshot.kind} snapshot with status "
                f"{snapshot.status}: {snapshot.identifier}"
            )
            continue

        log_info(f"Deleting {snapshot.kind} snapshot: {snapshot.identifier}")
        delete_snapshot(rds_client, snapshot)


def main() -> int:
    args = parse_args()
    validate_args(args)

    session = create_aws_session(args)
    verify_aws(session)
    rds_client = session.client("rds")

    snapshots = list_manual_snapshots(rds_client, args)
    expired_snapshots = select_expired_snapshots(snapshots, args.retention_days)

    print_snapshots(expired_snapshots)
    print()
    log_info(f"Matched old manual snapshots: {len(expired_snapshots)}")
    log_info(f"Retention: {args.retention_days} days")
    log_info(f"Region: {args.region}")

    if not args.delete:
        log_warn("Dry run only. Pass --delete to delete these snapshots.")
        return 0

    delete_snapshots(rds_client, expired_snapshots)
    log_info("Deletion requests submitted.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
