import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from cleanup_snapshots import (
    Snapshot,
    prune_database_snapshots,
    select_surplus_snapshots,
)
from database_catalog import DATABASES_BY_KEY


BASE = datetime(2026, 9, 16, 12, 0, tzinfo=timezone.utc)


def make_snapshot(identifier: str, days_old: int, kind: str = "instance") -> Snapshot:
    created_at = BASE - timedelta(days=days_old)
    return Snapshot(
        kind=kind,
        identifier=identifier,
        source_id="source",
        created_at=created_at,
        age_days=days_old,
        size_gb=10,
        status="available",
    )


class SelectSurplusSnapshotsTests(unittest.TestCase):
    def test_keeps_the_most_recent_and_returns_the_rest_oldest_first(self) -> None:
        snapshots = [make_snapshot(f"db-{day}", days_old=day) for day in range(8)]

        surplus = select_surplus_snapshots(snapshots, keep_last=5)

        self.assertEqual(
            [snapshot.identifier for snapshot in surplus],
            ["db-7", "db-6", "db-5"],
        )

    def test_keeps_everything_when_below_the_limit(self) -> None:
        snapshots = [make_snapshot(f"db-{day}", days_old=day) for day in range(3)]

        self.assertEqual(select_surplus_snapshots(snapshots, keep_last=5), [])

    def test_age_is_irrelevant_only_recency_rank_matters(self) -> None:
        """A year-old snapshot survives if it is among the newest kept."""
        snapshots = [make_snapshot("db-ancient", days_old=400)]

        self.assertEqual(select_surplus_snapshots(snapshots, keep_last=5), [])

    def test_handles_an_empty_source(self) -> None:
        self.assertEqual(select_surplus_snapshots([], keep_last=5), [])


class PruneDatabaseSnapshotsTests(unittest.TestCase):
    def test_cluster_database_is_scoped_by_cluster_id_and_key_prefix(self) -> None:
        db = DATABASES_BY_KEY["cmp"]
        rds_client = MagicMock()
        paginator = rds_client.get_paginator.return_value
        paginator.paginate.return_value = [
            {
                "DBClusterSnapshots": [
                    {
                        "DBClusterSnapshotIdentifier": f"cmp-2026-09-{day:02d}",
                        "DBClusterIdentifier": db.source_id,
                        "SnapshotCreateTime": BASE - timedelta(days=day),
                        "Status": "available",
                        "AllocatedStorage": 20,
                    }
                    for day in range(1, 8)
                ]
            }
        ]

        surplus = prune_database_snapshots(rds_client, db, keep_last=5)

        rds_client.get_paginator.assert_called_once_with(
            "describe_db_cluster_snapshots"
        )
        paginator.paginate.assert_called_once_with(
            SnapshotType="manual", DBClusterIdentifier=db.source_id
        )
        self.assertEqual(
            [snapshot.identifier for snapshot in surplus],
            ["cmp-2026-09-07", "cmp-2026-09-06"],
        )
        self.assertEqual(
            [
                call.kwargs["DBClusterSnapshotIdentifier"]
                for call in rds_client.delete_db_cluster_snapshot.call_args_list
            ],
            ["cmp-2026-09-07", "cmp-2026-09-06"],
        )

    def test_unrelated_snapshots_on_the_same_source_are_never_deleted(self) -> None:
        """Final snapshots of retired clusters must survive the prune."""
        db = DATABASES_BY_KEY["cmp"]
        rds_client = MagicMock()
        rds_client.get_paginator.return_value.paginate.return_value = [
            {
                "DBClusterSnapshots": [
                    {
                        "DBClusterSnapshotIdentifier": identifier,
                        "DBClusterIdentifier": db.source_id,
                        "SnapshotCreateTime": BASE - timedelta(days=days_old),
                        "Status": "available",
                        "AllocatedStorage": 20,
                    }
                    for identifier, days_old in [
                        ("course-management-cluster-2026-final-snapshot", 200),
                        ("dev-course-management-cluster-2026-02-26-22-02", 202),
                        ("cmp-2026-01-01", 100),
                    ]
                ]
            }
        ]

        surplus = prune_database_snapshots(rds_client, db, keep_last=1)

        self.assertEqual(surplus, [])
        rds_client.delete_db_cluster_snapshot.assert_not_called()

    def test_instance_database_uses_the_instance_api(self) -> None:
        db = DATABASES_BY_KEY["website"]
        rds_client = MagicMock()
        paginator = rds_client.get_paginator.return_value
        paginator.paginate.return_value = [
            {
                "DBSnapshots": [
                    {
                        "DBSnapshotIdentifier": f"website-2026-09-{day:02d}",
                        "DBInstanceIdentifier": db.source_id,
                        "SnapshotCreateTime": BASE - timedelta(days=day),
                        "Status": "available",
                        "AllocatedStorage": 5,
                    }
                    for day in range(1, 4)
                ]
            }
        ]

        surplus = prune_database_snapshots(rds_client, db, keep_last=2)

        paginator.paginate.assert_called_once_with(
            SnapshotType="manual", DBInstanceIdentifier=db.source_id
        )
        self.assertEqual(
            [snapshot.identifier for snapshot in surplus], ["website-2026-09-03"]
        )
        rds_client.delete_db_snapshot.assert_called_once_with(
            DBSnapshotIdentifier="website-2026-09-03"
        )

    def test_snapshots_still_being_created_are_skipped_not_deleted(self) -> None:
        db = DATABASES_BY_KEY["website"]
        rds_client = MagicMock()
        rds_client.get_paginator.return_value.paginate.return_value = [
            {
                "DBSnapshots": [
                    {
                        "DBSnapshotIdentifier": "website-2026-09-01",
                        "DBInstanceIdentifier": db.source_id,
                        "SnapshotCreateTime": BASE - timedelta(days=9),
                        "Status": "creating",
                        "AllocatedStorage": 5,
                    },
                    {
                        "DBSnapshotIdentifier": "website-2026-09-02",
                        "DBInstanceIdentifier": db.source_id,
                        "SnapshotCreateTime": BASE - timedelta(days=1),
                        "Status": "available",
                        "AllocatedStorage": 5,
                    },
                ]
            }
        ]

        prune_database_snapshots(rds_client, db, keep_last=1)

        rds_client.delete_db_snapshot.assert_not_called()


if __name__ == "__main__":
    unittest.main()
