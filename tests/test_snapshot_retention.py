import unittest
from datetime import datetime, timedelta, timezone

from cleanup_snapshots import Snapshot, select_surplus_snapshots


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


if __name__ == "__main__":
    unittest.main()
