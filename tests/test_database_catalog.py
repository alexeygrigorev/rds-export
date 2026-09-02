import unittest
from pathlib import Path
from unittest.mock import patch

from database_catalog import DATABASES, DATABASES_BY_KEY
from run_pipeline import target_environment


class DatabaseCatalogTests(unittest.TestCase):
    def test_catalog_contains_every_production_target(self) -> None:
        self.assertEqual(
            {
                database.key: (
                    database.snapshot_type,
                    database.source_id,
                    database.default_schema,
                )
                for database in DATABASES
            },
            {
                "aisl": ("instance", "ai-shipping-labs", "aisl_prod"),
                "cmp": ("cluster", "course-management-manual", "prod"),
                "website": ("instance", "website-production", "dtc_website"),
                "relay": ("instance", "relay-production", "relay"),
            },
        )

    def test_lookup_is_complete_and_uses_unique_keys(self) -> None:
        self.assertEqual(len(DATABASES_BY_KEY), len(DATABASES))
        self.assertEqual(tuple(DATABASES_BY_KEY.values()), DATABASES)

    def test_each_target_uses_an_isolated_workspace(self) -> None:
        base = Path("isolated-export-workspaces")
        with patch("run_pipeline.LOCAL_TMP", new=base):
            self.assertEqual(
                {
                    database.key: Path(target_environment(database)["LOCAL_TMP"])
                    for database in DATABASES
                },
                {
                    "aisl": base / "aisl",
                    "cmp": base / "cmp",
                    "website": base / "website",
                    "relay": base / "relay",
                },
            )


if __name__ == "__main__":
    unittest.main()
