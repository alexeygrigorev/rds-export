import sqlite3
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

from parquet_to_sqlite import main


class EmptyExportTests(unittest.TestCase):
    def test_explicit_schema_creates_valid_empty_sqlite_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            zip_path = root / "rds-backup-empty.zip"
            output_path = root / "website.db"
            with zipfile.ZipFile(zip_path, "w") as archive:
                archive.writestr("export-metadata.json", "{}")

            argv = [
                "parquet_to_sqlite.py",
                "--zip",
                str(zip_path),
                "--schema",
                "dtc_website",
                "--output",
                str(output_path),
            ]
            with patch.object(sys, "argv", argv):
                self.assertEqual(main(), 0)

            self.assertTrue(output_path.is_file())
            connection = sqlite3.connect(output_path)
            try:
                tables = connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            finally:
                connection.close()
            self.assertEqual(tables, [])

    def test_empty_export_without_explicit_schema_still_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            zip_path = Path(directory) / "rds-backup-empty.zip"
            with zipfile.ZipFile(zip_path, "w") as archive:
                archive.writestr("export-metadata.json", "{}")

            argv = ["parquet_to_sqlite.py", "--zip", str(zip_path)]
            with patch.object(sys, "argv", argv):
                self.assertEqual(main(), 1)


if __name__ == "__main__":
    unittest.main()
