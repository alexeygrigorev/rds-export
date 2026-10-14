import sqlite3
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from parquet_to_sqlite import find_parquet_files, import_parquet_to_sqlite


class MultiPartTableTests(unittest.TestCase):
    def test_every_part_file_of_a_table_is_imported(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            schema_dir = Path(directory)
            table_dir = schema_dir / "cmp.data_outbox" / "1"
            table_dir.mkdir(parents=True)
            for part, ids in enumerate(([1, 2], [3], [4, 5, 6])):
                pq.write_table(pa.table({"id": ids}), table_dir / f"part-{part}.parquet")
            single_dir = schema_dir / "cmp.data_user" / "1"
            single_dir.mkdir(parents=True)
            pq.write_table(pa.table({"id": [7]}), single_dir / "part-0.parquet")

            parquet_files = find_parquet_files(schema_dir)

            self.assertEqual(len(parquet_files["data_outbox"]), 3)
            self.assertEqual(len(parquet_files["data_user"]), 1)

            conn = sqlite3.connect(":memory:")
            rows = import_parquet_to_sqlite(parquet_files["data_outbox"], conn, "data_outbox")
            ids = [row[0] for row in conn.execute('SELECT id FROM "data_outbox" ORDER BY id')]
            conn.close()

            self.assertEqual(rows, 6)
            self.assertEqual(ids, [1, 2, 3, 4, 5, 6])


if __name__ == "__main__":
    unittest.main()
