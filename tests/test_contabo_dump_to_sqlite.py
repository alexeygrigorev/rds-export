import sqlite3
import sys
import tempfile
import unittest
import unittest.mock
from pathlib import Path

from contabo_dump_to_sqlite import (
    convert_sql_to_sqlite,
    main,
    parse_column_definition,
    parse_copy_field,
    parse_copy_header,
    pg_type_to_sqlite,
    strip_schema,
)

SAMPLE_SQL = """\
-- PostgreSQL database dump
SET statement_timeout = 0;

CREATE TABLE public.account_emailaddress (
    id integer NOT NULL,
    email character varying(254) NOT NULL,
    verified boolean NOT NULL,
    "primary" boolean NOT NULL,
    user_id bigint NOT NULL,
    CONSTRAINT email_check CHECK ((email <> ''::text))
);

ALTER TABLE public.account_emailaddress OWNER TO website_app;

COPY public.account_emailaddress (id, email, verified, "primary", user_id) FROM stdin;
1\talice@example.com\tt\tf\t42
2\tbob\\\\x@example.com\tt\tf\t\\N
\\.

-- Data for Name: account_emailconfirmation; Type: TABLE DATA; Schema: public; Owner: website_app

COPY public.account_emailconfirmation (id, created, sent, key, email_address_id) FROM stdin;
\\.

CREATE TABLE public.single_line (id bigint NOT NULL, note text);
COPY public.single_line (id, note) FROM stdin;
7\thello\\tworld
\\.
"""


class CopyFieldTests(unittest.TestCase):
    def test_null(self) -> None:
        self.assertIsNone(parse_copy_field("\\N"))

    def test_escapes(self) -> None:
        self.assertEqual(parse_copy_field("a\\\\b"), "a\\b")
        self.assertEqual(parse_copy_field("a\\tb"), "a\tb")
        self.assertEqual(parse_copy_field("a\\nb"), "a\nb")
        # Backslash-N inside a longer field is data, not NULL.
        self.assertEqual(parse_copy_field("a\\Nb"), "aNb")

    def test_plain(self) -> None:
        self.assertEqual(parse_copy_field("hello"), "hello")
        self.assertEqual(parse_copy_field(""), "")


class ParseTests(unittest.TestCase):
    def test_strip_schema(self) -> None:
        self.assertEqual(strip_schema("public.foo"), "foo")
        self.assertEqual(strip_schema('public."odd.name"'), "odd.name")
        self.assertEqual(strip_schema("foo"), "foo")

    def test_type_mapping(self) -> None:
        self.assertEqual(pg_type_to_sqlite("integer"), "INTEGER")
        self.assertEqual(pg_type_to_sqlite("bigint"), "INTEGER")
        self.assertEqual(pg_type_to_sqlite("boolean"), "INTEGER")
        self.assertEqual(pg_type_to_sqlite("double precision"), "REAL")
        self.assertEqual(pg_type_to_sqlite("numeric(10,2)"), "REAL")
        self.assertEqual(pg_type_to_sqlite("character varying(254)"), "TEXT")
        self.assertEqual(pg_type_to_sqlite("timestamp with time zone"), "TEXT")
        self.assertEqual(pg_type_to_sqlite("bytea"), "BLOB")
        self.assertEqual(pg_type_to_sqlite("interval"), "TEXT")
        self.assertEqual(pg_type_to_sqlite("text[]"), "TEXT")

    def test_column_definitions(self) -> None:
        self.assertEqual(
            parse_column_definition("    id integer NOT NULL,"),
            ("id", "INTEGER", "integer"),
        )
        self.assertEqual(
            parse_column_definition('    "primary" boolean NOT NULL,'),
            ("primary", "INTEGER", "boolean"),
        )
        self.assertEqual(
            parse_column_definition(
                "    promo text DEFAULT 'a,b'::text NOT NULL,"
            ),
            ("promo", "TEXT", "text"),
        )
        self.assertIsNone(
            parse_column_definition("    CONSTRAINT x CHECK ((a <> 0)),")
        )
        self.assertIsNone(parse_column_definition("    PRIMARY KEY (id),"))

    def test_copy_header(self) -> None:
        table, columns = parse_copy_header(
            'COPY public.account_emailaddress (id, email, "primary") FROM stdin;'
        )
        self.assertEqual(table, "account_emailaddress")
        self.assertEqual(columns, ["id", "email", "primary"])


class ConvertTests(unittest.TestCase):
    def test_end_to_end(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sql_path = root / "sample.sql"
            sql_path.write_text(SAMPLE_SQL)
            db_path = root / "out.db"

            tables, rows = convert_sql_to_sqlite(sql_path, db_path)

            self.assertEqual(tables, 3)
            self.assertEqual(rows, 3)
            self.assertEqual(db_path.read_bytes()[:16], b"SQLite format 3\x00")

            conn = sqlite3.connect(db_path)
            try:
                conn.row_factory = sqlite3.Row
                got = list(conn.execute('SELECT * FROM "account_emailaddress" ORDER BY id'))
                self.assertEqual(len(got), 2)
                # Postgres t/f booleans normalize to 1/0 for importer compatibility.
                self.assertEqual((got[0]["verified"], got[0]["primary"]), (1, 0))
                self.assertEqual(got[0]["email"], "alice@example.com")
                self.assertEqual(got[1]["email"], "bob\\x@example.com")
                self.assertIsNone(got[1]["user_id"])
                single = list(conn.execute('SELECT * FROM "single_line"'))
                self.assertEqual(single[0]["note"], "hello\tworld")
                empty = list(conn.execute('SELECT * FROM "account_emailconfirmation"'))
                self.assertEqual(empty, [])
            finally:
                conn.close()

    def test_cli_sql_mode(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sql_path = root / "sample.sql"
            sql_path.write_text(SAMPLE_SQL)
            out_path = root / "rds-dtc_website-test.db"
            argv = [
                "contabo_dump_to_sqlite.py",
                "--sql",
                str(sql_path),
                "--schema",
                "dtc_website",
                "--output",
                str(out_path),
            ]
            with unittest.mock.patch.object(sys, "argv", argv):
                self.assertEqual(main(), 0)
            self.assertTrue(out_path.is_file())
            self.assertGreater(out_path.stat().st_size, 0)


if __name__ == "__main__":
    unittest.main()
