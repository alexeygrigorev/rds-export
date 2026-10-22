#!/usr/bin/env -S uv run --python 3.13 python
"""
Convert a Contabo Coolify pg_dump custom-format backup to SQLite.

Contabo hosts pg_dump -Fc backups such as
/var/backups/coolify-postgres/website-pg-shared-*.dump.gz
(gzip-compressed, magic PGDMP). The dtc-website importers only accept
SQLite (review_import/workflow._validate_sqlite_file rejects anything
else with source-not-sqlite), and the RDS SnapshotExport pipeline only
produces empty 0-byte artifacts for the retired website database. This
script bridges the gap: it restores the dump to plain SQL with
pg_restore (read-only, no live database touched) and streams the
CREATE TABLE / COPY sections into a SQLite database that keeps the
existing artifact naming (rds-<schema>-YYYYMMDD-HHMMSS.db).

Usage:
    uv run contabo_dump_to_sqlite.py \
        --dump /var/backups/coolify-postgres/website-pg-shared-*.dump.gz \
        --schema dtc_website --out-dir /data/tmp/rds-export/website

    uv run contabo_dump_to_sqlite.py \
        --remote contabo-b:/var/backups/coolify-postgres/website-pg-shared-*.dump.gz \
        --schema dtc_website --out-dir /data/tmp/rds-export/website
"""

from __future__ import annotations

import argparse
import gzip
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path

LOCAL_TMP = os.getenv("LOCAL_TMP", "/data/tmp/rds-export")

COPY_RE = re.compile(r"^COPY\s+(\S+)\s*(?:\((.*?)\))?\s*FROM\s+stdin\s*;", re.IGNORECASE)
CREATE_TABLE_RE = re.compile(r"^CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)\s*\(", re.IGNORECASE)

CONSTRAINT_LEADERS = (
    "CONSTRAINT",
    "PRIMARY",
    "FOREIGN",
    "UNIQUE",
    "CHECK",
    "EXCLUDE",
    "LIKE",
)

# COPY text-format backslash escapes (whole-field \N is NULL, handled separately).
COPY_ESCAPES = {
    "b": "\b",
    "f": "\f",
    "n": "\n",
    "r": "\r",
    "t": "\t",
    "v": "\v",
    "\\": "\\",
}

TYPE_RE = re.compile(
    r"(?i)^(double\s+precision"
    r"|character\s+varying(\s*\(\s*\d+(\s*,\s*\d+)?\s*\))?"
    r"|timestamp\s+with(?:out)?\s+time\s+zone"
    r"|time\s+with(?:out)?\s+time\s+zone"
    r"|[A-Za-z_][\w$]*(?:\[\])?(\s*\(\s*\d+(\s*,\s*\d+)?\s*\))?)"
)


def log_info(msg: str) -> None:
    print(f"[INFO] {msg}")


def log_warn(msg: str) -> None:
    print(f"[WARN] {msg}")


def log_error(msg: str) -> None:
    print(f"[ERROR] {msg}", file=sys.stderr)


def unquote_ident(ident: str) -> str:
    ident = ident.strip()
    if len(ident) >= 2 and ident.startswith('"') and ident.endswith('"'):
        return ident[1:-1].replace('""', '"')
    return ident


def strip_schema(qualified: str) -> str:
    """Turn public.foo / public."odd.name" into foo / odd.name."""
    in_quotes = False
    for index, char in enumerate(qualified):
        if char == '"':
            # "" inside quotes is an escaped quote; skip both.
            if in_quotes and index + 1 < len(qualified) and qualified[index + 1] == '"':
                continue
            in_quotes = not in_quotes
        elif char == "." and not in_quotes:
            return unquote_ident(qualified[index + 1 :])
    return unquote_ident(qualified)


def split_top_level(body: str) -> list[str]:
    """Split a CREATE TABLE (...) body on top-level commas."""
    parts: list[str] = []
    depth = 0
    in_single = False
    in_double = False
    current: list[str] = []
    index = 0
    while index < len(body):
        char = body[index]
        if in_single:
            current.append(char)
            if char == "'":
                if index + 1 < len(body) and body[index + 1] == "'":
                    current.append("'")
                    index += 1
                else:
                    in_single = False
        elif in_double:
            current.append(char)
            if char == '"':
                if index + 1 < len(body) and body[index + 1] == '"':
                    current.append('"')
                    index += 1
                else:
                    in_double = False
        elif char == "'":
            in_single = True
            current.append(char)
        elif char == '"':
            in_double = True
            current.append(char)
        elif char == "(":
            depth += 1
            current.append(char)
        elif char == ")":
            depth = max(0, depth - 1)
            current.append(char)
        elif char == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)
        index += 1
    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


def pg_type_to_sqlite(pg_type: str) -> str:
    lowered = pg_type.strip().lower()
    if "bytea" in lowered:
        return "BLOB"
    if "bool" in lowered:
        return "INTEGER"
    if any(word in lowered for word in ("float", "double", "real", "numeric", "decimal")):
        return "REAL"
    if "interval" in lowered:
        return "TEXT"
    if "int" in lowered:
        return "INTEGER"
    return "TEXT"


def parse_column_definition(line: str) -> tuple[str, str, str] | None:
    """Parse one CREATE TABLE column line into (name, sqlite_type, pg_type)."""
    stripped = line.strip().rstrip(",").strip()
    if not stripped:
        return None
    first_token = stripped.split(None, 1)[0]
    # A quoted first token is always a column name, never a constraint
    # keyword (e.g. the common Django column "primary").
    if not first_token.startswith('"') and first_token.upper() in CONSTRAINT_LEADERS:
        return None
    if stripped.startswith('"'):
        end = stripped.find('"', 1)
        while end != -1 and end + 1 < len(stripped) and stripped[end + 1] == '"':
            end = stripped.find('"', end + 2)
        if end == -1:
            return None
        name = stripped[1:end].replace('""', '"')
        rest = stripped[end + 1 :].strip()
    else:
        bits = stripped.split(None, 1)
        if len(bits) < 2:
            return None
        name, rest = bits
    match = TYPE_RE.match(rest)
    pg_type = match.group(0) if match else "text"
    return name, pg_type_to_sqlite(pg_type), pg_type


def parse_copy_header(line: str) -> tuple[str, list[str]] | None:
    match = COPY_RE.match(line.strip())
    if not match:
        return None
    table = strip_schema(match.group(1))
    raw_columns = match.group(2)
    columns = [unquote_ident(col) for col in split_top_level(raw_columns)] if raw_columns else []
    return table, columns


def parse_copy_field(raw: str) -> str | None:
    """Decode one COPY text-format field; whole-field \\N is NULL."""
    if raw == "\\N":
        return None
    out: list[str] = []
    index = 0
    while index < len(raw):
        char = raw[index]
        if char == "\\" and index + 1 < len(raw):
            nxt = raw[index + 1]
            out.append(COPY_ESCAPES.get(nxt, nxt))
            index += 2
        else:
            out.append(char)
            index += 1
    return "".join(out)


def normalize_value(value: str | None, pg_type: str) -> object:
    if value is None:
        return None
    if "bool" in pg_type.strip().lower():
        lowered = value.lower()
        if lowered in ("t", "true", "1", "yes", "on"):
            return 1
        if lowered in ("f", "false", "0", "no", "off"):
            return 0
    return value


def convert_sql_to_sqlite(sql_path: Path, db_path: Path) -> tuple[int, int]:
    """Stream a pg_restore plain-SQL file into SQLite.

    Returns (tables, rows).
    """
    if db_path.exists():
        db_path.unlink()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = OFF")

    # table -> ([columns], {column: sqlite_type}, {column: pg_type})
    schemas: dict[str, tuple[list[str], dict[str, str], dict[str, str]]] = {}
    tables = 0
    total_rows = 0

    create_buffer: list[str] | None = None
    copy_table: str | None = None
    copy_columns: list[str] = []
    batch: list[tuple] = []
    placeholders = ""
    insert_sql = ""

    def flush() -> None:
        nonlocal total_rows
        if batch and copy_table is not None:
            conn.executemany(insert_sql, batch)
            total_rows += len(batch)
            batch.clear()

    opener = gzip.open if sql_path.suffix == ".gz" else open
    with opener(sql_path, "rt", encoding="utf-8", errors="replace") as handle:  # type: ignore[arg-type]
        for raw_line in handle:
            line = raw_line.rstrip("\n")
            if create_buffer is not None:
                create_buffer.append(line)
                if line.strip().endswith(");"):
                    first_line = create_buffer[0]
                    statement = "\n".join(create_buffer)
                    create_buffer = None
                    match = CREATE_TABLE_RE.match(first_line)
                    if match:
                        table = strip_schema(match.group(1))
                        body = statement[statement.find("(") + 1 : statement.rfind(")")]
                        columns: list[str] = []
                        col_types: dict[str, str] = {}
                        pg_types: dict[str, str] = {}
                        for part in split_top_level(body):
                            parsed = parse_column_definition(part)
                            if parsed is None:
                                continue
                            name, sqlite_type, pg_type = parsed
                            columns.append(name)
                            col_types[name] = sqlite_type
                            pg_types[name] = pg_type
                        schemas[table] = (columns, col_types, pg_types)
                continue

            if copy_table is not None:
                if line == "\\.":
                    flush()
                    copy_table = None
                    copy_columns = []
                    continue
                fields = [parse_copy_field(field) for field in line.split("\t")]
                _, _, pg_types = schemas.get(copy_table, ([], {}, {}))
                row = tuple(
                    normalize_value(
                        fields[index] if index < len(fields) else None,
                        pg_types.get(col, "text"),
                    )
                    for index, col in enumerate(copy_columns)
                )
                batch.append(row)
                if len(batch) >= 5000:
                    flush()
                continue

            stripped = line.strip()
            if stripped.upper().startswith("CREATE TABLE"):
                if stripped.endswith(");"):
                    # Single-line CREATE TABLE.
                    match = CREATE_TABLE_RE.match(stripped)
                    if match:
                        table = strip_schema(match.group(1))
                        body = stripped[stripped.find("(") + 1 : stripped.rfind(")")]
                        columns = []
                        col_types = {}
                        pg_types = {}
                        for part in split_top_level(body):
                            parsed = parse_column_definition(part)
                            if parsed is None:
                                continue
                            name, sqlite_type, pg_type = parsed
                            columns.append(name)
                            col_types[name] = sqlite_type
                            pg_types[name] = pg_type
                        schemas[table] = (columns, col_types, pg_types)
                else:
                    create_buffer = [line]
                continue

            copy_header = parse_copy_header(line)
            if copy_header is not None:
                flush()
                copy_table, copy_columns = copy_header
                if not copy_columns:
                    copy_columns = list(schemas.get(copy_table, ([], {}, {}))[0])
                if not copy_columns:
                    log_warn(f"Skipping COPY with no columns for {copy_table}")
                    copy_table = None
                    continue
                if copy_table not in schemas:
                    schemas[copy_table] = (
                        list(copy_columns),
                        {col: "TEXT" for col in copy_columns},
                        {col: "text" for col in copy_columns},
                    )
                else:
                    _, col_types, _ = schemas[copy_table]
                    for col in copy_columns:
                        col_types.setdefault(col, "TEXT")
                _, col_types, _ = schemas[copy_table]
                col_defs = ", ".join(
                    '"%s" %s' % (c, col_types.get(c, "TEXT")) for c in copy_columns
                )
                quoted = ", ".join(f'"{col}"' for col in copy_columns)
                conn.execute(f'CREATE TABLE IF NOT EXISTS "{copy_table}" ({col_defs})')
                if quoted:
                    placeholders = ", ".join(["?"] * len(copy_columns))
                    insert_sql = f'INSERT INTO "{copy_table}" ({quoted}) VALUES ({placeholders})'
                tables += 1
                continue

    flush()
    # Create empty tables that had a schema but no COPY section.
    for table, (columns, col_types, _) in schemas.items():
        if not columns:
            continue
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        if exists is None:
            defs = ", ".join(f'"{col}" {col_types.get(col, "TEXT")}' for col in columns)
            conn.execute(f'CREATE TABLE "{table}" ({defs})')
            tables += 1

    conn.commit()
    table_count = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
    ).fetchone()[0]
    conn.close()
    return table_count, total_rows


def restore_dump_to_sql(dump_path: Path, sql_path: Path) -> None:
    """Run pg_restore (read-only) to expand a custom-format dump to plain SQL."""
    pg_restore = shutil.which("pg_restore")
    if pg_restore is None:
        raise RuntimeError("pg_restore not found on PATH")
    result = subprocess.run(
        [pg_restore, "-f", str(sql_path), str(dump_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"pg_restore failed: {result.stderr.strip()[-2000:]}")


def resolve_remote_dump(remote: str, dest_dir: Path) -> Path:
    """Copy the newest dump matching host:glob into dest_dir."""
    if ":" not in remote:
        raise ValueError("--remote must look like host:/path/glob*.dump.gz")
    host, pattern = remote.split(":", 1)
    listed = subprocess.run(
        ["ssh", "-o", "ConnectTimeout=15", host, f"ls -t {pattern}"],
        capture_output=True,
        text=True,
        check=False,
    )
    if listed.returncode != 0 or not listed.stdout.strip():
        raise RuntimeError(f"no remote dump matches {remote}: {listed.stderr.strip()[-500:]}")
    newest = listed.stdout.strip().splitlines()[0].strip()
    suffix = ".dump.gz" if newest.endswith(".gz") else ".dump"
    local = dest_dir / f"source{suffix}"
    copied = subprocess.run(
        ["scp", "-o", "ConnectTimeout=15", f"{host}:{newest}", str(local)],
        capture_output=True,
        text=True,
        check=False,
    )
    if copied.returncode != 0:
        raise RuntimeError(f"scp failed: {copied.stderr.strip()[-500:]}")
    log_info(f"Fetched {host}:{newest}")
    return local


def gunzip_if_needed(path: Path, work_dir: Path) -> Path:
    if path.suffix != ".gz":
        return path
    out = work_dir / f"{path.name[:-3]}"
    with gzip.open(path, "rb") as src, open(out, "wb") as dst:
        shutil.copyfileobj(src, dst)
    return out


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--dump", type=Path, help="Local .dump or .dump.gz file.")
    source.add_argument("--sql", type=Path, help="Pre-extracted pg_restore plain SQL.")
    source.add_argument("--remote", help="host:/path/glob for the newest Contabo dump.")
    parser.add_argument("--schema", required=True, help="Schema tag for artifact naming.")
    parser.add_argument("--output", type=Path, help="Explicit SQLite output path.")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path(LOCAL_TMP),
        help="Directory for auto-named artifacts (default: $LOCAL_TMP).",
    )
    parser.add_argument("--work-dir", type=Path, help="Scratch dir for pg_restore SQL.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    tmpdir: tempfile.TemporaryDirectory | None = None
    try:
        if args.remote or (args.dump is None and args.sql is None):
            tmpdir = tempfile.TemporaryDirectory(prefix="contabo-sqlite-")
            work_dir = Path(tmpdir.name)
        else:
            work_dir = args.work_dir or Path(tempfile.mkdtemp(prefix="contabo-sqlite-"))

        if args.remote:
            assert args.dump is None and args.sql is None
            dump_path: Path | None = resolve_remote_dump(args.remote, work_dir)
        else:
            dump_path = args.dump
            if dump_path is not None and not dump_path.exists():
                log_error(f"Dump not found: {dump_path}")
                return 1
            if args.sql is not None and not args.sql.exists():
                log_error(f"SQL not found: {args.sql}")
                return 1

        work_dir.mkdir(parents=True, exist_ok=True)
        if args.sql is not None:
            sql_path = args.sql
        else:
            assert dump_path is not None
            expanded = gunzip_if_needed(dump_path, work_dir)
            sql_path = work_dir / f"{expanded.stem}.sql"
            log_info(f"Expanding {dump_path.name} with pg_restore (read-only)...")
            restore_dump_to_sql(expanded, sql_path)

        if args.output is not None:
            db_path = args.output
        else:
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            db_path = args.out_dir / f"rds-{args.schema}-{timestamp}.db"

        log_info(f"Converting to SQLite: {db_path}")
        tables, rows = convert_sql_to_sqlite(sql_path, db_path)
        size_mb = db_path.stat().st_size / 1024 / 1024
        log_info(f"Tables: {tables}  Rows: {rows:,}  Size: {size_mb:.1f} MB")
        if db_path.stat().st_size == 0:
            log_error("Output database is empty")
            return 1
        header = db_path.read_bytes()[:16]
        if header != b"SQLite format 3\x00":
            log_error("Output is not a valid SQLite file")
            return 1
        return 0
    except (RuntimeError, ValueError, OSError, subprocess.SubprocessError) as exc:
        log_error(str(exc))
        return 1
    finally:
        if tmpdir is not None:
            tmpdir.cleanup()


if __name__ == "__main__":
    sys.exit(main())
