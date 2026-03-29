#!/usr/bin/env python3
"""Generate certified hash test values for bitfilters_duckdb_hash.

This script runs against the ACTUAL DuckDB Python package for a specific
version, producing baked-in expected values for sqllogictests. Run it in
a venv with the target DuckDB version installed:

    uv venv --python 3.13 .venv-1.5.1
    uv pip install --python .venv-1.5.1 duckdb==1.5.1
    .venv-1.5.1/bin/python generate_hash_values.py

    uv venv --python 3.13 .venv-1.4.4
    uv pip install --python .venv-1.4.4 duckdb==1.4.4
    .venv-1.4.4/bin/python generate_hash_values.py

The output is a JSON file with all hash values keyed by version.
"""

from __future__ import annotations

import json
import sys

import duckdb


def get_hash(conn: duckdb.DuckDBPyConnection, expr: str) -> int:
    """Execute hash expression and return as unsigned int."""
    result = conn.execute(f"SELECT ({expr})::UBIGINT").fetchone()
    assert result is not None
    return result[0]


def generate_values() -> dict:
    """Generate all test hash values for the current DuckDB version."""
    conn = duckdb.connect()
    version = duckdb.__version__

    print(f"DuckDB version: {version}", file=sys.stderr)

    values: dict = {"duckdb_version": version}

    # === Single-value hashes ===
    single = {}

    # Integers
    single["42_integer"] = get_hash(conn, "hash(42::INTEGER)")
    single["42_bigint"] = get_hash(conn, "hash(42::BIGINT)")
    single["42_tinyint"] = get_hash(conn, "hash(42::TINYINT)")
    single["42_smallint"] = get_hash(conn, "hash(42::SMALLINT)")
    single["42_utinyint"] = get_hash(conn, "hash(42::UTINYINT)")
    single["42_usmallint"] = get_hash(conn, "hash(42::USMALLINT)")
    single["42_uinteger"] = get_hash(conn, "hash(42::UINTEGER)")
    single["42_ubigint"] = get_hash(conn, "hash(42::UBIGINT)")

    # Floats
    single["42_float"] = get_hash(conn, "hash(42::FLOAT)")
    single["42_double"] = get_hash(conn, "hash(42::DOUBLE)")
    single["0.0_float"] = get_hash(conn, "hash(0.0::FLOAT)")
    single["0.0_double"] = get_hash(conn, "hash(0.0::DOUBLE)")

    # Strings
    single["hello_varchar"] = get_hash(conn, "hash('hello'::VARCHAR)")
    single["empty_varchar"] = get_hash(conn, "hash(''::VARCHAR)")
    single["long_varchar"] = get_hash(conn, "hash('a longer string for testing'::VARCHAR)")

    # Blob
    single["dead_blob"] = get_hash(conn, r"hash('\xDEAD'::BLOB)")

    # Negative integers (critical: tests sign extension)
    single["neg1_tinyint"] = get_hash(conn, "hash((-1)::TINYINT)")
    single["neg1_smallint"] = get_hash(conn, "hash((-1)::SMALLINT)")
    single["neg1_integer"] = get_hash(conn, "hash((-1)::INTEGER)")
    single["neg1_bigint"] = get_hash(conn, "hash((-1)::BIGINT)")
    single["neg128_tinyint"] = get_hash(conn, "hash((-128)::TINYINT)")

    values["single"] = single

    # === Multi-value (CombineHash) ===
    multi = {}
    multi["42_hello"] = get_hash(conn, "hash(42, 'hello')")
    multi["1_2_3"] = get_hash(conn, "hash(1, 2, 3)")
    multi["int_double_varchar"] = get_hash(conn, "hash(42::INTEGER, 3.14::DOUBLE, 'test'::VARCHAR)")
    multi["42_test"] = get_hash(conn, "hash(42, 'test')")

    values["multi"] = multi

    # === Cartesian product: 4 types x 4 types ===
    types = [
        ("hi", "'hi'"),
        ("7", "7"),
        ("2.5d", "2.5::DOUBLE"),
        ("dead", r"'\xDEAD'::BLOB"),
    ]
    cartesian = {}
    for name_a, expr_a in types:
        for name_b, expr_b in types:
            key = f"{name_a}_x_{name_b}"
            cartesian[key] = get_hash(conn, f"hash({expr_a}, {expr_b})")

    values["cartesian"] = cartesian

    return values


if __name__ == "__main__":
    values = generate_values()
    json.dump(values, sys.stdout, indent=2)
    print()  # trailing newline
