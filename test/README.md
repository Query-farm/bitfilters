# Testing this extension
This directory contains all the tests for this extension. The `sql` directory holds tests that are written as [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html). DuckDB aims to have most its tests in this format as SQL statements, so for the bitfilters extension, this should probably be the goal too.

The root makefile contains targets to build and run all of these tests. To run the SQLLogicTests:
```bash
make test
```
or
```bash
make test_debug
```

`sql/duplicate_sampling.test` checks that periodic keys and consecutive runs
of duplicates produce filters as compact as explicit `DISTINCT`, for both
fingerprint widths of XOR and Binary Fuse. It also checks membership, unique
inputs around the sampling threshold, and repeated window finalization.

Duplicate detection hashes sample positions, sorts and deduplicates those
positions, then checks the sampled keys for repetition. The sample uses a
bounded 32 KiB buffer. Sampling remains a heuristic; detected duplicates use
the existing full-input sort/unique path, which can cost build time and
workspace in exchange for a smaller stored filter. Input collection and BLOB
formats are unchanged.
