/*
Package xsql is a minimal, stdlib-style layer over database/sql that provides
type-safe scanning into structs, primitives, and sql.Scanner types. You write
plain SQL; xsql maps results into Go values with a tiny, predictable API.

# Overview

xsql preserves database/sql semantics while removing repetitive row-mapping code.
It works with *sql.DB, *sql.Tx, and *sql.Conn. Mapping is deterministic, fast,
and easy to reason about in code review.

# Mapping rules

  - Fields bind by `db:"name"` first; otherwise case-insensitive field ←→ column name.
  - Nested structs can be flattened with `db:",inline"`.
  - If a destination type (or field) implements sql.Scanner, its Scan method receives the driver value.
  - Primitives (bool, numbers, string, []byte, time.Time, sql.RawBytes) are supported directly.
  - Extra columns are ignored; missing columns yield zero values (favors robustness).

# Performance

On first use of a (Type, ColumnSet) pair, xsql builds a scan plan (column → field
index path and destination strategy). Plans and per-type indexes are cached in a
lazily-initialized, concurrency-safe map (sync.Map). Subsequent scans reuse the
plan and avoid reflection on the hot path. Common safe conversions (e.g., []byte→string,
numeric widenings) are handled inline.

# Error handling

  - Get returns sql.ErrNoRows when no row matches.
  - Query and Exec propagate underlying driver errors.
  - Iterator / protocol issues surface via rows.Err() at the end of Query.

# Compatibility

xsql works with any database/sql driver (PostgreSQL, MySQL, SQLite, SQL Server, Oracle).
It does not rewrite SQL or placeholders; write queries exactly as your driver expects.

# Usage notes

Prefer explicit column lists over SELECT * to keep mapping stable. Add LIMIT 1
(or the equivalent) when you expect a single row. Use contexts to bound query
timeouts. Keep Go types close to database types to minimize surprises. For
large reads, consider streaming with Query and processing incrementally if memory
usage matters.

xsql is intended for production systems that value clarity and performance over
abstraction. It keeps the API small and predictable while giving you full control
over your SQL.
*/
package xsql
