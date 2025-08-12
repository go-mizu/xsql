# xsql - Type-Safe, Minimal SQL for Go

[![Go Reference](https://pkg.go.dev/badge/github.com/go-mizu/xsql.svg)](https://pkg.go.dev/github.com/go-mizu/xsql)
[![Go Report Card](https://goreportcard.com/badge/github.com/go-mizu/xsql)](https://goreportcard.com/report/github.com/go-mizu/xsql)
[![Tests](https://github.com/go-mizu/xsql/actions/workflows/test.yml/badge.svg)](https://github.com/go-mizu/xsql/actions/workflows/test.yml)
[![Coverage Status](https://coveralls.io/repos/github/go-mizu/xsql/badge.svg?branch=main)](https://coveralls.io/github/go-mizu/xsql?branch=main)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

xsql is a small, stdlib-style layer over `database/sql` that eliminates
repetitive row-mapping code without hiding SQL behind an ORM. You keep full
control of your queries while getting type-safe scanning into structs,
primitives, and custom types that implement `sql.Scanner`.

It’s designed for developers who value clarity, simplicity, and
performance,whether you’re working on a large production system or just learning
Go’s database API.

## Features

xsql is designed for Go developers who want to keep the simplicity of
`database/sql` while eliminating repetitive boilerplate. It integrates
seamlessly with your existing code without forcing an ORM or complex
abstractions.

Key capabilities include:

- Strongly typed query functions that map directly into structs, primitives, or
  custom scanner types without reflection-heavy frameworks.
- Zero configuration required; works with `*sql.DB`, `*sql.Tx`, and `*sql.Conn`.
- Automatic column-to-field mapping with `db` tags, falling back to
  case-insensitive field names.
- Safe handling of empty results for single-row queries, returning `nil` instead
  of panics.
- Built-in plan caching for performance, initialized lazily and safe for
  concurrent access.
- Fully compatible with native SQL syntax, no query builders or DSLs.
- Small, focused API: `Query`, `Get`, and `Exec` cover the majority of use
  cases.

## Why xsql

The standard `database/sql` package is intentionally low-level - it gives you
complete control, but it also means you spend a lot of time writing the same
patterns over and over: creating `rows`, looping, scanning values into
variables, handling conversion errors, and appending to slices.

That’s great for ultimate flexibility, but not great for everyday productivity.
Here’s the typical dance:

```go
rows, err := db.QueryContext(ctx, "SELECT id, email FROM users WHERE active = ?", 1)
if err != nil {
    return err
}
defer rows.Close()

var results []User
for rows.Next() {
    var u User
    if err := rows.Scan(&u.ID, &u.Email); err != nil {
        return err
    }
    results = append(results, u)
}

if err := rows.Err(); err != nil {
    return err
}
```

With xsql, the same thing becomes:

```go
users, err := xsql.Query[User](ctx, db, "SELECT id, email FROM users WHERE active = ?", 1)
```

That’s it. You still decide the SQL. You still decide how to structure your
queries and joins. xsql simply takes care of the mechanical parts - scanning,
type conversion, and slice building - while staying out of your way.

xsql is not an ORM, it doesn’t hide SQL, and it doesn’t try to reinvent how you
talk to databases. Instead, it gives you minimal, type-safe helpers so you can:

- Write SQL the way you like, with full control over queries and schema.
- Map results into your Go types automatically without losing performance.
- Reduce repetitive scanning code and keep your functions concise.
- Maintain compatibility with all database drivers supported by `database/sql`.
- Keep learning curves low for new developers while giving experts the control
  they expect.

The result is code that looks clean, compiles with type safety, and performs
just as well as hand-written scanning- while remaining transparent and
debuggable.

## Installation

xsql works with Go 1.20+ and any modern SQL driver that implements the
`database/sql/driver` interface. To install xsql, simply run:

```bash
go get github.com/go-mizu/xsql
```

You will also need a driver for your database. For example, you can use:

```bash
go get github.com/jackc/pgx/v5        # PostgreSQL  
go get github.com/go-sql-driver/mysql # MySQL  
go get modernc.org/sqlite             # SQLite (CGO-free)
```

In your code, import `xsql` alongside your chosen driver:

```go
import (
    "context"
    "database/sql"
    "log"

    "github.com/go-mizu/xsql"
    _ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver
)
```

## Quick Start

The following is a minimal example showing how to use xsql to query data into a
struct.

```go
type User struct {
    ID    int64  `db:"id"`
    Email string `db:"email"`
}
func main() {
    db, err := sql.Open("pgx", "postgres://user:pass@localhost/dbname")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    db.Exec(`CREATE TABLE users (id SERIAL PRIMARY KEY, email TEXT, active BOOLEAN)`)
    db.Exec(`INSERT INTO users (email, active) VALUES ('a@example.com', true), ('b@example.com', false)`)
    ctx := context.Background()
    users, err := xsql.Query[User](ctx, db, `SELECT id, email FROM users WHERE active = $1`, true)
    if err != nil {
        log.Fatal(err)
    }
    for _, u := range users {
        log.Println(u.ID, u.Email)
    }
}
```

You can also query directly into primitive slices when only one column is
needed:

```go
ids, err := xsql.Query[int64](ctx, db, `SELECT id FROM users WHERE active = $1`, true)
```

When you need just one record, `Get` returns a single value instead of a slice:

```go
u, err := xsql.Get[User](ctx, db, `SELECT id, email FROM users WHERE id = $1`, 1)
if err != nil {
    log.Fatal(err)
}
if u != nil {
    log.Println(u.ID, u.Email)
}
```

## Usage Guide

xsql keeps the surface small so you can learn it in minutes. There are three
core functions:

### Query

`Query[T any](ctx context.Context, db DB, query string, args ...any) ([]T, error)`

Runs a SQL query and maps all rows into a slice of type `T`. `T` can be a
struct, a primitive type, or a type implementing `sql.Scanner`. The `db`
argument can be a `*sql.DB`, `*sql.Tx`, or anything that implements
`QueryContext`.

```go
type Product struct {
ID    int64   `db:"id"`
Name  string  `db:"name"`
Price float64 `db:"price"`
}

ctx := context.Background()
products, err := xsql.Query[Product](ctx, db,
`SELECT id, name, price FROM products WHERE price > ?`, 10.0)
if err != nil {
log.Fatal(err)
}
fmt.Println(products)
```

### Get

`Get[T any](ctx context.Context, db DB, query string, args ...any) (T, error)`

Runs a SQL query and returns the first row mapped to type `T`. If no rows are
found, it returns `sql.ErrNoRows`. Perfect for single-value lookups or when you
expect at most one row.

```go
ctx := context.Background()
price, err := xsql.Get[float64](ctx, db,
`SELECT price FROM products WHERE id = ?`, 42)
if err != nil {
if errors.Is(err, sql.ErrNoRows) {
fmt.Println("No product found")
} else {
log.Fatal(err)
}
}
fmt.Println("Price:", price)
```

### Exec

`Exec(ctx context.Context, db DB, query string, args ...any) (sql.Result, error)`

Executes a SQL statement without returning rows, such as INSERT, UPDATE, or
DELETE. The `db` argument can be a `*sql.DB`, `*sql.Tx`, or anything that
implements `ExecContext`.

```go
ctx := context.Background()
res, err := xsql.Exec(ctx, db,
`UPDATE products SET price = price - 1.1 WHERE category_id = ?`, 5)
if err != nil {
log.Fatal(err)
}
affected, _ := res.RowsAffected()
fmt.Println("Updated rows:", affected)
```

## Advanced Usage

### Mapping to Custom Types

Any type implementing `sql.Scanner` can be directly used with xsql. This is
useful for enums, JSON fields, or other domain-specific types.

```go
type Email string

func (e *Email) Scan(src any) error {
    switch v := src.(type) {
        case []byte:
            *e = Email(string(v))
        case string:
            *e = Email(v)
        default:
            return fmt.Errorf("unexpected type %T", src)
    }
    return nil
}

ctx := context.Background()
emails, err := xsql.Query[Email](ctx, db, `SELECT email FROM users`)
if err != nil {
    log.Fatal(err)
}
fmt.Println(emails)
```

### Nested Structs and Inline Fields

xsql supports flattening nested structs when tagged with `db:",inline"`. This
makes it easy to combine related data into one Go value without manual joins in
your code.

```go
type Address struct {
    City   string `db:"city"`
    Street string `db:"street"`
}
type Customer struct {
    ID      int64   `db:"id"`
    Name    string  `db:"name"`
    Address `db:",inline"`
}

ctx := context.Background()
customers, err := xsql.Query[Customer](ctx, db,
    `SELECT id, name, city, street FROM customers`)
if err != nil {
    log.Fatal(err)
}
fmt.Println(customers)
```

### Transactions

xsql works seamlessly with transactions. Just pass a `*sql.Tx` in place of
`*sql.DB` for any function.

```go
ctx := context.Background()
tx, err := db.BeginTx(ctx, nil)
if err != nil {
    log.Fatal(err)
}
defer tx.Rollback()

_, err := xsql.Exec(ctx, tx, `INSERT INTO logs(message) VALUES (?)`, "started")
if err != nil {
    log.Fatal(err)
}

if err := tx.Commit(); err != nil {
    log.Fatal(err)
}
```

## How It Works
### Design Philosophy

xsql is not an ORM. It is a thin, type-safe wrapper around the `database/sql`
standard library, designed to make mapping query results into Go types
predictable and fast. Instead of generating code or maintaining complex
metadata, xsql uses reflection only once per unique type/column set and stores
the mapping in a cache. This keeps the runtime cost low without sacrificing
developer experience.

The library avoids hidden behaviors. Every query you run maps directly to the
SQL you write, so you remain in control of indexing, joins, and performance.
This approach encourages developers to think about database performance from the
start, while still enjoying concise and maintainable Go code.

We believe Go developers should not have to choose between bare `database/sql`
and heavy ORMs. xsql sits in the middle - minimal abstraction, but with enough
type awareness to eliminate repetitive boilerplate code.


### Mapping Strategy

When you call `Query` or `Get` with a type parameter `T`, xsql checks if a
column mapping for `T` already exists in its internal cache. If not, it inspects
the type using reflection, looks for `db` struct tags, and matches the columns
returned by the query to fields. Once computed, the mapping is stored in a
concurrency-safe cache keyed by the type and column list.

For primitive types or types implementing `sql.Scanner`, no field mapping is
required - values are scanned directly into the destination slice or variable.

### Execution Flow

1. **Prepare mapping**: For structs, find the field index for each column. For
   primitives, skip mapping entirely.
2. **Run query**: Execute using the `QueryContext` or `ExecContext` method of
   the provided `db`.
3. **Scan rows**: Use the mapping to scan each row directly into the appropriate
   fields or variables.
4. **Return typed result**: For `Query`, return a slice of `T`; for `Get`,
   return a single `T`.

### Performance Considerations

The first call for a given type/column set involves reflection and map
allocation. Subsequent calls reuse the computed mapping. This approach yields
ORM-like convenience without ORM-level overhead.

Custom `sql.Scanner` implementations receive raw database values, allowing
domain types to handle parsing themselves ( e.g., JSON fields, enum types, or
time formats).

## Contributing

We welcome contributions from both seasoned Go developers and those just
starting out. The goal of xsql is to remain minimal and type-safe while fitting
naturally into the Go ecosystem. If you have an idea, improvement, or bug fix,
here’s how you can help:

1. Fork the repository and create a feature branch.
2. Write clear, focused commits with descriptive messages.
3. Add or update tests to cover your changes.
4. Run the full test suite with `go test ./...` before submitting.
5. Open a pull request with a description of the changes and reasoning.

If you’re unsure about an idea, feel free to open an issue first to discuss it.
We’re happy to give feedback before you start coding.

## Running Tests

The test suite includes example-based documentation tests. This means many
examples from the README and doc comments are also run during testing, ensuring
that documentation and implementation stay in sync.

To run tests locally:

```bash
go test ./...
```

Some tests use SQLite in-memory mode (`modernc.org/sqlite`) for simplicity. No
additional database setup is required to run them.

## Project Structure

The repository is organized to keep the codebase simple and easy to navigate:

```
xsql/
  ├── doc.go         -  package overview and documentation
  ├── xsql.go        -  core interfaces and shared definitions
  ├── mapper.go      -  reflection-based mapping and caching
  ├── query.go       -  typed query helpers
  ├── get.go         -  single-row query helpers
  ├── exec.go        -  statement execution helper
  ├── *_test.go      -  example and unit tests
```

The package is designed for direct import without requiring code generation or
additional tooling.

## License

xsql is released under the MIT License. This means you can freely use, modify,
and distribute the library in your own projects, whether commercial or open
source, as long as the license terms are included.
