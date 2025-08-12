package xsql

import (
	"context"
	"database/sql"
)

// Get executes the SQL query and scans the first row into a value of type T.
//
// It returns [sql.ErrNoRows] if the query yields no rows and does not enforce
// "exactly one row" beyond the first; if more rows exist, they are ignored.
// You should use LIMIT 1 (or an equivalent WHERE clause) when you require
// at-most-one row.
//
// T may be a struct (supports `db` tags and ,inline), a primitive, or any type
// implementing [sql.Scanner]. Column mapping prefers `db:"name"` tags;
// otherwise it matches case-insensitive field names.
//
// Extra columns are ignored and missing columns set zero values unless strict
// mode is enabled internally. Safe for concurrent use, Get internally uses a
// lazily-initialized, concurrency-safe plan cache based on [sync.Map], which
// avoids global locks for most read operations.
//
// Example:
//
//	// Given a *sql.DB (or *sql.Tx, *sql.Conn) in variable `db`:
//	type User struct {
//	    ID    int64  `db:"id"`
//	    Email string `db:"email"`
//	}
//
//	ctx := context.Background()
//	u, err := xsql.Get[User](ctx, db, `SELECT id, email FROM users WHERE id = $1`, 42)
//	if err != nil {
//	    if errors.Is(err, sql.ErrNoRows) {
//	        // handle not found
//	    } else {
//	        // handle other errors
//	    }
//	}
//	// use u
func Get[T any](ctx context.Context, q Querier, query string, args ...any) (out T, err error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return out, err
	}
	// Ensure Close error is propagated if no earlier error occurred.
	defer func() {
		if cerr := rows.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	if !rows.Next() {
		if ne := rows.Err(); ne != nil {
			return out, ne
		}
		return out, sql.ErrNoRows
	}

	m := getMapper() // lazy, thread-safe
	v, scanErr := scanWithMapper[T](m, rows)
	if scanErr != nil {
		return out, scanErr
	}
	return v, nil
}
