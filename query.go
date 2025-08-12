package xsql

import (
	"context"
)

// Query executes the SQL query and scans all result rows into a slice of T.
//
// T may be a struct (supports `db` tags and ,inline), a primitive, or any type
// implementing [sql.Scanner]. Column mapping prefers `db:"name"` tags;
// otherwise it matches case-insensitive field names.
//
// Extra columns are ignored and missing columns set zero values unless strict
// mode is enabled internally. Safe for concurrent use, Query internally uses a
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
//	users, err := xsql.Query[User](ctx, db, `SELECT id, email FROM users ORDER BY id`)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	for _, u := range users {
//	    fmt.Println(u.ID, u.Email)
//	}
func Query[T any](ctx context.Context, q Querier, query string, args ...any) (out []T, err error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	// Propagate rows.Close() error if nothing else failed.
	defer func() {
		if cerr := rows.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	m := getMapper() // lazy, thread-safe
	for rows.Next() {
		v, scanErr := scanWithMapper[T](m, rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, v)
	}
	if ne := rows.Err(); ne != nil {
		return nil, ne
	}
	return out, nil
}
