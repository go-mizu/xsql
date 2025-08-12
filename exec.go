package xsql

import (
	"context"
	"database/sql"
)

// Exec executes a statement that does not return rows (INSERT, UPDATE, DELETE, DDL).
//
// It forwards to the underlying [Execer]. On success it returns the driver's
// [sql.Result], which may support LastInsertId and RowsAffected depending on
// the database/driver.
//
// Exec does not attempt SQL rendering or placeholder rewriting; write your SQL
// exactly as your driver expects.
//
// Example:
//
//	// Given a *sql.DB (or *sql.Tx, *sql.Conn) in variable `db`:
//	ctx := context.Background()
//	res, err := xsql.Exec(ctx, db, `INSERT INTO users (email) VALUES (?)`, "a@example.com")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	n, _ := res.RowsAffected()
//	fmt.Println("rows:", n)
//
// Notes:
//   - Use a transaction (BeginTx) around multiple Exec/Query calls when you need atomicity.
//   - Not all drivers support LastInsertId; prefer RETURNING with Query/Get where available.
func Exec(ctx context.Context, e Execer, query string, args ...any) (sql.Result, error) {
	return e.ExecContext(ctx, query, args...)
}
