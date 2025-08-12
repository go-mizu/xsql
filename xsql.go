package xsql

import (
	"context"
	"database/sql"
)

// Querier is implemented by *sql.DB, *sql.Tx, *sql.Conn, and any wrapper
// that can execute a query returning rows.
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// Execer is implemented by *sql.DB, *sql.Tx, *sql.Conn, and any wrapper
// that can execute a statement that does not return rows.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Beginner is implemented by *sql.DB and *sql.Conn. It starts a transaction.
type Beginner interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}
