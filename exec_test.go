package xsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
)

// --- Minimal exec-only in-test driver ---------------------------------------

type execHandler func(query string, args []driver.NamedValue) (driver.Result, error)

type execConnector struct{ h execHandler }

func (c *execConnector) Connect(context.Context) (driver.Conn, error) { return &execConn{h: c.h}, nil }
func (c *execConnector) Driver() driver.Driver                        { return execDriver{} }

type execDriver struct{}

func (execDriver) Open(name string) (driver.Conn, error) {
	return nil, errors.New("execDriver.Open should not be called; use sql.OpenDB with connector")
}

type execConn struct{ h execHandler }

func (c *execConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (c *execConn) Close() error                        { return nil }
func (c *execConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }

// Support ExecContext so *sql.DB.ExecContext hits this path.
func (c *execConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.h(query, args)
}

// Result implementation for tests.
type testResult struct {
	lastID int64
	rows   int64
	liErr  error
	raErr  error
}

func (r testResult) LastInsertId() (int64, error) { return r.lastID, r.liErr }
func (r testResult) RowsAffected() (int64, error) { return r.rows, r.raErr }

func newExecDB(t *testing.T, h execHandler) *sql.DB {
	t.Helper()
	return sql.OpenDB(&execConnector{h: h})
}

// --- Tests -------------------------------------------------------------------

func TestExec_RowsAffected(t *testing.T) {
	db := newExecDB(t, func(query string, args []driver.NamedValue) (driver.Result, error) {
		if query != `UPDATE users SET email = ? WHERE id > ?` {
			t.Fatalf("unexpected query: %q", query)
		}
		// ints are normalized to int64 by database/sql
		if len(args) != 2 || args[0].Value != "x@ex.com" || args[1].Value != int64(10) {
			t.Fatalf("unexpected args: %#v", args)
		}
		return testResult{rows: 3}, nil
	})
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	res, err := Exec(ctx, db, `UPDATE users SET email = ? WHERE id > ?`, "x@ex.com", 10)
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected err: %v", err)
	}
	if n != 3 {
		t.Fatalf("RowsAffected=%d want 3", n)
	}
}

func TestExec_LastInsertID(t *testing.T) {
	db := newExecDB(t, func(query string, args []driver.NamedValue) (driver.Result, error) {
		if query != `INSERT INTO users (email) VALUES (?)` {
			t.Fatalf("unexpected query: %q", query)
		}
		if len(args) != 1 || args[0].Value != "ada@lovelace.dev" {
			t.Fatalf("unexpected args: %#v", args)
		}
		return testResult{lastID: 99, rows: 1}, nil
	})
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	res, err := Exec(ctx, db, `INSERT INTO users (email) VALUES (?)`, "ada@lovelace.dev")
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("LastInsertId err: %v", err)
	}
	if id != 99 {
		t.Fatalf("LastInsertId=%d want 99", id)
	}
}

func TestExec_Error(t *testing.T) {
	sentinel := errors.New("boom")
	db := newExecDB(t, func(query string, args []driver.NamedValue) (driver.Result, error) {
		return nil, sentinel
	})
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	_, err := Exec(ctx, db, `DELETE FROM users WHERE id = ?`, 7)
	if err == nil {
		t.Fatalf("expected error")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("want %v, got %v", sentinel, err)
	}
}
