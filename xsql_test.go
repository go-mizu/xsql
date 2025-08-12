package xsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"testing"
)

type DBHandler func(query string, args []driver.NamedValue) (cols []string, rows [][]driver.Value, err error)

type testConnector struct {
	h DBHandler
}

func (c *testConnector) Connect(context.Context) (driver.Conn, error) { return &testConn{h: c.h}, nil }
func (c *testConnector) Driver() driver.Driver                        { return testDriver{} }

type testDriver struct{}

func (testDriver) Open(name string) (driver.Conn, error) {
	return nil, errors.New("testDriver.Open should not be called; use sql.OpenDB with connector")
}

type testConn struct {
	h DBHandler
}

func (c *testConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (c *testConn) Close() error                        { return nil }
func (c *testConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }

func (c *testConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	cols, data, err := c.h(query, args)
	if err != nil {
		return nil, err
	}
	return &testRows{cols: cols, data: data}, nil
}

type testRows struct {
	cols []string
	data [][]driver.Value
	i    int
}

func (r *testRows) Columns() []string { return append([]string(nil), r.cols...) }
func (r *testRows) Close() error      { return nil }
func (r *testRows) Next(dest []driver.Value) error {
	if r.i >= len(r.data) {
		return io.EOF
	}
	row := r.data[r.i]
	for i := range dest {
		if i < len(row) {
			dest[i] = row[i]
		} else {
			dest[i] = nil
		}
	}
	r.i++
	return nil
}

// newTestDB creates a *sql.DB backed by the in-memory test driver.
func newTestDB(t *testing.T, h DBHandler) *sql.DB {
	t.Helper()
	return sql.OpenDB(&testConnector{h: h})
}
