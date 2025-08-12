package xsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"testing"
)

/* -------------------------------------------------------
   Special connector for rows.Next error simulation
--------------------------------------------------------*/

type errNextConnector struct{}

func (c *errNextConnector) Connect(context.Context) (driver.Conn, error) { return &errNextConn{}, nil }
func (c *errNextConnector) Driver() driver.Driver                        { return testDriver{} }

type errNextConn struct{}

func (c *errNextConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (c *errNextConn) Close() error                        { return nil }
func (c *errNextConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }
func (c *errNextConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &errRows{}, nil
}

// errRows fails on first Next(); database/sql exposes it via rows.Err() after Next() returns false.
type errRows struct{}

func (e *errRows) Columns() []string { return []string{"a"} }
func (e *errRows) Close() error      { return nil }
func (e *errRows) Next(dest []driver.Value) error {
	return errors.New("driver next error")
}

/* -------------------------------------------------------
   Tests covering all get.go branches
--------------------------------------------------------*/

func TestGet_SuccessStruct(t *testing.T) {
	type Row struct {
		ID   int64  `db:"id"`
		Name string `db:"name"`
	}
	db := newTestDB(t, func(q string, _ []driver.NamedValue) ([]string, [][]driver.Value, error) {
		cols := []string{`"ID"`, "`NAME`"}
		rows := [][]driver.Value{{int64(7), []byte("alice")}}
		return cols, rows, nil
	})
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	got, err := Get[Row](ctx, db, "ok")
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if got.ID != 7 || got.Name != "alice" {
		t.Fatalf("unexpected row: %+v", got)
	}
}

func TestGet_QueryError(t *testing.T) {
	wantErr := errors.New("boom")
	db := newTestDB(t, func(q string, _ []driver.NamedValue) ([]string, [][]driver.Value, error) {
		return nil, nil, wantErr
	})
	defer func() { _ = db.Close() }()

	_, err := Get[int64](context.Background(), db, "any")
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected %v, got %v", wantErr, err)
	}
}

func TestGet_NoRows_ReturnsErrNoRows(t *testing.T) {
	db := newTestDB(t, func(q string, _ []driver.NamedValue) ([]string, [][]driver.Value, error) {
		// No rows; columns present
		return []string{"id"}, [][]driver.Value{}, nil
	})
	defer func() { _ = db.Close() }()

	_, err := Get[int64](context.Background(), db, "empty")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestGet_NextError_SurfacedViaRowsErr(t *testing.T) {
	// Use dedicated connector that always returns errRows; handler is never called.
	db := sql.OpenDB(&errNextConnector{})
	defer func() { _ = db.Close() }()

	_, err := Get[struct {
		A int `db:"a"`
	}](context.Background(), db, "ignored")
	if err == nil || err.Error() != "driver next error" {
		t.Fatalf("expected driver next error, got %v", err)
	}
}

func TestGet_ScanError_PrimitiveTooManyColumns(t *testing.T) {
	// Two columns into primitive T should cause scanWithMapper to fail.
	db := newTestDB(t, func(q string, _ []driver.NamedValue) ([]string, [][]driver.Value, error) {
		return []string{"a", "b"}, [][]driver.Value{{1, 2}}, nil
	})
	defer func() { _ = db.Close() }()

	_, err := Get[int64](context.Background(), db, "multi")
	if err == nil {
		t.Fatal("expected error for multiple columns into primitive")
	}
}

func TestGet_UsesLazyMapperSingleton(t *testing.T) {
	before := getMapper()
	db := newTestDB(t, func(q string, _ []driver.NamedValue) ([]string, [][]driver.Value, error) {
		return []string{"n"}, [][]driver.Value{{int64(1)}}, nil
	})
	defer func() { _ = db.Close() }()

	_, err := Get[int64](context.Background(), db, "one")
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	after := getMapper()
	if !reflect.ValueOf(before).IsValid() || after == nil || before != after {
		t.Fatal("lazy mapper singleton not stable across Get")
	}
}
