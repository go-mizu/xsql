package xsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
)

func TestQuery_SuccessStruct_MultiRows(t *testing.T) {
	type Row struct {
		ID   int64  `db:"id"`
		Name string `db:"name"`
	}
	db := newTestDB(t, func(q string, _ []driver.NamedValue) ([]string, [][]driver.Value, error) {
		cols := []string{`"ID"`, "`NAME`"}
		rows := [][]driver.Value{
			{int64(1), []byte("alice")},
			{int64(2), []byte("bob")},
		}
		return cols, rows, nil
	})
	defer func() { _ = db.Close() }()

	got, err := Query[Row](context.Background(), db, "ok")
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}
	if len(got) != 2 || got[0].ID != 1 || got[0].Name != "alice" || got[1].ID != 2 || got[1].Name != "bob" {
		t.Fatalf("unexpected rows: %+v", got)
	}
}

func TestQuery_Primitive_MultiRows(t *testing.T) {
	db := newTestDB(t, func(q string, _ []driver.NamedValue) ([]string, [][]driver.Value, error) {
		return []string{"n"}, [][]driver.Value{{int64(10)}, {int64(20)}, {int64(30)}}, nil
	})
	defer func() { _ = db.Close() }()

	got, err := Query[int64](context.Background(), db, "nums")
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}
	want := []int64{10, 20, 30}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("unexpected slice: %v", got)
	}
}

func TestQuery_Empty_NoError(t *testing.T) {
	db := newTestDB(t, func(q string, _ []driver.NamedValue) ([]string, [][]driver.Value, error) {
		return []string{"id"}, [][]driver.Value{}, nil
	})
	defer func() { _ = db.Close() }()

	got, err := Query[int64](context.Background(), db, "empty")
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty slice, got %v", got)
	}
}

func TestQuery_QueryError(t *testing.T) {
	wantErr := errors.New("boom")
	db := newTestDB(t, func(q string, _ []driver.NamedValue) ([]string, [][]driver.Value, error) {
		return nil, nil, wantErr
	})
	defer func() { _ = db.Close() }()

	_, err := Query[int64](context.Background(), db, "fail")
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected %v, got %v", wantErr, err)
	}
}

func TestQuery_NextError_SurfacedViaRowsErr(t *testing.T) {
	// Use the special connector that always returns a rows.Next() error.
	db := sql.OpenDB(&errNextConnector{})
	defer func() { _ = db.Close() }()

	_, err := Query[struct {
		A int `db:"a"`
	}](context.Background(), db, "ignored")
	if err == nil || err.Error() != "driver next error" {
		t.Fatalf("expected driver next error, got %v", err)
	}
}

func TestQuery_ScanError_PrimitiveTooManyColumns(t *testing.T) {
	// Two columns into primitive T should cause scanWithMapper to fail on the first row.
	db := newTestDB(t, func(q string, _ []driver.NamedValue) ([]string, [][]driver.Value, error) {
		return []string{"a", "b"}, [][]driver.Value{{1, 2}}, nil
	})
	defer func() { _ = db.Close() }()

	_, err := Query[int64](context.Background(), db, "multi")
	if err == nil {
		t.Fatal("expected error for multiple columns into primitive")
	}
}

/*** Additional cases to hit uncovered branches in mapper:
  - makeFieldStep: pickIndirect -> stepIndirect (custom named string type)
  - makeFieldStep: fallback stepDirect for non-scannable/non-indirect (interface{})
  - makeWholeStep: pickIndirect for primitive (int32 <- int64) to hit non-struct primitive stepIndirect in destPtrs
***/

func TestQuery_Field_Indirect_CustomNamedString(t *testing.T) {
	type MyStr string
	type Row struct {
		Val MyStr `db:"val"` // named string type -> pickIndirect (tmp string) -> stepIndirect
	}
	db := newTestDB(t, func(q string, _ []driver.NamedValue) ([]string, [][]driver.Value, error) {
		return []string{"val"}, [][]driver.Value{{"hello"}}, nil
	})
	defer func() { _ = db.Close() }()

	got, err := Query[Row](context.Background(), db, "q")
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}
	if len(got) != 1 || string(got[0].Val) != "hello" {
		t.Fatalf("unexpected: %+v", got)
	}
}

func TestQuery_Field_FallbackStepDirect_Interface(t *testing.T) {
	type Row struct {
		Any any `db:"v"` // interface{}: not directly scannable, not indirect -> fallback stepDirect
	}
	db := newTestDB(t, func(q string, _ []driver.NamedValue) ([]string, [][]driver.Value, error) {
		return []string{"v"}, [][]driver.Value{{int64(42)}}, nil
	})
	defer func() { _ = db.Close() }()

	got, err := Query[Row](context.Background(), db, "q")
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}
	// database/sql will assign driver value into interface{}
	if len(got) != 1 {
		t.Fatalf("len: %d", len(got))
	}
	if v, ok := got[0].Any.(int64); !ok || v != 42 {
		t.Fatalf("want interface holding int64(42), got %#v", got[0].Any)
	}
}

func TestQuery_Primitive_Indirect_Int32FromInt64(t *testing.T) {
	// Non-struct primitive indirect:
	// T=int32, driver returns int64 -> makeWholeStep indirect, destPtrs non-struct primitive stepIndirect branch.
	db := newTestDB(t, func(q string, _ []driver.NamedValue) ([]string, [][]driver.Value, error) {
		return []string{"n"}, [][]driver.Value{{int64(7)}, {int64(8)}}, nil
	})
	defer func() { _ = db.Close() }()

	got, err := Query[int32](context.Background(), db, "q")
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}
	if len(got) != 2 || got[0] != 7 || got[1] != 8 {
		t.Fatalf("unexpected: %v", got)
	}
}
