package xsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"reflect"
	"testing"
	"time"
)

func nextAndScan[T any](t *testing.T, m *Mapper, rows *sql.Rows) T {
	t.Helper()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			t.Fatalf("next err: %v", err)
		}
		t.Fatal("no row")
	}
	v, err := scanWithMapper[T](m, rows)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	return v
}

/* ---------------------------
   Tests: string/lower/quotes
----------------------------*/

func TestNormalizeAndLower(t *testing.T) {
	cases := map[string]string{
		`"Name"`:        "name",
		"`Camel`":       "camel",
		"[UPPER]":       "upper",
		"already_ok":    "already_ok",
		"MiXeD_123":     "mixed_123",
		`"miX"`:         "mix",
		"`x`":           "x",
		"[y]":           "y",
		`"unterminated`: `"unterminated`, // not trimmed; just lower
	}
	for in, want := range cases {
		got := normalizeColAscii(in)
		if got != want {
			t.Fatalf("normalize %q got %q want %q", in, got, want)
		}
	}
	if toLowerAscii("lower") != "lower" {
		t.Fatal("toLowerAscii changed already-lower")
	}
}

func TestParseTag(t *testing.T) {
	tests := []struct {
		tag    string
		name   string
		inline bool
		omit   bool
	}{
		{"", "", false, false},
		{"-", "", false, true},
		{"col", "col", false, false},
		{",inline", "", true, false},
		{"col,inline", "col", true, false},
		{"inline,col", "col", true, false},
	}
	for _, tc := range tests {
		name, inline, omit := parseTag(tc.tag)
		if name != tc.name || inline != tc.inline || omit != tc.omit {
			t.Fatalf("parseTag %q = (%q,%v,%v), want (%q,%v,%v)",
				tc.tag, name, inline, omit, tc.name, tc.inline, tc.omit)
		}
	}
}

/* ---------------------------
   Struct index & cache
----------------------------*/

func TestBuildStructIndex_InlineAndAnonymous(t *testing.T) {
	type Embedded struct {
		Inner string `db:"inner"`
	}
	type Outer struct {
		ID       int    `db:"id"`
		Embedded        // anonymous → treated as inline
		Skip     string `db:"-"`
		unexp    int    // unexported non-anonymous → ignored
	}

	// Touch the unexported field so linters consider it used.
	_ = Outer{unexp: 1}

	fi := buildStructIndex(reflect.TypeOf(Outer{}))
	if _, ok := fi.byName["id"]; !ok {
		t.Fatal("id missing")
	}
	if _, ok := fi.byName["inner"]; !ok {
		t.Fatal("inner missing (inline/embedded)")
	}
	if _, ok := fi.byName["skip"]; ok {
		t.Fatal("skip should be omitted")
	}
	if _, ok := fi.byName["unexp"]; ok {
		t.Fatal("unexported non-anonymous should be ignored")
	}
}

func TestStructIndexCacheAndPlanCacheReuse(t *testing.T) {
	type S struct {
		A int `db:"a"`
	}
	m := NewMapper()

	rt := reflect.TypeOf(S{})
	fi1 := m.structIndex(rt)
	fi2 := m.structIndex(rt)
	if fi1 != fi2 {
		t.Fatal("structIndexCache not reused")
	}

	cols := []string{"a"}
	h := fnv.New64a()
	for _, c := range cols {
		_, _ = h.Write([]byte(c))
		_, _ = h.Write([]byte{0})
	}
	p1, err := m.getPlan(rt, cols, h.Sum64())
	if err != nil {
		t.Fatal(err)
	}
	p2, err := m.getPlan(rt, cols, h.Sum64())
	if err != nil {
		t.Fatal(err)
	}
	if p1 != p2 {
		t.Fatal("planCache not reused")
	}
}

/* ---------------------------
   isStruct / deref / Scanner / direct
----------------------------*/

type scanString string

func (s *scanString) Scan(src any) error {
	switch v := src.(type) {
	case []byte:
		*s = scanString(string(v))
		return nil
	case string:
		*s = scanString(v)
		return nil
	default:
		return fmt.Errorf("bad %T", src)
	}
}

func TestTypeHelpers(t *testing.T) {
	type S struct{ A int }
	if !isStruct(reflect.TypeOf(S{})) {
		t.Fatal("isStruct false")
	}
	if derefPtr(reflect.TypeOf(&S{})) != reflect.TypeOf(S{}) {
		t.Fatal("derefPtr wrong")
	}
	if !implementsScanner(reflect.TypeOf(scanString(""))) {
		t.Fatal("implementsScanner false")
	}
	if !isDirectlyScannable(reflect.TypeOf(time.Now())) {
		t.Fatal("time.Time should be scannable")
	}
	if !isDirectlyScannable(reflect.TypeOf(sql.RawBytes{})) {
		t.Fatal("sql.RawBytes should be scannable")
	}
}

/* ---------------------------
   pickIndirect conversions
----------------------------*/

func TestPickIndirect_AllBranches(t *testing.T) {
	// []byte -> string (builtin string only)
	conv, post, ok := pickIndirect(reflect.TypeOf(""))
	if !ok || conv.Kind() != reflect.Slice || conv.Elem().Kind() != reflect.Uint8 {
		t.Fatal("[]byte->string not selected")
	}
	dst := reflect.New(reflect.TypeOf("")).Elem()
	src := reflect.New(conv).Elem()
	src.SetBytes([]byte("hi"))
	if err := post(dst, src); err != nil || dst.String() != "hi" {
		t.Fatalf("post string got %q err %v", dst.String(), err)
	}

	// int <- int64 (builtin numeric)
	conv, post, ok = pickIndirect(reflect.TypeOf(int32(0)))
	if !ok || conv.Kind() != reflect.Int64 {
		t.Fatal("int32<-int64 not selected")
	}
	dsti := reflect.New(reflect.TypeOf(int32(0))).Elem()
	srci := reflect.New(conv).Elem()
	srci.SetInt(7)
	if err := post(dsti, srci); err != nil || dsti.Int() != 7 {
		t.Fatalf("post int32 got %d err %v", dsti.Int(), err)
	}

	// uint <- uint64 (builtin numeric)
	conv, post, ok = pickIndirect(reflect.TypeOf(uint8(0)))
	if !ok || conv.Kind() != reflect.Uint64 {
		t.Fatal("uint8<-uint64 not selected")
	}
	dstu := reflect.New(reflect.TypeOf(uint8(0))).Elem()
	srcu := reflect.New(conv).Elem()
	srcu.SetUint(9)
	if err := post(dstu, srcu); err != nil || dstu.Uint() != 9 {
		t.Fatalf("post uint8 got %d err %v", dstu.Uint(), err)
	}

	// float <- float64 (builtin numeric)
	conv, post, ok = pickIndirect(reflect.TypeOf(float32(0)))
	if !ok || conv.Kind() != reflect.Float64 {
		t.Fatal("float32<-float64 not selected")
	}
	dstf := reflect.New(reflect.TypeOf(float32(0))).Elem()
	srcf := reflect.New(conv).Elem()
	srcf.SetFloat(1.25)
	if err := post(dstf, srcf); err != nil || dstf.Float() != 1.25 {
		t.Fatalf("post float32 got %f err %v", dstf.Float(), err)
	}

	// custom named underlying string -> tmp string
	type MyStr string
	conv, post, ok = pickIndirect(reflect.TypeOf(MyStr("")))
	if !ok || conv.Kind() != reflect.String {
		t.Fatal("custom string type not selected")
	}
	dstc := reflect.New(reflect.TypeOf(MyStr(""))).Elem()
	srcc := reflect.New(conv).Elem()
	srcc.SetString("x")
	if err := post(dstc, srcc); err != nil || dstc.Convert(reflect.TypeOf("")).String() != "x" {
		t.Fatalf("post custom string got %v err %v", dstc.Interface(), err)
	}

	// custom named underlying int -> tmp int64
	type MyInt int32
	conv, post, ok = pickIndirect(reflect.TypeOf(MyInt(0)))
	if !ok || conv.Kind() != reflect.Int64 {
		t.Fatal("custom int underlying not selected")
	}
	dstmi := reflect.New(reflect.TypeOf(MyInt(0))).Elem()
	srcmi := reflect.New(conv).Elem()
	srcmi.SetInt(123)
	if err := post(dstmi, srcmi); err != nil {
		t.Fatalf("post custom int err %v", err)
	}
	if got := dstmi.Convert(reflect.TypeOf(int64(0))).Int(); got != 123 {
		t.Fatalf("post custom int got %d", got)
	}

	// custom named underlying uint -> tmp uint64
	type MyUint uint16
	conv, post, ok = pickIndirect(reflect.TypeOf(MyUint(0)))
	if !ok || conv.Kind() != reflect.Uint64 {
		t.Fatal("custom uint underlying not selected")
	}
	dstmu := reflect.New(reflect.TypeOf(MyUint(0))).Elem()
	srcmu := reflect.New(conv).Elem()
	srcmu.SetUint(77)
	if err := post(dstmu, srcmu); err != nil {
		t.Fatalf("post custom uint err %v", err)
	}
	if got := dstmu.Convert(reflect.TypeOf(uint64(0))).Uint(); got != 77 {
		t.Fatalf("post custom uint got %d", got)
	}

	// custom named underlying float -> tmp float64
	type MyFloat float32
	conv, post, ok = pickIndirect(reflect.TypeOf(MyFloat(0)))
	if !ok || conv.Kind() != reflect.Float64 {
		t.Fatal("custom float underlying not selected")
	}
	dstmf := reflect.New(reflect.TypeOf(MyFloat(0))).Elem()
	srcmf := reflect.New(conv).Elem()
	srcmf.SetFloat(3.5)
	if err := post(dstmf, srcmf); err != nil {
		t.Fatalf("post custom float err %v", err)
	}
	if got := dstmf.Convert(reflect.TypeOf(float64(0))).Float(); got != 3.5 {
		t.Fatalf("post custom float got %v", got)
	}

	// ---- Named pointer to primitive: exercises pointer unwrapping + re-application ----

	// *int32
	type MyPtrInt *int32
	conv, post, ok = pickIndirect(reflect.TypeOf(MyPtrInt(nil)))
	if !ok || conv.Kind() != reflect.Int64 {
		t.Fatal("named *int32 not selected with tmp int64")
	}
	dstp := reflect.New(reflect.TypeOf(MyPtrInt(nil))).Elem() // dst of type MyPtrInt
	srcp := reflect.New(conv).Elem()
	srcp.SetInt(456)
	if err := post(dstp, srcp); err != nil {
		t.Fatalf("post named *int32 err %v", err)
	}
	if dstp.IsNil() || dstp.Elem().Int() != 456 {
		t.Fatalf("named *int32 assign failed: %+v", dstp.Interface())
	}

	// *uint16
	type MyPtrUint *uint16
	conv, post, ok = pickIndirect(reflect.TypeOf(MyPtrUint(nil)))
	if !ok || conv.Kind() != reflect.Uint64 {
		t.Fatal("named *uint16 not selected with tmp uint64")
	}
	dstpu := reflect.New(reflect.TypeOf(MyPtrUint(nil))).Elem()
	srcpu := reflect.New(conv).Elem()
	srcpu.SetUint(88)
	if err := post(dstpu, srcpu); err != nil {
		t.Fatalf("post named *uint16 err %v", err)
	}
	if dstpu.IsNil() || dstpu.Elem().Uint() != 88 {
		t.Fatalf("named *uint16 assign failed: %+v", dstpu.Interface())
	}

	// *float32
	type MyPtrFloat *float32
	conv, post, ok = pickIndirect(reflect.TypeOf(MyPtrFloat(nil)))
	if !ok || conv.Kind() != reflect.Float64 {
		t.Fatal("named *float32 not selected with tmp float64")
	}
	dstpf := reflect.New(reflect.TypeOf(MyPtrFloat(nil))).Elem()
	srcpf := reflect.New(conv).Elem()
	srcpf.SetFloat(6.25)
	if err := post(dstpf, srcpf); err != nil {
		t.Fatalf("post named *float32 err %v", err)
	}
	if dstpf.IsNil() || dstpf.Elem().Float() != 6.25 {
		t.Fatalf("named *float32 assign failed: %+v", dstpf.Interface())
	}

	// *string (named pointer to primitive string)
	type MyPtrStr *string
	conv, post, ok = pickIndirect(reflect.TypeOf(MyPtrStr(nil)))
	if !ok || conv.Kind() != reflect.String {
		t.Fatal("named *string not selected with tmp string")
	}
	dstps := reflect.New(reflect.TypeOf(MyPtrStr(nil))).Elem()
	srcps := reflect.New(conv).Elem()
	srcps.SetString("ptr-s")
	if err := post(dstps, srcps); err != nil {
		t.Fatalf("post named *string err %v", err)
	}
	if dstps.IsNil() || dstps.Elem().String() != "ptr-s" {
		t.Fatalf("named *string assign failed: %+v", dstps.Interface())
	}
}

/* ---------------------------
   Step construction helpers
----------------------------*/

func TestFieldTypeAndPathAlloc(t *testing.T) {
	type Inner struct{ P *int }
	type Outer struct{ I *Inner }
	rt := reflect.TypeOf(Outer{})
	ft := fieldTypeByPath(rt, []int{0})
	if ft.Kind() != reflect.Ptr {
		t.Fatal("fieldTypeByPath failed")
	}

	rv := reflect.New(rt).Elem()
	dst := fieldByPathAlloc(rv, []int{0, 0}) // Outer.I.P
	if dst.Kind() != reflect.Ptr || dst.IsNil() {
		t.Fatal("fieldByPathAlloc did not allocate nested pointers")
	}
}

/* ---------------------------
   End-to-end scans
----------------------------*/

func TestScan_Struct_DirectAndIndirectAndDrop(t *testing.T) {
	type Row struct {
		Name  string       `db:"name"` // []byte -> string
		Age   int32        `db:"age"`  // int64 -> int32 (indirect)
		Ok    bool         `db:"ok"`
		TS    time.Time    `db:"ts"`
		Raw   sql.RawBytes `db:"raw"`
		Email scanString   `db:"email"` // Scanner field
	}

	now := time.Unix(1700000000, 0).UTC()
	cols := []string{`"Name"`, "`Age`", "[OK]", "TS", "RAW", "EMAIL", "UNMAPPED"}
	vals := [][]driver.Value{
		{[]byte("bob"), int64(33), true, now, []byte("xyz"), []byte("bob@x"), "ignored"},
	}

	db := newTestDB(t, func(q string, _ []driver.NamedValue) ([]string, [][]driver.Value, error) {
		return cols, vals, nil
	})
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(context.Background(), "q")
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	m := NewMapper()
	got := nextAndScan[Row](t, m, rows)

	if got.Name != "bob" || got.Age != 33 || !got.Ok || !got.TS.Equal(now) || string(got.Raw) != "xyz" || string(got.Email) != "bob@x" {
		t.Fatalf("bad struct scan: %+v", got)
	}
}

func TestScan_Struct_PointerInline_Alloc(t *testing.T) {
	type Org struct {
		OrgID   int64  `db:"org_id"`
		OrgName string `db:"org_name"`
	}
	type Member struct {
		ID    int64  `db:"id"`
		Email string `db:"email"`
		Org   *Org   `db:",inline"`
	}
	cols := []string{"id", "email", "org_id", "org_name"}
	vals := [][]driver.Value{{int64(1), "a@x", int64(0), ""}} // non-NULL to avoid invalid NULL->int64

	db := newTestDB(t, func(q string, _ []driver.NamedValue) ([]string, [][]driver.Value, error) {
		return cols, vals, nil
	})
	defer func() { _ = db.Close() }()

	rows, _ := db.QueryContext(context.Background(), "q")
	m := NewMapper()
	got := nextAndScan[Member](t, m, rows)

	if got.Org == nil {
		t.Fatalf("inline pointer should be allocated for scanning")
	}
	if got.Org.OrgID != 0 || got.Org.OrgName != "" {
		t.Fatalf("unexpected org: %+v", got.Org)
	}
}

func TestScan_Primitive_OneColumn(t *testing.T) {
	cols := []string{"n"}
	vals := [][]driver.Value{{int64(42)}}

	db := newTestDB(t, func(q string, _ []driver.NamedValue) ([]string, [][]driver.Value, error) {
		return cols, vals, nil
	})
	defer func() { _ = db.Close() }()

	rows, _ := db.QueryContext(context.Background(), "q")
	m := NewMapper()
	got := nextAndScan[int64](t, m, rows)
	if got != 42 {
		t.Fatalf("got %d", got)
	}
}

func TestScan_Primitive_ErrTooManyCols(t *testing.T) {
	cols := []string{"a", "b"}
	vals := [][]driver.Value{{1, 2}}

	db := newTestDB(t, func(q string, _ []driver.NamedValue) ([]string, [][]driver.Value, error) {
		return cols, vals, nil
	})
	defer func() { _ = db.Close() }()

	rows, _ := db.QueryContext(context.Background(), "q")
	if !rows.Next() {
		t.Fatal("no row")
	}
	_, err := scanWithMapper[int64](NewMapper(), rows)
	if err == nil || err.Error() == "" {
		t.Fatal("expected error for multiple columns into primitive")
	}
}

func TestScan_ScannerWholeType_OneColumn(t *testing.T) {
	cols := []string{"s"}
	vals := [][]driver.Value{{[]byte("hi")}}

	db := newTestDB(t, func(q string, _ []driver.NamedValue) ([]string, [][]driver.Value, error) {
		return cols, vals, nil
	})
	defer func() { _ = db.Close() }()

	rows, _ := db.QueryContext(context.Background(), "q")
	got := nextAndScan[scanString](t, NewMapper(), rows)
	if string(got) != "hi" {
		t.Fatalf("got %q", got)
	}
}

func TestScan_ScannerWholeType_ErrTooManyCols(t *testing.T) {
	cols := []string{"a", "b"}
	vals := [][]driver.Value{{"x", "y"}}

	db := newTestDB(t, func(q string, _ []driver.NamedValue) ([]string, [][]driver.Value, error) {
		return cols, vals, nil
	})
	defer func() { _ = db.Close() }()

	rows, _ := db.QueryContext(context.Background(), "q")
	if !rows.Next() {
		t.Fatal("no row")
	}
	_, err := scanWithMapper[scanString](NewMapper(), rows)
	if err == nil {
		t.Fatal("expected error: scanner with >1 column")
	}
}

func TestScan_ZeroColumnsError(t *testing.T) {
	cols := []string{}
	vals := [][]driver.Value{{}}

	db := newTestDB(t, func(q string, _ []driver.NamedValue) ([]string, [][]driver.Value, error) {
		return cols, vals, nil
	})
	defer func() { _ = db.Close() }()

	rows, _ := db.QueryContext(context.Background(), "q")
	if !rows.Next() {
		t.Fatal("no row")
	}
	_, err := scanWithMapper[struct{}](NewMapper(), rows)
	if err == nil || err.Error() != "xsql: query returned zero columns" {
		t.Fatalf("unexpected err: %v", err)
	}
}

/* ---------------------------
   JSON bridge Scanner demo
----------------------------*/

type items []string

func (it *items) Scan(src any) error {
	var b []byte
	switch v := src.(type) {
	case string:
		b = []byte(v)
	case []byte:
		b = v
	default:
		return fmt.Errorf("items: %T", src)
	}
	return json.Unmarshal(b, it)
}

func TestScan_JSONScannerField(t *testing.T) {
	type Row struct {
		Items items `db:"items"`
	}

	js, _ := json.Marshal([]string{"a", "b"})
	cols := []string{"items"}
	vals := [][]driver.Value{{js}}

	db := newTestDB(t, func(q string, _ []driver.NamedValue) ([]string, [][]driver.Value, error) {
		return cols, vals, nil
	})
	defer func() { _ = db.Close() }()

	rows, _ := db.QueryContext(context.Background(), "q")
	got := nextAndScan[Row](t, NewMapper(), rows)
	if len(got.Items) != 2 || got.Items[0] != "a" || got.Items[1] != "b" {
		t.Fatalf("bad items: %+v", got.Items)
	}
}

/* ---------------------------
   getMapper lazy init
----------------------------*/

func TestGetMapper_Lazy(t *testing.T) {
	m1 := getMapper()
	m2 := getMapper()
	if m1 == nil || m1 != m2 {
		t.Fatal("getMapper not lazy/singleton")
	}
}

/* ---------------------------
   NEW: Plan/step coverage helpers
----------------------------*/

func TestPlan_MakeFieldStep_Indirect_CustomNamedString(t *testing.T) {
	// Ensure makeFieldStep selects stepIndirect via pickIndirect for named string type.
	type MyStr string
	type Row struct {
		V MyStr `db:"v"`
	}

	m := NewMapper()
	rt := reflect.TypeOf(Row{})
	cols := []string{"v"}
	// hash cols
	h := fnv.New64a()
	for _, c := range cols {
		_, _ = h.Write([]byte(c))
		_, _ = h.Write([]byte{0})
	}
	pl, err := m.getPlan(rt, cols, h.Sum64())
	if err != nil {
		t.Fatal(err)
	}
	if len(pl.steps) != 1 || pl.steps[0].kind != stepIndirect {
		t.Fatalf("want stepIndirect for named string, got %+v", pl.steps[0])
	}
}

func TestPlan_MakeFieldStep_FallbackStepDirect_Interface(t *testing.T) {
	// interface{} is neither directly scannable nor selected by pickIndirect.
	type Row struct {
		Any any `db:"any"`
	}

	m := NewMapper()
	rt := reflect.TypeOf(Row{})
	cols := []string{"any"}
	h := fnv.New64a()
	for _, c := range cols {
		_, _ = h.Write([]byte(c))
		_, _ = h.Write([]byte{0})
	}
	pl, err := m.getPlan(rt, cols, h.Sum64())
	if err != nil {
		t.Fatal(err)
	}
	if len(pl.steps) != 1 || pl.steps[0].kind != stepDirect {
		t.Fatalf("want fallback stepDirect, got %+v", pl.steps[0])
	}
}

func TestPlan_MakeWholeStep_Indirect_Primitive(t *testing.T) {
	// Non-struct primitive T=int32 with one column → makeWholeStep should pick stepIndirect.
	m := NewMapper()
	rt := reflect.TypeOf(int32(0))
	cols := []string{"n"}
	h := fnv.New64a()
	for _, c := range cols {
		_, _ = h.Write([]byte(c))
		_, _ = h.Write([]byte{0})
	}
	pl, err := m.getPlan(rt, cols, h.Sum64())
	if err != nil {
		t.Fatal(err)
	}
	if len(pl.steps) != 1 || pl.steps[0].kind != stepIndirect {
		t.Fatalf("want stepIndirect for int32<-int64, got %+v", pl.steps[0])
	}
}

func TestDestPtrs_NonStructPrimitive_Indirect_Path(t *testing.T) {
	// Drive destPtrs directly: for T=int32 and stepIndirect(int64 tmp), set tmp and run cleanup.
	m := NewMapper()
	rt := reflect.TypeOf(int32(0))
	cols := []string{"n"}
	h := fnv.New64a()
	for _, c := range cols {
		_, _ = h.Write([]byte(c))
		_, _ = h.Write([]byte{0})
	}
	pl, err := m.getPlan(rt, cols, h.Sum64())
	if err != nil {
		t.Fatal(err)
	}

	rv := reflect.New(rt) // *int32
	dests, cleanup, err := pl.destPtrs(rv)
	if err != nil {
		t.Fatal(err)
	}
	if len(dests) != 1 {
		t.Fatalf("want 1 dest, got %d", len(dests))
	}
	// dests[0] is *int64 (tmp). Set it to a value as if rows.Scan wrote it.
	tmp := reflect.ValueOf(dests[0]).Elem()
	tmp.SetInt(123)

	if err := cleanup(); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if rv.Elem().Int() != 123 {
		t.Fatalf("post assign failed, got %d", rv.Elem().Int())
	}
}

func TestPlan_Struct_StepKinds_Inspection(t *testing.T) {
	// Ensure a struct plan contains drop/direct/indirect as expected.
	type Row struct {
		Name string `db:"name"` // []byte -> string → stepIndirect (builtin string path is []byte->string)
		Age  int32  `db:"age"`  // int64 -> int32 → stepIndirect
		OK   bool   `db:"ok"`   // direct
	}
	m := NewMapper()
	cols := []string{"name", "age", "ok", "unmapped"} // last forces stepDrop
	h := fnv.New64a()
	for _, c := range cols {
		_, _ = h.Write([]byte(c))
		_, _ = h.Write([]byte{0})
	}
	pl, err := m.getPlan(reflect.TypeOf(Row{}), cols, h.Sum64())
	if err != nil {
		t.Fatal(err)
	}
	if len(pl.steps) != 4 {
		t.Fatalf("want 4 steps, got %d", len(pl.steps))
	}
	if pl.steps[0].kind != stepIndirect {
		t.Fatalf("name should be stepIndirect, got %v", pl.steps[0].kind)
	}
	if pl.steps[1].kind != stepIndirect {
		t.Fatalf("age should be stepIndirect, got %v", pl.steps[1].kind)
	}
	if pl.steps[2].kind != stepDirect {
		t.Fatalf("ok should be stepDirect, got %v", pl.steps[2].kind)
	}
	if pl.steps[3].kind != stepDrop {
		t.Fatalf("unmapped should be stepDrop, got %v", pl.steps[3].kind)
	}
}
