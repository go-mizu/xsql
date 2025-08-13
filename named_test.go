// named_test.go
package xsql

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

type result struct{ rows, lastID int64 }

func (r result) LastInsertId() (int64, error) { return r.lastID, nil }
func (r result) RowsAffected() (int64, error) { return r.rows, nil }

type execer struct {
	lastQuery string
	lastArgs  []any
	res       sql.Result
	err       error
}

func (e *execer) ExecContext(_ context.Context, q string, args ...any) (sql.Result, error) {
	e.lastQuery, e.lastArgs = q, args
	if e.err != nil {
		return nil, e.err
	}
	if e.res == nil {
		e.res = result{rows: int64(len(args)), lastID: 123}
	}
	return e.res, nil
}

type querier struct {
	lastQuery string
	lastArgs  []any
	err       error
}

func (q *querier) QueryContext(_ context.Context, query string, args ...any) (*sql.Rows, error) {
	q.lastQuery, q.lastArgs = query, args
	if q.err != nil {
		return nil, q.err
	}
	return nil, errors.New("rows not implemented in test querier")
}

func eq[T comparable](t *testing.T, got, want T, msg string) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: got=%v want=%v", msg, got, want)
	}
}

func eqSlice(t *testing.T, got, want []any, msg string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: len got=%d want=%d\n got=%v\nwant=%v", msg, len(got), len(want), got, want)
	}
	for i := range got {
		if !reflect.DeepEqual(got[i], want[i]) {
			t.Fatalf("%s: idx %d got=%#v want=%#v\n got=%v\nwant=%v", msg, i, got[i], want[i], got, want)
		}
	}
}

var reDollarToken = regexp.MustCompile(`\$\d+`)
var reAtPToken = regexp.MustCompile(`@p\d+`)
var reColonNum = regexp.MustCompile(`:\d+`)

type baseEmb struct {
	Tenant int `db:"tenant"`
}

type argStruct struct {
	baseEmb
	Status string    `db:"status"`
	IDs    []int64   `db:"ids"`
	Since  time.Time `db:"since"`
	Skip   string    `db:"-"` // ignored
}

func TestRebind_NamedStruct_Postgres(t *testing.T) {
	a := argStruct{
		baseEmb: baseEmb{Tenant: 42},
		Status:  "active",
		IDs:     []int64{7, 8, 9},
		Since:   time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC),
	}
	in := `
SELECT id
FROM users
WHERE tenant=:tenant AND status=:status
  AND id IN (:ids) AND created_at >= :since
-- :in_comment
/* :in_block */
$tag$ :in_dollar $tag$
`
	sqlOut, args, err := Rebind(in, PlaceholderDollar, a)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(reDollarToken.FindAllString(sqlOut, -1)); n != 6 {
		t.Fatalf("expected 6 positional tokens, got %d in:\n%s", n, sqlOut)
	}
	want := []any{42, "active", int64(7), int64(8), int64(9), a.Since}
	eqSlice(t, args, want, "args order")
	if strings.Contains(sqlOut, ":tenant") || strings.Contains(sqlOut, ":ids") || strings.Contains(sqlOut, ":since") {
		t.Fatalf("named tokens remain: %s", sqlOut)
	}
}

func TestRebind_NamedMap_SQLServer_EmptySliceToNULL(t *testing.T) {
	params := map[string]any{"status": "x", "ids": []int{}}
	in := `SELECT 1 WHERE status=:status AND id IN (:ids)`
	out, args, err := Rebind(in, PlaceholderAtP, params)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "IN (NULL)") {
		t.Fatalf("expected IN (NULL), got: %s", out)
	}
	if !reAtPToken.MatchString(out) || !strings.Contains(out, "@p1") {
		t.Fatalf("expected @p1 in: %s", out)
	}
	eqSlice(t, args, []any{"x"}, "args with empty slice")
}

func TestRebind_NamedMap_BytesAndArray(t *testing.T) {
	blob := []byte("hi")
	arr := [2]int{5, 6}
	params := map[string]any{"b": blob, "nums": arr}
	in := `SELECT 1 WHERE b=:b AND n IN (:nums)`
	out, args, err := Rebind(in, PlaceholderDollar, params)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "b=$1") || !strings.Contains(out, "IN ($2,$3)") {
		t.Fatalf("unexpected sql: %s", out)
	}
	eqSlice(t, args, []any{blob, 5, 6}, "bytes+array args")
}

func TestRebind_RepeatedNames_Numbering(t *testing.T) {
	type P struct {
		X   int   `db:"x"`
		Arr []int `db:"arr"`
	}
	p := P{X: 9, Arr: []int{1}}
	in := `WHERE a=:x OR b=:x OR c IN (:arr) OR d=:x`
	out, args, err := Rebind(in, PlaceholderDollar, p)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "a=$1") || !strings.Contains(out, "b=$2") || !strings.Contains(out, "IN ($3)") || !strings.Contains(out, "d=$4") {
		t.Fatalf("bad numbering/order: %s", out)
	}
	eqSlice(t, args, []any{9, 9, 1, 9}, "repeated named args order")
}

func TestRebind_PositionalPassthrough_Oracle(t *testing.T) {
	in := `SELECT * FROM t WHERE a=? AND b IN (?,?) -- ? in comment`
	out, args, err := Rebind(in, PlaceholderColonNum, "aa", 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, out, "SELECT * FROM t WHERE a=:1 AND b IN (:2,:3) -- ? in comment", "rewrite")
	eqSlice(t, args, []any{"aa", 2, 3}, "positional passthrough")
}

func TestRebind_NoParams_QuestionUnchanged(t *testing.T) {
	in := "SELECT ? AS x, '--' AS y"
	out, args, err := Rebind(in, PlaceholderQuestion)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, out, in, "no-op for question")
	if len(args) != 0 {
		t.Fatalf("expected zero args, got %v", args)
	}
}

func TestBindNamedParams_NilSingleParam_Error(t *testing.T) {
	var p *struct{ A int }
	_, _, err := bindNamedParams(`SELECT :a`, p)
	if !errors.Is(err, ErrNilParams) {
		t.Fatalf("want ErrNilParams, got %v", err)
	}
}

func TestRewrite_SkipsStringsComments_DollarQuoted(t *testing.T) {
	in := `
SELECT '?', $$ ? $$, $z$ ? $z$, -- ? line
/* ? block */ ? AS bind
`
	got := rewritePlaceholders(in, PlaceholderDollar)
	if n := len(reDollarToken.FindAllString(got, -1)); n != 1 || !strings.Contains(got, "$1 AS bind") {
		t.Fatalf("unexpected rewrite:\n%s", got)
	}
}

func TestRewrite_SkipsDoubleQuotedIdentifiers(t *testing.T) {
	in := `SELECT "a ? "" b", ?`
	got := rewritePlaceholders(in, PlaceholderDollar)
	if n := len(reDollarToken.FindAllString(got, -1)); n != 1 || !strings.HasSuffix(strings.TrimSpace(got), "$1") {
		t.Fatalf("expected only one $n, got: %s", got)
	}
}

func TestRewrite_SkipsBacktickQuotedIdentifiers(t *testing.T) {
	in := "SELECT `c ? `` d`, ?"
	got := rewritePlaceholders(in, PlaceholderDollar)
	if n := len(reDollarToken.FindAllString(got, -1)); n != 1 || !strings.HasSuffix(strings.TrimSpace(got), "$1") {
		t.Fatalf("expected only one $n, got: %s", got)
	}
}

func TestRewrite_SkipsDollarDollar(t *testing.T) {
	in := "SELECT $$ ? $$, ?;"
	got := rewritePlaceholders(in, PlaceholderDollar)
	if n := len(reDollarToken.FindAllString(got, -1)); n != 1 {
		t.Fatalf("expected exactly one $N token, got %d in: %s", n, got)
	}
	if !strings.Contains(got, " $1;") {
		t.Fatalf("expected trailing $1, got: %s", got)
	}
}

func TestRewrite_IgnoresPGCasts(t *testing.T) {
	in := `SELECT :1::int, :abc, x::text, ?`
	got := rewritePlaceholders(in, PlaceholderDollar)
	if !strings.HasSuffix(strings.TrimSpace(got), "$1") {
		t.Fatalf("expected last ? -> $1, got: %s", got)
	}
}

func TestRewrite_SQLServer_TwoDigitNumbers(t *testing.T) {
	in := "?" + strings.Repeat(",?", 11)
	got := rewritePlaceholders(in, PlaceholderAtP)
	for i := 1; i <= 12; i++ {
		if !strings.Contains(got, "@p"+strconv.Itoa(i)) {
			t.Fatalf("missing @p%d in %s", i, got)
		}
	}
}

func TestFindNamedParams_SkipsQuotesCommentsCasts_AndOrders(t *testing.T) {
	in := `
-- :skip
/* :also_skip */
SELECT ':no', ":no", ` + "`:no`" + `,
$tag$ :no $tag$,
:ok1, :ok_2, ::int, :x9, :_lead, :n1
`
	toks, err := findNamedParams(in)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, tk := range toks {
		names = append(names, tk.name)
		if in[tk.start:tk.end] != ":"+tk.name {
			t.Fatalf("bad offsets for %q (%d,%d)", tk.name, tk.start, tk.end)
		}
	}
	want := []string{"ok1", "ok_2", "x9", "_lead", "n1"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("names mismatch: got %v, want %v", names, want)
	}
}

func TestFindNamedParams_ErrorsFromSkippers(t *testing.T) {
	// single quote unterminated
	if _, err := findNamedParams("'abc"); err == nil {
		t.Fatalf("expected error for unterminated single-quoted")
	}
	// double quote unterminated
	if _, err := findNamedParams(`"abc`); err == nil {
		t.Fatalf("expected error for unterminated double-quoted")
	}
	// backtick unterminated
	if _, err := findNamedParams("`abc"); err == nil {
		t.Fatalf("expected error for unterminated backtick-quoted")
	}
	// block comment unterminated
	if _, err := findNamedParams("/* abc"); err == nil {
		t.Fatalf("expected error for unterminated block comment")
	}
	// dollar-quoted unterminated
	if _, err := findNamedParams("$tag$ abc"); err == nil {
		t.Fatalf("expected error for unterminated dollar-quoted")
	}
}

func TestBuildParamLookup_StructEmbeddedAndMap(t *testing.T) {
	type Inner struct {
		A int `db:"a"`
	}
	type Outer struct {
		Inner
		B string `db:"b"`
		C string `db:"-"`
	}
	o := Outer{Inner: Inner{A: 10}, B: "bee", C: "skip"}
	lut, err := buildParamLookup(o)
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := lut.lookup("A"); !ok || v.(int) != 10 {
		t.Fatalf("lookup A failed: %#v %#v", ok, v)
	}
	if v, ok := lut.lookup("b"); !ok || v.(string) != "bee" {
		t.Fatalf("lookup b failed: %#v %#v", ok, v)
	}
	if _, ok := lut.lookup("c"); ok {
		t.Fatalf(`db:"-" should be skipped`)
	}

	lut2, err := buildParamLookup(map[string]any{"X": 1})
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := lut2.lookup("x"); !ok || v.(int) != 1 {
		t.Fatalf("map lookup failed")
	}
}

func TestBuildParamLookup_ErrorsAndUnexported(t *testing.T) {
	// Nil pointer -> ErrNilParams
	var p *struct{ A int }
	if _, err := buildParamLookup(p); !errors.Is(err, ErrNilParams) {
		t.Fatalf("expected ErrNilParams, got %v", err)
	}
	// Unsupported arg kind: non-string key map
	if _, err := buildParamLookup(map[int]any{1: 2}); !errors.Is(err, ErrUnsupportedArg) {
		t.Fatalf("expected ErrUnsupportedArg for map[int]any, got %v", err)
	}
	// Unsupported arg kind: scalar
	if _, err := buildParamLookup(123); !errors.Is(err, ErrUnsupportedArg) {
		t.Fatalf("expected ErrUnsupportedArg, got %v", err)
	}

	// Unexported non-embedded field should be skipped (no key "x"),
	// and exported field should be present (key "y").
	type HasUnexported struct {
		x int `db:"x"` // unexported, not anonymous -> must be skipped
		Y int `db:"y"` // exported -> must be included under key "y"
	}
	lut, err := buildParamLookup(HasUnexported{x: 1, Y: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := lut.lookup("x"); ok {
		t.Fatalf("unexported non-embedded field must be skipped")
	}
	if v, ok := lut.lookup("y"); !ok || v.(int) != 2 {
		t.Fatalf("exported field missing or wrong; got (%v,%v)", v, ok)
	}
}

func TestAddStructFields_PointerChainNonNil_AndNilSkip(t *testing.T) {
	type E struct {
		Z int `db:"z"`
	}
	type OuterPtr struct {
		*E     // anonymous embedded pointer (non-nil → unwrap)
		Y  int `db:"y"`
	}
	op := OuterPtr{E: &E{Z: 7}, Y: 42}
	lut, err := buildParamLookup(op)
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := lut.lookup("z"); !ok || v.(int) != 7 {
		t.Fatalf("embedded pointer fields not flattened (non-nil)")
	}
	if v, ok := lut.lookup("y"); !ok || v.(int) != 42 {
		t.Fatalf("outer field y missing")
	}

	// Now nil embedded pointer path (should be skipped)
	op2 := OuterPtr{E: nil, Y: 99}
	lut2, err := buildParamLookup(op2)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := lut2.lookup("z"); ok {
		t.Fatalf("nil embedded pointer should be skipped")
	}
	if v, ok := lut2.lookup("y"); !ok || v.(int) != 99 {
		t.Fatalf("outer field y missing in nil case")
	}
}

func TestLooksBindable(t *testing.T) {
	type S struct{ X int }
	var nilPtr *S
	if looksBindable(nilPtr) {
		t.Fatalf("nil pointer must not be bindable")
	}
	if !looksBindable(S{}) {
		t.Fatalf("struct must be bindable")
	}
	if !looksBindable(map[string]any{"a": 1}) {
		t.Fatalf("map[string]any must be bindable")
	}
	if looksBindable(map[int]any{1: 2}) {
		t.Fatalf("map[int]any must NOT be bindable")
	}
}

func TestSkip_SingleQuoted_WithEscapes(t *testing.T) {
	end, err := skipSingleQuoted("'a''b''c'", 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if end != len("'a''b''c'") {
		t.Fatalf("unexpected end=%d want=%d", end, len("'a''b''c'"))
	}
}

func TestSkip_DoubleQuoted_WithEscapes(t *testing.T) {
	end, err := skipDoubleQuoted(`"a""b""c"`, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if end != len(`"a""b""c"`) {
		t.Fatalf("unexpected end=%d want=%d", end, len(`"a""b""c"`))
	}
}

func TestSkip_BacktickQuoted_WithEscapes(t *testing.T) {
	end, err := skipBacktickQuoted("`a``b``c`", 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if end != len("`a``b``c`") {
		t.Fatalf("unexpected end=%d want=%d", end, len("`a``b``c`"))
	}
}

func TestSkip_UnterminatedSingleQuoted(t *testing.T) {
	_, err := skipSingleQuoted("'abc", 1)
	if err == nil {
		t.Fatalf("expected unterminated single-quoted error")
	}
}

func TestSkip_UnterminatedDoubleQuoted(t *testing.T) {
	_, err := skipDoubleQuoted(`"abc`, 1)
	if err == nil {
		t.Fatalf("expected unterminated double-quoted error")
	}
}

func TestSkip_UnterminatedBacktickQuoted(t *testing.T) {
	_, err := skipBacktickQuoted("`abc", 1)
	if err == nil {
		t.Fatalf("expected unterminated backtick-quoted error")
	}
}

func TestSkip_UnterminatedBlockComment(t *testing.T) {
	_, err := skipBlockComment("/* x", 2)
	if err == nil {
		t.Fatalf("expected unterminated block comment")
	}
}

func TestSkipDollarQuoted_NotAtDollar(t *testing.T) {
	end, ok, err := skipDollarQuoted("notDollar", 0)
	if end != 0 || ok || err != nil {
		t.Fatalf("expected (0,false,nil), got (%d,%v,%v)", end, ok, err)
	}
}

func TestSkip_UnterminatedDollarQuoted(t *testing.T) {
	_, ok, err := skipDollarQuoted("$tag$ no end", 0)
	if !ok || err == nil {
		t.Fatalf("expected unterminated dollar-quoted error")
	}
}

func TestNamedExec_PassesFinalSQLAndArgs_SQLServer(t *testing.T) {
	e := &execer{}
	params := map[string]any{"p": 5, "ids": []int{10, 11}}
	in := `UPDATE t SET v=:p WHERE id IN (:ids)`
	res, err := NamedExec(context.Background(), e, PlaceholderAtP, in, params)
	if err != nil {
		t.Fatal(err)
	}
	wantSQL := "UPDATE t SET v=@p1 WHERE id IN (@p2,@p3)"
	if strings.TrimSpace(e.lastQuery) != wantSQL {
		t.Fatalf("rewrite mismatch:\n got: %s\nwant: %s", strings.TrimSpace(e.lastQuery), wantSQL)
	}
	eqSlice(t, e.lastArgs, []any{5, 10, 11}, "exec args order")
	if res == nil {
		t.Fatalf("expected non-nil result")
	}
}

func TestNamedExec_Oracle_NumberingWithRepeat(t *testing.T) {
	e := &execer{}
	params := map[string]any{"v": 1, "ids": []int{7, 8}}
	in := `UPDATE t SET a=:v WHERE id IN (:ids) AND flag=:v`
	_, err := NamedExec(context.Background(), e, PlaceholderColonNum, in, params)
	if err != nil {
		t.Fatal(err)
	}
	want := "UPDATE t SET a=:1 WHERE id IN (:2,:3) AND flag=:4"
	if strings.TrimSpace(e.lastQuery) != want {
		t.Fatalf("oracle numbering mismatch:\n got: %s\nwant: %s", strings.TrimSpace(e.lastQuery), want)
	}
	eqSlice(t, e.lastArgs, []any{1, 7, 8, 1}, "oracle args order")
}

func TestNamedQuery_PositionalPassthrough_UsesQuerier(t *testing.T) {
	q := &querier{}
	_, _ = NamedQuery[string](context.Background(), q, PlaceholderColonNum,
		`SELECT x FROM t WHERE a=? AND b=?`, "A", "B")
	want := "SELECT x FROM t WHERE a=:1 AND b=:2"
	if strings.TrimSpace(q.lastQuery) != want {
		t.Fatalf("NamedQuery rewrite mismatch:\n got: %s\nwant: %s", strings.TrimSpace(q.lastQuery), want)
	}
	eqSlice(t, q.lastArgs, []any{"A", "B"}, "NamedQuery passthrough args")
}

func TestPlaceholderFor(t *testing.T) {
	eq(t, PlaceholderFor("pgx"), PlaceholderDollar, "pgx")
	eq(t, PlaceholderFor("lib/pq"), PlaceholderDollar, "lib/pq")
	eq(t, PlaceholderFor("sqlserver"), PlaceholderAtP, "sqlserver")
	eq(t, PlaceholderFor("godror"), PlaceholderColonNum, "oracle")
	eq(t, PlaceholderFor("mysql"), PlaceholderQuestion, "default")
}

func TestIsSliceOrArray(t *testing.T) {
	if !isSliceOrArray(reflect.ValueOf([]int{1})) {
		t.Fatalf("[]int should expand")
	}
	if isSliceOrArray(reflect.ValueOf([]byte{1})) {
		t.Fatalf("[]byte should be scalar")
	}
	if !isSliceOrArray(reflect.ValueOf([2]int{1, 2})) {
		t.Fatalf("array should expand")
	}
	if isSliceOrArray(reflect.Value{}) {
		t.Fatalf("invalid value should not expand")
	}
}
