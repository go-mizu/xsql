// named.go
package xsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Placeholder selects the positional parameter style for a target database.
//
// Common choices:
//   - PlaceholderQuestion   → "?"           (MySQL, SQLite, DuckDB, ClickHouse)
//   - PlaceholderDollar     → "$1, $2, …"  (PostgreSQL)
//   - PlaceholderAtP        → "@p1, @p2…"  (SQL Server)
//   - PlaceholderColonNum   → ":1, :2, …"  (Oracle)
type Placeholder int

const (
	PlaceholderQuestion Placeholder = iota
	PlaceholderDollar
	PlaceholderAtP
	PlaceholderColonNum
)

// ErrNilParams is returned when named binding is requested with a nil pointer
// or nil params value. This typically means you passed a nil *struct to Rebind.
var ErrNilParams = errors.New("xsql: named bind: nil params")

// ErrUnsupportedArg is returned when the single named-binding argument is not a
// struct or map[string]any (e.g., passing an int or map[int]any).
var ErrUnsupportedArg = errors.New("xsql: named bind: params must be struct or map[string]any")

// ErrDuplicateKeyTag is returned when two struct fields (including embedded)
// resolve to the same logical parameter name (case-insensitive), e.g. via db:"name".
var ErrDuplicateKeyTag = errors.New("xsql: named bind: duplicate key from struct tags/fields")

// Rebind resolves :named parameters (if applicable) and rewrites placeholders.
//
// Usage:
//
//   - Named style (exactly one struct or map[string]any):
//     sql, args, err := xsql.Rebind(
//     `SELECT * FROM users WHERE status=:status AND id IN (:ids)`,
//     xsql.PlaceholderDollar,
//     map[string]any{"status":"active", "ids":[]int{1,2,3}},
//     )
//     // sql  => SELECT * FROM users WHERE status=$1 AND id IN ($2,$3,$4)
//     // args => ["active", 1, 2, 3]
//
//     Notes: slices/arrays expand; []byte is scalar; empty slice/array becomes NULL
//     (so `IN (NULL)` matches no rows on most engines).
//
//   - Positional passthrough (any other params shape):
//     // params are already positional; only placeholder rewriting is applied
//     sql, args, _ := xsql.Rebind(`a=? AND b=?`, xsql.PlaceholderColonNum, "A", 10)
//
// Rules of thumb:
//   - Pass exactly one struct or map to use :named binding.
//   - Pass multiple values (or a non-struct/map) to use positional args.
//   - SQL scanning safely skips quoted strings, comments, and PostgreSQL $tag$…$tag$ blocks.
func Rebind(query string, ph Placeholder, params ...any) (string, []any, error) {
	if len(params) == 1 && looksBindable(params[0]) {
		qPos, args, err := bindNamedParams(query, params[0])
		if err != nil {
			return "", nil, err
		}
		return rewritePlaceholders(qPos, ph), args, nil
	}
	return rewritePlaceholders(query, ph), params, nil
}

// NamedExec is a convenience for Exec with named or positional arguments.
// It calls Rebind, then ExecContext on your Execer (e.g., *sql.DB, *sql.Tx, *sql.Conn).
//
// Example:
//
//	_, err := xsql.NamedExec(ctx, db, xsql.PlaceholderAtP,
//	    `UPDATE items SET price=:p WHERE id IN (:ids)`,
//	    map[string]any{"p": 100, "ids": []int{7,8,9}},
//	)
func NamedExec(ctx context.Context, e Execer, ph Placeholder, query string, params ...any) (sql.Result, error) {
	bound, args, err := Rebind(query, ph, params...)
	if err != nil {
		return nil, err
	}
	return e.ExecContext(ctx, bound, args...)
}

// NamedQuery runs a query with named or positional arguments and scans results
// using your existing Query[T]. Use this when you want []T back with minimal ceremony.
//
// Example:
//
//	type User struct { ID int64 `db:"id"`; Email string `db:"email"` }
//	rows, err := xsql.NamedQuery[User](ctx, db, xsql.PlaceholderDollar,
//	    `SELECT id, email FROM users WHERE status=:s`,
//	    map[string]any{"s":"active"},
//	)
func NamedQuery[T any](ctx context.Context, q Querier, ph Placeholder, query string, params ...any) ([]T, error) {
	bound, args, err := Rebind(query, ph, params...)
	if err != nil {
		return nil, err
	}
	return Query[T](ctx, q, bound, args...)
}

// PlaceholderFor picks a Placeholder based on a driver name string.
// This is a convenience for one-off calls; you can also choose the enum directly.
//
// Examples:
//
//	ph := xsql.PlaceholderFor("pgx")       // => PlaceholderDollar
//	ph := xsql.PlaceholderFor("sqlserver") // => PlaceholderAtP
//	ph := xsql.PlaceholderFor("mysql")     // => PlaceholderQuestion
func PlaceholderFor(driverName string) Placeholder {
	switch strings.ToLower(driverName) {
	case "pgx", "postgres", "postgresql", "lib/pq", "pg":
		return PlaceholderDollar
	case "sqlserver", "mssql":
		return PlaceholderAtP
	case "godror", "oracle", "goracle":
		return PlaceholderColonNum
	default:
		return PlaceholderQuestion
	}
}

type nameToken struct {
	name  string
	start int
	end   int
}

func looksBindable(v any) bool {
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return false
		}
		rv = rv.Elem()
	}
	if rv.Kind() == reflect.Map {
		return rv.Type().Key().Kind() == reflect.String
	}
	return rv.Kind() == reflect.Struct
}

func bindNamedParams(query string, params any) (string, []any, error) {
	if params == nil {
		return "", nil, ErrNilParams
	}

	toks, err := findNamedParams(query)
	if err != nil {
		return "", nil, err
	}
	if len(toks) == 0 {
		return query, nil, nil
	}

	lut, err := buildParamLookup(params)
	if err != nil {
		return "", nil, err
	}

	var b strings.Builder
	b.Grow(len(query))
	args := make([]any, 0, len(toks))
	last := 0

	for _, t := range toks {
		b.WriteString(query[last:t.start])

		val, ok := lut.lookup(t.name)
		if !ok {
			return "", nil, fmt.Errorf("xsql: named bind: missing value for :%s", t.name)
		}

		rv := reflect.ValueOf(val)
		if isSliceOrArray(rv) {
			n := rv.Len()
			if n == 0 {
				b.WriteString("NULL")
			} else {
				for i := 0; i < n; i++ {
					if i > 0 {
						b.WriteByte(',')
					}
					b.WriteByte('?')
					args = append(args, rv.Index(i).Interface())
				}
			}
		} else {
			b.WriteByte('?')
			args = append(args, val)
		}
		last = t.end
	}
	b.WriteString(query[last:])
	return b.String(), args, nil
}

func findNamedParams(query string) ([]nameToken, error) {
	var out []nameToken
	i := 0
	for i < len(query) {
		r, w := utf8.DecodeRuneInString(query[i:])
		switch r {
		case '\'':
			j, err := skipSingleQuoted(query, i+w)
			if err != nil {
				return nil, err
			}
			i = j
			continue
		case '"':
			j, err := skipDoubleQuoted(query, i+w)
			if err != nil {
				return nil, err
			}
			i = j
			continue
		case '`':
			j, err := skipBacktickQuoted(query, i+w)
			if err != nil {
				return nil, err
			}
			i = j
			continue
		case '-':
			if hasPrefix(query[i:], "--") {
				i = skipLineComment(query, i+2)
				continue
			}
		case '/':
			if hasPrefix(query[i:], "/*") {
				j, err := skipBlockComment(query, i+2)
				if err != nil {
					return nil, err
				}
				i = j
				continue
			}
		case '$':
			if j, ok, err := skipDollarQuoted(query, i); err != nil {
				return nil, err
			} else if ok {
				i = j
				continue
			}
		case ':':
			if hasPrefix(query[i:], "::") {
				i += 2 // skip PG cast
				continue
			}
			start := i
			name, end := parseIdent(query, i+1)
			if name != "" {
				out = append(out, nameToken{name: name, start: start, end: end})
				i = end
				continue
			}
		}
		i += w
	}
	return out, nil
}

func rewritePlaceholders(query string, ph Placeholder) string {
	if ph == PlaceholderQuestion {
		return query
	}
	out := make([]byte, 0, len(query)+16)
	i, arg := 0, 1

	for i < len(query) {
		r, w := utf8.DecodeRuneInString(query[i:])
		switch r {
		case '\'':
			j, _ := skipSingleQuoted(query, i+w)
			out = append(out, query[i:j]...)
			i = j
			continue
		case '"':
			j, _ := skipDoubleQuoted(query, i+w)
			out = append(out, query[i:j]...)
			i = j
			continue
		case '`':
			j, _ := skipBacktickQuoted(query, i+w)
			out = append(out, query[i:j]...)
			i = j
			continue
		case '-':
			if hasPrefix(query[i:], "--") {
				j := skipLineComment(query, i+2)
				out = append(out, query[i:j]...)
				i = j
				continue
			}
		case '/':
			if hasPrefix(query[i:], "/*") {
				j, _ := skipBlockComment(query, i+2)
				out = append(out, query[i:j]...)
				i = j
				continue
			}
		case '$':
			if j, ok, _ := skipDollarQuoted(query, i); ok {
				out = append(out, query[i:j]...)
				i = j
				continue
			}
		case '?':
			switch ph {
			case PlaceholderDollar:
				out = append(out, '$')
				out = strconv.AppendInt(out, int64(arg), 10)
			case PlaceholderAtP:
				out = append(out, '@', 'p')
				out = strconv.AppendInt(out, int64(arg), 10)
			case PlaceholderColonNum:
				out = append(out, ':')
				out = strconv.AppendInt(out, int64(arg), 10)
			default:
				out = append(out, '?')
			}
			arg++
			i += w
			continue
		}
		out = append(out, query[i:i+w]...)
		i += w
	}
	return string(out)
}

func skipSingleQuoted(s string, i int) (int, error) {
	for i < len(s) {
		r, w := utf8.DecodeRuneInString(s[i:])
		i += w
		if r == '\'' {
			if i < len(s) && s[i] == '\'' {
				i++
				continue
			}
			return i, nil
		}
	}
	return 0, fmt.Errorf("xsql: unterminated single-quoted string")
}

func skipDoubleQuoted(s string, i int) (int, error) {
	for i < len(s) {
		r, w := utf8.DecodeRuneInString(s[i:])
		i += w
		if r == '"' {
			if i < len(s) && s[i] == '"' {
				i++
				continue
			}
			return i, nil
		}
	}
	return 0, fmt.Errorf("xsql: unterminated double-quoted identifier")
}

func skipBacktickQuoted(s string, i int) (int, error) {
	for i < len(s) {
		r, w := utf8.DecodeRuneInString(s[i:])
		i += w
		if r == '`' {
			if i < len(s) && s[i] == '`' {
				i++
				continue
			}
			return i, nil
		}
	}
	return 0, fmt.Errorf("xsql: unterminated backtick-quoted identifier")
}

func skipLineComment(s string, i int) int {
	for i < len(s) {
		if s[i] == '\n' {
			return i + 1
		}
		i++
	}
	return i
}

func skipBlockComment(s string, i int) (int, error) {
	for i < len(s)-1 {
		if s[i] == '*' && s[i+1] == '/' {
			return i + 2, nil
		}
		i++
	}
	return 0, fmt.Errorf("xsql: unterminated block comment")
}

// skipDollarQuoted handles $$...$$ and $tag$...$tag$ (PostgreSQL).
func skipDollarQuoted(s string, i int) (int, bool, error) {
	if s[i] != '$' {
		return 0, false, nil
	}
	j := i + 1
	for j < len(s) && s[j] != '$' && isTagChar(rune(s[j])) {
		j++
	}
	if j >= len(s) || s[j] != '$' {
		return 0, false, nil
	}
	tag := s[i : j+1]
	k := j + 1
	for {
		idx := strings.Index(s[k:], tag)
		if idx < 0 {
			return 0, true, fmt.Errorf("xsql: unterminated dollar-quoted string")
		}
		k += idx + len(tag)
		return k, true, nil
	}
}

func isTagChar(r rune) bool      { return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r) }
func hasPrefix(s, p string) bool { return len(s) >= len(p) && s[:len(p)] == p }

func parseIdent(s string, i int) (string, int) {
	start := i
	for i < len(s) {
		r, w := utf8.DecodeRuneInString(s[i:])
		if !(r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)) {
			break
		}
		i += w
	}
	if i == start {
		return "", i
	}
	return s[start:i], i
}

type paramLookup struct {
	m map[string]any // lowercase name -> value
}

func (l *paramLookup) lookup(name string) (any, bool) {
	v, ok := l.m[strings.ToLower(name)]
	return v, ok
}

func buildParamLookup(params any) (*paramLookup, error) {
	rv := reflect.ValueOf(params)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil, ErrNilParams
		}
		rv = rv.Elem()
	}
	switch rv.Kind() {
	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			return nil, ErrUnsupportedArg
		}
		m := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			m[strings.ToLower(iter.Key().String())] = iter.Value().Interface()
		}
		return &paramLookup{m: m}, nil
	case reflect.Struct:
		m := make(map[string]any)
		if err := addStructFields(m, rv); err != nil {
			return nil, err
		}
		return &paramLookup{m: m}, nil
	default:
		return nil, ErrUnsupportedArg
	}
}

func addStructFields(dst map[string]any, v reflect.Value) error {
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)

		if f.PkgPath != "" && !f.Anonymous {
			continue
		}

		// Embedded types: follow pointer chains; skip if nil; flatten fields.
		if f.Anonymous {
			ft := f.Type
			fv := v.Field(i)

			isNil := false
			for ft.Kind() == reflect.Pointer {
				if fv.IsNil() {
					isNil = true
					break
				}
				ft = ft.Elem()
				fv = fv.Elem()
			}
			if !isNil && ft.Kind() == reflect.Struct {
				if err := addStructFields(dst, fv); err != nil {
					return err
				}
				continue
			}
		}

		tag := f.Tag.Get("db")
		if tag == "-" {
			continue
		}
		name := tag
		if name == "" {
			name = f.Name
		}
		key := strings.ToLower(name)
		if _, exists := dst[key]; exists {
			return fmt.Errorf("%w: %q", ErrDuplicateKeyTag, key)
		}
		dst[key] = v.Field(i).Interface()
	}
	return nil
}

func isSliceOrArray(v reflect.Value) bool {
	if !v.IsValid() {
		return false
	}
	switch v.Kind() {
	case reflect.Slice:
		return v.Type().Elem().Kind() != reflect.Uint8 // []byte → scalar
	case reflect.Array:
		return true
	default:
		return false
	}
}
