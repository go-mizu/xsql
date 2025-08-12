package xsql

import (
	"database/sql"
	"fmt"
	"hash/fnv"
	"reflect"
	"sync"
	"time"
)

// Mapper owns caches. Use the package-level lazy getter (getMapper) or create your own in tests.
type Mapper struct {
	planCache        sync.Map // key: planKey -> *plan   (per (T, column-set))
	structIndexCache sync.Map // key: reflect.Type -> *fieldIndex (per T)
	Strict           bool     // reserved: future strict mode (not enforced here)
}

func NewMapper() *Mapper { return &Mapper{} }

// --- package-level lazy global mapper (used by Query/Get) ---

var (
	mapper     *Mapper
	mapperOnce sync.Once
)

func getMapper() *Mapper {
	mapperOnce.Do(func() { mapper = NewMapper() })
	return mapper
}

// scanWithMapper is the hot path used by Query/Get. It scans the *current row* into T using m's caches.
func scanWithMapper[T any](m *Mapper, rows *sql.Rows) (T, error) {
	var zero T

	cols, err := rows.Columns()
	if err != nil {
		return zero, err
	}
	if len(cols) == 0 {
		return zero, fmt.Errorf("xsql: query returned zero columns")
	}

	// Normalize & hash columns
	h := fnv.New64a()
	for i := range cols {
		cols[i] = normalizeColAscii(cols[i])
		_, _ = h.Write([]byte(cols[i]))
		_, _ = h.Write([]byte{0})
	}
	colHash := h.Sum64()

	rt := reflect.TypeOf((*T)(nil)).Elem()
	pl, err := m.getPlan(rt, cols, colHash)
	if err != nil {
		return zero, err
	}

	// Allocate destination & scan
	rv := reflect.New(rt) // *T
	dests, cleanup, err := pl.destPtrs(rv)
	if err != nil {
		return zero, err
	}
	if err := rows.Scan(dests...); err != nil {
		return zero, err
	}
	if err := cleanup(); err != nil {
		return zero, err
	}
	return rv.Elem().Interface().(T), nil
}

// ---------------- Planning & caches ----------------

type planKey struct {
	rt    reflect.Type
	hash  uint64 // FNV-1a of normalized columns
	ncols int
}

type plan struct {
	rt       reflect.Type
	steps    []step // one per column
	isStruct bool
	isScan   bool // T implements sql.Scanner
}

type stepKind uint8

const (
	stepDrop     stepKind = iota // sink into RawBytes
	stepDirect                   // scan directly into field address or *T
	stepIndirect                 // scan into temp, then convert/assign
	stepWhole                    // *T (Scanner) single-column
)

type step struct {
	kind   stepKind
	fpath  []int        // for struct fields
	convTo reflect.Type // for indirect
	post   func(dst, src reflect.Value) error
}

func (m *Mapper) getPlan(rt reflect.Type, cols []string, colHash uint64) (*plan, error) {
	key := planKey{rt: rt, hash: colHash, ncols: len(cols)}
	if v, ok := m.planCache.Load(key); ok {
		return v.(*plan), nil
	}

	p := &plan{
		rt:       rt,
		isStruct: isStruct(rt),
		isScan:   implementsScanner(rt),
	}

	if p.isStruct {
		indexer := m.structIndex(rt)
		p.steps = make([]step, len(cols))
		for i, c := range cols {
			if fp, ok := indexer.byName[c]; ok {
				st, err := makeFieldStep(rt, fp)
				if err != nil {
					return nil, err
				}
				p.steps[i] = st
			} else {
				p.steps[i] = step{kind: stepDrop}
			}
		}
	} else {
		// Non-struct T
		if p.isScan {
			if len(cols) != 1 {
				return nil, fmt.Errorf("xsql: scanning %s requires exactly 1 column; got %d", rt, len(cols))
			}
			p.steps = []step{{kind: stepWhole}}
		} else {
			if len(cols) != 1 {
				return nil, fmt.Errorf("xsql: cannot map %d columns into %s; use a struct", len(cols), rt)
			}
			st, err := makeWholeStep(rt)
			if err != nil {
				return nil, err
			}
			p.steps = []step{st}
		}
	}

	m.planCache.Store(key, p)
	return p, nil
}

type fieldIndex struct {
	byName map[string][]int // lower-case column name -> index path
}

func (m *Mapper) structIndex(rt reflect.Type) *fieldIndex {
	if v, ok := m.structIndexCache.Load(rt); ok {
		return v.(*fieldIndex)
	}
	fi := buildStructIndex(rt)
	m.structIndexCache.Store(rt, &fi)
	return &fi
}

// --------------- Dest allocation per scan ---------------

func (p *plan) destPtrs(rv reflect.Value) ([]any, func() error, error) {
	// Whole-type Scanner case
	if !p.isStruct && p.steps[0].kind == stepWhole {
		return []any{rv.Interface()}, func() error { return nil }, nil
	}

	// Non-struct primitive (single column)
	if !p.isStruct && len(p.steps) == 1 && p.steps[0].kind != stepWhole {
		st := p.steps[0]
		switch st.kind {
		case stepDirect:
			return []any{rv.Interface()}, func() error { return nil }, nil
		case stepIndirect:
			tmp := reflect.New(st.convTo).Elem()
			return []any{tmp.Addr().Interface()}, func() error {
				return st.post(rv.Elem(), tmp)
			}, nil
		default:
			var sink sql.RawBytes
			return []any{&sink}, func() error { return nil }, nil
		}
	}

	// Struct mapping
	root := rv.Elem()
	steps := p.steps
	dests := make([]any, len(steps))
	finals := make([]func() error, 0, 4)

	var sink sql.RawBytes // reused for all unmapped columns
	for i := 0; i < len(steps); i++ {
		st := steps[i]
		switch st.kind {
		case stepDrop:
			dests[i] = &sink
		case stepDirect:
			fv := fieldByPathAlloc(root, st.fpath)
			dests[i] = fv.Addr().Interface()
		case stepIndirect:
			tmp := reflect.New(st.convTo).Elem()
			fp := append([]int(nil), st.fpath...) // small copy
			post := st.post
			dests[i] = tmp.Addr().Interface()
			finals = append(finals, func() error {
				dst := fieldByPathAlloc(root, fp)
				return post(dst, tmp)
			})
		default:
			dests[i] = &sink
		}
	}

	cleanup := func() error {
		for _, f := range finals {
			if err := f(); err != nil {
				return err
			}
		}
		return nil
	}
	return dests, cleanup, nil
}

// ---------------- Struct indexing & tags ----------------

func buildStructIndex(rt reflect.Type) fieldIndex {
	idx := fieldIndex{byName: make(map[string][]int)}
	seen := make(map[string]struct{})

	var walk func(t reflect.Type, base []int, forceInline bool)
	walk = func(t reflect.Type, base []int, forceInline bool) {
		t = derefPtr(t)
		if t.Kind() != reflect.Struct {
			return
		}
		n := t.NumField()
		for i := 0; i < n; i++ {
			sf := t.Field(i)
			if sf.PkgPath != "" && !sf.Anonymous { // unexported, non-anonymous
				continue
			}
			tag := sf.Tag.Get("db")
			name, inline, omit := parseTag(tag)
			if omit {
				continue
			}
			ft := sf.Type
			path := append(append([]int(nil), base...), i)

			if inline || (sf.Anonymous && (forceInline || tag == "")) {
				if isStruct(ft) || (ft.Kind() == reflect.Ptr && isStruct(ft.Elem())) {
					walk(ft, path, inline)
					continue
				}
			}
			if name == "" {
				name = sf.Name
			}
			lc := toLowerAscii(name)
			if _, ok := seen[lc]; !ok {
				idx.byName[lc] = path
				seen[lc] = struct{}{}
			}
		}
	}
	walk(rt, nil, false)
	return idx
}

// parseTag supports: "-", "col", ",inline", "col,inline", "inline,col".
func parseTag(tag string) (name string, inline bool, omit bool) {
	if tag == "-" {
		return "", false, true
	}
	if tag == "" {
		return "", false, false
	}
	start := 0
	for i := 0; i <= len(tag); i++ {
		if i == len(tag) || tag[i] == ',' {
			part := tag[start:i]
			if part == "inline" {
				inline = true
			} else if part != "" && name == "" {
				name = part
			}
			start = i + 1
		}
	}
	return name, inline, false
}

// ---------------- Step construction ----------------

func makeFieldStep(rootType reflect.Type, fpath []int) (step, error) {
	ft := fieldTypeByPath(rootType, fpath)

	// 1) Field provides its own Scanner.
	if implementsScanner(ft) {
		return step{kind: stepDirect, fpath: fpath}, nil
	}
	// 2) Prefer known safe indirects (e.g., []byte->string, int64->int32, custom underlying types).
	if convTo, post, ok := pickIndirect(ft); ok {
		return step{kind: stepIndirect, fpath: fpath, convTo: convTo, post: post}, nil
	}
	// 3) Otherwise, let database/sql scan directly.
	if isDirectlyScannable(ft) {
		return step{kind: stepDirect, fpath: fpath}, nil
	}
	// 4) Fallback direct (database/sql may still convert).
	return step{kind: stepDirect, fpath: fpath}, nil
}

func makeWholeStep(t reflect.Type) (step, error) {
	// 1) Prefer known safe indirects for primitives and custom underlying types.
	if convTo, post, ok := pickIndirect(t); ok {
		return step{kind: stepIndirect, convTo: convTo, post: post}, nil
	}
	// 2) Otherwise, direct.
	if isDirectlyScannable(t) {
		return step{kind: stepDirect}, nil
	}
	// 3) Fallback direct.
	return step{kind: stepDirect}, nil
}

// ---------------- Type/convert helpers ----------------

func isStruct(t reflect.Type) bool { return derefPtr(t).Kind() == reflect.Struct }

func derefPtr(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}

func implementsScanner(t reflect.Type) bool {
	scanner := reflect.TypeOf((*sql.Scanner)(nil)).Elem()
	return reflect.PointerTo(t).Implements(scanner)
}

func isDirectlyScannable(t reflect.Type) bool {
	t = derefPtr(t)
	switch t.Kind() {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.String:
		return true
	case reflect.Slice:
		return t.Elem().Kind() == reflect.Uint8 // []byte
	}
	return t == reflect.TypeOf(time.Time{}) || t == reflect.TypeOf(sql.RawBytes{})
}

// pickIndirect returns a temporary scan type and a post-assignment function
// that converts from the temporary into dstType.
// It covers:
//   - []byte -> string (builtin string only)
//   - numeric widenings for builtin primitives (int*/uint*/float*)
//   - custom named types based on primitives
//   - named types whose underlying type is a pointer to a primitive (one or more layers)
func pickIndirect(dstType reflect.Type) (reflect.Type, func(dst, src reflect.Value) error, bool) {
	// Keep the original (possibly pointer) type so we can rebuild pointer layers.
	dt := dstType

	// Base is dstType with pointers removed for the builtin checks.
	base := derefPtr(dstType)

	// []byte -> string ONLY for builtin string (not named string).
	if base == reflect.TypeOf("") && dt.Kind() != reflect.Ptr {
		tmp := reflect.TypeOf([]byte(nil))
		return tmp, func(dst, src reflect.Value) error {
			if src.IsNil() {
				dst.SetString("")
				return nil
			}
			dst.SetString(string(src.Bytes()))
			return nil
		}, true
	}

	// Builtin numeric widenings (non-pointer, non-named)
	if dt == base {
		switch base.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			tmp := reflect.TypeOf(int64(0))
			return tmp, func(dst, src reflect.Value) error {
				dst.SetInt(src.Int())
				return nil
			}, true
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			tmp := reflect.TypeOf(uint64(0))
			return tmp, func(dst, src reflect.Value) error {
				dst.SetUint(src.Uint())
				return nil
			}, true
		case reflect.Float32, reflect.Float64:
			tmp := reflect.TypeOf(float64(0))
			return tmp, func(dst, src reflect.Value) error {
				dst.SetFloat(src.Float())
				return nil
			}, true
		}
	}

	// Custom/named types, including named-pointer-to-primitive.
	// Peel pointer layers from dt (not from base) so we can rebuild them later.
	under := dt
	ptrCount := 0
	for under.Kind() == reflect.Ptr {
		under = under.Elem()
		ptrCount++
	}

	// If the ultimate underlying is not a struct, handle primitive kinds.
	if under.Kind() != reflect.Struct {
		switch under.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			tmp := reflect.TypeOf(int64(0))
			return tmp, func(dst, src reflect.Value) error {
				val := reflect.New(under).Elem()
				val.SetInt(src.Int())
				return assignWithPointers(dst, val, dt, ptrCount)
			}, true
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			tmp := reflect.TypeOf(uint64(0))
			return tmp, func(dst, src reflect.Value) error {
				val := reflect.New(under).Elem()
				val.SetUint(src.Uint())
				return assignWithPointers(dst, val, dt, ptrCount)
			}, true
		case reflect.Float32, reflect.Float64:
			tmp := reflect.TypeOf(float64(0))
			return tmp, func(dst, src reflect.Value) error {
				val := reflect.New(under).Elem()
				val.SetFloat(src.Float())
				return assignWithPointers(dst, val, dt, ptrCount)
			}, true
		case reflect.String:
			tmp := reflect.TypeOf("")
			return tmp, func(dst, src reflect.Value) error {
				val := reflect.New(under).Elem()
				val.SetString(src.String())
				return assignWithPointers(dst, val, dt, ptrCount)
			}, true
		}
	}

	return nil, nil, false
}

// assignWithPointers converts 'val' to the destination type 'dt',
// re-applying 'ptrCount' pointer layers if needed before Convert.
//
// If ptrCount == 0: dst.Set(val.Convert(dt))
// If ptrCount >= 1:  take address ptrCount times before converting.
func assignWithPointers(dst, val reflect.Value, dt reflect.Type, ptrCount int) error {
	if ptrCount <= 0 {
		dst.Set(val.Convert(dt))
		return nil
	}
	// First pointer layer.
	cur := val.Addr()
	// Additional pointer layers (rare, but supported).
	for i := 1; i < ptrCount; i++ {
		tmp := reflect.New(cur.Type())
		tmp.Elem().Set(cur)
		cur = tmp
	}
	dst.Set(cur.Convert(dt))
	return nil
}

func fieldTypeByPath(root reflect.Type, fpath []int) reflect.Type {
	t := root
	for _, i := range fpath {
		t = derefPtr(t)
		t = t.Field(i).Type
	}
	return t
}

// fieldByPathAlloc walks fpath, allocating nil pointers so the final field is addressable.
func fieldByPathAlloc(root reflect.Value, fpath []int) reflect.Value {
	v := root
	for _, i := range fpath {
		if v.Kind() == reflect.Ptr {
			if v.IsNil() {
				v.Set(reflect.New(v.Type().Elem()))
			}
			v = v.Elem()
		}
		v = v.Field(i)
	}
	if v.Kind() == reflect.Ptr && v.IsNil() {
		v.Set(reflect.New(v.Type().Elem()))
	}
	return v
}

// ---------------- Column normalization (ASCII fast-path) ----------------

func normalizeColAscii(s string) string {
	if l := len(s); l >= 2 {
		switch s[0] {
		case '"':
			if s[l-1] == '"' {
				s = s[1 : l-1]
			}
		case '`':
			if s[l-1] == '`' {
				s = s[1 : l-1]
			}
		case '[':
			if s[l-1] == ']' {
				s = s[1 : l-1]
			}
		}
	}
	return toLowerAscii(s)
}

func toLowerAscii(s string) string {
	var need bool
	for i := 0; i < len(s); i++ {
		c := s[i]
		if 'A' <= c && c <= 'Z' {
			need = true
			break
		}
	}
	if !need {
		return s
	}
	b := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if 'A' <= c && c <= 'Z' {
			c = c + ('a' - 'A')
		}
		b[i] = c
	}
	return string(b)
}
