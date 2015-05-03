// Package reflectx implements extensions to the standard reflect lib suitable
// for implementing marshaling and unmarshaling packages.  The main Mapper type
// allows for Go-compatible named atribute access, including accessing embedded
// struct attributes and the ability to use  functions and struct tags to
// customize field names.
//
package reflectx

import "sync"

import (
	"reflect"
	"runtime"
)

type fieldMap map[string][]int

// Mapper is a general purpose mapper of names to struct fields.  A Mapper
// behaves like most marshallers, optionally obeying a field tag for name
// mapping and a function to provide a basic mapping of fields to names.
type Mapper struct {
	cache      map[reflect.Type]fieldMap
	tagName    string
	tagMapFunc func(string) string
	mapFunc    func(string) string
	mutex      sync.Mutex
}

// NewMapper returns a new mapper which optionally obeys the field tag given
// by tagName.  If tagName is the empty string, it is ignored.
func NewMapper(tagName string) *Mapper {
	return &Mapper{
		cache:   make(map[reflect.Type]fieldMap),
		tagName: tagName,
	}
}

// NewMapperTagFunc returns a new mapper which contains a mapper for field names
// AND a mapper for tag values.  This is useful for tags like json which can
// have values like "name,omitempty".
func NewMapperTagFunc(tagName string, mapFunc, tagMapFunc func(string) string) *Mapper {
	return &Mapper{
		cache:      make(map[reflect.Type]fieldMap),
		tagName:    tagName,
		mapFunc:    mapFunc,
		tagMapFunc: tagMapFunc,
	}
}

// NewMapperFunc returns a new mapper which optionally obeys a field tag and
// a struct field name mapper func given by f.  Tags will take precedence, but
// for any other field, the mapped name will be f(field.Name)
func NewMapperFunc(tagName string, f func(string) string) *Mapper {
	return &Mapper{
		cache:   make(map[reflect.Type]fieldMap),
		tagName: tagName,
		mapFunc: f,
	}
}

// TypeMap returns a mapping of field strings to int slices representing
// the traversal down the struct to reach the field.
func (m *Mapper) TypeMap(t reflect.Type) fieldMap {
	m.mutex.Lock()
	mapping, ok := m.cache[t]
	if !ok {
		mapping = getMapping(t, m.tagName, m.mapFunc, m.tagMapFunc)
		m.cache[t] = mapping
	}
	m.mutex.Unlock()
	return mapping
}

// FieldMap returns the mapper's mapping of field names to reflect values.  Panics
// if v's Kind is not Struct, or v is not Indirectable to a struct kind.
func (m *Mapper) FieldMap(v reflect.Value) map[string]reflect.Value {
	v = reflect.Indirect(v)
	mustBe(v, reflect.Struct)

	r := map[string]reflect.Value{}
	nm := m.TypeMap(v.Type())
	for tagName, indexes := range nm {
		r[tagName] = FieldByIndexes(v, indexes)
	}
	return r
}

// FieldByName returns a field by the its mapped name as a reflect.Value.
// Panics if v's Kind is not Struct or v is not Indirectable to a struct Kind.
// Returns zero Value if the name is not found.
func (m *Mapper) FieldByName(v reflect.Value, name string) reflect.Value {
	v = reflect.Indirect(v)
	mustBe(v, reflect.Struct)

	nm := m.TypeMap(v.Type())
	traversal, ok := nm[name]
	if !ok {
		return *new(reflect.Value)
	}
	return FieldByIndexes(v, traversal)
}

// FieldsByName returns a slice of values corresponding to the slice of names
// for the value.  Panics if v's Kind is not Struct or v is not Indirectable
// to a struct Kind.  Returns zero Value for each name not found.
func (m *Mapper) FieldsByName(v reflect.Value, names []string) []reflect.Value {
	v = reflect.Indirect(v)
	mustBe(v, reflect.Struct)

	nm := m.TypeMap(v.Type())

	vals := make([]reflect.Value, 0, len(names))
	for _, name := range names {
		traversal, ok := nm[name]
		if !ok {
			vals = append(vals, *new(reflect.Value))
		} else {
			vals = append(vals, FieldByIndexes(v, traversal))
		}
	}
	return vals
}

// TraversalsByName returns a slice of int slices which represent the struct
// traversals for each mapped name.  Panics if t is not a struct or Indirectable
// to a struct.  Returns empty int slice for each name not found.
func (m *Mapper) TraversalsByName(t reflect.Type, names []string) [][]int {
	t = Deref(t)
	mustBe(t, reflect.Struct)
	nm := m.TypeMap(t)

	r := make([][]int, 0, len(names))
	for _, name := range names {
		traversal, ok := nm[name]
		if !ok {
			r = append(r, []int{})
		} else {
			r = append(r, traversal)
		}
	}
	return r
}

// FieldByIndexes returns a value for a particular struct traversal.
func FieldByIndexes(v reflect.Value, indexes []int) reflect.Value {
	for _, i := range indexes {
		v = reflect.Indirect(v).Field(i)
		// if this is a pointer, it's possible it is nil
		if v.Kind() == reflect.Ptr && v.IsNil() {
			alloc := reflect.New(Deref(v.Type()))
			v.Set(alloc)
		}
		if v.Kind() == reflect.Map && v.IsNil() {
			v.Set(reflect.MakeMap(v.Type()))
		}
	}
	return v
}

// FieldByIndexesReadOnly returns a value for a particular struct traversal,
// but is not concerned with allocating nil pointers because the value is
// going to be used for reading and not setting.
func FieldByIndexesReadOnly(v reflect.Value, indexes []int) reflect.Value {
	for _, i := range indexes {
		v = reflect.Indirect(v).Field(i)
	}
	return v
}

// Deref is Indirect for reflect.Types
func Deref(t reflect.Type) reflect.Type {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}

// -- helpers & utilities --

type kinder interface {
	Kind() reflect.Kind
}

// mustBe checks a value against a kind, panicing with a reflect.ValueError
// if the kind isn't that which is required.
func mustBe(v kinder, expected reflect.Kind) {
	k := v.Kind()
	if k != expected {
		panic(&reflect.ValueError{Method: methodName(), Kind: k})
	}
}

// methodName is returns the caller of the function calling methodName
func methodName() string {
	pc, _, _, _ := runtime.Caller(2)
	f := runtime.FuncForPC(pc)
	if f == nil {
		return "unknown method"
	}
	return f.Name()
}

type typeQueue struct {
	t reflect.Type
	p []int
}

// A copying append that creates a new slice each time.
func apnd(is []int, i int) []int {
	x := make([]int, len(is)+1)
	for p, n := range is {
		x[p] = n
	}
	x[len(x)-1] = i
	return x
}

// getMapping returns a mapping for the t type, using the tagName, mapFunc and
// tagMapFunc to determine the canonical names of fields.
func getMapping(t reflect.Type, tagName string, mapFunc, tagMapFunc func(string) string) fieldMap {
	queue := []typeQueue{}
	queue = append(queue, typeQueue{Deref(t), []int{}})
	m := fieldMap{}
	for len(queue) != 0 {
		// pop the first item off of the queue
		tq := queue[0]
		queue = queue[1:]
		// iterate through all of its fields
		for fieldPos := 0; fieldPos < tq.t.NumField(); fieldPos++ {
			f := tq.t.Field(fieldPos)

			name := f.Tag.Get(tagName)
			if len(name) == 0 {
				if mapFunc != nil {
					name = mapFunc(f.Name)
				} else {
					name = f.Name
				}
			} else if tagMapFunc != nil {
				name = tagMapFunc(name)
			}

			// if the name is "-", disabled via a tag, skip it
			if name == "-" {
				continue
			}

			// skip unexported fields
			if len(f.PkgPath) != 0 {
				continue
			}

			// bfs search of anonymous embedded structs
			if f.Anonymous {
				queue = append(queue, typeQueue{Deref(f.Type), apnd(tq.p, fieldPos)})
				continue
			}

			// if the name is shadowed by an earlier identical name in the search, skip it
			if _, ok := m[name]; ok {
				continue
			}
			// add it to the map at the current position
			m[name] = apnd(tq.p, fieldPos)
		}
	}
	return m
}
