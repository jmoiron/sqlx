// Package reflect implements extensions to the standard reflect lib suitable
// for implementing marshaling and unmarshaling packages.  The main Mapper type
// allows for Go-compatible named atribute access, including accessing embedded
// struct attributes and the ability to use  functions and struct tags to
// customize field names.
//
package reflect

import "sync"

import (
	"reflect"
	"runtime"
)

type fieldMap map[string][]int

// mustBe checks a value against a kind, panicing with a reflect.ValueError
// if the kind isn't that which is required.
func mustBe(v reflect.Value, expected reflect.Kind) {
	k := v.Kind()
	if k != expected {
		panic(&reflect.ValueError{methodName(), k})
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

// Mapper is a general purpose mapper of names to struct fields.  A Mapper
// behaves like most marshallers, optionally obeying a field tag for name
// mapping and a function to provide a basic mapping of fields to names.
type Mapper struct {
	cache   map[reflect.Type]fieldMap
	tagName string
	mapFunc func(string) string
	sync.RWMutex
}

// NewMapper returns a new mapper which optionally obeys the field tag given
// by tagName.  If tagName is the empty string, it is ignored.
func NewMapper(tagName string) *Mapper {
	return &Mapper{
		cache:   make(map[reflect.Type]fieldMap),
		tagName: tagName,
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
	mapping, ok := m.cache[t]
	if !ok {
		mapping = getMapping(t, m.tagName, m.mapFunc)
		m.cache[t] = mapping
	}
	return mapping
}

// Fieldmap returns the mapper's mapping of field names to reflect values.  Panics
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

// FieldByIndexes returns a value for a particular struct traversal.
func FieldByIndexes(v reflect.Value, indexes []int) reflect.Value {
	f := v
	for _, i := range indexes {
		f = f.Field(i)
	}
	return f
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

// getMapping returns a mapping for the t type, using the tagName and the mapFunc
// to determine the canonical names of fields.
func getMapping(t reflect.Type, tagName string, mapFunc func(string) string) fieldMap {
	queue := []typeQueue{}
	queue = append(queue, typeQueue{t, []int{}})
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
				queue = append(queue, typeQueue{f.Type, apnd(tq.p, fieldPos)})
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
