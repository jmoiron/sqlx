package reflect

import (
	"fmt"
	"sync"
)

import "reflect"

type fieldMap map[string][]int

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

func (m *Mapper) MapType(t reflect.Type) fieldMap {
	mapping, ok := m.cache[t]
	if !ok {
		mapping = getMapping(t, m.tagName, m.mapFunc)
		m.cache[t] = mapping
	}
	return mapping
}

func fieldByIndexes(v reflect.Value, indexes []int) reflect.Value {
	f := v
	for _, i := range indexes {
		f = f.Field(i)
	}
	return f
}

func (m *Mapper) FieldMap(v reflect.Value) map[string]reflect.Value {
	r := map[string]reflect.Value{}
	nm := m.MapType(v.Type())
	fmt.Println(nm)
	for tagName, indexes := range nm {
		r[tagName] = fieldByIndexes(v, indexes)
	}
	return r
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
				name = mapFunc(f.Name)
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

func noop(s string) string { return s }

func FieldByName(i interface{}, name string) reflect.Value {
	t := reflect.TypeOf(i)
	m := getMapping(t, "db", noop)
	v := reflect.ValueOf(i)
	return fieldByIndexes(v, m[name])
}
