package sqlx

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"
)

// reflect.go contains extensions to reflect that make it easy to deal with
// structs with reflect.

// commonly used reflect types.
var (
	scannerIface = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
	valuerIface  = reflect.TypeOf((*driver.Valuer)(nil)).Elem()
	timeType     = reflect.TypeOf(time.Time{})
)

// deref will dereference t until it is of kind k
func deref(t reflect.Type, k reflect.Kind) (reflect.Type, error) {
	for {
		switch t.Kind() {
		case reflect.Ptr:
			t = t.Elem()
			continue
		case k:
			return t, nil
		default:
			return nil, fmt.Errorf("destination must be %s", k)
		}
	}
}

// BaseSliceType returns the type for a slice, dereferencing it if it is a pointer.
// Returns an error if the destination is not a slice or a pointer to a slice.
func BaseSliceType(t reflect.Type) (reflect.Type, error) {
	return deref(t, reflect.Slice)
}

// BaseStructType returns the type of a struct, dereferencing it if it is a pointer.
// Returns an error if the destination is not a struct or a pointer to a struct.
func BaseStructType(t reflect.Type) (reflect.Type, error) {
	return deref(t, reflect.Struct)
}

// an fmc is a cache of fieldmaps by reflect type to avoid having to do the
// costly traversal of a fieldmap each time.
type fmc struct {
	cache map[reflect.Type]fieldMap
	sync.RWMutex
}

var cache = fmc{cache: map[reflect.Type]fieldMap{}}

func getFieldMap(t reflect.Type) (fieldMap, error) {
	t, err := BaseStructType(t)
	if err != nil {
		return nil, err
	}
	cache.RLock()
	fm, ok := cache.cache[t]
	cache.RUnlock()
	if ok {
		return fm, nil
	}

	fm, err = newFieldMap(t)
	if err != nil {
		return nil, err
	}
	cache.Lock()
	cache.cache[t] = fm
	cache.Unlock()

	return fm, nil
}

// A FieldMap maintains mappings which allow it quick access to struct fields in
// a way that is consistent with the Go compiler's name resolution, eg. with
// respect to embedded struct fields.
type fieldMap map[string]int

// newFieldMap creates a new fieldMap based on t.  A fieldMap is a map of field
// names to an integer, where the integer is its breadth first position in its
// tree of embedded types.
func newFieldMap(t reflect.Type) (fieldMap, error) {
	t, err := BaseStructType(t)
	if err != nil {
		return nil, err
	}

	fm := fieldMap{}

	// maintain a queue of embedded types to descend into
	queue := []reflect.Type{t}

	for pos := 0; len(queue) != 0; {
		// pop the first item off of the queue
		ty := queue[0]
		queue = queue[1:]
		// iterate through all of its fields
		for fieldPos := 0; fieldPos < ty.NumField(); fieldPos++ {
			f := ty.Field(fieldPos)
			ft := f.Type

			// skip unexported fields
			if len(f.PkgPath) != 0 {
				continue
			}

			// perform one level of indirection for pointers, so that checking if a
			// type implements an interface is consistent across *Type & Type.
			if ft.Kind() == reflect.Ptr {
				ft = ft.Elem()
			}

			// if this field is a struct, it doesn't implement scanner, and it's not a Time,
			// throw it on the queue to be descended into.
			if ft.Kind() == reflect.Struct && !reflect.PtrTo(ft).Implements(scannerIface) && ft != timeType {
				queue = append(queue, ft)
			} else {
				// otherwise, figure out its name
				name := NameMapper(f.Name)
				if tag := f.Tag.Get("db"); tag != "" {
					name = tag
				}
				// if the name is shadowed by an earlier identical name in the search, skip it
				if _, ok := fm[name]; ok {
					continue
				}
				// if the name is "-", skip it
				if name == "-" {
					continue
				}
				// add it to the map at the current position
				fm[name] = pos
				pos++
			}
		}
	}

	return fm, nil
}

// getFieldIndexes returns a list of indexes corresponding to names from the
// fieldmap.  If a name can't be found, it gets a -1 associated with it.
func (f fieldMap) getFieldIndexes(names []string, unsafe bool) ([]int, error) {
	fields := make([]int, len(names))

	for i, name := range names {
		// find that name in the struct
		num, ok := f[name]
		if !ok && unsafe {
			fields[i] = -1
		} else if !ok {
			return fields, fmt.Errorf("Could not find name %s in destination.", name)
		} else {
			fields[i] = num
		}
	}
	return fields, nil
}

// getValues fills values with the interface{} from v for fields corresponding
// to indexes.  If v is addressable, these are pointers, otherwise they are just
// copies.
func (f fieldMap) getValues(v reflect.Value, indexes []int, values []interface{}, unsafe bool) error {
	all := f.allValues(v)
	for i, index := range indexes {
		if index >= 0 {
			values[i] = all[index]
		} else if unsafe {
			values[i] = new(interface{})
		} else {
			return errors.New("Unsafe not set but unknown field found.")
		}
	}
	return nil
}

// allValues fetches all field values from a struct value.  These values are in
// field order, as they use the same breadth-first search as the fieldmap.
//
// This code is shared between code which sets values (like rows.Scan) and code
// that reads values (named query support).  If v is addressable, we return
// pointers which are settable, but if it isn't, then we return copies.
func (f fieldMap) allValues(v reflect.Value) []interface{} {

	// as before, we use a queue, but this time of reflect.Value
	queue := []reflect.Value{v}
	values := make([]interface{}, len(f))

	// since we're building a list instead of a mapping, we have to have a
	// way to implement the Go selection shadowing properly, so more-nested
	// names don't clobber less-nested ones.
	seen := struct{}{}
	encountered := map[string]struct{}{}

	var isPtr, isScanner, isValuer bool
	returnAddrs := v.CanAddr()

	for pos := 0; len(queue) != 0; {
		va := queue[0]
		queue = queue[1:]
		for fieldPos := 0; fieldPos < va.NumField(); fieldPos++ {
			// fieldVal, fieldType, structFieldType
			fv := va.Field(fieldPos)
			ft := fv.Type()
			sft := va.Type().Field(fieldPos)
			isPtr, isScanner = false, false

			// skip unexported fields
			if len(sft.PkgPath) != 0 {
				continue
			}

			// skip duplicate names in the struct tree
			if _, ok := encountered[sft.Name]; ok {
				continue
			}

			// skip fields with the db tag set to "-"
			if tag := sft.Tag.Get("db"); tag == "-" {
				continue
			}

			encountered[sft.Name] = seen

			if ft.Kind() == reflect.Ptr {
				ft = ft.Elem()
				isPtr = true
			}

			if isPtr || !returnAddrs {
				_, isScanner = fv.Interface().(sql.Scanner)
				_, isValuer = fv.Interface().(driver.Valuer)
			} else {
				_, isScanner = fv.Addr().Interface().(sql.Scanner)
				_, isValuer = fv.Addr().Interface().(driver.Valuer)
			}

			// if the field is a struct but not a scanner, valuer, or timeType, then we
			// will descend into it
			if ft.Kind() == reflect.Struct && !isScanner && !isValuer && ft != timeType {
				// if this is a pointer, it's possible it is nil, so just allocate a new one
				val := fv
				if isPtr {
					alloc := reflect.New(ft)
					fv.Set(alloc)
					val = reflect.Indirect(fv)
				}
				// descend into the struct to handle embeds
				queue = append(queue, val)
			} else {
				if !returnAddrs {
					values[pos] = fv.Interface()
				} else if returnAddrs {
					values[pos] = fv.Addr().Interface()
				}
				pos++
			}
		}
	}
	return values
}
