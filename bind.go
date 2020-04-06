package sqlx

import (
	"bytes"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx/reflectx"
)

// Bindvar types supported by Rebind, BindMap and BindStruct.
const (
	UNKNOWN = iota
	QUESTION
	DOLLAR
	NAMED
	AT
)

// BindType returns the bindtype for a given database given a drivername.
func BindType(driverName string) int {
	switch driverName {
	case "postgres", "pgx", "pq-timeouts", "cloudsqlpostgres", "ql":
		return DOLLAR
	case "mysql":
		return QUESTION
	case "sqlite3":
		return QUESTION
	case "oci8", "ora", "goracle", "godror":
		return NAMED
	case "sqlserver":
		return AT
	}
	return UNKNOWN
}

// FIXME: this should be able to be tolerant of escaped ?'s in queries without
// losing much speed, and should be to avoid confusion.

// Rebind a query from the default bindtype (QUESTION) to the target bindtype.
func Rebind(bindType int, query string) string {
	switch bindType {
	case QUESTION, UNKNOWN:
		return query
	}

	// Add space enough for 10 params before we have to allocate
	rqb := make([]byte, 0, len(query)+10)

	var i, j int

	for i = strings.Index(query, "?"); i != -1; i = strings.Index(query, "?") {
		rqb = append(rqb, query[:i]...)

		switch bindType {
		case DOLLAR:
			rqb = append(rqb, '$')
		case NAMED:
			rqb = append(rqb, ':', 'a', 'r', 'g')
		case AT:
			rqb = append(rqb, '@', 'p')
		}

		j++
		rqb = strconv.AppendInt(rqb, int64(j), 10)

		query = query[i+1:]
	}

	return string(append(rqb, query...))
}

// Experimental implementation of Rebind which uses a bytes.Buffer.  The code is
// much simpler and should be more resistant to odd unicode, but it is twice as
// slow.  Kept here for benchmarking purposes and to possibly replace Rebind if
// problems arise with its somewhat naive handling of unicode.
func rebindBuff(bindType int, query string) string {
	if bindType != DOLLAR {
		return query
	}

	b := make([]byte, 0, len(query))
	rqb := bytes.NewBuffer(b)
	j := 1
	for _, r := range query {
		if r == '?' {
			rqb.WriteRune('$')
			rqb.WriteString(strconv.Itoa(j))
			j++
		} else {
			rqb.WriteRune(r)
		}
	}

	return rqb.String()
}

func asSliceForIn(i interface{}) (v reflect.Value, ok bool) {
	if i == nil {
		return reflect.Value{}, false
	}

	v = reflect.ValueOf(i)
	t := reflectx.Deref(v.Type())

	// Only expand slices
	if t.Kind() != reflect.Slice {
		return reflect.Value{}, false
	}

	// []byte is a driver.Value type so it should not be expanded
	if t == reflect.TypeOf([]byte{}) {
		return reflect.Value{}, false

	}

	return v, true
}

// In expands slice values in args, returning the modified query string
// and a new arg list that can be executed by a database. The `query` should
// use the `?` or `$n` bindVar.  The return values uses the `$n` bindVar if slices was found.
func In(query string, args ...interface{}) (string, []interface{}, error) {
	// argMeta stores reflect.Value and length for slices and
	// the value itself for non-slice arguments
	type argMeta struct {
		v      reflect.Value
		i      interface{}
		length int
		from   int
	}

	var flatArgsCount int
	var anySlices bool

	meta := make([]argMeta, len(args))

	for i, arg := range args {
		if a, ok := arg.(driver.Valuer); ok {
			aVal := reflect.ValueOf(a)
			switch aVal.Kind() {
			case reflect.Ptr:
				if aVal.IsNil() {
					arg = nil
				} else {
					arg, _ = a.Value()
				}
			default:
				arg, _ = a.Value()
			}
		}

		isSlice := false
		v := reflect.ValueOf(arg)
		if arg != nil {
			t := reflectx.Deref(v.Type())
			// []byte is a driver.Value type so it should not be expanded
			isSlice = t.Kind() == reflect.Slice && t != reflect.TypeOf([]byte{})
		}
		if isSlice {
			vlen := v.Len()
			meta[i].length = vlen
			meta[i].v = v

			anySlices = true
			meta[i].from = flatArgsCount + 1
			flatArgsCount += vlen

			if vlen == 0 {
				return "", nil, errors.New("empty slice passed to 'in' query")
			}
		} else {
			meta[i].i = arg
			meta[i].from = flatArgsCount + 1
			flatArgsCount++
		}
	}

	// don't do any parsing if there aren't any slices;  note that this means
	// some errors that we might have caught below will not be returned.
	if !anySlices {
		return query, args, nil
	}

	newArgs := make([]interface{}, flatArgsCount)
	buf := make([]byte, 0, len(query)+3*flatArgsCount)

	var arg, offset int

	for i := strings.IndexAny(query, "?$"); i != -1; i = strings.IndexAny(query, "?$") {
		if arg >= len(meta) {
			// if an argument wasn't passed, lets return an error;  this is
			// not actually how database/sql Exec/Query works, but since we are
			// creating an argument list programmatically, we want to be able
			// to catch these programmer errors earlier.
			return "", nil, errors.New("number of bindVars exceeds arguments")
		}
		offset = 0

		var argM argMeta
		if query[i] == '?' {
			if i+1 < len(query) && query[i+1] == '?' {
				// skip ??
				buf = append(buf, query[:i+2]...)
				query = query[i+2:]
				continue
			}
			argM = meta[arg]
			arg++
		} else {
			numa := 0
			for j := i + 1; j < len(query); j++ {
				if c := query[j]; c >= '0' && c <= '9' {
					numa = 10*numa + int(c-'0')
					offset++
				} else {
					break
				}
			}
			if numa > len(meta) {
				// if an argument wasn't passed, lets return an error;  this is
				// not actually how database/sql Exec/Query works, but since we are
				// creating an argument list programmatically, we want to be able
				// to catch these programmer errors earlier.
				return "", nil, fmt.Errorf("argument number '$%d' out of range", numa)
			}
			argM = meta[numa-1]
		}

		// write everything up to and including our ? character
		buf = append(buf, query[:i]...)
		buf = append(buf, '$')
		buf = strconv.AppendInt(buf, int64(argM.from), 10)

		if argM.length > 0 {
			for si := 1; si < argM.length; si++ {
				buf = append(buf, ',', '$')
				buf = strconv.AppendInt(buf, int64(argM.from+si), 10)
			}
			putReflectSlice(newArgs, argM.v, argM.length, argM.from-1)
		} else {
			// not a slice
			newArgs[argM.from-1] = argM.i
		}

		// slice the query and reset the offset. this avoids some bookkeeping for
		// the write after the loop
		query = query[offset+i+1:]
	}

	buf = append(buf, query...)

	// if arg < len(meta) {
	// 	return "", nil, errors.New("number of bindVars less than number arguments")
	// }

	return string(buf), newArgs, nil
}

func putReflectSlice(args []interface{}, v reflect.Value, vlen int, toidx int) {
	switch val := v.Interface().(type) {
	case []interface{}:
		for i := range val {
			args[i+toidx] = val[i]
		}
	case []int:
		for i := range val {
			args[i+toidx] = val[i]
		}
	case []string:
		for i := range val {
			args[i+toidx] = val[i]
		}
	default:
		for si := 0; si < vlen; si++ {
			args[si+toidx] = v.Index(si).Interface()
		}
	}
}
