package sqlx

import (
	"bytes"
	"database/sql/driver"
	"errors"
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
	case "oci8", "ora", "goracle":
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

	rebound := bytes.Buffer{}
	currentVar := 1
	byteOffset := 0

	lex := lexSQL(query)
	for tok := range lex.items {
		if tok.typ == itemIdentifier && tok.val == "?" {
			rebound.WriteString(query[byteOffset:tok.pos])
			switch bindType {
			case DOLLAR:
				rebound.WriteByte('$')
			case NAMED:
				rebound.WriteString(":arg")
			case AT:
				rebound.WriteString("@p")
			}
			rebound.WriteString(strconv.Itoa(currentVar))
			currentVar++
			byteOffset = tok.pos + len(tok.val)
		}
	}
	if byteOffset == 0 {
		return query
	}
	rebound.WriteString(query[byteOffset:])
	return rebound.String()
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

// In expands slice values in args, returning the modified query string
// and a new arg list that can be executed by a database. The `query` should
// use the `?` bindVar.  The return value uses the `?` bindVar.
func In(query string, args ...interface{}) (string, []interface{}, error) {
	// argMeta stores reflect.Value and length for slices and
	// the value itself for non-slice arguments
	type argMeta struct {
		v      reflect.Value
		i      interface{}
		length int
	}

	var flatArgsCount int
	var anySlices bool

	meta := make([]argMeta, len(args))

	for i, arg := range args {
		if a, ok := arg.(driver.Valuer); ok {
			arg, _ = a.Value()
		}
		v := reflect.ValueOf(arg)
		t := reflectx.Deref(v.Type())

		// []byte is a driver.Value type so it should not be expanded
		if t.Kind() == reflect.Slice && t != reflect.TypeOf([]byte{}) {
			meta[i].length = v.Len()
			meta[i].v = v

			anySlices = true
			flatArgsCount += meta[i].length

			if meta[i].length == 0 {
				return "", nil, errors.New("empty slice passed to 'in' query")
			}
		} else {
			meta[i].i = arg
			flatArgsCount++
		}
	}

	// don't do any parsing if there aren't any slices;  note that this means
	// some errors that we might have caught below will not be returned.
	if !anySlices {
		return query, args, nil
	}

	newArgs := make([]interface{}, 0, flatArgsCount)
	buf := make([]byte, 0, len(query)+len(", ?")*flatArgsCount)

	var arg, offset int

	for i := strings.IndexByte(query[offset:], '?'); i != -1; i = strings.IndexByte(query[offset:], '?') {
		if arg >= len(meta) {
			// if an argument wasn't passed, lets return an error;  this is
			// not actually how database/sql Exec/Query works, but since we are
			// creating an argument list programmatically, we want to be able
			// to catch these programmer errors earlier.
			return "", nil, errors.New("number of bindVars exceeds arguments")
		}

		argMeta := meta[arg]
		arg++

		// not a slice, continue.
		// our questionmark will either be written before the next expansion
		// of a slice or after the loop when writing the rest of the query
		if argMeta.length == 0 {
			offset = offset + i + 1
			newArgs = append(newArgs, argMeta.i)
			continue
		}

		// write everything up to and including our ? character
		buf = append(buf, query[:offset+i+1]...)

		for si := 1; si < argMeta.length; si++ {
			buf = append(buf, ", ?"...)
		}

		newArgs = appendReflectSlice(newArgs, argMeta.v, argMeta.length)

		// slice the query and reset the offset. this avoids some bookkeeping for
		// the write after the loop
		query = query[offset+i+1:]
		offset = 0
	}

	buf = append(buf, query...)

	if arg < len(meta) {
		return "", nil, errors.New("number of bindVars less than number arguments")
	}

	return string(buf), newArgs, nil
}

func appendReflectSlice(args []interface{}, v reflect.Value, vlen int) []interface{} {
	switch val := v.Interface().(type) {
	case []interface{}:
		args = append(args, val...)
	case []int:
		for i := range val {
			args = append(args, val[i])
		}
	case []string:
		for i := range val {
			args = append(args, val[i])
		}
	default:
		for si := 0; si < vlen; si++ {
			args = append(args, v.Index(si).Interface())
		}
	}

	return args
}
