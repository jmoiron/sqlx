package sqlx

import (
	"bytes"
	"errors"
	"reflect"
	"strconv"
)

// Bindvar types supported by Rebind, BindMap and BindStruct.
const (
	UNKNOWN = iota
	QUESTION
	DOLLAR
	NAMED
)

// BindType returns the bindtype for a given database given a drivername.
func BindType(driverName string) int {
	switch driverName {
	case "postgres", "pgx":
		return DOLLAR
	case "mysql":
		return QUESTION
	case "sqlite3":
		return QUESTION
	case "oci8":
		return NAMED
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

	qb := []byte(query)
	// Add space enough for 10 params before we have to allocate
	rqb := make([]byte, 0, len(qb)+10)
	j := 1
	for _, b := range qb {
		if b == '?' {
			switch bindType {
			case DOLLAR:
				rqb = append(rqb, '$')
			case NAMED:
				rqb = append(rqb, ':', 'a', 'r', 'g')
			}
			for _, b := range strconv.Itoa(j) {
				rqb = append(rqb, byte(b))
			}
			j++
		} else {
			rqb = append(rqb, b)
		}
	}
	return string(rqb)
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
	type ra struct {
		v       reflect.Value
		t       reflect.Type
		isSlice bool
	}
	ras := make([]ra, 0, len(args))
	for _, arg := range args {
		v := reflect.ValueOf(arg)
		t, _ := baseType(v.Type(), reflect.Slice)
		ras = append(ras, ra{v, t, t != nil})
	}

	anySlices := false
	for _, s := range ras {
		if s.isSlice {
			anySlices = true
			if s.v.Len() == 0 {
				return "", nil, errors.New("empty slice passed to 'in' query")
			}
		}
	}

	// don't do any parsing if there aren't any slices;  note that this means
	// some errors that we might have caught below will not be returned.
	if !anySlices {
		return query, args, nil
	}

	var a []interface{}
	var buf bytes.Buffer
	var pos int

	for _, r := range query {
		if r == '?' {
			if pos >= len(ras) {
				// if this argument wasn't passed, lets return an error;  this is
				// not actually how database/sql Exec/Query works, but since we are
				// creating an argument list programmatically, we want to be able
				// to catch these programmer errors earlier.
				return "", nil, errors.New("number of bindVars exceeds arguments")
			} else if ras[pos].isSlice {
				// if this argument is a slice, expand the slice into arguments and
				// assume that the bindVars should be comma separated.
				length := ras[pos].v.Len()
				for i := 0; i < length-1; i++ {
					buf.Write([]byte("?, "))
					a = append(a, ras[pos].v.Index(i).Interface())
				}
				a = append(a, ras[pos].v.Index(length-1).Interface())
				buf.WriteRune('?')
			} else {
				// a normal argument, procede as normal.
				a = append(a, args[pos])
				buf.WriteRune(r)
			}
			pos++
		} else {
			buf.WriteRune(r)
		}
	}

	if pos != len(ras) {
		return "", nil, errors.New("number of bindVars less than number arguments")
	}

	return buf.String(), a, nil
}
