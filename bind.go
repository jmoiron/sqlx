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

// FIXME: this is now produces the wrong results for oracle's NAMED bindtype

// Rebind a query from the default bindtype (QUESTION) to the target bindtype.
func Rebind(bindType int, query string) string {
	if bindType != DOLLAR {
		return query
	}

	qb := []byte(query)
	// Add space enough for 10 params before we have to allocate
	rqb := make([]byte, 0, len(qb)+10)
	j := 1
	for _, b := range qb {
		if b == '?' {
			rqb = append(rqb, '$')
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

// in expands query parms in args, returning the modified query string and
// a new list of args passable to Exec/Query/etc

func in(query string, args ...interface{}) (string, []interface{}, error) {
	// TODO: validate this short circuit as actually saving any time..
	type slice struct {
		v reflect.Value
		t reflect.Type
		l int
	}
	slices := make([]*slice, 0, len(args))
	for _, arg := range args {
		v := reflect.ValueOf(arg)
		t, _ := baseType(v.Type(), reflect.Slice)
		if t != nil {
			slices = append(slices, &slice{v, t, v.Len()})
		} else {
			slices = append(slices, nil)
		}
	}
	numArgs := 0
	anySlices := false
	for _, s := range slices {
		if s != nil {
			anySlices = true
			numArgs += s.l
			if s.l == 0 {
				return "", nil, errors.New("empty slice passed to 'in' query")
			}
		} else {
			numArgs++
		}
	}

	// if there's no slice kind args at all, just return the original query & args
	if !anySlices {
		return query, args, nil
	}

	a := make([]interface{}, 0, numArgs)
	var buf bytes.Buffer
	var pos int
	for _, r := range query {
		if r == '?' {
			// XXX: we have probably done something quite wrong here
			if pos >= len(slices) {
				return "", nil, errors.New("number of bindVars exceeds arguments")
			} else if slices[pos] != nil {
				for i := 0; i < slices[pos].l-1; i++ {
					buf.Write([]byte("?, "))
					a = append(a, slices[pos].v.Index(i).Interface())
				}
				a = append(a, slices[pos].v.Index(slices[pos].l-1).Interface())
				buf.WriteRune('?')
			} else {
				a = append(a, args[pos])
				buf.WriteRune(r)
			}
			pos++
		} else {
			buf.WriteRune(r)
		}
	}

	if pos != len(slices) {
		return "", nil, errors.New("number of bindVars less than number arguments")
	}

	return buf.String(), a, nil
}
