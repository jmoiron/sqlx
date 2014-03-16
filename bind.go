package sqlx

import (
	"bytes"
	"strconv"
)

// Bindvar types supported by sqlx's Rebind & BindMap/Struct functions.
const (
	UNKNOWN = iota
	QUESTION
	DOLLAR
	NAMED
)

// BindType returns the bindtype for a given database given a drivername
func BindType(driverName string) int {
	switch driverName {
	case "postgres":
		return DOLLAR
	case "mysql":
		return QUESTION
	case "sqlite":
		return QUESTION
	case "oci8":
		return NAMED
	}
	return UNKNOWN
}

// FIXME: this should be able to be tolerant of escaped ?'s in queries without
// losing much speed, and should be to avoid confusion.

// FIXME: this is now produces the wrong results for oracle's NAMED bindtype

// Rebind a query from the default bindtype (QUESTION) to the target bindtype
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
