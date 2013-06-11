package sqlx

import (
	"bytes"
	"errors"
	"reflect"
	"strconv"
	"unicode"
)

// Bindvar types supported by sqlx's Rebind & BindMap/Struct functions.
const (
	UNKNOWN = iota
	QUESTION
	DOLLAR
)

// Return the bindtype for a given database given a drivername
func BindType(driverName string) int {
	switch driverName {
	case "postgres":
		return DOLLAR
	case "mysql":
		return QUESTION
	case "sqlite":
		return QUESTION
	}
	return UNKNOWN
}

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

// Bind a named parameter query with fields from a struct argument.  The rules
// for binding field names to parameter names follow the same conventions as
// for StructScan, including obeying the `db` struct tags.
func BindStruct(bindType int, query string, arg interface{}) (string, []interface{}, error) {
	arglist := make([]interface{}, 0, 5)
	t, err := BaseStructType(reflect.TypeOf(arg))
	if err != nil {
		return "", arglist, err
	}

	// resolve this type into a map of fields to field positions
	fm, err := getFieldmap(t)
	if err != nil {
		return "", arglist, err
	}

	argmap := map[string]interface{}{}

	v := reflect.ValueOf(arg)
	for v = reflect.ValueOf(arg); v.Kind() == reflect.Ptr; {
		v = v.Elem()
	}
	for key, val := range fm {
		argmap[key] = v.Field(val).Interface()
	}

	return BindMap(bindType, query, argmap)
}

// Bind a named parameter query with a map of arguments.
func BindMap(bindType int, query string, args map[string]interface{}) (string, []interface{}, error) {
	arglist := make([]interface{}, 0, 5)
	// In all likelihood, the rebound query will be shorter
	qb := []byte(query)
	rebound := make([]byte, 0, len(qb))

	var name []byte
	var sname string
	var val interface{}
	var ok, inName bool
	var err error
	var last, j int

	inName = false
	last = len(qb) - 1
	j = 1

	for i, b := range qb {
		if b == ':' {
			if inName {
				err = errors.New("Unexpected `:` while reading named param at " + strconv.Itoa(i))
				return "", arglist, err
			}
			inName = true
			name = []byte{}
		} else if inName && (unicode.IsLetter(rune(b)) || b == '_') && i != last {
			// append the rune to the name if we are in a name and not on the last rune
			name = append(name, b)
		} else if inName {
			inName = false
			// if this is the final rune of the string and it is part of the name, then
			// make sure to add it to the name
			if i == last && unicode.IsLetter(rune(b)) {
				name = append(name, b)
			}
			sname = string(name)
			val, ok = args[sname]
			if !ok {
				err = errors.New("Could not find name `" + sname + "` in args")
				return "", arglist, err
			}
			// the name has been found and is complete, add to arglist and insert the
			// proper bindvar for the bindType
			arglist = append(arglist, val)
			switch bindType {
			case QUESTION, UNKNOWN:
				rebound = append(rebound, '?')
			case DOLLAR:
				rebound = append(rebound, '$')
				for _, b := range strconv.Itoa(j) {
					rebound = append(rebound, byte(b))
				}
				j++
			}
			// add this rune to string unless if it is not last or if it
			// is last but is not a letter
			if i != last {
				rebound = append(rebound, b)
			} else if !unicode.IsLetter(rune(b)) {
				rebound = append(rebound, b)
			}
		} else {
			rebound = append(rebound, b)
		}
	}
	return string(rebound), arglist, nil
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
