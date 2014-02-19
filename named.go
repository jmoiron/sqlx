package sqlx

// Named Query Support
//
//  * BindStruct, BindMap - bind query bindvars to map/struct args
//	* NamedExec, NamedQuery - named query w/ struct
//  * NamedExecMap, NamedQueryMap - named query w/ maps
//  * NamedStmt - a pre-compiled named query which is a prepared statement
//
// Internal Interfaces:
//
//  * compileNamedQuery - rebind a named query, returning a query and list of names
//
import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"unicode"
)

// NamedStmt is a prepared statement that executes named queries.  Prepare it
// how you would execute a NamedQuery, but pass in a struct (or map for the map
// variants) when you go to execute.
type namedStmt struct {
	Params []string
	Query  string
	Stmt   *Stmt
}

// BindStruct binds a named parameter query with fields from a struct argument.
// The rules for binding field names to parameter names follow the same
// conventions as for StructScan, including obeying the `db` struct tags.
func BindStruct(bindType int, query string, arg interface{}) (string, []interface{}, error) {
	bound, names, err := compileNamedQuery([]byte(query), bindType)
	if err != nil {
		return "", []interface{}{}, err
	}

	arglist := make([]interface{}, 0, len(names))

	t, err := BaseStructType(reflect.TypeOf(arg))
	if err != nil {
		return "", arglist, err
	}

	// resolve this arg's type into a map of fields to field positions
	fm, err := getFieldmap(t)
	if err != nil {
		return "", arglist, err
	}

	// grab the indirected value of arg
	v := reflect.ValueOf(arg)
	for v = reflect.ValueOf(arg); v.Kind() == reflect.Ptr; {
		v = v.Elem()
	}

	values := getValues(v)

	for _, name := range names {
		val, ok := fm[name]
		if !ok {
			return "", arglist, fmt.Errorf("could not find name %s in %v", name, arg)
		}
		arglist = append(arglist, values[val])
	}

	return bound, arglist, nil
}

// BindMap binds a named parameter query with a map of arguments.
func BindMap(bindType int, query string, args map[string]interface{}) (string, []interface{}, error) {
	bound, names, err := compileNamedQuery([]byte(query), bindType)
	if err != nil {
		return "", []interface{}{}, err
	}

	arglist := make([]interface{}, 0, len(names))

	for _, name := range names {
		val, ok := args[name]
		if !ok {
			return "", arglist, fmt.Errorf("could not find name %s in %v", name, args)
		}
		arglist = append(arglist, val)
	}

	return bound, arglist, nil
}

// -- Compilation of Named Queries

// Allow digits and letters in bind params;  additionally runes are
// checked against underscores, meaning that bind params can have be
// alphanumeric with underscores.  Mind the difference between unicode
// digits and numbers, where '5' is a digit but '五' is not.
var allowedBindRunes = []*unicode.RangeTable{unicode.Letter, unicode.Digit}

// FIXME: this function isn't safe for unicode named params, as a failing test
// can testify.  This is not a regression but a failure of the original code
// as well.  It should be modified to range over runes in a string rather than
// bytes, even though this is less convenient and slower.  Hopefully the
// addition of the prepared NamedStmt (which will only do this once) will make
// up for the slightly slower ad-hoc NamedExec/NamedQuery.

// compile a NamedQuery into an unbound query (using the '?' bindvar) and
// a list of names.
func compileNamedQuery(qs []byte, bindType int) (query string, names []string, err error) {
	names = make([]string, 0, 10)
	rebound := make([]byte, 0, len(qs))

	inName := false
	last := len(qs) - 1
	currentVar := 1
	name := make([]byte, 0, 10)

	for i, b := range qs {
		// a ':' while we're in a name is an error
		if b == ':' && inName {
			err = errors.New("unexpected `:` while reading named param at " + strconv.Itoa(i))
			return query, names, err
			// if we encounter a ':' and we aren't in a name, it's a new parameter
			// FIXME: escaping?
		} else if b == ':' {
			inName = true
			name = []byte{}
			// if we're in a name, and this is an allowed character, continue
		} else if inName && (unicode.IsOneOf(allowedBindRunes, rune(b)) || b == '_') && i != last {
			// append the byte to the name if we are in a name and not on the last byte
			name = append(name, b)
			// if we're in a name and it's not an allowed character, the name is done
		} else if inName {
			inName = false
			// if this is the final byte of the string and it is part of the name, then
			// make sure to add it to the name
			if i == last && unicode.IsOneOf(allowedBindRunes, rune(b)) {
				name = append(name, b)
			}
			// add the string representation to the names list and reset our name buffer
			names = append(names, string(name))
			name = make([]byte, 0, 10)
			// add a proper bindvar for the bindType
			switch bindType {
			case QUESTION, UNKNOWN:
				rebound = append(rebound, '?')
			case DOLLAR:
				rebound = append(rebound, '$')
				for _, b := range strconv.Itoa(currentVar) {
					rebound = append(rebound, byte(b))
				}
				currentVar++
			}
			// add this byte to string unless it was not part of the name
			if i != last {
				rebound = append(rebound, b)
			} else if !unicode.IsOneOf(allowedBindRunes, rune(b)) {
				rebound = append(rebound, b)
			}
		} else {
			// this is a normal byte and should just go onto the rebound query
			rebound = append(rebound, b)
		}
	}

	return string(rebound), names, err
}
