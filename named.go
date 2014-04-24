package sqlx

// Named Query Support
//
//  * BindStruct, BindMap - bind query bindvars to map/struct args
//	* NamedExec, NamedQuery - named query w/ struct or map
//  * NamedExecMap, NamedQueryMap - named query w/ maps (DEPRECATED)
//  * NamedStmt - a pre-compiled named query which is a prepared statement
//
// Internal Interfaces:
//
//  * compileNamedQuery - rebind a named query, returning a query and list of names
//  * bindArgs, bindMapArgs, bindAnyArgs - given a list of names, return an arglist
//  * bindAny - call BindStruct or BindMap depending on the type of the argument
//
import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"unicode"
)

// NamedStmt is a prepared statement that executes named queries.  Prepare it
// how you would execute a NamedQuery, but pass in a struct or map when executing.
type NamedStmt struct {
	Params      []string
	QueryString string
	Stmt        *Stmt
}

// Close closes the named statement.
func (n *NamedStmt) Close() error {
	return n.Stmt.Close()
}

// Exec executes a named statement using the struct passed.
func (n *NamedStmt) Exec(arg interface{}) (sql.Result, error) {
	args, err := bindAnyArgs(n.Params, arg)
	if err != nil {
		return *new(sql.Result), err
	}
	return n.Stmt.Exec(args...)
}

// Query executes a named statement using the struct argument, returning rows.
func (n *NamedStmt) Query(arg interface{}) (*sql.Rows, error) {
	args, err := bindAnyArgs(n.Params, arg)
	if err != nil {
		return nil, err
	}
	return n.Stmt.Query(args...)
}

// QueryRow executes a named statement against the database.  Because sqlx cannot
// create a *sql.Row with an error condition pre-set for binding errors, sqlx
// returns a *sqlx.Row instead.
func (n *NamedStmt) QueryRow(arg interface{}) *Row {
	args, err := bindAnyArgs(n.Params, arg)
	if err != nil {
		return &Row{err: err}
	}
	return n.Stmt.QueryRowx(args...)
}

// Execv execs a NamedStmt with the given arg, printing errors and returning them
func (n *NamedStmt) Execv(arg interface{}) (sql.Result, error) {
	res, err := n.Exec(arg)
	if err != nil {
		log.Println(n.QueryString, res, err)
	}
	return res, err
}

// Execl execs a NamedStmt with the given arg, logging errors
func (n *NamedStmt) Execl(arg interface{}) sql.Result {
	res, err := n.Exec(arg)
	if err != nil {
		log.Println(n.QueryString, res, err)
	}
	return res
}

// Execf execs a NamedStmt, using log.fatal to print out errors
func (n *NamedStmt) Execf(arg interface{}) sql.Result {
	res, err := n.Exec(arg)
	if err != nil {
		log.Fatal(n.QueryString, res, err)
	}
	return res
}

// Execp execs a NamedStmt, panicing on error
func (n *NamedStmt) Execp(arg interface{}) sql.Result {
	return n.MustExec(arg)
}

// MustExec execs a NamedStmt, panicing on error
func (n *NamedStmt) MustExec(arg interface{}) sql.Result {
	res, err := n.Exec(arg)
	if err != nil {
		panic(err)
	}
	return res
}

// Queryx using this NamedStmt
func (n *NamedStmt) Queryx(arg interface{}) (*Rows, error) {
	r, err := n.Query(arg)
	if err != nil {
		return nil, err
	}
	return &Rows{Rows: *r}, err
}

// QueryRowx this NamedStmt.  Because of limitations with QueryRow, this is
// an alias for QueryRow.
func (n *NamedStmt) QueryRowx(arg interface{}) *Row {
	return n.QueryRow(arg)
}

// Select using this NamedStmt
func (n *NamedStmt) Select(dest interface{}, arg interface{}) error {
	rows, err := n.Query(arg)
	if err != nil {
		return err
	}
	// if something happens here, we want to make sure the rows are Closed
	defer rows.Close()
	return StructScan(rows, dest)
}

// Selectv using this NamedStmt
func (n *NamedStmt) Selectv(dest interface{}, arg interface{}) error {
	err := n.Select(dest, arg)
	if err != nil {
		log.Println(n.QueryString, err)
	}
	return err
}

// Selectf using this NamedStmt
func (n *NamedStmt) Selectf(dest interface{}, arg interface{}) {
	err := n.Select(dest, arg)
	if err != nil {
		log.Fatal(n.QueryString, err)
	}
}

// Get using this NamedStmt
func (n *NamedStmt) Get(dest interface{}, arg interface{}) error {
	r := n.QueryRowx(arg)
	return r.StructScan(dest)
}

// A union interface of preparer and binder, required to be able to prepare
// named statements (as the bindtype must be determined).
type namedPreparer interface {
	Preparer
	Binder
}

func prepareNamed(p namedPreparer, query string) (*NamedStmt, error) {
	bindType := BindType(p.DriverName())
	q, args, err := compileNamedQuery([]byte(query), bindType)
	if err != nil {
		return nil, err
	}
	stmt, err := Preparex(p, q)
	if err != nil {
		return nil, err
	}
	return &NamedStmt{
		QueryString: q,
		Params:      args,
		Stmt:        stmt,
	}, nil
}

func bindAnyArgs(names []string, arg interface{}) ([]interface{}, error) {
	if maparg, ok := arg.(map[string]interface{}); ok {
		return bindMapArgs(names, maparg)
	}
	return bindArgs(names, arg)
}

// private interface to generate a list of interfaces from a given struct
// type, given a list of names to pull out of the struct.  Used by public
// BindStruct interface.
func bindArgs(names []string, arg interface{}) ([]interface{}, error) {
	arglist := make([]interface{}, 0, len(names))

	t, err := BaseStructType(reflect.TypeOf(arg))
	if err != nil {
		return arglist, err
	}

	// resolve this arg's type into a map of fields to field positions
	fm, err := getFieldMap(t)
	if err != nil {
		return arglist, err
	}

	// grab the indirected value of arg
	v := reflect.ValueOf(arg)
	for v = reflect.ValueOf(arg); v.Kind() == reflect.Ptr; {
		v = v.Elem()
	}

	// FIXME: why aren't we using reflect helpers here?

	values := fm.allValues(v)

	for _, name := range names {
		val, ok := fm[name]
		if !ok {
			return arglist, fmt.Errorf("could not find name %s in %v", name, arg)
		}
		arglist = append(arglist, values[val])
	}

	return arglist, nil
}

// like bindArgs, but for maps.
func bindMapArgs(names []string, arg map[string]interface{}) ([]interface{}, error) {
	arglist := make([]interface{}, 0, len(names))

	for _, name := range names {
		val, ok := arg[name]
		if !ok {
			return arglist, fmt.Errorf("could not find name %s in %v", name, arg)
		}
		arglist = append(arglist, val)
	}
	return arglist, nil
}

// BindStruct binds a named parameter query with fields from a struct argument.
// The rules for binding field names to parameter names follow the same
// conventions as for StructScan, including obeying the `db` struct tags.
func BindStruct(bindType int, query string, arg interface{}) (string, []interface{}, error) {
	bound, names, err := compileNamedQuery([]byte(query), bindType)
	if err != nil {
		return "", []interface{}{}, err
	}

	arglist, err := bindArgs(names, arg)
	if err != nil {
		return "", []interface{}{}, err
	}

	return bound, arglist, nil
}

// BindMap binds a named parameter query with a map of arguments.
func BindMap(bindType int, query string, args map[string]interface{}) (string, []interface{}, error) {
	bound, names, err := compileNamedQuery([]byte(query), bindType)
	if err != nil {
		return "", []interface{}{}, err
	}

	arglist, err := bindMapArgs(names, args)
	return bound, arglist, err
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
			// add the string representation to the names list
			names = append(names, string(name))
			// add a proper bindvar for the bindType
			switch bindType {
			// oracle only supports named type bind vars even for positional
			case NAMED:
				rebound = append(rebound, ':')
				rebound = append(rebound, name...)
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

// bindAny binds a struct or a map by inspecting the arg interface.
func bindAny(e Ext, query string, arg interface{}) (string, []interface{}, error) {
	if maparg, ok := arg.(map[string]interface{}); ok {
		return e.BindMap(query, maparg)
	}
	return e.BindStruct(query, arg)
}

// NamedQuery binds a named query and then runs Query on the result using the
// provided Ext (sqlx.Tx, sqlx.Db).  It works with both structs and with
// map[string]interface{} types.
func NamedQuery(e Ext, query string, arg interface{}) (*Rows, error) {
	q, args, err := bindAny(e, query, arg)
	if err != nil {
		return nil, err
	}
	return e.Queryx(q, args...)
}

// NamedExec uses BindStruct to get a query executable by the driver and
// then runs Exec on the result.  Returns an error from the binding
// or the query excution itself.
func NamedExec(e Ext, query string, arg interface{}) (sql.Result, error) {
	q, args, err := bindAny(e, query, arg)
	if err != nil {
		return nil, err
	}
	return e.Exec(q, args...)
}

// NamedQueryMap runs a named query using a map instead of a struct.
// DEPRECATED:  Use NamedQuery instead, which also supports maps.
func NamedQueryMap(e Ext, query string, argmap map[string]interface{}) (*Rows, error) {
	q, args, err := e.BindMap(query, argmap)
	if err != nil {
		return nil, err
	}
	return e.Queryx(q, args...)
}

// NamedExecMap executes a named query using a map instead of a struct.
// DEPRECATED: Use NamedExec instead, which also supports maps.
func NamedExecMap(e Ext, query string, argmap map[string]interface{}) (sql.Result, error) {
	q, args, err := e.BindMap(query, argmap)
	if err != nil {
		return nil, err
	}
	return e.Exec(q, args...)
}
