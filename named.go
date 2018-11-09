package sqlx

// Named Query Support
//
//  * BindMap - bind query bindvars to map/struct args
//	* NamedExec, NamedQuery - named query w/ struct or map
//  * NamedStmt - a pre-compiled named query which is a prepared statement
//
// Internal Interfaces:
//
//  * compileNamedQuery - rebind a named query, returning a query and list of names
//  * bindArgs, bindMapArgs, bindAnyArgs - given a list of names, return an arglist
//
import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx/reflectx"
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
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) Exec(arg interface{}) (sql.Result, error) {
	args, err := bindAnyArgs(n.Params, arg, n.Stmt.Mapper)
	if err != nil {
		return *new(sql.Result), err
	}
	return n.Stmt.Exec(args...)
}

// Query executes a named statement using the struct argument, returning rows.
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) Query(arg interface{}) (*sql.Rows, error) {
	args, err := bindAnyArgs(n.Params, arg, n.Stmt.Mapper)
	if err != nil {
		return nil, err
	}
	return n.Stmt.Query(args...)
}

// QueryRow executes a named statement against the database.  Because sqlx cannot
// create a *sql.Row with an error condition pre-set for binding errors, sqlx
// returns a *sqlx.Row instead.
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) QueryRow(arg interface{}) *Row {
	args, err := bindAnyArgs(n.Params, arg, n.Stmt.Mapper)
	if err != nil {
		return &Row{err: err}
	}
	return n.Stmt.QueryRowx(args...)
}

// MustExec execs a NamedStmt, panicing on error
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) MustExec(arg interface{}) sql.Result {
	res, err := n.Exec(arg)
	if err != nil {
		panic(err)
	}
	return res
}

// Queryx using this NamedStmt
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) Queryx(arg interface{}) (*Rows, error) {
	r, err := n.Query(arg)
	if err != nil {
		return nil, err
	}
	return &Rows{Rows: r, Mapper: n.Stmt.Mapper, unsafe: isUnsafe(n)}, err
}

// QueryRowx this NamedStmt.  Because of limitations with QueryRow, this is
// an alias for QueryRow.
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) QueryRowx(arg interface{}) *Row {
	return n.QueryRow(arg)
}

// Select using this NamedStmt
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) Select(dest interface{}, arg interface{}) error {
	rows, err := n.Queryx(arg)
	if err != nil {
		return err
	}
	// if something happens here, we want to make sure the rows are Closed
	defer rows.Close()
	return scanAll(rows, dest, false)
}

// Get using this NamedStmt
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) Get(dest interface{}, arg interface{}) error {
	r := n.QueryRowx(arg)
	return r.scanAny(dest, false)
}

// Unsafe creates an unsafe version of the NamedStmt
func (n *NamedStmt) Unsafe() *NamedStmt {
	r := &NamedStmt{Params: n.Params, Stmt: n.Stmt, QueryString: n.QueryString}
	r.Stmt.unsafe = true
	return r
}

// A union interface of preparer and binder, required to be able to prepare
// named statements (as the bindtype must be determined).
type namedPreparer interface {
	Preparer
	binder
}

func prepareNamed(p namedPreparer, query string) (*NamedStmt, error) {
	bindType := BindType(p.DriverName())
	q, args := compileNamedQuery(query, bindType)
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

func bindAnyArgs(names []string, arg interface{}, m *reflectx.Mapper) ([]interface{}, error) {
	if maparg, ok := arg.(map[string]interface{}); ok {
		return bindMapArgs(names, maparg)
	}
	return bindArgs(names, arg, m)
}

// private interface to generate a list of interfaces from a given struct
// type, given a list of names to pull out of the struct.  Used by public
// BindStruct interface.
func bindArgs(names []string, arg interface{}, m *reflectx.Mapper) ([]interface{}, error) {
	arglist := make([]interface{}, 0, len(names))

	// grab the indirected value of arg
	v := reflect.ValueOf(arg)
	for v = reflect.ValueOf(arg); v.Kind() == reflect.Ptr; {
		v = v.Elem()
	}

	err := m.TraversalsByNameFunc(v.Type(), names, func(i int, t []int) error {
		if len(t) == 0 {
			return fmt.Errorf("could not find name %s in %#v", names[i], arg)
		}

		val := reflectx.FieldByIndexesReadOnly(v, t)
		arglist = append(arglist, val.Interface())

		return nil
	})

	return arglist, err
}

// like bindArgs, but for maps.
func bindMapArgs(names []string, arg map[string]interface{}) ([]interface{}, error) {
	arglist := make([]interface{}, 0, len(names))

	for _, name := range names {
		val, ok := arg[name]
		if !ok {
			return arglist, fmt.Errorf("could not find name %s in %#v", name, arg)
		}
		arglist = append(arglist, val)
	}
	return arglist, nil
}

// bindStruct binds a named parameter query with fields from a struct argument.
// The rules for binding field names to parameter names follow the same
// conventions as for StructScan, including obeying the `db` struct tags.
func bindStruct(bindType int, query string, arg interface{}, m *reflectx.Mapper) (string, []interface{}, error) {
	bound, names := compileNamedQuery(query, bindType)

	arglist, err := bindArgs(names, arg, m)
	if err != nil {
		return "", []interface{}{}, err
	}

	return bound, arglist, nil
}

// bindMap binds a named parameter query with a map of arguments.
func bindMap(bindType int, query string, args map[string]interface{}) (string, []interface{}, error) {
	bound, names := compileNamedQuery(query, bindType)

	arglist, err := bindMapArgs(names, args)
	return bound, arglist, err
}

// -- Compilation of Named Queries

// compile a NamedQuery into a unbound query (using the '?' bindvar) and
// a list of names.
func compileNamedQuery(qs string, bindType int) (query string, names []string) {
	rebound := strings.Builder{}
	names = make([]string, 0, 10)
	currentVar := 1
	byteOffset := 0

	lex := lexSQL(qs)
	for tok := range lex.items {
		isBindVar := tok.typ == itemIdentifier &&
			tok.val[0] == ':' &&
			len(tok.val) > 1 &&
			tok.val[1] != ':'
		if isBindVar {
			name := tok.val[1:]
			names = append(names, name)
			rebound.WriteString(qs[byteOffset:tok.pos])
			switch bindType {
			// oracle only supports named type bind vars even for positional
			case NAMED:
				rebound.WriteString(tok.val)
			case QUESTION, UNKNOWN:
				rebound.WriteByte('?')
			case DOLLAR:
				rebound.WriteByte('$')
				rebound.WriteString(strconv.Itoa(currentVar))
				currentVar++
			case AT:
				rebound.WriteString("@p")
				rebound.WriteString(strconv.Itoa(currentVar))
				currentVar++
			}
			byteOffset = tok.pos + len(tok.val)
		}
	}
	if byteOffset == 0 {
		return qs, names
	}
	rebound.WriteString(qs[byteOffset:])
	return rebound.String(), names
}

// BindNamed binds a struct or a map to a query with named parameters.
// DEPRECATED: use sqlx.Named` instead of this, it may be removed in future.
func BindNamed(bindType int, query string, arg interface{}) (string, []interface{}, error) {
	return bindNamedMapper(bindType, query, arg, mapper())
}

// Named takes a query using named parameters and an argument and
// returns a new query with a list of args that can be executed by
// a database.  The return value uses the `?` bindvar.
func Named(query string, arg interface{}) (string, []interface{}, error) {
	return bindNamedMapper(QUESTION, query, arg, mapper())
}

func bindNamedMapper(bindType int, query string, arg interface{}, m *reflectx.Mapper) (string, []interface{}, error) {
	if maparg, ok := arg.(map[string]interface{}); ok {
		return bindMap(bindType, query, maparg)
	}
	return bindStruct(bindType, query, arg, m)
}

// NamedQuery binds a named query and then runs Query on the result using the
// provided Ext (sqlx.Tx, sqlx.Db).  It works with both structs and with
// map[string]interface{} types.
func NamedQuery(e Ext, query string, arg interface{}) (*Rows, error) {
	q, args, err := bindNamedMapper(BindType(e.DriverName()), query, arg, mapperFor(e))
	if err != nil {
		return nil, err
	}
	return e.Queryx(q, args...)
}

// NamedExec uses BindStruct to get a query executable by the driver and
// then runs Exec on the result.  Returns an error from the binding
// or the query excution itself.
func NamedExec(e Ext, query string, arg interface{}) (sql.Result, error) {
	q, args, err := bindNamedMapper(BindType(e.DriverName()), query, arg, mapperFor(e))
	if err != nil {
		return nil, err
	}
	return e.Exec(q, args...)
}
