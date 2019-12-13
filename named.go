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
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

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
	bound, names, err := compileNamedQuery([]byte(query), bindType)
	if err != nil {
		return "", []interface{}{}, err
	}

	arglist, err := bindArgs(names, arg, m)
	if err != nil {
		return "", []interface{}{}, err
	}

	return bound, arglist, nil
}

var valueBracketReg = regexp.MustCompile(`\([^(]*\?+[^)]*\)`)

func fixBound(bound string, loop int) string {
	loc := valueBracketReg.FindStringIndex(bound)
	if len(loc) != 2 {
		return bound
	}
	var buffer bytes.Buffer

	buffer.WriteString(bound[0:loc[1]])
	for i := 0; i < loop-1; i++ {
		buffer.WriteString(",")
		buffer.WriteString(bound[loc[0]:loc[1]])
	}
	buffer.WriteString(bound[loc[1]:])
	return buffer.String()
}

// bindArray binds a named parameter query with fields from an array or slice of
// structs argument.
func bindArray(bindType int, query string, arg interface{}, m *reflectx.Mapper) (string, []interface{}, error) {
	// do the initial binding with QUESTION;  if bindType is not question,
	// we can rebind it at the end.
	bound, names, err := compileNamedQuery([]byte(query), QUESTION)
	if err != nil {
		return "", []interface{}{}, err
	}
	arrayValue := reflect.ValueOf(arg)
	arrayLen := arrayValue.Len()
	if arrayLen == 0 {
		return "", []interface{}{}, fmt.Errorf("length of array is 0: %#v", arg)
	}
	var arglist []interface{}
	for i := 0; i < arrayLen; i++ {
		elemArglist, err := bindArgs(names, arrayValue.Index(i).Interface(), m)
		if err != nil {
			return "", []interface{}{}, err
		}
		arglist = append(arglist, elemArglist...)
	}
	if arrayLen > 1 {
		bound = fixBound(bound, arrayLen)
	}
	// adjust binding type if we weren't on question
	if bindType != QUESTION {
		bound = Rebind(bindType, bound)
	}
	return bound, arglist, nil
}

// bindMap binds a named parameter query with a map of arguments.
func bindMap(bindType int, query string, args map[string]interface{}) (string, []interface{}, error) {
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

type parseNamedState int

const (
	parseStateConsumingIdent parseNamedState = iota
	parseStateQuery
	parseStateQuotedIdent
	parseStateStringConstant
	parseStateLineComment
	parseStateBlockComment
	parseStateSkipThenTransition
	parseStateDollarQuoteLiteral
)

type parseNamedContext struct {
	state parseNamedState
	data  map[string]interface{}
}

const (
	colon        = ':'
	backSlash    = '\\'
	forwardSlash = '/'
	singleQuote  = '\''
	dash         = '-'
	star         = '*'
	newLine      = '\n'
	dollarSign   = '$'
	doubleQuote  = '"'
)

// compile a NamedQuery into an unbound query (using the '?' bindvar) and
// a list of names.
func compileNamedQuery(qs []byte, bindType int) (query string, names []string, err error) {
	var result strings.Builder

	paramCount := 1
	var params []string
	addParam := func(paramName string) {
		params = append(params, paramName)

		switch bindType {
		// oracle only supports named type bind vars even for positional
		case NAMED:
			result.WriteByte(':')
			result.WriteString(paramName)
		case QUESTION, UNKNOWN:
			result.WriteByte('?')
		case DOLLAR:
			result.WriteByte('$')
			result.WriteString(strconv.Itoa(paramCount))
		case AT:
			result.WriteString("@p")
			result.WriteString(strconv.Itoa(paramCount))
		}

		paramCount++
	}

	isRuneStartOfIdent := func(r rune) bool {
		return unicode.In(r, unicode.Letter) || r == '_'
	}

	isRunePartOfIdent := func(r rune) bool {
		return isRuneStartOfIdent(r) || unicode.In(r, allowedBindRunes...) || r == '_' || r == '.'
	}

	ctx := parseNamedContext{state: parseStateQuery}

	setState := func(s parseNamedState, d map[string]interface{}) {
		ctx.data = d
		ctx.state = s
	}

	var previousRune rune
	maxIndex := len(qs)

	for byteIndex := 0; byteIndex < maxIndex; {
		currentRune, runeWidth := utf8.DecodeRune(qs[byteIndex:])
		nextRuneByteIndex := byteIndex + runeWidth

		nextRune := utf8.RuneError
		if nextRuneByteIndex < maxIndex {
			nextRune, _ = utf8.DecodeRune(qs[nextRuneByteIndex:])
		}

		writeCurrentRune := true
		switch ctx.state {
		case parseStateQuery:
			if currentRune == colon && previousRune != colon && isRuneStartOfIdent(nextRune) {
				// :foo
				writeCurrentRune = false
				setState(parseStateConsumingIdent, map[string]interface{}{
					"ident": &strings.Builder{},
				})
			} else if currentRune == singleQuote && previousRune != backSlash {
				// \'
				setState(parseStateStringConstant, nil)
			} else if currentRune == dash && nextRune == dash {
				// -- single line comment
				setState(parseStateLineComment, nil)
			} else if currentRune == forwardSlash && nextRune == star {
				// /*
				setState(parseStateSkipThenTransition, map[string]interface{}{
					"state": parseStateBlockComment,
					"data": map[string]interface{}{
						"depth": 1,
					},
				})
			} else if currentRune == dollarSign && previousRune == dollarSign {
				// $$
				setState(parseStateDollarQuoteLiteral, nil)
			} else if currentRune == doubleQuote {
				// "foo"."bar"
				setState(parseStateQuotedIdent, nil)
			}
		case parseStateConsumingIdent:
			if isRunePartOfIdent(currentRune) {
				ctx.data["ident"].(*strings.Builder).WriteRune(currentRune)
				writeCurrentRune = false
			} else {
				addParam(ctx.data["ident"].(*strings.Builder).String())
				setState(parseStateQuery, nil)
			}
		case parseStateBlockComment:
			if previousRune == star && currentRune == forwardSlash {
				newDepth := ctx.data["depth"].(int) - 1
				if newDepth == 0 {
					setState(parseStateQuery, nil)
				} else {
					ctx.data["depth"] = newDepth
				}
			}
		case parseStateLineComment:
			if currentRune == newLine {
				setState(parseStateQuery, nil)
			}
		case parseStateStringConstant:
			if currentRune == singleQuote && previousRune != backSlash {
				setState(parseStateQuery, nil)
			}
		case parseStateDollarQuoteLiteral:
			if currentRune == dollarSign && previousRune != dollarSign {
				setState(parseStateQuery, nil)
			}
		case parseStateQuotedIdent:
			if currentRune == doubleQuote {
				setState(parseStateQuery, nil)
			}
		case parseStateSkipThenTransition:
			setState(ctx.data["state"].(parseNamedState), ctx.data["data"].(map[string]interface{}))
		default:
			setState(parseStateQuery, nil)
		}

		if writeCurrentRune {
			result.WriteRune(currentRune)
		}

		previousRune = currentRune
		byteIndex = nextRuneByteIndex
	}

	// If parsing left off while consuming an ident, add that ident to params
	if ctx.state == parseStateConsumingIdent {
		addParam(ctx.data["ident"].(*strings.Builder).String())
	}

	return result.String(), params, nil
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
	switch reflect.TypeOf(arg).Kind() {
	case reflect.Array, reflect.Slice:
		return bindArray(bindType, query, arg, m)
	default:
		return bindStruct(bindType, query, arg, m)
	}
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
// or the query execution itself.
func NamedExec(e Ext, query string, arg interface{}) (sql.Result, error) {
	q, args, err := bindNamedMapper(BindType(e.DriverName()), query, arg, mapperFor(e))
	if err != nil {
		return nil, err
	}
	return e.Exec(q, args...)
}
