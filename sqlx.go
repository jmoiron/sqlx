package sqlx

import (
	"database/sql"
	"errors"
	"io/ioutil"
	"log"
	"path/filepath"
	"reflect"
	"strings"
)

type Rows struct {
	sql.Rows
	started bool
	fields  []int
	base    reflect.Type
	values  []interface{}
}

type Stmt struct{ sql.Stmt }

// An sqlx wrapper around database/sql's Tx with extra functionality
type Tx struct{ sql.Tx }

// An interface for something which can Execute sql queries (Tx, DB)
type Querier interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
}

// An interface for something which can Execute sql commands (Tx, DB)
type Execer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

// An sqlx wrapper around database/sql's DB with extra functionality
type DB struct{ sql.DB }

// Same as database/sql's Open, but returns an *sqlx.DB instead.
func Open(driverName, dataSourceName string) (*DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	return &DB{*db}, err
}

// Call Select using this db to issue the query.
func (db *DB) Select(typ interface{}, query string, args ...interface{}) ([]interface{}, error) {
	return Select(db, typ, query, args...)
}

// Call LoadFile using this db to issue the Exec.
func (db *DB) LoadFile(path string) (*sql.Result, error) {
	return LoadFile(db, path)
}

// Begin starts a transaction, and panics on error.  Returns an *sqlx.Tx instead
// of an *sql.Tx.
func (db *DB) MustBegin() *Tx {
	tx, err := db.Beginx()
	if err != nil {
		panic(err)
	}
	return tx
}

// Beginx is the same as Begin, but returns an *sqlx.Tx instead of an *sql.Tx
func (db *DB) Beginx() (*Tx, error) {
	tx, err := db.DB.Begin()
	return &Tx{*tx}, err
}

// Queryx is the same as Query, but returns an *sqlx.Rows instead of *sql.Rows
func (db *DB) Queryx(query string, args ...interface{}) (*Rows, error) {
	r, err := db.DB.Query(query, args...)
	return &Rows{Rows: *r}, err
}

// Call Select using this transaction to issue the Query.
func (tx *Tx) Select(typ interface{}, query string, args ...interface{}) ([]interface{}, error) {
	return Select(tx, typ, query, args...)
}

// Call LoadFile using this transaction to issue the Exec.
func (tx *Tx) LoadFile(path string) (*sql.Result, error) {
	return LoadFile(tx, path)
}

// Like sql.Rows.Scan, but scans a single Row into a single Struct.  Use this
// and iterate over Rows manually when the memory load of Select() might be
// prohibitive.  *Rows.StructScan caches the reflect work of matching up
// column positions to fields to avoid that overhead per scan, which means it
// is not safe to run StructScan on the same Rows instance with different
// struct types.
func (r *Rows) StructScan(typ interface{}) (interface{}, error) {
	var v reflect.Value
	if !r.started {
		v = reflect.ValueOf(typ)
		base, fm, err := getFieldmap(v.Type())

		columns, err := r.Columns()
		if err != nil {
			return nil, err
		}

		var ok bool
		var num int

		r.fields = make([]int, len(columns))
		r.values = make([]interface{}, len(columns))

		for i, name := range columns {
			// find that name in the struct
			num, ok = fm[name]
			if !ok {
				return nil, errors.New("Could not find name " + name + " in interface.")
			}
			r.fields[i] = num
		}
		r.started = true
		r.base = base
	}

	v = reflect.Indirect(reflect.New(r.base))
	for i, field := range r.fields {
		r.values[i] = v.Field(field).Addr().Interface()
	}
	r.Scan(r.values...)

	return v.Interface(), nil
}

// Select uses a Querier (*DB or *Tx, by default), issues the query w/ args
// via that Querier, and returns the results as a slice of typs.
func Select(q Querier, typ interface{}, query string, args ...interface{}) ([]interface{}, error) {
	rows, err := q.Query(query, args...)
	if err != nil {
		return nil, err
	}
	return StructScan(rows, typ)
}

// LoadFile exec's every statement in a file (as a single call to Exec).
// LoadFile returns a nil pointer and error if an error is encountered since
// errors can be encountered locating or reading the file, before a Result is
// created. LoadFile reads the entire file into memory, so it is not suitable
// for loading large data dumps, but can be useful for initializing database
// schemas or loading indexes. 
func LoadFile(e Execer, path string) (*sql.Result, error) {
	realpath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	contents, err := ioutil.ReadFile(realpath)
	if err != nil {
		return nil, err
	}
	res, err := e.Exec(string(contents))
	return &res, err
}

// Execv ("verbose") runs Exec on the query and args and uses log.Println to
// print the query, result, and error in the event of an error.  Since Execv
// returns flow to the caller, it returns the result and error.
func Execv(e Execer, query string, args ...interface{}) (sql.Result, error) {
	res, err := e.Exec(query, args...)
	if err != nil {
		log.Println(query, res, err)
	}
	return res, err
}

// Execl ("log") runs Exec on the query and args and ses log.Println to
// print the query, result, and error in the event of an error.  Unlike Execv,
// Execl does not return the error, and can be used in single-value contexts.
//
// Do not abuse Execl; it is convenient for experimentation but generally not
// for production use.
func Execl(e Execer, query string, args ...interface{}) sql.Result {
	res, err := e.Exec(query, args...)
	if err != nil {
		log.Println(query, res, err)
	}
	return res
}

// Execf ("fatal") runs Exec on the query and args and uses log.Fatal to
// print the query, result, and error in the event of an error.  Since
// errors are non-recoverable, only a Result is returned on success.
func Execf(e Execer, query string, args ...interface{}) sql.Result {
	res, err := e.Exec(query, args...)
	if err != nil {
		log.Fatal(query, res, err)
	}
	return res
}

// Execp ("panic") runs Exec on the query and args and panics on error.  Since
// the panic interrupts the control flow, errors are not returned to the caller.
func Execp(e Execer, query string, args ...interface{}) sql.Result {
	res, err := e.Exec(query, args...)
	if err != nil {
		panic(err)
	}
	return res
}

// A map of names to field positions for destination structs
type fieldmap map[string]int

// A cache of fieldmaps for reflect Types
var fieldmapCache = map[reflect.Type]fieldmap{}

// Return a reflect.Type's base struct type, or an error if it is not a struct
// or pointer to a struct.
func baseStructType(t reflect.Type) (reflect.Type, error) {
check:
	switch t.Kind() {
	case reflect.Ptr:
		t = t.Elem()
		goto check
	case reflect.Struct:
	default:
		return nil, errors.New("Destination must be a struct type.")
	}
	return t, nil
}

// Create a fieldmap for a given type and return its base type and fieldmap (or error)
func getFieldmap(t reflect.Type) (base reflect.Type, fm fieldmap, err error) {
	base, err = baseStructType(t)
	if err != nil {
		return base, fieldmap{}, err
	}
	// if we have a fieldmap cached, return it
	fm, ok := fieldmapCache[base]
	if ok {
		return base, fm, nil
	}

	var f reflect.StructField
	var name string
	for i := 0; i < base.NumField(); i++ {
		f = t.Field(i)
		name = strings.ToLower(f.Name)
		if tag := f.Tag.Get("db"); tag != "" {
			name = tag
		}
		fm[name] = i
	}
	fieldmapCache[base] = fm
	return base, fm, nil
}

// Fully scan a sql.Rows result into a slice of "typ" typed structs.
//
// StructScan can incompletely fill a struct, and will also work with
// any values order returned by the sql driver.
// StructScan will scan in the entire rows result, so if you need to iterate
// one at a time (to reduce memory usage, eg) avoid it.
func StructScan(rows *sql.Rows, typ interface{}) ([]interface{}, error) {
	var v, vp reflect.Value
	var ok bool
	v = reflect.ValueOf(typ)
	base, fm, err := getFieldmap(v.Type())

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var num int
	slice := make([]interface{}, 0)
	fields := make([]int, len(columns))

	for i, name := range columns {
		// find that name in the struct
		num, ok = fm[name]
		if !ok {
			return nil, errors.New("Could not find name " + name + " in interface.")
		}
		fields[i] = num
	}

	// this will hold interfaces which are pointers to each field in the struct
	values := make([]interface{}, len(columns))
	for rows.Next() {
		// create a new struct type (which returns PtrTo) and indirect it
		vp = reflect.New(base)
		v = reflect.Indirect(vp)
		for i, field := range fields {
			values[i] = v.Field(field).Addr().Interface()
		}
		// scan into the struct field pointers and append to our results
		rows.Scan(values...)
		slice = append(slice, v.Interface())
	}

	return slice, nil
}
