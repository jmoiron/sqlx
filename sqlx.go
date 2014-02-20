package sqlx

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"

	"io/ioutil"
	"log"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"
)

// NameMapper is used to map column names to struct field names.  By default,
// it uses strings.ToLower to lowercase struct field names.  It can be set
// to whatever you want, but it is encouraged to be set before sqlx is used
// as field-to-name mappings are cached after first use on a type.
var NameMapper = strings.ToLower

// Rows is a wrapper around sql.Rows which caches costly reflect operations
// during a looped StructScan
type Rows struct {
	sql.Rows
	started bool
	fields  []int
	base    reflect.Type
	values  []interface{}
}

// Row is a reimplementation of sql.Row in order to gain access to the underlying
// sql.Rows.Columns() data, necessary for StructScan.
type Row struct {
	err  error
	rows *sql.Rows
}

// ColScanner is an interface for something which can Scan and return a list
// of columns (Row, Rows)
type ColScanner interface {
	Columns() ([]string, error)
	Scan(dest ...interface{}) error
	Err() error
}

// Queryer is an interface for something which can Query (Tx, DB, Stmt)
type Queryer interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	Queryx(query string, args ...interface{}) (*Rows, error)
	QueryRowx(query string, args ...interface{}) *Row
}

// Execer is an interface for something which can Exec (Tx, DB, Stmt)
type Execer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

// Binder is an interface for something which can bind queries (Tx, DB)
type Binder interface {
	DriverName() string
	Rebind(string) string
	BindMap(string, map[string]interface{}) (string, []interface{}, error)
	BindStruct(string, interface{}) (string, []interface{}, error)
}

// Ext is a union interface which can bind, query, and exec (Tx, DB), used for
// NamedQuery and NamedExec, which requires exec/query and BindMap/Struct
type Ext interface {
	Binder
	Queryer
	Execer
}

// Preparer is an interface for something which can prepare sql statements (Tx, DB)
type Preparer interface {
	Prepare(query string) (*sql.Stmt, error)
}

// Scan is a fixed implementation of sql.Row.Scan, which does not discard the
// underlying error from the internal rows object if it exists.
func (r *Row) Scan(dest ...interface{}) error {
	if r.err != nil {
		return r.err
	}

	// TODO(bradfitz): for now we need to defensively clone all
	// []byte that the driver returned (not permitting
	// *RawBytes in Rows.Scan), since we're about to close
	// the Rows in our defer, when we return from this function.
	// the contract with the driver.Next(...) interface is that it
	// can return slices into read-only temporary memory that's
	// only valid until the next Scan/Close.  But the TODO is that
	// for a lot of drivers, this copy will be unnecessary.  We
	// should provide an optional interface for drivers to
	// implement to say, "don't worry, the []bytes that I return
	// from Next will not be modified again." (for instance, if
	// they were obtained from the network anyway) But for now we
	// don't care.
	for _, dp := range dest {
		if _, ok := dp.(*sql.RawBytes); ok {
			return errors.New("sql: RawBytes isn't allowed on Row.Scan")
		}
	}

	defer r.rows.Close()
	if !r.rows.Next() {
		if r.rows.Err() != nil {
			r.err = r.rows.Err()
			return r.err
		}
		return sql.ErrNoRows
	}
	return r.rows.Scan(dest...)
}

// Columns returns the underlying sql.Rows.Columns(), or the deferred error usually
// returned by Row.Scan()
func (r *Row) Columns() ([]string, error) {
	if r.err != nil {
		return []string{}, r.err
	}
	return r.rows.Columns()
}

// Err returns the error encountered while scanning.
func (r *Row) Err() error {
	return r.err
}

// DB is a wrapper around sql.DB which keeps track of the driverName upon Open,
// used mostly to automatically bind named queries using the right bindvars.
type DB struct {
	*sql.DB
	driverName string
}

// NewDb returns a new sqlx DB wrapper for a pre-existing *sql.DB.  The
// driverName of the original database is required for named query support.
func NewDb(db *sql.DB, driverName string) *DB {
	return &DB{db, driverName}
}

// DriverName returns the driverName passed to the Open function for this DB.
func (db *DB) DriverName() string {
	return db.driverName
}

// Open is the same as database/sql's Open, but returns an *sqlx.DB instead.
func Open(driverName, dataSourceName string) (*DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	return &DB{db, driverName}, err
}

// Rebind transforms a query from QUESTION to the DB driver's bindvar type.
func (db *DB) Rebind(query string) string {
	return Rebind(BindType(db.driverName), query)
}

// BindMap binds a query using the DB driver's bindvar type.
func (db *DB) BindMap(query string, argmap map[string]interface{}) (string, []interface{}, error) {
	return BindMap(BindType(db.driverName), query, argmap)
}

// BindStruct binds a query using the DB driver's bindvar type.
func (db *DB) BindStruct(query string, arg interface{}) (string, []interface{}, error) {
	return BindStruct(BindType(db.driverName), query, arg)
}

// NamedQueryMap using this DB.
func (db *DB) NamedQueryMap(query string, argmap map[string]interface{}) (*Rows, error) {
	return NamedQueryMap(db, query, argmap)
}

// NamedExecMap using this DB.
func (db *DB) NamedExecMap(query string, argmap map[string]interface{}) (sql.Result, error) {
	return NamedExecMap(db, query, argmap)
}

// NamedQuery using this DB.
func (db *DB) NamedQuery(query string, arg interface{}) (*Rows, error) {
	return NamedQuery(db, query, arg)
}

// NamedExec using this DB.
func (db *DB) NamedExec(query string, arg interface{}) (sql.Result, error) {
	return NamedExec(db, query, arg)
}

// Select using this DB.
func (db *DB) Select(dest interface{}, query string, args ...interface{}) error {
	return Select(db, dest, query, args...)
}

// Selectf using this DB.
func (db *DB) Selectf(dest interface{}, query string, args ...interface{}) {
	Selectf(db, dest, query, args...)
}

// Selectv using this DB.
func (db *DB) Selectv(dest interface{}, query string, args ...interface{}) error {
	return Selectv(db, dest, query, args...)
}

// Get using this DB.
func (db *DB) Get(dest interface{}, query string, args ...interface{}) error {
	return Get(db, dest, query, args...)
}

// LoadFile using this DB.
func (db *DB) LoadFile(path string) (*sql.Result, error) {
	return LoadFile(db, path)
}

// MustBegin starts a transaction, and panics on error.  Returns an *sqlx.Tx instead
// of an *sql.Tx.
func (db *DB) MustBegin() *Tx {
	tx, err := db.Beginx()
	if err != nil {
		panic(err)
	}
	return tx
}

// Beginx begins a transaction and returns an *sqlx.Tx instead of an *sql.Tx.
func (db *DB) Beginx() (*Tx, error) {
	tx, err := db.DB.Begin()
	if err != nil {
		return nil, err
	}
	return &Tx{*tx, db.driverName}, err
}

// Queryx queries the database and returns an *sqlx.Rows.
func (db *DB) Queryx(query string, args ...interface{}) (*Rows, error) {
	r, err := db.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	return &Rows{Rows: *r}, err
}

// QueryRowx queries the database and returns an *sqlx.Row.
func (db *DB) QueryRowx(query string, args ...interface{}) *Row {
	rows, err := db.DB.Query(query, args...)
	return &Row{rows: rows, err: err}
}

// Execv (verbose) runs Execv using this database.
func (db *DB) Execv(query string, args ...interface{}) (sql.Result, error) {
	return Execv(db, query, args...)
}

// Execl (log) runs Execl using this database.
func (db *DB) Execl(query string, args ...interface{}) sql.Result {
	return Execl(db, query, args...)
}

// Execf (fatal) runs Execf using this database.
func (db *DB) Execf(query string, args ...interface{}) sql.Result {
	return Execf(db, query, args...)
}

// Execp (panic) runs Execp using this database.
func (db *DB) Execp(query string, args ...interface{}) sql.Result {
	return Execp(db, query, args...)
}

// MustExec (panic) runs MustExec using this database.
func (db *DB) MustExec(query string, args ...interface{}) sql.Result {
	return MustExec(db, query, args...)
}

// Preparex returns an sqlx.Stmt instead of a sql.Stmt
func (db *DB) Preparex(query string) (*Stmt, error) {
	return Preparex(db, query)
}

// PrepareNamed returns an sqlx.NamedStmt
func (db *DB) PrepareNamed(query string) (*NamedStmt, error) {
	return prepareNamed(db, query)
}

// Tx is an sqlx wrapper around database/sql's Tx with extra functionality
type Tx struct {
	sql.Tx
	driverName string
}

// DriverName returns the driverName used by the DB which began this transaction.
func (tx *Tx) DriverName() string {
	return tx.driverName
}

// Rebind a query within a transaction's bindvar type.
func (tx *Tx) Rebind(query string) string {
	return Rebind(BindType(tx.driverName), query)
}

// BindMap binds a query within a transaction's bindvar type.
func (tx *Tx) BindMap(query string, argmap map[string]interface{}) (string, []interface{}, error) {
	return BindMap(BindType(tx.driverName), query, argmap)
}

// BindStruct binds a query within a transaction's bindvar type.
func (tx *Tx) BindStruct(query string, arg interface{}) (string, []interface{}, error) {
	return BindStruct(BindType(tx.driverName), query, arg)
}

// NamedQuery within a transaction.
func (tx *Tx) NamedQuery(query string, arg interface{}) (*Rows, error) {
	return NamedQuery(tx, query, arg)
}

// NamedExec a named query within a transaction.
func (tx *Tx) NamedExec(query string, arg interface{}) (sql.Result, error) {
	return NamedExec(tx, query, arg)
}

// NamedQueryMap within a transaction.
func (tx *Tx) NamedQueryMap(query string, arg map[string]interface{}) (*Rows, error) {
	return NamedQueryMap(tx, query, arg)
}

// NamedExecMap a named query within a transaction.
func (tx *Tx) NamedExecMap(query string, arg map[string]interface{}) (sql.Result, error) {
	return NamedExecMap(tx, query, arg)
}

// LoadFile within a transaction.
func (tx *Tx) LoadFile(path string) (*sql.Result, error) {
	return LoadFile(tx, path)
}

// Select within a transaction.
func (tx *Tx) Select(dest interface{}, query string, args ...interface{}) error {
	return Select(tx, dest, query, args...)
}

// Queryx within a transaction.
func (tx *Tx) Queryx(query string, args ...interface{}) (*Rows, error) {
	r, err := tx.Tx.Query(query, args...)
	if err != nil {
		return nil, err
	}
	return &Rows{Rows: *r}, err
}

// QueryRowx within a transaction.
func (tx *Tx) QueryRowx(query string, args ...interface{}) *Row {
	rows, err := tx.Tx.Query(query, args...)
	return &Row{rows: rows, err: err}
}

// Get within a transaction.
func (tx *Tx) Get(dest interface{}, query string, args ...interface{}) error {
	return Get(tx, dest, query, args...)
}

// Selectv (verbose) within a transaction.
func (tx *Tx) Selectv(dest interface{}, query string, args ...interface{}) error {
	return Selectv(tx, dest, query, args...)
}

// Selectf (fatal) within a transaction.
func (tx *Tx) Selectf(dest interface{}, query string, args ...interface{}) {
	Selectf(tx, dest, query, args...)
}

// Execv (verbose) runs Execv within a transaction.
func (tx *Tx) Execv(query string, args ...interface{}) (sql.Result, error) {
	return Execv(tx, query, args...)
}

// Execl (log) runs Execl within a transaction.
func (tx *Tx) Execl(query string, args ...interface{}) sql.Result {
	return Execl(tx, query, args...)
}

// Execf (fatal) runs Execf within a transaction.
func (tx *Tx) Execf(query string, args ...interface{}) sql.Result {
	return Execf(tx, query, args...)
}

// Execp (panic) runs Execp within a transaction.
func (tx *Tx) Execp(query string, args ...interface{}) sql.Result {
	return Execp(tx, query, args...)
}

// MustExec runs MustExec within a transaction.
func (tx *Tx) MustExec(query string, args ...interface{}) sql.Result {
	return MustExec(tx, query, args...)
}

// Preparex  a statement within a transaction.
func (tx *Tx) Preparex(query string) (*Stmt, error) {
	return Preparex(tx, query)
}

// Stmtx returns a version of the prepared statement which runs within a transaction.  Provided
// stmt can be either *sql.Stmt or *sqlx.Stmt.
func (tx *Tx) Stmtx(stmt interface{}) *Stmt {
	var st sql.Stmt
	var s *sql.Stmt
	switch stmt.(type) {
	case sql.Stmt:
		st = stmt.(sql.Stmt)
		s = &st
	case Stmt:
		s = stmt.(Stmt).Stmt
	case *Stmt:
		s = stmt.(*Stmt).Stmt
	case *sql.Stmt:
		s = stmt.(*sql.Stmt)
	}
	return &Stmt{tx.Stmt(s)}
}

// NamedStmt returns a version of the prepared statement which runs within a transaction.
func (tx *Tx) NamedStmt(stmt *NamedStmt) *NamedStmt {
	return &NamedStmt{
		QueryString: stmt.QueryString,
		Params:      stmt.Params,
		Stmt:        tx.Stmtx(stmt.Stmt),
	}
}

// PrepareNamed returns an sqlx.NamedStmt
func (tx *Tx) PrepareNamed(query string) (*NamedStmt, error) {
	return prepareNamed(tx, query)
}

// Stmt is an sqlx wrapper around database/sql's Stmt with extra functionality
type Stmt struct{ *sql.Stmt }

// qStmt is an unexposed wrapper which lets you use a Stmt as a Queryer & Execer by
// implementing those interfaces and ignoring the `query` argument.
type qStmt struct{ Stmt }

func (q *qStmt) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return q.Stmt.Query(args...)
}

func (q *qStmt) Queryx(query string, args ...interface{}) (*Rows, error) {
	r, err := q.Stmt.Query(args...)
	if err != nil {
		return nil, err
	}
	return &Rows{Rows: *r}, err
}

func (q *qStmt) QueryRowx(query string, args ...interface{}) *Row {
	rows, err := q.Stmt.Query(args...)
	return &Row{rows: rows, err: err}
}

func (q *qStmt) Exec(query string, args ...interface{}) (sql.Result, error) {
	return q.Stmt.Exec(args...)
}

// Select using the prepared statement.
func (s *Stmt) Select(dest interface{}, args ...interface{}) error {
	return Select(&qStmt{*s}, dest, "", args...)
}

// Selectv (verbose) using the prepared statement.
func (s *Stmt) Selectv(dest interface{}, args ...interface{}) error {
	return Selectv(&qStmt{*s}, dest, "", args...)
}

// Selectf (fatal) using the prepared statement.
func (s *Stmt) Selectf(dest interface{}, args ...interface{}) {
	Selectf(&qStmt{*s}, dest, "", args...)
}

// Get using the prepared statement.
func (s *Stmt) Get(dest interface{}, args ...interface{}) error {
	return Get(&qStmt{*s}, dest, "", args...)
}

// Execv (verbose) runs Execv using this statement.  Note that the query
// portion of the error output will be blank, as Stmt does not expose its query.
func (s *Stmt) Execv(args ...interface{}) (sql.Result, error) {
	return Execv(&qStmt{*s}, "", args...)
}

// Execl (log) using this statement.  Note that the query portion of the error
// output will be blank, as Stmt does not expose its query.
func (s *Stmt) Execl(args ...interface{}) sql.Result {
	return Execl(&qStmt{*s}, "", args...)
}

// Execf (fatal) using this statement.  Note that the query portion of the error
// output will be blank, as Stmt does not expose its query.
func (s *Stmt) Execf(args ...interface{}) sql.Result {
	return Execf(&qStmt{*s}, "", args...)
}

// Execp (panic) using this statement.  Note that the query portion of the error
// output will be blank, as Stmt does not expose its query.
func (s *Stmt) Execp(args ...interface{}) sql.Result {
	return Execp(&qStmt{*s}, "", args...)
}

// MustExec (panic) using this statement.  Note that the query portion of the error
// output will be blank, as Stmt does not expose its query.
func (s *Stmt) MustExec(args ...interface{}) sql.Result {
	return MustExec(&qStmt{*s}, "", args...)
}

// QueryRowx using this statement.
func (s *Stmt) QueryRowx(args ...interface{}) *Row {
	qs := &qStmt{*s}
	return qs.QueryRowx("", args...)
}

// Queryx using this statement.
func (s *Stmt) Queryx(args ...interface{}) (*Rows, error) {
	qs := &qStmt{*s}
	return qs.Queryx("", args...)
}

// SliceScan using this Rows.
func (r *Rows) SliceScan() ([]interface{}, error) {
	return SliceScan(r)
}

// MapScan using this Rows.
func (r *Rows) MapScan(dest map[string]interface{}) error {
	return MapScan(r, dest)
}

// StructScan is like sql.Rows.Scan, but scans a single Row into a single Struct.
// Use this and iterate over Rows manually when the memory load of Select() might be
// prohibitive.  *Rows.StructScan caches the reflect work of matching up column
// positions to fields to avoid that overhead per scan, which means it is not safe
// to run StructScan on the same Rows instance with different struct types.
func (r *Rows) StructScan(dest interface{}) error {
	var v reflect.Value
	v = reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr {
		return errors.New("must pass a pointer, not a value, to StructScan destination")
	}
	base := reflect.Indirect(v)
	// see if we have a cached fieldmap
	if !r.started {

		fm, err := getFieldmap(base.Type())
		if err != nil {
			return err
		}
		columns, err := r.Columns()
		if err != nil {
			return err
		}

		var ok bool
		var num int

		r.fields = make([]int, len(columns))
		r.values = make([]interface{}, len(columns))

		for i, name := range columns {
			// find that name in the struct
			num, ok = fm[name]
			if !ok {
				return errors.New("Could not find name " + name + " in interface.")
			}
			r.fields[i] = num
		}
		r.started = true
	}
	for i, field := range r.fields {
		r.values[i] = base.Field(field).Addr().Interface()
	}
	err := r.Scan(r.values...)
	if err != nil {
		return err
	}
	return r.Err()
}

// Connect to a database and verify with a ping.
func Connect(driverName, dataSourceName string) (*DB, error) {
	db, err := Open(driverName, dataSourceName)
	if err != nil {
		return db, err
	}
	err = db.Ping()
	return db, err
}

// MustConnect connects to a database and panics on error.
func MustConnect(driverName, dataSourceName string) *DB {
	db, err := Connect(driverName, dataSourceName)
	if err != nil {
		panic(err)
	}
	return db
}

// Preparex prepares a statement.
func Preparex(p Preparer, query string) (*Stmt, error) {
	s, err := p.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &Stmt{s}, err
}

// Select executes a query using the provided Queryer, and StructScans each row
// into dest, which must be a slice of structs. The *sql.Rows are closed
// automatically.
func Select(q Queryer, dest interface{}, query string, args ...interface{}) error {
	rows, err := q.Query(query, args...)
	if err != nil {
		return err
	}
	// if something happens here, we want to make sure the rows are Closed
	defer rows.Close()
	return StructScan(rows, dest)
}

// Selectv (verbose) will Select using a Queryer and use log.Println to print
//the query and the error in the event of an error.
func Selectv(q Queryer, dest interface{}, query string, args ...interface{}) error {
	err := Select(q, dest, query, args...)
	if err != nil {
		log.Println(query, err)
	}
	return err
}

// Selectf (fatal) will Select using a Queryer and use log.Fatal to print
// the query and the error in the event of an error.
func Selectf(q Queryer, dest interface{}, query string, args ...interface{}) {
	err := Select(q, dest, query, args...)
	if err != nil {
		log.Fatal(query, err)
	}
}

// Get does a QueryRow using the provided Queryer, and StructScan the resulting
// row into dest, which must be a pointer to a struct.  If there was no row,
// Get will return sql.ErrNoRows like row.Scan would.
func Get(q Queryer, dest interface{}, query string, args ...interface{}) error {
	r := q.QueryRowx(query, args...)
	return r.StructScan(dest)
}

// LoadFile exec's every statement in a file (as a single call to Exec).
// LoadFile may return a nil *sql.Result if errors are encountered locating or
// reading the file at path.  LoadFile reads the entire file into memory, so it
// is not suitable for loading large data dumps, but can be useful for initializing
// schemas or loading indexes.
// FIXME: this does not really work with multi-statement files for mattn/go-sqlite3
// or the go-mysql-driver/mysql drivers;  pq seems to be an exception here.  Detecting
// this by requiring something with DriverName() and then attempting to split the
// queries will be difficult to get right, and its current driver-specific behavior
// is deemed at least not complex in its incorrectness.
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

// Execv (verbose) Exec's the query using the Execer and uses log.Println to
// print the query, result, and error in the event of an error.
func Execv(e Execer, query string, args ...interface{}) (sql.Result, error) {
	res, err := e.Exec(query, args...)
	if err != nil {
		log.Println(query, res, err)
	}
	return res, err
}

// Execl (log) runs Exec on the query and args and ses log.Println to
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

// Execf (fatal) runs Exec on the query and args and uses log.Fatal to
// print the query, result, and error in the event of an error.
func Execf(e Execer, query string, args ...interface{}) sql.Result {
	res, err := e.Exec(query, args...)
	if err != nil {
		log.Fatal(query, res, err)
	}
	return res
}

// Execp (panic) runs Exec on the query and args and panics on error.
func Execp(e Execer, query string, args ...interface{}) sql.Result {
	return MustExec(e, query, args...)
}

// MustExec (panic) is an alias for Execp.
func MustExec(e Execer, query string, args ...interface{}) sql.Result {
	res, err := e.Exec(query, args...)
	if err != nil {
		panic(err)
	}
	return res
}

// A map of names to field positions for destination structs
type fieldmap map[string]int

// A cache of fieldmaps for reflect Types
var (
	fieldmapCache     = map[reflect.Type]fieldmap{}
	fieldmapCacheLock sync.RWMutex
)

// dereference pointers to a particular kind
func deref(t reflect.Type, k reflect.Kind) (reflect.Type, error) {
	for {
		switch t.Kind() {
		case reflect.Ptr:
			t = t.Elem()
			continue
		case k:
			return t, nil
		default:
			return nil, fmt.Errorf("destination must be %s", k)
		}
	}
}

// BaseSliceType returns the type for a slice, dereferencing it if it is a pointer.
// Returns an error if the destination is not a slice or a pointer to a slice.
func BaseSliceType(t reflect.Type) (reflect.Type, error) {
	return deref(t, reflect.Slice)
}

// BaseStructType returns the type of a struct, dereferencing it if it is a pointer.
// Returns an error if the destination is not a struct or a pointer to a struct.
func BaseStructType(t reflect.Type) (reflect.Type, error) {
	return deref(t, reflect.Struct)
}

// commonly used reflect types.
var (
	scannerIface = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
	valuerIface  = reflect.TypeOf((*driver.Valuer)(nil)).Elem()
	timeType     = reflect.TypeOf(time.Time{})
)

// Create a fieldmap for a given type and return its fieldmap (or error)
// The fieldmap maps names to integers which represent the position of
// a struct field in a breadth first search of the fields.
func getFieldmap(t reflect.Type) (fm fieldmap, err error) {
	// if we have a fieldmap cached, return it
	t, err = BaseStructType(t)
	if err != nil {
		return nil, err
	}

	fieldmapCacheLock.RLock()
	fm, ok := fieldmapCache[t]
	fieldmapCacheLock.RUnlock()

	if ok {
		return fm, nil
	}

	fm = fieldmap{}
	queue := []reflect.Type{t}

	for i := 0; len(queue) != 0; {
		ty := queue[0]
		queue = queue[1:]
		for j := 0; j < ty.NumField(); j++ {
			f := ty.Field(j)
			ft := f.Type
			// skip unexported field
			if len(f.PkgPath) != 0 {
				continue
			}
			// perform one level of indirection for pointers to structs
			if ft.Kind() == reflect.Ptr {
				ft = ft.Elem()
			}
			if ft.Kind() == reflect.Struct && !reflect.PtrTo(ft).Implements(scannerIface) && ft != timeType {
				queue = append(queue, ft)
			} else {
				name := NameMapper(f.Name)
				if tag := f.Tag.Get("db"); tag != "" {
					name = tag
				}
				if _, ok := fm[name]; ok {
					// this name is already in the map, so skip it
					continue
				}
				if name == "-" {
					continue
				}
				fm[name] = i
				i++
			}
		}
	}
	fieldmapCacheLock.Lock()
	fieldmapCache[t] = fm
	fieldmapCacheLock.Unlock()
	return fm, nil
}

// Return the numeric fields corresponding to the columns
func getFields(fm fieldmap, columns []string) ([]int, error) {
	var num int
	var ok bool
	fields := make([]int, len(columns))
	for i, name := range columns {
		// find that name in the struct
		num, ok = fm[name]
		if !ok {
			return fields, errors.New("Could not find name " + name + " in interface")
		}
		fields[i] = num
	}
	return fields, nil
}

// Given a value for a struct, return a slice of values which are pointers
// to the well ordered fields in the struct, including embedded structs.
// The indexes of this list correspond to the indexes from the fieldmap.
func getValues(v reflect.Value) []interface{} {

	queue := []reflect.Value{v}
	fieldMap, _ := getFieldmap(v.Type())
	values := make([]interface{}, len(fieldMap))
	encountered := map[string]uint8{}
	var isPtr, isScanner, isValuer bool

	// if v is addressable, we return value pointers which are settable.
	// if v is not addressable, we return the values themselves, which are
	// then not settable.  This behavior is so that we can use getValues
	// in read-only contexts, like named binding.
	returnAddrs := v.CanAddr()

	for i := 0; len(queue) != 0; {
		vptr := queue[0]
		queue = queue[1:]
		for j := 0; j < vptr.NumField(); j++ {
			v := vptr.Field(j)
			vsf := vptr.Type().Field(j)
			vt := v.Type()
			isPtr = false
			isScanner = false

			// skip unexported fields
			if len(vsf.PkgPath) != 0 {
				continue
			}

			// skip duplicate names in the struct tree
			if _, ok := encountered[vsf.Name]; ok {
				continue
			}
			// skip fields with the db tag set to "-"
			if tag := vsf.Tag.Get("db"); tag == "-" {
				continue
			}

			encountered[vsf.Name] = 0
			if vt.Kind() == reflect.Ptr {
				vt = vt.Elem()
				isPtr = true
			}

			if isPtr || !returnAddrs {
				_, isScanner = v.Interface().(sql.Scanner)
				_, isValuer = v.Interface().(driver.Valuer)
			} else {
				_, isScanner = v.Addr().Interface().(sql.Scanner)
				_, isValuer = v.Addr().Interface().(driver.Valuer)
			}

			if vt.Kind() == reflect.Struct && !isScanner && !isValuer && vt != timeType {
				if isPtr {
					// Allocate a new struct for this poissibly nil pointer field, set it, and add to queue
					alloc := reflect.New(vt)
					v.Set(alloc)
					queue = append(queue, reflect.Indirect(v))
				} else {
					queue = append(queue, v)
				}
			} else {
				if !returnAddrs {
					values[i] = v.Interface()
				} else if returnAddrs {
					values[i] = v.Addr().Interface()
				}
				i++
			}
		}
	}

	return values
}

// Return a slice of values representing the columns
// These values are actually pointers into the addresses of struct fields
// The values interface must be initialized to the length of fields, ie
// make([]interface{}, len(fields)).  This function is the complement of
// the getFieldmap function, in that they enumerate struct fields the same way.
func setValues(fields []int, vptr reflect.Value, values []interface{}) {
	vals := getValues(vptr)
	for i, field := range fields {
		values[i] = vals[field]
	}
}

// SliceScan using this Rows.
func (r *Row) SliceScan() ([]interface{}, error) {
	return SliceScan(r)
}

// MapScan using this Rows.
func (r *Row) MapScan(dest map[string]interface{}) error {
	return MapScan(r, dest)
}

// StructScan a single Row into dest.
func (r *Row) StructScan(dest interface{}) error {
	if r.err != nil {
		return r.err
	}
	defer r.rows.Close()

	var v reflect.Value
	v = reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr {
		return errors.New("must pass a pointer, not a value, to StructScan destination")
	}

	direct := reflect.Indirect(v)
	base, err := BaseStructType(direct.Type())
	if err != nil {
		return err
	}

	fm, err := getFieldmap(base)
	if err != nil {
		return err
	}

	columns, err := r.Columns()
	if err != nil {
		return err
	}

	fields, err := getFields(fm, columns)
	if err != nil {
		return err
	}

	values := make([]interface{}, len(columns))
	// create a new struct type (which returns PtrTo) and indirect it
	setValues(fields, reflect.Indirect(v), values)
	// scan into the struct field pointers and append to our results
	return r.Scan(values...)
}

// SliceScan a row, returning a []interface{} with values similar to MapScan.
// This function is primarly intended for use where the number of columns
// is not known.  Because you can pass an []interface{} directly to Scan,
// it's recommended that you do that as it will not have to allocate new
// slices per row.
func SliceScan(r ColScanner) ([]interface{}, error) {
	// ignore r.started, since we needn't use reflect for anything.
	columns, err := r.Columns()
	if err != nil {
		return []interface{}{}, err
	}

	values := make([]interface{}, len(columns))
	for i := range values {
		values[i] = &sql.NullString{}
	}

	err = r.Scan(values...)

	if err != nil {
		return values, err
	}

	for i := range columns {
		ns := *(values[i].(*sql.NullString))
		if ns.Valid {
			values[i] = ns.String
		} else {
			values[i] = nil
		}
	}

	return values, r.Err()
}

// MapScan scans a single Row into the dest map[string]interface{}.
// Use this to get results for SQL that might not be under your control
// (for instance, if you're building an interface for an SQL server that
// executes SQL from input).  Please do not use this as a primary interface!
// This will modify the map sent to it in place, so do not reuse the same one
// on different queries or you may end up with something odd!  Columns which
// occur more than once in the result will overwrite eachother!
//
// The resultant map values will be string representations of the various
// SQL datatypes for existing values and a nil for null values.
func MapScan(r ColScanner, dest map[string]interface{}) error {
	// ignore r.started, since we needn't use reflect for anything.
	columns, err := r.Columns()
	if err != nil {
		return err
	}

	values := make([]interface{}, len(columns))
	for i := range values {
		values[i] = &sql.NullString{}
	}

	err = r.Scan(values...)
	if err != nil {
		return err
	}

	for i, column := range columns {
		ns := *(values[i].(*sql.NullString))
		if ns.Valid {
			dest[column] = ns.String
		} else {
			dest[column] = nil
		}
	}

	return r.Err()
}

// StructScan all rows from a sql.Rows into the dest slice.  StructScan destinations MUST
// have fields that map to every column in the result, and they MAY have fields
// in addition to those.  Fields are mapped to column names by lowercasing the
// field names by default:  use the struct tag `db` to specify exact column names
// for each field.
//
// StructScan will scan in the entire rows result, so if you need do not want to
// allocate structs for the entire result, use Queryx and see sqlx.Rows.StructScan.
func StructScan(rows *sql.Rows, dest interface{}) error {
	var v, vp reflect.Value
	var isPtr bool

	value := reflect.ValueOf(dest)
	if value.Kind() != reflect.Ptr {
		return errors.New("must pass a pointer, not a value, to StructScan destination")
	}

	direct := reflect.Indirect(value)

	slice, err := BaseSliceType(value.Type())
	if err != nil {
		return err
	}
	isPtr = slice.Elem().Kind() == reflect.Ptr
	base, err := BaseStructType(slice.Elem())
	if err != nil {
		return err
	}

	fm, err := getFieldmap(base)
	if err != nil {
		return err
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	fields, err := getFields(fm, columns)
	if err != nil {
		return err
	}
	// this will hold interfaces which are pointers to each field in the struct
	values := make([]interface{}, len(columns))
	for rows.Next() {
		// create a new struct type (which returns PtrTo) and indirect it
		vp = reflect.New(base)
		v = reflect.Indirect(vp)

		setValues(fields, v, values)

		// scan into the struct field pointers and append to our results
		err = rows.Scan(values...)
		if err != nil {
			return err
		}
		if isPtr {
			direct.Set(reflect.Append(direct, vp))
		} else {
			direct.Set(reflect.Append(direct, v))
		}
	}

	return rows.Err()
}

// NamedQuery uses BindStruct to get a query executable by the driver and
// then run Queryx on the result.  Returns an error from the binding or
// from the execution itself.
func NamedQuery(e Ext, query string, arg interface{}) (*Rows, error) {
	q, args, err := e.BindStruct(query, arg)
	if err != nil {
		return nil, err
	}
	return e.Queryx(q, args...)
}

// NamedExec uses BindStruct to get a query executable by the driver and
// then runs Exec on the result.  Returns an error from the binding
// or the query excution itself.
func NamedExec(e Ext, query string, arg interface{}) (sql.Result, error) {
	q, args, err := e.BindStruct(query, arg)
	if err != nil {
		return nil, err
	}
	return e.Exec(q, args...)
}

// NamedQueryMap runs a named query using a map instead of a struct.
func NamedQueryMap(e Ext, query string, argmap map[string]interface{}) (*Rows, error) {
	q, args, err := e.BindMap(query, argmap)
	if err != nil {
		return nil, err
	}
	return e.Queryx(q, args...)
}

// NamedExecMap executes a named query using a map instead of a struct.
func NamedExecMap(e Ext, query string, argmap map[string]interface{}) (sql.Result, error) {
	q, args, err := e.BindMap(query, argmap)
	if err != nil {
		return nil, err
	}
	return e.Exec(q, args...)
}
