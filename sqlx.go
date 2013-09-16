package sqlx

import (
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"reflect"
	"strings"
)

// A wrapper around sql.Rows which caches costly reflect operations
// during a looped StructScan
type Rows struct {
	sql.Rows
	started bool
	fields  []int
	base    reflect.Type
	values  []interface{}
}

// A reimplementation of sql.Row in order to gain access to the underlying
// sql.Rows.Columns() data, necessary for StructScan.
type Row struct {
	rows sql.Rows
	err  error
}

// An interface for something which can Execute sql queries (Tx, DB, Stmt)
type Queryer interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	Queryx(query string, args ...interface{}) (*Rows, error)
	QueryRowx(query string, args ...interface{}) *Row
}

// An interface for something which can Execute sql commands (Tx, DB, Stmt)
type Execer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

// An interface for something which can bind queries (Tx, DB)
type Binder interface {
	DriverName() string
	Rebind(string) string
	BindMap(string, map[string]interface{}) (string, []interface{}, error)
	BindStruct(string, interface{}) (string, []interface{}, error)
}

// A union interface which can bind, query, and exec (Tx, DB), used for
// NamedQuery and NamedExec, which requires exec/query and BindMap/Struct
type Ext interface {
	Binder
	Queryer
	Execer
}

// An interface for something which can Prepare sql statements (Tx, DB)
type Preparer interface {
	Prepare(query string) (*sql.Stmt, error)
}

// Same implementation as sql.Row.Scan
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
		return sql.ErrNoRows
	}
	return r.rows.Scan(dest...)
}

// Return the underlying sql.Rows.Columns(), or the deferred error usually
// returned by Row.Scan()
func (r *Row) Columns() ([]string, error) {
	if r.err != nil {
		return []string{}, r.err
	}
	return r.rows.Columns()
}

// An wrapper around sql.DB which keeps track of the driverName upon Open,
// used mostly to automatically bind named queries using the right bindvars.
type DB struct {
	sql.DB
	driverName string
}

func NewDb(db *sql.DB, driverName string) *DB {
	return &DB{*db, driverName}
}

// Returns the driverName passed to the Open function for this DB.
func (db *DB) DriverName() string {
	return db.driverName
}

// Same as database/sql's Open, but returns an *sqlx.DB instead.
func Open(driverName, dataSourceName string) (*DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	return &DB{*db, driverName}, err
}

// Rebinds a query from QUESTION to the DB driver's bindvar type.
func (db *DB) Rebind(query string) string {
	return Rebind(BindType(db.driverName), query)
}

// BindMap's a query using the DB driver's bindvar type.
func (db *DB) BindMap(query string, argmap map[string]interface{}) (string, []interface{}, error) {
	return BindMap(BindType(db.driverName), query, argmap)
}

// BindStruct's a query using the DB driver's bindvar type.
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

// Begin starts a transaction, and panics on error.  Returns an *sqlx.Tx instead
// of an *sql.Tx.
func (db *DB) MustBegin() *Tx {
	tx, err := db.Beginx()
	if err != nil {
		panic(err)
	}
	return tx
}

// Same as Begin, but returns an *sqlx.Tx instead of an *sql.Tx.
func (db *DB) Beginx() (*Tx, error) {
	if tx, err := db.DB.Begin(); err != nil {
		return nil, err
	} else {
		return &Tx{*tx, db.driverName}, err
	}
}

// Same as Query, but returns an *sqlx.Rows instead of *sql.Rows.
func (db *DB) Queryx(query string, args ...interface{}) (*Rows, error) {
	if r, err := db.DB.Query(query, args...); err != nil {
		return nil, err
	} else {
		return &Rows{Rows: *r}, err
	}
}

// Same as QueryRow, but returns an *sqlx.Row instead of *sql.Row.
func (db *DB) QueryRowx(query string, args ...interface{}) *Row {
	if r, err := db.DB.Query(query, args...); err != nil {
		return &Row{err: err}
	} else {
		return &Row{rows: *r, err: err}
	}
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

// An sqlx wrapper around database/sql's Tx with extra functionality
type Tx struct {
	sql.Tx
	driverName string
}

// Return the driverName used by the DB which began a transaction.
func (tx *Tx) DriverName() string {
	return tx.driverName
}

// Rebind a query within a transaction's bindvar type.
func (tx *Tx) Rebind(query string) string {
	return Rebind(BindType(tx.driverName), query)
}

// BindMap's a query within a transaction's bindvar type.
func (tx *Tx) BindMap(query string, argmap map[string]interface{}) (string, []interface{}, error) {
	return BindMap(BindType(tx.driverName), query, argmap)
}

// BindStruct's a query within a transaction's bindvar type.
func (tx *Tx) BindStruct(query string, arg interface{}) (string, []interface{}, error) {
	return BindStruct(BindType(tx.driverName), query, arg)
}

// NamedQuery within a transaction.
func (tx *Tx) NamedQuery(query string, arg interface{}) (*Rows, error) {
	return NamedQuery(tx, query, arg)
}

// Exec a named query within a transaction.
func (tx *Tx) NamedExec(query string, arg interface{}) (sql.Result, error) {
	return NamedExec(tx, query, arg)
}

// LoadFile within a transaction.
func (tx *Tx) LoadFile(path string) (*sql.Result, error) {
	return LoadFile(tx, path)
}

// Select within a transaction.
func (tx *Tx) Select(dest interface{}, query string, args ...interface{}) error {
	return Select(tx, dest, query, args...)
}

// Query within a transaction, returning *sqlx.Rows instead of *sql.Rows.
func (tx *Tx) Queryx(query string, args ...interface{}) (*Rows, error) {
	if r, err := tx.Tx.Query(query, args...); err != nil {
		return nil, err
	} else {
		return &Rows{Rows: *r}, err
	}
}

// QueryRow within a transaction, returning *sqlx.Row instead of *sql.Row.
func (tx *Tx) QueryRowx(query string, args ...interface{}) *Row {
	if r, err := tx.Tx.Query(query, args...); err != nil {
		return &Row{err: err}
	} else {
		return &Row{rows: *r, err: err}
	}
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

// MustExec (panic) runs MustExec within a transaction.
func (tx *Tx) MustExec(query string, args ...interface{}) sql.Result {
	return MustExec(tx, query, args...)
}

// Prepare's a statement within a transaction, returning a *sqlx.Stmt instead of an *sql.Stmt.
func (tx *Tx) Preparex(query string) (*Stmt, error) {
	return Preparex(tx, query)
}

// Returns a version of the prepared statement which runs within a transaction.  Provided
// stmt can be either *sql.Stmt or *sqlx.Stmt, and the return value is always *sqlx.Stmt.
func (tx *Tx) Stmtx(stmt interface{}) *Stmt {
	var st sql.Stmt
	var s *sql.Stmt
	switch stmt.(type) {
	case sql.Stmt:
		st = stmt.(sql.Stmt)
	case Stmt:
		st = stmt.(Stmt).Stmt
	}
	s = tx.Stmt(&st)
	return &Stmt{*s}
}

// An sqlx wrapper around database/sql's Stmt with extra functionality
// Although a Stmt's interface differs from Tx and DB's, internally,
// a wrapper is used to satisfy the Queryer & Execer interfaces.
type Stmt struct{ sql.Stmt }

// this unexposed wrapper lets you use a Stmt as a Queryer & Execer by
// implementing those interfaces but ignoring the `query` argument.
type qStmt struct{ Stmt }

func (q *qStmt) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return q.Stmt.Query(args...)
}

func (q *qStmt) Queryx(query string, args ...interface{}) (*Rows, error) {
	if r, err := q.Stmt.Query(args...); err != nil {
		return nil, err
	} else {
		return &Rows{Rows: *r}, err
	}
}

func (q *qStmt) QueryRowx(query string, args ...interface{}) *Row {
	if r, err := q.Stmt.Query(args...); err != nil {
		return &Row{err: err}
	} else {
		return &Row{rows: *r, err: err}
	}
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

// Execf (panic) using this statement.  Note that the query portion of the error
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

// Like sql.Rows.Scan, but scans a single Row into a single Struct.  Use this
// and iterate over Rows manually when the memory load of Select() might be
// prohibitive.  *Rows.StructScan caches the reflect work of matching up
// column positions to fields to avoid that overhead per scan, which means it
// is not safe to run StructScan on the same Rows instance with different
// struct types.
func (r *Rows) StructScan(dest interface{}) error {
	var v reflect.Value
	v = reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr {
		return errors.New("Must pass a pointer, not a value, to StructScan destination.")
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
	r.Scan(r.values...)
	return nil
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

// Connect, but panic on error.
func MustConnect(driverName, dataSourceName string) *DB {
	db, err := Connect(driverName, dataSourceName)
	if err != nil {
		panic(err)
	}
	return db
}

// Preparex prepares a statement given a Preparer (Tx, DB), returning an *sqlx.Stmt.
func Preparex(p Preparer, query string) (*Stmt, error) {
	if s, err := p.Prepare(query); err != nil {
		return nil, err
	} else {
		return &Stmt{*s}, err
	}
}

// Query using the provided Queryer, and StructScan each row into dest, which must
// be a slice of structs.  The resulting *sql.Rows are closed automatically.
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

// QueryRow using the provided Queryer, and StructScan the resulting row into dest,
// which must be a pointer to a struct.  If there was no row, Get will return sql.ErrNoRows.
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
	res, err := e.Exec(query, args...)
	if err != nil {
		panic(err)
	}
	return res
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
var fieldmapCache = map[reflect.Type]fieldmap{}

// Return the type for a slice, dereferencing it if it is a pointer.  Returns
// an error if the destination is not a slice or a pointer to a slice.
func BaseSliceType(t reflect.Type) (reflect.Type, error) {
start:
	switch t.Kind() {
	case reflect.Ptr:
		t = t.Elem()
		goto start
	case reflect.Slice:
	default:
		return nil, errors.New("Destination must be a slice.")
	}
	return t, nil
}

// Return the type of a struct, dereferencing it if it is a pointer.  Returns
// an error if the destination is not a struct or a pointer to a struct.
func BaseStructType(t reflect.Type) (reflect.Type, error) {
	switch t.Kind() {
	case reflect.Ptr:
		t = t.Elem()
		fallthrough
	case reflect.Struct:
	default:
		return nil, errors.New("Destination must be a struct type.")
	}
	return t, nil
}

// Create a fieldmap for a given type and return its fieldmap (or error)
func getFieldmap(t reflect.Type) (fm fieldmap, err error) {
	// if we have a fieldmap cached, return it
	t, err = BaseStructType(t)
	if err != nil {
		return nil, err
	}
	fm, ok := fieldmapCache[t]
	if ok {
		return fm, nil
	} else {
		fm = fieldmap{}
	}

	var f reflect.StructField
	var name string

	for i := 0; i < t.NumField(); i++ {
		f = t.Field(i)
		name = strings.ToLower(f.Name)
		if tag := f.Tag.Get("db"); tag != "" {
			name = tag
		}
		fm[name] = i
	}
	fieldmapCache[t] = fm
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
			fmt.Println(fm)
			return fields, errors.New("Could not find name " + name + " in interface")
		}
		fields[i] = num
	}
	return fields, nil
}

// Return a slice of values representing the columns
// These values are actually pointers into the addresses of struct fields
// The values interface must be initialized to the length of fields, ie
// make([]interface{}, len(fields)).
func setValues(fields []int, vptr reflect.Value, values []interface{}) {
	for i, field := range fields {
		values[i] = vptr.Field(field).Addr().Interface()
	}
}

// StructScan's a single Row (result of QueryRowx) into dest
func (r *Row) StructScan(dest interface{}) error {
	var v reflect.Value
	v = reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr {
		return errors.New("Must pass a pointer, not a value, to StructScan destination.")
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

// Fully scan a sql.Rows result into the dest slice.  StructScan destinations MUST
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
		return errors.New("Must pass a pointer, not a value, to StructScan destination.")
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

	return nil
}

// Issue a named query using BindStruct to get a query executable
// by the driver and then run Queryx on the result.  May return an error
// from the binding or from the execution itself.
func NamedQuery(e Ext, query string, arg interface{}) (*Rows, error) {
	q, args, err := e.BindStruct(query, arg)
	if err != nil {
		return nil, err
	}
	return e.Queryx(q, args...)
}

// Like NamedQuery, but use Exec instead of Queryx.
func NamedExec(e Ext, query string, arg interface{}) (sql.Result, error) {
	q, args, err := e.BindStruct(query, arg)
	if err != nil {
		return nil, err
	}
	return e.Exec(q, args...)
}

// Issue a named query using BindMap to get a query executable by the driver
// and then run Queryx on the result.  May return an error from the binding
// or from the query execution itself.
func NamedQueryMap(e Ext, query string, argmap map[string]interface{}) (*Rows, error) {
	q, args, err := e.BindMap(query, argmap)
	if err != nil {
		return nil, err
	}
	return e.Queryx(q, args...)
}

// Like NamedQuery, but use Exec instead of Queryx.
func NamedExecMap(e Ext, query string, argmap map[string]interface{}) (sql.Result, error) {
	q, args, err := e.BindMap(query, argmap)
	if err != nil {
		return nil, err
	}
	return e.Exec(q, args...)
}
