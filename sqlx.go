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

type Rows struct {
	sql.Rows
	started bool
	fields  []int
	base    reflect.Type
	values  []interface{}
}

type Row struct {
	rows sql.Rows
	err  error
}

// An interface for something which can Execute sql queries (Tx, DB, Stmt)
type Queryer interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
}

// An interface for something which can Execute sql commands (Tx, DB, Stmt)
type Execer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

// An interface for something which can Prepare sql statements (Tx, DB)
type Preparer interface {
	Prepare(query string) (*sql.Stmt, error)
}

// Same implementation as database/sql.Row.Scan
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
	err := r.rows.Scan(dest...)
	if err != nil {
		return err
	}

	return nil
}

func (r *Row) Columns() ([]string, error) {
	if r.err != nil {
		return []string{}, r.err
	}
	return r.rows.Columns()
}

// An sqlx wrapper around database/sql's DB with extra functionality
type DB struct{ sql.DB }

// Same as database/sql's Open, but returns an *sqlx.DB instead.
func Open(driverName, dataSourceName string) (*DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	return &DB{*db}, err
}

// Call Select using this db to issue the query.
func (db *DB) Select(dest interface{}, query string, args ...interface{}) error {
	return Select(db, dest, query, args...)
}

// Call Get using this db to issue the query.
func (db *DB) Get(dest interface{}, query string, args ...interface{}) error {
	return Get(db, dest, query, args...)
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

// QueryRowx is the same as QueryRow, but returns an *sqlx.Row instead of *sql.Row
func (db *DB) QueryRowx(query string, args ...interface{}) *Row {
	r, err := db.DB.Query(query, args...)
	return &Row{rows: *r, err: err}
}

// Execv ("verbose") runs Execv using this database.
func (db *DB) Execv(query string, args ...interface{}) (sql.Result, error) {
	return Execv(db, query, args...)
}

// Execl ("log") runs Execl using this database.
func (db *DB) Execl(query string, args ...interface{}) sql.Result {
	return Execl(db, query, args...)
}

// Execf ("fatal") runs Execf using this database.
func (db *DB) Execf(query string, args ...interface{}) sql.Result {
	return Execf(db, query, args...)
}

// Execp ("panic") runs Execp using this database.
func (db *DB) Execp(query string, args ...interface{}) sql.Result {
	return Execp(db, query, args...)
}

// MustExec ("panic") runs MustExec using this database.
func (db *DB) MustExec(query string, args ...interface{}) sql.Result {
	return MustExec(db, query, args...)
}

// Preparex returns an sqlx.Stmt instead of a sql.Stmt
func (db *DB) Preparex(query string) (*Stmt, error) {
	return Preparex(db, query)
}

// An sqlx wrapper around database/sql's Tx with extra functionality
type Tx struct{ sql.Tx }

// Call LoadFile using this transaction to issue the Exec.
func (tx *Tx) LoadFile(path string) (*sql.Result, error) {
	return LoadFile(tx, path)
}

// Call Select using this transaction to issue the Query.
func (tx *Tx) Select(dest interface{}, query string, args ...interface{}) error {
	return Select(tx, dest, query, args...)
}

func (tx *Tx) Queryx(query string, args ...interface{}) (*Rows, error) {
	r, err := tx.Tx.Query(query, args...)
	return &Rows{Rows: *r}, err
}

func (tx *Tx) QueryRowx(query string, args ...interface{}) *Row {
	r, err := tx.Tx.Query(query, args...)
	return &Row{rows: *r, err: err}
}

// Call Get using this transaction to issue the query.
func (tx *Tx) Get(dest interface{}, query string, args ...interface{}) error {
	return Get(tx, dest, query, args...)
}

// Call Select using this transaction to issue the Query.
func (tx *Tx) Selectv(dest interface{}, query string, args ...interface{}) error {
	return Selectv(tx, dest, query, args...)
}

// Call Selectf using this transaction to issue the Query.
func (tx *Tx) Selectf(dest interface{}, query string, args ...interface{}) {
	Selectf(tx, dest, query, args...)
}

// Execv ("verbose") runs Execv using this transaction.
func (tx *Tx) Execv(query string, args ...interface{}) (sql.Result, error) {
	return Execv(tx, query, args...)
}

// Execl ("log") runs Execl using this transaction.
func (tx *Tx) Execl(query string, args ...interface{}) sql.Result {
	return Execl(tx, query, args...)
}

// Execf ("fatal") runs Execf using this transaction.
func (tx *Tx) Execf(query string, args ...interface{}) sql.Result {
	return Execf(tx, query, args...)
}

// Execp ("panic") runs Execp using this transaction.
func (tx *Tx) Execp(query string, args ...interface{}) sql.Result {
	return Execp(tx, query, args...)
}

// MustExec ("panic") runs MustExec using this transaction.
func (tx *Tx) MustExec(query string, args ...interface{}) sql.Result {
	return MustExec(tx, query, args...)
}

func (tx *Tx) Preparex(query string) (*Stmt, error) {
	st, err := tx.Tx.Prepare(query)
	return &Stmt{*st}, err
}

// Returns a transaction prepared statement given the provided statement,
// which can be either an sql.Stmt or an sqlx.Stmt
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

// this unexposed wrapper lets you use a Stmt as a Queryer & Execer
type qStmt struct{ Stmt }

func (q *qStmt) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return q.Stmt.Query(args...)
}

func (q *qStmt) QueryRowx(query string, args ...interface{}) *Row {
	r, err := q.Stmt.Query(args...)
	return &Row{rows: *r, err: err}
}

func (q *qStmt) Exec(query string, args ...interface{}) (sql.Result, error) {
	return q.Stmt.Exec(args...)
}

// Call Select using this statement to issue the Query.
func (s *Stmt) Select(dest interface{}, args ...interface{}) error {
	return Select(&qStmt{*s}, dest, "", args...)
}

// Call Get using this statement to issue the query.
func (s *Stmt) Get(dest interface{}, query string, args ...interface{}) error {
	return Get(&qStmt{*s}, dest, query, args...)
}

// Call Selectv using this statement to issue the Query.
func (s *Stmt) Selectv(dest interface{}, args ...interface{}) error {
	return Selectv(&qStmt{*s}, dest, "", args...)
}

// Call Selectf using this statement to issue the Query.
func (s *Stmt) Selectf(dest interface{}, args ...interface{}) {
	Selectf(&qStmt{*s}, dest, "", args...)
}

// Execv ("verbose") runs Execv using this statement.  Note that the query is
// not recoverable once a statement has been prepared, so the query portion
// will be blank.
func (s *Stmt) Execv(args ...interface{}) (sql.Result, error) {
	return Execv(&qStmt{*s}, "", args...)
}

// Execl ("log") runs Execl using this statement.  Note that the query is
// not recoverable once a statement has been prepared, so the query portion
// will be blank.
func (s *Stmt) Execl(args ...interface{}) sql.Result {
	return Execl(&qStmt{*s}, "", args...)
}

// Execf ("fatal") runs Execf using this statement.  Note that the query is
// not recoverable once a statement has been prepared, so the query portion
// will be blank.
func (s *Stmt) Execf(args ...interface{}) sql.Result {
	return Execf(&qStmt{*s}, "", args...)
}

// Execp ("panic") runs Execp using this statement.
func (s *Stmt) Execp(args ...interface{}) sql.Result {
	return Execp(&qStmt{*s}, "", args...)
}

// MustExec ("panic") runs MustExec using this statement.
func (s *Stmt) MustExec(args ...interface{}) sql.Result {
	return MustExec(&qStmt{*s}, "", args...)
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

// Connect to a database and panic on error.  Similar to sql.Open, but attempts
// a simple `SELECT 1` statement to ensure that the connection is made and successful.
func MustConnect(driverName, dataSourceName string) *DB {
	db, err := Open(driverName, dataSourceName)
	if err != nil {
		panic(err)
	}
	err = db.Ping()
	if err != nil {
		panic(err)
	}
	return db
}

// Preparex prepares a statement given a Preparer (Tx, DB), returning an sqlx
// wrapped *Stmt.
func Preparex(p Preparer, query string) (*Stmt, error) {
	s, err := p.Prepare(query)
	return &Stmt{*s}, err
}

// Select uses a Queryer (*DB or *Tx, by default), issues the query w/ args
// via that Queryer, and sets the dest slice using rows.StructScan
func Select(q Queryer, dest interface{}, query string, args ...interface{}) error {
	rows, err := q.Query(query, args...)
	if err != nil {
		return err
	}
	// StructScan will exhaust the rows here, which we are never returning to
	// the caller, so we have to close it
	defer rows.Close()
	return StructScan(rows, dest)
}

// Get uses a queryer (*DB, *Tx, or *qStmt by default), issues a QueryRow w/ args
// via that Queryer and sets the dest interface using row.StructScan
func Get(q Queryer, dest interface{}, query string, args ...interface{}) error {
	rows, err := q.Query(query, args...)
	r := &Row{rows: *rows, err: err}
	return r.StructScan(dest)
}

// Selectv ("verbose") runs Select on its arguments and uses log.Println to print
// the query and the error in the event of an error.
func Selectv(q Queryer, dest interface{}, query string, args ...interface{}) error {
	err := Select(q, dest, query, args...)
	if err != nil {
		log.Println(query, err)
	}
	return err
}

// Selectf ("fatal") runs Select on its arguments and uses log.Fatal to print
// the query and the error in the event of an error.
func Selectf(q Queryer, dest interface{}, query string, args ...interface{}) {
	err := Select(q, dest, query, args...)
	if err != nil {
		log.Fatal(query, err)
	}
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

// MustExec ("panic") runs Exec on the query and args and panics on error.  Since
// the panic interrupts the control flow, errors are not returned to the caller.
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

// Return the underlying slice's type, or an error if the type is
// not a slice or a pointer to a slice.
func BaseSliceType(t reflect.Type) (reflect.Type, error) {
	switch t.Kind() {
	case reflect.Ptr:
		t = t.Elem()
		fallthrough
	case reflect.Slice:
	default:
		return nil, errors.New("Destination must be a slice.")
	}
	return t, nil
}

// Return a reflect.Type's base struct type, or an error if it is not a struct
// or pointer to a struct.
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

func (r *Row) StructScan(dest interface{}) error {
	var v, vp reflect.Value
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

	var num int
	var ok bool

	fields := make([]int, len(columns))

	for i, name := range columns {
		// find that name in the struct
		num, ok = fm[name]
		if !ok {
			fmt.Println(fm)
			return errors.New("Could not find name " + name + " in interface")
		}
		fields[i] = num
	}

	values := make([]interface{}, len(columns))
	// create a new struct type (which returns PtrTo) and indirect it
	vp = reflect.Indirect(v)
	for i, field := range fields {
		values[i] = vp.Field(field).Addr().Interface()
	}

	// scan into the struct field pointers and append to our results
	return r.Scan(values...)
}

// Fully scan a sql.Rows result into the dest slice.
//
// StructScan can incompletely fill a struct, and will also work with
// any values order returned by the sql driver.
// StructScan will scan in the entire rows result, so if you need to iterate
// one at a time (to reduce memory usage, eg) avoid it.
func StructScan(rows *sql.Rows, dest interface{}) error {
	var v, vp reflect.Value
	var ok, isPtr bool

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

	var num int
	fields := make([]int, len(columns))

	for i, name := range columns {
		// find that name in the struct
		num, ok = fm[name]
		if !ok {
			fmt.Println(fm)
			return errors.New("Could not find name " + name + " in interface")
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
