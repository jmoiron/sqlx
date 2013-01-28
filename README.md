
# sqlx
  
import "github.com/jmoiron/sqlx"

### General purpose extensions to database/sql

sqlx is intended to seamlessly wrap database/sql and provide some
convenience methods which range from basic common error handling
techniques to complex reflect-base Scan extensions. You should be able
to integrate sqlx into your current codebase by replacing:

	import "database/sql"

with:

	import sql "github.com/jmoiron/sqlx"

## FUNCTIONS

###func Execf(e Execer, query string, args ...interface{}) sql.Result
Execf ("fatal") runs Exec on the query and args and uses log.Fatal to
print the query, result, and error in the event of an error. Since
errors are non-recoverable, only a Result is returned on success.

###func Execl(e Execer, query string, args ...interface{}) sql.Result
Execl ("log") runs Exec on the query and args and ses log.Println to
print the query, result, and error in the event of an error. Unlike
Execv, Execl does not return the error, and can be used in single-value
contexts.

Do not abuse Execl; it is convenient for experimentation but generally
not for production use.

###func Execp(e Execer, query string, args ...interface{}) sql.Result
    Execp ("panic") runs Exec on the query and args and panics on error.
    Since the panic interrupts the control flow, errors are not returned to
    the caller.

###func Execv(e Execer, query string, args ...interface{}) (sql.Result, error)
    Execv ("verbose") runs Exec on the query and args and uses log.Println
    to print the query, result, and error in the event of an error. Since
    Execv returns flow to the caller, it returns the result and error.

###func LoadFile(e Execer, path string) (\*sql.Result, error)
    LoadFile exec's every statement in a file (as a single call to Exec).
    LoadFile returns a nil pointer and error if an error is encountered
    since errors can be encountered locating or reading the file, before a
    Result is created. LoadFile reads the entire file into memory, so it is
    not suitable for loading large data dumps, but can be useful for
    initializing database schemas or loading indexes.

###func Select(q Querier, typ interface{}, query string, args ...interface{}) ([]interface{}, error)
    Select uses a Querier (*DB or *Tx, by default), issues the query w/ args
    via that Querier, and returns the results as a slice of typs.

###func StructScan(rows *sql.Rows, typ interface{}) ([]interface{}, error)
    Fully scan a sql.Rows result into a slice of "typ" typed structs.

    StructScan can incompletely fill a struct, and will also work with any
    values order returned by the sql driver. StructScan will scan in the
    entire rows result, so if you need to iterate one at a time (to reduce
    memory usage, eg) avoid it.


##TYPES

##type DB struct{ sql.DB }
    An sqlx wrapper around database/sql's DB with extra functionality

###func Open(driverName, dataSourceName string) (\*DB, error)
    Same as database/sql's Open, but returns an *sqlx.DB instead.

###func (db \*DB) Beginx() (\*Tx, error)
    Beginx is the same as Begin, but returns an *sqlx.Tx instead of an
    *sql.Tx

###func (db \*DB) LoadFile(path string) (\*sql.Result, error)
    Call LoadFile using this db to issue the Exec.

###func (db \*DB) MustBegin() \*Tx
    Begin starts a transaction, and panics on error. Returns an *sqlx.Tx
    instead of an *sql.Tx.

###func (db \*DB) Queryx(query string, args ...interface{}) (\*Rows, error)
    Queryx is the same as Query, but returns an *sqlx.Rows instead of
    *sql.Rows

###func (db \*DB) Select(typ interface{}, query string, args ...interface{}) ([]interface{}, error)
    Call Select using this db to issue the query.

###type Execer interface {
    Exec(query string, args ...interface{}) (sql.Result, error)
}
    An interface for something which can Execute sql commands (Tx, DB)

###type Querier interface {
    Query(query string, args ...interface{}) (*sql.Rows, error)
}
    An interface for something which can Execute sql queries (Tx, DB)

###type Rows struct {
    sql.Rows
    // contains filtered or unexported fields
}

###func (r \*Rows) StructScan(typ interface{}) (interface{}, error)
    Like sql.Rows.Scan, but scans a single Row into a single Struct. Use
    this and iterate over Rows manually when the memory load of Select()
    might be prohibitive. *Rows.StructScan caches the reflect work of
    matching up column positions to fields to avoid that overhead per scan,
    which means it is not safe to run StructScan on the same Rows instance
    with different struct types.

###type Stmt struct{ sql.Stmt }

###type Tx struct{ sql.Tx }
    An sqlx wrapper around database/sql's Tx with extra functionality

###func (tx \*Tx) LoadFile(path string) (\*sql.Result, error)
    Call LoadFile using this transaction to issue the Exec.

###func (tx \*Tx) Select(typ interface{}, query string, args ...interface{}) ([]interface{}, error)
    Call Select using this transaction to issue the Query.


