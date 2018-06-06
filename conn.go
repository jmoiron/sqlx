// +build go1.9

package sqlx

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx/reflectx"
)

// Return a single connection using this DB.
// Queries run on the same Conn will be run in the same database session.
// Every Conn must be returned to the database pool after use by calling Conn.Close.
func (db *DB) Connx() (*Conn, error) {
	return db.ConnxContext(context.Background())
}

// Return a single connection using this DB.
// Queries run on the same Conn will be run in the same database session.
// Every Conn must be returned to the database pool after use by calling Conn.Close.
func (db *DB) ConnxContext(ctx context.Context) (*Conn, error) {
	conn, err := db.DB.Conn(ctx)
	if err != nil {
		return nil, err
	}
	return &Conn{Conn: conn, driverName: db.driverName, unsafe: db.unsafe, Mapper: db.Mapper}, err
}

// Conn is an sqlx wrapper around sql.Conn with extra functionality
type Conn struct {
	*sql.Conn
	driverName string
	unsafe     bool
	Mapper     *reflectx.Mapper
}

// DriverName returns the driverName used by the DB which opened this connection.
func (conn *Conn) DriverName() string {
	return conn.driverName
}

// Rebind a query within a connection's bindvar type.
func (conn *Conn) Rebind(query string) string {
	return Rebind(BindType(conn.driverName), query)
}

// Unsafe returns a version of Conn which will silently succeed to scan when
// columns in the SQL result have no fields in the destination struct.
func (conn *Conn) Unsafe() *Conn {
	return &Conn{Conn: conn.Conn, driverName: conn.driverName, unsafe: true, Mapper: conn.Mapper}
}

// BindNamed binds a query within a connection's bindvar type.
func (conn *Conn) BindNamed(query string, arg interface{}) (string, []interface{}, error) {
	return bindNamedMapper(BindType(conn.driverName), query, arg, conn.Mapper)
}

// NamedQuery within a connection.
// Any named placeholder parameters are replaced with fields from arg.
func (conn *Conn) NamedQuery(query string, arg interface{}) (*Rows, error) {
	return NamedQuery(conn, query, arg)
}

// NamedQueryContext using this connection.
// Any named placeholder parameters are replaced with fields from arg.
func (conn *Conn) NamedQueryContext(ctx context.Context, query string, arg interface{}) (*Rows, error) {
	return NamedQueryContext(ctx, conn, query, arg)
}

// NamedExec a named query within a connection.
// Any named placeholder parameters are replaced with fields from arg.
func (conn *Conn) NamedExec(query string, arg interface{}) (sql.Result, error) {
	return NamedExec(conn, query, arg)
}

// NamedExecContext using this connection.
// Any named placeholder parameters are replaced with fields from arg.
func (conn *Conn) NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error) {
	return NamedExecContext(ctx, conn, query, arg)
}

// Beginx begins a transaction and returns an *sqlx.Tx instead of an *sql.Tx.
func (conn *Conn) Beginx() (*Tx, error) {
	return conn.BeginTxx(context.Background(), nil)
}

// MustBegin starts a transaction, and panics on error.  Returns an *sqlx.Tx instead
// of an *sql.Tx.
func (conn *Conn) MustBegin() *Tx {
	tx, err := conn.Beginx()
	if err != nil {
		panic(err)
	}
	return tx
}

// BeginTxx begins a transaction and returns an *sqlx.Tx instead of an
// *sql.Tx.
//
// The provided context is used until the transaction is committed or rolled
// back. If the context is canceled, the sql package will roll back the
// transaction. Tx.Commit will return an error if the context provided to
// BeginxContext is canceled.
func (conn *Conn) BeginTxx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := conn.Conn.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{Tx: tx, driverName: conn.driverName, unsafe: conn.unsafe, Mapper: conn.Mapper}, err
}

// MustBeginTx starts a transaction, and panics on error.  Returns an *sqlx.Tx instead
// of an *sql.Tx.
//
// The provided context is used until the transaction is committed or rolled
// back. If the context is canceled, the sql package will roll back the
// transaction. Tx.Commit will return an error if the context provided to
// MustBeginContext is canceled.
func (conn *Conn) MustBeginTx(ctx context.Context, opts *sql.TxOptions) *Tx {
	tx, err := conn.BeginTxx(ctx, opts)
	if err != nil {
		panic(err)
	}
	return tx
}

// Select using this connection.
// Any placeholder parameters are replaced with supplied args.
func (conn *Conn) Select(dest interface{}, query string, args ...interface{}) error {
	return Select(conn, dest, query, args...)
}

// SelectContext using this connection.
// Any placeholder parameters are replaced with supplied args.
func (conn *Conn) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return SelectContext(ctx, conn, dest, query, args...)
}

// Query using this connection.
// Any placeholder parameters are replaced with supplied args.
func (conn *Conn) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return conn.Conn.QueryContext(context.Background(), query, args...)
}

// Queryx using this connection.
// Any placeholder parameters are replaced with supplied args.
func (conn *Conn) Queryx(query string, args ...interface{}) (*Rows, error) {
	r, err := conn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	return &Rows{Rows: r, unsafe: conn.unsafe, Mapper: conn.Mapper}, err
}

// QueryxContext queries the database and returns an *sqlx.Rows.
// Any placeholder parameters are replaced with supplied args.
func (conn *Conn) QueryxContext(ctx context.Context, query string, args ...interface{}) (*Rows, error) {
	r, err := conn.Conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &Rows{Rows: r, unsafe: conn.unsafe, Mapper: conn.Mapper}, err
}

// QueryRowx using this connection.
// Any placeholder parameters are replaced with supplied args.
func (conn *Conn) QueryRowx(query string, args ...interface{}) *Row {
	rows, err := conn.Query(query, args...)
	return &Row{rows: rows, err: err, unsafe: conn.unsafe, Mapper: conn.Mapper}
}

// QueryRowxContext queries the database and returns an *sqlx.Row.
// Any placeholder parameters are replaced with supplied args.
func (conn *Conn) QueryRowxContext(ctx context.Context, query string, args ...interface{}) *Row {
	rows, err := conn.Conn.QueryContext(ctx, query, args...)
	return &Row{rows: rows, err: err, unsafe: conn.unsafe, Mapper: conn.Mapper}
}

// Get using this connection.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (conn *Conn) Get(dest interface{}, query string, args ...interface{}) error {
	return Get(conn, dest, query, args...)
}

// GetContext using this connection.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (conn *Conn) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return GetContext(ctx, conn, dest, query, args...)
}

// Exec using this connection.
// Any placeholder parameters are replaced with supplied args.
func (conn *Conn) Exec(query string, args ...interface{}) (sql.Result, error) {
	return conn.Conn.ExecContext(context.Background(), query, args...)
}

// MustExec (panic) using this connection.
// Any placeholder parameters are replaced with supplied args.
func (conn *Conn) MustExec(query string, args ...interface{}) sql.Result {
	return MustExec(conn, query, args...)
}

// MustExecContext (panic) runs MustExecContext using this connection.
// Any placeholder parameters are replaced with supplied args.
func (conn *Conn) MustExecContext(ctx context.Context, query string, args ...interface{}) sql.Result {
	return MustExecContext(ctx, conn, query, args...)
}

// Prepare a statement using this connection.
func (conn *Conn) Prepare(query string) (*sql.Stmt, error) {
	return conn.PrepareContext(context.Background(), query)
}

// Preparex a statement using this connection.
func (conn *Conn) Preparex(query string) (*Stmt, error) {
	return Preparex(conn, query)
}

// PreparexContext returns an sqlx.Stmt instead of a sql.Stmt.
//
// The provided context is used for the preparation of the statement, not for
// the execution of the statement.
func (conn *Conn) PreparexContext(ctx context.Context, query string) (*Stmt, error) {
	return PreparexContext(ctx, conn, query)
}

// PrepareNamed returns an sqlx.NamedStmt
func (conn *Conn) PrepareNamed(query string) (*NamedStmt, error) {
	return prepareNamed(conn, query)
}

// PrepareNamedContext returns an sqlx.NamedStmt
func (conn *Conn) PrepareNamedContext(ctx context.Context, query string) (*NamedStmt, error) {
	return prepareNamedContext(ctx, conn, query)
}
