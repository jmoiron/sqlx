// +build go1.9

package sqlx

import (
	"context"
	"database/sql"
)

// Conn represents a single database connection rather than a pool of database
// connections. Prefer running queries from DB unless there is a specific
// need for a continuous single database connection.
//
// A Conn must call Close to return the connection to the database pool
// and may do so concurrently with a running query.
//
// After a call to Close, all operations on the
// connection fail with sql.ErrConnDone.
type Conn struct {
	*sql.Conn
	db *DB
}

// Conn returns a single connection by either opening a new connection
// or returning an existing connection from the connection pool. Conn will
// block until either a connection is returned or ctx is canceled.
// Queries run on the same Conn will be run in the same database session.
//
// Every Conn must be returned to the database pool after use by
// calling Conn.Close.
func (db *DB) Conn(ctx context.Context) (*Conn, error) {
	conn, err := db.DB.Conn(ctx)
	if err != nil {
		return nil, err
	}
	return &Conn{
		Conn: conn,
		db:   db,
	}, nil
}

// Close returns the connection to the connection pool.
// All operations after a Close will return with sql.ErrConnDone.
// Close is safe to call concurrently with other operations and will
// block until all other operations finish. It may be useful to first
// cancel any used context and then call close directly after.
func (c *Conn) Close() {
	c.Conn.Close()
}

// DriverName returns the driverName used by the DB which began this transaction.
func (c *Conn) DriverName() string {
	return c.db.DriverName()
}

// Rebind transforms a query from QUESTION to the DB driver's bindvar type.
func (c *Conn) Rebind(query string) string {
	return c.db.Rebind(query)
}

// BindNamed binds a query using the DB driver's bindvar type.
func (c *Conn) BindNamed(query string, arg interface{}) (string, []interface{}, error) {
	return c.db.BindNamed(query, arg)
}

// BeginTxx begins a transaction and returns an *sqlx.Tx instead of an
// *sql.Tx.
//
// The provided context is used until the transaction is committed or rolled
// back. If the context is canceled, the sql package will roll back the
// transaction. Tx.Commit will return an error if the context provided to
// BeginxContext is canceled.
func (c *Conn) BeginTxx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := c.Conn.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{Tx: tx, driverName: c.db.driverName, unsafe: c.db.unsafe, Mapper: c.db.Mapper}, err
}

// Beginx begins a transaction and returns an *sqlx.Tx instead of an *sql.Tx.
func (c *Conn) Beginx() (*Tx, error) {
	return c.BeginTxx(context.Background(), nil)
}

// PrepareNamedContext returns an sqlx.NamedStmt
func (c *Conn) PrepareNamedContext(ctx context.Context, query string) (*NamedStmt, error) {
	return prepareNamedContext(ctx, c, query)
}

// NamedQueryContext using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (c *Conn) NamedQueryContext(ctx context.Context, query string, arg interface{}) (*Rows, error) {
	return NamedQueryContext(ctx, c, query, arg)
}

// NamedExecContext using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (c *Conn) NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error) {
	return NamedExecContext(ctx, c, query, arg)
}

// PreparexContext returns an sqlx.Stmt instead of a sql.Stmt.
//
// The provided context is used for the preparation of the statement, not for
// the execution of the statement.
func (c *Conn) PreparexContext(ctx context.Context, query string) (*Stmt, error) {
	return PreparexContext(ctx, c, query)
}

// SelectContext using this DB.
// Any placeholder parameters are replaced with supplied args.
func (c *Conn) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return SelectContext(ctx, c, dest, query, args...)
}

// GetContext using this DB.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (c *Conn) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return GetContext(ctx, c, dest, query, args...)
}

// QueryxContext queries the database and returns an *sqlx.Rows.
// Any placeholder parameters are replaced with supplied args.
func (c *Conn) QueryxContext(ctx context.Context, query string, args ...interface{}) (*Rows, error) {
	r, err := c.Conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &Rows{Rows: r, unsafe: c.db.unsafe, Mapper: c.db.Mapper}, err
}

// QueryRowxContext queries the database and returns an *sqlx.Row.
// Any placeholder parameters are replaced with supplied args.
func (c *Conn) QueryRowxContext(ctx context.Context, query string, args ...interface{}) *Row {
	rows, err := c.Conn.QueryContext(ctx, query, args...)
	return &Row{rows: rows, err: err, unsafe: c.db.unsafe, Mapper: c.db.Mapper}
}
