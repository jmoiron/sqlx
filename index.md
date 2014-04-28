# Illustrated guide to SQLX

<a href="https://github.com/jmoiron/sqlx/"><img style="position: absolute; top: 0; right: 0; border: 0;" src="https://s3.amazonaws.com/github/ribbons/forkme_right_darkblue_121621.png" alt="Fork me on GitHub"></a>

`sqlx` is a package for Go which provides a set of extentions on top of the excellent built-in `database/sql` package.  

Examining *Go* idioms is the focus of this document, so there is no presumption being made that any *SQL* herein is actually a recommended way to use a database.  It will not cover setting up a Go development environment, basic Go information about syntax or semantics, or SQL itself. 

Finally, the standard `err` variable will be used to indicate that errors are being returned, but for brevity they will be ignored.  You should make sure to check and attempt to deal with all errors in an actual program.

## Resources

There are other resources of excellent information about using SQL in Go:

* [database/sql documentation](http://golang.org/pkg/database/sql/)
* [go-database-sql tutorial](http://go-database-sql.org/)

Because the underlying database/sql interface is left in tact by sqlx, all of the advice in these documents also apply to the extensions provided by sqlx.

## Getting Started

You will want to install `sqlx` and a database driver.  Since it's infrastructureless, I recommend mattn's sqlite3 driver:

```
$ go get github.com/jmoiron/sqlx
$ go get github.com/mattn/go-sqlite3
```

## Introduction to SQLX

`sqlx` is intended to have the same *feel* as `databse/sql`.  There are 4 main **handle** types:

* `sqlx.DB` - analagous to `sql.DB`, a representation of a database
* `sqlx.Tx` - analagous to `sql.Tx`, a representation of a transaction
* `sqlx.Stmt` - analagous to `sql.Stmt`, a representation of a prepared statement
* `sqlx.NamedStmt` - a representation of a prepared statement with support for [named
  parameters](#named-params)

Handle types all [embed](http://golang.org/doc/effective_go.html#embedding) their database/sql equivalents, meaning that when you call `sqlx.DB.Query`, you are calling the *same* code as `sql.DB.Query`.  This makes it easy to introduce into an existing codebase.

In addition to these, there are 2 **cursor** types:

* `sqlx.Rows` - analagous to `sql.Rows`, a cursor returned from `Queryx`
* `sqlx.Row` - analagous to `sql.Row`, a result returned from `QueryRowx`

Similarly to the handle types, `sqlx.Rows` embeds its equivalent in the standard library.  Because the underlying implementation was inaccessible, `sqlx.Row` is a partial re-implementation of `sql.Row` with the same behavior for the standard interface.

### Connecting to Your Database

A `DB` instance is *not* a connection, but an abstraction representing a Database.  This is why creating a DB does not return an error and will not panic.  It maintains a connection pool internally once an attempt to connect to a databse is successful.  You can create an sqlx.DB via `Open` or by creating a new sqlx DB handle from an existing sql.DB via `NewDB`:

```go
var db *sqlx.DB

// exactly the same as the built-in 
db = sqlx.Open("sqlite3", ":memory:")

// from a pre-existing sql.DB; note the required driverName
db = sqlx.NewDB(sql.Open("sqlite3", ":memory:"), "sqlite3")

// force a connection
err = db.Ping()
```

In some situations, you might want to open a DB and connect at the same time; for instance, in order to catch configuration issues during your initialization phase.  You can do this in one go with `Connect`, which Opens a new DB and attempts a `Ping`.  The `MustConnect` variant follows the common Go idiom of a `Must` variant which, instead of returning an error, panics on it instead, suitable for use at the module level of your package.

```
var err error
// open and connect at the same time:
db, err = sqlx.Connect("sqlite3", ":memory:")

// open and connect at the same time, panicing on error
db = sqlx.MustConnect("sqlite3", ":memory:")
```

### Querying 101

The handle types in sqlx implement the same basic verbs for querying your database:

* `Query(...) (*sql.Rows, error)` - unchanged from database/sql
* `QueryRow(...) *sql.Row` - unchanged from database/sql
* `Exec(...) (sql.Result, error)` - unchanged from database/sql

These extensions to the built-in verbs:

* `Queryx(...) (*sqlx.Rows, error)` - Query, but return an sqlx.Rows
* `QueryRowx(...) *sqlx.Row` -- QueryRow, but return an sqlx.Row
* `MustExec() sql.Result` -- Exec, but panic on error

And these new semantics:

* `Get(dest interface{}, ...) error`
* `Select(dest interface{}, ...) error`

Let's go from the unchanged interface through the new semantics, explaining their use.

#### Exec

The Exec family of functions grabs a connection from the connection pool and executes that query on the server.  For drivers that do not support ad-hoc query execution, a prepared statement *may* be created behind the scenes to be executed.  The connection is returned to the pool before the result is returned.

```go
schema := `CREATE TABLE place (
    country text,
    city text NULL,
    telcode integer);`

// execute a query on the server
result, err := db.Exec(schema)

// or, you can use MustExec, which panics on error
cityState := `INSERT INTO place (country, telcode) VALUES (?, ?)`
countryCity := `INSERT INTO place (country, city, telcode) VALUES (?, ?, ?)`
db.MustExec(cityState, "Hong Kong", 852)
db.MustExec(cityState, "Singapore", 65)
db.MustExec(countryCity, "South Africa", "Johannesburg", 27)
```

The [result](http://golang.org/pkg/database/sql/#Result) has two possible pieces of data: `LastCreatedId()` or `RowsAffected()`, both of which are available in different situations depending on the driver.  In MySQL, for instance, `LastCreatedId()` will be available on inserts with an auto-increment key, but in PostgreSQL, this information can only be retrieved from a row cursor with the `RETURNING` clause.

The `?` query placeholders, called `bindvars` internally, are important;  you should *always* use these to send values to the database, as they will prevent [SQL injection](http://en.wikipedia.org/wiki/SQL_injection) attacks.  Note that for most popular databases, the escaping happens on the server, *not* in the database/sql library and not in the driver code.  This means that bindvars are database specific;  MySQL accepts the `?` variant shown above, PostgreSQL requires an enumerated `$1`, `$2`, etc bindvar syntax, and SQLite accepts either.  You can use the `sqlx.DB.Rebind()` function with the `?` syntax to ensure that the query is executable on the current database type.

#### Query

Query is the primary interface in database/sql to run queries against your database.  It returns an `sql.Rows` object, which acts more like a database cursor than a list of rows:

```go
// fetch all places from the db
rows, err := db.Query("SELECT country, city, telcode FROM place")
for rows.Next() {
    var country, city string
    var telcode int
    err = rows.Scan(&country, &city, &telcode)
}
```

The error returned by Query is any error that might have happened while performing the actual query on the server.  This can include grabbing a bad connection from the pool, although database/sql will [retry 10 times](http://golang.org/src/pkg/database/sql/sql.go?s=23888:23957#L885) to attempt to find or create a working connection.  Generally, the error will be due to an error in SQL syntax or incorrect field and table names.

The rows return is database cursor which can be used to iterate through the query results.  Although the driver will buffer the actual network traffic on its own in most cases, iterating via `Next()` is a good way to bound the memory usage of large queries, as you're only scanning a single row at a time.  `Scan()` uses [reflect](http://golang.org/pkg/reflect) to map sql column return types to Go types like `string`, `[]byte`, et al.  

In most cases, Scan will copy the data it gets from the driver, as it is not aware of how the driver may reuse its buffers.  The special type `sql.RawBytes` can be used to get a *zero-copy* slice of bytes from the actual data returned by the driver.  After the next call to Next(), such a value is no longer valid, as that memory might have been overwritten by the driver.

Finally, the connection taken from the pool by Query remains active until *either* all rows are exhausted by the iteration via Next, or `rows.Close()` is called.  For more information, see the section on [the connection pool](#connection-pool).

The sqlx extension `Queryx` behaves exactly as Query does, but returns an `sqlx.Rows`, which has extended behaviors:

```go
type Place struct {
    Country       string
    City          string
    TelephoneCode int `db:"telcode"`
}

rows, err := db.Queryx("SELECT * FROM place")
for rows.Next() {
    var p Place
    err = rows.StructScan(&p)
}
```

The primary interface extension added to Rows is `StructScan()`, which automatically scans results into struct fields.  Note that the fields must be [exported](http://golang.org/doc/effective_go.html#names) in order for sqlx to be able to write into them, something true of *all* marshallers in Go.  You can use the `db` struct tag to specify which column name maps to each struct field, or set `sqlx.NameMapper` to a `func(string) string` to define a mapping.  The default mapping is to use `strings.Lower`.  For more information about `StructScan` and the additional extensions `SliceScan` and `MapScan`, see the [section on advanced scanning](#scanning).

#### QueryRow

QueryRow is suitable for fetching one row from the server.  It takes a connection from the connection pool and executes the query using Query, returning a `Row` object which has its own internal rows and cached error result:

```go
row := db.QueryRow("SELECT * FROM place WHERE telcode=?", 852)
var telcode int
err = row.Scan(&telcode)
```

Unlike Query, QueryRow returns a Row type result with no error, making it safe to chain the Scan off of the return.  If there was an error executing the query, that error is return when the row is Scanned.  If there are no rows, `sql.ErrNoRows` is returned.  If the scan itself fails (eg. due to type mismatch), that error is also returned.  

The Rows struct internal to the Row result is Closed upon Scan, meaning that the connection used by QueryRow is kept open until the result is scanned.  It also means that `sql.RawBytes` is not usable here, since the referenced memory belongs to the driver and may already be invalid by the time control is returned to the caller.

The sqlx extension `QueryRowx` will return an sqlx.Row instead of an sql.Row, and it implements identical scanning behaviors as displayed by sqlx.Rows above and in the [advanced scanning section](#scanning):

```go
var p Place
err := db.QueryRowx("SELECT city, telcode FROM place LIMIT 1").StructScan(&p)
```

#### Get and Select

Get and Select are extensions to the handle types which combine the execution stage of the statement with StructScan.  `Get` is therefore analagous with the QueryRowx example above, while `Select` provides an interface to select a list of results directly into a slice of Go structs:

```go
p := Place{}
pp := []Place{}

// this will pull the first place directly into p
err = db.Get(&p, "SELECT * FROM place LIMIT 1")
// this will pull places with telcode > 50 into the slice pp
err = db.Select(&pp, "SELECT * FROM place WHERE telcode > ?", 50)
```

Get and Select both will close the Rows they create under the hood to execute the query, and will return any error encountered at any step of the process.  Since they use StructScan under the hood, the details on this in the [advanced scanning section](#scanning) below also applies to Get and Select.

Select can save you a lot of typing, but beware!  It's semantically different from `Queryx`, since it will load the entire result set into memory at once.  If that set is not bounded by your query to some reasonable size, it might be best to use the classic Queryx/StructScan iteration instead.

<a id="transactions"></a>
### Transactions

To use transactions, you must create a transaction handle with `DB.Begin()`.  Code like this **will not work**:

```go
// this will not work if connection pool > 1
db.MustExec("BEGIN;")
db.MustExec(...)
db.MustExec("COMMIT;")
```

Remember, Exec and all other query verbs will ask the DB for a connection and then return it to the pool each time.  There's no guarantee that you will receive the same connection that the BEGIN statement was executed on.  To use transactions, you must therefore use `DB.Begin()`

```go
tx, err := db.Begin()
err = tx.Exec(...)
err = tx.Commit()
```

The DB handle also has the extensions `Beginx()` and `MustBegin()`, which return an `sqlx.Tx` instead of an `sql.Tx`:

```go
tx := db.MustBegin()
tx.MustExec(...)
err = tx.Commit()
```

As hinted at above, a `sqlx.Tx` has all of the query verb extensions that an `sqlx.DB` has.

<a id="prepared-statements"></a>
### Prepared Statements

<a id="scanning"></a>
### Advanced Scanning

`StructScan` is deceptively sophisticated.  It supports embedded structs, and assigns to fields using an order of precedence that is identical to the way Go resolves embedded fields.  This allows you to share common parts of a table model among many tables.  For instance:

```go
type AutoIncr struct {
    ID       uint64
    Created  time.Time
}

type Place struct {
    Address string
    AutoIncr
}

type Person struct {
    Name string
    AutoIncr
}
```

With the structs above, Person and Place will both be able to receive `id` and `created` columns from a StructScan, because they embed the `AutoIncr` struct which defines them.  This feature can enable you to quickly create an ad-hoc table for joins.  It works recursively as well;  the following will have the Person's Name and its AutoIncr ID and Created fields accessible, both via the Go dot operator and via StructScan:

```go
type Employee struct {
    BossID uint64
    EmployeeID uint64
    Person
}
```

Unfortunately, this also opens us up to a problem.  In Go, it's okay to shadow descendent fields;  if Employee defined a `Name`, it would take precedence over the Person's Name.  But *ambiguous* selectors are illegal and cause [a runtime error](http://play.golang.org/p/MGRxdjLaUc).  If we wanted to create a quick JOIN type for Person and Place, where would we put the `id` column, which is defined in both via their embedded AutoIncr?  Would there be an error?

Because of the way that sqlx builds the mapping of field name to field address, by the time you Scan into a struct, it no longer knows whether or not a name was encountered twice during its traversal of the struct tree.  So unlike Go, StructScan will choose the "first" field encountered which has that name.  Since Go struct fields are ordered from top to bottom, and sqlx does a breadth-first traversal in order to maintain precedence rules, it would happen in the shallowest, top-most definition.  For example, in the type:

```go
type PersonPlace struct {
    Person
    Place
}
```

A StructScan will set an `id` column result in `Person.AutoIncr.ID`, also accessible as `Person.ID`.  To avoid confusion, it's suggested that you use `AS` to create column aliases in your SQL instead.

#### Alternate Scan Types

In addition to using Scan and StructScan, an sqlx Row or Rows can be used to automatically return a slice or a map of results:

```go
rows, err := db.Queryx("SELECT * FROM place")
for rows.Next() {
    // cols is an []interface{} of all of the column results
    cols, err := rows.SliceScan()
}

rows, err := db.Queryx("SELECT * FROM place")
for rows.Next() {
    results := map[string]interface{}
    err = rows.MapScan(results)
}
```

SliceScan returns an `[]interface{}` of all columns, which can be useful in [situations](http://wts.jmoiron.net) where you are executing queries on behalf of a third party and have no way of knowing what columns may be returned.  MapScan behaves the same way, but maps the column names to interface{} values.  An important caveat here is that the results returned by `rows.Columns()` does not (*FIXME*: ever?) include table or alias namespaces, such that `SELECT a.id, b.id FROM a NATURAL JOIN b` will result in a Columns result of `[]string{"id", "id"}`, clobbering one of the results in your map. 

<a id="connection-pool"></a>
### The Connection Pool

If you neglect to do one of these things, the connection may be held until garbage collection, and your db will end up creating far more connections at once in order to compensate for the ones its using.  Since Close can be called more than once without issue, it's acceptable to always call it.

