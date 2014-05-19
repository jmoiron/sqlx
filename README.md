#sqlx

[![Build Status](https://drone.io/github.com/jmoiron/sqlx/status.png)](https://drone.io/github.com/jmoiron/sqlx/latest)

## Important API Stability Note

The sqlx API has been stable for a long time as I have attempted to learn
from the way that it interacts with real code and taken in bug reports.
There have been very minor breaking changes in the past, many of which would
not have affected most code.

The API of database/sql is impressively succinct, something which is currently
*not* mirrored by sqlx.  While the goals were obviously to create extensions,
a lot of noise has crept in as well.  Much of the API bloat is due to the
proliferation of functions which do not save programmers much time and energy.

In addition to this is the fact that several behavioral or design mistakes have
crept into sqlx:

* the way that non-embedded structs are *also* used for field scanning opens
  us up to cycles and infinite loops and is odd behavior
* the global NameMapper function makes it difficult for code that is intended
  to be a library to rely on any particular behavior of sqlx.
* separating named bind/exec/query functions by map/struct was unnecessary
  type safety pandering;  it buys so little and costs a lot of API noise.
* reflection helpers are exposed as part of the `sqlx` namespace but are
  essentially useless to anyone not building ORMs.

To address this, there will be a **breaking API release** coming up.  This will
likely break some existing code, but it should be the last major API change that
is at least 99% backwards compatible.

It should be known that a release will be made for the "current" version of
sqlx, to ease vendoring where it is desired.

### API Changes

#### Mnemonics

The mnemonic error handling Exec family, `Execl`, `Execf`, `Execv`, will all
be removed without replacement.  Likewise, `Selectf` and `Selectv` are also
removed without replacement.  These are 3 line functions and have little use
in real code.  `Execp` will be removed but its alias, `MustExec`, remains.

#### Named Queries

`BindMap` and `BindStruct` are replaced with a single `BindNamed`, which will
handle structs or maps. `NamedExec` and `NamedQuery` are renamed to `QueryNamed`
and `ExecNamed` to fit in with the rest of the VerbNamed API (eg. `PrepareNamed`,
`BindNamed`). `NamedExecMap` and `NamedQueryMap` are removed as `ExecNamed` and
`QueryNamed` now support both structs and maps.  The `Binder` interface, not
used in any exported functions, will no longer be exported.

#### Reflect


sqlx is a library which provides a set of extensions on go's standard
`database/sql` library.  The sqlx versions of `sql.DB`, `sql.TX`, `sql.Stmt`,
et al. all leave the underlying interfaces untouched, so that their interfaces
are a superset on the standard ones.  This makes it relatively painless to
integrate existing codebases using database/sql with sqlx.

Major additional concepts are:

* Marshal rows into structs (with embedded struct support), maps, and slices
* Named parameter support including prepared statements
* `Get` and `Select` to go quickly from query to struct/slice
* `LoadFile` for executing statements from a file

There is now some [fairly comprehensive documentation](http://jmoiron.github.io/sqlx/) for sqlx.
You can also read the usage below for a quick sample on how sqlx works, or check out the [API
documentation on godoc](http://godoc.org/github.com/jmoiron/sqlx).

## install

    go get github.com/jmoiron/sqlx

## issues

Row headers can be ambiguous (`SELECT 1 AS a, 2 AS a`), and the result of
`Columns()` can have duplicate names on queries like:

```sql
SELECT a.id, a.name, b.id, b.name FROM foos AS a JOIN foos AS b ON a.parent = b.id;
```

making a struct or map destination ambiguous.  Use `AS` in your queries
to give rows distinct names, `rows.Scan` to scan them manually, or 
`SliceScan` to get a slice of results.

## usage

Below is an example which shows some common use cases for sqlx.  Check 
[sqlx_test.go](https://github.com/jmoiron/sqlx/blob/master/sqlx_test.go) for more
usage.  


```go
package main

import (
    _ "github.com/lib/pq"
    "database/sql"
    "github.com/jmoiron/sqlx"
    "log"
)

var schema = `
CREATE TABLE person (
    first_name text,
    last_name text,
    email text
);

CREATE TABLE place (
    country text,
    city text NULL,
    telcode integer
)`

type Person struct {
    FirstName string `db:"first_name"`
    LastName  string `db:"last_name"`
    Email     string
}

type Place struct {
    Country string
    City    sql.NullString
    TelCode int
}

func main() {
    // this connects & tries a simple 'SELECT 1', panics on error
    // use sqlx.Open() for sql.Open() semantics
    db, err := sqlx.Connect("postgres", "user=foo dbname=bar sslmode=disable")
    if err != nil {
        log.Fatalln(err)
    }

    // exec the schema or fail; multi-statement Exec behavior varies between
    // database drivers;  pq will exec them all, sqlite3 won't, ymmv
    db.MustExec(schema)
    
    tx := db.MustBegin()
    tx.MustExec("INSERT INTO person (first_name, last_name, email) VALUES ($1, $2, $3)", "Jason", "Moiron", "jmoiron@jmoiron.net")
    tx.MustExec("INSERT INTO person (first_name, last_name, email) VALUES ($1, $2, $3)", "John", "Doe", "johndoeDNE@gmail.net")
    tx.MustExec("INSERT INTO place (country, city, telcode) VALUES ($1, $2, $3)", "United States", "New York", "1")
    tx.MustExec("INSERT INTO place (country, telcode) VALUES ($1, $2)", "Hong Kong", "852")
    tx.MustExec("INSERT INTO place (country, telcode) VALUES ($1, $2)", "Singapore", "65")
    // Named queries can use structs, so if you have an existing struct (i.e. person := &Person{}) that you have populated, you can pass it in as &person
    tx.NamedExec("INSERT INTO person (first_name, last_name, email) VALUES (:first_name, :last_name, :email)", &Person{"Jane", "Citizen", "jane.citzen@example.com"})
    tx.Commit()

    // Query the database, storing results in a []Person (wrapped in []interface{})
    people := []Person{}
    db.Select(&people, "SELECT * FROM person ORDER BY first_name ASC")
    jason, john := people[0], people[1]

    fmt.Printf("%#v\n%#v", jason, john)
    // Person{FirstName:"Jason", LastName:"Moiron", Email:"jmoiron@jmoiron.net"}
    // Person{FirstName:"John", LastName:"Doe", Email:"johndoeDNE@gmail.net"}

    // You can also get a single result, a la QueryRow
    jason = Person{}
    err = db.Get(&jason, "SELECT * FROM person WHERE first_name=$1", "Jason")
    fmt.Printf("%#v\n", jason)
    // Person{FirstName:"Jason", LastName:"Moiron", Email:"jmoiron@jmoiron.net"}

    // if you have null fields and use SELECT *, you must use sql.Null* in your struct
    places := []Place{}
    err := db.Select(&places, "SELECT * FROM place ORDER BY telcode ASC")
    if err != nil {
        fmt.Printf(err)
        return
    }
    usa, singsing, honkers = places[0], places[1], places[2]
    
    fmt.Printf("%#v\n%#v\n%#v\n", usa, singsing, honkers)
    // Place{Country:"United States", City:sql.NullString{String:"New York", Valid:true}, TelCode:1}
    // Place{Country:"Singapore", City:sql.NullString{String:"", Valid:false}, TelCode:65}
    // Place{Country:"Hong Kong", City:sql.NullString{String:"", Valid:false}, TelCode:852}

    // Loop through rows using only one struct
    place := Place{}
    rows, err := db.Queryx("SELECT * FROM place")
    for rows.Next() {
        err := rows.StructScan(&place)
        if err != nil {
            log.Fataln(err)
        } 
        fmt.Printf("%#v\n", place)
    }
    // Place{Country:"United States", City:sql.NullString{String:"New York", Valid:true}, TelCode:1}
    // Place{Country:"Hong Kong", City:sql.NullString{String:"", Valid:false}, TelCode:852}
    // Place{Country:"Singapore", City:sql.NullString{String:"", Valid:false}, TelCode:65}

    // Named queries, using `:name` as the bindvar.  Automatic bindvar support
    // which takes into account the dbtype based on the driverName on sqlx.Open/Connect
    _, err = db.NamedExec(`INSERT INTO person (first_name,last_name,email) VALUES (:first,:last,:email)`, 
        map[string]interface{}{
            "first": "Bin",
            "last": "Smuth",
            "email": "bensmith@allblacks.nz",
    })

    // Selects Mr. Smith from the database
    rows, err := db.NamedQuery(`SELECT * FROM person WHERE first_name=:fn`, map[string]interface{}{"fn": "Bin"})

    // Named queries can also use structs.  Their bind names follow the same rules
    // as the name -> db mapping, so struct fields are lowercased and the `db` tag
    // is taken into consideration.
    rows, err := db.NamedQuery(`SELECT * FROM person WHERE first_name=:first_name`, jason)
}
```

## embedded structs

Structs which do not implement the [sql.Scanner](http://golang.org/pkg/database/sql/#Scanner)
interface will be inspected and their fields used as possible targets for a scan.  This includes
embedded and non-embedded structs.

Go makes '[ambiguous selectors](http://play.golang.org/p/MGRxdjLaUc)' a compile time error,
but does not make structs with possible ambiguous selectors errors.  Sqlx will decide
which field to use on a struct based on a breadth first search of the struct and any
structs it contains or embeds, as specified by the order of the fields as accessible
by `reflect`, which generally means in source-order.

## scan safety

By default, scanning into structs requires the structs to have fields for all of the
columns in the query.  This was done for a few reasons:

* A mistake in naming during development could lead you to believe that data is
  being written to a field when actually it can't be found and it is being dropped
* This behavior mirrors the behavior of the Go compiler with respect to unused
  variables
* Selecting more data than you need is wasteful (more data on the wire, more time
  marshalling, etc)

Unlike Marshallers in the stdlib, the programmer scanning an sql result into a struct
will generally have a full understanding of what the underlying data model is *and*
full control over the SQL statement.

Despite this, there are use cases where it's convenient to be able to ignore unknown
columns.  In most of these cases, you might be better off with `ScanSlice`, but where
you want to still use structs, there is now the `Unsafe` method.  Its usage is most
simply shown in an example:

```go
    db, err := sqlx.Connect("postgres", "user=foo dbname=bar sslmode=disable")
    if err != nil {
        log.Fatal(err)
    }

    type Person {
        Name string
    }
    var p Person

    // This fails, because there is no destination for location in Person
    err = db.Get(&p, "SELECT name, location FROM person LIMIT 1")
    
    udb := db.Unsafe()
    
    // This succeeds and just sets `Name` in the p struct
    err = udb.Get(&p, "SELECT name, location FROM person LIMIT 1")
```

The `Unsafe` method is implemented on `Tx`, `DB`, and `Stmt`.  When you use an unsafe
`Tx` or `DB` to create a new `Tx` or `Stmt`, those inherit its lack of safety.

