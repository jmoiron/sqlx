#sqlx

[![Build Status](https://drone.io/github.com/jmoiron/sqlx/status.png)](https://drone.io/github.com/jmoiron/sqlx/latest)

sqlx is a library which provides a set of extensions on go's standard
`database/sql` library.  The sqlx versions of `sql.DB`, `sql.TX`, `sql.Stmt`,
et al. all leave the underlying interfaces untouched, so that their interfaces
are a superset on the standard ones.

Major additional concepts are:

* Named parameter support (unmarshal struct fields and maps)
* `StructScan` (marshal rows to structs)
* `Get` and `Select` for storing one more more results directly into a struct or slice
* Common error handling mnemonics (eg. `Execf`, `Execp` (`MustExec`), and `Execl`)
* `LoadFile` for executing statements from a file

## install

    go get github.com/jmoiron/sqlx

## usage

Read the [API documentation on godoc](http://godoc.org/github.com/jmoiron/sqlx) 
for detailed API docs, and check 
[sqlx_test.go](https://github.com/jmoiron/sqlx/blob/master/sqlx_test.go) for more
usage.  

Below is an example which shows some common use cases for sqlx.

```go
package main

import (
    _ "github.com/lib/pq"
    "database/sql"
    "github.com/jmoiron/sqlx"
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
    db := sqlx.Connect("postgres", "user=foo dbname=bar sslmode=disable")

    // exec the schema or fail; multi-statement Exec behavior varies between
    // database drivers;  pq will exec them all, sqlite3 won't, ymmv
    db.Execf(schema)
    
    tx := db.MustBegin()
    tx.Execl("INSERT INTO person (first_name, last_name, email) VALUES ($1, $2, $3)", "Jason", "Moiron", "jmoiron@jmoiron.net")
    tx.Execl("INSERT INTO person (first_name, last_name, email) VALUES ($1, $2, $3)", "John", "Doe", "johndoeDNE@gmail.net")
    tx.Execl("INSERT INTO place (country, city, telcode) VALUES ($1, $2, $3)", "United States", "New York", "1")
    tx.Execl("INSERT INTO place (country, telcode) VALUES ($1, $2)", "Hong Kong", "852")
    tx.Execl("INSERT INTO place (country, telcode) VALUES ($1, $2)", "Singapore", "65")
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
        rows.StructScan(&place)
        fmt.Printf("%#v\n", place)
    }
    // Place{Country:"United States", City:sql.NullString{String:"New York", Valid:true}, TelCode:1}
    // Place{Country:"Hong Kong", City:sql.NullString{String:"", Valid:false}, TelCode:852}
    // Place{Country:"Singapore", City:sql.NullString{String:"", Valid:false}, TelCode:65}

    // Named queries, using `:name` as the bindvar.  Automatic bindvar support
    // which takes into account the dbtype based on the driverName on sqlx.Open/Connect
    _, err = db.NamedExecMap(`INSERT INTO person (first_name,last_name,email) VALUES (:first,:last,:email)`, 
        map[string]interface{}{
            "first": "Bin",
            "last": "Smuth",
            "email": "bensmith@allblacks.nz",
    })

    // Selects Mr. Smith from the database
    rows, err := db.NamedQueryMap(`SELECT * FROM person WHERE first_name=:fn`, map[string]interface{}{"fn": "Bin"})

    // Named queries can also use structs.  Their bind names follow the same rules
    // as the name -> db mapping, so struct fields are lowercased and the `db` tag
    // is taken into consideration.
    rows, err := db.NamedQuery(`SELECT * FROM person WHERE first_name=:first_name`, jason)
}
```

