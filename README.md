#sqlx

sqlx is a set of extensions upon go's basic `database/sql` module.

## usage

```go

package main

import (
    _ "github.com/bmizerany/pq"
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
    people, _ := db.Select(Person{}, "SELECT * FROM person ORDER BY first_name ASC")
    jason, john := people[0].(Person), people[1].(Person)

    fmt.Printf("%#v\n%#v", jason, john)
    // Person{FirstName:"Jason", LastName:"Moiron", Email:"jmoiron@jmoiron.net"}
    // Person{FirstName:"John", LastName:"Doe", Email:"johndoeDNE@gmail.net"}

    // if you have null fields and use SELECT *, you must use sql.Null* in your struct
    places, err = db.Select(Place{}, "SELECT * FROM place ORDER BY telcode ASC")
    usa, singsing, honkers = places[0].(Place), places[1].(Place), places[2].(Place)
    
    fmt.Printf("%#v\n%#v\n%#v\n", usa, singsing, honkers)
    // Place{Country:"United States", City:sql.NullString{String:"New York", Valid:true}, TelCode:1}
    // Place{Country:"Singapore", City:sql.NullString{String:"", Valid:false}, TelCode:65}
    // Place{Country:"Hong Kong", City:sql.NullString{String:"", Valid:false}, TelCode:852}

}

```
