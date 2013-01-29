#sqlx

sqlx is a set of extensions upon go's basic `database/sql` module.

## usage

```golang
package main

import (
    _ "github.com/bmizerany/pq"
    "github.com/jmoiron/sqlx"
)

func main() {
    // this connects & tries a simple 'SELECT 1', panics on error
    // use sqlx.Open() for sql.Open() semantics
    db := sqlx.Connect("postgres", "user=foo dbname=bar sslmode=disable")
   
    
}

```
