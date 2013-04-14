package sqlx

import (
	"database/sql"
	"fmt"
	_ "github.com/bmizerany/pq"
	_ "github.com/mattn/go-sqlite3"
	"os/user"
	"strings"
	"testing"
)

var TestPostgres = true
var TestSqlite = true

var sldb *DB
var pgdb *DB
var active = []*DB{}

func init() {
	PostgresConnect()
	SqliteConnect()
}

func PostgresConnect() {
	var username string
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Could not connect to postgres db, try `createdb sqlxtest`, disabling PG tests:\n    %v", r)
			TestPostgres = false
		}
	}()
	u, err := user.Current()
	if err != nil {
		fmt.Printf("Could not find current user username, trying 'test' instead.")
		username = "test"
	} else {
		username = u.Username
	}
	pgdb = MustConnect("postgres", "user="+username+" dbname=sqlxtest sslmode=disable")
}

func SqliteConnect() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Could not create sqlite3 db in /tmp/sqlxtest.db:\n	%v", r)
			TestSqlite = false
		}
	}()

	sldb = MustConnect("sqlite3", "/tmp/sqlxtest.db")
}

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

var drop = `
drop table person;
drop table place;
`

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

func MultiExec(e Execer, query string) {
	stmts := strings.Split(query, ";")
	if len(strings.Trim(stmts[len(stmts)-1], " \n\t\r")) == 0 {
		stmts = stmts[:len(stmts)-1]
	}
	for _, s := range stmts {
		e.Exec(s)
	}
}

func TestUsage(t *testing.T) {
	RunTest := func(db *DB, t *testing.T, dbtype string) {
		defer func(dbtype string) {
			if dbtype == "sqlite" {
				MultiExec(db, drop)
			} else {
				db.Execf(drop)
			}
		}(dbtype)

		// pq will execute multi-query statements, but sqlite3 won't!
		if dbtype == "sqlite" {
			MultiExec(db, schema)
		} else {
			db.Execf(schema)
		}

		tx := db.MustBegin()
		tx.Execl("INSERT INTO person (first_name, last_name, email) VALUES ($1, $2, $3)", "Jason", "Moiron", "jmoiron@jmoiron.net")
		tx.Execl("INSERT INTO person (first_name, last_name, email) VALUES ($1, $2, $3)", "John", "Doe", "johndoeDNE@gmail.net")
		tx.Execl("INSERT INTO place (country, city, telcode) VALUES ($1, $2, $3)", "United States", "New York", "1")
		tx.Execl("INSERT INTO place (country, telcode) VALUES ($1, $2)", "Hong Kong", "852")
		tx.Execl("INSERT INTO place (country, telcode) VALUES ($1, $2)", "Singapore", "65")
		tx.Commit()

		people := []Person{}

		err := db.Select(&people, "SELECT * FROM person ORDER BY first_name ASC")
		if err != nil {
			t.Fatalf("Could not select from people")
		}

		jason, john := people[0], people[1]
		if jason.FirstName != "Jason" {
			t.Errorf("Expecting FirstName of Jason, got %s", jason.FirstName)
		}
		if jason.LastName != "Moiron" {
			t.Errorf("Expecting LastName of Moiron, got %s", jason.LastName)
		}
		if jason.Email != "jmoiron@jmoiron.net" {
			t.Errorf("Expecting Email of jmoiron@jmoiron.net, got %s", jason.Email)
		}
		if john.FirstName != "John" || john.LastName != "Doe" || john.Email != "johndoeDNE@gmail.net" {
			t.Errorf("John Doe's person record not what expected:  Got %v\n", john)
		}

		places := []*Place{}

		err = db.Select(&places, "SELECT telcode FROM place ORDER BY telcode ASC")
		usa, singsing, honkers := places[0], places[1], places[2]

		if usa.TelCode != 1 || honkers.TelCode != 852 || singsing.TelCode != 65 {
			t.Errorf("Expected integer telcodes to work, got %#v", places)
		}

		// if you have null fields and use SELECT *, you must use sql.Null* in your struct
		places = []*Place{}
		err = db.Select(&places, "SELECT * FROM place ORDER BY telcode ASC")
		usa, singsing, honkers = places[0], places[1], places[2]

		if usa.TelCode != 1 || honkers.TelCode != 852 || singsing.TelCode != 65 {
			t.Errorf("Expected integer telcodes to work, got %#v", places)
		}

		stmt, err := db.Preparex("SELECT country, telcode FROM place WHERE telcode > $1 ORDER BY telcode ASC")
		if err != nil {
			t.Error(err)
		}

		places = []*Place{}
		err = stmt.Select(&places, 10)
		if len(places) != 2 {
			t.Error("Expected 2 places, got 0.")
		}
		if err != nil {
			t.Fatal(err)
		}
		singsing, honkers = places[0], places[1]
		if singsing.TelCode != 65 || honkers.TelCode != 852 {
			t.Errorf("Expected the right telcodes, got %#v", places)
		}

		rows, err := db.Queryx("SELECT * FROM place")
		if err != nil {
			t.Fatal(err)
		}
		place := Place{}
		for rows.Next() {
			err = rows.StructScan(&place)
			if err != nil {
				t.Fatal(err)
			}
		}

	}

	if TestPostgres {
		RunTest(pgdb, t, "postgres")
	}
	if TestSqlite {
		RunTest(sldb, t, "sqlite")
	}
}
