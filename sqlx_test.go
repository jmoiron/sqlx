// The following environment variables, if set, will be used:
//
//  Sqlite:
//		* SQLX_SQLITEPATH
//
//  Postgres:
//		* SQLX_PGUSER
//		* SQLX_PGPASS
//
//	MySQL:
//		* SQLX_MYSQLUSER
//		* SQLX_MYSQLPASS
//
//  To disable testing against any of these databases, set one of:
//		* SQLX_NOPG, SQLX_NOMYSQL, SQLX_NOSQLITE
package sqlx

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"os"
	"os/user"
	"strings"
	"testing"
)

var TestPostgres = true
var TestSqlite = true
var TestMysql = true

var sldb *DB
var pgdb *DB
var mysqldb *DB
var active = []*DB{}

func init() {
	PostgresConnect()
	SqliteConnect()
	MysqlConnect()
}

func PostgresConnect() {

	if len(os.Getenv("SQLX_NOPG")) > 0 {
		TestPostgres = false
		fmt.Printf("Skipping Postgres tests, SQLX_NOPG set.\n")
		return
	}

	var username, password string
	var err error

	username = os.Getenv("SQLX_PGUSER")
	password = os.Getenv("SQLX_PGPASS")

	if len(username) == 0 {
		u, err := user.Current()
		if err != nil {
			fmt.Printf("Could not find current user username, trying 'test' instead.")
			username = "test"
		} else {
			username = u.Username
		}

	}

	dsn := fmt.Sprintf("user=%s dbname=sqlxtest sslmode=disable", username)
	if len(password) > 0 {
		dsn = fmt.Sprintf("user=%s password=%s dbname=sqlxtest sslmode=disable", username, password)
	}

	pgdb, err = Connect("postgres", dsn)
	if err != nil {
		fmt.Printf("Could not connect to postgres, try `createdb sqlxtext`, disabling PG tests:\n	%v\n", err)
		TestPostgres = false
	}
}

func SqliteConnect() {

	if len(os.Getenv("SQLX_NOSQLITE")) > 0 {
		TestSqlite = false
		fmt.Printf("Skipping sqlite tests, SQLX_NOSQLITE set.\n")
		return
	}

	var path string
	var err error

	path = os.Getenv("SQLX_SQLITE_PATH")
	if len(path) == 0 {
		path = "/tmp/sqlxtest.db"
	}

	sldb, err = Connect("sqlite3", path)
	if err != nil {
		fmt.Printf("Could not create sqlite3 db in %s:\n	%v", path, err)
		TestSqlite = false
	}
}

func MysqlConnect() {

	if len(os.Getenv("SQLX_NOMYSQL")) > 0 {
		TestSqlite = false
		fmt.Printf("Skipping mysql tests, SQLX_NOMYSQL set.\n")
		return
	}
	var username, dbname, password string
	var err error

	username = os.Getenv("SQLX_MYSQLUSER")
	password = os.Getenv("SQLX_MYSQLPASS")
	dbname = "sqlxtest"

	if len(username) == 0 {
		u, err := user.Current()
		if err != nil {
			fmt.Printf("Could not find current user username, trying 'test' instead.")
			username = "test"
		} else {
			username = u.Username
		}
	}
	mysqldb, err = Connect("mysql", fmt.Sprintf("%s:%s@/%s", username, password, dbname))
	if err != nil {
		fmt.Printf("Could not connect to mysql db, try `mysql -e 'create database sqlxtest'`, disabling MySQL tests:\n    %v", err)
		TestMysql = false
	}
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

func tr(query, dialect string) string {
	if dialect != "postgres" {
		return query
	}
	for i, j := 0, strings.Index(query, "?"); j >= 0; i++ {
		query = strings.Replace(query, "?", fmt.Sprintf("$%d", i+1), 1)
		j = strings.Index(query, "?")
	}
	return query
}

func TestUsage(t *testing.T) {
	RunTest := func(db *DB, t *testing.T, dbtype string) {
		defer func(dbtype string) {
			if dbtype != "postgres" {
				MultiExec(db, drop)
			} else {
				db.Execf(drop)
			}
		}(dbtype)

		// pq will execute multi-query statements, but sqlite3 won't!
		if dbtype != "postgres" {
			MultiExec(db, schema)
		} else {
			db.Execf(schema)
		}

		tx := db.MustBegin()
		tx.Execl(tr("INSERT INTO person (first_name, last_name, email) VALUES (?, ?, ?)", dbtype), "Jason", "Moiron", "jmoiron@jmoiron.net")
		tx.Execl(tr("INSERT INTO person (first_name, last_name, email) VALUES (?, ?, ?)", dbtype), "John", "Doe", "johndoeDNE@gmail.net")
		tx.Execl(tr("INSERT INTO place (country, city, telcode) VALUES (?, ?, ?)", dbtype), "United States", "New York", "1")
		tx.Execl(tr("INSERT INTO place (country, telcode) VALUES (?, ?)", dbtype), "Hong Kong", "852")
		tx.Execl(tr("INSERT INTO place (country, telcode) VALUES (?, ?)", dbtype), "Singapore", "65")
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

		jason = Person{}
		err = db.Get(&jason, tr("SELECT * FROM person WHERE first_name=?", dbtype), "Jason")

		if err != nil {
			t.Errorf("Expecting no error, got %v\n", err)
		}
		if jason.FirstName != "Jason" {
			t.Errorf("Expecting to get back Jason, but got %v\n", jason.FirstName)
		}

		err = db.Get(&jason, tr("SELECT * FROM person WHERE first_name=?", dbtype), "Foobar")
		if err == nil {
			t.Errorf("Expecting an error, got nil\n")
		}
		if err != sql.ErrNoRows {
			t.Errorf("Expected sql.ErrNoRows, got %v\n", err)
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

		stmt, err := db.Preparex(tr("SELECT country, telcode FROM place WHERE telcode > ? ORDER BY telcode ASC", dbtype))
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
	if TestMysql {
		RunTest(mysqldb, t, "mysql")
	}
}
