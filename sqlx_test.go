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
		TestMysql = false
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
	stmts := strings.Split(query, ";\n")
	if len(strings.Trim(stmts[len(stmts)-1], " \n\t\r")) == 0 {
		stmts = stmts[:len(stmts)-1]
	}
	for _, s := range stmts {
		e.Exec(s)
	}
}

func TestUsage(t *testing.T) {
	RunTest := func(db *DB, t *testing.T, dbtype string) {
		var err error

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
		tx.Execl(tx.Rebind("INSERT INTO person (first_name, last_name, email) VALUES (?, ?, ?)"), "Jason", "Moiron", "jmoiron@jmoiron.net")
		tx.Execl(tx.Rebind("INSERT INTO person (first_name, last_name, email) VALUES (?, ?, ?)"), "John", "Doe", "johndoeDNE@gmail.net")
		tx.Execl(tx.Rebind("INSERT INTO place (country, city, telcode) VALUES (?, ?, ?)"), "United States", "New York", "1")
		tx.Execl(tx.Rebind("INSERT INTO place (country, telcode) VALUES (?, ?)"), "Hong Kong", "852")
		tx.Execl(tx.Rebind("INSERT INTO place (country, telcode) VALUES (?, ?)"), "Singapore", "65")
		tx.Commit()

		people := []Person{}

		err = db.Select(&people, "SELECT * FROM person ORDER BY first_name ASC")
		if err != nil {
			t.Fatal(err)
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
		err = db.Get(&jason, db.Rebind("SELECT * FROM person WHERE first_name=?"), "Jason")

		if err != nil {
			t.Fatal(err)
		}
		if jason.FirstName != "Jason" {
			t.Errorf("Expecting to get back Jason, but got %v\n", jason.FirstName)
		}

		err = db.Get(&jason, db.Rebind("SELECT * FROM person WHERE first_name=?"), "Foobar")
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
		// this test also verifies that you can use either a []Struct{} or a []*Struct{}
		places2 := []Place{}
		err = db.Select(&places2, "SELECT * FROM place ORDER BY telcode ASC")
		usa, singsing, honkers = &places2[0], &places2[1], &places2[2]

		// this should return a type error that &p is not a pointer to a struct slice
		p := Place{}
		err = db.Select(&p, "SELECT * FROM place ORDER BY telcode ASC")
		if err == nil {
			t.Errorf("Expected an error, argument to select should be a pointer to a struct slice")
		}

		// this should be an error because
		pl := []Place{}
		err = db.Select(pl, "SELECT * FROM place ORDER BY telcode ASC")
		if err == nil {
			t.Errorf("Expected an error, argument to select should be a pointer to a struct slice, not a slice.")
		}

		if usa.TelCode != 1 || honkers.TelCode != 852 || singsing.TelCode != 65 {
			t.Errorf("Expected integer telcodes to work, got %#v", places)
		}

		stmt, err := db.Preparex(db.Rebind("SELECT country, telcode FROM place WHERE telcode > ? ORDER BY telcode ASC"))
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

		// test advanced querying
		_, err = db.NamedExecMap("INSERT INTO person (first_name, last_name, email) VALUES (:first, :last, :email)", map[string]interface{}{
			"first": "Bin",
			"last":  "Smuth",
			"email": "bensmith@allblacks.nz",
		})
		if err != nil {
			t.Fatal(err)
		}

		// ensure that if the named param happens right at the end it still works
		rows, err = db.NamedQueryMap("SELECT * FROM person WHERE first_name=:first", map[string]interface{}{"first": "Bin"})
		if err != nil {
			t.Fatal(err)
		}

		ben := &Person{}
		for rows.Next() {
			err = rows.StructScan(ben)
			if err != nil {
				t.Fatal(err)
			}
			if ben.FirstName != "Bin" {
				t.Fatal("Expected first name of `Bin`, got " + ben.FirstName)
			}
			if ben.LastName != "Smuth" {
				t.Fatal("Expected first name of `Smuth`, got " + ben.LastName)
			}
		}

		ben.FirstName = "Ben"
		ben.LastName = "Smith"

		// Insert via a named query using the struct
		_, err = db.NamedExec("INSERT INTO person (first_name, last_name, email) VALUES (:first_name, :last_name, :email)", ben)

		if err != nil {
			t.Fatal(err)
		}

		rows, err = db.NamedQuery("SELECT * FROM person WHERE first_name=:first_name", ben)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			err = rows.StructScan(ben)
			if err != nil {
				t.Fatal(err)
			}
			if ben.FirstName != "Ben" {
				t.Fatal("Expected first name of `Ben`, got " + ben.FirstName)
			}
			if ben.LastName != "Smith" {
				t.Fatal("Expected first name of `Smith`, got " + ben.LastName)
			}
		}
		// ensure that Get does not panic on emppty result set
		person := &Person{}
		err = db.Get(person, "SELECT * FROM person WHERE first_name=$1", "does-not-exist")
		if err == nil {
			t.Fatal("Should have got an error for Get on non-existant row.")
		}

		// lets test prepared statements some more

		stmt, err = db.Preparex(db.Rebind("SELECT * FROM person WHERE first_name=?"))
		if err != nil {
			t.Fatal(err)
		}
		rows, err = stmt.Queryx("Ben")
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			err = rows.StructScan(ben)
			if err != nil {
				t.Fatal(err)
			}
			if ben.FirstName != "Ben" {
				t.Fatal("Expected first name of `Ben`, got " + ben.FirstName)
			}
			if ben.LastName != "Smith" {
				t.Fatal("Expected first name of `Smith`, got " + ben.LastName)
			}
		}

		john = Person{}
		stmt, err = db.Preparex(db.Rebind("SELECT * FROM person WHERE first_name=?"))
		if err != nil {
			t.Fatal(err)
		}
		err = stmt.Get(&john, "John")
		if err != nil {
			t.Fatal(err)
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

// tests that sqlx will not panic when the wrong driver is passed because
// of an automatic nil dereference in sqlx.Open(), which was fixed.
func TestDoNotPanicOnConnect(t *testing.T) {
	_, err := Connect("bogus", "hehe")
	if err == nil {
		t.Errorf("Should return error when using bogus driverName")
	}
}

func TestRebind(t *testing.T) {
	q1 := `INSERT INTO foo (a, b, c, d, e, f, g, h, i) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	q2 := `INSERT INTO foo (a, b, c) VALUES (?, ?, "foo"), ("Hi", ?, ?)`

	s1 := Rebind(DOLLAR, q1)
	s2 := Rebind(DOLLAR, q2)

	if s1 != `INSERT INTO foo (a, b, c, d, e, f, g, h, i) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)` {
		t.Errorf("q1 failed")
	}

	if s2 != `INSERT INTO foo (a, b, c) VALUES ($1, $2, "foo"), ("Hi", $3, $4)` {
		t.Errorf("q2 failed")
	}
}

func TestBindMap(t *testing.T) {
	// Test that it works..
	q1 := `INSERT INTO foo (a, b, c, d) VALUES (:name, :age, :first, :last)`
	am := map[string]interface{}{
		"name":  "Jason Moiron",
		"age":   30,
		"first": "Jason",
		"last":  "Moiron",
	}

	bq, args, _ := BindMap(QUESTION, q1, am)
	expect := `INSERT INTO foo (a, b, c, d) VALUES (?, ?, ?, ?)`
	if bq != expect {
		t.Errorf("Interpolation of query failed: got `%v`, expected `%v`\n", bq, expect)
	}

	if args[0].(string) != "Jason Moiron" {
		t.Errorf("Expected `Jason Moiron`, got %v\n", args[0])
	}

	if args[1].(int) != 30 {
		t.Errorf("Expected 30, got %v\n", args[1])
	}

	if args[2].(string) != "Jason" {
		t.Errorf("Expected Jason, got %v\n", args[2])
	}

	if args[3].(string) != "Moiron" {
		t.Errorf("Expected Moiron, got %v\n", args[3])
	}
}

func TestBindStruct(t *testing.T) {
	q1 := `INSERT INTO foo (a, b, c, d) VALUES (:name, :age, :first, :last)`
	type tt struct {
		Name  string
		Age   int
		First string
		Last  string
	}
	am := tt{"Jason Moiron", 30, "Jason", "Moiron"}

	bq, args, _ := BindStruct(QUESTION, q1, am)
	expect := `INSERT INTO foo (a, b, c, d) VALUES (?, ?, ?, ?)`
	if bq != expect {
		t.Errorf("Interpolation of query failed: got `%v`, expected `%v`\n", bq, expect)
	}

	if args[0].(string) != "Jason Moiron" {
		t.Errorf("Expected `Jason Moiron`, got %v\n", args[0])
	}

	if args[1].(int) != 30 {
		t.Errorf("Expected 30, got %v\n", args[1])
	}

	if args[2].(string) != "Jason" {
		t.Errorf("Expected Jason, got %v\n", args[2])
	}

	if args[3].(string) != "Moiron" {
		t.Errorf("Expected Moiron, got %v\n", args[3])
	}

}

func BenchmarkBindStruct(b *testing.B) {
	b.StopTimer()
	q1 := `INSERT INTO foo (a, b, c, d) VALUES (:name, :age, :first, :last)`
	type t struct {
		Name  string
		Age   int
		First string
		Last  string
	}
	am := t{"Jason Moiron", 30, "Jason", "Moiron"}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		BindStruct(DOLLAR, q1, am)
		//bindMap(QUESTION, q1, am)
	}
}

func BenchmarkBindMap(b *testing.B) {
	b.StopTimer()
	q1 := `INSERT INTO foo (a, b, c, d) VALUES (:name, :age, :first, :last)`
	am := map[string]interface{}{
		"name":  "Jason Moiron",
		"age":   30,
		"first": "Jason",
		"last":  "Moiron",
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		BindMap(DOLLAR, q1, am)
		//bindMap(QUESTION, q1, am)
	}
}

func BenchmarkRebind(b *testing.B) {
	b.StopTimer()
	q1 := `INSERT INTO foo (a, b, c, d, e, f, g, h, i) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	q2 := `INSERT INTO foo (a, b, c) VALUES (?, ?, "foo"), ("Hi", ?, ?)`
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		Rebind(DOLLAR, q1)
		Rebind(DOLLAR, q2)
	}
}

func BenchmarkRebindBuffer(b *testing.B) {
	b.StopTimer()
	q1 := `INSERT INTO foo (a, b, c, d, e, f, g, h, i) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	q2 := `INSERT INTO foo (a, b, c) VALUES (?, ?, "foo"), ("Hi", ?, ?)`
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		rebindBuff(DOLLAR, q1)
		rebindBuff(DOLLAR, q2)
	}
}
