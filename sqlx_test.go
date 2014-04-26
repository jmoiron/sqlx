// The following environment variables, if set, will be used:
//
//	* SQLX_SQLITE_DSN
//	* SQLX_POSTGRES_DSN
//	* SQLX_MYSQL_DSN
//
// Set any of these variables to 'skip' to skip them.  Note that for MySQL,
// the string '?parseTime=True' will be appended to the DSN if it's not there
// already.
//
package sqlx

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

/* compile time checks that Db, Tx, Stmt (qStmt) implement expected interfaces */
var _, _ Ext = &DB{}, &Tx{}
var _, _ ColScanner = &Row{}, &Rows{}
var _ Queryer = &qStmt{}
var _ Execer = &qStmt{}

var TestPostgres = true
var TestSqlite = true
var TestMysql = true

var sldb *DB
var pgdb *DB
var mysqldb *DB
var active = []*DB{}

func init() {
	ConnectAll()
}

func ConnectAll() {
	var err error

	pgdsn := os.Getenv("SQLX_POSTGRES_DSN")
	mydsn := os.Getenv("SQLX_MYSQL_DSN")
	sqdsn := os.Getenv("SQLX_SQLITE_DSN")

	TestPostgres = pgdsn != "skip"
	TestMysql = mydsn != "skip"
	TestSqlite = sqdsn != "skip"

	if TestPostgres {
		pgdb, err = Connect("postgres", pgdsn)
		if err != nil {
			fmt.Printf("Disabling PG tests:\n    %v\n", err)
			TestPostgres = false
		}
	} else {
		fmt.Println("Disabling Postgres tests.")
	}

	if TestMysql {
		mysqldb, err = Connect("mysql", mydsn)
		if err != nil {
			fmt.Printf("Disabling MySQL tests:\n    %v", err)
			TestMysql = false
		}
	} else {
		fmt.Println("Disabling MySQL tests.")
	}

	if TestSqlite {
		sldb, err = Connect("sqlite3", sqdsn)
		if err != nil {
			fmt.Printf("Disabling SQLite:\n    %v", err)
			TestSqlite = false
		}
	} else {
		fmt.Println("Disabling SQLite tests.")
	}
}

type Schema struct {
	create string
	drop   string
}

func (s Schema) Postgres() (string, string) {
	return s.create, s.drop
}

func (s Schema) MySQL() (string, string) {
	return strings.Replace(s.create, `"`, "`", -1), s.drop
}

func (s Schema) Sqlite3() (string, string) {
	return strings.Replace(s.create, `now()`, `CURRENT_TIMESTAMP`, -1), s.drop
}

var defaultSchema = Schema{
	create: `
CREATE TABLE person (
	first_name text,
	last_name text,
	email text,
	added_at timestamp default now()
);

CREATE TABLE place (
	country text,
	city text NULL,
	telcode integer
);

CREATE TABLE capplace (
	"COUNTRY" text,
	"CITY" text NULL,
	"TELCODE" integer
);

CREATE TABLE nullperson (
    first_name text NULL,
    last_name text NULL,
    email text NULL
);
`,
	drop: `
drop table person;
drop table place;
drop table capplace;
drop table nullperson;
`,
}

type Person struct {
	FirstName string `db:"first_name"`
	LastName  string `db:"last_name"`
	Email     string
	AddedAt   time.Time `db:"added_at"`
}

type Person2 struct {
	FirstName sql.NullString `db:"first_name"`
	LastName  sql.NullString `db:"last_name"`
	Email     sql.NullString
}

type Place struct {
	Country string
	City    sql.NullString
	TelCode int
}

type PlacePtr struct {
	Country string
	City    *string
	TelCode int
}

type PersonPlace struct {
	Person
	Place
}

type PersonPlacePtr struct {
	*Person
	*Place
}

type NonEmbedded struct {
	Person Person
	Place  Place
}

type EmbedConflict struct {
	FirstName string `db:"first_name"`
	Person
}

type Loop1 struct {
	Person
}

type Loop2 struct {
	Loop1
}

type Loop3 struct {
	Loop2
}

type SliceMember struct {
	Country   string
	City      sql.NullString
	TelCode   int
	People    []Person `db:"-"`
	Addresses []Place  `db:"-"`
}

// Note that because of field map caching, we need a new type here
// if we've used Place already soemwhere in sqlx
type CPlace Place

func MultiExec(e Execer, query string) {
	stmts := strings.Split(query, ";\n")
	if len(strings.Trim(stmts[len(stmts)-1], " \n\t\r")) == 0 {
		stmts = stmts[:len(stmts)-1]
	}
	for _, s := range stmts {
		_, err := e.Exec(s)
		if err != nil {
			fmt.Println(err, s)
		}
	}
}

func RunWithSchema(schema Schema, t *testing.T, test func(db *DB, t *testing.T)) {
	runner := func(db *DB, t *testing.T, create, drop string) {
		defer func() {
			MultiExec(db, drop)
		}()

		MultiExec(db, create)
		test(db, t)
	}

	if TestPostgres {
		create, drop := schema.Postgres()
		runner(pgdb, t, create, drop)
	}
	if TestSqlite {
		create, drop := schema.Sqlite3()
		runner(sldb, t, create, drop)
	}
	if TestMysql {
		create, drop := schema.MySQL()
		runner(mysqldb, t, create, drop)
	}
}

func loadDefaultFixture(db *DB, t *testing.T) {
	tx := db.MustBegin()
	tx.Execl(tx.Rebind("INSERT INTO person (first_name, last_name, email) VALUES (?, ?, ?)"), "Jason", "Moiron", "jmoiron@jmoiron.net")
	tx.Execl(tx.Rebind("INSERT INTO person (first_name, last_name, email) VALUES (?, ?, ?)"), "John", "Doe", "johndoeDNE@gmail.net")
	tx.Execl(tx.Rebind("INSERT INTO place (country, city, telcode) VALUES (?, ?, ?)"), "United States", "New York", "1")
	tx.Execl(tx.Rebind("INSERT INTO place (country, telcode) VALUES (?, ?)"), "Hong Kong", "852")
	tx.Execl(tx.Rebind("INSERT INTO place (country, telcode) VALUES (?, ?)"), "Singapore", "65")
	if db.DriverName() == "mysql" {
		tx.Execl(tx.Rebind("INSERT INTO capplace (`COUNTRY`, `TELCODE`) VALUES (?, ?)"), "Sarf Efrica", "27")
	} else {
		tx.Execl(tx.Rebind("INSERT INTO capplace (\"COUNTRY\", \"TELCODE\") VALUES (?, ?)"), "Sarf Efrica", "27")
	}
	tx.Commit()
}

// Test a new backwards compatible feature, that missing scan destinations
// will silently scan into sql.RawText rather than failing/panicing
func TestMissingNames(t *testing.T) {
	RunWithSchema(defaultSchema, t, func(db *DB, t *testing.T) {
		loadDefaultFixture(db, t)
		type PersonPlus struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
			Email     string
			//AddedAt time.Time `db:"added_at"`
		}

		// test Select first
		pps := []PersonPlus{}
		// pps lacks added_at destination
		err := db.Select(&pps, "SELECT * FROM person")
		if err == nil {
			t.Error("Expected missing name from Select to fail, but it did not.")
		}

		// test Get
		pp := PersonPlus{}
		err = db.Get(&pp, "SELECT * FROM person LIMIT 1")
		if err == nil {
			t.Error("Expected missing name Get to fail, but it did not.")
		}

		// test naked StructScan
		pps = []PersonPlus{}
		rows, err := db.Query("SELECT * FROM person LIMIT 1")
		if err != nil {
			t.Fatal(err)
		}
		rows.Next()
		err = StructScan(rows, &pps)
		if err == nil {
			t.Error("Expected missing name in StructScan to fail, but it did not.")
		}
		rows.Close()

		// now try various things with unsafe set.
		db = db.Unsafe()
		pps = []PersonPlus{}
		err = db.Select(&pps, "SELECT * FROM person")
		if err != nil {
			t.Error(err)
		}

		// test Get
		pp = PersonPlus{}
		err = db.Get(&pp, "SELECT * FROM person LIMIT 1")
		if err != nil {
			t.Error(err)
		}

		// test naked StructScan
		pps = []PersonPlus{}
		rowsx, err := db.Queryx("SELECT * FROM person LIMIT 1")
		if err != nil {
			t.Fatal(err)
		}
		rowsx.Next()
		err = StructScan(rowsx, &pps)
		if err != nil {
			t.Error(err)
		}
		rowsx.Close()

	})
}

func TestEmbeddedStructs(t *testing.T) {
	RunWithSchema(defaultSchema, t, func(db *DB, t *testing.T) {
		loadDefaultFixture(db, t)
		peopleAndPlaces := []PersonPlace{}
		err := db.Select(
			&peopleAndPlaces,
			`SELECT person.*, place.* FROM
             person natural join place`)
		if err != nil {
			t.Fatal(err)
		}
		for _, pp := range peopleAndPlaces {
			if len(pp.Person.FirstName) == 0 {
				t.Errorf("Expected non zero lengthed first name.")
			}
			if len(pp.Place.Country) == 0 {
				t.Errorf("Expected non zero lengthed country.")
			}
		}

		// test embedded structs with StructScan
		rows, err := db.Queryx(
			`SELECT person.*, place.* FROM
         person natural join place`)
		if err != nil {
			t.Error(err)
		}

		perp := PersonPlace{}
		rows.Next()
		err = rows.StructScan(&perp)
		if err != nil {
			t.Error(err)
		}

		if len(perp.Person.FirstName) == 0 {
			t.Errorf("Expected non zero lengthed first name.")
		}
		if len(perp.Place.Country) == 0 {
			t.Errorf("Expected non zero lengthed country.")
		}

		rows.Close()

		// test the same for embedded pointer structs
		peopleAndPlacesPtrs := []PersonPlacePtr{}
		err = db.Select(
			&peopleAndPlacesPtrs,
			`SELECT person.*, place.* FROM
             person natural join place`)
		if err != nil {
			t.Fatal(err)
		}
		for _, pp := range peopleAndPlacesPtrs {
			if len(pp.Person.FirstName) == 0 {
				t.Errorf("Expected non zero lengthed first name.")
			}
			if len(pp.Place.Country) == 0 {
				t.Errorf("Expected non zero lengthed country.")
			}
		}

		// test "non embedded" struct namespace collapsing..
		nes := []NonEmbedded{}
		err = db.Select(&nes, `select person.*, place.* FROM person natural join place`)
		if err != nil {
			t.Fatal(err)
		}
		for _, ne := range nes {
			if len(ne.Person.FirstName) == 0 {
				t.Errorf("Expected non zero lengthed first name.")
			}
			if len(ne.Place.Country) == 0 {
				t.Errorf("Expected non zero lengthed country.")
			}
		}

		// test "deep nesting"
		l3s := []Loop3{}
		err = db.Select(&l3s, `select * from person`)
		if err != nil {
			t.Fatal(err)
		}
		for _, l3 := range l3s {
			if len(l3.Loop2.Loop1.Person.FirstName) == 0 {
				t.Errorf("Expected non zero lengthed first name.")
			}
		}

		// test "embed conflicts"
		ec := []EmbedConflict{}
		err = db.Select(&ec, `select * from person`)
		// I'm torn between erroring here or having some kind of working behavior
		// in order to allow for more flexibility in destination structs
		if err != nil {
			t.Errorf("Was not expecting an error on embed conflicts.")
		}
	})
}

func TestNilReceiver(t *testing.T) {
	RunWithSchema(defaultSchema, t, func(db *DB, t *testing.T) {
		loadDefaultFixture(db, t)
		var p *Person
		err := db.Get(p, "SELECT * FROM person LIMIT 1")
		if err == nil {
			t.Error("Expected error when getting into nil struct ptr.")
		}
		var pp *[]Person
		err = db.Select(pp, "SELECT * FROM person")
		if err == nil {
			t.Error("Expected an error when selecting into nil slice ptr.")
		}
	})
}

func TestNamedQuery(t *testing.T) {
	var schema = Schema{
		create: `
			CREATE TABLE person (
				first_name text NULL,
				last_name text NULL,
				email text NULL
			);`,
		drop: `drop table person;`,
	}

	RunWithSchema(schema, t, func(db *DB, t *testing.T) {
		type Person struct {
			FirstName sql.NullString `db:"first_name"`
			LastName  sql.NullString `db:"last_name"`
			Email     sql.NullString
		}

		p := Person{
			FirstName: sql.NullString{"ben", true},
			LastName:  sql.NullString{"doe", true},
			Email:     sql.NullString{"ben@doe.com", true},
		}

		q1 := `INSERT INTO person (first_name, last_name, email) VALUES (:first_name, :last_name, :email)`
		_, err := db.NamedExec(q1, p)
		if err != nil {
			log.Fatal(err)
		}

		p2 := &Person{}
		rows, err := db.NamedQuery("SELECT * FROM person WHERE first_name=:first_name", p)
		if err != nil {
			log.Fatal(err)
		}
		for rows.Next() {
			err = rows.StructScan(p2)
			if err != nil {
				t.Error(err)
			}
			if p2.FirstName.String != "ben" {
				t.Error("Expected first name of `ben`, got " + p2.FirstName.String)
			}
			if p2.LastName.String != "doe" {
				t.Error("Expected first name of `doe`, got " + p2.LastName.String)
			}
		}
	})
}

func TestScanError(t *testing.T) {
	var schema = Schema{
		create: `
			CREATE TABLE kv (
				k text,
				v integer
			);`,
		drop: `drop table kv;`,
	}

	RunWithSchema(schema, t, func(db *DB, t *testing.T) {
		type WrongTypes struct {
			K int
			V string
		}
		_, err := db.Exec(db.Rebind("INSERT INTO kv (k, v) VALUES (?, ?)"), "hi", 1)
		if err != nil {
			t.Error(err)
		}

		rows, err := db.Queryx("SELECT * FROM kv")
		if err != nil {
			t.Error(err)
		}
		for rows.Next() {
			var wt WrongTypes
			err := rows.StructScan(&wt)
			if err == nil {
				t.Errorf("%s: Scanning wrong types into keys should have errored.", db.DriverName())
			}
		}
	})
}

func TestUsage(t *testing.T) {
	RunWithSchema(defaultSchema, t, func(db *DB, t *testing.T) {
		loadDefaultFixture(db, t)
		slicemembers := []SliceMember{}
		err := db.Select(&slicemembers, "SELECT * FROM place ORDER BY telcode ASC")
		if err != nil {
			t.Fatal(err)
		}

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

		// The following tests check statement reuse, which was actually a problem
		// due to copying being done when creating Stmt's which was eventually removed
		stmt1, err := db.Preparex(db.Rebind("SELECT * FROM person WHERE first_name=?"))
		if err != nil {
			t.Fatal(err)
		}
		jason = Person{}

		row := stmt1.QueryRowx("DoesNotExist")
		row.Scan(&jason)
		row = stmt1.QueryRowx("DoesNotExist")
		row.Scan(&jason)

		err = stmt1.Get(&jason, "DoesNotExist User")
		if err == nil {
			t.Error("Expected an error")
		}
		err = stmt1.Get(&jason, "DoesNotExist User 2")

		stmt2, err := db.Preparex(db.Rebind("SELECT * FROM person WHERE first_name=?"))
		if err != nil {
			t.Fatal(err)
		}
		jason = Person{}
		tx, err := db.Beginx()
		if err != nil {
			t.Fatal(err)
		}
		tstmt2 := tx.Stmtx(stmt2)
		row2 := tstmt2.QueryRowx("Jason")
		err = row2.StructScan(&jason)
		if err != nil {
			t.Error(err)
		}
		tx.Commit()

		places := []*Place{}
		err = db.Select(&places, "SELECT telcode FROM place ORDER BY telcode ASC")
		usa, singsing, honkers := places[0], places[1], places[2]

		if usa.TelCode != 1 || honkers.TelCode != 852 || singsing.TelCode != 65 {
			t.Errorf("Expected integer telcodes to work, got %#v", places)
		}

		placesptr := []PlacePtr{}
		err = db.Select(&placesptr, "SELECT * FROM place ORDER BY telcode ASC")
		if err != nil {
			t.Error(err)
		}
		//fmt.Printf("%#v\n%#v\n%#v\n", placesptr[0], placesptr[1], placesptr[2])

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

		// this should be an error
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

		rows, err = db.Queryx("SELECT * FROM place")
		if err != nil {
			t.Fatal(err)
		}
		m := map[string]interface{}{}
		for rows.Next() {
			err = rows.MapScan(m)
			if err != nil {
				t.Fatal(err)
			}
			_, ok := m["country"]
			if !ok {
				t.Errorf("Expected key `country` in map but could not find it (%#v)\n", m)
			}
		}

		rows, err = db.Queryx("SELECT * FROM place")
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			s, err := rows.SliceScan()
			if err != nil {
				t.Error(err)
			}
			if len(s) != 3 {
				t.Errorf("Expected 3 columns in result, got %d\n", len(s))
			}
		}

		// test advanced querying
		// test that NamedExec works with a map as well as a struct
		_, err = db.NamedExec("INSERT INTO person (first_name, last_name, email) VALUES (:first, :last, :email)", map[string]interface{}{
			"first": "Bin",
			"last":  "Smuth",
			"email": "bensmith@allblacks.nz",
		})
		if err != nil {
			t.Fatal(err)
		}

		// ensure that if the named param happens right at the end it still works
		// ensure that NamedQuery works with a map[string]interface{}
		rows, err = db.NamedQuery("SELECT * FROM person WHERE first_name=:first", map[string]interface{}{"first": "Bin"})
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
			t.Error(err)
		}
		err = stmt.Get(&john, "John")
		if err != nil {
			t.Error(err)
		}

		// test name mapping
		NameMapper = strings.ToUpper
		rsa := CPlace{}
		err = db.Get(&rsa, "SELECT * FROM capplace;")
		if err != nil {
			fmt.Println(db.DriverName())
			t.Error(err)
		}
		NameMapper = strings.ToLower
	})

}

type Product struct {
	ProductID int
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
	var err error

	q1 := `INSERT INTO foo (a, b, c, d) VALUES (:name, :age, :first, :last)`

	type tt struct {
		Name  string
		Age   int
		First string
		Last  string
	}

	type tt2 struct {
		Field1 string `db:"field_1"`
		Field2 string `db:"field_2"`
	}

	type tt3 struct {
		tt2
		Name string
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

	am2 := tt2{"Hello", "World"}
	bq, args, _ = BindStruct(QUESTION, "INSERT INTO foo (a, b) VALUES (:field_2, :field_1)", am2)
	expect = `INSERT INTO foo (a, b) VALUES (?, ?)`
	if bq != expect {
		t.Errorf("Interpolation of query failed: got `%v`, expected `%v`\n", bq, expect)
	}

	if args[0].(string) != "World" {
		t.Errorf("Expected 'World', got %s\n", args[0].(string))
	}
	if args[1].(string) != "Hello" {
		t.Errorf("Expected 'Hello', got %s\n", args[1].(string))
	}

	am3 := tt3{Name: "Hello!"}
	am3.Field1 = "Hello"
	am3.Field2 = "World"

	bq, args, err = BindStruct(QUESTION, "INSERT INTO foo (a, b, c) VALUES (:name, :field_1, :field_2)", am3)

	if err != nil {
		t.Fatal(err)
	}

	expect = `INSERT INTO foo (a, b, c) VALUES (?, ?, ?)`
	if bq != expect {
		t.Errorf("Interpolation of query failed: got `%v`, expected `%v`\n", bq, expect)
	}

	if args[0].(string) != "Hello!" {
		t.Errorf("Expected 'Hello!', got %s\n", args[0].(string))
	}
	if args[1].(string) != "Hello" {
		t.Errorf("Expected 'Hello', got %s\n", args[1].(string))
	}
	if args[2].(string) != "World" {
		t.Errorf("Expected 'World', got %s\n", args[0].(string))
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

func TestGetFieldMap(t *testing.T) {
	testingTable := map[reflect.Type]fieldMap{
		reflect.TypeOf(new(Person)): {"first_name": 0, "last_name": 1, "email": 2, "added_at": 3},
		reflect.TypeOf(new(Place)):  {"country": 0, "city": 1, "telcode": 2},
		reflect.TypeOf(new(PersonPlace)): {
			"first_name": 0, "last_name": 1, "email": 2, "added_at": 3,
			"country": 4, "city": 5, "telcode": 6},
		reflect.TypeOf(new(PersonPlacePtr)): {
			"first_name": 0, "last_name": 1, "email": 2, "added_at": 3,
			"country": 4, "city": 5, "telcode": 6},
	}
	for typ, expected := range testingTable {
		fields, err := getFieldMap(typ)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(fields, expected) {
			t.Fatalf("Fieldmap error: got `%v`, expected `%v`", fields, expected)
		}
	}
}
