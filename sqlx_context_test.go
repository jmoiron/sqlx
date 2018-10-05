// +build go1.8

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
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx/reflectx"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

func MultiExecContext(ctx context.Context, e ExecerContext, query string) {
	stmts := strings.Split(query, ";\n")
	if len(strings.Trim(stmts[len(stmts)-1], " \n\t\r")) == 0 {
		stmts = stmts[:len(stmts)-1]
	}
	for _, s := range stmts {
		_, err := e.ExecContext(ctx, s)
		if err != nil {
			fmt.Println(err, s)
		}
	}
}

func RunWithSchemaContext(ctx context.Context, schema Schema, t *testing.T, test func(ctx context.Context, db *DB, t *testing.T)) {
	runner := func(ctx context.Context, db *DB, t *testing.T, create, drop string) {
		defer func() {
			MultiExecContext(ctx, db, drop)
		}()

		MultiExecContext(ctx, db, create)
		test(ctx, db, t)
	}

	if TestPostgres {
		create, drop := schema.Postgres()
		runner(ctx, pgdb, t, create, drop)
	}
	if TestSqlite {
		create, drop := schema.Sqlite3()
		runner(ctx, sldb, t, create, drop)
	}
	if TestMysql {
		create, drop := schema.MySQL()
		runner(ctx, mysqldb, t, create, drop)
	}
}

func loadDefaultFixtureContext(ctx context.Context, db *DB, t *testing.T) {
	tx := db.MustBeginTx(ctx, nil)
	tx.MustExecContext(ctx, tx.Rebind("INSERT INTO person (first_name, last_name, email) VALUES (?, ?, ?)"), "Jason", "Moiron", "jmoiron@jmoiron.net")
	tx.MustExecContext(ctx, tx.Rebind("INSERT INTO person (first_name, last_name, email) VALUES (?, ?, ?)"), "John", "Doe", "johndoeDNE@gmail.net")
	tx.MustExecContext(ctx, tx.Rebind("INSERT INTO place (country, city, telcode) VALUES (?, ?, ?)"), "United States", "New York", "1")
	tx.MustExecContext(ctx, tx.Rebind("INSERT INTO place (country, telcode) VALUES (?, ?)"), "Hong Kong", "852")
	tx.MustExecContext(ctx, tx.Rebind("INSERT INTO place (country, telcode) VALUES (?, ?)"), "Singapore", "65")
	if db.DriverName() == "mysql" {
		tx.MustExecContext(ctx, tx.Rebind("INSERT INTO capplace (`COUNTRY`, `TELCODE`) VALUES (?, ?)"), "Sarf Efrica", "27")
	} else {
		tx.MustExecContext(ctx, tx.Rebind("INSERT INTO capplace (\"COUNTRY\", \"TELCODE\") VALUES (?, ?)"), "Sarf Efrica", "27")
	}
	tx.MustExecContext(ctx, tx.Rebind("INSERT INTO employees (name, id) VALUES (?, ?)"), "Peter", "4444")
	tx.MustExecContext(ctx, tx.Rebind("INSERT INTO employees (name, id, boss_id) VALUES (?, ?, ?)"), "Joe", "1", "4444")
	tx.MustExecContext(ctx, tx.Rebind("INSERT INTO employees (name, id, boss_id) VALUES (?, ?, ?)"), "Martin", "2", "4444")
	tx.Commit()
}

// Test a new backwards compatible feature, that missing scan destinations
// will silently scan into sql.RawText rather than failing/panicing
func TestMissingNamesContextContext(t *testing.T) {
	RunWithSchemaContext(context.Background(), defaultSchema, t, func(ctx context.Context, db *DB, t *testing.T) {
		loadDefaultFixtureContext(ctx, db, t)
		type PersonPlus struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
			Email     string
			//AddedAt time.Time `db:"added_at"`
		}

		// test Select first
		pps := []PersonPlus{}
		// pps lacks added_at destination
		err := db.SelectContext(ctx, &pps, "SELECT * FROM person")
		if err == nil {
			t.Error("Expected missing name from Select to fail, but it did not.")
		}

		// test Get
		pp := PersonPlus{}
		err = db.GetContext(ctx, &pp, "SELECT * FROM person LIMIT 1")
		if err == nil {
			t.Error("Expected missing name Get to fail, but it did not.")
		}

		// test naked StructScan
		pps = []PersonPlus{}
		rows, err := db.QueryContext(ctx, "SELECT * FROM person LIMIT 1")
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
		err = db.SelectContext(ctx, &pps, "SELECT * FROM person")
		if err != nil {
			t.Error(err)
		}

		// test Get
		pp = PersonPlus{}
		err = db.GetContext(ctx, &pp, "SELECT * FROM person LIMIT 1")
		if err != nil {
			t.Error(err)
		}

		// test naked StructScan
		pps = []PersonPlus{}
		rowsx, err := db.QueryxContext(ctx, "SELECT * FROM person LIMIT 1")
		if err != nil {
			t.Fatal(err)
		}
		rowsx.Next()
		err = StructScan(rowsx, &pps)
		if err != nil {
			t.Error(err)
		}
		rowsx.Close()

		// test Named stmt
		if !isUnsafe(db) {
			t.Error("Expected db to be unsafe, but it isn't")
		}
		nstmt, err := db.PrepareNamedContext(ctx, `SELECT * FROM person WHERE first_name != :name`)
		if err != nil {
			t.Fatal(err)
		}
		// its internal stmt should be marked unsafe
		if !nstmt.Stmt.unsafe {
			t.Error("expected NamedStmt to be unsafe but its underlying stmt did not inherit safety")
		}
		pps = []PersonPlus{}
		err = nstmt.SelectContext(ctx, &pps, map[string]interface{}{"name": "Jason"})
		if err != nil {
			t.Fatal(err)
		}
		if len(pps) != 1 {
			t.Errorf("Expected 1 person back, got %d", len(pps))
		}

		// test it with a safe db
		db.unsafe = false
		if isUnsafe(db) {
			t.Error("expected db to be safe but it isn't")
		}
		nstmt, err = db.PrepareNamedContext(ctx, `SELECT * FROM person WHERE first_name != :name`)
		if err != nil {
			t.Fatal(err)
		}
		// it should be safe
		if isUnsafe(nstmt) {
			t.Error("NamedStmt did not inherit safety")
		}
		nstmt.Unsafe()
		if !isUnsafe(nstmt) {
			t.Error("expected newly unsafed NamedStmt to be unsafe")
		}
		pps = []PersonPlus{}
		err = nstmt.SelectContext(ctx, &pps, map[string]interface{}{"name": "Jason"})
		if err != nil {
			t.Fatal(err)
		}
		if len(pps) != 1 {
			t.Errorf("Expected 1 person back, got %d", len(pps))
		}

	})
}

func TestEmbeddedStructsContextContext(t *testing.T) {
	type Loop1 struct{ Person }
	type Loop2 struct{ Loop1 }
	type Loop3 struct{ Loop2 }

	RunWithSchemaContext(context.Background(), defaultSchema, t, func(ctx context.Context, db *DB, t *testing.T) {
		loadDefaultFixtureContext(ctx, db, t)
		peopleAndPlaces := []PersonPlace{}
		err := db.SelectContext(
			ctx,
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
		rows, err := db.QueryxContext(
			ctx,
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
		err = db.SelectContext(
			ctx,
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

		// test "deep nesting"
		l3s := []Loop3{}
		err = db.SelectContext(ctx, &l3s, `select * from person`)
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
		err = db.SelectContext(ctx, &ec, `select * from person`)
		// I'm torn between erroring here or having some kind of working behavior
		// in order to allow for more flexibility in destination structs
		if err != nil {
			t.Errorf("Was not expecting an error on embed conflicts.")
		}
	})
}

func TestJoinQueryContext(t *testing.T) {
	type Employee struct {
		Name string
		ID   int64
		// BossID is an id into the employee table
		BossID sql.NullInt64 `db:"boss_id"`
	}
	type Boss Employee

	RunWithSchemaContext(context.Background(), defaultSchema, t, func(ctx context.Context, db *DB, t *testing.T) {
		loadDefaultFixtureContext(ctx, db, t)

		var employees []struct {
			Employee
			Boss `db:"boss"`
		}

		err := db.SelectContext(ctx,
			&employees,
			`SELECT employees.*, boss.id "boss.id", boss.name "boss.name" FROM employees
			  JOIN employees AS boss ON employees.boss_id = boss.id`)
		if err != nil {
			t.Fatal(err)
		}

		for _, em := range employees {
			if len(em.Employee.Name) == 0 {
				t.Errorf("Expected non zero lengthed name.")
			}
			if em.Employee.BossID.Int64 != em.Boss.ID {
				t.Errorf("Expected boss ids to match")
			}
		}
	})
}

func TestJoinQueryNamedPointerStructsContext(t *testing.T) {
	type Employee struct {
		Name string
		ID   int64
		// BossID is an id into the employee table
		BossID sql.NullInt64 `db:"boss_id"`
	}
	type Boss Employee

	RunWithSchemaContext(context.Background(), defaultSchema, t, func(ctx context.Context, db *DB, t *testing.T) {
		loadDefaultFixtureContext(ctx, db, t)

		var employees []struct {
			Emp1  *Employee `db:"emp1"`
			Emp2  *Employee `db:"emp2"`
			*Boss `db:"boss"`
		}

		err := db.SelectContext(ctx,
			&employees,
			`SELECT emp.name "emp1.name", emp.id "emp1.id", emp.boss_id "emp1.boss_id",
			 emp.name "emp2.name", emp.id "emp2.id", emp.boss_id "emp2.boss_id",
			 boss.id "boss.id", boss.name "boss.name" FROM employees AS emp
			  JOIN employees AS boss ON emp.boss_id = boss.id
			  `)
		if err != nil {
			t.Fatal(err)
		}

		for _, em := range employees {
			if len(em.Emp1.Name) == 0 || len(em.Emp2.Name) == 0 {
				t.Errorf("Expected non zero lengthed name.")
			}
			if em.Emp1.BossID.Int64 != em.Boss.ID || em.Emp2.BossID.Int64 != em.Boss.ID {
				t.Errorf("Expected boss ids to match")
			}
		}
	})
}

func TestSelectSliceMapTimeContext(t *testing.T) {
	RunWithSchemaContext(context.Background(), defaultSchema, t, func(ctx context.Context, db *DB, t *testing.T) {
		loadDefaultFixtureContext(ctx, db, t)
		rows, err := db.QueryxContext(ctx, "SELECT * FROM person")
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			_, err := rows.SliceScan()
			if err != nil {
				t.Error(err)
			}
		}

		rows, err = db.QueryxContext(ctx, "SELECT * FROM person")
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			m := map[string]interface{}{}
			err := rows.MapScan(m)
			if err != nil {
				t.Error(err)
			}
		}

	})
}

func TestNilReceiverContext(t *testing.T) {
	RunWithSchemaContext(context.Background(), defaultSchema, t, func(ctx context.Context, db *DB, t *testing.T) {
		loadDefaultFixtureContext(ctx, db, t)
		var p *Person
		err := db.GetContext(ctx, p, "SELECT * FROM person LIMIT 1")
		if err == nil {
			t.Error("Expected error when getting into nil struct ptr.")
		}
		var pp *[]Person
		err = db.SelectContext(ctx, pp, "SELECT * FROM person")
		if err == nil {
			t.Error("Expected an error when selecting into nil slice ptr.")
		}
	})
}

func TestNamedQueryContext(t *testing.T) {
	var schema = Schema{
		create: `
			CREATE TABLE place (
				id integer PRIMARY KEY,
				name text NULL
			);
			CREATE TABLE person (
				first_name text NULL,
				last_name text NULL,
				email text NULL
			);
			CREATE TABLE placeperson (
				first_name text NULL,
				last_name text NULL,
				email text NULL,
				place_id integer NULL
			);
			CREATE TABLE jsperson (
				"FIRST" text NULL,
				last_name text NULL,
				"EMAIL" text NULL
			);`,
		drop: `
			drop table person;
			drop table jsperson;
			drop table place;
			drop table placeperson;
			`,
	}

	RunWithSchemaContext(context.Background(), schema, t, func(ctx context.Context, db *DB, t *testing.T) {
		type Person struct {
			FirstName sql.NullString `db:"first_name"`
			LastName  sql.NullString `db:"last_name"`
			Email     sql.NullString
		}

		p := Person{
			FirstName: sql.NullString{String: "ben", Valid: true},
			LastName:  sql.NullString{String: "doe", Valid: true},
			Email:     sql.NullString{String: "ben@doe.com", Valid: true},
		}

		q1 := `INSERT INTO person (first_name, last_name, email) VALUES (:first_name, :last_name, :email)`
		_, err := db.NamedExecContext(ctx, q1, p)
		if err != nil {
			log.Fatal(err)
		}

		p2 := &Person{}
		rows, err := db.NamedQueryContext(ctx, "SELECT * FROM person WHERE first_name=:first_name", p)
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

		// these are tests for #73;  they verify that named queries work if you've
		// changed the db mapper.  This code checks both NamedQuery "ad-hoc" style
		// queries and NamedStmt queries, which use different code paths internally.
		old := *db.Mapper

		type JSONPerson struct {
			FirstName sql.NullString `json:"FIRST"`
			LastName  sql.NullString `json:"last_name"`
			Email     sql.NullString
		}

		jp := JSONPerson{
			FirstName: sql.NullString{String: "ben", Valid: true},
			LastName:  sql.NullString{String: "smith", Valid: true},
			Email:     sql.NullString{String: "ben@smith.com", Valid: true},
		}

		db.Mapper = reflectx.NewMapperFunc("json", strings.ToUpper)

		// prepare queries for case sensitivity to test our ToUpper function.
		// postgres and sqlite accept "", but mysql uses ``;  since Go's multi-line
		// strings are `` we use "" by default and swap out for MySQL
		pdb := func(s string, db *DB) string {
			if db.DriverName() == "mysql" {
				return strings.Replace(s, `"`, "`", -1)
			}
			return s
		}

		q1 = `INSERT INTO jsperson ("FIRST", last_name, "EMAIL") VALUES (:FIRST, :last_name, :EMAIL)`
		_, err = db.NamedExecContext(ctx, pdb(q1, db), jp)
		if err != nil {
			t.Fatal(err, db.DriverName())
		}

		// Checks that a person pulled out of the db matches the one we put in
		check := func(t *testing.T, rows *Rows) {
			jp = JSONPerson{}
			for rows.Next() {
				err = rows.StructScan(&jp)
				if err != nil {
					t.Error(err)
				}
				if jp.FirstName.String != "ben" {
					t.Errorf("Expected first name of `ben`, got `%s` (%s) ", jp.FirstName.String, db.DriverName())
				}
				if jp.LastName.String != "smith" {
					t.Errorf("Expected LastName of `smith`, got `%s` (%s)", jp.LastName.String, db.DriverName())
				}
				if jp.Email.String != "ben@smith.com" {
					t.Errorf("Expected first name of `doe`, got `%s` (%s)", jp.Email.String, db.DriverName())
				}
			}
		}

		ns, err := db.PrepareNamed(pdb(`
			SELECT * FROM jsperson
			WHERE
				"FIRST"=:FIRST AND
				last_name=:last_name AND
				"EMAIL"=:EMAIL
		`, db))

		if err != nil {
			t.Fatal(err)
		}
		rows, err = ns.QueryxContext(ctx, jp)
		if err != nil {
			t.Fatal(err)
		}

		check(t, rows)

		// Check exactly the same thing, but with db.NamedQuery, which does not go
		// through the PrepareNamed/NamedStmt path.
		rows, err = db.NamedQueryContext(ctx, pdb(`
			SELECT * FROM jsperson
			WHERE
				"FIRST"=:FIRST AND
				last_name=:last_name AND
				"EMAIL"=:EMAIL
		`, db), jp)
		if err != nil {
			t.Fatal(err)
		}

		check(t, rows)

		db.Mapper = &old

		// Test nested structs
		type Place struct {
			ID   int            `db:"id"`
			Name sql.NullString `db:"name"`
		}
		type PlacePerson struct {
			FirstName sql.NullString `db:"first_name"`
			LastName  sql.NullString `db:"last_name"`
			Email     sql.NullString
			Place     Place `db:"place"`
		}

		pl := Place{
			Name: sql.NullString{String: "myplace", Valid: true},
		}

		pp := PlacePerson{
			FirstName: sql.NullString{String: "ben", Valid: true},
			LastName:  sql.NullString{String: "doe", Valid: true},
			Email:     sql.NullString{String: "ben@doe.com", Valid: true},
		}

		q2 := `INSERT INTO place (id, name) VALUES (1, :name)`
		_, err = db.NamedExecContext(ctx, q2, pl)
		if err != nil {
			log.Fatal(err)
		}

		id := 1
		pp.Place.ID = id

		q3 := `INSERT INTO placeperson (first_name, last_name, email, place_id) VALUES (:first_name, :last_name, :email, :place.id)`
		_, err = db.NamedExecContext(ctx, q3, pp)
		if err != nil {
			log.Fatal(err)
		}

		pp2 := &PlacePerson{}
		rows, err = db.NamedQueryContext(ctx, `
			SELECT
				first_name,
				last_name,
				email,
				place.id AS "place.id",
				place.name AS "place.name"
			FROM placeperson
			INNER JOIN place ON place.id = placeperson.place_id
			WHERE
				place.id=:place.id`, pp)
		if err != nil {
			log.Fatal(err)
		}
		for rows.Next() {
			err = rows.StructScan(pp2)
			if err != nil {
				t.Error(err)
			}
			if pp2.FirstName.String != "ben" {
				t.Error("Expected first name of `ben`, got " + pp2.FirstName.String)
			}
			if pp2.LastName.String != "doe" {
				t.Error("Expected first name of `doe`, got " + pp2.LastName.String)
			}
			if pp2.Place.Name.String != "myplace" {
				t.Error("Expected place name of `myplace`, got " + pp2.Place.Name.String)
			}
			if pp2.Place.ID != pp.Place.ID {
				t.Errorf("Expected place name of %v, got %v", pp.Place.ID, pp2.Place.ID)
			}
		}
	})
}

func TestNilInsertsContext(t *testing.T) {
	var schema = Schema{
		create: `
			CREATE TABLE tt (
				id integer,
				value text NULL DEFAULT NULL
			);`,
		drop: "drop table tt;",
	}

	RunWithSchemaContext(context.Background(), schema, t, func(ctx context.Context, db *DB, t *testing.T) {
		type TT struct {
			ID    int
			Value *string
		}
		var v, v2 TT
		r := db.Rebind

		db.MustExecContext(ctx, r(`INSERT INTO tt (id) VALUES (1)`))
		db.GetContext(ctx, &v, r(`SELECT * FROM tt`))
		if v.ID != 1 {
			t.Errorf("Expecting id of 1, got %v", v.ID)
		}
		if v.Value != nil {
			t.Errorf("Expecting NULL to map to nil, got %s", *v.Value)
		}

		v.ID = 2
		// NOTE: this incidentally uncovered a bug which was that named queries with
		// pointer destinations would not work if the passed value here was not addressable,
		// as reflectx.FieldByIndexes attempts to allocate nil pointer receivers for
		// writing.  This was fixed by creating & using the reflectx.FieldByIndexesReadOnly
		// function.  This next line is important as it provides the only coverage for this.
		db.NamedExecContext(ctx, `INSERT INTO tt (id, value) VALUES (:id, :value)`, v)

		db.GetContext(ctx, &v2, r(`SELECT * FROM tt WHERE id=2`))
		if v.ID != v2.ID {
			t.Errorf("%v != %v", v.ID, v2.ID)
		}
		if v2.Value != nil {
			t.Errorf("Expecting NULL to map to nil, got %s", *v.Value)
		}
	})
}

func TestScanErrorContext(t *testing.T) {
	var schema = Schema{
		create: `
			CREATE TABLE kv (
				k text,
				v integer
			);`,
		drop: `drop table kv;`,
	}

	RunWithSchemaContext(context.Background(), schema, t, func(ctx context.Context, db *DB, t *testing.T) {
		type WrongTypes struct {
			K int
			V string
		}
		_, err := db.Exec(db.Rebind("INSERT INTO kv (k, v) VALUES (?, ?)"), "hi", 1)
		if err != nil {
			t.Error(err)
		}

		rows, err := db.QueryxContext(ctx, "SELECT * FROM kv")
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

// FIXME: this function is kinda big but it slows things down to be constantly
// loading and reloading the schema..

func TestUsageContext(t *testing.T) {
	RunWithSchemaContext(context.Background(), defaultSchema, t, func(ctx context.Context, db *DB, t *testing.T) {
		loadDefaultFixtureContext(ctx, db, t)
		slicemembers := []SliceMember{}
		err := db.SelectContext(ctx, &slicemembers, "SELECT * FROM place ORDER BY telcode ASC")
		if err != nil {
			t.Fatal(err)
		}

		people := []Person{}

		err = db.SelectContext(ctx, &people, "SELECT * FROM person ORDER BY first_name ASC")
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
		err = db.GetContext(ctx, &jason, db.Rebind("SELECT * FROM person WHERE first_name=?"), "Jason")

		if err != nil {
			t.Fatal(err)
		}
		if jason.FirstName != "Jason" {
			t.Errorf("Expecting to get back Jason, but got %v\n", jason.FirstName)
		}

		err = db.GetContext(ctx, &jason, db.Rebind("SELECT * FROM person WHERE first_name=?"), "Foobar")
		if err == nil {
			t.Errorf("Expecting an error, got nil\n")
		}
		if err != sql.ErrNoRows {
			t.Errorf("Expected sql.ErrNoRows, got %v\n", err)
		}

		// The following tests check statement reuse, which was actually a problem
		// due to copying being done when creating Stmt's which was eventually removed
		stmt1, err := db.PreparexContext(ctx, db.Rebind("SELECT * FROM person WHERE first_name=?"))
		if err != nil {
			t.Fatal(err)
		}
		jason = Person{}

		row := stmt1.QueryRowx("DoesNotExist")
		row.Scan(&jason)
		row = stmt1.QueryRowx("DoesNotExist")
		row.Scan(&jason)

		err = stmt1.GetContext(ctx, &jason, "DoesNotExist User")
		if err == nil {
			t.Error("Expected an error")
		}
		err = stmt1.GetContext(ctx, &jason, "DoesNotExist User 2")
		if err == nil {
			t.Fatal(err)
		}

		stmt2, err := db.PreparexContext(ctx, db.Rebind("SELECT * FROM person WHERE first_name=?"))
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
		err = db.SelectContext(ctx, &places, "SELECT telcode FROM place ORDER BY telcode ASC")
		if err != nil {
			t.Fatal(err)
		}

		usa, singsing, honkers := places[0], places[1], places[2]

		if usa.TelCode != 1 || honkers.TelCode != 852 || singsing.TelCode != 65 {
			t.Errorf("Expected integer telcodes to work, got %#v", places)
		}

		placesptr := []PlacePtr{}
		err = db.SelectContext(ctx, &placesptr, "SELECT * FROM place ORDER BY telcode ASC")
		if err != nil {
			t.Error(err)
		}
		//fmt.Printf("%#v\n%#v\n%#v\n", placesptr[0], placesptr[1], placesptr[2])

		// if you have null fields and use SELECT *, you must use sql.Null* in your struct
		// this test also verifies that you can use either a []Struct{} or a []*Struct{}
		places2 := []Place{}
		err = db.SelectContext(ctx, &places2, "SELECT * FROM place ORDER BY telcode ASC")
		if err != nil {
			t.Fatal(err)
		}

		usa, singsing, honkers = &places2[0], &places2[1], &places2[2]

		// this should return a type error that &p is not a pointer to a struct slice
		p := Place{}
		err = db.SelectContext(ctx, &p, "SELECT * FROM place ORDER BY telcode ASC")
		if err == nil {
			t.Errorf("Expected an error, argument to select should be a pointer to a struct slice")
		}

		// this should be an error
		pl := []Place{}
		err = db.SelectContext(ctx, pl, "SELECT * FROM place ORDER BY telcode ASC")
		if err == nil {
			t.Errorf("Expected an error, argument to select should be a pointer to a struct slice, not a slice.")
		}

		if usa.TelCode != 1 || honkers.TelCode != 852 || singsing.TelCode != 65 {
			t.Errorf("Expected integer telcodes to work, got %#v", places)
		}

		stmt, err := db.PreparexContext(ctx, db.Rebind("SELECT country, telcode FROM place WHERE telcode > ? ORDER BY telcode ASC"))
		if err != nil {
			t.Error(err)
		}

		places = []*Place{}
		err = stmt.SelectContext(ctx, &places, 10)
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

		rows, err := db.QueryxContext(ctx, "SELECT * FROM place")
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

		rows, err = db.QueryxContext(ctx, "SELECT * FROM place")
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

		rows, err = db.QueryxContext(ctx, "SELECT * FROM place")
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
		_, err = db.NamedExecContext(ctx, "INSERT INTO person (first_name, last_name, email) VALUES (:first, :last, :email)", map[string]interface{}{
			"first": "Bin",
			"last":  "Smuth",
			"email": "bensmith@allblacks.nz",
		})
		if err != nil {
			t.Fatal(err)
		}

		// ensure that if the named param happens right at the end it still works
		// ensure that NamedQuery works with a map[string]interface{}
		rows, err = db.NamedQueryContext(ctx, "SELECT * FROM person WHERE first_name=:first", map[string]interface{}{"first": "Bin"})
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
		ben.Email = "binsmuth@allblacks.nz"

		// Insert via a named query using the struct
		_, err = db.NamedExecContext(ctx, "INSERT INTO person (first_name, last_name, email) VALUES (:first_name, :last_name, :email)", ben)

		if err != nil {
			t.Fatal(err)
		}

		rows, err = db.NamedQueryContext(ctx, "SELECT * FROM person WHERE first_name=:first_name", ben)
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
		err = db.GetContext(ctx, person, "SELECT * FROM person WHERE first_name=$1", "does-not-exist")
		if err == nil {
			t.Fatal("Should have got an error for Get on non-existent row.")
		}

		// lets test prepared statements some more

		stmt, err = db.PreparexContext(ctx, db.Rebind("SELECT * FROM person WHERE first_name=?"))
		if err != nil {
			t.Fatal(err)
		}
		rows, err = stmt.QueryxContext(ctx, "Ben")
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
		stmt, err = db.PreparexContext(ctx, db.Rebind("SELECT * FROM person WHERE first_name=?"))
		if err != nil {
			t.Error(err)
		}
		err = stmt.GetContext(ctx, &john, "John")
		if err != nil {
			t.Error(err)
		}

		// test name mapping
		// THIS USED TO WORK BUT WILL NO LONGER WORK.
		db.MapperFunc(strings.ToUpper)
		rsa := CPlace{}
		err = db.GetContext(ctx, &rsa, "SELECT * FROM capplace;")
		if err != nil {
			t.Error(err, "in db:", db.DriverName())
		}
		db.MapperFunc(strings.ToLower)

		// create a copy and change the mapper, then verify the copy behaves
		// differently from the original.
		dbCopy := NewDb(db.DB, db.DriverName())
		dbCopy.MapperFunc(strings.ToUpper)
		err = dbCopy.GetContext(ctx, &rsa, "SELECT * FROM capplace;")
		if err != nil {
			fmt.Println(db.DriverName())
			t.Error(err)
		}

		err = db.GetContext(ctx, &rsa, "SELECT * FROM cappplace;")
		if err == nil {
			t.Error("Expected no error, got ", err)
		}

		// test base type slices
		var sdest []string
		rows, err = db.QueryxContext(ctx, "SELECT email FROM person ORDER BY email ASC;")
		if err != nil {
			t.Error(err)
		}
		err = scanAll(rows, &sdest, false)
		if err != nil {
			t.Error(err)
		}

		// test Get with base types
		var count int
		err = db.GetContext(ctx, &count, "SELECT count(*) FROM person;")
		if err != nil {
			t.Error(err)
		}
		if count != len(sdest) {
			t.Errorf("Expected %d == %d (count(*) vs len(SELECT ..)", count, len(sdest))
		}

		// test Get and Select with time.Time, #84
		var addedAt time.Time
		err = db.GetContext(ctx, &addedAt, "SELECT added_at FROM person LIMIT 1;")
		if err != nil {
			t.Error(err)
		}

		var addedAts []time.Time
		err = db.SelectContext(ctx, &addedAts, "SELECT added_at FROM person;")
		if err != nil {
			t.Error(err)
		}

		// test it on a double pointer
		var pcount *int
		err = db.GetContext(ctx, &pcount, "SELECT count(*) FROM person;")
		if err != nil {
			t.Error(err)
		}
		if *pcount != count {
			t.Errorf("expected %d = %d", *pcount, count)
		}

		// test Select...
		sdest = []string{}
		err = db.SelectContext(ctx, &sdest, "SELECT first_name FROM person ORDER BY first_name ASC;")
		if err != nil {
			t.Error(err)
		}
		expected := []string{"Ben", "Bin", "Jason", "John"}
		for i, got := range sdest {
			if got != expected[i] {
				t.Errorf("Expected %d result to be %s, but got %s", i, expected[i], got)
			}
		}

		var nsdest []sql.NullString
		err = db.SelectContext(ctx, &nsdest, "SELECT city FROM place ORDER BY city ASC")
		if err != nil {
			t.Error(err)
		}
		for _, val := range nsdest {
			if val.Valid && val.String != "New York" {
				t.Errorf("expected single valid result to be `New York`, but got %s", val.String)
			}
		}
	})
}

// tests that sqlx will not panic when the wrong driver is passed because
// of an automatic nil dereference in sqlx.Open(), which was fixed.
func TestDoNotPanicOnConnectContext(t *testing.T) {
	_, err := ConnectContext(context.Background(), "bogus", "hehe")
	if err == nil {
		t.Errorf("Should return error when using bogus driverName")
	}
}

func TestEmbeddedMapsContext(t *testing.T) {
	var schema = Schema{
		create: `
			CREATE TABLE message (
				string text,
				properties text
			);`,
		drop: `drop table message;`,
	}

	RunWithSchemaContext(context.Background(), schema, t, func(ctx context.Context, db *DB, t *testing.T) {
		messages := []Message{
			{"Hello, World", PropertyMap{"one": "1", "two": "2"}},
			{"Thanks, Joy", PropertyMap{"pull": "request"}},
		}
		q1 := `INSERT INTO message (string, properties) VALUES (:string, :properties);`
		for _, m := range messages {
			_, err := db.NamedExecContext(ctx, q1, m)
			if err != nil {
				t.Fatal(err)
			}
		}
		var count int
		err := db.GetContext(ctx, &count, "SELECT count(*) FROM message")
		if err != nil {
			t.Fatal(err)
		}
		if count != len(messages) {
			t.Fatalf("Expected %d messages in DB, found %d", len(messages), count)
		}

		var m Message
		err = db.GetContext(ctx, &m, "SELECT * FROM message LIMIT 1;")
		if err != nil {
			t.Fatal(err)
		}
		if m.Properties == nil {
			t.Fatal("Expected m.Properties to not be nil, but it was.")
		}
	})
}

func TestIssue197Context(t *testing.T) {
	// this test actually tests for a bug in database/sql:
	//   https://github.com/golang/go/issues/13905
	// this potentially makes _any_ named type that is an alias for []byte
	// unsafe to use in a lot of different ways (basically, unsafe to hold
	// onto after loading from the database).
	t.Skip()

	type mybyte []byte
	type Var struct{ Raw json.RawMessage }
	type Var2 struct{ Raw []byte }
	type Var3 struct{ Raw mybyte }
	RunWithSchemaContext(context.Background(), defaultSchema, t, func(ctx context.Context, db *DB, t *testing.T) {
		var err error
		var v, q Var
		if err = db.GetContext(ctx, &v, `SELECT '{"a": "b"}' AS raw`); err != nil {
			t.Fatal(err)
		}
		if err = db.GetContext(ctx, &q, `SELECT 'null' AS raw`); err != nil {
			t.Fatal(err)
		}

		var v2, q2 Var2
		if err = db.GetContext(ctx, &v2, `SELECT '{"a": "b"}' AS raw`); err != nil {
			t.Fatal(err)
		}
		if err = db.GetContext(ctx, &q2, `SELECT 'null' AS raw`); err != nil {
			t.Fatal(err)
		}

		var v3, q3 Var3
		if err = db.QueryRowContext(ctx, `SELECT '{"a": "b"}' AS raw`).Scan(&v3.Raw); err != nil {
			t.Fatal(err)
		}
		if err = db.QueryRowContext(ctx, `SELECT '{"c": "d"}' AS raw`).Scan(&q3.Raw); err != nil {
			t.Fatal(err)
		}
		t.Fail()
	})
}

func TestInContext(t *testing.T) {
	// some quite normal situations
	type tr struct {
		q    string
		args []interface{}
		c    int
	}
	tests := []tr{
		{"SELECT * FROM foo WHERE x = ? AND v in (?) AND y = ?",
			[]interface{}{"foo", []int{0, 5, 7, 2, 9}, "bar"},
			7},
		{"SELECT * FROM foo WHERE x in (?)",
			[]interface{}{[]int{1, 2, 3, 4, 5, 6, 7, 8}},
			8},
	}
	for _, test := range tests {
		q, a, err := In(test.q, test.args...)
		if err != nil {
			t.Error(err)
		}
		if len(a) != test.c {
			t.Errorf("Expected %d args, but got %d (%+v)", test.c, len(a), a)
		}
		if strings.Count(q, "?") != test.c {
			t.Errorf("Expected %d bindVars, got %d", test.c, strings.Count(q, "?"))
		}
	}

	// too many bindVars, but no slices, so short circuits parsing
	// i'm not sure if this is the right behavior;  this query/arg combo
	// might not work, but we shouldn't parse if we don't need to
	{
		orig := "SELECT * FROM foo WHERE x = ? AND y = ?"
		q, a, err := In(orig, "foo", "bar", "baz")
		if err != nil {
			t.Error(err)
		}
		if len(a) != 3 {
			t.Errorf("Expected 3 args, but got %d (%+v)", len(a), a)
		}
		if q != orig {
			t.Error("Expected unchanged query.")
		}
	}

	tests = []tr{
		// too many bindvars;  slice present so should return error during parse
		{"SELECT * FROM foo WHERE x = ? and y = ?",
			[]interface{}{"foo", []int{1, 2, 3}, "bar"},
			0},
		// empty slice, should return error before parse
		{"SELECT * FROM foo WHERE x = ?",
			[]interface{}{[]int{}},
			0},
		// too *few* bindvars, should return an error
		{"SELECT * FROM foo WHERE x = ? AND y in (?)",
			[]interface{}{[]int{1, 2, 3}},
			0},
	}
	for _, test := range tests {
		_, _, err := In(test.q, test.args...)
		if err == nil {
			t.Error("Expected an error, but got nil.")
		}
	}
	RunWithSchemaContext(context.Background(), defaultSchema, t, func(ctx context.Context, db *DB, t *testing.T) {
		loadDefaultFixtureContext(ctx, db, t)
		//tx.MustExecContext(ctx, tx.Rebind("INSERT INTO place (country, city, telcode) VALUES (?, ?, ?)"), "United States", "New York", "1")
		//tx.MustExecContext(ctx, tx.Rebind("INSERT INTO place (country, telcode) VALUES (?, ?)"), "Hong Kong", "852")
		//tx.MustExecContext(ctx, tx.Rebind("INSERT INTO place (country, telcode) VALUES (?, ?)"), "Singapore", "65")
		telcodes := []int{852, 65}
		q := "SELECT * FROM place WHERE telcode IN(?) ORDER BY telcode"
		query, args, err := In(q, telcodes)
		if err != nil {
			t.Error(err)
		}
		query = db.Rebind(query)
		places := []Place{}
		err = db.SelectContext(ctx, &places, query, args...)
		if err != nil {
			t.Error(err)
		}
		if len(places) != 2 {
			t.Fatalf("Expecting 2 results, got %d", len(places))
		}
		if places[0].TelCode != 65 {
			t.Errorf("Expecting singapore first, but got %#v", places[0])
		}
		if places[1].TelCode != 852 {
			t.Errorf("Expecting hong kong second, but got %#v", places[1])
		}
	})
}

func TestEmbeddedLiteralsContext(t *testing.T) {
	var schema = Schema{
		create: `
			CREATE TABLE x (
				k text
			);`,
		drop: `drop table x;`,
	}

	RunWithSchemaContext(context.Background(), schema, t, func(ctx context.Context, db *DB, t *testing.T) {
		type t1 struct {
			K *string
		}
		type t2 struct {
			Inline struct {
				F string
			}
			K *string
		}

		db.MustExecContext(ctx, db.Rebind("INSERT INTO x (k) VALUES (?), (?), (?);"), "one", "two", "three")

		target := t1{}
		err := db.GetContext(ctx, &target, db.Rebind("SELECT * FROM x WHERE k=?"), "one")
		if err != nil {
			t.Error(err)
		}
		if *target.K != "one" {
			t.Error("Expected target.K to be `one`, got ", target.K)
		}

		target2 := t2{}
		err = db.GetContext(ctx, &target2, db.Rebind("SELECT * FROM x WHERE k=?"), "one")
		if err != nil {
			t.Error(err)
		}
		if *target2.K != "one" {
			t.Errorf("Expected target2.K to be `one`, got `%v`", target2.K)
		}
	})
}
