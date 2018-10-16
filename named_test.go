package sqlx

import (
	"database/sql"
	"testing"
)

func TestCompileQuery(t *testing.T) {
	table := []struct {
		Q, R, D, T, N string
		V             []string
	}{
		// basic test for named parameters, invalid char ',' terminating
		{
			Q: `INSERT INTO foo (a,b,c,d) VALUES (:name, :age, :first, :last)`,
			R: `INSERT INTO foo (a,b,c,d) VALUES (?, ?, ?, ?)`,
			D: `INSERT INTO foo (a,b,c,d) VALUES ($1, $2, $3, $4)`,
			T: `INSERT INTO foo (a,b,c,d) VALUES (@p1, @p2, @p3, @p4)`,
			N: `INSERT INTO foo (a,b,c,d) VALUES (:name, :age, :first, :last)`,
			V: []string{"name", "age", "first", "last"},
		},
		// This query tests a named parameter ending the string as well as numbers
		{
			Q: `SELECT * FROM a WHERE first_name=:name1 AND last_name=:name2`,
			R: `SELECT * FROM a WHERE first_name=? AND last_name=?`,
			D: `SELECT * FROM a WHERE first_name=$1 AND last_name=$2`,
			T: `SELECT * FROM a WHERE first_name=@p1 AND last_name=@p2`,
			N: `SELECT * FROM a WHERE first_name=:name1 AND last_name=:name2`,
			V: []string{"name1", "name2"},
		},
		{
			Q: `SELECT "::foo" FROM a WHERE first_name=:name1 AND last_name=:name2`,
			R: `SELECT ":foo" FROM a WHERE first_name=? AND last_name=?`,
			D: `SELECT ":foo" FROM a WHERE first_name=$1 AND last_name=$2`,
			T: `SELECT ":foo" FROM a WHERE first_name=@p1 AND last_name=@p2`,
			N: `SELECT ":foo" FROM a WHERE first_name=:name1 AND last_name=:name2`,
			V: []string{"name1", "name2"},
		},
		{
			Q: `SELECT 'a::b::c' || first_name, '::::ABC::_::' FROM person WHERE first_name=:first_name AND last_name=:last_name`,
			R: `SELECT 'a:b:c' || first_name, '::ABC:_:' FROM person WHERE first_name=? AND last_name=?`,
			D: `SELECT 'a:b:c' || first_name, '::ABC:_:' FROM person WHERE first_name=$1 AND last_name=$2`,
			T: `SELECT 'a:b:c' || first_name, '::ABC:_:' FROM person WHERE first_name=@p1 AND last_name=@p2`,
			N: `SELECT 'a:b:c' || first_name, '::ABC:_:' FROM person WHERE first_name=:first_name AND last_name=:last_name`,
			V: []string{"first_name", "last_name"},
		},
		{
			Q: `SELECT @name := "name", :age, :first, :last`,
			R: `SELECT @name := "name", ?, ?, ?`,
			D: `SELECT @name := "name", $1, $2, $3`,
			N: `SELECT @name := "name", :age, :first, :last`,
			T: `SELECT @name := "name", @p1, @p2, @p3`,
			V: []string{"age", "first", "last"},
		},
		/* This unicode awareness test sadly fails, because of our byte-wise worldview.
		 * We could certainly iterate by Rune instead, though it's a great deal slower,
		 * it's probably the RightWay(tm)
		{
			Q: `INSERT INTO foo (a,b,c,d) VALUES (:あ, :b, :キコ, :名前)`,
			R: `INSERT INTO foo (a,b,c,d) VALUES (?, ?, ?, ?)`,
			D: `INSERT INTO foo (a,b,c,d) VALUES ($1, $2, $3, $4)`,
			N: []string{"name", "age", "first", "last"},
		},
		*/
	}

	for _, test := range table {
		qr, names, err := compileNamedQuery([]byte(test.Q), QUESTION)
		if err != nil {
			t.Error(err)
		}
		if qr != test.R {
			t.Errorf("expected %s, got %s", test.R, qr)
		}
		if len(names) != len(test.V) {
			t.Errorf("expected %#v, got %#v", test.V, names)
		} else {
			for i, name := range names {
				if name != test.V[i] {
					t.Errorf("expected %dth name to be %s, got %s", i+1, test.V[i], name)
				}
			}
		}
		qd, _, _ := compileNamedQuery([]byte(test.Q), DOLLAR)
		if qd != test.D {
			t.Errorf("\nexpected: `%s`\ngot:      `%s`", test.D, qd)
		}

		qt, _, _ := compileNamedQuery([]byte(test.Q), AT)
		if qt != test.T {
			t.Errorf("\nexpected: `%s`\ngot:      `%s`", test.T, qt)
		}

		qq, _, _ := compileNamedQuery([]byte(test.Q), NAMED)
		if qq != test.N {
			t.Errorf("\nexpected: `%s`\ngot:      `%s`\n(len: %d vs %d)", test.N, qq, len(test.N), len(qq))
		}
	}
}

type Test struct {
	t *testing.T
}

func (t Test) Error(err error, msg ...interface{}) {
	if err != nil {
		if len(msg) == 0 {
			t.t.Error(err)
		} else {
			t.t.Error(msg...)
		}
	}
}

func (t Test) Errorf(err error, format string, args ...interface{}) {
	if err != nil {
		t.t.Errorf(format, args...)
	}
}

func TestNamedQueries(t *testing.T) {
	RunWithSchema(defaultSchema, t, func(db *DB, t *testing.T) {
		loadDefaultFixture(db, t)
		test := Test{t}
		var ns *NamedStmt
		var err error

		// Check that invalid preparations fail
		ns, err = db.PrepareNamed("SELECT * FROM person WHERE first_name=:first:name")
		if err == nil {
			t.Error("Expected an error with invalid prepared statement.")
		}

		ns, err = db.PrepareNamed("invalid sql")
		if err == nil {
			t.Error("Expected an error with invalid prepared statement.")
		}

		// Check closing works as anticipated
		ns, err = db.PrepareNamed("SELECT * FROM person WHERE first_name=:first_name")
		test.Error(err)
		err = ns.Close()
		test.Error(err)

		ns, err = db.PrepareNamed(`
			SELECT first_name, last_name, email 
			FROM person WHERE first_name=:first_name AND email=:email`)
		test.Error(err)

		// test Queryx w/ uses Query
		p := Person{FirstName: "Jason", LastName: "Moiron", Email: "jmoiron@jmoiron.net"}

		rows, err := ns.Queryx(p)
		test.Error(err)
		for rows.Next() {
			var p2 Person
			rows.StructScan(&p2)
			if p.FirstName != p2.FirstName {
				t.Errorf("got %s, expected %s", p.FirstName, p2.FirstName)
			}
			if p.LastName != p2.LastName {
				t.Errorf("got %s, expected %s", p.LastName, p2.LastName)
			}
			if p.Email != p2.Email {
				t.Errorf("got %s, expected %s", p.Email, p2.Email)
			}
		}

		// test Select
		people := make([]Person, 0, 5)
		err = ns.Select(&people, p)
		test.Error(err)

		if len(people) != 1 {
			t.Errorf("got %d results, expected %d", len(people), 1)
		}
		if p.FirstName != people[0].FirstName {
			t.Errorf("got %s, expected %s", p.FirstName, people[0].FirstName)
		}
		if p.LastName != people[0].LastName {
			t.Errorf("got %s, expected %s", p.LastName, people[0].LastName)
		}
		if p.Email != people[0].Email {
			t.Errorf("got %s, expected %s", p.Email, people[0].Email)
		}

		// test Exec
		ns, err = db.PrepareNamed(`
			INSERT INTO person (first_name, last_name, email)
			VALUES (:first_name, :last_name, :email)`)
		test.Error(err)

		js := Person{
			FirstName: "Julien",
			LastName:  "Savea",
			Email:     "jsavea@ab.co.nz",
		}
		_, err = ns.Exec(js)
		test.Error(err)

		// Make sure we can pull him out again
		p2 := Person{}
		db.Get(&p2, db.Rebind("SELECT * FROM person WHERE email=?"), js.Email)
		if p2.Email != js.Email {
			t.Errorf("expected %s, got %s", js.Email, p2.Email)
		}

		// test Txn NamedStmts
		tx := db.MustBegin()
		txns := tx.NamedStmt(ns)

		// We're going to add Steven in this txn
		sl := Person{
			FirstName: "Steven",
			LastName:  "Luatua",
			Email:     "sluatua@ab.co.nz",
		}

		_, err = txns.Exec(sl)
		test.Error(err)
		// then rollback...
		tx.Rollback()
		// looking for Steven after a rollback should fail
		err = db.Get(&p2, db.Rebind("SELECT * FROM person WHERE email=?"), sl.Email)
		if err != sql.ErrNoRows {
			t.Errorf("expected no rows error, got %v", err)
		}

		// now do the same, but commit
		tx = db.MustBegin()
		txns = tx.NamedStmt(ns)
		_, err = txns.Exec(sl)
		test.Error(err)
		tx.Commit()

		// looking for Steven after a Commit should succeed
		err = db.Get(&p2, db.Rebind("SELECT * FROM person WHERE email=?"), sl.Email)
		test.Error(err)
		if p2.Email != sl.Email {
			t.Errorf("expected %s, got %s", sl.Email, p2.Email)
		}

	})
}
