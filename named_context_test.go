// +build go1.8

package sqlx

import (
	"context"
	"database/sql"
	"testing"
)

func TestNamedContextQueries(t *testing.T) {
	RunWithSchema(defaultSchema, t, func(db *DB, t *testing.T) {
		loadDefaultFixture(db, t)
		test := Test{t}
		var ns *NamedStmt
		var err error

		ctx := context.Background()

		// Check that invalid preparations fail
		ns, err = db.PrepareNamedContext(ctx, "SELECT * FROM person WHERE first_name=:first:name")
		if err == nil {
			t.Error("Expected an error with invalid prepared statement.")
		}

		ns, err = db.PrepareNamedContext(ctx, "invalid sql")
		if err == nil {
			t.Error("Expected an error with invalid prepared statement.")
		}

		// Check closing works as anticipated
		ns, err = db.PrepareNamedContext(ctx, "SELECT * FROM person WHERE first_name=:first_name")
		test.Error(err)
		err = ns.Close()
		test.Error(err)

		ns, err = db.PrepareNamedContext(ctx, `
			SELECT first_name, last_name, email
			FROM person WHERE first_name=:first_name AND email=:email`)
		test.Error(err)

		// test Queryx w/ uses Query
		p := Person{FirstName: "Jason", LastName: "Moiron", Email: "jmoiron@jmoiron.net"}

		rows, err := ns.QueryxContext(ctx, p)
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
		err = ns.SelectContext(ctx, &people, p)
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
		ns, err = db.PrepareNamedContext(ctx, `
			INSERT INTO person (first_name, last_name, email)
			VALUES (:first_name, :last_name, :email)`)
		test.Error(err)

		js := Person{
			FirstName: "Julien",
			LastName:  "Savea",
			Email:     "jsavea@ab.co.nz",
		}
		_, err = ns.ExecContext(ctx, js)
		test.Error(err)

		// Make sure we can pull him out again
		p2 := Person{}
		db.GetContext(ctx, &p2, db.Rebind("SELECT * FROM person WHERE email=?"), js.Email)
		if p2.Email != js.Email {
			t.Errorf("expected %s, got %s", js.Email, p2.Email)
		}

		// test Txn NamedStmts
		tx := db.MustBeginTx(ctx, nil)
		txns := tx.NamedStmtContext(ctx, ns)

		// We're going to add Steven in this txn
		sl := Person{
			FirstName: "Steven",
			LastName:  "Luatua",
			Email:     "sluatua@ab.co.nz",
		}

		_, err = txns.ExecContext(ctx, sl)
		test.Error(err)
		// then rollback...
		tx.Rollback()
		// looking for Steven after a rollback should fail
		err = db.GetContext(ctx, &p2, db.Rebind("SELECT * FROM person WHERE email=?"), sl.Email)
		if err != sql.ErrNoRows {
			t.Errorf("expected no rows error, got %v", err)
		}

		// now do the same, but commit
		tx = db.MustBeginTx(ctx, nil)
		txns = tx.NamedStmtContext(ctx, ns)
		_, err = txns.ExecContext(ctx, sl)
		test.Error(err)
		tx.Commit()

		// looking for Steven after a Commit should succeed
		err = db.GetContext(ctx, &p2, db.Rebind("SELECT * FROM person WHERE email=?"), sl.Email)
		test.Error(err)
		if p2.Email != sl.Email {
			t.Errorf("expected %s, got %s", sl.Email, p2.Email)
		}

	})
}
