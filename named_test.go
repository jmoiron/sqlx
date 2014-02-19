package sqlx

import "testing"

func TestCompileQuery(t *testing.T) {
	table := []struct {
		Q, R, D string
		N       []string
	}{
		// basic test for named parameters, invalid char ',' terminating
		{
			Q: `INSERT INTO foo (a,b,c,d) VALUES (:name, :age, :first, :last)`,
			R: `INSERT INTO foo (a,b,c,d) VALUES (?, ?, ?, ?)`,
			D: `INSERT INTO foo (a,b,c,d) VALUES ($1, $2, $3, $4)`,
			N: []string{"name", "age", "first", "last"},
		},
		// This query tests a named parameter ending the string as well as numbers
		{
			Q: `SELECT * FROM a WHERE first_name=:name1 AND last_name=:name2`,
			R: `SELECT * FROM a WHERE first_name=? AND last_name=?`,
			D: `SELECT * FROM a WHERE first_name=$1 AND last_name=$2`,
			N: []string{"name1", "name2"},
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
		qq, names, err := compileNamedQuery([]byte(test.Q), QUESTION)
		if err != nil {
			t.Error(err)
		}
		if qq != test.R {
			t.Errorf("expected %s, got %s", test.R, qq)
		}
		if len(names) != len(test.N) {
			t.Errorf("expected %#v, got %#v", test.N, names)
		} else {
			for i, name := range names {
				if name != test.N[i] {
					t.Errorf("expected %dth name to be %s, got %s", i+1, test.N[i], name)
				}
			}
		}
		qd, _, _ := compileNamedQuery([]byte(test.Q), DOLLAR)
		if qd != test.D {
			t.Errorf("expected %s, got %s", test.D, qd)
		}
	}
}
