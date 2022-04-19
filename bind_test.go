package sqlx

import (
	"math/rand"
	"testing"
)

func oldBindType(driverName string) int {
	switch driverName {
	case "postgres", "pgx", "pq-timeouts", "cloudsqlpostgres", "ql":
		return DOLLAR
	case "mysql":
		return QUESTION
	case "sqlite3":
		return QUESTION
	case "oci8", "ora", "goracle", "godror":
		return NAMED
	case "sqlserver":
		return AT
	}
	return UNKNOWN
}

/*
sync.Map implementation:

goos: linux
goarch: amd64
pkg: github.com/jmoiron/sqlx
BenchmarkBindSpeed/old-4         	100000000	        11.0 ns/op
BenchmarkBindSpeed/new-4         	24575726	        50.8 ns/op


async.Value map implementation:

goos: linux
goarch: amd64
pkg: github.com/jmoiron/sqlx
BenchmarkBindSpeed/old-4         	100000000	        11.0 ns/op
BenchmarkBindSpeed/new-4         	42535839	        27.5 ns/op
*/

func BenchmarkBindSpeed(b *testing.B) {
	testDrivers := []string{
		"postgres", "pgx", "mysql", "sqlite3", "ora", "sqlserver",
	}

	b.Run("old", func(b *testing.B) {
		b.StopTimer()
		var seq []int
		for i := 0; i < b.N; i++ {
			seq = append(seq, rand.Intn(len(testDrivers)))
		}
		b.StartTimer()
		for i := 0; i < b.N; i++ {
			s := oldBindType(testDrivers[seq[i]])
			if s == UNKNOWN {
				b.Error("unknown driver")
			}
		}

	})

	b.Run("new", func(b *testing.B) {
		b.StopTimer()
		var seq []int
		for i := 0; i < b.N; i++ {
			seq = append(seq, rand.Intn(len(testDrivers)))
		}
		b.StartTimer()
		for i := 0; i < b.N; i++ {
			s := BindType(testDrivers[seq[i]])
			if s == UNKNOWN {
				b.Error("unknown driver")
			}
		}

	})
}

func TestNewRebind(t *testing.T) {
	var tests = []struct {
		name     string
		query    string
		question string
		dollar   string
		named    string
		at       string
	}{
		{
			name:     "q1",
			query:    `INSERT INTO foo (a, b, c, d, e, f, g, h, i) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			question: `INSERT INTO foo (a, b, c, d, e, f, g, h, i) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			dollar:   `INSERT INTO foo (a, b, c, d, e, f, g, h, i) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
			named:    `INSERT INTO foo (a, b, c, d, e, f, g, h, i) VALUES (:arg1, :arg2, :arg3, :arg4, :arg5, :arg6, :arg7, :arg8, :arg9, :arg10)`,
			at:       `INSERT INTO foo (a, b, c, d, e, f, g, h, i) VALUES (@p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10)`,
		},
		{
			name:     "q2",
			query:    `INSERT INTO foo (a, b, c) VALUES (?, ?, "foo"), ("Hi", ?, ?)`,
			question: `INSERT INTO foo (a, b, c) VALUES (?, ?, "foo"), ("Hi", ?, ?)`,
			dollar:   `INSERT INTO foo (a, b, c) VALUES ($1, $2, "foo"), ("Hi", $3, $4)`,
			named:    `INSERT INTO foo (a, b, c) VALUES (:arg1, :arg2, "foo"), ("Hi", :arg3, :arg4)`,
			at:       `INSERT INTO foo (a, b, c) VALUES (@p1, @p2, "foo"), ("Hi", @p3, @p4)`,
		},
		{
			name:     "q3: question escaped",
			query:    `SELECT * FROM test_table where id = ? AND name = 'four?'`,
			question: `SELECT * FROM test_table where id = ? AND name = 'four?'`,
			dollar:   `SELECT * FROM test_table where id = $1 AND name = 'four?'`,
			named:    `SELECT * FROM test_table where id = :arg1 AND name = 'four?'`,
			at:       `SELECT * FROM test_table where id = @p1 AND name = 'four?'`,
		},
		{
			name:     "q4: escaped single quote and escaped question mark",
			query:    `INSERT INTO test_table (name, country) VALUES ('Maybe O''Reilly?', 'US') WHERE id = ?`,
			question: `INSERT INTO test_table (name, country) VALUES ('Maybe O''Reilly?', 'US') WHERE id = ?`,
			dollar:   `INSERT INTO test_table (name, country) VALUES ('Maybe O''Reilly?', 'US') WHERE id = $1`,
			named:    `INSERT INTO test_table (name, country) VALUES ('Maybe O''Reilly?', 'US') WHERE id = :arg1`,
			at:       `INSERT INTO test_table (name, country) VALUES ('Maybe O''Reilly?', 'US') WHERE id = @p1`,
		},
		{
			name:     "q5: escaped double quote and escaped question mark",
			query:    `INSERT INTO test_table ("inches aka"" is the column name i think?") VALUES (42) WHERE id = ?`,
			question: `INSERT INTO test_table ("inches aka"" is the column name i think?") VALUES (42) WHERE id = ?`,
			dollar:   `INSERT INTO test_table ("inches aka"" is the column name i think?") VALUES (42) WHERE id = $1`,
			named:    `INSERT INTO test_table ("inches aka"" is the column name i think?") VALUES (42) WHERE id = :arg1`,
			at:       `INSERT INTO test_table ("inches aka"" is the column name i think?") VALUES (42) WHERE id = @p1`,
		},
		{
			name:     "q6: backslash escaped quote and escaped question mark",
			query:    `INSERT INTO test_table (name, country) VALUES ('Maybe O\'Reilly?', 'US') WHERE id = ?`,
			question: `INSERT INTO test_table (name, country) VALUES ('Maybe O\'Reilly?', 'US') WHERE id = ?`,
			dollar:   `INSERT INTO test_table (name, country) VALUES ('Maybe O\'Reilly?', 'US') WHERE id = $1`,
			named:    `INSERT INTO test_table (name, country) VALUES ('Maybe O\'Reilly?', 'US') WHERE id = :arg1`,
			at:       `INSERT INTO test_table (name, country) VALUES ('Maybe O\'Reilly?', 'US') WHERE id = @p1`,
		},
	}

	for _, test := range tests {
		q := Rebind(QUESTION, test.query)
		if q != test.question {
			t.Errorf("%s failed at 'question'", test.name)
		}
		d := Rebind(DOLLAR, test.query)
		if d != test.dollar {
			t.Errorf("%s failed at 'dollar'", test.name)
		}
		n := Rebind(NAMED, test.query)
		if n != test.named {
			t.Errorf("%s failed at 'named'", test.name)
		}
		a := Rebind(AT, test.query)
		if a != test.at {
			t.Errorf("%s failed at 'at'", test.name)
		}
	}
}

/*
goos: linux
goarch: amd64
pkg: github.com/jmoiron/sqlx
BenchmarkRebind/index-8        	 2114181	       598 ns/op
BenchmarkRebind/new-8         	 2827218	       747 ns/op
BenchmarkRebind/buff-8        	 1000000	      1678 ns/op
*/

func BenchmarkRebind(b *testing.B) {
	q1 := `INSERT INTO foo (a, b, c, d, e, f, g, h, i) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	q2 := `INSERT INTO foo (a, b, c) VALUES (?, ?, "foo"), ("Hi", ?, ?)`

	b.Run("index", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rebindIndex(DOLLAR, q1)
			rebindIndex(DOLLAR, q2)
		}
	})

	b.Run("new", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			Rebind(DOLLAR, q1)
			Rebind(DOLLAR, q2)
		}
	})

	b.Run("buff", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rebindBuff(DOLLAR, q1)
			rebindBuff(DOLLAR, q2)
		}
	})
}
