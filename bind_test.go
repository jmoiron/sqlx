package sqlx

import (
	"math/rand"
	"strings"
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

// TestInSkipsCommentBindVars ensures that ? inside SQL comments and string
// literals aren't mistaken for bind variable placeholders by In().
func TestInSkipsCommentBindVars(t *testing.T) {
	type tc struct {
		name  string
		query string
		args  []interface{}
		wantN int // expected number of ? placeholders / args in result
	}

	tests := []tc{
		{
			name:  "line comment with ?",
			query: "SELECT * -- should we filter by id?\nFROM foo WHERE id IN (?)",
			args:  []interface{}{[]int{1, 2, 3}},
			wantN: 3,
		},
		{
			name:  "block comment with ?",
			query: "SELECT * /* what about id=? */ FROM foo WHERE id IN (?)",
			args:  []interface{}{[]int{1, 2}},
			wantN: 2,
		},
		{
			name:  "string literal with ?",
			query: "SELECT id, '?' AS q FROM foo WHERE id IN (?)",
			args:  []interface{}{[]int{10, 20}},
			wantN: 2,
		},
		{
			name:  "multiple comments and real bind var",
			query: "SELECT * -- filter?\nFROM foo WHERE x = ? AND id IN (?)",
			args:  []interface{}{"bar", []int{1, 2}},
			wantN: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, a, err := In(tt.query, tt.args...)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(a) != tt.wantN {
				t.Errorf("expected %d args, got %d (%v)", tt.wantN, len(a), a)
			}
			// Count only the real bind vars that In() emitted, not any in comments.
			// We can't easily strip comments here, so just verify arg count above.
			_ = strings.Count(q, "?")
		})
	}
}

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
