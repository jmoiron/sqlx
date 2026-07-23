package sqlx

import (
	"strings"
	"testing"
)

func TestNamedFieldsStruct(t *testing.T) {
	type Client struct {
		ID     string `db:"id"`
		Name   string `db:"client_name"`
		Secret string `db:"client_secret"`
		Skip   string `db:"-"`
		Plain  string // lowercased via NameMapper
	}
	c := Client{ID: "1", Name: "n", Secret: "s", Plain: "p"}
	names, err := NamedFields(c)
	if err != nil {
		t.Fatal(err)
	}
	// expect id, client_name, client_secret, plain — not Skip
	joined := strings.Join(names, ",")
	for _, want := range []string{"id", "client_name", "client_secret", "plain"} {
		found := false
		for _, n := range names {
			if n == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing %q in %v (%s)", want, names, joined)
		}
	}
	for _, n := range names {
		if n == "skip" || n == "Skip" {
			t.Fatalf("db:\"-\" field should be omitted: %v", names)
		}
	}

	cols, vals, err := NamedInsertFields(&c)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(cols, "client_name") || !strings.Contains(vals, ":client_name") {
		t.Fatalf("insert fields cols=%q vals=%q", cols, vals)
	}
	// placeholders must match columns 1:1
	if strings.Count(cols, ",")+1 != strings.Count(vals, ",")+1 {
		t.Fatalf("col/ph count mismatch: %q vs %q", cols, vals)
	}

	set, err := NamedUpdateFields(c)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(set, "id=:id") || !strings.Contains(set, "client_name=:client_name") {
		t.Fatalf("update fields: %q", set)
	}
}

func TestNamedFieldsMap(t *testing.T) {
	m := map[string]interface{}{"a": 1, "b": 2}
	names, err := NamedFields(m)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 2 {
		t.Fatalf("%v", names)
	}
}

func TestNamedFieldsErrors(t *testing.T) {
	if _, err := NamedFields(nil); err == nil {
		t.Fatal("expected nil error")
	}
	if _, err := NamedFields(42); err == nil {
		t.Fatal("expected non-struct error")
	}
	type empty struct {
		hidden int
	}
	if _, err := NamedFields(empty{}); err == nil {
		t.Fatal("expected no-fields error")
	}
}
