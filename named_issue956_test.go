package sqlx

import "testing"

func TestNamedPostgresCastInIdentifier(t *testing.T) {
	query := `SELECT DISTINCT t.path::text AS catalog_path WHERE t.company_id = :company_id FROM table AS t`
	q, args, err := Named(query, map[string]interface{}{"company_id": 555})
	if err != nil {
		t.Fatal(err)
	}
	want := `SELECT DISTINCT t.path::text AS catalog_path WHERE t.company_id = ? FROM table AS t`
	if q != want {
		t.Fatalf("got %q want %q", q, want)
	}
	if len(args) != 1 || args[0].(int) != 555 {
		t.Fatalf("args = %#v", args)
	}
}

func TestNamedPostgresCastAfterNamedParam(t *testing.T) {
	query := `SELECT :boundary::jsonb AS boundary`
	q, args, err := Named(query, map[string]interface{}{"boundary": `{"type":"Polygon"}`})
	if err != nil {
		t.Fatal(err)
	}
	want := `SELECT ?::jsonb AS boundary`
	if q != want {
		t.Fatalf("got %q want %q", q, want)
	}
	if len(args) != 1 {
		t.Fatalf("args = %#v", args)
	}
}
