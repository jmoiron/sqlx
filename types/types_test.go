package types

import "testing"

func TestGzipText(t *testing.T) {
	g := GzippedText("Hello, world")
	v, err := g.Value()
	if err != nil {
		t.Errorf("Was not expecting an error")
	}
	err = (&g).Scan(v)
	if err != nil {
		t.Errorf("Was not expecting an error")
	}
	if string(g) != "Hello, world" {
		t.Errorf("Was expecting the string we sent in (Hello World), got %s", string(g))
	}
}

func TestJSONText(t *testing.T) {
	j := JSONText(`{"foo": 1, "bar": 2}`)
	v, err := j.Value()
	if err != nil {
		t.Errorf("Was not expecting an error")
	}
	err = (&j).Scan(v)
	if err != nil {
		t.Errorf("Was not expecting an error")
	}
	m := map[string]interface{}{}
	j.Unmarshal(&m)

	if m["foo"].(float64) != 1 || m["bar"].(float64) != 2 {
		t.Errorf("Expected valid json but got some garbage instead? %#v", m)
	}

	j = JSONText(`{"foo": 1, invalid, false}`)
	v, err = j.Value()
	if err == nil {
		t.Errorf("Was expecting invalid json to fail!")
	}
}

func TestBitBool(t *testing.T) {
	// Test true value
	var b BitBool = true

	v, err := b.Value()
	if err != nil {
		t.Errorf("Cannot return error")
	}
	err = (&b).Scan(v)
	if err != nil {
		t.Errorf("Was not expecting an error")
	}
	if !b {
		t.Errorf("Was expecting the bool we sent in (true), got %b", b)
	}

	// Test false value
	b = false

	v, err = b.Value()
	if err != nil {
		t.Errorf("Cannot return error")
	}
	err = (&b).Scan(v)
	if err != nil {
		t.Errorf("Was not expecting an error")
	}
	if b {
		t.Errorf("Was expecting the bool we sent in (false), got %b", b)
	}
}
