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

func TestJsonText(t *testing.T) {
	j := JsonText(`{"foo": 1, "bar": 2}`)
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

	j = JsonText(`{"foo": 1, invalid, false}`)
	v, err = j.Value()
	if err == nil {
		t.Errorf("Was expecting invalid json to fail!")
	}
}
