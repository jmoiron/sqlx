package reflect

import "testing"
import r "reflect"

func ival(v r.Value) int {
	return v.Interface().(int)
}

func TestBasic(t *testing.T) {
	type Foo struct {
		A int
		B int
		C int
	}

	f := Foo{1, 2, 3}

	v := FieldByName(f, "A")
	if ival(v) != f.A {
		t.Errorf("Expecting %d, got %d", ival(v), f.A)
	}
	v = FieldByName(f, "B")
	if ival(v) != f.B {
		t.Errorf("Expecting %d, got %d", ival(v), f.B)
	}
	v = FieldByName(f, "C")
	if ival(v) != f.C {
		t.Errorf("Expecting %d, got %d", ival(v), f.C)
	}
}

func TestEmbedded(t *testing.T) {
	type Foo struct {
		A int
	}

	type Bar struct {
		Foo
		B int
	}

	type Baz struct {
		A int
		Bar
	}

	z := Baz{}
	z.A = 1
	z.B = 2
	z.Bar.Foo.A = 3

	v := FieldByName(z, "A")
	if ival(v) != z.A {
		t.Errorf("Expecting %d, got %d", ival(v), z.A)
	}
	v = FieldByName(z, "B")
	if ival(v) != z.B {
		t.Errorf("Expecting %d, got %d", ival(v), z.B)
	}
}
