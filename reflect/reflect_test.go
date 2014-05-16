package reflect

import (
	"reflect"
	"testing"
)

func ival(v reflect.Value) int {
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

func TestMapping(t *testing.T) {
	type Person struct {
		ID           int
		Name         string
		WearsGlasses bool `db:"wears_glasses"`
	}

	//m := NewMapperFunc("db", strings.ToLower)
	//p := Person{1, "Jason", true}
}

type E1 struct {
	A int
}
type E2 struct {
	E1
	B int
}
type E3 struct {
	E2
	C int
}
type E4 struct {
	E3
	D int
}

func BenchmarkFieldNameL1(b *testing.B) {
	e4 := E4{D: 1}
	for i := 0; i < b.N; i++ {
		v := reflect.ValueOf(e4)
		f := v.FieldByName("D")
		if f.Interface().(int) != 1 {
			b.Fatal("Wrong value.")
		}
	}
}

func BenchmarkFieldNameL4(b *testing.B) {
	e4 := E4{}
	e4.A = 1
	for i := 0; i < b.N; i++ {
		v := reflect.ValueOf(e4)
		f := v.FieldByName("A")
		if f.Interface().(int) != 1 {
			b.Fatal("Wrong value.")
		}
	}
}

func BenchmarkFieldPosL1(b *testing.B) {
	e4 := E4{D: 1}
	for i := 0; i < b.N; i++ {
		v := reflect.ValueOf(e4)
		f := v.Field(1)
		if f.Interface().(int) != 1 {
			b.Fatal("Wrong value.")
		}
	}
}

func BenchmarkFieldPosL4(b *testing.B) {
	e4 := E4{}
	e4.A = 1
	for i := 0; i < b.N; i++ {
		v := reflect.ValueOf(e4)
		f := v.Field(0)
		f = f.Field(0)
		f = f.Field(0)
		f = f.Field(0)
		if f.Interface().(int) != 1 {
			b.Fatal("Wrong value.")
		}
	}
}
