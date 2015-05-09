package reflectx

import (
	"log"
	"reflect"
	"strings"
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
	fv := reflect.ValueOf(f)
	m := NewMapper("")

	v := m.FieldByName(fv, "A")
	if ival(v) != f.A {
		t.Errorf("Expecting %d, got %d", ival(v), f.A)
	}
	v = m.FieldByName(fv, "B")
	if ival(v) != f.B {
		t.Errorf("Expecting %d, got %d", f.B, ival(v))
	}
	v = m.FieldByName(fv, "C")
	if ival(v) != f.C {
		t.Errorf("Expecting %d, got %d", f.C, ival(v))
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

	m := NewMapper("")

	z := Baz{}
	z.A = 1
	z.B = 2
	z.Bar.Foo.A = 3
	zv := reflect.ValueOf(z)

	// m.FieldInfoByName(zv, "A")

	// .. or m.FieldByName() .. make this return the fieldInfo object...?1

	v := m.FieldByName(zv, "A")
	if ival(v) != z.A {
		t.Errorf("Expecting %d, got %d", ival(v), z.A)
	}
	v = m.FieldByName(zv, "B")
	if ival(v) != z.B {
		t.Errorf("Expecting %d, got %d", ival(v), z.B)
	}
}

func TestFieldInfo(t *testing.T) {
	type Foo struct {
		A int `db:"a,omitempty"`
	}

	type Bar struct {
		Foo
		B int `db:"b"`
	}

	m := NewMapper("db")

	b := Bar{}
	b.A = 1
	b.B = 2
	bv := reflect.ValueOf(b)

	v := m.FieldByName(bv, "a") // .. hmm.. so this returns the reflect.Value .. but we want options for a field..
	if ival(v) != b.A {
		t.Errorf("Expecting %d, got %d", ival(v), b.A)
	}
	v = m.FieldByName(bv, "b")
	if ival(v) != b.B {
		t.Errorf("Expecting %d, got %d", ival(v), b.B)
	}

	// hmm.. return it all separately,
	// then, have a flatten thing..? the returns output
	// is already flattened..

	//---------
	// a question would be, how does reflect traverse a struct now with other structs..?
	//---------

	// .... hmm.. so we get "a" from BV ....  cuz of the inline..
	// its like, ",inline" isnt even necessary

	// log.Println("--- BAR ---")
	// fi := m.TypeMap(reflect.TypeOf(b))
	// log.Println(fi)
	// log.Println("====>", fi["b"])
	// log.Println("====>", fi["a"])

	//--
	// Test Bar2

	// so in this case... right now.. the db:"f" does nothing at all..
	// but it kinda should...
	// what would be an ideal response...?

	type Bar2 struct {
		Foo `db:"f"`
		B   int `db:"b"`
	}

	b2 := Bar2{}
	b2.A = 1
	b2.B = 2
	bv2 := reflect.ValueOf(b2)
	v = m.FieldByName(bv2, "a")
	if ival(v) != b2.A {
		t.Errorf("Expecting %d, got %d", ival(v), b2.A)
	}
	v = m.FieldByName(bv2, "b")
	if ival(v) != b2.B {
		t.Errorf("Expecting %d, got %d", ival(v), b2.B)
	}

	//--
	// Test Bar3

	type Bar3 struct {
		F Foo `db:"f"`
		B int `db:"b"`
	}

	b3 := Bar3{}
	b3.F.A = 1
	b3.B = 2
	bv3 := reflect.ValueOf(b3)

	v = m.FieldByName(bv3, "f")
	if v.Type() != reflect.TypeOf(Foo{}) {
		t.Errorf("Expecting %d, got %d", reflect.TypeOf(Foo{}), v.Type())
	}
	vfoo := v.Interface().(Foo)
	if vfoo.A != b3.F.A {
		t.Errorf("Expecting %d, got %d", b3.F.A, vfoo)
	}

	v = m.FieldByName(bv2, "b")
	if ival(v) != b3.B {
		t.Errorf("Expecting %d, got %d", b3.B, ival(v))
	}

	//--
	// Test Bar4

	type Bar4 struct {
		Foo `db:"-"`
		B   int `db:"b"`
	}

	b4 := Bar4{}
	b4.A = 1
	b4.B = 2
	bv4 := reflect.ValueOf(b4)
	v = m.FieldByName(bv4, "a")
	if v.IsValid() {
		t.Errorf("should not be defined..")
	}
	v = m.FieldByName(bv4, "b")
	if ival(v) != b4.B {
		t.Errorf("Expecting %d, got %d", ival(v), b4.B)
	}

	// ...

	type Person struct {
		Name string
	}
	type Place struct {
		Name string
	}
	type PP struct {
		Person `db:"person,prefix=person."`
		Place  `db:",prefix=place."`
	}

	log.Println("** PPV **")

	pp := PP{}
	pp.Person.Name = "Peter"
	pp.Place.Name = "Toronto"

	fi := m.TypeMap(reflect.TypeOf(pp))
	log.Println("FI:", fi)
	for k, i := range fi {
		log.Println("key:", k, "=", i)
	}

	ppv := reflect.ValueOf(pp)

	v = m.FieldByName(ppv, "person") // should return Person struct
	if v.Type() != reflect.TypeOf(Person{}) {
		t.Errorf("Expecting %d, got %d", reflect.TypeOf(Person{}), v.Type())
	}

	v = m.FieldByName(ppv, "Name") // should return Place.Name string

	// ??
	v = m.FieldByName(ppv, "Place") // should return Place struct
	// .. Path: [1], Embedded: true, Name: "", Options: {"prefix":"place."}, Field: Place{..}
	// .. no name, uses default of the FieldName .....?
	// or, dont return a fieldMap .. returns []fieldInfo with a .Get() .. what does json do..?

	v = m.FieldByName(ppv, "person.Name") //
}

func TestTagNameMapping(t *testing.T) {
	type Strategy struct {
		StrategyID   string `protobuf:"bytes,1,opt,name=strategy_id" json:"strategy_id,omitempty"`
		StrategyName string
	}

	m := NewMapperTagFunc("json", strings.ToUpper, func(value string) string {
		if strings.Contains(value, ",") {
			return strings.Split(value, ",")[0]
		}
		return value
	})
	strategy := Strategy{"1", "Alpah"}
	mapping := m.TypeMap(reflect.TypeOf(strategy))

	for _, key := range []string{"strategy_id", "STRATEGYNAME"} {
		if _, ok := mapping[key]; !ok {
			t.Errorf("Expecting to find key %s in mapping but did not.", key)
		}
	}
}

func TestMapping(t *testing.T) {
	type Person struct {
		ID           int
		Name         string
		WearsGlasses bool `db:"wears_glasses"`
	}

	m := NewMapperFunc("db", strings.ToLower)
	p := Person{1, "Jason", true}
	mapping := m.TypeMap(reflect.TypeOf(p))

	for _, key := range []string{"id", "name", "wears_glasses"} {
		if _, ok := mapping[key]; !ok {
			t.Errorf("Expecting to find key %s in mapping but did not.", key)
		}
	}

	type SportsPerson struct {
		Weight int
		Age    int
		Person
	}
	s := SportsPerson{Weight: 100, Age: 30, Person: p}
	mapping = m.TypeMap(reflect.TypeOf(s))
	for _, key := range []string{"id", "name", "wears_glasses", "weight", "age"} {
		if _, ok := mapping[key]; !ok {
			t.Errorf("Expecting to find key %s in mapping but did not.", key)
		}

	}

	type RugbyPlayer struct {
		Position   int
		IsIntense  bool `db:"is_intense"`
		IsAllBlack bool `db:"-"`
		SportsPerson
	}
	r := RugbyPlayer{12, true, false, s}
	mapping = m.TypeMap(reflect.TypeOf(r))
	for _, key := range []string{"id", "name", "wears_glasses", "weight", "age", "position", "is_intense"} {
		if _, ok := mapping[key]; !ok {
			t.Errorf("Expecting to find key %s in mapping but did not.", key)
		}
	}

	if _, ok := mapping["isallblack"]; ok {
		t.Errorf("Expecting to ignore `IsAllBlack` field")
	}

	type EmbeddedLiteral struct {
		Embedded struct {
			Person   string
			Position int
		}
		IsIntense bool
	}

	e := EmbeddedLiteral{}
	mapping = m.TypeMap(reflect.TypeOf(e))
	//fmt.Printf("Mapping: %#v\n", mapping)

	//f := FieldByIndexes(reflect.ValueOf(e), mapping["isintense"])
	//fmt.Println(f, f.Interface())

	//tbn := m.TraversalsByName(reflect.TypeOf(e), []string{"isintense"})
	//fmt.Printf("%#v\n", tbn)

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

func BenchmarkFieldByIndexL4(b *testing.B) {
	e4 := E4{}
	e4.A = 1
	idx := []int{0, 0, 0, 0}
	for i := 0; i < b.N; i++ {
		v := reflect.ValueOf(e4)
		f := FieldByIndexes(v, idx)
		if f.Interface().(int) != 1 {
			b.Fatal("Wrong value.")
		}
	}
}
