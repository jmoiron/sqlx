package reflectx

import (
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
	m := NewMapperFunc("", func(s string) string { return s })

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

func TestBasicEmbedded(t *testing.T) {
	type Foo struct {
		A int
	}

	type Bar struct {
		Foo // `db:""` is implied for an embedded struct
		B   int
		C   int `db:"-"`
	}

	type Baz struct {
		A   int
		Bar `db:"Bar"`
	}

	m := NewMapperFunc("db", func(s string) string { return s })

	z := Baz{}
	z.A = 1
	z.B = 2
	z.C = 4
	z.Bar.Foo.A = 3

	zv := reflect.ValueOf(z)
	fields := m.TypeMap(reflect.TypeOf(z))

	if len(fields.Index) != 5 {
		t.Errorf("Expecting 5 fields")
	}

	// for _, fi := range fields.Index {
	// 	log.Println(fi)
	// }

	v := m.FieldByName(zv, "A")
	if ival(v) != z.A {
		t.Errorf("Expecting %d, got %d", z.A, ival(v))
	}
	v = m.FieldByName(zv, "Bar.B")
	if ival(v) != z.Bar.B {
		t.Errorf("Expecting %d, got %d", z.Bar.B, ival(v))
	}
	v = m.FieldByName(zv, "Bar.A")
	if ival(v) != z.Bar.Foo.A {
		t.Errorf("Expecting %d, got %d", z.Bar.Foo.A, ival(v))
	}
	v = m.FieldByName(zv, "Bar.C")
	if _, ok := v.Interface().(int); ok {
		t.Errorf("Expecting Bar.C to not exist")
	}

	fi := fields.GetByPath("Bar.C")
	if fi != nil {
		t.Errorf("Bar.C should not exist")
	}
}

func TestEmbeddedSimple(t *testing.T) {
	type UUID [16]byte
	type MyID struct {
		UUID
	}
	type Item struct {
		ID MyID
	}
	z := Item{}

	m := NewMapper("db")
	m.TypeMap(reflect.TypeOf(z))
}

func TestBasicEmbeddedWithTags(t *testing.T) {
	type Foo struct {
		A int `db:"a"`
	}

	type Bar struct {
		Foo     // `db:""` is implied for an embedded struct
		B   int `db:"b"`
	}

	type Baz struct {
		A   int `db:"a"`
		Bar     // `db:""` is implied for an embedded struct
	}

	m := NewMapper("db")

	z := Baz{}
	z.A = 1
	z.B = 2
	z.Bar.Foo.A = 3

	zv := reflect.ValueOf(z)
	fields := m.TypeMap(reflect.TypeOf(z))

	if len(fields.Index) != 5 {
		t.Errorf("Expecting 5 fields")
	}

	// for _, fi := range fields.index {
	// 	log.Println(fi)
	// }

	v := m.FieldByName(zv, "a")
	if ival(v) != z.Bar.Foo.A { // the dominant field
		t.Errorf("Expecting %d, got %d", z.Bar.Foo.A, ival(v))
	}
	v = m.FieldByName(zv, "b")
	if ival(v) != z.B {
		t.Errorf("Expecting %d, got %d", z.B, ival(v))
	}
}

func TestFlatTags(t *testing.T) {
	m := NewMapper("db")

	type Asset struct {
		Title string `db:"title"`
	}
	type Post struct {
		Author string `db:"author,required"`
		Asset  Asset  `db:""`
	}
	// Post columns: (author title)

	post := Post{Author: "Joe", Asset: Asset{Title: "Hello"}}
	pv := reflect.ValueOf(post)

	v := m.FieldByName(pv, "author")
	if v.Interface().(string) != post.Author {
		t.Errorf("Expecting %s, got %s", post.Author, v.Interface().(string))
	}
	v = m.FieldByName(pv, "title")
	if v.Interface().(string) != post.Asset.Title {
		t.Errorf("Expecting %s, got %s", post.Asset.Title, v.Interface().(string))
	}
}

func TestNestedStruct(t *testing.T) {
	m := NewMapper("db")

	type Details struct {
		Active bool `db:"active"`
	}
	type Asset struct {
		Title   string  `db:"title"`
		Details Details `db:"details"`
	}
	type Post struct {
		Author string `db:"author,required"`
		Asset  `db:"asset"`
	}
	// Post columns: (author asset.title asset.details.active)

	post := Post{
		Author: "Joe",
		Asset:  Asset{Title: "Hello", Details: Details{Active: true}},
	}
	pv := reflect.ValueOf(post)

	v := m.FieldByName(pv, "author")
	if v.Interface().(string) != post.Author {
		t.Errorf("Expecting %s, got %s", post.Author, v.Interface().(string))
	}
	v = m.FieldByName(pv, "title")
	if _, ok := v.Interface().(string); ok {
		t.Errorf("Expecting field to not exist")
	}
	v = m.FieldByName(pv, "asset.title")
	if v.Interface().(string) != post.Asset.Title {
		t.Errorf("Expecting %s, got %s", post.Asset.Title, v.Interface().(string))
	}
	v = m.FieldByName(pv, "asset.details.active")
	if v.Interface().(bool) != post.Asset.Details.Active {
		t.Errorf("Expecting %v, got %v", post.Asset.Details.Active, v.Interface().(bool))
	}
}

func TestInlineStruct(t *testing.T) {
	m := NewMapperTagFunc("db", strings.ToLower, nil)

	type Employee struct {
		Name string
		ID   int
	}
	type Boss Employee
	type person struct {
		Employee `db:"employee"`
		Boss     `db:"boss"`
	}
	// employees columns: (employee.name employee.id boss.name boss.id)

	em := person{Employee: Employee{Name: "Joe", ID: 2}, Boss: Boss{Name: "Dick", ID: 1}}
	ev := reflect.ValueOf(em)

	fields := m.TypeMap(reflect.TypeOf(em))
	if len(fields.Index) != 6 {
		t.Errorf("Expecting 6 fields")
	}

	v := m.FieldByName(ev, "employee.name")
	if v.Interface().(string) != em.Employee.Name {
		t.Errorf("Expecting %s, got %s", em.Employee.Name, v.Interface().(string))
	}
	v = m.FieldByName(ev, "boss.id")
	if ival(v) != em.Boss.ID {
		t.Errorf("Expecting %v, got %v", em.Boss.ID, ival(v))
	}
}

func TestRecursiveStruct(t *testing.T) {
	type Person struct {
		Parent *Person
	}
	m := NewMapperFunc("db", strings.ToLower)
	var p *Person
	m.TypeMap(reflect.TypeOf(p))
}

func TestFieldsEmbedded(t *testing.T) {
	m := NewMapper("db")

	type Person struct {
		Name string `db:"name,size=64"`
	}
	type Place struct {
		Name string `db:"name"`
	}
	type Article struct {
		Title string `db:"title"`
	}
	type PP struct {
		Person  `db:"person,required"`
		Place   `db:",someflag"`
		Article `db:",required"`
	}
	// PP columns: (person.name name title)

	pp := PP{}
	pp.Person.Name = "Peter"
	pp.Place.Name = "Toronto"
	pp.Article.Title = "Best city ever"

	fields := m.TypeMap(reflect.TypeOf(pp))
	// for i, f := range fields {
	// 	log.Println(i, f)
	// }

	ppv := reflect.ValueOf(pp)

	v := m.FieldByName(ppv, "person.name")
	if v.Interface().(string) != pp.Person.Name {
		t.Errorf("Expecting %s, got %s", pp.Person.Name, v.Interface().(string))
	}

	v = m.FieldByName(ppv, "name")
	if v.Interface().(string) != pp.Place.Name {
		t.Errorf("Expecting %s, got %s", pp.Place.Name, v.Interface().(string))
	}

	v = m.FieldByName(ppv, "title")
	if v.Interface().(string) != pp.Article.Title {
		t.Errorf("Expecting %s, got %s", pp.Article.Title, v.Interface().(string))
	}

	fi := fields.GetByPath("person")
	if _, ok := fi.Options["required"]; !ok {
		t.Errorf("Expecting required option to be set")
	}
	if !fi.Embedded {
		t.Errorf("Expecting field to be embedded")
	}
	if len(fi.Index) != 1 || fi.Index[0] != 0 {
		t.Errorf("Expecting index to be [0]")
	}

	fi = fields.GetByPath("person.name")
	if fi == nil {
		t.Errorf("Expecting person.name to exist")
	}
	if fi.Path != "person.name" {
		t.Errorf("Expecting %s, got %s", "person.name", fi.Path)
	}
	if fi.Options["size"] != "64" {
		t.Errorf("Expecting %s, got %s", "64", fi.Options["size"])
	}

	fi = fields.GetByTraversal([]int{1, 0})
	if fi == nil {
		t.Errorf("Expecting traveral to exist")
	}
	if fi.Path != "name" {
		t.Errorf("Expecting %s, got %s", "name", fi.Path)
	}

	fi = fields.GetByTraversal([]int{2})
	if fi == nil {
		t.Errorf("Expecting traversal to exist")
	}
	if _, ok := fi.Options["required"]; !ok {
		t.Errorf("Expecting required option to be set")
	}

	trs := m.TraversalsByName(reflect.TypeOf(pp), []string{"person.name", "name", "title"})
	if !reflect.DeepEqual(trs, [][]int{{0, 0}, {1, 0}, {2, 0}}) {
		t.Errorf("Expecting traversal: %v", trs)
	}
}

func TestPtrFields(t *testing.T) {
	m := NewMapperTagFunc("db", strings.ToLower, nil)
	type Asset struct {
		Title string
	}
	type Post struct {
		*Asset `db:"asset"`
		Author string
	}

	post := &Post{Author: "Joe", Asset: &Asset{Title: "Hiyo"}}
	pv := reflect.ValueOf(post)

	fields := m.TypeMap(reflect.TypeOf(post))
	if len(fields.Index) != 3 {
		t.Errorf("Expecting 3 fields")
	}

	v := m.FieldByName(pv, "asset.title")
	if v.Interface().(string) != post.Asset.Title {
		t.Errorf("Expecting %s, got %s", post.Asset.Title, v.Interface().(string))
	}
	v = m.FieldByName(pv, "author")
	if v.Interface().(string) != post.Author {
		t.Errorf("Expecting %s, got %s", post.Author, v.Interface().(string))
	}
}

func TestNamedPtrFields(t *testing.T) {
	m := NewMapperTagFunc("db", strings.ToLower, nil)

	type User struct {
		Name string
	}

	type Asset struct {
		Title string

		Owner *User `db:"owner"`
	}
	type Post struct {
		Author string

		Asset1 *Asset `db:"asset1"`
		Asset2 *Asset `db:"asset2"`
	}

	post := &Post{Author: "Joe", Asset1: &Asset{Title: "Hiyo", Owner: &User{"Username"}}} // Let Asset2 be nil
	pv := reflect.ValueOf(post)

	fields := m.TypeMap(reflect.TypeOf(post))
	if len(fields.Index) != 9 {
		t.Errorf("Expecting 9 fields")
	}

	v := m.FieldByName(pv, "asset1.title")
	if v.Interface().(string) != post.Asset1.Title {
		t.Errorf("Expecting %s, got %s", post.Asset1.Title, v.Interface().(string))
	}
	v = m.FieldByName(pv, "asset1.owner.name")
	if v.Interface().(string) != post.Asset1.Owner.Name {
		t.Errorf("Expecting %s, got %s", post.Asset1.Owner.Name, v.Interface().(string))
	}
	v = m.FieldByName(pv, "asset2.title")
	if v.Interface().(string) != post.Asset2.Title {
		t.Errorf("Expecting %s, got %s", post.Asset2.Title, v.Interface().(string))
	}
	v = m.FieldByName(pv, "asset2.owner.name")
	if v.Interface().(string) != post.Asset2.Owner.Name {
		t.Errorf("Expecting %s, got %s", post.Asset2.Owner.Name, v.Interface().(string))
	}
	v = m.FieldByName(pv, "author")
	if v.Interface().(string) != post.Author {
		t.Errorf("Expecting %s, got %s", post.Author, v.Interface().(string))
	}
}

func TestFieldMap(t *testing.T) {
	type Foo struct {
		A int
		B int
		C int
	}

	f := Foo{1, 2, 3}
	m := NewMapperFunc("db", strings.ToLower)

	fm := m.FieldMap(reflect.ValueOf(f))

	if len(fm) != 3 {
		t.Errorf("Expecting %d keys, got %d", 3, len(fm))
	}
	if fm["a"].Interface().(int) != 1 {
		t.Errorf("Expecting %d, got %d", 1, ival(fm["a"]))
	}
	if fm["b"].Interface().(int) != 2 {
		t.Errorf("Expecting %d, got %d", 2, ival(fm["b"]))
	}
	if fm["c"].Interface().(int) != 3 {
		t.Errorf("Expecting %d, got %d", 3, ival(fm["c"]))
	}
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
		if fi := mapping.GetByPath(key); fi == nil {
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
		if fi := mapping.GetByPath(key); fi == nil {
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
		if fi := mapping.GetByPath(key); fi == nil {
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
		if fi := mapping.GetByPath(key); fi == nil {
			t.Errorf("Expecting to find key %s in mapping but did not.", key)
		}
	}

	if fi := mapping.GetByPath("isallblack"); fi != nil {
		t.Errorf("Expecting to ignore `IsAllBlack` field")
	}
}

func TestGetByTraversal(t *testing.T) {
	type C struct {
		C0 int
		C1 int
	}
	type B struct {
		B0 string
		B1 *C
	}
	type A struct {
		A0 int
		A1 B
	}

	testCases := []struct {
		Index        []int
		ExpectedName string
		ExpectNil    bool
	}{
		{
			Index:        []int{0},
			ExpectedName: "A0",
		},
		{
			Index:        []int{1, 0},
			ExpectedName: "B0",
		},
		{
			Index:        []int{1, 1, 1},
			ExpectedName: "C1",
		},
		{
			Index:     []int{3, 4, 5},
			ExpectNil: true,
		},
		{
			Index:     []int{},
			ExpectNil: true,
		},
		{
			Index:     nil,
			ExpectNil: true,
		},
	}

	m := NewMapperFunc("db", func(n string) string { return n })
	tm := m.TypeMap(reflect.TypeOf(A{}))

	for i, tc := range testCases {
		fi := tm.GetByTraversal(tc.Index)
		if tc.ExpectNil {
			if fi != nil {
				t.Errorf("%d: expected nil, got %v", i, fi)
			}
			continue
		}

		if fi == nil {
			t.Errorf("%d: expected %s, got nil", i, tc.ExpectedName)
			continue
		}

		if fi.Name != tc.ExpectedName {
			t.Errorf("%d: expected %s, got %s", i, tc.ExpectedName, fi.Name)
		}
	}
}

// TestMapperMethodsByName tests Mapper methods FieldByName and TraversalsByName
func TestMapperMethodsByName(t *testing.T) {
	type C struct {
		C0 string
		C1 int
	}
	type B struct {
		B0 *C     `db:"B0"`
		B1 C      `db:"B1"`
		B2 string `db:"B2"`
	}
	type A struct {
		A0 *B `db:"A0"`
		B  `db:"A1"`
		A2 int
		a3 int
	}

	val := &A{
		A0: &B{
			B0: &C{C0: "0", C1: 1},
			B1: C{C0: "2", C1: 3},
			B2: "4",
		},
		B: B{
			B0: nil,
			B1: C{C0: "5", C1: 6},
			B2: "7",
		},
		A2: 8,
	}

	testCases := []struct {
		Name            string
		ExpectInvalid   bool
		ExpectedValue   interface{}
		ExpectedIndexes []int
	}{
		{
			Name:            "A0.B0.C0",
			ExpectedValue:   "0",
			ExpectedIndexes: []int{0, 0, 0},
		},
		{
			Name:            "A0.B0.C1",
			ExpectedValue:   1,
			ExpectedIndexes: []int{0, 0, 1},
		},
		{
			Name:            "A0.B1.C0",
			ExpectedValue:   "2",
			ExpectedIndexes: []int{0, 1, 0},
		},
		{
			Name:            "A0.B1.C1",
			ExpectedValue:   3,
			ExpectedIndexes: []int{0, 1, 1},
		},
		{
			Name:            "A0.B2",
			ExpectedValue:   "4",
			ExpectedIndexes: []int{0, 2},
		},
		{
			Name:            "A1.B0.C0",
			ExpectedValue:   "",
			ExpectedIndexes: []int{1, 0, 0},
		},
		{
			Name:            "A1.B0.C1",
			ExpectedValue:   0,
			ExpectedIndexes: []int{1, 0, 1},
		},
		{
			Name:            "A1.B1.C0",
			ExpectedValue:   "5",
			ExpectedIndexes: []int{1, 1, 0},
		},
		{
			Name:            "A1.B1.C1",
			ExpectedValue:   6,
			ExpectedIndexes: []int{1, 1, 1},
		},
		{
			Name:            "A1.B2",
			ExpectedValue:   "7",
			ExpectedIndexes: []int{1, 2},
		},
		{
			Name:            "A2",
			ExpectedValue:   8,
			ExpectedIndexes: []int{2},
		},
		{
			Name:            "XYZ",
			ExpectInvalid:   true,
			ExpectedIndexes: []int{},
		},
		{
			Name:            "a3",
			ExpectInvalid:   true,
			ExpectedIndexes: []int{},
		},
	}

	// build the names array from the test cases
	names := make([]string, len(testCases))
	for i, tc := range testCases {
		names[i] = tc.Name
	}
	m := NewMapperFunc("db", func(n string) string { return n })
	v := reflect.ValueOf(val)
	values := m.FieldsByName(v, names)
	if len(values) != len(testCases) {
		t.Errorf("expected %d values, got %d", len(testCases), len(values))
		t.FailNow()
	}
	indexes := m.TraversalsByName(v.Type(), names)
	if len(indexes) != len(testCases) {
		t.Errorf("expected %d traversals, got %d", len(testCases), len(indexes))
		t.FailNow()
	}
	for i, val := range values {
		tc := testCases[i]
		traversal := indexes[i]
		if !reflect.DeepEqual(tc.ExpectedIndexes, traversal) {
			t.Errorf("expected %v, got %v", tc.ExpectedIndexes, traversal)
			t.FailNow()
		}
		val = reflect.Indirect(val)
		if tc.ExpectInvalid {
			if val.IsValid() {
				t.Errorf("%d: expected zero value, got %v", i, val)
			}
			continue
		}
		if !val.IsValid() {
			t.Errorf("%d: expected valid value, got %v", i, val)
			continue
		}
		actualValue := reflect.Indirect(val).Interface()
		if !reflect.DeepEqual(tc.ExpectedValue, actualValue) {
			t.Errorf("%d: expected %v, got %v", i, tc.ExpectedValue, actualValue)
		}
	}
}

func TestFieldByIndexes(t *testing.T) {
	type C struct {
		C0 bool
		C1 string
		C2 int
		C3 map[string]int
	}
	type B struct {
		B1 C
		B2 *C
	}
	type A struct {
		A1 B
		A2 *B
	}
	testCases := []struct {
		value         interface{}
		indexes       []int
		expectedValue interface{}
		readOnly      bool
	}{
		{
			value: A{
				A1: B{B1: C{C0: true}},
			},
			indexes:       []int{0, 0, 0},
			expectedValue: true,
			readOnly:      true,
		},
		{
			value: A{
				A2: &B{B2: &C{C1: "answer"}},
			},
			indexes:       []int{1, 1, 1},
			expectedValue: "answer",
			readOnly:      true,
		},
		{
			value:         &A{},
			indexes:       []int{1, 1, 3},
			expectedValue: map[string]int{},
		},
	}

	for i, tc := range testCases {
		checkResults := func(v reflect.Value) {
			if tc.expectedValue == nil {
				if !v.IsNil() {
					t.Errorf("%d: expected nil, actual %v", i, v.Interface())
				}
			} else {
				if !reflect.DeepEqual(tc.expectedValue, v.Interface()) {
					t.Errorf("%d: expected %v, actual %v", i, tc.expectedValue, v.Interface())
				}
			}
		}

		checkResults(FieldByIndexes(reflect.ValueOf(tc.value), tc.indexes))
		if tc.readOnly {
			checkResults(FieldByIndexesReadOnly(reflect.ValueOf(tc.value), tc.indexes))
		}
	}
}

func TestMustBe(t *testing.T) {
	typ := reflect.TypeOf(E1{})
	mustBe(typ, reflect.Struct)

	defer func() {
		if r := recover(); r != nil {
			valueErr, ok := r.(*reflect.ValueError)
			if !ok {
				t.Errorf("unexpected Method: %s", valueErr.Method)
				t.Error("expected panic with *reflect.ValueError")
				return
			}
			if valueErr.Method != "github.com/jmoiron/sqlx/reflectx.TestMustBe" {
			}
			if valueErr.Kind != reflect.String {
				t.Errorf("unexpected Kind: %s", valueErr.Kind)
			}
		} else {
			t.Error("expected panic")
		}
	}()

	typ = reflect.TypeOf("string")
	mustBe(typ, reflect.Struct)
	t.Error("got here, didn't expect to")
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

func BenchmarkTraversalsByName(b *testing.B) {
	type A struct {
		Value int
	}

	type B struct {
		A A
	}

	type C struct {
		B B
	}

	type D struct {
		C C
	}

	m := NewMapper("")
	t := reflect.TypeOf(D{})
	names := []string{"C", "B", "A", "Value"}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if l := len(m.TraversalsByName(t, names)); l != len(names) {
			b.Errorf("expected %d values, got %d", len(names), l)
		}
	}
}

func BenchmarkTraversalsByNameFunc(b *testing.B) {
	type A struct {
		Z int
	}

	type B struct {
		A A
	}

	type C struct {
		B B
	}

	type D struct {
		C C
	}

	m := NewMapper("")
	t := reflect.TypeOf(D{})
	names := []string{"C", "B", "A", "Z", "Y"}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var l int

		if err := m.TraversalsByNameFunc(t, names, func(_ int, _ []int) error {
			l++
			return nil
		}); err != nil {
			b.Errorf("unexpected error %s", err)
		}

		if l != len(names) {
			b.Errorf("expected %d values, got %d", len(names), l)
		}
	}
}