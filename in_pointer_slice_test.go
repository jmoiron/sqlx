package sqlx

import (
	"database/sql/driver"
	"reflect"
	"testing"
)

func TestInPointerSlices(t *testing.T) {
	type numbers []int
	cases := []struct {
		name string
		arg  interface{}
		want []interface{}
	}{
		{"int", &[]int{1, 2}, []interface{}{1, 2}},
		{"string", &[]string{"a", "b"}, []interface{}{"a", "b"}},
		{"interface", &[]interface{}{1, "b"}, []interface{}{1, "b"}},
		{"generic", &[]int64{1, 2}, []interface{}{int64(1), int64(2)}},
		{"named", &numbers{1, 2}, []interface{}{1, 2}},
		{"control slice", []int{1, 2}, []interface{}{1, 2}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("In panicked:%v", r)
				}
			}()
			q, args, err := In("SELECT ? WHERE id IN (?)", 99, tt.arg)
			if err != nil {
				t.Fatal(err)
			}
			if q != "SELECT ? WHERE id IN (?, ?)" {
				t.Errorf("query=%q", q)
			}
			want := append([]interface{}{99}, tt.want...)
			if !reflect.DeepEqual(args, want) {
				t.Errorf("args=%#v,want%#v", args, want)
			}
		})
	}
}

func TestInPointerSliceEdges(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		defer reportInPanic(t)
		for _, arg := range []interface{}{&[]int{}, new([]int)} {
			_, _, err := In("SELECT ?", arg)
			if err == nil {
				t.Error("empty slice should return error")
			}
		}
	})
	t.Run("nil pointer", func(t *testing.T) {
		defer reportInPanic(t)
		var arg *[]int
		q, args, err := In("SELECT ? WHERE id IN (?)", arg, []int{1, 2})
		if err != nil {
			t.Fatal(err)
		}
		if q != "SELECT ? WHERE id IN (?, ?)" {
			t.Errorf("query=%q", q)
		}
		if len(args) != 3 || args[0] != arg || args[1] != 1 || args[2] != 2 {
			t.Errorf("args=%#v", args)
		}
		converted, err := driver.DefaultParameterConverter.ConvertValue(args[0])
		if err != nil || converted != nil {
			t.Errorf("nil pointer conversion=%#v,error=%v", converted, err)
		}
	})
	t.Run("byte pointer", func(t *testing.T) {
		defer reportInPanic(t)
		bytes := []byte{1, 2}
		_, args, err := In("SELECT ? WHERE id IN (?)", &bytes, []int{1, 2})
		if err != nil {
			t.Fatal(err)
		}
		if len(args) != 3 || args[0] != &bytes {
			t.Errorf("byte pointer expanded:%#v", args)
		}
	})
	t.Run("single argument", func(t *testing.T) {
		defer reportInPanic(t)
		values := []int{1, 2}
		q, args, err := In("SELECT ?", &values)
		if err != nil {
			t.Fatal(err)
		}
		if q != "SELECT ?, ?" || !reflect.DeepEqual(args, []interface{}{1, 2}) {
			t.Errorf("query=%q,args=%#v", q, args)
		}
	})
}

func reportInPanic(t *testing.T) {
	t.Helper()
	if r := recover(); r != nil {
		t.Errorf("In panicked: %v", r)
	}
}

func TestInPointerSlicesSQLite(t *testing.T) {
	if !TestSqlite {
		t.Skip("SQLite is disabled")
	}
	db, err := Connect("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	values := []int{1, 2}
	query, args, err := In("SELECT ? IN (?)", 2, &values)
	if err != nil {
		t.Fatal(err)
	}
	var matched int
	if err := db.Get(&matched, query, args...); err != nil {
		t.Fatal(err)
	}
	if matched != 1 {
		t.Errorf("matched=%d,want1", matched)
	}
}
