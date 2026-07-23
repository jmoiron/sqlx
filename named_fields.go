package sqlx

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx/reflectx"
)

// NamedFields returns the ordered column names implied by arg for use with
// named queries. arg may be a struct (or pointer to struct) using `db` tags
// (same rules as NamedExec / StructScan), or a map[string]T.
//
// Embedded structs are flattened; fields tagged `db:"-"` are skipped.
// Map keys are returned in an unspecified order.
//
// See NamedInsertFields and NamedUpdateFields for ready-made SQL fragments.
func NamedFields(arg interface{}) ([]string, error) {
	return namedFields(arg, mapper())
}

func namedFields(arg interface{}, m *reflectx.Mapper) ([]string, error) {
	if arg == nil {
		return nil, errors.New("NamedFields: nil argument")
	}
	v := reflect.ValueOf(arg)
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil, errors.New("NamedFields: nil pointer")
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Map:
		if v.Type().Key().Kind() != reflect.String {
			return nil, fmt.Errorf("NamedFields: map key type must be string, got %s", v.Type().Key())
		}
		keys := v.MapKeys()
		names := make([]string, 0, len(keys))
		for _, k := range keys {
			names = append(names, k.String())
		}
		return names, nil
	case reflect.Struct:
		tm := m.TypeMap(v.Type())
		names := make([]string, 0, len(tm.Index))
		seen := make(map[string]struct{}, len(tm.Index))
		for _, fi := range tm.Index {
			if fi == nil || fi.Embedded {
				continue
			}
			if fi.Name == "" {
				continue
			}
			// Traversal-only nodes / anonymous parents appear in Index;
			// Names map holds the leaf bind names we care about.
			if _, ok := tm.Names[fi.Name]; !ok {
				continue
			}
			if _, ok := seen[fi.Name]; ok {
				continue
			}
			seen[fi.Name] = struct{}{}
			names = append(names, fi.Name)
		}
		// Prefer stable order from Names iteration via Index order above.
		// If Index yielded nothing (unusual), fall back to Names.
		if len(names) == 0 {
			for name := range tm.Names {
				names = append(names, name)
			}
		}
		if len(names) == 0 {
			return nil, fmt.Errorf("NamedFields: struct %s has no exported db fields", v.Type())
		}
		return names, nil
	default:
		return nil, fmt.Errorf("NamedFields: expected struct or map, got %s", v.Kind())
	}
}

// NamedInsertFields returns the column list and named placeholder list for an
// INSERT statement driven by arg's fields, e.g.
//
//	cols, vals, err := sqlx.NamedInsertFields(client)
//	// cols == "id, client_name, client_secret"
//	// vals == ":id, :client_name, :client_secret"
//	db.NamedExec("INSERT INTO client ("+cols+") VALUES ("+vals+")", client)
//
// Addresses the boilerplate described in issue #410.
func NamedInsertFields(arg interface{}) (columns, placeholders string, err error) {
	names, err := NamedFields(arg)
	if err != nil {
		return "", "", err
	}
	cols := strings.Join(names, ", ")
	ph := make([]string, len(names))
	for i, n := range names {
		ph[i] = ":" + n
	}
	return cols, strings.Join(ph, ", "), nil
}

// NamedUpdateFields returns a SET clause of the form "col=:col, ..." for
// UPDATE statements driven by arg's fields.
//
//	set, err := sqlx.NamedUpdateFields(client)
//	db.NamedExec("UPDATE client SET "+set+" WHERE id=:id", client)
func NamedUpdateFields(arg interface{}) (string, error) {
	names, err := NamedFields(arg)
	if err != nil {
		return "", err
	}
	parts := make([]string, len(names))
	for i, n := range names {
		parts[i] = n + "=:" + n
	}
	return strings.Join(parts, ", "), nil
}
