package sqlx

import (
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"time"
)

type DBI interface {
	NamedExec(query string, arg interface{}) (sql.Result, error)
	NamedQueryRow(query string, arg interface{}) *Row
	NamedQuery(query string, arg interface{}) (*Rows, error)
	QueryRowx(query string, args ...interface{}) *Row
	Queryx(query string, args ...interface{}) (*Rows, error)
	Rebind(query string) string
	Exec(query string, args ...interface{}) (sql.Result, error)
}

// Return the name of a Struct to tablename
func DefaultTableName(i interface{}) string {
	return strings.ToLower(reflect.TypeOf(reflect.Indirect(reflect.ValueOf(i)).Interface()).Name())
}

type Helper struct {
	DBI
}

type StructTable interface {
	TableName() string
	Validate() error
}

type SafeSelector map[string]interface{}

// expand expands the selector into a clause delimited by some space and a list of
// args to append into prepared statements
func expand(s map[string]interface{}, spacer string) (string, []interface{}) {
	args := []interface{}{}
	cnt := 0
	query := ""
	for key, value := range s {
		query += key
		query += "=?"
		if cnt != len(s)-1 {
			query += spacer
		}
		args = append(args, value)
		cnt += 1
	}
	return query, args
}

// extract takes in a struct object and extracts out the mapping
func extract(obj StructTable) (map[string]interface{}, error) {
	// Validate the schema.
	if err := obj.Validate(); err != nil {
		return nil, err
	}

	base := reflect.Indirect(reflect.ValueOf(obj)) // a parameter itself
	baseType := reflect.TypeOf(base.Interface())   // eg. Parameter
	items := map[string]interface{}{}
	for i := 0; i < baseType.NumField(); i++ {
		fieldName := baseType.Field(i).Name // eg. "Torsion"
		possiblePtrFieldValue := base.FieldByName(fieldName)

		if possiblePtrFieldValue.Kind() == reflect.Ptr && possiblePtrFieldValue.IsNil() {
			// pass
		} else {
			// we are not a nil pointer, then indirect would always work.
			fieldValue := reflect.Indirect(possiblePtrFieldValue)
			concreteValue := fieldValue.Interface()
			dbName, _ := parseTag(baseType.Field(i).Tag.Get("json"))
			// if tagOptions.Contains("nonzero") && isZeroValue(fieldValue) {
			// 	return nil, errors.New("Zero value found for tagged nonzero field:" + fieldName)
			// }
			switch item := concreteValue.(type) {
			default:
				items[dbName] = concreteValue
				// dbVals = append(dbVals, ":"+dbName)
			case time.Time:
				if isZeroValue(reflect.ValueOf(item)) {
					items[dbName] = "NOW"
				} else {
					items[dbName] = concreteValue
				}
			}
		}
	}
	return items, nil
}

/* START ripped from unexported std lib END */
type tagOptions string

func parseTag(tag string) (string, tagOptions) {
	if idx := strings.Index(tag, ","); idx != -1 {
		return tag[:idx], tagOptions(tag[idx+1:])
	}
	return tag, tagOptions("")
}

func (o tagOptions) Contains(optionName string) bool {
	if len(o) == 0 {
		return false
	}
	s := string(o)
	for s != "" {
		var next string
		i := strings.Index(s, ",")
		if i >= 0 {
			s, next = s[:i], s[i+1:]
		}
		if s == optionName {
			return true
		}
		s = next
	}
	return false
}

/* END ripped from unexported std lib END */

func MapJsonToStruct(input map[string]interface{}, s StructTable) error {
	// YT: LOL
	b, err := json.Marshal(input)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, s)
}

func MakeStructTable(input map[string]interface{}, obj StructTable) error {
	base := reflect.Indirect(reflect.ValueOf(obj))
	baseType := reflect.TypeOf(base.Interface())
	for k, v := range input {
		_, ok := baseType.FieldByName(k)
		if !ok {
			return errors.New("Bad input name: " + k)
		}
		fv := base.FieldByName(k)
		ptr := reflect.New(reflect.TypeOf(v))
		reflect.Indirect(ptr).Set(reflect.ValueOf(v))
		fv.Set(ptr)
	}
	return nil
}

// MakeProjector creates a projector that is used to filter the columns returned, like its
// cousin MakeSelector, it validates against the given object.
func MakeProjector(input []string, obj interface{}) ([]string, error) {
	base := reflect.TypeOf(reflect.Indirect(reflect.ValueOf(obj)).Interface())
	cols := []string{}
	for _, fname := range input {
		b, ok := base.FieldByName(fname)
		if !ok {
			return nil, errors.New("Unknown Field Name")
		}
		new_key, _ := parseTag(b.Tag.Get("db"))
		cols = append(cols, new_key)
	}
	return cols, nil
}

// Checks to see if x is the Zero Value
// This is not fully implemented for Arrays, or Structs, or other weird stuff
func isZeroValue(v reflect.Value) bool {
	// v := reflect.ValueOf(x)
	switch v.Kind() {
	case reflect.Map, reflect.Slice:
		return v.IsNil()
	}
	return v.Interface() == reflect.Zero(v.Type()).Interface()
}

// special insertion rules:
// 		if type is time.Time, and the value is a Zero Value, then CURRENT_TIMESTAMP will be inserted
//		if type is a Pointer, and its indirected value is nil, then it is omitted.
func (h *Helper) CreateObject(obj StructTable) error {
	msi, err := extract(obj)
	if err != nil {
		return err
	}
	dbKeys := []string{}
	dbVals := []interface{}{}
	for k, v := range msi {
		dbKeys = append(dbKeys, k)
		dbVals = append(dbVals, v)
	}
	query := "INSERT INTO " + obj.TableName()
	query += " ("
	for idx, key := range dbKeys {
		query += key
		if idx != len(dbKeys)-1 {
			query += ","
		}
	}
	query += ") VALUES ("
	for idx, _ := range dbKeys {
		query += "?"
		if idx != len(dbKeys)-1 {
			query += ","
		}
	}
	query += ")"
	query = h.Rebind(query)
	_, err = h.Exec(query, dbVals...)
	return err
}

// type SafeProjector []string

func (h *Helper) MustDelete(obj StructTable) error {
	tableName := obj.TableName()
	msi, err := extract(obj)
	if err != nil {
		return err
	}
	query := "DELETE FROM " + tableName
	query += " WHERE "
	where, args := expand(msi, " AND ")
	query += where
	query = h.Rebind(query)
	res, err := h.Exec(query, args...)
	if err != nil {
		return err
	}
	cnt, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if cnt == 0 {
		return errors.New("No row was deleted.")
	}
	return nil
}

// A query bounded to a given object
func (h *Helper) QueryOne(cond StructTable, newObj StructTable, projection ...string) error {
	tableName := cond.TableName()
	projs, err := MakeProjector(projection, cond)
	if err != nil {
		return err
	}
	query := "SELECT "
	if len(projs) > 0 {
		for idx, p := range projs {
			query += p
			if idx != len(projs)-1 {
				query += ","
			}
		}
	} else {
		query += "*"
	}
	query += " FROM "
	query += tableName
	query += " WHERE "
	msi, err := extract(cond)
	if err != nil {
		return err
	}
	where, args := expand(msi, " AND ")
	query += where
	query += " LIMIT 1"
	query = h.Rebind(query)
	return h.QueryRowx(query, args...).StructScan(newObj)
}

// Update a subset of the columns of a table using a struct. This method returns an error
// if no rows were affected. Note that updating the same row twice is not classified as an
// error.
func (h *Helper) UpdateTable(updates StructTable, conds StructTable) error {
	tableName := updates.TableName()

	msi1, err := extract(updates)
	if err != nil {
		return err
	}
	msi2, err := extract(conds)
	if err != nil {
		return err
	}
	query := "UPDATE " + tableName + " SET "
	expansion, args := expand(msi1, ",")
	query += expansion
	query += " WHERE "
	expansion2, args2 := expand(msi2, " AND ")
	query += expansion2
	all_args := append(args, args2...)
	query = h.Rebind(query)
	res, err := h.Exec(query, all_args...)
	if err != nil {
		return err
	}
	cnt, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if cnt == 0 {
		return errors.New("No row was updated.")
	}
	return nil
}
