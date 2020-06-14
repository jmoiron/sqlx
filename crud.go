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
	QueryRow(query string, args ...interface{}) *sql.Row
	Queryx(query string, args ...interface{}) (*Rows, error)
	ExecOne(query string, args ...interface{}) error
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

// Expand expands the selector into a clause delimited by some space and a list of
// args to append into prepared statements
func Expand(s map[string]interface{}, spacer string) (string, []interface{}) {
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

// Extract takes in a struct object and extracts out the mapping
func Extract(obj StructTable) (map[string]interface{}, error) {
	// Validate the schema.
	if err := obj.Validate(); err != nil {
		return nil, err
	}
	baseType := reflect.TypeOf(obj) // eg. Parameter
	items := map[string]interface{}{}
	for i := 0; i < baseType.NumField(); i++ {
		fieldName := baseType.Field(i).Name // eg. "Torsion"
		possiblyPtr := reflect.ValueOf(obj).FieldByName(fieldName)
		// possiblyPtr could also be a struct or pointer
		if possiblyPtr.Kind() == reflect.Struct {
			subMap, err := Extract(possiblyPtr.Interface().(StructTable))
			if err != nil {
				return nil, err
			}
			for k, v := range subMap {
				items[k] = v
			}
			continue
		}
		if possiblyPtr.IsNil() {
			// pass
		} else {
			// we are not a nil pointer, then indirect would always work.
			fieldValue := reflect.Indirect(possiblyPtr)
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
				if item.IsZero() {
					items[dbName] = "NOW"
				} else {
					items[dbName] = concreteValue
				}
			}
		}
	}
	return items, nil
}

func LookupTag(obj StructTable, field string) string {
	b, ok := reflect.TypeOf(obj).FieldByName(field)
	if !ok {
		return ""
	}
	tagName, _ := parseTag(b.Tag.Get("json"))
	return tagName
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

// MsiToStruct takes in a JSON serializable map[string]interface{} and converts
// it the actual object
func JsonToStruct(input map[string]interface{}, s StructTable) error {
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

// special insertion rules:
// 		if type is time.Time, and the value is a Zero Value, then CURRENT_TIMESTAMP will be inserted
//		if type is a Pointer, and its indirected value is nil, then it is omitted.
func (h *Helper) CreateObject(obj StructTable) error {
	msi, err := Extract(obj)
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

// DeleteAll removes all rows in the table matching condition.
// If no matching row was deleted, then an error is returned.
func (h *Helper) DeleteAll(condition StructTable) error {
	tableName := condition.TableName()
	msi, err := Extract(condition)
	if err != nil {
		return err
	}
	query := "DELETE FROM " + tableName
	query += " WHERE "
	where, args := Expand(msi, " AND ")
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
		return sql.ErrNoRows
	}
	return nil
}

func (h *Helper) buildQuery(condition StructTable, projection []string) (string, []interface{}, error) {
	tableName := condition.TableName()
	query := "SELECT "
	if len(projection) > 0 {
		for idx, p := range projection {
			query += p
			if idx != len(projection)-1 {
				query += ","
			}
		}
	} else {
		query += "*"
	}
	query += " FROM "
	query += tableName
	msi, err := Extract(condition)
	if err != nil {
		return "", nil, err
	}
	args := []interface{}{}
	if len(msi) > 0 {
		query += " WHERE "
		var where string
		where, args = Expand(msi, " AND ")
		query += where
	}
	return query, args, nil
}

// QueryOne returns a scanned object corresponding to the first row matching condition. For
// more complicated tasks such as pagination, etc. It's more sensible to build your own SQL.
// objPtr must be some pointer to a StructTable to receive the deserialized value. Projection
// should be json tags.
func (h *Helper) QueryOne(condition StructTable, objPtr StructTable, projection ...string) error {
	query, args, err := h.buildQuery(condition, projection)
	if err != nil {
		return err
	}
	query += " LIMIT 1"
	query = h.Rebind(query)
	return h.QueryRowx(query, args...).StructScan(objPtr)
}

// QueryRows returns a pointer to a sql.Rows object that can iterated over and scanned. Projection
// should be json tags.
func (h *Helper) QueryRows(condition StructTable, projection ...string) (*Rows, error) {
	query, args, err := h.buildQuery(condition, projection)
	if err != nil {
		return nil, err
	}
	query = h.Rebind(query)
	return h.Queryx(query, args...)
}

// UpdateAll updates rows matching condition with new values given by updates.
// If no matching row was updated, then an error is returned.
func (h *Helper) UpdateAll(update StructTable, condition StructTable) error {
	tableName := update.TableName()
	msi1, err := Extract(update)
	if err != nil {
		return err
	}
	if len(msi1) == 0 {
		// nothing to update, all nil
		return nil
	}
	msi2, err := Extract(condition)
	if err != nil {
		return err
	}
	query := "UPDATE " + tableName + " SET "
	expansion, args := Expand(msi1, ",")
	query += expansion
	// all_args := append(args
	if len(msi2) > 0 {
		query += " WHERE "
		expansion2, args2 := Expand(msi2, " AND ")
		query += expansion2
		args = append(args, args2...)
	}
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
		return errors.New("No row was updated.")
	}
	return nil
}
