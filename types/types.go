package types

import (
	"bytes"
	"compress/gzip"
	"database/sql/driver"
	"encoding/json"
	"errors"

	"io/ioutil"
)

// GzippedText is a []byte which transparently gzips data being submitted to
// a database and ungzips data being Scanned from a database.
type GzippedText []byte

// Value implements the driver.Valuer interface, gzipping the raw value of
// this GzippedText.
func (g GzippedText) Value() (driver.Value, error) {
	b := make([]byte, 0, len(g))
	buf := bytes.NewBuffer(b)
	w := gzip.NewWriter(buf)
	w.Write(g)
	w.Close()
	return buf.Bytes(), nil

}

// Scan implements the sql.Scanner interface, ungzipping the value coming off
// the wire and storing the raw result in the GzippedText.
func (g *GzippedText) Scan(src interface{}) error {
	var source []byte
	switch src.(type) {
	case string:
		source = []byte(src.(string))
	case []byte:
		source = src.([]byte)
	default:
		return errors.New("Incompatible type for GzippedText")
	}
	reader, err := gzip.NewReader(bytes.NewReader(source))
	defer reader.Close()
	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	*g = GzippedText(b)
	return nil
}

// JsonText is a json.RawMessage, which is a []byte underneath.
// Value() validates the json format in the source, and returns an error if
// the json is not valid.  Scan does no validation.  JsonText additionally
// implements `Unmarshal`, which unmarshals the json within to an interface{}
type JsonText json.RawMessage

// MarshalJSON returns the *j as the JSON encoding of j.
func (j *JsonText) MarshalJSON() ([]byte, error) {
	return *j, nil
}

// UnmarshalJSON sets *j to a copy of data
func (j *JsonText) UnmarshalJSON(data []byte) error {
	if j == nil {
		return errors.New("JsonText: UnmarshalJSON on nil pointer")
	}
	*j = append((*j)[0:0], data...)
	return nil

}

// Value returns j as a value.  This does a validating unmarshal into another
// RawMessage.  If j is invalid json, it returns an error.
func (j JsonText) Value() (driver.Value, error) {
	var m json.RawMessage
	var err = j.Unmarshal(&m)
	if err != nil {
		return []byte{}, err
	}
	return []byte(j), nil
}

// Scan stores the src in *j.  No validation is done.
func (j *JsonText) Scan(src interface{}) error {
	var source []byte
	switch src.(type) {
	case string:
		source = []byte(src.(string))
	case []byte:
		source = src.([]byte)
	default:
		return errors.New("Incompatible type for JsonText")
	}
	*j = JsonText(append((*j)[0:0], source...))
	return nil
}

// Unmarshal unmarshal's the json in j to v, as in json.Unmarshal.
func (j *JsonText) Unmarshal(v interface{}) error {
	return json.Unmarshal([]byte(*j), v)
}

// Pretty printing for JsonText types
func (j JsonText) String() string {
	return string(j)
}
