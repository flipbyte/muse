// Copyright © 2018 Earncef Sequeira <earncef@earncef.com>
// This file is part of Konnektify.

package db

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"unicode"
	"unicode/utf8"

	"github.com/flipbyte/golib"
	"github.com/gocraft/dbr"
	"github.com/stretchr/objx"
)

func Initialize(dbType, dsn string) {
	dbr.NameMapping = ColumnNameConversion
	var conn, err = dbr.Open(dbType, dsn, nil)
	if err != nil {
		panic(fmt.Sprintf("Fatal error in connecting to database: %s \n", err))
	}

	library.Repository().Set("db", conn)
}

func GetSession() *dbr.Session {
	var connection, ok = library.Repository().Get("db").(*dbr.Connection)
	if !ok {
		panic("Could not get db connection.")
	}

	return connection.NewSession(nil)
}

type DbrObjxMap struct {
	Data  objx.Map
	Valid bool
}

func (o DbrObjxMap) Value() (driver.Value, error) {
	return o.Data.JSON()
}

// MarshalJSON correctly serializes a NullTime to JSON
func (o DbrObjxMap) MarshalJSON() ([]byte, error) {
	json, err := o.Data.JSON()
	if err != nil {
		return nil, err
	}
	return []byte(json), nil
}

// UnmarshalJSON correctly deserializes an DbrObjxMap from JSON
func (o *DbrObjxMap) UnmarshalJSON(b []byte) error {
	var s interface{}
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	return o.Scan(s)
}

func NewObjxMap(v interface{}) (o DbrObjxMap) {
	o.Scan(v)
	return
}

// Scan implements the Scanner interface.
// The value type must be time.Time or string / []byte (formatted time-string),
// otherwise Scan fails.
func (o *DbrObjxMap) Scan(value interface{}) error {
	var err error

	if value == nil {
		o.Data, o.Valid = objx.Map{}, false
		return nil
	}

	switch v := value.(type) {
	case []byte:
		o.Data, err = objx.FromJSON(string(v))
		o.Valid = (err == nil)
		return err
	case string:
		o.Data, err = objx.FromJSON(v)
		o.Valid = (err == nil)
		return err
	case objx.Map:
		o.Data = value.(objx.Map)
		o.Valid = true
	case map[string]interface{}:
		o.Data = objx.Map(value.(map[string]interface{}))
		o.Valid = true
	}

	o.Valid = false
	return nil
}

func ColumnNameConversion(name string) string {
	if name == "" {
		return ""
	}
	r, n := utf8.DecodeRuneInString(name)
	return string(unicode.ToLower(r)) + name[n:]
}
