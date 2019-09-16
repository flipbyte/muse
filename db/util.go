// Copyright © 2018 Earncef Sequeira <earncef@earncef.com>
// This file is part of Konnektify.

package db

import (
	"database/sql/driver"
	"reflect"
	"unicode"
	"unicode/utf8"
)

func structMap(value reflect.Value) map[string]reflect.Value {
	m := make(map[string]reflect.Value)
	structValue(m, value)
	return m
}

var (
	typeValuer = reflect.TypeOf((*driver.Valuer)(nil)).Elem()
)

func structValue(m map[string]reflect.Value, value reflect.Value) {
	if value.Type().Implements(typeValuer) {
		return
	}
	switch value.Kind() {
	case reflect.Ptr:
		if value.IsNil() {
			return
		}
		structValue(m, value.Elem())
	case reflect.Struct:
		t := value.Type()
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			if field.PkgPath != "" && !field.Anonymous {
				// unexported
				continue
			}
			tag := field.Tag.Get("db")
			if tag == "-" {
				// ignore
				continue
			}
			if tag == "" {
				// no tag, but we can record the field name
				tag = GetColumnName(field.Name)
			}
			fieldValue := value.Field(i)
			if _, ok := m[tag]; !ok {
				m[tag] = fieldValue
			}
			structValue(m, fieldValue)
		}
	}
}

func GetColumnName(name string) string {
	if name == "" {
		return ""
	}
	r, n := utf8.DecodeRuneInString(name)
	return string(unicode.ToLower(r)) + name[n:]
}
