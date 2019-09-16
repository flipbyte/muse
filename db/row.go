// Copyright © 2018 Earncef Sequeira <earncef@earncef.com>
// This file is part of Konnektify.

package db

import (
	"fmt"
	"reflect"
)

type Row struct {
	Table    *Table       `json:"-"`
	Columns  []string     `json:"-"`
	Instance RowInterface `json:"-"`
	isValid  bool         `json:"-"`
}

type RowInterface interface {
	GetColumns() []string
	Initialize() RowInterface
	IsValid() bool
	BeforeSave() error
	Save() error
	Delete() error
	ContainsPrimaryKeys() bool
}

func (r *Row) Initialize() RowInterface {
	return r
}

func (r *Row) GetColumns() []string {
	if len(r.Columns) == 0 {
		v := reflect.TypeOf(r.Instance).Elem()
		r.Columns = []string{}
		for i := 0; i < v.NumField(); i++ {
			column := v.Field(i).Name
			if column == "Row" {
				continue
			}

			r.Columns = append(r.Columns, GetColumnName(column))
		}
	}

	return r.Columns
}

func (r *Row) IsValid() bool {
	if r.Instance == nil {
		return false
	}

	if r.isValid {
		return true
	}

	r.Instance.Initialize()
	if len(r.Table.PkColumns) > 0 && r.Table.Name != "" {
		r.isValid = true
	}

	return r.isValid
}

func (r *Row) BeforeSave() error {
	return nil
}

func (r *Row) ContainsPrimaryKeys() bool {
	v := reflect.Indirect(reflect.ValueOf(r.Instance))
	m := structMap(v)

	for _, col := range r.Table.PkColumns {
		if val, ok := m[col]; !ok || val.Interface() == reflect.Zero(val.Type()).Interface() {
			return false
		}
	}

	return true
}

func (r *Row) Save() error {
	if !r.IsValid() {
		return fmt.Errorf("Invalid row")
	}

	err := r.Instance.BeforeSave()
	if err != nil {
		return err
	}

	isUpdate := r.Instance.ContainsPrimaryKeys()
	if !isUpdate {
		_, err := r.Table.Insert().Columns(r.Instance.GetColumns()...).Record(r.Instance).Exec()
		return err
	}

	stmt := r.Table.Update()

	v := reflect.Indirect(reflect.ValueOf(r.Instance))
	m := structMap(v)
	for _, col := range r.Instance.GetColumns() {
		if val, ok := m[col]; ok {
			stmt.Set(col, val.Interface())
		}
	}

	for _, col := range r.Table.PkColumns {
		if val, ok := m[col]; ok {
			stmt.Where(col+" = ?", val.Interface())
		}
	}

	_, err = stmt.Exec()
	return err
}

func (r *Row) Delete() error {
	if !r.IsValid() || !r.Instance.ContainsPrimaryKeys() {
		return fmt.Errorf("Invalid row")
	}

	stmt := r.Table.Delete()
	v := reflect.Indirect(reflect.ValueOf(r.Instance))
	m := structMap(v)
	for _, col := range r.Table.PkColumns {
		if val, ok := m[col]; ok {
			stmt.Where(col+" = ?", val.Interface())
		}
	}
	_, err := stmt.Exec()
	return err
}
