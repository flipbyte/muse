// Copyright © 2018 Earncef Sequeira <earncef@earncef.com>
// This file is part of Konnektify.

package db

import (
	"fmt"

	"github.com/gocraft/dbr"
)

type Table struct {
	Name      string   `json:"-"`
	PkColumns []string `json:"-"`
	Instance  TableInterface
}

type TableInterface interface {
	Initialize() TableInterface
	Select(column ...string) *dbr.SelectBuilder
	Insert() *dbr.InsertBuilder
	Update() *dbr.UpdateBuilder
	Delete() *dbr.DeleteBuilder
	Load(value ...interface{}) (RowInterface, error)
	GetName() string
	GetRow() RowInterface
	GetRowCollection() interface{}
}

func (t *Table) Initialize() TableInterface {
	return t
}

func (t *Table) Select(column ...string) *dbr.SelectBuilder {
	if len(column) == 0 {
		return GetSession().Select("*").From(t.Name)
	}
	return GetSession().Select(column...).From(t.Name)
}

func (t *Table) Insert() *dbr.InsertBuilder {
	return GetSession().InsertInto(t.Name)
}

func (t *Table) Update() *dbr.UpdateBuilder {
	return GetSession().Update(t.Name)
}

func (t *Table) Delete() *dbr.DeleteBuilder {
	return GetSession().DeleteFrom(t.Name)
}

func (t *Table) Load(value ...interface{}) (RowInterface, error) {
	row := t.Instance.GetRow()
	row.Initialize()
	if row == nil {
		return nil, fmt.Errorf("No row prototype set for this table")
	}

	if len(value) != len(t.PkColumns) {
		return nil, fmt.Errorf("Arguments do not match the number of primary key columns")
	}

	s := t.Select()
	for i, column := range t.PkColumns {
		s.Where(column+" = ?", value[i])
	}

	err := s.LoadOne(row)
	if err != nil {
		return nil, err
	}

	return row, nil
}

func (t *Table) GetName() string {
	return t.Name
}

func (t *Table) GetRow() RowInterface {
	row := &Row{}
	row.Initialize()
	return row
}

func (t *Table) GetRowCollection() interface{} {
	rows := []Row{}
	return &rows
}
