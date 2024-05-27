package indexing

import (
	"reflect"

	"github.com/bypepe77/ZenithDB/database/document"
	"github.com/bypepe77/ZenithDB/database/query"
)

type IndexManager struct {
	indexes map[string]*Index
}

func NewIndexManager() *IndexManager {
	return &IndexManager{
		indexes: make(map[string]*Index),
	}
}

func (im *IndexManager) CreateIndex(name, field, indexType string) error {
	if _, exists := im.indexes[field]; exists {
		return nil
	}

	index := NewIndex(name, field, indexType)
	im.indexes[field] = index

	return nil
}

func (im *IndexManager) GetIndex(field string) (*Index, bool) {
	index, exists := im.indexes[field]
	return index, exists
}

func (im *IndexManager) GetIndexes() map[string]*Index {
	return im.indexes
}

func (im *IndexManager) BuildIndexes(doc *document.Document) error {
	for _, field := range getIndexFields(doc.Data) {
		if err := im.CreateIndex(field+"_index", field, "btree"); err != nil {
			return err
		}

		index := im.indexes[field]
		if err := index.Insert(doc); err != nil {
			return err
		}
	}

	return nil
}

func (im *IndexManager) UpdateIndexes(oldDoc, newDoc *document.Document) error {
	for _, field := range getIndexFields(newDoc.Data) {
		index, exists := im.indexes[field]
		if !exists {
			continue
		}

		if err := index.Update(oldDoc, newDoc); err != nil {
			return err
		}
	}

	return nil
}

func (im *IndexManager) DeleteIndexes(doc *document.Document) error {
	for _, field := range getIndexFields(doc.Data) {
		index, exists := im.indexes[field]
		if !exists {
			continue
		}

		if err := index.Delete(doc); err != nil {
			return err
		}
	}

	return nil
}

func (im *IndexManager) FindIndexForQuery(q *query.Query) *Index {
	for _, index := range im.indexes {
		if index.CanUseIndex(q) {
			return index
		}
	}
	return nil
}

func getIndexFields(data interface{}) []string {
	var indexFields []string
	v := reflect.ValueOf(data)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return indexFields
	}
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)
		if value := field.Tag.Get("index"); value == "true" {
			indexFields = append(indexFields, field.Name)
		}
	}
	return indexFields
}
