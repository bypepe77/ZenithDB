package indexing

import (
	"fmt"
	"reflect"

	"github.com/bypepe77/ZenithDB/database/document"
	"github.com/bypepe77/ZenithDB/database/query"

	"github.com/google/btree"
)

type Index struct {
	Name      string
	Field     string
	IndexType string
	Tree      *btree.BTree
}

func NewIndex(name, field, indexType string) *Index {
	return &Index{
		Name:      name,
		Field:     field,
		IndexType: indexType,
		Tree:      btree.New(128),
	}
}

func (i *Index) Insert(doc *document.Document) error {
	fieldValue, err := getFieldValue(doc.Data, i.Field)
	if err != nil {
		return err
	}

	entry := &indexEntry{
		Value: fmt.Sprintf("%v", fieldValue),
		DocID: doc.ID,
	}

	i.Tree.ReplaceOrInsert(entry)

	return nil
}

func (i *Index) Update(oldDoc, newDoc *document.Document) error {
	if err := i.Delete(oldDoc); err != nil {
		return err
	}

	if err := i.Insert(newDoc); err != nil {
		return err
	}

	return nil
}

func (i *Index) Delete(doc *document.Document) error {
	fieldValue, err := getFieldValue(doc.Data, i.Field)
	if err != nil {
		return err
	}

	entry := &indexEntry{
		Value: fmt.Sprintf("%v", fieldValue),
		DocID: doc.ID,
	}

	i.Tree.Delete(entry)

	return nil
}

func (i *Index) Find(q *query.Query) ([]string, error) {
	var ids []string

	for _, condition := range q.Conditions {
		if condition.Field == i.Field {
			valueStr := fmt.Sprintf("%v", condition.Value)

			i.Tree.AscendGreaterOrEqual(&indexEntry{Value: valueStr}, func(item btree.Item) bool {
				entry := item.(*indexEntry)
				if entry.Value.(string) > valueStr {
					return false
				}
				ids = append(ids, entry.DocID)
				return true
			})

			break
		}
	}

	return ids, nil
}

func (i *Index) CanUseIndex(q *query.Query) bool {
	for _, condition := range q.Conditions {
		if condition.Field == i.Field {
			return true
		}
	}
	return false
}

func getFieldValue(data interface{}, field string) (interface{}, error) {
	v := reflect.ValueOf(data)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("data must be a struct or pointer to struct")
	}

	fieldValue := v.FieldByName(field)
	if !fieldValue.IsValid() {
		return nil, fmt.Errorf("field '%s' not found", field)
	}

	return fieldValue.Interface(), nil
}

type indexEntry struct {
	Value interface{}
	DocID string
}

func (e *indexEntry) Less(than btree.Item) bool {
	return e.Value.(string) < than.(*indexEntry).Value.(string)
}
