package indexing

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/bypepe77/ZenithDB/database/document"
	"github.com/bypepe77/ZenithDB/database/query"

	"github.com/google/btree"
)

type Index struct {
	Field   string
	Options *IndexOptions
	Tree    *btree.BTreeG[*indexEntry]
	mu      sync.RWMutex
}

type indexEntry struct {
	Value interface{}
	DocID string
}

type IndexOptions struct {
	Unique bool `json:"unique"`
}

// NewIndex creates a new Index instance.
func NewIndex(field string, options *IndexOptions) *Index {
	return &Index{
		Field:   field,
		Options: options,
		Tree:    btree.NewG(32, lessIndexEntry),
	}
}

// lessIndexEntry defines the comparison function for index entries.
func lessIndexEntry(a, b *indexEntry) bool {
	switch aValue := a.Value.(type) {
	case int:
		return aValue < b.Value.(int)
	case float64:
		return aValue < b.Value.(float64)
	case string:
		return aValue < b.Value.(string)
	default:
		return fmt.Sprintf("%v", a.Value) < fmt.Sprintf("%v", b.Value)
	}
}

// CanUseIndex checks if the index can be used for the given query.
func (i *Index) CanUseIndex(q *query.Query) bool {
	for _, condition := range q.Conditions {
		if condition.Field == i.Field {
			return true
		}
	}
	return false
}

// Insert inserts a new document into the index.
func (i *Index) Insert(doc *document.Document) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	value, err := getFieldValue(doc.Data, i.Field)
	if err != nil {
		fmt.Println("Error getting field value:", err)
		return err
	}

	entry := &indexEntry{
		Value: value,
		DocID: doc.ID,
	}

	if i.Options.Unique && i.Tree.Has(entry) {
		return fmt.Errorf("duplicate key value violates unique constraint")
	}

	i.Tree.ReplaceOrInsert(entry)
	return nil
}

// Delete removes a document from the index.
func (i *Index) Delete(doc *document.Document) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	value, err := getFieldValue(doc.Data, i.Field)
	if err != nil {
		return err
	}

	entry := &indexEntry{
		Value: value,
		DocID: doc.ID,
	}

	i.Tree.Delete(entry)
	return nil
}

// Find retrieves document IDs that match the query conditions.
func (i *Index) Find(q *query.Query) ([]string, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	var docIDs []string

	for _, condition := range q.Conditions {
		if condition.Field == i.Field {
			value := condition.Value

			i.Tree.AscendGreaterOrEqual(&indexEntry{Value: value}, func(item *indexEntry) bool {
				if item.Value != value {
					return false
				}
				docIDs = append(docIDs, item.DocID)
				return true
			})

			break
		}
	}

	return docIDs, nil
}

// getFieldValue retrieves the value of a field from the document data.
func getFieldValue(data interface{}, field string) (interface{}, error) {
	v := reflect.ValueOf(data)
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil, fmt.Errorf("data is a nil pointer")
		}
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Map:
		return getFieldValueFromMap(v, field)
	case reflect.Struct:
		return getFieldValueFromStruct(v, field)
	default:
		return nil, fmt.Errorf("unsupported data type: %v", v.Kind())
	}
}

// getFieldValueFromMap retrieves the value of a field from a map.
func getFieldValueFromMap(v reflect.Value, field string) (interface{}, error) {
	mapValue := v.Interface().(map[string]interface{})
	value, exists := mapValue[field]
	if !exists {
		// Try converting the field to lowercase
		field = strings.ToLower(field)
		value, exists = mapValue[field]
		if !exists {
			return nil, fmt.Errorf("field '%s' not found in map", field)
		}
	}
	return value, nil
}

// getFieldValueFromStruct retrieves the value of a field from a struct.
func getFieldValueFromStruct(v reflect.Value, field string) (interface{}, error) {
	fieldValue := v.FieldByName(field)
	if !fieldValue.IsValid() {
		// Try converting the field to lowercase
		field = strings.ToLower(field)
		fieldValue = v.FieldByName(field)
		if !fieldValue.IsValid() {
			return nil, fmt.Errorf("field '%s' not found in struct", field)
		}
	}
	return fieldValue.Interface(), nil
}
