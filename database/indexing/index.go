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

func NewIndex(field string, options *IndexOptions) *Index {
	return &Index{
		Field:   field,
		Options: options,
		Tree:    btree.NewG(32, lessIndexEntry),
	}
}

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

func (i *Index) CanUseIndex(q *query.Query) bool {
	for _, condition := range q.Conditions {
		if condition.Field == i.Field {
			return true
		}
	}
	return false
}

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

func getFieldValue(data interface{}, field string) (interface{}, error) {
	v := reflect.ValueOf(data)
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil, fmt.Errorf("data is a nil pointer")
		}
		v = v.Elem()
	}

	if v.Kind() == reflect.Map {
		// Handle map type
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
	} else if v.Kind() == reflect.Struct {
		// Handle struct type
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
	} else {
		return nil, fmt.Errorf("unsupported data type: %v", v.Kind())
	}
}
