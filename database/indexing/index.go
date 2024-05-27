package indexing

import (
	"fmt"
	"strings"

	"github.com/bypepe77/ZenithDB/database/document"
	"github.com/bypepe77/ZenithDB/database/query"
	"github.com/google/btree"
)

type Index struct {
	Field   string
	Options *IndexOptions
	Tree    *btree.BTreeG[*indexEntry]
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
	return fmt.Sprintf("%v", a.Value) < fmt.Sprintf("%v", b.Value)
}

func (i *Index) Insert(doc *document.Document) error {
	value, err := getFieldValue(doc.Data, i.Field)
	if err != nil {
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
	value := getFieldValueByPath(data, field)
	if value == nil {
		return nil, fmt.Errorf("field '%s' not found", field)
	}
	return value, nil
}

func getFieldValueByPath(data interface{}, path string) interface{} {
	fields := strings.Split(path, ".")
	value := data

	for _, field := range fields {
		if m, ok := value.(map[string]interface{}); ok {
			value, ok = m[field]
			if !ok {
				return nil
			}
		} else {
			return nil
		}
	}

	return value
}
