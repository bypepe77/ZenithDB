package storage

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/bypepe77/ZenithDB/database/document"
	"github.com/bypepe77/ZenithDB/database/indexing"
	"github.com/bypepe77/ZenithDB/database/query"
)

type Collection struct {
	Name    string
	data    map[string]*document.Document
	indexes map[string]*indexing.Index
	mutex   sync.RWMutex
	db      *MemoryStorage
}

func NewCollection(name string, db *MemoryStorage) *Collection {
	return &Collection{
		Name:    name,
		data:    make(map[string]*document.Document),
		indexes: make(map[string]*indexing.Index),
		db:      db,
	}
}

func (c *Collection) CreateIndex(field string, options *indexing.IndexOptions) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if _, exists := c.indexes[field]; exists {
		return fmt.Errorf("index already exists for field %s", field)
	}

	index := indexing.NewIndex(field, options)
	c.indexes[field] = index

	for _, doc := range c.data {
		if err := index.Insert(doc); err != nil {
			return fmt.Errorf("error inserting document into index: %v", err)
		}
	}

	return nil
}

func (c *Collection) Insert(id string, doc *document.Document) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if _, exists := c.data[id]; exists {
		return nil
	}

	c.data[id] = doc

	if err := c.createIndexesFromModel(doc); err != nil {
		return fmt.Errorf("error creating indexes from model: %v", err)
	}

	for _, index := range c.indexes {
		if err := index.Insert(doc); err != nil {
			return err
		}
	}

	if err := c.db.SaveCollection(c.Name, c.data); err != nil {
		return err
	}

	return nil
}

func (c *Collection) Delete(id string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	doc, exists := c.data[id]
	if !exists {
		return fmt.Errorf("document with ID %s not found", id)
	}

	delete(c.data, id)

	for _, index := range c.indexes {
		if err := index.Delete(doc); err != nil {
			return err
		}
	}

	if err := c.db.SaveCollection(c.Name, c.data); err != nil {
		return err
	}

	return nil
}

func (c *Collection) Find(q *query.Query) ([]*document.Document, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	var foundDocs []*document.Document

	for _, index := range c.indexes {
		if index.CanUseIndex(q) {
			docIDs, err := index.Find(q)
			if err != nil {
				return nil, err
			}

			for _, docID := range docIDs {
				doc, exists := c.data[docID]
				if exists {
					foundDocs = append(foundDocs, doc)
				}
			}

			if len(foundDocs) > 0 {
				return foundDocs, nil
			}
		}
	}

	for _, doc := range c.data {
		if q.Matches(doc) {
			foundDocs = append(foundDocs, doc)
		}
	}

	return foundDocs, nil
}

func (c *Collection) Get(id string) (*document.Document, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	doc, exists := c.data[id]
	if !exists {
		return nil, fmt.Errorf("document with ID %s not found", id)
	}

	return doc, nil
}

func (c *Collection) Update(id string, doc *document.Document) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	oldDoc, exists := c.data[id]
	if !exists {
		return fmt.Errorf("document with ID %s not found", id)
	}

	c.data[id] = doc

	for _, index := range c.indexes {
		if err := index.Delete(oldDoc); err != nil {
			return err
		}
		if err := index.Insert(doc); err != nil {
			return err
		}
	}

	if err := c.db.SaveCollection(c.Name, c.data); err != nil {
		return err
	}

	return nil
}

func (c *Collection) createIndexesFromModel(model interface{}) error {
	modelValue := reflect.ValueOf(model)

	if modelValue.Kind() == reflect.Ptr {
		if modelValue.IsNil() {
			return fmt.Errorf("model is a nil pointer")
		}
		modelValue = modelValue.Elem()
	}

	if modelValue.Kind() != reflect.Struct {
		return fmt.Errorf("model must be a struct or a pointer to a struct")
	}

	for i := 0; i < modelValue.NumField(); i++ {
		field := modelValue.Type().Field(i)
		if value := field.Tag.Get("index"); value == "true" {
			options := &indexing.IndexOptions{
				Unique: false,
			}
			if err := c.CreateIndex(field.Name, options); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *Collection) BulkInsert(docs []*document.Document, batchSize int) error {
	var wg sync.WaitGroup
	numBatches := (len(docs) + batchSize - 1) / batchSize

	errChan := make(chan error, numBatches)
	defer close(errChan)

	for i := 0; i < numBatches; i++ {
		start := i * batchSize
		end := start + batchSize
		if end > len(docs) {
			end = len(docs)
		}

		wg.Add(1)
		go func(batch []*document.Document) {
			defer wg.Done()
			c.mutex.Lock()
			defer c.mutex.Unlock()

			for _, doc := range batch {
				id := doc.ID

				if _, exists := c.data[id]; exists {
					errChan <- nil
					return
				}

				c.data[id] = doc
			}
		}(docs[start:end])
	}

	wg.Wait()

	select {
	case err := <-errChan:
		return err
	default:
	}

	if err := c.db.SaveCollection(c.Name, c.data); err != nil {
		return err
	}

	for _, doc := range docs {
		if err := c.createIndexesFromModel(doc); err != nil {
			return fmt.Errorf("error creating indexes from model: %v", err)
		}
		for _, index := range c.indexes {
			if err := index.Insert(doc); err != nil {
				return err
			}
		}
	}

	return nil
}
