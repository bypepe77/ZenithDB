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

	if err := c.CreateIndexesFromModel(doc); err != nil {
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
				fmt.Println("Found docs using index", foundDocs)
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
func (c *Collection) CreateIndexesFromModel(model interface{}) error {
	modelType := reflect.TypeOf(model)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}

	if modelType.Kind() != reflect.Struct {
		return fmt.Errorf("model must be a struct or a pointer to a struct")
	}

	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		if indexTag := field.Tag.Get("index"); indexTag == "true" {
			indexName := field.Name
			indexOptions := &indexing.IndexOptions{
				Unique: false,
			}
			if err := c.CreateIndex(indexName, indexOptions); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *Collection) BulkInsert(docs []*document.Document, batchSize int) error {
	for _, doc := range docs {
		if err := c.insertDocument(doc); err != nil {
			return err
		}
	}

	if err := c.db.SaveCollection(c.Name, c.data); err != nil {
		return err
	}

	for _, doc := range docs {
		if err := c.CreateIndexesFromModel(doc); err != nil {
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

func (c *Collection) CreateIndexes(fields []string, collection string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	fmt.Println("fields", fields)
	for _, field := range fields {
		if _, exists := c.indexes[field]; !exists {
			options := &indexing.IndexOptions{
				Unique: false,
			}
			index := indexing.NewIndex(field, options)
			if index == nil {
				fmt.Printf("Error creating index for field %s\n", field)
				continue
			}
			c.indexes[field] = index
			for _, doc := range c.data {
				if index == nil {
					fmt.Printf("Index is nil for field %s\n", field)
					continue
				}
				if err := index.Insert(doc); err != nil {
					fmt.Printf("Error inserting document into index for field %s: %v\n", field, err)
				}
			}
		}
	}
}

func (c *Collection) insertDocument(doc *document.Document) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	id := doc.ID

	if _, exists := c.data[id]; exists {
		return nil
	}

	c.data[id] = doc

	return nil
}
