package storage

import (
	"fmt"
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
			return err
		}
	}

	return nil
}

func (c *Collection) Insert(id string, doc *document.Document) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if _, exists := c.data[id]; exists {
		return fmt.Errorf("document with ID %s already exists", id)
	}

	// Agregar el documento a la memoria
	c.data[id] = doc

	// Insertar el documento en los índices
	for _, index := range c.indexes {
		if err := index.Insert(doc); err != nil {
			return err
		}
	}

	// Guardar la colección en el archivo
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

	// Eliminar el documento de la memoria
	delete(c.data, id)

	// Eliminar el documento de los índices
	for _, index := range c.indexes {
		if err := index.Delete(doc); err != nil {
			return err
		}
	}

	// Guardar la colección en el archivo
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
	}

	if len(foundDocs) == 0 {
		fmt.Println("No documents found")
		for _, doc := range c.data {
			if q.Matches(doc) {
				foundDocs = append(foundDocs, doc)
			}
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

	// Actualizar el documento en la memoria
	c.data[id] = doc

	// Actualizar el documento en los índices
	for _, index := range c.indexes {
		if err := index.Delete(oldDoc); err != nil {
			return err
		}
		if err := index.Insert(doc); err != nil {
			return err
		}
	}

	// Guardar la colección en el archivo
	if err := c.db.SaveCollection(c.Name, c.data); err != nil {
		return err
	}

	return nil
}
