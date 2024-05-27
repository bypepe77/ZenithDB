package storage

import (
	"errors"
	"fmt"
	"sync"

	"github.com/bypepe77/ZenithDB/database/document"
	"github.com/bypepe77/ZenithDB/database/indexing"
	"github.com/bypepe77/ZenithDB/database/query"
)

type Collection struct {
	Name    string
	data    map[string]*document.Document
	indexes *indexing.IndexManager
	mutex   sync.RWMutex
	db      *MemoryStorage
}

func NewCollection(name string, db *MemoryStorage) *Collection {
	return &Collection{
		Name:    name,
		data:    make(map[string]*document.Document),
		indexes: indexing.NewIndexManager(),
		db:      db,
	}
}

func (c *Collection) Insert(id string, doc *document.Document) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if _, exists := c.data[id]; exists {
		return errors.New("document already exists")
	}

	c.data[id] = doc

	if err := c.indexes.BuildIndexes(doc); err != nil {
		return err
	}

	// Guardar la colección en el archivo JSON
	if err := c.db.SaveCollection(c); err != nil {
		return err
	}

	return nil
}

func (c *Collection) Get(id string) (*document.Document, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	doc, exists := c.data[id]
	if !exists {
		return nil, errors.New("document not found")
	}

	return doc, nil
}

func (c *Collection) Update(id string, doc *document.Document) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	oldDoc, exists := c.data[id]
	if !exists {
		return errors.New("document not found")
	}

	c.data[id] = doc

	if err := c.indexes.UpdateIndexes(oldDoc, doc); err != nil {
		return err
	}

	return nil
}

func (c *Collection) Delete(id string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	doc, exists := c.data[id]
	if !exists {
		return errors.New("document not found")
	}

	delete(c.data, id)

	if err := c.indexes.DeleteIndexes(doc); err != nil {
		return err
	}

	return nil
}

func (c *Collection) Find(q *query.Query) ([]*document.Document, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	indexToUse := c.indexes.FindIndexForQuery(q)

	var foundDocs []*document.Document

	if indexToUse != nil {
		docIDs, err := indexToUse.Find(q)
		if err != nil {
			return nil, fmt.Errorf("error finding documents using index: %w", err)
		}

		for _, docID := range docIDs {
			doc, ok := c.data[docID]
			if ok {
				foundDocs = append(foundDocs, doc)
			}
		}
	} else {
		for _, doc := range c.data {
			if q.Matches(doc) {
				foundDocs = append(foundDocs, doc)
			}
		}
	}

	return foundDocs, nil
}
