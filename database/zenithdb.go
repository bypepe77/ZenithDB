package zenithdb

import (
	"fmt"
	"sync"

	"github.com/bypepe77/ZenithDB/database/document"
	"github.com/bypepe77/ZenithDB/database/query"
	"github.com/bypepe77/ZenithDB/database/storage"
)

// ZenithDB is the main database structure.
type ZenithDB struct {
	storage *storage.MemoryStorage
	mutex   sync.RWMutex
}

// NewZenithDB creates a new instance of ZenithDB with the provided storage.
func New(storage *storage.MemoryStorage) *ZenithDB {
	return &ZenithDB{
		storage: storage,
	}
}

// Collection represents a collection of documents.
type Collection struct {
	db         *ZenithDB
	name       string
	collection *storage.Collection
}

// CreateCollection creates a new collection in the database or returns the existing one.
func (db *ZenithDB) CreateCollection(name string) (*Collection, error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	existCollection, err := db.storage.GetCollection(name)
	if err != nil {
		return nil, fmt.Errorf("collection not found: %s", name)
	}

	if existCollection != nil {
		return &Collection{
			db:         db,
			name:       name,
			collection: existCollection,
		}, nil
	}

	// Create the collection in the storage
	collection, err := db.storage.CreateCollection(name)
	if err != nil {
		return nil, fmt.Errorf("failed to create collection in storage: %w", err)
	}

	model := db.storage.GetModel(name)
	err = collection.CreateIndexesFromModel(model)
	if err != nil {
		return nil, fmt.Errorf("failed to create indexes from model: %w", err)
	}

	return &Collection{
		db:         db,
		name:       name,
		collection: collection,
	}, nil
}

// GetCollection retrieves a collection by name.
func (db *ZenithDB) GetCollection(name string) (*Collection, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	collection, err := db.storage.GetCollection(name)
	if err != nil {
		return nil, fmt.Errorf("collection not found: %s", name)
	}

	return &Collection{
		db:         db,
		name:       name,
		collection: collection,
	}, nil
}

// Insert inserts a new document into the collection.
func (c *Collection) Insert(doc *document.Document) error {
	return c.collection.Insert(doc.ID, doc)
}

// GetByID retrieves a document by its ID.
func (c *Collection) GetByID(id string) (*document.Document, error) {
	return c.collection.Get(id)
}

// Update modifies an existing document.
func (c *Collection) Update(doc *document.Document) error {
	return c.collection.Update(doc.ID, doc)
}

// Delete removes a document from the collection.
func (c *Collection) Delete(id string) error {
	return c.collection.Delete(id)
}

// Find performs a search operation on the collection using the provided query.
func (c *Collection) Find(q *query.Query) ([]*document.Document, error) {
	return c.collection.Find(q)
}

func (c *Collection) BulkInsert(docs []*document.Document, batchSize int) error {
	return c.collection.BulkInsert(docs, batchSize)
}
