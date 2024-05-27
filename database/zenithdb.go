package zenithdb

import (
	"errors"
	"fmt"
	"sync"

	"github.com/bypepe77/ZenithDB/database/document"
	"github.com/bypepe77/ZenithDB/database/storage"
)

// ZenithDB is the main database structure.
type ZenithDB struct {
	storage     storage.MemoryStorage
	collections map[string]*Collection // Collection name -> Collection instance
	mutex       sync.RWMutex
}

// NewZenithDB creates a new instance of ZenithDB with the provided storage.
func New(storage storage.MemoryStorage) *ZenithDB {
	return &ZenithDB{
		storage:     storage,
		collections: make(map[string]*Collection),
	}
}

// Collection represents a collection of documents.
type Collection struct {
	db   *ZenithDB
	name string
}

// CreateCollection creates a new collection in the database or returns the existing one.
func (db *ZenithDB) CreateCollection(name string) (*Collection, error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	// Check if the collection already exists
	if collection, exists := db.collections[name]; exists {
		fmt.Println("Collection already exists")
		return collection, nil // Return the existing collection
	}

	// Create the collection in the storage
	if err := db.storage.CreateCollection(name); err != nil {
		return nil, fmt.Errorf("failed to create collection in storage: %w", err)
	}

	collection := &Collection{
		db:   db,
		name: name,
	}
	db.collections[name] = collection

	return collection, nil
}

// GetCollection retrieves a collection by name.
func (db *ZenithDB) GetCollection(name string) (*Collection, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	collection, exists := db.collections[name]
	if !exists {
		return nil, fmt.Errorf("collection not found: %s", name)
	}

	return collection, nil
}

// Insert inserts a new document into the database.
func (c *Collection) Insert(doc *document.Document) error {
	c.db.mutex.Lock()
	defer c.db.mutex.Unlock()

	return c.db.storage.Insert(c.name, doc.ID, doc)
}

// Get retrieves a document by its ID.
func (c *Collection) Get(id string) (*document.Document, error) {
	c.db.mutex.RLock()
	defer c.db.mutex.RUnlock()

	return c.db.storage.Get(c.name, id)
}

// Update modifies an existing document.
func (c *Collection) Update(doc *document.Document) error {
	c.db.mutex.Lock()
	defer c.db.mutex.Unlock()

	if _, err := c.db.storage.Get(c.name, doc.ID); err != nil {
		return errors.New("document not found")
	}

	return c.db.storage.Update(c.name, doc.ID, doc)
}

// Delete removes a document from the database.
func (c *Collection) Delete(id string) error {
	c.db.mutex.Lock()
	defer c.db.mutex.Unlock()

	return c.db.storage.Delete(c.name, id)
}

// Close closes the database.
func (db *ZenithDB) Close() error {
	return db.storage.Close()
}
