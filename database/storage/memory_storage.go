package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/bypepe77/ZenithDB/database/document"
)

// MemoryStorage implements Storage in memory with disk persistence for collections.
type MemoryStorage struct {
	dataDir     string                     // Directory to store collection data
	collections map[string]*collectionData // Collection name -> collection data
	mutex       sync.RWMutex
}

// collectionData holds data for a single collection.
type collectionData struct {
	data map[string]*document.Document
}

// NewMemoryStorage creates a new instance of MemoryStorage.
// It loads data from the specified directory if it exists.
func NewMemoryStorage(dataDir string) (*MemoryStorage, error) {
	ms := &MemoryStorage{
		dataDir:     dataDir,
		collections: make(map[string]*collectionData),
	}

	// Attempt to load existing collections from the directory.
	if err := ms.loadCollections(); err != nil {
		return nil, err
	}

	return ms, nil
}

// CreateCollection creates a new collection.
func (ms *MemoryStorage) CreateCollection(name string) error {
	ms.mutex.Lock()
	defer ms.mutex.Unlock()

	// Check if the collection already exists in memory
	if _, exists := ms.collections[name]; exists {
		return nil // Collection already exists, no need to create
	}

	ms.collections[name] = &collectionData{
		data: make(map[string]*document.Document),
	}

	// Check if the collection's data file already exists on disk
	filePath := ms.getCollectionFilePath(name)
	if _, err := os.Stat(filePath); err == nil {
		return nil // Collection file already exists, no need to create
	}

	// Create the collection's data file (even if empty) to mark its existence.
	return os.WriteFile(filePath, []byte("{}"), 0644)
}

// Insert adds a new document to the specified collection.
func (ms *MemoryStorage) Insert(collectionName, id string, doc *document.Document) error {
	ms.mutex.Lock()
	defer ms.mutex.Unlock()

	collData, ok := ms.collections[collectionName]
	if !ok {
		return fmt.Errorf("collection not found: %s", collectionName)
	}

	if _, exists := collData.data[id]; exists {
		return errors.New("document already exists")
	}

	collData.data[id] = doc

	// Persist to disk after insertion
	return ms.saveCollection(collectionName)
}

// Get retrieves a document by its ID from the specified collection.
func (ms *MemoryStorage) Get(collectionName, id string) (*document.Document, error) {
	ms.mutex.RLock()
	defer ms.mutex.RUnlock()

	collData, ok := ms.collections[collectionName]
	if !ok {
		return nil, fmt.Errorf("collection not found: %s", collectionName)
	}

	doc, exists := collData.data[id]
	if !exists {
		return nil, errors.New("document not found")
	}

	return doc, nil
}

// Update modifies an existing document in the specified collection.
func (ms *MemoryStorage) Update(collectionName, id string, doc *document.Document) error {
	ms.mutex.Lock()
	defer ms.mutex.Unlock()

	collData, ok := ms.collections[collectionName]
	if !ok {
		return fmt.Errorf("collection not found: %s", collectionName)
	}

	if _, exists := collData.data[id]; !exists {
		return errors.New("document not found")
	}

	collData.data[id] = doc

	// Persist to disk after update
	return ms.saveCollection(collectionName)
}

// Delete removes a document from the specified collection.
func (ms *MemoryStorage) Delete(collectionName, id string) error {
	ms.mutex.Lock()
	defer ms.mutex.Unlock()

	collData, ok := ms.collections[collectionName]
	if !ok {
		return fmt.Errorf("collection not found: %s", collectionName)
	}

	if _, exists := collData.data[id]; !exists {
		return errors.New("document not found")
	}

	delete(collData.data, id)

	// Persist to disk ONLY if the document existed and was deleted
	return ms.saveCollection(collectionName)
}

// Close closes the storage, persisting all collection data to disk.
func (ms *MemoryStorage) Close() error {
	for collectionName := range ms.collections {
		if err := ms.saveCollection(collectionName); err != nil {
			return err // Handle or log errors as needed
		}
	}
	return nil
}

// saveCollection persists the data for a specific collection to disk.
func (ms *MemoryStorage) saveCollection(collectionName string) error {
	collData, ok := ms.collections[collectionName]
	if !ok {
		return fmt.Errorf("collection not found: %s", collectionName)
	}

	// If data is empty, write an empty JSON object to the file
	if len(collData.data) == 0 {
		fmt.Printf("Data for collection '%s' is empty. Writing an empty JSON object to the file.\n", collectionName)
		return os.WriteFile(ms.getCollectionFilePath(collectionName), []byte("{}"), 0644)
	}

	data, err := json.MarshalIndent(collData.data, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(ms.getCollectionFilePath(collectionName), data, 0644)
}

// loadCollections loads data for all collections from the data directory.
func (ms *MemoryStorage) loadCollections() error {
	// Create the data directory if it doesn't exist.
	if err := os.MkdirAll(ms.dataDir, 0755); err != nil {
		return err
	}

	// Read all files in the data directory.
	files, err := os.ReadDir(ms.dataDir)
	if err != nil {
		return err
	}

	// Load data for each collection file.
	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".json" {
			collectionName := file.Name()[:len(file.Name())-5] // Remove ".json" extension

			// Load the collection data into memory
			if err := ms.loadCollection(collectionName); err != nil {
				return fmt.Errorf("failed to load collection '%s': %w", collectionName, err)
			}

			// Add the collection to the collections map
			ms.collections[collectionName] = &collectionData{
				data: make(map[string]*document.Document),
			}
		}
	}

	return nil
}

// loadCollection loads data for a specific collection from disk.
func (ms *MemoryStorage) loadCollection(collectionName string) error {
	data, err := os.ReadFile(ms.getCollectionFilePath(collectionName))
	if err != nil {
		return err
	}

	// If the file is empty, initialize an empty map for the collection.
	if len(data) == 0 {
		ms.collections[collectionName] = &collectionData{
			data: make(map[string]*document.Document),
		}
		return nil
	}

	// Unmarshal the JSON data into the collection's data map.
	collData := &collectionData{
		data: make(map[string]*document.Document),
	}
	if err := json.Unmarshal(data, &collData.data); err != nil {
		return err
	}

	ms.collections[collectionName] = collData
	return nil
}

// getCollectionFilePath returns the file path for a specific collection.
func (ms *MemoryStorage) getCollectionFilePath(collectionName string) string {
	return filepath.Join(ms.dataDir, collectionName+".json")
}
