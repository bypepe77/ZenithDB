package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/bypepe77/ZenithDB/database/document"
)

type MemoryStorage struct {
	dataDir     string
	collections map[string]*Collection
	models      map[string]interface{}
	mutex       sync.RWMutex
}

// NewMemoryStorage creates a new MemoryStorage instance with the specified data directory.
func NewMemoryStorage(dataDir string) (*MemoryStorage, error) {
	err := os.MkdirAll(dataDir, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("failed to create data directory: %v", err)
	}

	ms := &MemoryStorage{
		dataDir:     dataDir,
		collections: make(map[string]*Collection),
		models:      make(map[string]interface{}),
	}
	return ms, nil
}

// RegisterModel registers a model for a specific collection.
func (ms *MemoryStorage) RegisterModel(collectionName string, model interface{}) {
	ms.mutex.Lock()
	defer ms.mutex.Unlock()
	ms.models[collectionName] = model
}

// GetModel retrieves the model registered for a specific collection.
func (ms *MemoryStorage) GetModel(collectionName string) interface{} {
	ms.mutex.RLock()
	defer ms.mutex.RUnlock()
	model, exists := ms.models[collectionName]
	if !exists {
		fmt.Printf("No model registered for collection %s\n", collectionName)
		return nil
	}
	return model
}

// LoadExistingCollections loads all existing collections from the data directory.
func (ms *MemoryStorage) LoadExistingCollections() error {
	files, err := os.ReadDir(ms.dataDir)
	if err != nil {
		return fmt.Errorf("failed to read data directory: %v", err)
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		collectionName := file.Name()
		collectionName = collectionName[:len(collectionName)-5] // Remove .json extension

		collection, err := ms.LoadCollection(collectionName)
		if err != nil {
			return fmt.Errorf("failed to load collection '%s': %v", collectionName, err)
		}

		collectionInstance := NewCollection(collectionName, ms)
		fmt.Println("Loaded collection", collectionName, "with", len(collection), "documents")

		collectionInstance.data = collection

		ms.collections[collectionName] = collectionInstance
		model := ms.GetModel(collectionName)
		err = collectionInstance.ApplyIndexesFromModel(model)
		if err != nil {
			return fmt.Errorf("failed to apply indexes from model: %v", err)
		}
	}
	return nil
}

// CreateCollection creates a new collection with the specified name.
func (ms *MemoryStorage) CreateCollection(name string) (*Collection, error) {
	ms.mutex.Lock()
	defer ms.mutex.Unlock()

	if _, exists := ms.collections[name]; exists {
		return nil, fmt.Errorf("collection '%s' already exists", name)
	}

	collection := NewCollection(name, ms)
	ms.collections[name] = collection

	return collection, nil
}

// GetCollection retrieves a collection by name.
func (ms *MemoryStorage) GetCollection(name string) (*Collection, error) {
	ms.mutex.RLock()
	defer ms.mutex.RUnlock()

	collection, exists := ms.collections[name]
	if exists {
		return collection, nil
	}

	return nil, fmt.Errorf("collection '%s' does not exist", name)
}

// SaveCollection saves the specified collection data to a file.
func (ms *MemoryStorage) SaveCollection(name string, data map[string]*document.Document) error {
	ms.mutex.Lock()
	defer ms.mutex.Unlock()
	filePath := ms.getCollectionFilePath(name)

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create collection file: %v", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	err = encoder.Encode(data)
	if err != nil {
		return fmt.Errorf("failed to encode collection data: %v", err)
	}

	return nil
}

// LoadCollection loads the specified collection data from a file.
func (ms *MemoryStorage) LoadCollection(name string) (map[string]*document.Document, error) {
	ms.mutex.Lock()
	defer ms.mutex.Unlock()

	filePath := ms.getCollectionFilePath(name)

	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return make(map[string]*document.Document), nil
		}
		return nil, fmt.Errorf("failed to open collection file: %v", err)
	}
	defer file.Close()

	var collectionData map[string]*document.Document
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&collectionData)
	if err != nil {
		return nil, fmt.Errorf("failed to decode collection data: %v", err)
	}

	return collectionData, nil
}

// getCollectionFilePath constructs the file path for a collection.
func (ms *MemoryStorage) getCollectionFilePath(name string) string {
	return filepath.Join(ms.dataDir, name+".json")
}
