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
	mutex       sync.RWMutex
}

func NewMemoryStorage(dataDir string) (*MemoryStorage, error) {
	ms := &MemoryStorage{
		dataDir:     dataDir,
		collections: make(map[string]*Collection),
	}

	if err := ms.loadCollections(); err != nil {
		return nil, err
	}

	return ms, nil
}

func (ms *MemoryStorage) CreateCollection(name string) (*Collection, error) {
	ms.mutex.Lock()
	defer ms.mutex.Unlock()

	// Check if the collection already exists in memory
	if collection, exists := ms.collections[name]; exists {
		return collection, nil
	}

	// Check if the collection's data file already exists on disk
	filePath := ms.getCollectionFilePath(name)
	if _, err := os.Stat(filePath); err == nil {
		collection, err := ms.loadCollection(name)
		if err != nil {
			return nil, fmt.Errorf("failed to load collection '%s': %w", name, err)
		}
		ms.collections[name] = collection
		return collection, nil
	}

	// Create a new collection
	collection := NewCollection(name, ms)
	ms.collections[name] = collection

	return collection, nil
}
func (ms *MemoryStorage) GetCollection(name string) (*Collection, error) {
	ms.mutex.RLock()
	defer ms.mutex.RUnlock()

	collection, exists := ms.collections[name]
	if !exists {
		return nil, fmt.Errorf("collection '%s' not found", name)
	}

	return collection, nil
}

func (ms *MemoryStorage) SaveCollection(collection *Collection) error {
	ms.mutex.Lock()
	defer ms.mutex.Unlock()

	data, err := json.MarshalIndent(collection.data, "", "  ")
	if err != nil {
		return err
	}

	filePath := ms.getCollectionFilePath(collection.Name)
	return os.WriteFile(filePath, data, 0644)
}

func (ms *MemoryStorage) loadCollections() error {
	files, err := os.ReadDir(ms.dataDir)
	if err != nil {
		return err
	}

	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".json" {
			collectionName := file.Name()[:len(file.Name())-5]
			collection, err := ms.loadCollection(collectionName)
			if err != nil {
				return err
			}
			ms.collections[collectionName] = collection
		}
	}

	return nil
}

func (ms *MemoryStorage) loadCollection(name string) (*Collection, error) {
	filePath := ms.getCollectionFilePath(name)
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var collectionData map[string]*document.Document
	if len(data) == 0 {
		collectionData = make(map[string]*document.Document)
	} else {
		if err := json.Unmarshal(data, &collectionData); err != nil {
			return nil, err
		}
	}

	collection := NewCollection(name, ms)
	collection.data = collectionData

	return collection, nil
}
func (ms *MemoryStorage) getCollectionFilePath(name string) string {
	return filepath.Join(ms.dataDir, name+".json")
}
