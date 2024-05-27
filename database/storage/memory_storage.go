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
	err := os.MkdirAll(dataDir, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("failed to create data directory: %v", err)
	}

	return &MemoryStorage{
		dataDir:     dataDir,
		collections: make(map[string]*Collection),
	}, nil
}

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

func (ms *MemoryStorage) GetCollection(name string) (*Collection, error) {
	ms.mutex.RLock()
	defer ms.mutex.RUnlock()

	collection, exists := ms.collections[name]
	if !exists {
		return nil, fmt.Errorf("collection '%s' not found", name)
	}

	return collection, nil
}

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

func (ms *MemoryStorage) getCollectionFilePath(name string) string {
	return filepath.Join(ms.dataDir, name+".json")
}
