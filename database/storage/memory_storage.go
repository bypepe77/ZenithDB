package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"

	"github.com/bypepe77/ZenithDB/database/document"
	"github.com/bypepe77/ZenithDB/modelregistry"
)

var ErrCollectionNotFound = errors.New("collection not found")

type MemoryStorage struct {
	dataDir       string
	collections   map[string]*Collection
	modelRegistry *modelregistry.ModelRegistry
	mutex         sync.RWMutex
}

func NewMemoryStorage(dataDir string) (*MemoryStorage, error) {
	err := os.MkdirAll(dataDir, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("failed to create data directory: %v", err)
	}

	return &MemoryStorage{
		dataDir:       dataDir,
		collections:   make(map[string]*Collection),
		modelRegistry: modelregistry.New(),
	}, nil
}

func (ms *MemoryStorage) RegisterDefaultModels(collectionName string, model interface{}) error {
	modelType := reflect.TypeOf(model)
	if modelType.Kind() != reflect.Struct && modelType.Kind() != reflect.Ptr {
		return fmt.Errorf("Model registered for collection %s is not a struct or a pointer to a struct", collectionName)
	}

	err := ms.modelRegistry.RegisterModel(collectionName, model)
	if err != nil {
		return err
	}

	return nil
}

func (ms *MemoryStorage) RegisterIndex(collectionName string, fields []string) {
	ms.mutex.RLock()
	defer ms.mutex.RUnlock()

	collection, err := ms.GetCollection(collectionName)
	if err != nil {
		fmt.Println("Error getting collection", err)
		return
	}

	collection.CreateIndexes(fields, collectionName)
}

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

		err := ms.LoadAndCreateCollection(collectionName)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ms *MemoryStorage) LoadAndCreateCollection(collectionName string) error {
	collection, err := ms.LoadCollection(collectionName)
	if err != nil {
		return fmt.Errorf("failed to load collection '%s': %v", collectionName, err)
	}

	collectionInstance := NewCollection(collectionName, ms)
	collectionInstance.data = collection

	ms.collections[collectionName] = collectionInstance
	fmt.Println("Loaded collection", collectionName, "with", len(collection), "documents")

	return nil
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
	if exists {
		return collection, nil
	}

	return nil, ErrCollectionNotFound
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
