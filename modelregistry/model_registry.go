package modelregistry

import (
	"fmt"
	"reflect"
	"sync"
)

type ModelRegistry struct {
	models map[string]interface{}
	sync.RWMutex
}

func New() *ModelRegistry {
	return &ModelRegistry{
		models: make(map[string]interface{}),
	}
}

func (mr *ModelRegistry) RegisterModel(collectionName string, model interface{}) error {
	modelType := reflect.TypeOf(model)
	if modelType.Kind() != reflect.Struct && modelType.Kind() != reflect.Ptr {
		return fmt.Errorf("Model registered for collection %s is not a struct or a pointer to a struct", collectionName)
	}

	mr.Lock()
	defer mr.Unlock()
	mr.models[collectionName] = model

	return nil
}

func (mr *ModelRegistry) GetModel(collectionName string) (interface{}, error) {
	mr.RLock()
	defer mr.RUnlock()

	model, exists := mr.models[collectionName]
	if !exists {
		return nil, fmt.Errorf("No model registered for collection %s", collectionName)
	}

	modelValue := reflect.ValueOf(model)
	modelType := modelValue.Type()

	if modelType.Kind() != reflect.Struct && modelType.Kind() != reflect.Ptr && modelType.Elem().Kind() != reflect.Struct {
		return nil, fmt.Errorf("Model registered for collection %s is not a struct or a pointer to a struct", collectionName)
	}

	return model, nil
}
