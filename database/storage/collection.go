package storage

import (
	"fmt"
	"reflect"
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
			fmt.Println("Error inserting document into index:", err)
			return err
		}
	}

	return nil
}

func (c *Collection) Insert(id string, doc *document.Document) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if _, exists := c.data[id]; exists {
		return nil
	}

	c.data[id] = doc

	err := c.CreateIndexesFromModel(doc)
	if err != nil {
		fmt.Println("Error creating indexes from model:", err)
		return err
	}

	for _, index := range c.indexes {
		if err := index.Insert(doc); err != nil {
			return err
		}
	}

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

	// Buscar en los índices
	for _, index := range c.indexes {
		fmt.Println("Checking index:", index.Field)

		if index.CanUseIndex(q) {
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

			// Si se encontraron documentos en el índice, no es necesario realizar una búsqueda completa
			if len(foundDocs) > 0 {
				fmt.Println("Found documents using index:", index.Field)
				return foundDocs, nil
			}
		}
	}

	// Si no se encontraron documentos en los índices, realizar una búsqueda completa
	fmt.Println("No suitable index found, performing full collection scan")
	for _, doc := range c.data {
		if q.Matches(doc) {
			foundDocs = append(foundDocs, doc)
		}
	}

	if len(foundDocs) == 0 {
		fmt.Println("No documents found")
	}

	return foundDocs, nil
}

func (c *Collection) Get(id string) (*document.Document, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	fmt.Println("Getting document with ID", id)
	fmt.Println("Collection data:", c.data)
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

func (c *Collection) CreateIndexesFromModel(model interface{}) error {
	fmt.Printf("Model inside CreateIndexesFromModel: %+v\n", model)

	// Get the value of the model
	modelValue := reflect.ValueOf(model)

	// Check if the model is a pointer
	if modelValue.Kind() == reflect.Ptr {
		if modelValue.IsNil() {
			return fmt.Errorf("model is a nil pointer")
		}
		// Get the underlying value of the pointer
		modelValue = modelValue.Elem()
	}

	// Check if the model is a struct
	if modelValue.Kind() != reflect.Struct {
		return fmt.Errorf("model must be a struct or a pointer to a struct")
	}

	// Iterate over the fields of the model
	for i := 0; i < modelValue.NumField(); i++ {
		field := modelValue.Type().Field(i)
		if value := field.Tag.Get("index"); value == "true" {
			fmt.Println("Creating index for field", field.Name)
			options := &indexing.IndexOptions{
				Unique: false,
			}
			err := c.CreateIndex(field.Name, options)
			if err != nil {
				fmt.Println("Error creating index:", err)
				return err
			}
		}
	}

	return nil
}

// BulkInsert inserts multiple documents into the collection in batches.
func (c *Collection) BulkInsert(docs []*document.Document, batchSize int) error {
	var wg sync.WaitGroup
	numBatches := (len(docs) + batchSize - 1) / batchSize

	errChan := make(chan error, numBatches)
	defer close(errChan)

	for i := 0; i < numBatches; i++ {
		start := i * batchSize
		end := start + batchSize
		if end > len(docs) {
			end = len(docs)
		}

		wg.Add(1)
		go func(batch []*document.Document) {
			defer wg.Done()
			c.mutex.Lock()
			defer c.mutex.Unlock()

			for _, doc := range batch {
				id := doc.ID

				if _, exists := c.data[id]; exists {
					errChan <- nil
					return
				}

				// Add the document to memory
				c.data[id] = doc
			}
		}(docs[start:end])
	}

	wg.Wait()

	// Check for errors
	select {
	case err := <-errChan:
		return err
	default:
	}

	// Save the collection to the file once
	if err := c.db.SaveCollection(c.Name, c.data); err != nil {
		return err
	}

	// Create indexes after bulk insertion
	for _, doc := range docs {
		if err := c.CreateIndexesFromModel(doc); err != nil {
			fmt.Println("Error creating indexes from model:", err)
			return err
		}
		for _, index := range c.indexes {
			if err := index.Insert(doc); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *Collection) GetIndexes() map[string]*indexing.Index {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c.indexes
}

func (c *Collection) GetDocuments() map[string]*document.Document {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c.data
}

func (c *Collection) ApplyIndexesFromModel(model interface{}) error {
	fmt.Println("Applying indexes from model")
	fmt.Printf("Model inside ApplyIndexesFromModel: %+v\n", model)
	err := c.CreateIndexesFromModel(model)
	if err != nil {
		return err
	}

	for _, doc := range c.data {
		for _, index := range c.indexes {
			if err := index.Insert(doc); err != nil {
				fmt.Println("Error inserting document into index:", err)
				return err
			}
		}
	}

	return nil
}
