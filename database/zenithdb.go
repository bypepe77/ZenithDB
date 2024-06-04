package zenithdb

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/bypepe77/ZenithDB/database/document"
	"github.com/bypepe77/ZenithDB/database/query"
	"github.com/bypepe77/ZenithDB/database/storage"
)

// ZenithDB es la estructura principal de la base de datos.
type ZenithDB struct {
	storage *storage.MemoryStorage
	mutex   sync.RWMutex
}

// New crea una nueva instancia de ZenithDB con el almacenamiento proporcionado.
func New(storage *storage.MemoryStorage) *ZenithDB {
	return &ZenithDB{
		storage: storage,
	}
}

// Collection representa una colección de documentos.
type Collection struct {
	db         *ZenithDB
	name       string
	collection *storage.Collection
}

// CreateCollection crea una nueva colección en la base de datos o devuelve la existente.
func (db *ZenithDB) CreateCollection(name string) (*Collection, error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	// Intenta obtener la colección existente
	existCollection, err := db.storage.GetCollection(name)
	if err != nil && err != storage.ErrCollectionNotFound {
		return nil, fmt.Errorf("error al comprobar la existencia de la colección: %w", err)
	}

	// Si la colección existe, devuélvela
	if existCollection != nil {
		return &Collection{
			db:         db,
			name:       name,
			collection: existCollection,
		}, nil
	}

	// Crea la colección en el almacenamiento
	collection, err := db.storage.CreateCollection(name)
	if err != nil {
		return nil, fmt.Errorf("fallo al crear la colección en el almacenamiento: %w", err)
	}

	return &Collection{
		db:         db,
		name:       name,
		collection: collection,
	}, nil
}

// GetCollection recupera una colección por nombre.
func (db *ZenithDB) GetCollection(name string) (*Collection, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	collection, err := db.storage.GetCollection(name)
	if err != nil {
		return nil, fmt.Errorf("colección no encontrada: %s", name)
	}

	return &Collection{
		db:         db,
		name:       name,
		collection: collection,
	}, nil
}

// Insert inserta un nuevo documento en la colección.
func (c *Collection) Insert(doc *document.Document) error {
	return c.collection.Insert(doc.ID, doc)
}

// GetByID recupera un documento por su ID.
func (c *Collection) GetByID(id string) (*document.Document, error) {
	return c.collection.Get(id)
}

// Update modifica un documento existente.
func (c *Collection) Update(doc *document.Document) error {
	return c.collection.Update(doc.ID, doc)
}

// Delete elimina un documento de la colección.
func (c *Collection) Delete(id string) error {
	return c.collection.Delete(id)
}

// Find realiza una operación de búsqueda en la colección usando la consulta proporcionada.
func (c *Collection) Find(q *query.Query) ([]*document.Document, error) {
	docs, err := c.collection.Find(q)
	if err != nil {
		return nil, err
	}

	if q.ShouldPopulate() {
		for _, doc := range docs {
			err := c.populateDocument(doc, q.GetPopulateFields(), q.GetRelatedCollection(), q.GetPopulatedOutputField())
			if err != nil {
				return nil, err
			}
		}
	}

	return docs, nil
}

// BulkInsert inserta múltiples documentos en la colección en bloque.
func (c *Collection) BulkInsert(docs []*document.Document, batchSize int) error {
	return c.collection.BulkInsert(docs, batchSize)
}

// populateDocument llena los campos especificados de un documento.
func (c *Collection) populateDocument(doc *document.Document, fields []string, collection, outputField string) error {

	// Verificar el tipo de datos del documento
	data, ok := doc.Data.(map[string]interface{})
	if !ok {
		// Intentar convertir los datos a JSON y luego a un mapa
		dataJSON, err := json.Marshal(doc.Data)
		if err != nil {
			return fmt.Errorf("error al convertir los datos del documento a JSON: %v", err)
		}

		var convertedData map[string]interface{}
		if err := json.Unmarshal(dataJSON, &convertedData); err != nil {
			return fmt.Errorf("error al convertir los datos del documento a un mapa: %v", err)
		}

		data = convertedData
	}

	for _, field := range fields {
		relatedCollection, err := c.db.GetCollection(collection)
		if err != nil {
			return fmt.Errorf("error al obtener la colección relacionada: %v", err)
		}

		relatedDoc, err := relatedCollection.GetByID(data[field].(string))
		if err != nil {
			return fmt.Errorf("error al obtener el documento relacionado por ID: %v", err)
		}
		data[outputField] = relatedDoc.Data
	}

	doc.Data = data
	return nil
}
