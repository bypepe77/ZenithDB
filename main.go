package main

import (
	"fmt"
	"log"

	zenithdb "github.com/bypepe77/ZenithDB/database"
	"github.com/bypepe77/ZenithDB/database/document"
	"github.com/bypepe77/ZenithDB/database/storage"
)

// Product represents a product document.
type Product struct {
	ID          string  `json:"id"`
	Name        string  `json:"name"`
	Description string  `json:"description"`
	Price       float64 `json:"price"`
}

// NewProduct creates a new Product instance.
func NewProduct(id, name, description string, price float64) *Product {
	return &Product{
		ID:          id,
		Name:        name,
		Description: description,
		Price:       price,
	}
}

func main() {
	// Data directory for collections
	dataDir := "collections"

	// Create a new MemoryStorage with the data directory
	memStorage, err := storage.NewMemoryStorage(dataDir)
	if err != nil {
		log.Fatal(err)
	}

	// Create a new ZenithDB instance
	db := zenithdb.New(*memStorage)

	// Create a collection named "products"
	productsCollection, err := db.CreateCollection("products")
	if err != nil {
		log.Fatal(err)
	}

	product := NewProduct(
		"product-2",
		"Example Product",
		"An example product for demonstration purposes",
		9.99,
	)

	// Insert a document into the "products" collection
	newDoc := document.New("doc-3", product)
	if err := productsCollection.Insert(newDoc); err != nil { // Use collection method
		log.Fatal(err)
	}

	// Retrieve the document from the "products" collection
	doc, err := productsCollection.Get("doc-3") // Use collection method
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Retrieved document: %+v\n", doc)

}
