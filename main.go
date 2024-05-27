package main

import (
	"fmt"
	"log"

	zenithdb "github.com/bypepe77/ZenithDB/database"
	"github.com/bypepe77/ZenithDB/database/document"
	"github.com/bypepe77/ZenithDB/database/query"
	"github.com/bypepe77/ZenithDB/database/storage"
)

// Product represents a product document.
type Product struct {
	ID          string  `json:"id"`
	Name        string  `json:"name" index:"true"`
	Description string  `json:"description"`
	Price       float64 `json:"price" index:"true"`
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
	db := zenithdb.New(memStorage)

	// Create a collection named "products"
	productsCollection, err := db.CreateCollection("products")
	if err != nil {
		fmt.Println("Error creating collection:", err)
		log.Fatal(err)
	}

	// Insert documents into the "products" collection
	product1 := NewProduct(
		"product-1",
		"Example Product 1",
		"An example product for demonstration purposes",
		9.99,
	)
	newDoc1 := document.New("doc-1", product1)
	if err := productsCollection.Insert(newDoc1); err != nil {
		fmt.Println("Error inserting document:", err)
		log.Fatal(err)
	}

	product2 := NewProduct(
		"product-2",
		"Example Product 2",
		"Another example product for demonstration purposes",
		19.99,
	)
	newDoc2 := document.New("doc-2", product2)
	if err := productsCollection.Insert(newDoc2); err != nil {
		fmt.Println("Error inserting document:", err)
		log.Fatal(err)
	}

	query := query.NewQuery().Where("name", query.OpEqual, "Example Product 1")
	docs, err := productsCollection.Find(query)
	if err != nil {
		fmt.Println("Error finding documents:", err)
		log.Fatal(err)
	}

	for _, doc := range docs {
		fmt.Println("Found document:", doc.ID)
	}

}
