package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

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

var productsCollection *zenithdb.Collection

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
	productsCollection, err = db.CreateCollection("products", Product{})
	if err != nil {
		fmt.Println("Error creating collection:", err)
		log.Fatal(err)
	}

	// Prepare 200k documents for bulk insertion
	var docs []*document.Document
	for i := 1; i <= 1000000; i++ {
		productID := "product-" + strconv.Itoa(i)
		productName := "Product " + strconv.Itoa(i)
		productDescription := "Description for product " + strconv.Itoa(i)
		productPrice := float64(i) * 0.1 // Just an example price

		product := NewProduct(productID, productName, productDescription, productPrice)
		newDoc := document.New(productID, product)
		docs = append(docs, newDoc)
	}

	// Measure insertion time
	startInsertion := time.Now()

	// Insert documents in bulk
	if err := productsCollection.BulkInsert(docs, 200000); err != nil {
		fmt.Println("Error inserting documents:", err)
		log.Fatal(err)
	}

	elapsedInsertion := time.Since(startInsertion)
	fmt.Printf("Insertion time: %d ms\n", elapsedInsertion.Milliseconds())

	// Start HTTP server
	http.HandleFunc("/product", getProductByName)
	fmt.Println("Server started at http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// getProductByName handles GET requests and returns product details by name.
func getProductByName(w http.ResponseWriter, r *http.Request) {
	// Parse query parameter
	name := r.URL.Query().Get("name")
	if name == "" {
		http.Error(w, "Missing 'name' query parameter", http.StatusBadRequest)
		return
	}

	// Measure query time
	startQuery := time.Now()

	// Query the product by name
	q := query.NewQuery().Where("Name", query.OpEqual, name)
	docs, err := productsCollection.Find(q)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error querying documents: %v", err), http.StatusInternalServerError)
		return
	}

	elapsedQuery := time.Since(startQuery)
	fmt.Printf("Query time: %d ms\n", elapsedQuery.Milliseconds())

	// If no documents found, return 404
	if len(docs) == 0 {
		http.Error(w, "Product not found", http.StatusNotFound)
		return
	}

	// Return the product details as JSON
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(docs[0].Data); err != nil {
		http.Error(w, fmt.Sprintf("Error encoding response: %v", err), http.StatusInternalServerError)
		return
	}
}
