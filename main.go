package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	zenithdb "github.com/bypepe77/ZenithDB/database"
	"github.com/bypepe77/ZenithDB/database/query"
	"github.com/bypepe77/ZenithDB/database/storage"
)

// Product represents a product document.
type Product struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Price       float64   `json:"price" `
	CategoryID  string    `json:"category_id" `
	Category    *Category `json:"category,omitempty"`
}

// NewProduct creates a new Product instance.
func NewProduct(id, name, description string, price float64, categoryID string) *Product {
	return &Product{
		ID:          id,
		Name:        name,
		Description: description,
		Price:       price,
		CategoryID:  categoryID,
	}
}

// Category represents a category document.
type Category struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// NewCategory creates a new Category instance.
func NewCategory(id, name string) *Category {
	return &Category{
		ID:   id,
		Name: name,
	}
}

var productsCollection *zenithdb.Collection
var categoriesCollection *zenithdb.Collection

func main() {
	// Data directory for collections
	dataDir := "collections"

	// Create a new MemoryStorage with the data directory
	memStorage, err := storage.NewMemoryStorage(dataDir)
	if err != nil {
		log.Fatal(err)
	}

	productModel := &Product{}
	categoryModel := &Category{}
	err = memStorage.RegisterDefaultModels("products", productModel)
	if err != nil {
		log.Fatal(err)
	}

	err = memStorage.RegisterDefaultModels("categories", categoryModel)
	if err != nil {
		log.Fatal(err)
	}

	err = memStorage.LoadExistingCollections()
	if err != nil {
		log.Fatal(err)
	}

	// Create a new ZenithDB instance
	db := zenithdb.New(memStorage)

	// Create collections

	productsCollection, err = db.CreateCollection("products")
	if err != nil {
		fmt.Println("Error creating collection:", err)
		log.Fatal(err)
	}

	categoriesCollection, err = db.CreateCollection("categories")
	if err != nil {
		fmt.Println("Error creating collection:", err)
		log.Fatal(err)
	}

	/*
			// Insert sample categories
			for i := 1; i <= 10; i++ {
				categoryID := "category-" + strconv.Itoa(i)
				categoryName := "Category " + strconv.Itoa(i)
				category := NewCategory(categoryID, categoryName)
				newDoc := document.New(categoryID, category)
				if err := categoriesCollection.Insert(newDoc); err != nil {
					fmt.Println("Error inserting category:", err)
					log.Fatal(err)
				}
			}

			// Prepare 200k documents for bulk insertion
			var docs []*document.Document
			for i := 1; i <= 200000; i++ {
				productID := "product-" + strconv.Itoa(i)
				productName := "Product " + strconv.Itoa(i)
				productDescription := "Description for product " + strconv.Itoa(i)
				productPrice := float64(i) * 0.1                   // Just an example price
				categoryID := "category-" + strconv.Itoa((i%10)+1) // Example category ID

				product := NewProduct(productID, productName, productDescription, productPrice, categoryID)
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
	*/

	memStorage.RegisterIndex("products", []string{"name", "category_id", "price"})
	memStorage.RegisterIndex("categories", []string{"name", "id"})

	// Start HTTP server
	http.HandleFunc("/product", getProductByName)
	fmt.Println("Server started at http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func getProductByName(w http.ResponseWriter, r *http.Request) {
	// Parse query parameter
	name := r.URL.Query().Get("name")
	if name == "" {
		http.Error(w, "Missing 'name' query parameter", http.StatusBadRequest)
		return
	}

	// Measure query time
	startQuery := time.Now()

	// Create query with populate
	productQuery := query.NewQuery().
		Where("name", query.OpEqual, name).
		Populate("category_id", "categories", "category", query.NewQuery())

	allProducts, err := productsCollection.Find(productQuery)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error querying products: %v", err), http.StatusInternalServerError)
		return
	}

	elapsedQuery := time.Since(startQuery)
	fmt.Printf("Query time: %d ms\n", elapsedQuery.Milliseconds())

	// If no product found, return 404
	if len(allProducts) == 0 {
		http.Error(w, "Product not found", http.StatusNotFound)
		return
	}

	// Assume only one product will match the name
	productDataMap := allProducts[0].Data.(map[string]interface{})

	// Convert map to JSON and then to Product struct
	productDataJSON, err := json.Marshal(productDataMap)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error marshalling product data: %v", err), http.StatusInternalServerError)
		return
	}

	var productData Product
	fmt.Println(string(productDataJSON))
	if err := json.Unmarshal(productDataJSON, &productData); err != nil {
		http.Error(w, fmt.Sprintf("Error unmarshalling product data: %v", err), http.StatusInternalServerError)
		return
	}

	// Return the product details as JSON
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(productData); err != nil {
		http.Error(w, fmt.Sprintf("Error encoding response: %v", err), http.StatusInternalServerError)
		return
	}
}
