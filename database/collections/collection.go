// zenithdb/collection.go
package zenithdb

import (
	"github.com/bypepe77/ZenithDB/database/storage"
)

// Collection represents a collection of documents.
type Collection struct {
	Name    string
	Storage storage.MemoryStorage // Each collection has its own memory storage
}
