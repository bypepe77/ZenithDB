package document

import "encoding/json"

// Document represents a JSON document in ZenithDB.
type Document struct {
	ID   string      `json:"id"`
	Data interface{} `json:"data"` // Use interface{} to store any model or array of models
}

// NewDocument creates a new Document instance.
func New(id string, data ...interface{}) *Document {
	// If more than one data element is provided, treat it as an array
	if len(data) > 1 {
		return &Document{
			ID:   id,
			Data: data,
		}
	}

	// If only one data element is provided, assign it directly
	if len(data) == 1 {
		return &Document{
			ID:   id,
			Data: data[0],
		}
	}

	// If no data is provided, initialize Data as nil
	return &Document{
		ID:   id,
		Data: nil,
	}
}

// JSON returns the JSON representation of the document.
func (d *Document) JSON() ([]byte, error) {
	return json.Marshal(d)
}
