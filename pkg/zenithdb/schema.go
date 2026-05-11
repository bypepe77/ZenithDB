package zenithdb

import "fmt"

// FieldKind describes the storage-level type ZenithDB understands.
type FieldKind string

const (
	FieldString FieldKind = "string"
	FieldInt64  FieldKind = "int64"
	FieldBool   FieldKind = "bool"
	FieldFloat  FieldKind = "float"
	FieldTime   FieldKind = "time"
)

// Field defines one model property.
type Field struct {
	Name     string
	Kind     FieldKind
	Required bool
}

// Index defines a secondary lookup path.
type Index struct {
	Name   string
	Fields []string
	Unique bool
}

// Relation defines metadata for a Prisma-like relation.
type Relation struct {
	Name       string
	Model      string
	Fields     []string
	References []string
	Many       bool
}

// Model defines the schema for a logical collection.
type Model struct {
	Name       string
	Fields     []Field
	PrimaryKey []string
	Indexes    []Index
	Relations  []Relation
}

// Schema is the complete database model definition.
type Schema struct {
	Models []Model
}

// Validate checks model names, fields, indexes, primary keys, and relations.
func (s Schema) Validate() error {
	return s.validate()
}

func (s Schema) validate() error {
	seenModels := make(map[string]struct{}, len(s.Models))
	for _, model := range s.Models {
		if model.Name == "" {
			return fmt.Errorf("model name is required")
		}
		if _, ok := seenModels[model.Name]; ok {
			return fmt.Errorf("model %q is defined more than once", model.Name)
		}
		seenModels[model.Name] = struct{}{}

		if len(model.PrimaryKey) == 0 {
			return fmt.Errorf("model %q must define a primary key", model.Name)
		}

		fields := make(map[string]Field, len(model.Fields))
		for _, field := range model.Fields {
			if field.Name == "" {
				return fmt.Errorf("model %q has a field without a name", model.Name)
			}
			if _, ok := fields[field.Name]; ok {
				return fmt.Errorf("model %q field %q is defined more than once", model.Name, field.Name)
			}
			fields[field.Name] = field
		}

		for _, name := range model.PrimaryKey {
			if _, ok := fields[name]; !ok {
				return fmt.Errorf("model %q primary key references unknown field %q", model.Name, name)
			}
		}

		indexNames := make(map[string]struct{}, len(model.Indexes))
		for _, index := range model.Indexes {
			if index.Name == "" {
				return fmt.Errorf("model %q has an index without a name", model.Name)
			}
			if _, ok := indexNames[index.Name]; ok {
				return fmt.Errorf("model %q index %q is defined more than once", model.Name, index.Name)
			}
			indexNames[index.Name] = struct{}{}
			if len(index.Fields) == 0 {
				return fmt.Errorf("model %q index %q must include at least one field", model.Name, index.Name)
			}
			for _, field := range index.Fields {
				if _, ok := fields[field]; !ok {
					return fmt.Errorf("model %q index %q references unknown field %q", model.Name, index.Name, field)
				}
			}
		}
	}

	for _, model := range s.Models {
		for _, relation := range model.Relations {
			if _, ok := seenModels[relation.Model]; !ok {
				return fmt.Errorf("model %q relation %q references unknown model %q", model.Name, relation.Name, relation.Model)
			}
			if len(relation.Fields) != len(relation.References) {
				return fmt.Errorf("model %q relation %q must have the same number of fields and references", model.Name, relation.Name)
			}
		}
	}

	return nil
}
