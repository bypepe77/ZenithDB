package compiler

import (
	"bytes"
	"fmt"
	"go/format"
	"strings"

	"github.com/bypepe77/ZenithDB/pkg/zenithdb"
)

// GenerateGoSchema emits Go code that builds the parsed schema without runtime parsing.
func GenerateGoSchema(packageName, variableName string, schema zenithdb.Schema) ([]byte, error) {
	var buffer bytes.Buffer
	fmt.Fprintf(&buffer, "package %s\n\n", packageName)
	fmt.Fprintf(&buffer, "import zenithdb %q\n\n", "github.com/bypepe77/ZenithDB/pkg/zenithdb")
	fmt.Fprintf(&buffer, "var %s = zenithdb.Schema{\nModels: []zenithdb.Model{\n", variableName)

	for _, model := range schema.Models {
		fmt.Fprintf(&buffer, "{\nName: %q,\n", model.Name)
		writeFields(&buffer, model.Fields)
		writeStringSlice(&buffer, "PrimaryKey", model.PrimaryKey)
		writeIndexes(&buffer, model.Indexes)
		writeRelations(&buffer, model.Relations)
		fmt.Fprintf(&buffer, "},\n")
	}

	fmt.Fprintf(&buffer, "},\n}\n")
	formatted, err := format.Source(buffer.Bytes())
	if err != nil {
		return nil, err
	}
	return formatted, nil
}

func writeFields(buffer *bytes.Buffer, fields []zenithdb.Field) {
	fmt.Fprintf(buffer, "Fields: []zenithdb.Field{\n")
	for _, field := range fields {
		fmt.Fprintf(buffer, "{Name: %q, Kind: zenithdb.%s, Required: %t},\n", field.Name, exportedKind(field.Kind), field.Required)
	}
	fmt.Fprintf(buffer, "},\n")
}

func writeStringSlice(buffer *bytes.Buffer, name string, values []string) {
	fmt.Fprintf(buffer, "%s: []string{%s},\n", name, quotedStrings(values))
}

func writeIndexes(buffer *bytes.Buffer, indexes []zenithdb.Index) {
	fmt.Fprintf(buffer, "Indexes: []zenithdb.Index{\n")
	for _, index := range indexes {
		fmt.Fprintf(buffer, "{Name: %q, Fields: []string{%s}, Unique: %t},\n", index.Name, quotedStrings(index.Fields), index.Unique)
	}
	fmt.Fprintf(buffer, "},\n")
}

func writeRelations(buffer *bytes.Buffer, relations []zenithdb.Relation) {
	fmt.Fprintf(buffer, "Relations: []zenithdb.Relation{\n")
	for _, relation := range relations {
		fmt.Fprintf(
			buffer,
			"{Name: %q, Model: %q, Fields: []string{%s}, References: []string{%s}, Many: %t},\n",
			relation.Name,
			relation.Model,
			quotedStrings(relation.Fields),
			quotedStrings(relation.References),
			relation.Many,
		)
	}
	fmt.Fprintf(buffer, "},\n")
}

func quotedStrings(values []string) string {
	quoted := make([]string, len(values))
	for i, value := range values {
		quoted[i] = fmt.Sprintf("%q", value)
	}
	return strings.Join(quoted, ", ")
}

func exportedKind(kind zenithdb.FieldKind) string {
	switch kind {
	case zenithdb.FieldString:
		return "FieldString"
	case zenithdb.FieldInt64:
		return "FieldInt64"
	case zenithdb.FieldBool:
		return "FieldBool"
	case zenithdb.FieldFloat:
		return "FieldFloat"
	case zenithdb.FieldTime:
		return "FieldTime"
	default:
		return "FieldString"
	}
}
