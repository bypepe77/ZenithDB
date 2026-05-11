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
	writeSchemaVariable(&buffer, variableName, schema)

	formatted, err := format.Source(buffer.Bytes())
	if err != nil {
		return nil, err
	}
	return formatted, nil
}

// GenerateGoClient emits a typed Go client backed by the generic engine.
func GenerateGoClient(packageName string, schema zenithdb.Schema) ([]byte, error) {
	var buffer bytes.Buffer
	fmt.Fprintf(&buffer, "package %s\n\n", packageName)
	if schemaUsesTime(schema) {
		fmt.Fprintf(&buffer, "import (\n%q\n%q\nzenithdb %q\nremote %q\n)\n\n", "context", "time", "github.com/bypepe77/ZenithDB/pkg/zenithdb", "github.com/bypepe77/ZenithDB/pkg/zenithdb/remote")
	} else {
		fmt.Fprintf(&buffer, "import (\n%q\nzenithdb %q\nremote %q\n)\n\n", "context", "github.com/bypepe77/ZenithDB/pkg/zenithdb", "github.com/bypepe77/ZenithDB/pkg/zenithdb/remote")
	}
	writeSchemaVariable(&buffer, "Schema", schema)
	writeClient(&buffer, schema)
	for _, model := range schema.Models {
		writeModelTypes(&buffer, schema, model)
		writeModelStore(&buffer, model)
		writeModelClient(&buffer, model)
	}

	formatted, err := format.Source(buffer.Bytes())
	if err != nil {
		return nil, err
	}
	return formatted, nil
}

func writeSchemaVariable(buffer *bytes.Buffer, variableName string, schema zenithdb.Schema) {
	fmt.Fprintf(buffer, "var %s = zenithdb.Schema{\nModels: []zenithdb.Model{\n", variableName)

	for _, model := range schema.Models {
		fmt.Fprintf(buffer, "{\nName: %q,\n", model.Name)
		writeFields(buffer, model.Fields)
		writeStringSlice(buffer, "PrimaryKey", model.PrimaryKey)
		writeIndexes(buffer, model.Indexes)
		writeRelations(buffer, model.Relations)
		fmt.Fprintf(buffer, "},\n")
	}

	fmt.Fprintf(buffer, "},\n}\n\n")
}

func writeClient(buffer *bytes.Buffer, schema zenithdb.Schema) {
	fmt.Fprintf(buffer, "type engine interface {\nCreate(context.Context, string, zenithdb.Record) (zenithdb.MutationResult, error)\nUpdate(context.Context, string, map[string]any, zenithdb.Record) (zenithdb.Record, error)\nDelete(context.Context, string, map[string]any) (zenithdb.Record, error)\nFindUnique(context.Context, string, map[string]any, map[string]zenithdb.Include) (zenithdb.Record, bool, error)\nFindMany(context.Context, string, zenithdb.Query) ([]zenithdb.Record, error)\nClose() error\n}\n\n")
	fmt.Fprintf(buffer, "type Client struct {\ndb engine\nremote bool\n")
	for _, model := range schema.Models {
		fmt.Fprintf(buffer, "%s *%sStore\n", lowerIdentifier(model.Name), lowerIdentifier(model.Name))
		fmt.Fprintf(buffer, "%s %sClient\n", model.Name, model.Name)
	}
	fmt.Fprintf(buffer, "}\n\n")

	fmt.Fprintf(buffer, "func Open(ctx context.Context, options zenithdb.Options) (*Client, error) {\n")
	fmt.Fprintf(buffer, "db, err := zenithdb.Open(ctx, Schema, options)\nif err != nil {\nreturn nil, err\n}\n")
	fmt.Fprintf(buffer, "return newClientFromEngine(ctx, db, true, false)\n}\n\n")
	fmt.Fprintf(buffer, "func OpenURL(ctx context.Context, connectionURL string) (*Client, error) {\noptions, err := zenithdb.ParseConnectionURL(connectionURL)\nif err != nil {\nreturn nil, err\n}\nif options.WireURL != \"\" {\nschemaHash, err := Schema.Hash()\nif err != nil {\nreturn nil, err\n}\ndb, err := remote.OpenWithOptions(ctx, remote.OpenOptions{ConnectionURL: connectionURL, SchemaHash: schemaHash})\nif err != nil {\nreturn nil, err\n}\nreturn newClientFromEngine(ctx, db, false, true)\n}\nreturn Open(ctx, zenithdb.Options{ConnectionURL: connectionURL})\n}\n\n")
	fmt.Fprintf(buffer, "func (c *Client) Close() error {\nreturn c.db.Close()\n}\n\n")
	fmt.Fprintf(buffer, "func newClientFromEngine(ctx context.Context, db engine, preload bool, remote bool) (*Client, error) {\nclient := &Client{db: db, remote: remote}\n")
	for _, model := range schema.Models {
		fmt.Fprintf(buffer, "client.%s = new%sStore()\n", lowerIdentifier(model.Name), model.Name)
	}
	fmt.Fprintf(buffer, "if preload {\n")
	for _, model := range schema.Models {
		fmt.Fprintf(buffer, "if err := client.load%s(ctx); err != nil {\n_ = db.Close()\nreturn nil, err\n}\n", model.Name)
	}
	fmt.Fprintf(buffer, "}\n")
	for _, model := range schema.Models {
		fmt.Fprintf(buffer, "client.%s = %sClient{client: client}\n", model.Name, model.Name)
	}
	fmt.Fprintf(buffer, "return client, nil\n}\n\n")
	for _, model := range schema.Models {
		fmt.Fprintf(buffer, "func (c *Client) load%s(ctx context.Context) error {\nrecords, err := c.db.FindMany(ctx, %q, zenithdb.Query{})\nif err != nil {\nreturn err\n}\nfor _, record := range records {\nc.%s.put(recordTo%s(record))\n}\nreturn nil\n}\n\n", model.Name, model.Name, lowerIdentifier(model.Name), model.Name)
	}
	for _, model := range schema.Models {
		writeIncludeExpander(buffer, schema, model)
	}
}

func writeModelTypes(buffer *bytes.Buffer, schema zenithdb.Schema, model zenithdb.Model) {
	fmt.Fprintf(buffer, "type %s struct {\n", model.Name)
	for _, field := range model.Fields {
		fmt.Fprintf(buffer, "%s %s `json:%q`\n", exportedIdentifier(field.Name), goType(field.Kind), field.Name)
	}
	for _, relation := range model.Relations {
		if relation.Many {
			fmt.Fprintf(buffer, "%s []%s `json:%q`\n", exportedIdentifier(relation.Name), relation.Model, relation.Name)
		} else {
			fmt.Fprintf(buffer, "%s *%s `json:%q`\n", exportedIdentifier(relation.Name), relation.Model, relation.Name)
		}
	}
	fmt.Fprintf(buffer, "}\n\n")

	fmt.Fprintf(buffer, "type %sCreateInput struct {\n", model.Name)
	for _, field := range model.Fields {
		fmt.Fprintf(buffer, "%s %s\n", exportedIdentifier(field.Name), goType(field.Kind))
	}
	fmt.Fprintf(buffer, "}\n\n")

	fmt.Fprintf(buffer, "type %sUpdateInput struct {\n", model.Name)
	for _, field := range model.Fields {
		if isPrimaryField(model, field.Name) {
			continue
		}
		fmt.Fprintf(buffer, "%s *%s\n", exportedIdentifier(field.Name), goType(field.Kind))
	}
	fmt.Fprintf(buffer, "}\n\n")

	fmt.Fprintf(buffer, "func (input %sCreateInput) record() zenithdb.Record {\nreturn zenithdb.Record{\n", model.Name)
	for _, field := range model.Fields {
		fmt.Fprintf(buffer, "%q: input.%s,\n", field.Name, exportedIdentifier(field.Name))
	}
	fmt.Fprintf(buffer, "}\n}\n\n")

	fmt.Fprintf(buffer, "func (input %sUpdateInput) record() zenithdb.Record {\nrecord := zenithdb.Record{}\n", model.Name)
	for _, field := range model.Fields {
		if isPrimaryField(model, field.Name) {
			continue
		}
		fmt.Fprintf(buffer, "if input.%s != nil {\nrecord[%q] = *input.%s\n}\n", exportedIdentifier(field.Name), field.Name, exportedIdentifier(field.Name))
	}
	fmt.Fprintf(buffer, "return record\n}\n\n")

	writeWhereTypes(buffer, model)
	writeIncludeType(buffer, model)

	fmt.Fprintf(buffer, "func recordTo%s(record zenithdb.Record) %s {\nresult := %s{\n", model.Name, model.Name, model.Name)
	for _, field := range model.Fields {
		fmt.Fprintf(buffer, "%s: record[%q].(%s),\n", exportedIdentifier(field.Name), field.Name, goType(field.Kind))
	}
	fmt.Fprintf(buffer, "}\n")
	for _, relation := range model.Relations {
		if !modelExists(schema, relation.Model) {
			continue
		}
		if relation.Many {
			fmt.Fprintf(buffer, "if raw, ok := record[%q].([]zenithdb.Record); ok {\nresult.%s = make([]%s, 0, len(raw))\nfor _, item := range raw {\nconverted := recordTo%s(item)\nresult.%s = append(result.%s, converted)\n}\n}\n", relation.Name, exportedIdentifier(relation.Name), relation.Model, relation.Model, exportedIdentifier(relation.Name), exportedIdentifier(relation.Name))
		} else {
			fmt.Fprintf(buffer, "if raw, ok := record[%q].(zenithdb.Record); ok {\nconverted := recordTo%s(raw)\nresult.%s = &converted\n}\n", relation.Name, relation.Model, exportedIdentifier(relation.Name))
		}
	}
	fmt.Fprintf(buffer, "return result\n}\n\n")
}

func writeModelClient(buffer *bytes.Buffer, model zenithdb.Model) {
	fmt.Fprintf(buffer, "type %sClient struct {\nclient *Client\n}\n\n", model.Name)
	fmt.Fprintf(buffer, "func (c %sClient) Create(ctx context.Context, input %sCreateInput) (%s, error) {\n", model.Name, model.Name, model.Name)
	fmt.Fprintf(buffer, "_, err := c.client.db.Create(ctx, %q, input.record())\nif err != nil {\nreturn %s{}, err\n}\nrecord := recordTo%s(input.record())\nc.client.%s.put(record)\nreturn record, nil\n}\n\n", model.Name, model.Name, model.Name, lowerIdentifier(model.Name))
	writePrismaLikeMethods(buffer, model)

	written := make(map[string]struct{})
	writeUniqueMethods(buffer, model, model.PrimaryKey, written)
	for _, index := range model.Indexes {
		if index.Unique {
			writeUniqueMethods(buffer, model, index.Fields, written)
		} else {
			writeFindManyMethod(buffer, model, index)
		}
	}
}

func writeModelStore(buffer *bytes.Buffer, model zenithdb.Model) {
	pk, ok := primaryField(model)
	if !ok {
		return
	}
	store := lowerIdentifier(model.Name) + "Store"
	fmt.Fprintf(buffer, "type %s struct {\nby%s map[%s]%s\n", store, exportedIdentifier(pk.Name), goType(pk.Kind), model.Name)
	for _, index := range model.Indexes {
		if len(index.Fields) != 1 {
			continue
		}
		field, ok := findField(model, index.Fields[0])
		if !ok {
			continue
		}
		if index.Unique {
			fmt.Fprintf(buffer, "by%s map[%s]%s\n", exportedIdentifier(field.Name), goType(field.Kind), goType(pk.Kind))
		} else {
			fmt.Fprintf(buffer, "by%s map[%s][]%s\n", exportedIdentifier(field.Name), goType(field.Kind), goType(pk.Kind))
		}
	}
	fmt.Fprintf(buffer, "}\n\n")

	fmt.Fprintf(buffer, "func new%sStore() *%s {\nreturn &%s{\nby%s: make(map[%s]%s),\n", model.Name, store, store, exportedIdentifier(pk.Name), goType(pk.Kind), model.Name)
	for _, index := range model.Indexes {
		if len(index.Fields) != 1 {
			continue
		}
		field, ok := findField(model, index.Fields[0])
		if !ok {
			continue
		}
		if index.Unique {
			fmt.Fprintf(buffer, "by%s: make(map[%s]%s),\n", exportedIdentifier(field.Name), goType(field.Kind), goType(pk.Kind))
		} else {
			fmt.Fprintf(buffer, "by%s: make(map[%s][]%s),\n", exportedIdentifier(field.Name), goType(field.Kind), goType(pk.Kind))
		}
	}
	fmt.Fprintf(buffer, "}\n}\n\n")

	fmt.Fprintf(buffer, "func (s *%s) put(record %s) {\ns.by%s[record.%s] = record\n", store, model.Name, exportedIdentifier(pk.Name), exportedIdentifier(pk.Name))
	for _, index := range model.Indexes {
		if len(index.Fields) != 1 {
			continue
		}
		field, ok := findField(model, index.Fields[0])
		if !ok {
			continue
		}
		if index.Unique {
			fmt.Fprintf(buffer, "s.by%s[record.%s] = record.%s\n", exportedIdentifier(field.Name), exportedIdentifier(field.Name), exportedIdentifier(pk.Name))
		} else {
			fmt.Fprintf(buffer, "s.by%s[record.%s] = append(s.by%s[record.%s], record.%s)\n", exportedIdentifier(field.Name), exportedIdentifier(field.Name), exportedIdentifier(field.Name), exportedIdentifier(field.Name), exportedIdentifier(pk.Name))
		}
	}
	fmt.Fprintf(buffer, "}\n\n")

	fmt.Fprintf(buffer, "func (s *%s) remove(record %s) {\ndelete(s.by%s, record.%s)\n", store, model.Name, exportedIdentifier(pk.Name), exportedIdentifier(pk.Name))
	for _, index := range model.Indexes {
		if len(index.Fields) != 1 {
			continue
		}
		field, ok := findField(model, index.Fields[0])
		if !ok {
			continue
		}
		if index.Unique {
			fmt.Fprintf(buffer, "delete(s.by%s, record.%s)\n", exportedIdentifier(field.Name), exportedIdentifier(field.Name))
		} else {
			fmt.Fprintf(buffer, "ids := s.by%s[record.%s]\nfor i, id := range ids {\nif id == record.%s {\nids = append(ids[:i], ids[i+1:]...)\nbreak\n}\n}\nif len(ids) == 0 {\ndelete(s.by%s, record.%s)\n} else {\ns.by%s[record.%s] = ids\n}\n", exportedIdentifier(field.Name), exportedIdentifier(field.Name), exportedIdentifier(pk.Name), exportedIdentifier(field.Name), exportedIdentifier(field.Name), exportedIdentifier(field.Name), exportedIdentifier(field.Name))
		}
	}
	fmt.Fprintf(buffer, "}\n\n")

	fmt.Fprintf(buffer, "func (s *%s) replace(previous %s, next %s) {\ns.remove(previous)\ns.put(next)\n}\n\n", store, model.Name, model.Name)

	writeStoreFindByField(buffer, model, pk, true)
	for _, index := range model.Indexes {
		if len(index.Fields) != 1 {
			continue
		}
		field, ok := findField(model, index.Fields[0])
		if !ok {
			continue
		}
		if index.Unique {
			writeStoreFindByField(buffer, model, field, true)
		} else {
			writeStoreFindManyByField(buffer, model, field)
		}
	}
}

func writeIncludeExpander(buffer *bytes.Buffer, schema zenithdb.Schema, model zenithdb.Model) {
	fmt.Fprintf(buffer, "func (c *Client) include%s(record *%s, include *%sInclude) {\nif include == nil {\nreturn\n}\n", model.Name, model.Name, model.Name)
	for _, relation := range model.Relations {
		if len(relation.Fields) != 1 || len(relation.References) != 1 {
			continue
		}
		localField, ok := findField(model, relation.Fields[0])
		if !ok {
			continue
		}
		target, ok := findModel(schema, relation.Model)
		if !ok {
			continue
		}
		referenceField, ok := findField(target, relation.References[0])
		if !ok {
			continue
		}
		if relation.Many {
			if !hasNonUniqueSingleFieldIndex(target, referenceField.Name) {
				continue
			}
			fmt.Fprintf(buffer, "if include.%s {\nrecord.%s = c.%s.findManyBy%s(record.%s, 0)\n}\n", exportedIdentifier(relation.Name), exportedIdentifier(relation.Name), lowerIdentifier(target.Name), exportedIdentifier(referenceField.Name), exportedIdentifier(localField.Name))
			continue
		}
		if !isUniqueLookupField(target, referenceField.Name) {
			continue
		}
		fmt.Fprintf(buffer, "if include.%s {\nrelated, ok := c.%s.findBy%s(record.%s)\nif ok {\nrecord.%s = &related\n}\n}\n", exportedIdentifier(relation.Name), lowerIdentifier(target.Name), exportedIdentifier(referenceField.Name), exportedIdentifier(localField.Name), exportedIdentifier(relation.Name))
	}
	fmt.Fprintf(buffer, "}\n\n")
}

func writeStoreFindByField(buffer *bytes.Buffer, model zenithdb.Model, field zenithdb.Field, unique bool) {
	pk, ok := primaryField(model)
	if !ok {
		return
	}
	store := lowerIdentifier(model.Name) + "Store"
	method := "findBy" + exportedIdentifier(field.Name)
	if field.Name == pk.Name {
		fmt.Fprintf(buffer, "func (s *%s) %s(value %s) (%s, bool) {\nrecord, ok := s.by%s[value]\nreturn record, ok\n}\n\n", store, method, goType(field.Kind), model.Name, exportedIdentifier(pk.Name))
		return
	}
	fmt.Fprintf(buffer, "func (s *%s) %s(value %s) (%s, bool) {\nprimaryKey, ok := s.by%s[value]\nif !ok {\nreturn %s{}, false\n}\nreturn s.findBy%s(primaryKey)\n}\n\n", store, method, goType(field.Kind), model.Name, exportedIdentifier(field.Name), model.Name, exportedIdentifier(pk.Name))
}

func writeStoreFindManyByField(buffer *bytes.Buffer, model zenithdb.Model, field zenithdb.Field) {
	pk, ok := primaryField(model)
	if !ok {
		return
	}
	store := lowerIdentifier(model.Name) + "Store"
	fmt.Fprintf(buffer, "func (s *%s) findManyBy%s(value %s, limit int) []%s {\nids := s.by%s[value]\nif limit > 0 && len(ids) > limit {\nids = ids[:limit]\n}\nresult := make([]%s, 0, len(ids))\nfor _, id := range ids {\nif record, ok := s.findBy%s(id); ok {\nresult = append(result, record)\n}\n}\nreturn result\n}\n\n", store, exportedIdentifier(field.Name), goType(field.Kind), model.Name, exportedIdentifier(field.Name), model.Name, exportedIdentifier(pk.Name))
}

func writeWhereTypes(buffer *bytes.Buffer, model zenithdb.Model) {
	fmt.Fprintf(buffer, "type %sWhereUniqueInput struct {\n", model.Name)
	uniqueFields := uniqueLookupFields(model)
	for _, field := range uniqueFields {
		fmt.Fprintf(buffer, "%s %s\n", exportedIdentifier(field.Name), goType(field.Kind))
	}
	fmt.Fprintf(buffer, "}\n\n")

	fmt.Fprintf(buffer, "func (input %sWhereUniqueInput) where() map[string]any {\n", model.Name)
	for _, field := range uniqueFields {
		fmt.Fprintf(buffer, "if input.%s != %s {\nreturn map[string]any{%q: input.%s}\n}\n", exportedIdentifier(field.Name), zeroValue(field.Kind), field.Name, exportedIdentifier(field.Name))
	}
	fmt.Fprintf(buffer, "return nil\n}\n\n")

	fmt.Fprintf(buffer, "type %sWhereInput struct {\n", model.Name)
	for _, field := range model.Fields {
		fmt.Fprintf(buffer, "%s *%s\n", exportedIdentifier(field.Name), goType(field.Kind))
	}
	fmt.Fprintf(buffer, "}\n\n")

	fmt.Fprintf(buffer, "func (input %sWhereInput) where() map[string]any {\nwhere := make(map[string]any)\n", model.Name)
	for _, field := range model.Fields {
		fmt.Fprintf(buffer, "if input.%s != nil {\nwhere[%q] = *input.%s\n}\n", exportedIdentifier(field.Name), field.Name, exportedIdentifier(field.Name))
	}
	fmt.Fprintf(buffer, "return where\n}\n\n")

	fmt.Fprintf(buffer, "func (input %sWhereInput) index() string {\n", model.Name)
	for _, index := range model.Indexes {
		if len(index.Fields) == 1 {
			field, ok := findField(model, index.Fields[0])
			if ok {
				fmt.Fprintf(buffer, "if input.%s != nil {\nreturn %q\n}\n", exportedIdentifier(field.Name), index.Name)
			}
		}
	}
	fmt.Fprintf(buffer, "return \"\"\n}\n\n")
}

func writeIncludeType(buffer *bytes.Buffer, model zenithdb.Model) {
	fmt.Fprintf(buffer, "type %sInclude struct {\n", model.Name)
	for _, relation := range model.Relations {
		fmt.Fprintf(buffer, "%s bool\n", exportedIdentifier(relation.Name))
	}
	fmt.Fprintf(buffer, "}\n\n")

	fmt.Fprintf(buffer, "func (input *%sInclude) include() map[string]zenithdb.Include {\nif input == nil {\nreturn nil\n}\ninclude := make(map[string]zenithdb.Include)\n", model.Name)
	for _, relation := range model.Relations {
		fmt.Fprintf(buffer, "if input.%s {\ninclude[%q] = zenithdb.Include{}\n}\n", exportedIdentifier(relation.Name), relation.Name)
	}
	fmt.Fprintf(buffer, "return include\n}\n\n")

	fmt.Fprintf(buffer, "type %sFindUniqueArgs struct {\nWhere %sWhereUniqueInput\nInclude *%sInclude\n}\n\n", model.Name, model.Name, model.Name)
	fmt.Fprintf(buffer, "type %sFindManyArgs struct {\nWhere %sWhereInput\nInclude *%sInclude\nTake int\n}\n\n", model.Name, model.Name, model.Name)
	fmt.Fprintf(buffer, "type %sUpdateArgs struct {\nWhere %sWhereUniqueInput\nData %sUpdateInput\nInclude *%sInclude\n}\n\n", model.Name, model.Name, model.Name, model.Name)
	fmt.Fprintf(buffer, "type %sDeleteArgs struct {\nWhere %sWhereUniqueInput\nInclude *%sInclude\n}\n\n", model.Name, model.Name, model.Name)
}

func writePrismaLikeMethods(buffer *bytes.Buffer, model zenithdb.Model) {
	fmt.Fprintf(buffer, "func (c %sClient) FindUnique(ctx context.Context, args %sFindUniqueArgs) (%s, bool, error) {\n", model.Name, model.Name, model.Name)
	fmt.Fprintf(buffer, "if c.client.remote {\nwhere := args.Where.where()\nif where == nil {\nreturn %s{}, false, nil\n}\nrecord, ok, err := c.client.db.FindUnique(ctx, %q, where, args.Include.include())\nif err != nil || !ok {\nreturn %s{}, ok, err\n}\nreturn recordTo%s(record), true, nil\n}\n", model.Name, model.Name, model.Name, model.Name)
	for _, field := range uniqueLookupFields(model) {
		fmt.Fprintf(buffer, "if args.Where.%s != %s {\nrecord, ok := c.client.%s.findBy%s(args.Where.%s)\nif !ok {\nreturn %s{}, false, nil\n}\nc.client.include%s(&record, args.Include)\nreturn record, true, nil\n}\n", exportedIdentifier(field.Name), zeroValue(field.Kind), lowerIdentifier(model.Name), exportedIdentifier(field.Name), exportedIdentifier(field.Name), model.Name, model.Name)
	}
	fmt.Fprintf(buffer, "return %s{}, false, nil\n}\n\n", model.Name)

	fmt.Fprintf(buffer, "func (c %sClient) FindMany(ctx context.Context, args %sFindManyArgs) ([]%s, error) {\n", model.Name, model.Name, model.Name)
	fmt.Fprintf(buffer, "if c.client.remote {\nrecords, err := c.client.db.FindMany(ctx, %q, zenithdb.Query{Where: args.Where.where(), Index: args.Where.index(), Include: args.Include.include(), Limit: args.Take})\nif err != nil {\nreturn nil, err\n}\nresult := make([]%s, 0, len(records))\nfor _, record := range records {\nresult = append(result, recordTo%s(record))\n}\nreturn result, nil\n}\n", model.Name, model.Name, model.Name)
	pk, hasPK := primaryField(model)
	if hasPK {
		fmt.Fprintf(buffer, "if args.Where.%s != nil {\nrecord, ok := c.client.%s.findBy%s(*args.Where.%s)\nif !ok {\nreturn nil, nil\n}\nc.client.include%s(&record, args.Include)\nreturn []%s{record}, nil\n}\n", exportedIdentifier(pk.Name), lowerIdentifier(model.Name), exportedIdentifier(pk.Name), exportedIdentifier(pk.Name), model.Name, model.Name)
	}
	for _, index := range model.Indexes {
		if len(index.Fields) != 1 {
			continue
		}
		field, ok := findField(model, index.Fields[0])
		if !ok {
			continue
		}
		if index.Unique {
			fmt.Fprintf(buffer, "if args.Where.%s != nil {\nrecord, ok := c.client.%s.findBy%s(*args.Where.%s)\nif !ok {\nreturn nil, nil\n}\nc.client.include%s(&record, args.Include)\nreturn []%s{record}, nil\n}\n", exportedIdentifier(field.Name), lowerIdentifier(model.Name), exportedIdentifier(field.Name), exportedIdentifier(field.Name), model.Name, model.Name)
		} else {
			fmt.Fprintf(buffer, "if args.Where.%s != nil {\nresult := c.client.%s.findManyBy%s(*args.Where.%s, args.Take)\nfor i := range result {\nc.client.include%s(&result[i], args.Include)\n}\nreturn result, nil\n}\n", exportedIdentifier(field.Name), lowerIdentifier(model.Name), exportedIdentifier(field.Name), exportedIdentifier(field.Name), model.Name)
		}
	}
	fmt.Fprintf(buffer, "records, err := c.client.db.FindMany(ctx, %q, zenithdb.Query{Where: args.Where.where(), Index: args.Where.index(), Include: args.Include.include(), Limit: args.Take})\n", model.Name)
	fmt.Fprintf(buffer, "if err != nil {\nreturn nil, err\n}\nresult := make([]%s, 0, len(records))\nfor _, record := range records {\nconverted := recordTo%s(record)\nc.client.include%s(&converted, args.Include)\nresult = append(result, converted)\n}\nreturn result, nil\n}\n\n", model.Name, model.Name, model.Name)

	fmt.Fprintf(buffer, "func (c %sClient) Update(ctx context.Context, args %sUpdateArgs) (%s, bool, error) {\n", model.Name, model.Name, model.Name)
	fmt.Fprintf(buffer, "if c.client.remote {\nupdatedRecord, err := c.client.db.Update(ctx, %q, args.Where.where(), args.Data.record())\nif err != nil {\nreturn %s{}, false, err\n}\nif args.Include != nil {\nrecord, ok, err := c.client.db.FindUnique(ctx, %q, args.Where.where(), args.Include.include())\nif err != nil || !ok {\nreturn %s{}, ok, err\n}\nreturn recordTo%s(record), true, nil\n}\nreturn recordTo%s(updatedRecord), true, nil\n}\n", model.Name, model.Name, model.Name, model.Name, model.Name, model.Name)
	fmt.Fprintf(buffer, "previous, ok, err := c.FindUnique(ctx, %sFindUniqueArgs{Where: args.Where})\nif err != nil || !ok {\nreturn %s{}, ok, err\n}\nupdatedRecord, err := c.client.db.Update(ctx, %q, args.Where.where(), args.Data.record())\nif err != nil {\nreturn %s{}, false, err\n}\nupdated := recordTo%s(updatedRecord)\nc.client.%s.replace(previous, updated)\nc.client.include%s(&updated, args.Include)\nreturn updated, true, nil\n}\n\n", model.Name, model.Name, model.Name, model.Name, model.Name, lowerIdentifier(model.Name), model.Name)

	fmt.Fprintf(buffer, "func (c %sClient) Delete(ctx context.Context, args %sDeleteArgs) (%s, bool, error) {\n", model.Name, model.Name, model.Name)
	fmt.Fprintf(buffer, "if c.client.remote {\nprevious, ok, err := c.FindUnique(ctx, %sFindUniqueArgs{Where: args.Where, Include: args.Include})\nif err != nil || !ok {\nreturn %s{}, ok, err\n}\n_, err = c.client.db.Delete(ctx, %q, args.Where.where())\nif err != nil {\nreturn %s{}, false, err\n}\nreturn previous, true, nil\n}\n", model.Name, model.Name, model.Name, model.Name)
	fmt.Fprintf(buffer, "previous, ok, err := c.FindUnique(ctx, %sFindUniqueArgs{Where: args.Where})\nif err != nil || !ok {\nreturn %s{}, ok, err\n}\n_, err = c.client.db.Delete(ctx, %q, args.Where.where())\nif err != nil {\nreturn %s{}, false, err\n}\nc.client.%s.remove(previous)\nc.client.include%s(&previous, args.Include)\nreturn previous, true, nil\n}\n\n", model.Name, model.Name, model.Name, model.Name, lowerIdentifier(model.Name), model.Name)
}

func writeUniqueMethods(buffer *bytes.Buffer, model zenithdb.Model, fields []string, written map[string]struct{}) {
	if len(fields) != 1 {
		return
	}
	field, ok := findField(model, fields[0])
	if !ok {
		return
	}
	method := "FindUniqueBy" + exportedIdentifier(field.Name)
	if _, ok := written[method]; ok {
		return
	}
	written[method] = struct{}{}

	fmt.Fprintf(buffer, "func (c %sClient) %s(ctx context.Context, value %s) (%s, bool, error) {\n", model.Name, method, goType(field.Kind), model.Name)
	fmt.Fprintf(buffer, "if c.client.remote {\nrecord, ok, err := c.client.db.FindUnique(ctx, %q, map[string]any{%q: value}, nil)\nif err != nil || !ok {\nreturn %s{}, ok, err\n}\nreturn recordTo%s(record), true, nil\n}\n", model.Name, field.Name, model.Name, model.Name)
	fmt.Fprintf(buffer, "record, ok := c.client.%s.findBy%s(value)\nreturn record, ok, nil\n}\n\n", lowerIdentifier(model.Name), exportedIdentifier(field.Name))
}

func writeFindManyMethod(buffer *bytes.Buffer, model zenithdb.Model, index zenithdb.Index) {
	if len(index.Fields) != 1 {
		return
	}
	field, ok := findField(model, index.Fields[0])
	if !ok {
		return
	}
	method := "FindManyBy" + exportedIdentifier(field.Name)

	fmt.Fprintf(buffer, "func (c %sClient) %s(ctx context.Context, value %s, limit int) ([]%s, error) {\n", model.Name, method, goType(field.Kind), model.Name)
	fmt.Fprintf(buffer, "if c.client.remote {\nrecords, err := c.client.db.FindMany(ctx, %q, zenithdb.Query{Where: map[string]any{%q: value}, Index: %q, Limit: limit})\nif err != nil {\nreturn nil, err\n}\nresult := make([]%s, 0, len(records))\nfor _, record := range records {\nresult = append(result, recordTo%s(record))\n}\nreturn result, nil\n}\n", model.Name, field.Name, index.Name, model.Name, model.Name)
	fmt.Fprintf(buffer, "return c.client.%s.findManyBy%s(value, limit), nil\n}\n\n", lowerIdentifier(model.Name), exportedIdentifier(field.Name))
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

func findField(model zenithdb.Model, name string) (zenithdb.Field, bool) {
	for _, field := range model.Fields {
		if field.Name == name {
			return field, true
		}
	}
	return zenithdb.Field{}, false
}

func findModel(schema zenithdb.Schema, name string) (zenithdb.Model, bool) {
	for _, model := range schema.Models {
		if model.Name == name {
			return model, true
		}
	}
	return zenithdb.Model{}, false
}

func primaryField(model zenithdb.Model) (zenithdb.Field, bool) {
	if len(model.PrimaryKey) != 1 {
		return zenithdb.Field{}, false
	}
	return findField(model, model.PrimaryKey[0])
}

func isUniqueLookupField(model zenithdb.Model, fieldName string) bool {
	if len(model.PrimaryKey) == 1 && model.PrimaryKey[0] == fieldName {
		return true
	}
	for _, index := range model.Indexes {
		if index.Unique && len(index.Fields) == 1 && index.Fields[0] == fieldName {
			return true
		}
	}
	return false
}

func isPrimaryField(model zenithdb.Model, fieldName string) bool {
	for _, primary := range model.PrimaryKey {
		if primary == fieldName {
			return true
		}
	}
	return false
}

func hasNonUniqueSingleFieldIndex(model zenithdb.Model, fieldName string) bool {
	for _, index := range model.Indexes {
		if !index.Unique && len(index.Fields) == 1 && index.Fields[0] == fieldName {
			return true
		}
	}
	return false
}

func uniqueLookupFields(model zenithdb.Model) []zenithdb.Field {
	seen := make(map[string]struct{})
	var result []zenithdb.Field
	for _, name := range model.PrimaryKey {
		field, ok := findField(model, name)
		if !ok {
			continue
		}
		seen[field.Name] = struct{}{}
		result = append(result, field)
	}
	for _, index := range model.Indexes {
		if !index.Unique || len(index.Fields) != 1 {
			continue
		}
		field, ok := findField(model, index.Fields[0])
		if !ok {
			continue
		}
		if _, ok := seen[field.Name]; ok {
			continue
		}
		seen[field.Name] = struct{}{}
		result = append(result, field)
	}
	return result
}

func modelExists(schema zenithdb.Schema, name string) bool {
	for _, model := range schema.Models {
		if model.Name == name {
			return true
		}
	}
	return false
}

func goType(kind zenithdb.FieldKind) string {
	switch kind {
	case zenithdb.FieldString:
		return "string"
	case zenithdb.FieldInt64:
		return "int64"
	case zenithdb.FieldBool:
		return "bool"
	case zenithdb.FieldFloat:
		return "float64"
	case zenithdb.FieldTime:
		return "time.Time"
	default:
		return "string"
	}
}

func zeroValue(kind zenithdb.FieldKind) string {
	switch kind {
	case zenithdb.FieldString:
		return `""`
	case zenithdb.FieldInt64:
		return "0"
	case zenithdb.FieldBool:
		return "false"
	case zenithdb.FieldFloat:
		return "0"
	case zenithdb.FieldTime:
		return "time.Time{}"
	default:
		return `""`
	}
}

func exportedIdentifier(name string) string {
	if strings.EqualFold(name, "id") {
		return "ID"
	}
	parts := splitIdentifier(name)
	for i, part := range parts {
		if strings.EqualFold(part, "id") {
			parts[i] = "ID"
			continue
		}
		parts[i] = strings.ToUpper(part[:1]) + part[1:]
	}
	return strings.Join(parts, "")
}

func lowerIdentifier(name string) string {
	exported := exportedIdentifier(name)
	if exported == "" {
		return ""
	}
	if strings.HasPrefix(exported, "ID") {
		return "id" + exported[2:]
	}
	return strings.ToLower(exported[:1]) + exported[1:]
}

func splitIdentifier(name string) []string {
	var parts []string
	start := 0
	for i, r := range name {
		if i > 0 && r >= 'A' && r <= 'Z' {
			parts = append(parts, name[start:i])
			start = i
		}
	}
	parts = append(parts, name[start:])
	return parts
}

func schemaUsesTime(schema zenithdb.Schema) bool {
	for _, model := range schema.Models {
		for _, field := range model.Fields {
			if field.Kind == zenithdb.FieldTime {
				return true
			}
		}
	}
	return false
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
