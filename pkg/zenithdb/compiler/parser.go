package compiler

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/bypepe77/ZenithDB/pkg/zenithdb"
)

var relationRE = regexp.MustCompile(`@relation\s*\(\s*fields\s*:\s*\[([^\]]*)\]\s*,\s*references\s*:\s*\[([^\]]*)\]\s*\)`)

// ParseSchema parses a focused Prisma-like schema subset into ZenithDB metadata.
func ParseSchema(source string) (zenithdb.Schema, error) {
	source = stripComments(source)
	blocks, err := parseModelBlocks(source)
	if err != nil {
		return zenithdb.Schema{}, err
	}

	models := make([]zenithdb.Model, 0, len(blocks))
	for _, block := range blocks {
		model, err := parseModel(block)
		if err != nil {
			return zenithdb.Schema{}, err
		}
		models = append(models, model)
	}
	resolveImplicitRelations(models)

	schema := zenithdb.Schema{Models: models}
	if err := schema.Validate(); err != nil {
		return zenithdb.Schema{}, err
	}
	return schema, nil
}

type modelBlock struct {
	name string
	body string
}

func parseModelBlocks(source string) ([]modelBlock, error) {
	var blocks []modelBlock
	offset := 0
	for {
		idx := strings.Index(source[offset:], "model ")
		if idx < 0 {
			break
		}
		start := offset + idx + len("model ")
		nameEnd := start
		for nameEnd < len(source) && isIdentRune(rune(source[nameEnd])) {
			nameEnd++
		}
		if nameEnd == start {
			return nil, fmt.Errorf("model name is required near byte %d", start)
		}

		open := strings.IndexByte(source[nameEnd:], '{')
		if open < 0 {
			return nil, fmt.Errorf("model %q is missing an opening brace", source[start:nameEnd])
		}
		open += nameEnd
		close, err := matchingBrace(source, open)
		if err != nil {
			return nil, err
		}

		blocks = append(blocks, modelBlock{
			name: strings.TrimSpace(source[start:nameEnd]),
			body: source[open+1 : close],
		})
		offset = close + 1
	}

	if len(blocks) == 0 {
		return nil, fmt.Errorf("schema does not define any models")
	}
	return blocks, nil
}

func parseModel(block modelBlock) (zenithdb.Model, error) {
	model := zenithdb.Model{Name: block.name}
	lines := strings.Split(block.body, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		if strings.HasPrefix(line, "@@index") {
			fields, err := parseBlockAttributeFields(line)
			if err != nil {
				return zenithdb.Model{}, err
			}
			model.Indexes = append(model.Indexes, zenithdb.Index{
				Name:   defaultIndexName(model.Name, fields, false),
				Fields: fields,
			})
			continue
		}

		if strings.HasPrefix(line, "@@unique") {
			fields, err := parseBlockAttributeFields(line)
			if err != nil {
				return zenithdb.Model{}, err
			}
			model.Indexes = append(model.Indexes, zenithdb.Index{
				Name:   defaultIndexName(model.Name, fields, true),
				Fields: fields,
				Unique: true,
			})
			continue
		}

		field, relation, err := parseFieldLine(line)
		if err != nil {
			return zenithdb.Model{}, err
		}
		if relation != nil {
			model.Relations = append(model.Relations, *relation)
			continue
		}

		model.Fields = append(model.Fields, field)
		if strings.Contains(line, "@id") {
			model.PrimaryKey = append(model.PrimaryKey, field.Name)
		}
		if strings.Contains(line, "@unique") {
			model.Indexes = append(model.Indexes, zenithdb.Index{
				Name:   defaultIndexName(model.Name, []string{field.Name}, true),
				Fields: []string{field.Name},
				Unique: true,
			})
		}
	}

	if len(model.PrimaryKey) == 0 {
		model.PrimaryKey = []string{"id"}
	}
	return model, nil
}

func resolveImplicitRelations(models []zenithdb.Model) {
	modelByName := make(map[string]*zenithdb.Model, len(models))
	for i := range models {
		modelByName[models[i].Name] = &models[i]
	}

	for i := range models {
		source := &models[i]
		for j := range source.Relations {
			relation := &source.Relations[j]
			if len(relation.Fields) > 0 || len(relation.References) > 0 {
				continue
			}
			target := modelByName[relation.Model]
			if target == nil {
				continue
			}
			for _, candidate := range target.Relations {
				if candidate.Model != source.Name || len(candidate.Fields) == 0 || len(candidate.References) == 0 {
					continue
				}
				relation.Fields = append([]string(nil), candidate.References...)
				relation.References = append([]string(nil), candidate.Fields...)
				break
			}
		}
	}
}

func parseFieldLine(line string) (zenithdb.Field, *zenithdb.Relation, error) {
	parts := strings.Fields(line)
	if len(parts) < 2 {
		return zenithdb.Field{}, nil, fmt.Errorf("invalid field line %q", line)
	}

	name := parts[0]
	rawType := parts[1]
	if relation := parseRelation(name, rawType, line); relation != nil {
		return zenithdb.Field{}, relation, nil
	}

	kind, ok := mapFieldKind(rawType)
	if !ok {
		return zenithdb.Field{}, nil, fmt.Errorf("unsupported scalar type %q for field %q", rawType, name)
	}

	return zenithdb.Field{
		Name:     name,
		Kind:     kind,
		Required: !strings.HasSuffix(rawType, "?"),
	}, nil, nil
}

func parseRelation(name, rawType, line string) *zenithdb.Relation {
	relationType := strings.TrimSuffix(rawType, "[]")
	relationType = strings.TrimSuffix(relationType, "?")
	if _, ok := mapFieldKind(relationType); ok {
		return nil
	}

	relation := &zenithdb.Relation{
		Name:  name,
		Model: relationType,
		Many:  strings.HasSuffix(rawType, "[]"),
	}
	matches := relationRE.FindStringSubmatch(line)
	if len(matches) == 3 {
		relation.Fields = parseList(matches[1])
		relation.References = parseList(matches[2])
	}
	return relation
}

func parseBlockAttributeFields(line string) ([]string, error) {
	open := strings.IndexByte(line, '[')
	close := strings.IndexByte(line, ']')
	if open < 0 || close < open {
		return nil, fmt.Errorf("invalid block attribute %q", line)
	}
	return parseList(line[open+1 : close]), nil
}

func parseList(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}

func mapFieldKind(raw string) (zenithdb.FieldKind, bool) {
	raw = strings.TrimSuffix(raw, "?")
	switch raw {
	case "String":
		return zenithdb.FieldString, true
	case "Int", "BigInt":
		return zenithdb.FieldInt64, true
	case "Boolean", "Bool":
		return zenithdb.FieldBool, true
	case "Float", "Decimal":
		return zenithdb.FieldFloat, true
	case "DateTime":
		return zenithdb.FieldTime, true
	default:
		return "", false
	}
}

func defaultIndexName(model string, fields []string, unique bool) string {
	kind := "idx"
	if unique {
		kind = "uniq"
	}
	return strings.ToLower(model + "_" + strings.Join(fields, "_") + "_" + kind)
}

func stripComments(source string) string {
	lines := strings.Split(source, "\n")
	for i, line := range lines {
		if idx := strings.Index(line, "//"); idx >= 0 {
			lines[i] = line[:idx]
		}
	}
	return strings.Join(lines, "\n")
}

func matchingBrace(source string, open int) (int, error) {
	depth := 0
	for i := open; i < len(source); i++ {
		switch source[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return i, nil
			}
		}
	}
	return -1, fmt.Errorf("missing closing brace")
}

func isIdentRune(r rune) bool {
	return r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9'
}
