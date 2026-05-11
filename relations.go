package zenithdb

import "fmt"

func (db *DB) expandIncludesLocked(modelName string, record Record, includes map[string]Include) error {
	table, err := db.table(modelName)
	if err != nil {
		return err
	}

	relations := make(map[string]Relation, len(table.model.Relations))
	for _, relation := range table.model.Relations {
		relations[relation.Name] = relation
	}

	for name, include := range includes {
		relation, ok := relations[name]
		if !ok {
			return fmt.Errorf("model %q does not define relation %q", modelName, name)
		}

		relatedTable, err := db.table(relation.Model)
		if err != nil {
			return err
		}

		where := make(map[string]any, len(relation.References))
		for i, field := range relation.Fields {
			value, ok := record[field]
			if !ok {
				return fmt.Errorf("model %q relation %q missing local field %q", modelName, name, field)
			}
			where[relation.References[i]] = value
		}

		if relation.Many {
			records, err := relatedTable.findMany(Query{Where: where, Limit: include.Limit})
			if err != nil {
				return err
			}
			record[name] = records
			continue
		}

		related, ok, err := db.findUniqueLocked(relatedTable, where)
		if err != nil {
			return err
		}
		if ok {
			record[name] = related
		} else {
			record[name] = nil
		}
	}

	return nil
}
