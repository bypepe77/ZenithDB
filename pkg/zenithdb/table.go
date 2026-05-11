package zenithdb

import "fmt"

type table struct {
	model   Model
	rows    map[string]Record
	indexes map[string]*secondaryIndex
}

func newTable(model Model) *table {
	indexes := make(map[string]*secondaryIndex, len(model.Indexes))
	for _, index := range model.Indexes {
		indexes[index.Name] = newSecondaryIndex(index)
	}
	return &table{
		model:   model,
		rows:    make(map[string]Record),
		indexes: indexes,
	}
}

func (t *table) insert(record Record) (string, error) {
	normalized, primaryKey, err := t.prepareInsert(record)
	if err != nil {
		return "", err
	}

	t.insertPrepared(normalized, primaryKey)
	return primaryKey, nil
}

func (t *table) prepareInsert(record Record) (Record, string, error) {
	normalized, err := normalizeRecord(t.model, record)
	if err != nil {
		return nil, "", err
	}

	primaryKey, err := keyFromRecord(normalized, t.model.PrimaryKey)
	if err != nil {
		return nil, "", err
	}
	if _, ok := t.rows[primaryKey]; ok {
		return nil, "", fmt.Errorf("model %q already contains primary key %q", t.model.Name, primaryKey)
	}

	for _, index := range t.indexes {
		if err := index.canAdd(normalized, primaryKey); err != nil {
			return nil, "", err
		}
	}
	return normalized, primaryKey, nil
}

func (t *table) insertPrepared(normalized Record, primaryKey string) {
	for _, index := range t.indexes {
		_ = index.add(normalized, primaryKey)
	}
	t.rows[primaryKey] = normalized
}

func (t *table) update(where map[string]any, patch Record) (string, Record, error) {
	primaryKey, next, err := t.prepareUpdate(where, patch)
	if err != nil {
		return "", nil, err
	}

	t.updatePrepared(primaryKey, next)
	return primaryKey, cloneRecord(next), nil
}

func (t *table) prepareUpdate(where map[string]any, patch Record) (string, Record, error) {
	primaryKey, err := t.primaryKeyFromWhere(where)
	if err != nil {
		return "", nil, err
	}

	current, ok := t.rows[primaryKey]
	if !ok {
		return "", nil, ErrNotFound
	}

	next := cloneRecord(current)
	normalizedPatch, err := normalizePartial(t.model, patch)
	if err != nil {
		return "", nil, err
	}
	for key, value := range normalizedPatch {
		next[key] = value
	}
	next, err = normalizeRecord(t.model, next)
	if err != nil {
		return "", nil, err
	}

	nextPrimaryKey, err := keyFromRecord(next, t.model.PrimaryKey)
	if err != nil {
		return "", nil, err
	}
	if nextPrimaryKey != primaryKey {
		return "", nil, fmt.Errorf("primary key updates are not supported")
	}

	for _, index := range t.indexes {
		if err := index.canAdd(next, primaryKey); err != nil {
			return "", nil, err
		}
	}

	return primaryKey, next, nil
}

func (t *table) updatePrepared(primaryKey string, next Record) {
	current := t.rows[primaryKey]
	for _, index := range t.indexes {
		_ = index.remove(current, primaryKey)
	}
	for _, index := range t.indexes {
		_ = index.add(next, primaryKey)
	}
	t.rows[primaryKey] = next
}

func (t *table) delete(where map[string]any) (string, Record, error) {
	primaryKey, current, err := t.prepareDelete(where)
	if err != nil {
		return "", nil, err
	}

	t.deletePrepared(primaryKey)
	return primaryKey, cloneRecord(current), nil
}

func (t *table) prepareDelete(where map[string]any) (string, Record, error) {
	primaryKey, err := t.primaryKeyFromWhere(where)
	if err != nil {
		return "", nil, err
	}

	current, ok := t.rows[primaryKey]
	if !ok {
		return "", nil, ErrNotFound
	}

	return primaryKey, current, nil
}

func (t *table) deletePrepared(primaryKey string) {
	current := t.rows[primaryKey]
	for _, index := range t.indexes {
		_ = index.remove(current, primaryKey)
	}
	delete(t.rows, primaryKey)
}

func (t *table) findByPrimaryKey(where map[string]any) (Record, bool, error) {
	primaryKey, err := t.primaryKeyFromWhere(where)
	if err != nil {
		return nil, false, err
	}
	record, ok := t.rows[primaryKey]
	if !ok {
		return nil, false, nil
	}
	return cloneRecord(record), true, nil
}

func (t *table) findMany(query Query) ([]Record, error) {
	normalizedWhere, err := normalizePartial(t.model, query.Where)
	if err != nil {
		return nil, err
	}
	query.Where = normalizedWhere

	ids, ok, err := t.idsFromIndex(query)
	if err != nil {
		return nil, err
	}

	if !ok {
		ids = make([]string, 0, len(t.rows))
		for id := range t.rows {
			ids = append(ids, id)
			if query.Limit > 0 && len(ids) >= query.Limit {
				break
			}
		}
	}

	result := make([]Record, 0, len(ids))
	for _, id := range ids {
		record, ok := t.rows[id]
		if !ok {
			continue
		}
		if !matchesWhere(record, query.Where) {
			continue
		}
		result = append(result, cloneRecord(record))
		if query.Limit > 0 && len(result) >= query.Limit {
			break
		}
	}
	return result, nil
}

func (t *table) idsFromIndex(query Query) ([]string, bool, error) {
	if len(query.Where) == 0 {
		return nil, false, nil
	}

	if query.Index != "" {
		index, ok := t.indexes[query.Index]
		if !ok {
			return nil, false, fmt.Errorf("model %q does not define index %q", t.model.Name, query.Index)
		}
		ids, err := index.lookup(query.Where, query.Limit)
		return ids, true, err
	}

	if containsAll(query.Where, t.model.PrimaryKey) {
		primaryKey, err := keyFromValues(query.Where, t.model.PrimaryKey)
		if err != nil {
			return nil, false, err
		}
		return []string{primaryKey}, true, nil
	}

	for _, index := range t.indexes {
		if containsAll(query.Where, index.definition.Fields) {
			ids, err := index.lookup(query.Where, query.Limit)
			return ids, true, err
		}
	}

	return nil, false, nil
}

func (t *table) primaryKeyFromWhere(where map[string]any) (string, error) {
	if !containsAll(where, t.model.PrimaryKey) {
		return "", fmt.Errorf("model %q lookup requires primary key fields %v", t.model.Name, t.model.PrimaryKey)
	}
	normalized, err := normalizePartial(t.model, where)
	if err != nil {
		return "", err
	}
	return keyFromValues(normalized, t.model.PrimaryKey)
}

func containsAll(values map[string]any, fields []string) bool {
	for _, field := range fields {
		if _, ok := values[field]; !ok {
			return false
		}
	}
	return true
}

func matchesWhere(record Record, where map[string]any) bool {
	for key, expected := range where {
		if record[key] != expected {
			return false
		}
	}
	return true
}
