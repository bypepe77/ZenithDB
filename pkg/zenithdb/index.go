package zenithdb

import "fmt"

type secondaryIndex struct {
	definition Index
	unique     map[string]string
	multi      map[string]map[string]struct{}
}

func newSecondaryIndex(definition Index) *secondaryIndex {
	idx := &secondaryIndex{definition: definition}
	if definition.Unique {
		idx.unique = make(map[string]string)
	} else {
		idx.multi = make(map[string]map[string]struct{})
	}
	return idx
}

func (idx *secondaryIndex) add(record Record, primaryKey string) error {
	if err := idx.canAdd(record, primaryKey); err != nil {
		return err
	}

	key, err := keyFromRecord(record, idx.definition.Fields)
	if err != nil {
		return err
	}

	if idx.definition.Unique {
		existing, ok := idx.unique[key]
		if ok && existing != primaryKey {
			return fmt.Errorf("unique index %q already contains key %q", idx.definition.Name, key)
		}
		idx.unique[key] = primaryKey
		return nil
	}

	bucket, ok := idx.multi[key]
	if !ok {
		bucket = make(map[string]struct{})
		idx.multi[key] = bucket
	}
	bucket[primaryKey] = struct{}{}
	return nil
}

func (idx *secondaryIndex) canAdd(record Record, primaryKey string) error {
	if !idx.definition.Unique {
		return nil
	}

	key, err := keyFromRecord(record, idx.definition.Fields)
	if err != nil {
		return err
	}
	existing, ok := idx.unique[key]
	if ok && existing != primaryKey {
		return fmt.Errorf("unique index %q already contains key %q", idx.definition.Name, key)
	}
	return nil
}

func (idx *secondaryIndex) remove(record Record, primaryKey string) error {
	key, err := keyFromRecord(record, idx.definition.Fields)
	if err != nil {
		return err
	}

	if idx.definition.Unique {
		delete(idx.unique, key)
		return nil
	}

	bucket, ok := idx.multi[key]
	if !ok {
		return nil
	}
	delete(bucket, primaryKey)
	if len(bucket) == 0 {
		delete(idx.multi, key)
	}
	return nil
}

func (idx *secondaryIndex) lookup(values map[string]any, limit int) ([]string, error) {
	key, err := keyFromValues(values, idx.definition.Fields)
	if err != nil {
		return nil, err
	}

	if idx.definition.Unique {
		primaryKey, ok := idx.unique[key]
		if !ok {
			return nil, nil
		}
		return []string{primaryKey}, nil
	}

	bucket, ok := idx.multi[key]
	if !ok {
		return nil, nil
	}
	if limit > 0 {
		result := make([]string, 0, min(limit, len(bucket)))
		for primaryKey := range bucket {
			result = append(result, primaryKey)
			if len(result) >= limit {
				break
			}
		}
		return result, nil
	}
	return sortedIDs(bucket), nil
}
