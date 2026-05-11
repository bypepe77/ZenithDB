package zenithdb

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

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

func (t *table) clone() *table {
	cloned := newTable(t.model)
	for key, record := range t.rows {
		next := cloneRecord(record)
		cloned.rows[key] = next
		for _, index := range cloned.indexes {
			_ = index.add(next, key)
		}
	}
	return cloned
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
	normalizedFilters, err := normalizeFilters(t.model, query.Filters)
	if err != nil {
		return nil, err
	}
	query.Filters = normalizedFilters
	if err := validateOrderBy(t.model, query.OrderBy); err != nil {
		return nil, err
	}
	normalizedCursor, err := normalizePartial(t.model, query.Cursor)
	if err != nil {
		return nil, err
	}
	query.Cursor = normalizedCursor

	indexQuery := query
	if !canLimitDuringIDLookup(query) {
		indexQuery.Limit = 0
	}
	ids, ok, err := t.idsFromIndex(indexQuery)
	if err != nil {
		return nil, err
	}

	if !ok {
		ids = make([]string, 0, len(t.rows))
		for id := range t.rows {
			ids = append(ids, id)
			if canLimitDuringIDLookup(query) && len(ids) >= query.Limit {
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
		if !matchesFilters(record, query.Filters) {
			continue
		}
		result = append(result, cloneRecord(record))
		if canLimitDuringResultScan(query) && len(result) >= query.Limit {
			break
		}
	}
	sortRecords(result, query.OrderBy)
	result = applyCursor(result, query.Cursor)
	return paginateRecords(result, query.Skip, query.Limit), nil
}

func (t *table) count(query Query) (int, error) {
	normalizedWhere, err := normalizePartial(t.model, query.Where)
	if err != nil {
		return 0, err
	}
	query.Where = normalizedWhere
	normalizedFilters, err := normalizeFilters(t.model, query.Filters)
	if err != nil {
		return 0, err
	}
	query.Filters = normalizedFilters

	indexQuery := query
	indexQuery.Limit = 0
	ids, ok, err := t.idsFromIndex(indexQuery)
	if err != nil {
		return 0, err
	}
	if !ok {
		ids = make([]string, 0, len(t.rows))
		for id := range t.rows {
			ids = append(ids, id)
		}
	}

	count := 0
	for _, id := range ids {
		record, ok := t.rows[id]
		if !ok {
			continue
		}
		if !matchesWhere(record, query.Where) || !matchesFilters(record, query.Filters) {
			continue
		}
		count++
	}
	return count, nil
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

func canLimitDuringIDLookup(query Query) bool {
	return query.Limit > 0 && query.Skip == 0 && len(query.Cursor) == 0 && len(query.OrderBy) == 0 && len(query.Filters) == 0
}

func canLimitDuringResultScan(query Query) bool {
	return query.Limit > 0 && query.Skip == 0 && len(query.Cursor) == 0 && len(query.OrderBy) == 0
}

func normalizeFilters(model Model, filters map[string]Filter) (map[string]Filter, error) {
	if len(filters) == 0 {
		return nil, nil
	}
	fields := make(map[string]Field, len(model.Fields))
	for _, field := range model.Fields {
		fields[field.Name] = field
	}
	normalized := make(map[string]Filter, len(filters))
	for name, filter := range filters {
		field, ok := fields[name]
		if !ok {
			return nil, fmt.Errorf("model %q does not define field %q", model.Name, name)
		}
		next, err := normalizeFilter(field, filter)
		if err != nil {
			return nil, fmt.Errorf("model %q field %q: %w", model.Name, name, err)
		}
		normalized[name] = next
	}
	return normalized, nil
}

func normalizeFilter(field Field, filter Filter) (Filter, error) {
	var err error
	if filter.Equals != nil {
		filter.Equals, err = normalizeValue(field.Kind, filter.Equals)
		if err != nil {
			return Filter{}, err
		}
	}
	for i, value := range filter.In {
		filter.In[i], err = normalizeValue(field.Kind, value)
		if err != nil {
			return Filter{}, err
		}
	}
	if filter.GT != nil {
		filter.GT, err = normalizeValue(field.Kind, filter.GT)
		if err != nil {
			return Filter{}, err
		}
	}
	if filter.GTE != nil {
		filter.GTE, err = normalizeValue(field.Kind, filter.GTE)
		if err != nil {
			return Filter{}, err
		}
	}
	if filter.LT != nil {
		filter.LT, err = normalizeValue(field.Kind, filter.LT)
		if err != nil {
			return Filter{}, err
		}
	}
	if filter.LTE != nil {
		filter.LTE, err = normalizeValue(field.Kind, filter.LTE)
		if err != nil {
			return Filter{}, err
		}
	}
	return filter, nil
}

func validateOrderBy(model Model, orderBy []OrderBy) error {
	if len(orderBy) == 0 {
		return nil
	}
	fields := make(map[string]struct{}, len(model.Fields))
	for _, field := range model.Fields {
		fields[field.Name] = struct{}{}
	}
	for _, order := range orderBy {
		if _, ok := fields[order.Field]; !ok {
			return fmt.Errorf("model %q does not define field %q", model.Name, order.Field)
		}
		if order.Direction != "" && order.Direction != SortAsc && order.Direction != SortDesc {
			return fmt.Errorf("unsupported sort direction %q", order.Direction)
		}
	}
	return nil
}

func matchesFilters(record Record, filters map[string]Filter) bool {
	for field, filter := range filters {
		value := record[field]
		if filter.Equals != nil && value != filter.Equals {
			return false
		}
		if len(filter.In) > 0 && !containsFilterValue(filter.In, value) {
			return false
		}
		if filter.Contains != "" {
			typed, ok := value.(string)
			if !ok || !strings.Contains(typed, filter.Contains) {
				return false
			}
		}
		if filter.GT != nil && compareValues(value, filter.GT) <= 0 {
			return false
		}
		if filter.GTE != nil && compareValues(value, filter.GTE) < 0 {
			return false
		}
		if filter.LT != nil && compareValues(value, filter.LT) >= 0 {
			return false
		}
		if filter.LTE != nil && compareValues(value, filter.LTE) > 0 {
			return false
		}
	}
	return true
}

func containsFilterValue(values []any, target any) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func sortRecords(records []Record, orderBy []OrderBy) {
	if len(orderBy) == 0 || len(records) < 2 {
		return
	}
	sort.SliceStable(records, func(i, j int) bool {
		left := records[i]
		right := records[j]
		for _, order := range orderBy {
			comparison := compareValues(left[order.Field], right[order.Field])
			if comparison == 0 {
				continue
			}
			if order.Direction == SortDesc {
				return comparison > 0
			}
			return comparison < 0
		}
		return false
	})
}

func paginateRecords(records []Record, skip int, limit int) []Record {
	if skip < 0 {
		skip = 0
	}
	if skip >= len(records) {
		return nil
	}
	records = records[skip:]
	if limit > 0 && len(records) > limit {
		return records[:limit]
	}
	return records
}

func applyCursor(records []Record, cursor map[string]any) []Record {
	if len(cursor) == 0 {
		return records
	}
	for i, record := range records {
		if matchesWhere(record, cursor) {
			if i+1 >= len(records) {
				return nil
			}
			return records[i+1:]
		}
	}
	return nil
}

func compareValues(left any, right any) int {
	switch typedLeft := left.(type) {
	case string:
		typedRight, _ := right.(string)
		return strings.Compare(typedLeft, typedRight)
	case int64:
		typedRight, _ := right.(int64)
		return compareOrdered(typedLeft, typedRight)
	case bool:
		typedRight, _ := right.(bool)
		return compareOrdered(boolRank(typedLeft), boolRank(typedRight))
	case float64:
		typedRight, _ := right.(float64)
		return compareOrdered(typedLeft, typedRight)
	case time.Time:
		typedRight, _ := right.(time.Time)
		return compareOrdered(typedLeft.UnixNano(), typedRight.UnixNano())
	default:
		return strings.Compare(fmt.Sprint(left), fmt.Sprint(right))
	}
}

func boolRank(value bool) int {
	if value {
		return 1
	}
	return 0
}

func compareOrdered[T ~int | ~int64 | ~float64](left T, right T) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}
