package zenithdb

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrNotFound is returned when a mutation targets a missing record.
var ErrNotFound = errors.New("record not found")

// Options configures the database engine.
type Options struct {
	WALPath string
}

// DB is the ZenithDB in-process engine.
type DB struct {
	mu     sync.RWMutex
	schema Schema
	tables map[string]*table
	wal    *WAL
}

// Open creates an in-memory database and optionally replays its WAL.
func Open(ctx context.Context, schema Schema, options Options) (*DB, error) {
	if err := schema.validate(); err != nil {
		return nil, err
	}

	db := &DB{
		schema: schema,
		tables: make(map[string]*table, len(schema.Models)),
	}
	for _, model := range schema.Models {
		db.tables[model.Name] = newTable(model)
	}

	if options.WALPath != "" {
		wal, err := OpenWAL(options.WALPath)
		if err != nil {
			return nil, err
		}
		db.wal = wal
		if err := wal.Replay(ctx, db.applyOperation); err != nil {
			_ = wal.Close()
			return nil, err
		}
	}

	return db, nil
}

// Close flushes and closes database resources.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.wal == nil {
		return nil
	}
	return db.wal.Close()
}

// Create inserts one record.
func (db *DB) Create(ctx context.Context, model string, record Record) (MutationResult, error) {
	if err := ctx.Err(); err != nil {
		return MutationResult{}, err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	table, err := db.table(model)
	if err != nil {
		return MutationResult{}, err
	}

	normalized, key, err := table.prepareInsert(record)
	if err != nil {
		return MutationResult{}, err
	}
	if db.wal != nil {
		if err := db.wal.Append(ctx, operation{Type: opCreate, Model: model, Record: normalized}); err != nil {
			return MutationResult{}, err
		}
	}

	table.insertPrepared(normalized, key)
	return MutationResult{Model: model, Key: key}, nil
}

// Update patches one record addressed by its primary key.
func (db *DB) Update(ctx context.Context, model string, where map[string]any, patch Record) (Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	table, err := db.table(model)
	if err != nil {
		return nil, err
	}

	primaryKey, next, err := table.prepareUpdate(where, patch)
	if err != nil {
		return nil, err
	}
	if db.wal != nil {
		if err := db.wal.Append(ctx, operation{Type: opUpdate, Model: model, Where: where, Record: cloneRecord(patch)}); err != nil {
			return nil, err
		}
	}

	table.updatePrepared(primaryKey, next)
	return cloneRecord(next), nil
}

// Delete removes one record addressed by its primary key.
func (db *DB) Delete(ctx context.Context, model string, where map[string]any) (Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	table, err := db.table(model)
	if err != nil {
		return nil, err
	}

	primaryKey, record, err := table.prepareDelete(where)
	if err != nil {
		return nil, err
	}
	if db.wal != nil {
		if err := db.wal.Append(ctx, operation{Type: opDelete, Model: model, Where: where}); err != nil {
			return nil, err
		}
	}

	table.deletePrepared(primaryKey)
	return cloneRecord(record), nil
}

// FindUnique returns one record by primary key or unique index.
func (db *DB) FindUnique(ctx context.Context, model string, where map[string]any, include map[string]Include) (Record, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	table, err := db.table(model)
	if err != nil {
		return nil, false, err
	}

	record, ok, err := db.findUniqueLocked(table, where)
	if err != nil || !ok {
		return record, ok, err
	}
	if len(include) > 0 {
		if err := db.expandIncludesLocked(model, record, include); err != nil {
			return nil, false, err
		}
	}
	return record, true, nil
}

// FindMany returns records using the best available index for the query shape.
func (db *DB) FindMany(ctx context.Context, model string, query Query) ([]Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	table, err := db.table(model)
	if err != nil {
		return nil, err
	}

	records, err := table.findMany(query)
	if err != nil {
		return nil, err
	}
	if len(query.Include) > 0 {
		for i := range records {
			if err := db.expandIncludesLocked(model, records[i], query.Include); err != nil {
				return nil, err
			}
		}
	}
	return records, nil
}

func (db *DB) table(model string) (*table, error) {
	table, ok := db.tables[model]
	if !ok {
		return nil, fmt.Errorf("unknown model %q", model)
	}
	return table, nil
}

func (db *DB) findUniqueLocked(table *table, where map[string]any) (Record, bool, error) {
	normalizedWhere, err := normalizePartial(table.model, where)
	if err != nil {
		return nil, false, err
	}

	if containsAll(normalizedWhere, table.model.PrimaryKey) {
		return table.findByPrimaryKey(normalizedWhere)
	}

	for _, index := range table.indexes {
		if !index.definition.Unique || !containsAll(normalizedWhere, index.definition.Fields) {
			continue
		}
		ids, err := index.lookup(normalizedWhere, 1)
		if err != nil {
			return nil, false, err
		}
		if len(ids) == 0 {
			return nil, false, nil
		}
		record, ok := table.rows[ids[0]]
		if !ok {
			return nil, false, nil
		}
		return cloneRecord(record), true, nil
	}

	return nil, false, fmt.Errorf("model %q has no unique lookup for fields %v", table.model.Name, where)
}

func (db *DB) applyOperation(operation operation) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.applyOperationLocked(operation)
}

func (db *DB) applyOperationLocked(operation operation) error {
	table, err := db.table(operation.Model)
	if err != nil {
		return err
	}

	switch operation.Type {
	case opCreate:
		_, err := table.insert(operation.Record)
		return err
	case opUpdate:
		_, _, err := table.update(operation.Where, operation.Record)
		return err
	case opDelete:
		_, _, err := table.delete(operation.Where)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		return err
	default:
		return fmt.Errorf("unknown wal operation %q", operation.Type)
	}
}
