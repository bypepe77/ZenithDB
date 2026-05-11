package zenithdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
)

// ErrNotFound is returned when a mutation targets a missing record.
var ErrNotFound = errors.New("record not found")

// Options configures the database engine.
type Options struct {
	ConnectionURL string
	WireURL       string
	AuthToken     string
	DataDir       string
	WALPath       string
	SyncPolicy    SyncPolicy
	WALFormat     WALFormat
}

// DB is the ZenithDB in-process engine.
type DB struct {
	mu     sync.RWMutex
	schema Schema
	tables map[string]*table
	wal    *WAL
	storage *storageManager
	sequence uint64
}

// Open creates an in-memory database and optionally replays its WAL.
func Open(ctx context.Context, schema Schema, options Options) (*DB, error) {
	var err error
	options, err = resolveOptions(options)
	if err != nil {
		return nil, err
	}
	if err := schema.validate(); err != nil {
		return nil, err
	}
	if options.WireURL != "" && options.DataDir == "" && options.WALPath == "" {
		return nil, fmt.Errorf("remote connection URL requires a remote client")
	}

	db := &DB{
		schema: schema,
		tables: make(map[string]*table, len(schema.Models)),
	}
	for _, model := range schema.Models {
		db.tables[model.Name] = newTable(model)
	}

	if options.SyncPolicy == 0 {
		options.SyncPolicy = SyncAlways
	}
	if options.WALFormat == 0 {
		options.WALFormat = WALFormatJSONL
	}

	if options.DataDir != "" {
		storage, err := openStorageManager(options.DataDir)
		if err != nil {
			return nil, err
		}
		db.storage = storage

		if snapshotPath := storage.snapshotPath(); snapshotPath != "" {
			if _, err := os.Stat(snapshotPath); err == nil {
				if _, err := db.loadSnapshot(ctx, snapshotPath); err != nil {
					_ = storage.Close()
					return nil, err
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				_ = storage.Close()
				return nil, err
			}
		}

		wal, err := storage.openWAL(options.SyncPolicy, options.WALFormat)
		if err != nil {
			_ = storage.Close()
			return nil, err
		}
		db.wal = wal
		if err := wal.ReplayFrom(ctx, storage.manifest.SnapshotSequence, db.applyOperation); err != nil {
			_ = wal.Close()
			_ = storage.Close()
			return nil, err
		}
		if db.sequence < storage.manifest.LastSequence {
			db.sequence = storage.manifest.LastSequence
		}
		return db, nil
	}

	if options.WALPath != "" {
		wal, err := OpenWALWithOptions(options.WALPath, options.SyncPolicy, options.WALFormat)
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

// OpenURL opens a database from a connection URL.
func OpenURL(ctx context.Context, schema Schema, connectionURL string) (*DB, error) {
	return Open(ctx, schema, Options{ConnectionURL: connectionURL})
}

// Close flushes and closes database resources.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.wal == nil {
		if db.storage != nil {
			return db.storage.Close()
		}
		return nil
	}
	walErr := db.wal.Close()
	var storageErr error
	if db.storage != nil {
		if err := db.storage.saveLastSequence(db.sequence); err != nil {
			storageErr = err
		}
		if err := db.storage.Close(); err != nil {
			storageErr = errors.Join(storageErr, err)
		}
	}
	return errors.Join(walErr, storageErr)
}

// Checkpoint writes an atomic snapshot for faster future recovery.
func (db *DB) Checkpoint(ctx context.Context) error {
	if db.storage == nil {
		return fmt.Errorf("checkpoint requires Options.DataDir")
	}

	db.mu.RLock()
	sequence := db.sequence
	db.mu.RUnlock()

	if err := db.Snapshot(ctx, db.storage.checkpointPath()); err != nil {
		return err
	}
	return db.storage.saveCheckpointManifest(ctx, sequence)
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
	sequence := db.nextSequenceLocked()
	if db.wal != nil {
		if err := db.wal.Append(ctx, operation{Sequence: sequence, Type: opCreate, Model: model, Record: normalized}); err != nil {
			db.sequence--
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
	sequence := db.nextSequenceLocked()
	if db.wal != nil {
		if err := db.wal.Append(ctx, operation{Sequence: sequence, Type: opUpdate, Model: model, Where: where, Record: cloneRecord(patch)}); err != nil {
			db.sequence--
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
	sequence := db.nextSequenceLocked()
	if db.wal != nil {
		if err := db.wal.Append(ctx, operation{Sequence: sequence, Type: opDelete, Model: model, Where: where}); err != nil {
			db.sequence--
			return nil, err
		}
	}

	table.deletePrepared(primaryKey)
	return cloneRecord(record), nil
}

// Upsert updates a record selected by a unique lookup or inserts createRecord.
func (db *DB) Upsert(ctx context.Context, model string, where map[string]any, createRecord Record, updatePatch Record) (Record, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	nextTables := db.cloneTablesLocked()
	table, ok := nextTables[model]
	if !ok {
		return nil, false, fmt.Errorf("unknown model %q", model)
	}

	next, created, err := db.prepareUpsertLocked(table, where, createRecord, updatePatch)
	if err != nil {
		return nil, false, err
	}
	sequence := db.nextSequenceLocked()
	if db.wal != nil {
		if err := db.wal.Append(ctx, operation{Sequence: sequence, Type: opUpsert, Model: model, Where: cloneMap(where), Record: cloneRecord(createRecord), Patch: cloneRecord(updatePatch)}); err != nil {
			db.sequence--
			return nil, false, err
		}
	}
	db.tables = nextTables
	return next, created, nil
}

// Batch applies multiple mutations atomically. Either every operation is
// persisted and published, or the database state is left unchanged.
func (db *DB) Batch(ctx context.Context, operations []BatchOperation) ([]BatchResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	return db.batchLocked(ctx, operations)
}

func (db *DB) CreateMany(ctx context.Context, model string, records []Record) ([]MutationResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	operations := make([]BatchOperation, 0, len(records))
	for _, record := range records {
		operations = append(operations, BatchOperation{Type: BatchCreate, Model: model, Record: record})
	}
	results, err := db.Batch(ctx, operations)
	if err != nil {
		return nil, err
	}
	mutations := make([]MutationResult, 0, len(results))
	for _, result := range results {
		mutations = append(mutations, MutationResult{Model: result.Model, Key: result.Key})
	}
	return mutations, nil
}

func (db *DB) UpdateMany(ctx context.Context, model string, query Query, patch Record) (ManyResult, error) {
	if err := ctx.Err(); err != nil {
		return ManyResult{}, err
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	table, err := db.table(model)
	if err != nil {
		return ManyResult{}, err
	}
	records, err := table.findMany(query)
	if err != nil {
		return ManyResult{}, err
	}
	operations := make([]BatchOperation, 0, len(records))
	for _, record := range records {
		where, err := primaryWhereFromRecord(table.model, record)
		if err != nil {
			return ManyResult{}, err
		}
		operations = append(operations, BatchOperation{Type: BatchUpdate, Model: model, Where: where, Record: patch})
	}
	if _, err := db.batchLocked(ctx, operations); err != nil {
		return ManyResult{}, err
	}
	return ManyResult{Model: model, Count: len(operations)}, nil
}

func (db *DB) DeleteMany(ctx context.Context, model string, query Query) (ManyResult, error) {
	if err := ctx.Err(); err != nil {
		return ManyResult{}, err
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	table, err := db.table(model)
	if err != nil {
		return ManyResult{}, err
	}
	records, err := table.findMany(query)
	if err != nil {
		return ManyResult{}, err
	}
	operations := make([]BatchOperation, 0, len(records))
	for _, record := range records {
		where, err := primaryWhereFromRecord(table.model, record)
		if err != nil {
			return ManyResult{}, err
		}
		operations = append(operations, BatchOperation{Type: BatchDelete, Model: model, Where: where})
	}
	if _, err := db.batchLocked(ctx, operations); err != nil {
		return ManyResult{}, err
	}
	return ManyResult{Model: model, Count: len(operations)}, nil
}

func (db *DB) batchLocked(ctx context.Context, operations []BatchOperation) ([]BatchResult, error) {
	if len(operations) == 0 {
		return nil, nil
	}
	nextTables := db.cloneTablesLocked()
	walOperations := make([]operation, 0, len(operations))
	results := make([]BatchResult, 0, len(operations))
	for _, batchOperation := range operations {
		walOperation, result, err := applyBatchOperation(nextTables, batchOperation)
		if err != nil {
			return nil, err
		}
		walOperations = append(walOperations, walOperation)
		results = append(results, result)
	}

	sequence := db.nextSequenceLocked()
	if db.wal != nil {
		if err := db.wal.Append(ctx, operation{Sequence: sequence, Type: opBatch, Operations: walOperations}); err != nil {
			db.sequence--
			return nil, err
		}
	}

	db.tables = nextTables
	return results, nil
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

// Count returns the number of records matching Where and Filters.
func (db *DB) Count(ctx context.Context, model string, query Query) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	table, err := db.table(model)
	if err != nil {
		return 0, err
	}
	return table.count(query)
}

func (db *DB) table(model string) (*table, error) {
	table, ok := db.tables[model]
	if !ok {
		return nil, fmt.Errorf("unknown model %q", model)
	}
	return table, nil
}

func (db *DB) cloneTablesLocked() map[string]*table {
	cloned := make(map[string]*table, len(db.tables))
	for name, table := range db.tables {
		cloned[name] = table.clone()
	}
	return cloned
}

func applyBatchOperation(tables map[string]*table, batchOperation BatchOperation) (operation, BatchResult, error) {
	table, ok := tables[batchOperation.Model]
	if !ok {
		return operation{}, BatchResult{}, fmt.Errorf("unknown model %q", batchOperation.Model)
	}

	switch batchOperation.Type {
	case BatchCreate:
		normalized, key, err := table.prepareInsert(batchOperation.Record)
		if err != nil {
			return operation{}, BatchResult{}, err
		}
		table.insertPrepared(normalized, key)
		return operation{Type: opCreate, Model: batchOperation.Model, Record: normalized}, BatchResult{Type: batchOperation.Type, Model: batchOperation.Model, Key: key, Record: cloneRecord(normalized)}, nil
	case BatchUpdate:
		primaryKey, next, err := table.prepareUpdate(batchOperation.Where, batchOperation.Record)
		if err != nil {
			return operation{}, BatchResult{}, err
		}
		table.updatePrepared(primaryKey, next)
		return operation{Type: opUpdate, Model: batchOperation.Model, Where: cloneMap(batchOperation.Where), Record: cloneRecord(batchOperation.Record)}, BatchResult{Type: batchOperation.Type, Model: batchOperation.Model, Key: primaryKey, Record: cloneRecord(next)}, nil
	case BatchDelete:
		primaryKey, record, err := table.prepareDelete(batchOperation.Where)
		if err != nil {
			return operation{}, BatchResult{}, err
		}
		table.deletePrepared(primaryKey)
		return operation{Type: opDelete, Model: batchOperation.Model, Where: cloneMap(batchOperation.Where)}, BatchResult{Type: batchOperation.Type, Model: batchOperation.Model, Key: primaryKey, Record: cloneRecord(record)}, nil
	default:
		return operation{}, BatchResult{}, fmt.Errorf("unsupported batch operation %q", batchOperation.Type)
	}
}

func (db *DB) prepareUpsertLocked(table *table, where map[string]any, createRecord Record, updatePatch Record) (Record, bool, error) {
	found, ok, err := db.findUniqueLocked(table, where)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		normalized, key, err := table.prepareInsert(createRecord)
		if err != nil {
			return nil, false, err
		}
		table.insertPrepared(normalized, key)
		return cloneRecord(normalized), true, nil
	}

	primaryWhere, err := primaryWhereFromRecord(table.model, found)
	if err != nil {
		return nil, false, err
	}
	primaryKey, next, err := table.prepareUpdate(primaryWhere, updatePatch)
	if err != nil {
		return nil, false, err
	}
	table.updatePrepared(primaryKey, next)
	return cloneRecord(next), false, nil
}

func primaryWhereFromRecord(model Model, record Record) (map[string]any, error) {
	where := make(map[string]any, len(model.PrimaryKey))
	for _, field := range model.PrimaryKey {
		value, ok := record[field]
		if !ok {
			return nil, fmt.Errorf("record for model %q is missing primary key field %q", model.Name, field)
		}
		where[field] = value
	}
	return where, nil
}

func cloneMap(values map[string]any) map[string]any {
	if values == nil {
		return nil
	}
	cloned := make(map[string]any, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
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
	if operation.Sequence > db.sequence {
		db.sequence = operation.Sequence
	}

	switch operation.Type {
	case opCreate:
		table, err := db.table(operation.Model)
		if err != nil {
			return err
		}
		_, err = table.insert(operation.Record)
		return err
	case opUpdate:
		table, err := db.table(operation.Model)
		if err != nil {
			return err
		}
		_, _, err = table.update(operation.Where, operation.Record)
		return err
	case opDelete:
		table, err := db.table(operation.Model)
		if err != nil {
			return err
		}
		_, _, err = table.delete(operation.Where)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		return err
	case opUpsert:
		table, err := db.table(operation.Model)
		if err != nil {
			return err
		}
		_, _, err = db.prepareUpsertLocked(table, operation.Where, operation.Record, operation.Patch)
		return err
	case opBatch:
		nextTables := db.cloneTablesLocked()
		for _, child := range operation.Operations {
			if _, _, err := applyBatchOperation(nextTables, BatchOperation{
				Type:   BatchOperationType(child.Type),
				Model:  child.Model,
				Where:  child.Where,
				Record: child.Record,
			}); err != nil {
				return err
			}
		}
		db.tables = nextTables
		return nil
	default:
		return fmt.Errorf("unknown wal operation %q", operation.Type)
	}
}

func (db *DB) nextSequenceLocked() uint64 {
	db.sequence++
	return db.sequence
}
