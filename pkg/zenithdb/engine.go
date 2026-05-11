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
	if operation.Sequence > db.sequence {
		db.sequence = operation.Sequence
	}
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

func (db *DB) nextSequenceLocked() uint64 {
	db.sequence++
	return db.sequence
}
