package zenithdb

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
)

type snapshotFile struct {
	Version int                 `json:"version"`
	Models  map[string][]Record `json:"models"`
}

// Snapshot writes a compact point-in-time image of the in-memory state.
func (db *DB) Snapshot(ctx context.Context, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	db.mu.RLock()
	snapshot := snapshotFile{
		Version: 1,
		Models:  make(map[string][]Record, len(db.tables)),
	}
	for name, table := range db.tables {
		records := make([]Record, 0, len(table.rows))
		for _, record := range table.rows {
			records = append(records, cloneRecord(record))
		}
		snapshot.Models[name] = records
	}
	db.mu.RUnlock()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	temp, err := os.CreateTemp(filepath.Dir(path), ".zenithdb-snapshot-*")
	if err != nil {
		return err
	}
	tempName := temp.Name()

	encoder := json.NewEncoder(temp)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(snapshot); err != nil {
		_ = temp.Close()
		_ = os.Remove(tempName)
		return err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		_ = os.Remove(tempName)
		return err
	}
	if err := temp.Close(); err != nil {
		_ = os.Remove(tempName)
		return err
	}

	return os.Rename(tempName, path)
}

// LoadSnapshot replaces the current in-memory state with records from a snapshot.
func (db *DB) LoadSnapshot(ctx context.Context, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	var snapshot snapshotFile
	decoder := json.NewDecoder(file)
	decoder.UseNumber()
	if err := decoder.Decode(&snapshot); err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	next := make(map[string]*table, len(db.schema.Models))
	for _, model := range db.schema.Models {
		next[model.Name] = newTable(model)
	}

	for model, records := range snapshot.Models {
		table, ok := next[model]
		if !ok {
			return os.ErrInvalid
		}
		for _, record := range records {
			if _, err := table.insert(record); err != nil {
				return err
			}
		}
	}

	db.tables = next
	return nil
}
