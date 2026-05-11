package zenithdb

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"
)

const (
	defaultWALFile      = "000001.wal"
	defaultSnapshotFile = "000001.snapshot.json"
	manifestFileName   = "manifest.json"
)

// SyncPolicy controls how aggressively ZenithDB fsyncs persisted writes.
type SyncPolicy int

const (
	// SyncAlways fsyncs every WAL append. It is the safest default.
	SyncAlways SyncPolicy = iota
	// SyncBatch is reserved for grouped fsyncs. It currently behaves like SyncAlways.
	SyncBatch
	// SyncNever leaves flushing to the operating system.
	SyncNever
)

type storageManager struct {
	root     string
	manifest manifest
	lockFile *os.File
}

type manifest struct {
	Version          int       `json:"version"`
	CreatedAt        time.Time `json:"createdAt"`
	UpdatedAt        time.Time `json:"updatedAt"`
	ActiveWAL        string    `json:"activeWal"`
	LatestSnapshot   string    `json:"latestSnapshot,omitempty"`
	LastSequence     uint64    `json:"lastSequence"`
	SnapshotSequence uint64    `json:"snapshotSequence"`
}

func openStorageManager(root string) (*storageManager, error) {
	manager := &storageManager{root: root}
	for _, dir := range []string{root, manager.walDir(), manager.snapshotDir(), manager.lockDir()} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}

	lockFile, err := os.OpenFile(manager.lockPath(), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	manager.lockFile = lockFile

	if err := manager.loadManifest(); err != nil {
		_ = manager.Close()
		return nil, err
	}
	return manager, nil
}

func (m *storageManager) Close() error {
	if m.lockFile == nil {
		return nil
	}
	err := m.lockFile.Close()
	m.lockFile = nil
	return err
}

func (m *storageManager) openWAL(syncPolicy SyncPolicy, format WALFormat) (*WAL, error) {
	return OpenWALWithOptions(filepath.Join(m.walDir(), m.manifest.ActiveWAL), syncPolicy, format)
}

func (m *storageManager) snapshotPath() string {
	if m.manifest.LatestSnapshot == "" {
		return ""
	}
	return filepath.Join(m.snapshotDir(), m.manifest.LatestSnapshot)
}

func (m *storageManager) checkpointPath() string {
	return filepath.Join(m.snapshotDir(), defaultSnapshotFile)
}

func (m *storageManager) saveCheckpointManifest(ctx context.Context, sequence uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.manifest.LatestSnapshot = defaultSnapshotFile
	m.manifest.SnapshotSequence = sequence
	m.manifest.LastSequence = sequence
	return m.saveManifest()
}

func (m *storageManager) saveLastSequence(sequence uint64) error {
	if sequence > m.manifest.LastSequence {
		m.manifest.LastSequence = sequence
	}
	return m.saveManifest()
}

func (m *storageManager) loadManifest() error {
	path := filepath.Join(m.root, manifestFileName)
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		now := time.Now().UTC()
		m.manifest = manifest{
			Version:   1,
			CreatedAt: now,
			UpdatedAt: now,
			ActiveWAL: defaultWALFile,
		}
		return m.saveManifest()
	}
	if err != nil {
		return err
	}
	if err := json.Unmarshal(raw, &m.manifest); err != nil {
		return err
	}
	if m.manifest.ActiveWAL == "" {
		m.manifest.ActiveWAL = defaultWALFile
	}
	return nil
}

func (m *storageManager) saveManifest() error {
	m.manifest.UpdatedAt = time.Now().UTC()
	raw, err := json.MarshalIndent(m.manifest, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(m.root, manifestFileName)
	temp, err := os.CreateTemp(m.root, ".manifest-*")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	if _, err := temp.Write(raw); err != nil {
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

func (m *storageManager) walDir() string {
	return filepath.Join(m.root, "wal")
}

func (m *storageManager) snapshotDir() string {
	return filepath.Join(m.root, "snapshots")
}

func (m *storageManager) lockDir() string {
	return filepath.Join(m.root, "locks")
}

func (m *storageManager) lockPath() string {
	return filepath.Join(m.lockDir(), "db.lock")
}
