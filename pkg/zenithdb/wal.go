package zenithdb

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	opCreate = "create"
	opUpdate = "update"
	opDelete = "delete"
)

type operation struct {
	Sequence uint64         `json:"seq,omitempty"`
	Type     string         `json:"type"`
	Model    string         `json:"model"`
	Where    map[string]any `json:"where,omitempty"`
	Record   Record         `json:"record,omitempty"`
}

// WAL is an append-only operation log.
type WAL struct {
	mu         sync.Mutex
	file       *os.File
	syncPolicy SyncPolicy
}

// OpenWAL opens or creates a write-ahead log.
func OpenWAL(path string) (*WAL, error) {
	return OpenWALWithSyncPolicy(path, SyncAlways)
}

// OpenWALWithSyncPolicy opens or creates a write-ahead log with the given sync policy.
func OpenWALWithSyncPolicy(path string, syncPolicy SyncPolicy) (*WAL, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	return &WAL{file: file, syncPolicy: syncPolicy}, nil
}

// Append persists one operation and fsyncs it before returning.
func (wal *WAL) Append(ctx context.Context, operation operation) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	wal.mu.Lock()
	defer wal.mu.Unlock()

	payload, err := json.Marshal(operation)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	if _, err := wal.file.Write(payload); err != nil {
		return err
	}
	if wal.syncPolicy == SyncNever {
		return nil
	}
	return wal.file.Sync()
}

// Replay applies every operation in log order.
func (wal *WAL) Replay(ctx context.Context, apply func(operation) error) error {
	return wal.ReplayFrom(ctx, 0, apply)
}

// ReplayFrom applies operations whose sequence is newer than afterSequence.
func (wal *WAL) ReplayFrom(ctx context.Context, afterSequence uint64, apply func(operation) error) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if _, err := wal.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	scanner := bufio.NewScanner(wal.file)
	buffer := make([]byte, 0, 1024*1024)
	scanner.Buffer(buffer, 64*1024*1024)

	line := 0
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		line++
		raw := bytes.TrimSpace(scanner.Bytes())
		if len(raw) == 0 {
			continue
		}

		var operation operation
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		if err := decoder.Decode(&operation); err != nil {
			return fmt.Errorf("wal line %d: %w", line, err)
		}
		if operation.Sequence != 0 && operation.Sequence <= afterSequence {
			continue
		}
		if err := apply(operation); err != nil {
			return fmt.Errorf("wal line %d: %w", line, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	_, err := wal.file.Seek(0, io.SeekEnd)
	return err
}

// Close flushes and closes the log.
func (wal *WAL) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if wal.file == nil {
		return nil
	}

	syncErr := wal.file.Sync()
	closeErr := wal.file.Close()
	wal.file = nil
	return errors.Join(syncErr, closeErr)
}
