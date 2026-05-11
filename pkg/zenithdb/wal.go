package zenithdb

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const binaryWALRecordMarker byte = 0x1e

const (
	opCreate = "create"
	opUpdate = "update"
	opDelete = "delete"
)

// WALFormat controls how operations are encoded on disk.
type WALFormat int

const (
	WALFormatJSONL WALFormat = iota
	WALFormatBinary
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
	format     WALFormat
}

// OpenWAL opens or creates a write-ahead log.
func OpenWAL(path string) (*WAL, error) {
	return OpenWALWithSyncPolicy(path, SyncAlways)
}

// OpenWALWithSyncPolicy opens or creates a write-ahead log with the given sync policy.
func OpenWALWithSyncPolicy(path string, syncPolicy SyncPolicy) (*WAL, error) {
	return OpenWALWithOptions(path, syncPolicy, WALFormatJSONL)
}

// OpenWALWithOptions opens or creates a write-ahead log.
func OpenWALWithOptions(path string, syncPolicy SyncPolicy, format WALFormat) (*WAL, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	return &WAL{file: file, syncPolicy: syncPolicy, format: format}, nil
}

// Append persists one operation and fsyncs it before returning.
func (wal *WAL) Append(ctx context.Context, operation operation) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	wal.mu.Lock()
	defer wal.mu.Unlock()

	var err error
	if wal.format == WALFormatBinary {
		err = wal.appendBinary(operation)
	} else {
		err = wal.appendJSONL(operation)
	}
	if err != nil {
		return err
	}
	if wal.syncPolicy == SyncNever {
		return nil
	}
	return wal.file.Sync()
}

func (wal *WAL) appendJSONL(operation operation) error {
	payload, err := json.Marshal(operation)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	_, err = wal.file.Write(payload)
	return err
}

func (wal *WAL) appendBinary(operation operation) error {
	payload, err := json.Marshal(operation)
	if err != nil {
		return err
	}
	header := []byte{binaryWALRecordMarker, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))
	if _, err := wal.file.Write(header); err != nil {
		return err
	}
	_, err = wal.file.Write(payload)
	return err
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
	if binary, err := wal.isBinary(); err != nil {
		return err
	} else if binary {
		return wal.replayBinary(ctx, afterSequence, apply)
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

func (wal *WAL) isBinary() (bool, error) {
	var marker [1]byte
	n, err := wal.file.Read(marker[:])
	if errors.Is(err, io.EOF) || n == 0 {
		_, seekErr := wal.file.Seek(0, io.SeekStart)
		return false, seekErr
	}
	if err != nil {
		return false, err
	}
	_, err = wal.file.Seek(0, io.SeekStart)
	return marker[0] == binaryWALRecordMarker, err
}

func (wal *WAL) replayBinary(ctx context.Context, afterSequence uint64, apply func(operation) error) error {
	line := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		header := make([]byte, 5)
		if _, err := io.ReadFull(wal.file, header); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return err
		}
		line++
		if header[0] != binaryWALRecordMarker {
			return fmt.Errorf("binary wal record %d has invalid marker", line)
		}
		size := binary.BigEndian.Uint32(header[1:])
		payload := make([]byte, size)
		if _, err := io.ReadFull(wal.file, payload); err != nil {
			return err
		}
		var operation operation
		decoder := json.NewDecoder(bytes.NewReader(payload))
		decoder.UseNumber()
		if err := decoder.Decode(&operation); err != nil {
			return fmt.Errorf("binary wal record %d: %w", line, err)
		}
		if operation.Sequence != 0 && operation.Sequence <= afterSequence {
			continue
		}
		if err := apply(operation); err != nil {
			return fmt.Errorf("binary wal record %d: %w", line, err)
		}
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
