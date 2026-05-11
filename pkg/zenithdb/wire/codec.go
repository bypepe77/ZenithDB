package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/bypepe77/ZenithDB/pkg/zenithdb"
)

const (
	protocolMagic = "ZDBW1"

	opCreate byte = iota + 1
	opUpdate
	opDelete
	opFindUnique
	opFindMany
	opCheckpoint
	opPullSchema
	opValidateSchema
)

const (
	valueNil byte = iota
	valueString
	valueInt64
	valueBool
	valueFloat64
	valueTime
	valueRecord
	valueRecordSlice
)

func writeFrame(w io.Writer, op byte, payload []byte) error {
	header := [5]byte{op}
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func readFrame(r io.Reader) (byte, []byte, error) {
	var header [5]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return 0, nil, err
	}
	size := binary.BigEndian.Uint32(header[1:])
	payload := make([]byte, size)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return header[0], payload, nil
}

func writeResponse(w io.Writer, payload []byte) error {
	return writeFrame(w, 0, payload)
}

func writeErrorResponse(w io.Writer, err error) error {
	var payload bytes.Buffer
	writeString(&payload, err.Error())
	return writeFrame(w, 1, payload.Bytes())
}

func readResponse(r io.Reader) ([]byte, error) {
	status, payload, err := readFrame(r)
	if err != nil {
		return nil, err
	}
	if status == 0 {
		return payload, nil
	}
	reader := bytes.NewReader(payload)
	message, decodeErr := readString(reader)
	if decodeErr != nil {
		return nil, decodeErr
	}
	return nil, errors.New(message)
}

func writeString(w io.Writer, value string) {
	writeUint32(w, uint32(len(value)))
	_, _ = io.WriteString(w, value)
}

func readString(r *bytes.Reader) (string, error) {
	size, err := readUint32(r)
	if err != nil {
		return "", err
	}
	raw := make([]byte, size)
	if _, err := io.ReadFull(r, raw); err != nil {
		return "", err
	}
	return string(raw), nil
}

func writeBool(w io.Writer, value bool) {
	if value {
		_, _ = w.Write([]byte{1})
		return
	}
	_, _ = w.Write([]byte{0})
}

func readBool(r *bytes.Reader) (bool, error) {
	value, err := r.ReadByte()
	return value == 1, err
}

func writeUint32(w io.Writer, value uint32) {
	var raw [4]byte
	binary.BigEndian.PutUint32(raw[:], value)
	_, _ = w.Write(raw[:])
}

func readUint32(r *bytes.Reader) (uint32, error) {
	var raw [4]byte
	if _, err := io.ReadFull(r, raw[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(raw[:]), nil
}

func writeInt64(w io.Writer, value int64) {
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], uint64(value))
	_, _ = w.Write(raw[:])
}

func readInt64(r *bytes.Reader) (int64, error) {
	var raw [8]byte
	if _, err := io.ReadFull(r, raw[:]); err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(raw[:])), nil
}

func writeFloat64(w io.Writer, value float64) {
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], math.Float64bits(value))
	_, _ = w.Write(raw[:])
}

func readFloat64(r *bytes.Reader) (float64, error) {
	var raw [8]byte
	if _, err := io.ReadFull(r, raw[:]); err != nil {
		return 0, err
	}
	return math.Float64frombits(binary.BigEndian.Uint64(raw[:])), nil
}

func writeRecord(w io.Writer, record zenithdb.Record) {
	writeUint32(w, uint32(len(record)))
	for key, value := range record {
		writeString(w, key)
		writeValue(w, value)
	}
}

func readRecord(r *bytes.Reader) (zenithdb.Record, error) {
	size, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	record := make(zenithdb.Record, size)
	for i := uint32(0); i < size; i++ {
		key, err := readString(r)
		if err != nil {
			return nil, err
		}
		value, err := readValue(r)
		if err != nil {
			return nil, err
		}
		record[key] = value
	}
	return record, nil
}

func writeRecordSlice(w io.Writer, records []zenithdb.Record) {
	writeUint32(w, uint32(len(records)))
	for _, record := range records {
		writeRecord(w, record)
	}
}

func readRecordSlice(r *bytes.Reader) ([]zenithdb.Record, error) {
	size, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	records := make([]zenithdb.Record, 0, size)
	for i := uint32(0); i < size; i++ {
		record, err := readRecord(r)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

func writeStringMap(w io.Writer, values map[string]any) {
	writeRecord(w, zenithdb.Record(values))
}

func readStringMap(r *bytes.Reader) (map[string]any, error) {
	record, err := readRecord(r)
	return map[string]any(record), err
}

func writeIncludeMap(w io.Writer, includes map[string]zenithdb.Include) {
	writeUint32(w, uint32(len(includes)))
	for key, include := range includes {
		writeString(w, key)
		writeInt64(w, int64(include.Limit))
	}
}

func readIncludeMap(r *bytes.Reader) (map[string]zenithdb.Include, error) {
	size, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	includes := make(map[string]zenithdb.Include, size)
	for i := uint32(0); i < size; i++ {
		key, err := readString(r)
		if err != nil {
			return nil, err
		}
		limit, err := readInt64(r)
		if err != nil {
			return nil, err
		}
		includes[key] = zenithdb.Include{Limit: int(limit)}
	}
	return includes, nil
}

func writeQuery(w io.Writer, query zenithdb.Query) {
	writeStringMap(w, query.Where)
	writeString(w, query.Index)
	writeInt64(w, int64(query.Limit))
	writeIncludeMap(w, query.Include)
}

func readQuery(r *bytes.Reader) (zenithdb.Query, error) {
	where, err := readStringMap(r)
	if err != nil {
		return zenithdb.Query{}, err
	}
	index, err := readString(r)
	if err != nil {
		return zenithdb.Query{}, err
	}
	limit, err := readInt64(r)
	if err != nil {
		return zenithdb.Query{}, err
	}
	include, err := readIncludeMap(r)
	if err != nil {
		return zenithdb.Query{}, err
	}
	return zenithdb.Query{Where: where, Index: index, Limit: int(limit), Include: include}, nil
}

func writeValue(w io.Writer, value any) {
	switch typed := value.(type) {
	case nil:
		_, _ = w.Write([]byte{valueNil})
	case string:
		_, _ = w.Write([]byte{valueString})
		writeString(w, typed)
	case int:
		_, _ = w.Write([]byte{valueInt64})
		writeInt64(w, int64(typed))
	case int64:
		_, _ = w.Write([]byte{valueInt64})
		writeInt64(w, typed)
	case bool:
		_, _ = w.Write([]byte{valueBool})
		writeBool(w, typed)
	case float64:
		_, _ = w.Write([]byte{valueFloat64})
		writeFloat64(w, typed)
	case time.Time:
		_, _ = w.Write([]byte{valueTime})
		writeInt64(w, typed.UTC().UnixNano())
	case zenithdb.Record:
		_, _ = w.Write([]byte{valueRecord})
		writeRecord(w, typed)
	case []zenithdb.Record:
		_, _ = w.Write([]byte{valueRecordSlice})
		writeRecordSlice(w, typed)
	default:
		_, _ = w.Write([]byte{valueString})
		writeString(w, fmt.Sprint(typed))
	}
}

func readValue(r *bytes.Reader) (any, error) {
	kind, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	switch kind {
	case valueNil:
		return nil, nil
	case valueString:
		return readString(r)
	case valueInt64:
		return readInt64(r)
	case valueBool:
		return readBool(r)
	case valueFloat64:
		return readFloat64(r)
	case valueTime:
		nanos, err := readInt64(r)
		if err != nil {
			return nil, err
		}
		return time.Unix(0, nanos).UTC(), nil
	case valueRecord:
		return readRecord(r)
	case valueRecordSlice:
		return readRecordSlice(r)
	default:
		return nil, fmt.Errorf("unknown value kind %d", kind)
	}
}
