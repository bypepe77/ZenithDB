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
	protocolVersion uint16 = 1
	maxFramePayloadBytes = 64 << 20

	opCreate byte = iota + 1
	opUpdate
	opDelete
	opUpsert
	opBatch
	opCreateMany
	opUpdateMany
	opDeleteMany
	opFindUnique
	opFindMany
	opCount
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
	if len(payload) > maxFramePayloadBytes {
		return fmt.Errorf("wire frame payload exceeds %d bytes", maxFramePayloadBytes)
	}
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
	if size > maxFramePayloadBytes {
		return 0, nil, fmt.Errorf("wire frame payload exceeds %d bytes", maxFramePayloadBytes)
	}
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

func writeUint16(w io.Writer, value uint16) {
	var raw [2]byte
	binary.BigEndian.PutUint16(raw[:], value)
	_, _ = w.Write(raw[:])
}

func readUint16FromReader(r io.Reader) (uint16, error) {
	var raw [2]byte
	if _, err := io.ReadFull(r, raw[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(raw[:]), nil
}

func readStringFromReader(r io.Reader) (string, error) {
	var rawSize [4]byte
	if _, err := io.ReadFull(r, rawSize[:]); err != nil {
		return "", err
	}
	size := binary.BigEndian.Uint32(rawSize[:])
	if size > maxFramePayloadBytes {
		return "", fmt.Errorf("wire string exceeds %d bytes", maxFramePayloadBytes)
	}
	raw := make([]byte, size)
	if _, err := io.ReadFull(r, raw); err != nil {
		return "", err
	}
	return string(raw), nil
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
	writeFilterMap(w, query.Filters)
	writeString(w, query.Index)
	writeInt64(w, int64(query.Limit))
	writeInt64(w, int64(query.Skip))
	writeStringMap(w, query.Cursor)
	writeOrderBy(w, query.OrderBy)
	writeIncludeMap(w, query.Include)
}

func readQuery(r *bytes.Reader) (zenithdb.Query, error) {
	where, err := readStringMap(r)
	if err != nil {
		return zenithdb.Query{}, err
	}
	filters, err := readFilterMap(r)
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
	skip, err := readInt64(r)
	if err != nil {
		return zenithdb.Query{}, err
	}
	cursor, err := readStringMap(r)
	if err != nil {
		return zenithdb.Query{}, err
	}
	orderBy, err := readOrderBy(r)
	if err != nil {
		return zenithdb.Query{}, err
	}
	include, err := readIncludeMap(r)
	if err != nil {
		return zenithdb.Query{}, err
	}
	return zenithdb.Query{Where: where, Filters: filters, Index: index, Limit: int(limit), Skip: int(skip), Cursor: cursor, OrderBy: orderBy, Include: include}, nil
}

func writeFilterMap(w io.Writer, filters map[string]zenithdb.Filter) {
	writeUint32(w, uint32(len(filters)))
	for field, filter := range filters {
		writeString(w, field)
		writeValue(w, filter.Equals)
		writeUint32(w, uint32(len(filter.In)))
		for _, value := range filter.In {
			writeValue(w, value)
		}
		writeString(w, filter.Contains)
		writeValue(w, filter.GT)
		writeValue(w, filter.GTE)
		writeValue(w, filter.LT)
		writeValue(w, filter.LTE)
	}
}

func readFilterMap(r *bytes.Reader) (map[string]zenithdb.Filter, error) {
	size, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	if size == 0 {
		return nil, nil
	}
	filters := make(map[string]zenithdb.Filter, size)
	for i := uint32(0); i < size; i++ {
		field, err := readString(r)
		if err != nil {
			return nil, err
		}
		equals, err := readValue(r)
		if err != nil {
			return nil, err
		}
		inSize, err := readUint32(r)
		if err != nil {
			return nil, err
		}
		in := make([]any, 0, inSize)
		for j := uint32(0); j < inSize; j++ {
			value, err := readValue(r)
			if err != nil {
				return nil, err
			}
			in = append(in, value)
		}
		contains, err := readString(r)
		if err != nil {
			return nil, err
		}
		gt, err := readValue(r)
		if err != nil {
			return nil, err
		}
		gte, err := readValue(r)
		if err != nil {
			return nil, err
		}
		lt, err := readValue(r)
		if err != nil {
			return nil, err
		}
		lte, err := readValue(r)
		if err != nil {
			return nil, err
		}
		filters[field] = zenithdb.Filter{Equals: equals, In: in, Contains: contains, GT: gt, GTE: gte, LT: lt, LTE: lte}
	}
	return filters, nil
}

func writeOrderBy(w io.Writer, orderBy []zenithdb.OrderBy) {
	writeUint32(w, uint32(len(orderBy)))
	for _, order := range orderBy {
		writeString(w, order.Field)
		writeString(w, string(order.Direction))
	}
}

func readOrderBy(r *bytes.Reader) ([]zenithdb.OrderBy, error) {
	size, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	orderBy := make([]zenithdb.OrderBy, 0, size)
	for i := uint32(0); i < size; i++ {
		field, err := readString(r)
		if err != nil {
			return nil, err
		}
		direction, err := readString(r)
		if err != nil {
			return nil, err
		}
		orderBy = append(orderBy, zenithdb.OrderBy{Field: field, Direction: zenithdb.SortDirection(direction)})
	}
	return orderBy, nil
}

func writeBatchOperations(w io.Writer, operations []zenithdb.BatchOperation) {
	writeUint32(w, uint32(len(operations)))
	for _, operation := range operations {
		writeString(w, string(operation.Type))
		writeString(w, operation.Model)
		writeStringMap(w, operation.Where)
		writeRecord(w, operation.Record)
	}
}

func readBatchOperations(r *bytes.Reader) ([]zenithdb.BatchOperation, error) {
	size, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	operations := make([]zenithdb.BatchOperation, 0, size)
	for i := uint32(0); i < size; i++ {
		operationType, err := readString(r)
		if err != nil {
			return nil, err
		}
		model, err := readString(r)
		if err != nil {
			return nil, err
		}
		where, err := readStringMap(r)
		if err != nil {
			return nil, err
		}
		record, err := readRecord(r)
		if err != nil {
			return nil, err
		}
		operations = append(operations, zenithdb.BatchOperation{Type: zenithdb.BatchOperationType(operationType), Model: model, Where: where, Record: record})
	}
	return operations, nil
}

func writeBatchResults(w io.Writer, results []zenithdb.BatchResult) {
	writeUint32(w, uint32(len(results)))
	for _, result := range results {
		writeString(w, string(result.Type))
		writeString(w, result.Model)
		writeString(w, result.Key)
		writeRecord(w, result.Record)
	}
}

func readBatchResults(r *bytes.Reader) ([]zenithdb.BatchResult, error) {
	size, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	results := make([]zenithdb.BatchResult, 0, size)
	for i := uint32(0); i < size; i++ {
		operationType, err := readString(r)
		if err != nil {
			return nil, err
		}
		model, err := readString(r)
		if err != nil {
			return nil, err
		}
		key, err := readString(r)
		if err != nil {
			return nil, err
		}
		record, err := readRecord(r)
		if err != nil {
			return nil, err
		}
		results = append(results, zenithdb.BatchResult{Type: zenithdb.BatchOperationType(operationType), Model: model, Key: key, Record: record})
	}
	return results, nil
}

func writeMutationResults(w io.Writer, results []zenithdb.MutationResult) {
	writeUint32(w, uint32(len(results)))
	for _, result := range results {
		writeString(w, result.Model)
		writeString(w, result.Key)
	}
}

func readMutationResults(r *bytes.Reader) ([]zenithdb.MutationResult, error) {
	size, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	results := make([]zenithdb.MutationResult, 0, size)
	for i := uint32(0); i < size; i++ {
		model, err := readString(r)
		if err != nil {
			return nil, err
		}
		key, err := readString(r)
		if err != nil {
			return nil, err
		}
		results = append(results, zenithdb.MutationResult{Model: model, Key: key})
	}
	return results, nil
}

func writeManyResult(w io.Writer, result zenithdb.ManyResult) {
	writeString(w, result.Model)
	writeInt64(w, int64(result.Count))
}

func readManyResult(r *bytes.Reader) (zenithdb.ManyResult, error) {
	model, err := readString(r)
	if err != nil {
		return zenithdb.ManyResult{}, err
	}
	count, err := readInt64(r)
	if err != nil {
		return zenithdb.ManyResult{}, err
	}
	return zenithdb.ManyResult{Model: model, Count: int(count)}, nil
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
