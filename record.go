package zenithdb

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Record stores one model instance.
type Record map[string]any

func cloneRecord(record Record) Record {
	cloned := make(Record, len(record))
	for key, value := range record {
		cloned[key] = value
	}
	return cloned
}

func normalizeRecord(model Model, record Record) (Record, error) {
	return normalizeValues(model, record, true)
}

func normalizePartial(model Model, values map[string]any) (Record, error) {
	return normalizeValues(model, values, false)
}

func normalizeValues(model Model, values map[string]any, requireRequired bool) (Record, error) {
	normalized := make(Record, len(values))
	fields := make(map[string]Field, len(model.Fields))
	for _, field := range model.Fields {
		fields[field.Name] = field
		if requireRequired && field.Required {
			if _, ok := values[field.Name]; !ok {
				return nil, fmt.Errorf("model %q requires field %q", model.Name, field.Name)
			}
		}
	}

	for key, value := range values {
		field, ok := fields[key]
		if !ok {
			return nil, fmt.Errorf("model %q does not define field %q", model.Name, key)
		}
		normalizedValue, err := normalizeValue(field.Kind, value)
		if err != nil {
			return nil, fmt.Errorf("model %q field %q: %w", model.Name, key, err)
		}
		normalized[key] = normalizedValue
	}

	return normalized, nil
}

func normalizeValue(kind FieldKind, value any) (any, error) {
	if value == nil {
		return nil, nil
	}

	switch kind {
	case FieldString:
		if typed, ok := value.(string); ok {
			return typed, nil
		}
		return nil, fmt.Errorf("expected string")
	case FieldInt64:
		switch typed := value.(type) {
		case int:
			return int64(typed), nil
		case int8:
			return int64(typed), nil
		case int16:
			return int64(typed), nil
		case int32:
			return int64(typed), nil
		case int64:
			return typed, nil
		case uint:
			return int64(typed), nil
		case uint8:
			return int64(typed), nil
		case uint16:
			return int64(typed), nil
		case uint32:
			return int64(typed), nil
		case uint64:
			return int64(typed), nil
		case json.Number:
			value, err := typed.Int64()
			if err != nil {
				return nil, err
			}
			return value, nil
		default:
			return nil, fmt.Errorf("expected integer")
		}
	case FieldBool:
		if typed, ok := value.(bool); ok {
			return typed, nil
		}
		return nil, fmt.Errorf("expected bool")
	case FieldFloat:
		switch typed := value.(type) {
		case float32:
			return float64(typed), nil
		case float64:
			return typed, nil
		case json.Number:
			value, err := typed.Float64()
			if err != nil {
				return nil, err
			}
			return value, nil
		default:
			return nil, fmt.Errorf("expected float")
		}
	case FieldTime:
		switch typed := value.(type) {
		case time.Time:
			return typed.UTC(), nil
		case int64:
			return time.Unix(0, typed).UTC(), nil
		case json.Number:
			nanos, err := typed.Int64()
			if err != nil {
				return nil, err
			}
			return time.Unix(0, nanos).UTC(), nil
		case string:
			parsed, err := time.Parse(time.RFC3339Nano, typed)
			if err != nil {
				return nil, err
			}
			return parsed.UTC(), nil
		default:
			return nil, fmt.Errorf("expected time.Time, int64, or RFC3339 string")
		}
	default:
		return nil, fmt.Errorf("unsupported field kind %q", kind)
	}
}

func keyFromRecord(record Record, fields []string) (string, error) {
	values := make([]any, len(fields))
	for i, field := range fields {
		value, ok := record[field]
		if !ok {
			return "", fmt.Errorf("missing key field %q", field)
		}
		values[i] = value
	}
	return encodeKey(values), nil
}

func keyFromValues(values map[string]any, fields []string) (string, error) {
	ordered := make([]any, len(fields))
	for i, field := range fields {
		value, ok := values[field]
		if !ok {
			return "", fmt.Errorf("missing key field %q", field)
		}
		ordered[i] = value
	}
	return encodeKey(ordered), nil
}

func encodeKey(values []any) string {
	var builder strings.Builder
	for _, value := range values {
		encoded := encodeValue(value)
		builder.WriteString(strconv.Itoa(len(encoded)))
		builder.WriteByte(':')
		builder.WriteString(encoded)
		builder.WriteByte('|')
	}
	return builder.String()
}

func encodeValue(value any) string {
	switch typed := value.(type) {
	case string:
		return "s:" + typed
	case int:
		return "i:" + strconv.FormatInt(int64(typed), 10)
	case int8:
		return "i:" + strconv.FormatInt(int64(typed), 10)
	case int16:
		return "i:" + strconv.FormatInt(int64(typed), 10)
	case int32:
		return "i:" + strconv.FormatInt(int64(typed), 10)
	case int64:
		return "i:" + strconv.FormatInt(typed, 10)
	case uint:
		return "u:" + strconv.FormatUint(uint64(typed), 10)
	case uint8:
		return "u:" + strconv.FormatUint(uint64(typed), 10)
	case uint16:
		return "u:" + strconv.FormatUint(uint64(typed), 10)
	case uint32:
		return "u:" + strconv.FormatUint(uint64(typed), 10)
	case uint64:
		return "u:" + strconv.FormatUint(typed, 10)
	case bool:
		return "b:" + strconv.FormatBool(typed)
	case float32:
		return "f:" + strconv.FormatFloat(float64(typed), 'g', -1, 32)
	case float64:
		return "f:" + strconv.FormatFloat(typed, 'g', -1, 64)
	case time.Time:
		return "t:" + typed.UTC().Format(time.RFC3339Nano)
	default:
		return fmt.Sprintf("%T:%v", value, value)
	}
}

func sortedIDs(ids map[string]struct{}) []string {
	result := make([]string, 0, len(ids))
	for id := range ids {
		result = append(result, id)
	}
	sort.Strings(result)
	return result
}
