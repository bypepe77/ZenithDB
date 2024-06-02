package query

import (
	"encoding/json"
	"errors"
	"reflect"

	"github.com/bypepe77/ZenithDB/database/document"
	"github.com/tidwall/gjson"
)

type Operator string

const (
	OpEqual        Operator = "="
	OpNotEqual     Operator = "!="
	OpGreaterThan  Operator = ">"
	OpGreaterEqual Operator = ">="
	OpLessThan     Operator = "<"
	OpLessEqual    Operator = "<="
)

type Condition struct {
	Field    string
	Operator Operator
	Value    interface{}
}

type Query struct {
	Conditions []Condition
}

func NewQuery() *Query {
	return &Query{
		Conditions: make([]Condition, 0),
	}
}

func (q *Query) Where(field string, operator Operator, value interface{}) *Query {
	q.Conditions = append(q.Conditions, Condition{
		Field:    field,
		Operator: operator,
		Value:    value,
	})
	return q
}

func (q *Query) Matches(doc *document.Document) bool {
	for _, condition := range q.Conditions {
		if !condition.Matches(doc) {
			return false
		}
	}
	return true
}

func (c *Condition) Matches(doc *document.Document) bool {
	value := getFieldValue(doc.Data, c.Field)

	switch c.Operator {
	case OpEqual:
		return reflect.DeepEqual(value, c.Value)
	case OpNotEqual:
		return !reflect.DeepEqual(value, c.Value)
	case OpGreaterThan:
		result, err := compare(value, c.Value)
		return err == nil && result > 0
	case OpGreaterEqual:
		result, err := compare(value, c.Value)
		return err == nil && result >= 0
	case OpLessThan:
		result, err := compare(value, c.Value)
		return err == nil && result < 0
	case OpLessEqual:
		result, err := compare(value, c.Value)
		return err == nil && result <= 0
	default:
		return false
	}
}

func getFieldValue(data interface{}, path string) interface{} {
	result := gjson.GetBytes(toJSON(data), path)

	if result.Exists() {
		return result.Value()
	}
	return nil
}

func toJSON(data interface{}) []byte {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil
	}
	return jsonData
}

func compare(a, b interface{}) (int, error) {
	v1 := reflect.ValueOf(a)
	v2 := reflect.ValueOf(b)

	if v1.Type() != v2.Type() {
		return 0, errors.New("type mismatch")
	}

	switch v1.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		aInt := v1.Int()
		bInt := v2.Int()
		switch {
		case aInt < bInt:
			return -1, nil
		case aInt > bInt:
			return 1, nil
		default:
			return 0, nil
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		aUint := v1.Uint()
		bUint := v2.Uint()
		switch {
		case aUint < bUint:
			return -1, nil
		case aUint > bUint:
			return 1, nil
		default:
			return 0, nil
		}
	case reflect.Float32, reflect.Float64:
		aFloat := v1.Float()
		bFloat := v2.Float()
		switch {
		case aFloat < bFloat:
			return -1, nil
		case aFloat > bFloat:
			return 1, nil
		default:
			return 0, nil
		}
	case reflect.String:
		aStr := v1.String()
		bStr := v2.String()
		switch {
		case aStr < bStr:
			return -1, nil
		case aStr > bStr:
			return 1, nil
		default:
			return 0, nil
		}
	default:
		return 0, errors.New("unsupported type")
	}
}
