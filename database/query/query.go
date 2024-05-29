package query

import (
	"encoding/json"
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
		return compare(value, c.Value) > 0
	case OpGreaterEqual:
		return compare(value, c.Value) >= 0
	case OpLessThan:
		return compare(value, c.Value) < 0
	case OpLessEqual:
		return compare(value, c.Value) <= 0
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
	jsonData, _ := json.Marshal(data)
	return jsonData
}

func compare(a, b interface{}) int {
	aInt, ok := a.(int)
	if !ok {
		return 0
	}
	bInt, ok := b.(int)
	if !ok {
		return 0
	}
	if aInt < bInt {
		return -1
	} else if aInt > bInt {
		return 1
	} else {
		return 0
	}
}
