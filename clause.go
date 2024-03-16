package nosqlite

import (
	"fmt"
	"slices"
	"strings"

	"golang.org/x/exp/constraints"
)

type operator string

var (
	equalsOperator             operator = "="
	lessThanOperator           operator = "<"
	greaterThanOperator        operator = ">"
	lessThanOrEqualOperator    operator = "<="
	greaterThanOrEqualOperator operator = ">="
	notEqualsOperator          operator = "!="
	likeOperator               operator = "LIKE"
)

type number interface {
	constraints.Integer | constraints.Float
}

type Clause interface {
	Clause() string
	Values() []any
}

func jsonField(field string) string {
	return fmt.Sprintf("data->>'%s'", field)
}

type andClause struct {
	clauseOne Clause
	clauseTwo Clause
}

func (c andClause) Clause() string {
	return fmt.Sprintf("(%s AND %s)", c.clauseOne.Clause(), c.clauseTwo.Clause())
}

func (c andClause) Values() []any {
	valuesOne := slices.Clone(c.clauseOne.Values())
	return append(valuesOne, c.clauseTwo.Values()...)
}

func And(clauseOne, clauseTwo Clause) Clause {
	return andClause{clauseOne: clauseOne, clauseTwo: clauseTwo}
}

type orClause struct {
	clauseOne Clause
	clauseTwo Clause
}

func (c orClause) Clause() string {
	return fmt.Sprintf("(%s OR %s)", c.clauseOne.Clause(), c.clauseTwo.Clause())
}

func (c orClause) Values() []any {
	valuesOne := slices.Clone(c.clauseOne.Values())
	return append(valuesOne, c.clauseTwo.Values()...)
}

func Or(clauseOne, clauseTwo Clause) Clause {
	return orClause{clauseOne: clauseOne, clauseTwo: clauseTwo}
}

type condition[T string | number] struct {
	Field    string
	Value    T
	Operator operator
}

func (c *condition[T]) Clause() string {
	return fmt.Sprintf("(%s %s ?)", jsonField(c.Field), c.Operator)
}

func (c *condition[T]) Values() []any {
	return []any{fmt.Sprintf("%v", c.Value)}
}

func Equal[T string | number](field string, value T) Clause {
	return &condition[T]{Field: field, Value: value, Operator: equalsOperator}
}

func LessThan[T string | number](field string, value T) Clause {
	return &condition[T]{Field: field, Value: value, Operator: lessThanOperator}
}

func GreaterThan[T string | number](field string, value T) Clause {
	return &condition[T]{Field: field, Value: value, Operator: greaterThanOperator}
}

func LessThanOrEqual[T string | number](field string, value T) Clause {
	return &condition[T]{Field: field, Value: value, Operator: lessThanOrEqualOperator}
}

func GreaterThanOrEqual[T string | number](field string, value T) Clause {
	return &condition[T]{Field: field, Value: value, Operator: greaterThanOrEqualOperator}
}

func NotEqual[T string | number](field string, value T) Clause {
	return &condition[T]{Field: field, Value: value, Operator: notEqualsOperator}
}

func Like(field string, value string) Clause {
	return &condition[string]{Field: field, Value: value, Operator: likeOperator}
}

type inCondition struct {
	Field  string
	values []any
}

func mapToParameter(values []any) []string {
	s := make([]string, len(values))
	for i := range values {
		s[i] = "?"
	}
	return s
}

func (c *inCondition) Clause() string {
	values := strings.Join(mapToParameter(c.values), ",")
	return fmt.Sprintf("(%s IN (%s))", jsonField(c.Field), values)
}

func (c *inCondition) Values() []any {
	return c.values
}

func In(field string, values ...any) Clause {
	return &inCondition{Field: field, values: values}
}

type betweenCondition[T string | number] struct {
	Field string
	From  T
	To    T
}

func (c *betweenCondition[T]) Clause() string {
	return fmt.Sprintf("(%s BETWEEN ? AND ?)", jsonField(c.Field))
}

func (c *betweenCondition[T]) Values() []any {
	return []any{c.From, c.To}
}

func Between[T string | number](field string, from, to T) Clause {
	return &betweenCondition[T]{Field: field, From: from, To: to}
}
