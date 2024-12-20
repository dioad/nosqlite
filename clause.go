package nosqlite

import (
	"fmt"
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

type combinator string

var (
	andCombinator combinator = "AND"
	orCombinator  combinator = "OR"
)

type number interface {
	constraints.Integer | constraints.Float
}

// Clause represents a clause in a query
type Clause interface {
	// Clause returns the clause as a string using parameters for values
	Clause() string
	// Values returns the values to assign to the parameters in the clause
	Values() []any

	And(c Clause) Clause
	Or(c Clause) Clause
}

func jsonField(field string) string {
	return fmt.Sprintf("data->>'%s'", field)
}

type combinatorClause struct {
	combinator    combinator
	clauses       []Clause
	clauseStrings []string
	values        []any
}

func (c *combinatorClause) Clause() string {
	if len(c.clauses) == 0 {
		return "(1 == 1)"
	}
	joiner := fmt.Sprintf(" %s ", string(c.combinator))

	return fmt.Sprintf("(%s)", strings.Join(c.clauseStrings, joiner))
}

func (c *combinatorClause) Values() []any {
	// valuesOne := slices.Clone(c.clauseOne.Values())
	return c.values
}

func (c *combinatorClause) And(cl Clause) Clause {
	return And(c, cl)
}

func (c *combinatorClause) Or(cl Clause) Clause {
	return Or(c, cl)
}

func combine(combinator combinator, clauses ...Clause) Clause {
	clauseStrings := make([]string, len(clauses))
	for i, clause := range clauses {
		clauseStrings[i] = clause.Clause()
	}

	values := make([]any, 0, len(clauses))
	for _, clause := range clauses {
		values = append(values, clause.Values()...)
	}

	return &combinatorClause{
		combinator:    combinator,
		clauses:       clauses,
		clauseStrings: clauseStrings,
		values:        values,
	}
}

// And returns a clause that combines clauses with an AND operator
func And(clauses ...Clause) Clause {
	return combine(andCombinator, clauses...)
}

// Or returns a clause that combines clauses with an OR operator
func Or(clauses ...Clause) Clause {
	return combine(orCombinator, clauses...)
}

type condition[T string | number | bool] struct {
	Field    string
	Value    T
	Operator operator
}

func (c *condition[T]) Clause() string {
	return fmt.Sprintf("(%s %s ?)", jsonField(c.Field), c.Operator)
}

func (c *condition[T]) Values() []any {
	switch v := any(c.Value).(type) {
	case string:
		return []any{v}
	case int, float64, bool:
		return []any{v}
	default:
		return []any{fmt.Sprintf("%v", v)}
	}
	// return []any{fmt.Sprintf("%v", c.Value)}
}

func (c *condition[T]) And(cl Clause) Clause {
	return And(c, cl)
}

func (c *condition[T]) Or(cl Clause) Clause {
	return Or(c, cl)
}

// Equal returns a clause that checks if a field is equal to a value
func Equal[T string | number | bool](field string, value T) Clause {
	return &condition[T]{Field: field, Value: value, Operator: equalsOperator}
}

func True(field string) Clause {
	return Equal(field, 1)
}

func False(field string) Clause {
	return Equal(field, 0)
}

// LessThan returns a clause that checks if a field is less than a value
func LessThan[T string | number](field string, value T) Clause {
	return &condition[T]{Field: field, Value: value, Operator: lessThanOperator}
}

// GreaterThan returns a clause that checks if a field is greater than a value
func GreaterThan[T string | number](field string, value T) Clause {
	return &condition[T]{Field: field, Value: value, Operator: greaterThanOperator}
}

// LessThanOrEqual returns a clause that checks if a field is less than or equal to a value
func LessThanOrEqual[T string | number](field string, value T) Clause {
	return &condition[T]{Field: field, Value: value, Operator: lessThanOrEqualOperator}
}

// GreaterThanOrEqual returns a clause that checks if a field is greater than or equal to a value
func GreaterThanOrEqual[T string | number](field string, value T) Clause {
	return &condition[T]{Field: field, Value: value, Operator: greaterThanOrEqualOperator}
}

func All() Clause {
	return And()
}

// NotEqual returns a clause that checks if a field is not equal to a value
func NotEqual[T string | number | bool](field string, value T) Clause {
	return &condition[T]{Field: field, Value: value, Operator: notEqualsOperator}
}

// Like returns a clause that checks if a field is like a value
// It's up to the user to add the requisite % characters
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

func (c *inCondition) And(cl Clause) Clause {
	return And(c, cl)
}

func (c *inCondition) Or(cl Clause) Clause {
	return Or(c, cl)
}

// In returns a clause that checks if a field is in a list of values
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

func (c *betweenCondition[T]) And(cl Clause) Clause {
	return And(c, cl)
}

func (c *betweenCondition[T]) Or(cl Clause) Clause {
	return Or(c, cl)
}

// Between returns a clause that checks if a field is between two values
func Between[T string | number](field string, from, to T) Clause {
	return &betweenCondition[T]{Field: field, From: from, To: to}
}

type containsCondition struct {
	Field      string
	combinator combinator
	values     []any
}

func (c *containsCondition) singleClause() string {
	return fmt.Sprintf("(EXISTS (SELECT 1 FROM json_each(%s) WHERE json_each.value = ?))", jsonField(c.Field))
}

func (c *containsCondition) Clause() string {
	if len(c.values) == 1 {
		return c.singleClause()
	}
	clauses := make([]string, len(c.values))
	for i := range c.values {
		clauses[i] = c.singleClause()
	}
	return fmt.Sprintf("(%s)", strings.Join(clauses, fmt.Sprintf(" %s ", c.combinator)))
}

func (c *containsCondition) Values() []any {
	return c.values
}

func (c *containsCondition) And(cl Clause) Clause {
	return And(c, cl)
}

func (c *containsCondition) Or(cl Clause) Clause {
	return Or(c, cl)
}

// Contains returns a clause that checks if a list field contains a single value
func Contains[T string | number | bool](field string, value T) Clause {
	return ContainsAll(field, value)
}

func andCondition[T string | number | bool](field string, values []T) Clause {
	return newContainsCondition(field, andCombinator, values)
}

func orCondition[T string | number | bool](field string, values []T) Clause {
	return newContainsCondition(field, orCombinator, values)
}

func newContainsCondition[T string | number | bool](field string, combinator combinator, values []T) Clause {
	anyValues := make([]any, len(values))
	for i, tag := range values {
		anyValues[i] = tag
	}
	return &containsCondition{Field: field, combinator: combinator, values: anyValues}
}

func ContainsAll[T string | number | bool](field string, values ...T) Clause {
	return andCondition(field, values)
}

func ContainsAny[T string | number | bool](field string, values ...T) Clause {
	return orCondition(field, values)
}
