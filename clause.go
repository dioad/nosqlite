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

func (c combinatorClause) Clause() string {
	joiner := fmt.Sprintf(" %s ", string(c.combinator))
	return fmt.Sprintf("(%s)", strings.Join(c.clauseStrings, joiner))
}

func (c combinatorClause) Values() []any {
	//valuesOne := slices.Clone(c.clauseOne.Values())
	return c.values
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

	return combinatorClause{
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

// Equal returns a clause that checks if a field is equal to a value
func Equal[T string | number](field string, value T) Clause {
	return &condition[T]{Field: field, Value: value, Operator: equalsOperator}
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

// NotEqual returns a clause that checks if a field is not equal to a value
func NotEqual[T string | number](field string, value T) Clause {
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
	return fmt.Sprintf("(EXISTS (SELECT 1 FROM json_each(%s) WHERE value = ?))", jsonField(c.Field))
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

// Contains returns a clause that checks if a list field contains a single value
func Contains(field string, value any) Clause {
	return &containsCondition{Field: field, combinator: andCombinator, values: []any{value}}
}

// ContainsAll returns a clause that checks if a list field contains all the values
func ContainsAll(field string, value ...any) Clause {
	return &containsCondition{Field: field, combinator: andCombinator, values: value}
}

// ContainsAny returns a clause that checks if a list field contains any of the values
func ContainsAny(field string, value ...any) Clause {
	return &containsCondition{Field: field, combinator: orCombinator, values: value}
}
