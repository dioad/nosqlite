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

// Clause represents a query condition that can be converted to SQL.
// It provides a fluent interface for combining multiple conditions using AND and OR operators.
type Clause interface {
	// Clause returns the SQL representation of the condition, using '?' as
	// placeholders for comparison values, and for the JSON field path too
	// when that path is bound rather than interpolated -- see jsonField and
	// Values, which supplies bound arguments in the matching order.
	Clause() string
	// Values returns the arguments to be used with the SQL query, in the
	// order their '?' placeholders appear in Clause: the JSON field
	// path(s) first if and only if Clause bound rather than interpolated
	// them, then the comparison value(s).
	Values() []any

	// And combines this clause with another one using the AND operator.
	And(c Clause) Clause
	// Or combines this clause with another one using the OR operator.
	Or(c Clause) Clause
}

// jsonFieldExpr is the SQL fragment used to extract a JSON field from the
// data column when the field path is supplied as a bound parameter (see
// jsonField) rather than interpolated into the SQL text, so a field value
// can never alter the query's structure.
const jsonFieldExpr = "data->>?"

// jsonField returns the SQL fragment that extracts a JSON field from the
// data column for a query clause, and whether that fragment (the first
// return value) interpolates field literally rather than binding it as a
// parameter (the second).
//
// field is interpolated literally (e.g. data->>'$.name') when it matches
// validIndexField, the same allow-listed JSON-path character set
// CreateIndex requires for its DDL. This is required for SQLite's query
// planner to recognize an expression index on that path: an index is
// matched by the exact expression text used to create it, so a bound
// parameter (data->>?) can never match an index built on a literal path,
// even though the two forms are semantically identical. Without this, every
// query clause silently lost its index match when the field path moved
// from interpolated text to a bound parameter (see jsonFieldExpr's prior
// history and TestTable_QueryOneInjectInField).
//
// field is left for the caller to bind via jsonFieldExpr instead when it
// does not match validIndexField, because interpolating it literally would
// then be unsafe -- field could alter the query's structure. A
// non-conforming field therefore never widens a match or produces invalid
// SQL, it just can't use an index.
func jsonField(field string) (string, bool) {
	if validIndexField.MatchString(field) {
		return fmt.Sprintf("data->>'%s'", field), true
	}
	return jsonFieldExpr, false
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

// And returns a Clause that combines multiple clauses with an AND operator.
// If no clauses are provided, it returns a clause that always evaluates to true.
func And(clauses ...Clause) Clause {
	return combine(andCombinator, clauses...)
}

// Or returns a Clause that combines multiple clauses with an OR operator.
func Or(clauses ...Clause) Clause {
	return combine(orCombinator, clauses...)
}

type condition[T string | number | bool] struct {
	Field        string
	Value        T
	Operator     operator
	fieldExpr    string
	fieldLiteral bool
}

func newCondition[T string | number | bool](field string, value T, op operator) *condition[T] {
	expr, literal := jsonField(field)
	return &condition[T]{Field: field, Value: value, Operator: op, fieldExpr: expr, fieldLiteral: literal}
}

func (c *condition[T]) Clause() string {
	return fmt.Sprintf("(%s %s ?)", c.fieldExpr, c.Operator)
}

func (c *condition[T]) Values() []any {
	if c.fieldLiteral {
		return []any{c.Value}
	}
	return []any{c.Field, c.Value}
}

func (c *condition[T]) And(cl Clause) Clause {
	return And(c, cl)
}

func (c *condition[T]) Or(cl Clause) Clause {
	return Or(c, cl)
}

// Equal returns a Clause that checks if a field is equal to a value.
func Equal[T string | number | bool](field string, value T) Clause {
	return newCondition(field, value, equalsOperator)
}

// True returns a Clause that checks if a boolean field is true (equal to 1).
func True(field string) Clause {
	return Equal(field, 1)
}

// False returns a Clause that checks if a boolean field is false (equal to 0).
func False(field string) Clause {
	return Equal(field, 0)
}

// LessThan returns a Clause that checks if a field is less than a value.
func LessThan[T string | number](field string, value T) Clause {
	return newCondition(field, value, lessThanOperator)
}

// GreaterThan returns a Clause that checks if a field is greater than a value.
func GreaterThan[T string | number](field string, value T) Clause {
	return newCondition(field, value, greaterThanOperator)
}

// LessThanOrEqual returns a Clause that checks if a field is less than or equal to a value.
func LessThanOrEqual[T string | number](field string, value T) Clause {
	return newCondition(field, value, lessThanOrEqualOperator)
}

// GreaterThanOrEqual returns a Clause that checks if a field is greater than or equal to a value.
func GreaterThanOrEqual[T string | number](field string, value T) Clause {
	return newCondition(field, value, greaterThanOrEqualOperator)
}

// All returns a Clause that matches all records.
func All() Clause {
	return And()
}

// NotEqual returns a Clause that checks if a field is not equal to a value.
func NotEqual[T string | number | bool](field string, value T) Clause {
	return newCondition(field, value, notEqualsOperator)
}

// Like returns a Clause that checks if a field matches a pattern using the SQL LIKE operator.
// It's up to the user to add the requisite % characters to the value.
func Like(field string, value string) Clause {
	return newCondition(field, value, likeOperator)
}

type inCondition struct {
	Field        string
	values       []any
	fieldExpr    string
	fieldLiteral bool
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

	return fmt.Sprintf("(%s IN (%s))", c.fieldExpr, values)
}

func (c *inCondition) Values() []any {
	if c.fieldLiteral {
		return c.values
	}
	return append([]any{c.Field}, c.values...)
}

func (c *inCondition) And(cl Clause) Clause {
	return And(c, cl)
}

func (c *inCondition) Or(cl Clause) Clause {
	return Or(c, cl)
}

// In returns a Clause that checks if a field is in a list of values.
func In(field string, values ...any) Clause {
	expr, literal := jsonField(field)
	return &inCondition{Field: field, values: values, fieldExpr: expr, fieldLiteral: literal}
}

type betweenCondition[T string | number] struct {
	Field        string
	From         T
	To           T
	fieldExpr    string
	fieldLiteral bool
}

func (c *betweenCondition[T]) Clause() string {
	return fmt.Sprintf("(%s BETWEEN ? AND ?)", c.fieldExpr)
}

func (c *betweenCondition[T]) Values() []any {
	if c.fieldLiteral {
		return []any{c.From, c.To}
	}
	return []any{c.Field, c.From, c.To}
}

func (c *betweenCondition[T]) And(cl Clause) Clause {
	return And(c, cl)
}

func (c *betweenCondition[T]) Or(cl Clause) Clause {
	return Or(c, cl)
}

// Between returns a Clause that checks if a field is between two values (inclusive).
func Between[T string | number](field string, from, to T) Clause {
	expr, literal := jsonField(field)
	return &betweenCondition[T]{Field: field, From: from, To: to, fieldExpr: expr, fieldLiteral: literal}
}

type containsCondition struct {
	Field        string
	combinator   combinator
	values       []any
	fieldExpr    string
	fieldLiteral bool
}

func (c *containsCondition) singleClause() string {
	return fmt.Sprintf("(EXISTS (SELECT 1 FROM json_each(%s) WHERE json_each.value = ?))", c.fieldExpr)
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
	if c.fieldLiteral {
		return c.values
	}
	// Each value has its own singleClause() fragment with its own
	// jsonFieldExpr placeholder, so the field must be repeated once per
	// value, immediately before that value, to match placeholder order.
	values := make([]any, 0, len(c.values)*2)
	for _, v := range c.values {
		values = append(values, c.Field, v)
	}

	return values
}

func (c *containsCondition) And(cl Clause) Clause {
	return And(c, cl)
}

func (c *containsCondition) Or(cl Clause) Clause {
	return Or(c, cl)
}

// Contains returns a Clause that checks if a JSON array field contains a single value.
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

	expr, literal := jsonField(field)
	return &containsCondition{Field: field, combinator: combinator, values: anyValues, fieldExpr: expr, fieldLiteral: literal}
}

// ContainsAll returns a Clause that checks if a JSON array field contains all the given values.
func ContainsAll[T string | number | bool](field string, values ...T) Clause {
	return andCondition(field, values)
}

// ContainsAny returns a Clause that checks if a JSON array field contains any of the given values.
func ContainsAny[T string | number | bool](field string, values ...T) Clause {
	return orCondition(field, values)
}
