package nosqlite

import (
	"testing"
)

func TestInClause(t *testing.T) {
	c := In("id", "1", "2", "3")

	if got := c.Clause(); got != "(data->>'id' IN (?,?,?))" {
		t.Errorf("got = %v, want %v", got, "(data->>'id' IN (?,?,?))")
	}

	c = In("id", 1, 2, 3)

	if got := c.Clause(); got != "(data->>'id' IN (?,?,?))" {
		t.Errorf("got = %v, want %v", got, "(data->>'id' IN (?,?,?))")
	}
}

func TestBetweenClause(t *testing.T) {
	c := Between[int]("id", 1, 2)

	if got := c.Clause(); got != "(data->>'id' BETWEEN ? AND ?)" {
		t.Errorf("got = %v, want %v", got, "(data->>'id' BETWEEN ? AND ?)")
	}

	if got := c.Values(); got[0] != 1 || got[1] != 2 {
		t.Errorf("got = %v, want %v", got, []string{"1", "2"})
	}
}

func TestAndClauses(t *testing.T) {
	clauseOne := &condition[int]{
		Field:    "id",
		Operator: equalsOperator,
		Value:    1,
	}
	clauseTwo := &condition[string]{
		Field:    "name",
		Operator: equalsOperator,
		Value:    "test",
	}

	want := "((data->>'id' = ?) AND (data->>'name' = ?))"

	c := And(clauseOne, clauseTwo)
	if got := c.Clause(); got != want {
		t.Errorf("got = %v, want %v", got, want)
	}

	if got := c.Values(); got[0] != "1" || got[1] != "test" {
		t.Errorf("got = %v, want %v", got, []string{"1", "test"})
	}
}

func TestOrClauses(t *testing.T) {
	clauseOne := &condition[int]{
		Field:    "id",
		Operator: equalsOperator,
		Value:    1,
	}
	clauseTwo := &condition[string]{
		Field:    "name",
		Operator: equalsOperator,
		Value:    "test",
	}

	want := "((data->>'id' = ?) OR (data->>'name' = ?))"

	c := Or(clauseOne, clauseTwo)
	if got := c.Clause(); got != want {
		t.Errorf("got = %v, want %v", got, want)
	}

	if got := c.Values(); got[0] != "1" || got[1] != "test" {
		t.Errorf("got = %v, want %v", got, []string{"1", "test"})
	}
}

func TestAndOrClauses(t *testing.T) {
	clauseOne := &condition[int]{
		Field:    "id",
		Operator: equalsOperator,
		Value:    1,
	}
	clauseTwo := &condition[string]{
		Field:    "name",
		Operator: equalsOperator,
		Value:    "test",
	}
	clauseThree := &condition[string]{
		Field:    "foo",
		Operator: equalsOperator,
		Value:    "bar",
	}

	want := "(((data->>'id' = ?) AND (data->>'name' = ?)) OR (data->>'foo' = ?))"
	c1 := And(clauseOne, clauseTwo)
	c2 := Or(c1, clauseThree)

	if got := c2.Clause(); got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if got := c2.Values(); got[0] != "1" || got[1] != "test" || got[2] != "bar" {
		t.Errorf("got %v, want %v", got, []string{"1", "test", "bar"})
	}
}

func TestConditions(t *testing.T) {
	tests := []struct {
		condition      Clause
		expectedClause string
		expectedValues []string
	}{
		{
			condition:      Equal("id", 1),
			expectedClause: "(data->>'id' = ?)",
			expectedValues: []string{"1"},
		},
		{
			condition:      GreaterThan("id", 1),
			expectedClause: "(data->>'id' > ?)",
			expectedValues: []string{"1"},
		},
		{
			condition:      LessThan("id", 1),
			expectedClause: "(data->>'id' < ?)",
			expectedValues: []string{"1"},
		},
		{
			condition:      LessThanOrEqual("id", 1),
			expectedClause: "(data->>'id' <= ?)",
			expectedValues: []string{"1"},
		},
		{
			condition:      GreaterThanOrEqual("id", 1),
			expectedClause: "(data->>'id' >= ?)",
			expectedValues: []string{"1"},
		},
		{
			condition:      NotEqual("id", 1),
			expectedClause: "(data->>'id' != ?)",
			expectedValues: []string{"1"},
		},
		{
			condition:      Like("id", "%hello%"),
			expectedClause: "(data->>'id' LIKE ?)",
			expectedValues: []string{"%hello%"},
		},
	}

	for _, test := range tests {
		if got := test.condition.Clause(); got != test.expectedClause {
			t.Errorf("got = %v, want %v", got, test.expectedClause)
		}

		if got := test.condition.Values(); got[0] != test.expectedValues[0] {
			t.Errorf("got = %v, want %v", got, test.expectedValues)
		}
	}
}
