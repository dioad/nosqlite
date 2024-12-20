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

	if got := c.Values(); got[0] != 1 || got[1] != "test" {
		t.Errorf("got = %v, want %v", got, []any{1, "test"})
	}
}

func TestAndClausesFluent(t *testing.T) {
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

	c := clauseOne.And(clauseTwo)
	if got := c.Clause(); got != want {
		t.Errorf("got = %v, want %v", got, want)
	}

	if got := c.Values(); got[0] != 1 || got[1] != "test" {
		t.Errorf("got = %v, want %v", got, []any{1, "test"})
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

	if got := c.Values(); got[0] != 1 || got[1] != "test" {
		t.Errorf("got = %v, want %v", got, []any{1, "test"})
	}
}

func TestOrClausesFluent(t *testing.T) {
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

	c := clauseOne.Or(clauseTwo)
	if got := c.Clause(); got != want {
		t.Errorf("got = %v, want %v", got, want)
	}

	if got := c.Values(); got[0] != 1 || got[1] != "test" {
		t.Errorf("got = %v, want %v", got, []any{1, "test"})
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

	if got := c2.Values(); got[0] != 1 || got[1] != "test" || got[2] != "bar" {
		t.Errorf("got %v, want %v", got, []any{1, "test", "bar"})
	}
}

func TestAndOrClausesFluent(t *testing.T) {
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

	c := clauseOne.And(clauseTwo).Or(clauseThree)

	if got := c.Clause(); got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if got := c.Values(); got[0] != 1 || got[1] != "test" || got[2] != "bar" {
		t.Errorf("got %v, want %v", got, []any{1, "test", "bar"})
	}
}

func TestConditions(t *testing.T) {
	tests := []struct {
		condition      Clause
		expectedClause string
		expectedValues []any
	}{
		{
			condition:      Equal("id", 1),
			expectedClause: "(data->>'id' = ?)",
			expectedValues: []any{1},
		},
		{
			condition:      GreaterThan("id", 1),
			expectedClause: "(data->>'id' > ?)",
			expectedValues: []any{1},
		},
		{
			condition:      LessThan("id", 1),
			expectedClause: "(data->>'id' < ?)",
			expectedValues: []any{1},
		},
		{
			condition:      LessThanOrEqual("id", 1),
			expectedClause: "(data->>'id' <= ?)",
			expectedValues: []any{1},
		},
		{
			condition:      GreaterThanOrEqual("id", 1),
			expectedClause: "(data->>'id' >= ?)",
			expectedValues: []any{1},
		},
		{
			condition:      NotEqual("id", 1),
			expectedClause: "(data->>'id' != ?)",
			expectedValues: []any{1},
		},
		{
			condition:      Like("id", "%hello%"),
			expectedClause: "(data->>'id' LIKE ?)",
			expectedValues: []any{"%hello%"},
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

func TestContains(t *testing.T) {
	c := Contains("$.list", "one")

	expected := "(EXISTS (SELECT 1 FROM json_each(data->>'$.list') WHERE json_each.value = ?))"

	if got := c.Clause(); got != expected {
		t.Errorf("got = %v, want %v", got, expected)
	}
}

func TestContainsAll(t *testing.T) {
	c := ContainsAll("$.list", "one", "two")

	expected := "((EXISTS (SELECT 1 FROM json_each(data->>'$.list') WHERE json_each.value = ?)) AND (EXISTS (SELECT 1 FROM json_each(data->>'$.list') WHERE json_each.value = ?)))"

	if got := c.Clause(); got != expected {
		t.Errorf("got = %v, want %v", got, expected)
	}
}

func TestContainsAny(t *testing.T) {
	c := ContainsAny("$.list", "one", "two")

	expected := "((EXISTS (SELECT 1 FROM json_each(data->>'$.list') WHERE json_each.value = ?)) OR (EXISTS (SELECT 1 FROM json_each(data->>'$.list') WHERE json_each.value = ?)))"

	if got := c.Clause(); got != expected {
		t.Errorf("got = %v, want %v", got, expected)
	}
}

func TestTrueClause(t *testing.T) {
	c := True("$.approved")

	expected := "(data->>'$.approved' = ?)"

	if got := c.Clause(); got != expected {
		t.Errorf("got = %v, want %v", got, expected)
	}
}

func TestFalseClause(t *testing.T) {
	c := False("$.approved")

	expected := "(data->>'$.approved' = ?)"

	if got := c.Clause(); got != expected {
		t.Errorf("got = %v, want %v", got, expected)
	}
}
