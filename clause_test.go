package nosqlite

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInClause(t *testing.T) {
	t.Parallel()

	c := In("id", "1", "2", "3")
	assert.Equal(t, "(data->>? IN (?,?,?))", c.Clause())
	assert.Equal(t, []any{"id", "1", "2", "3"}, c.Values())

	c = In("id", 1, 2, 3)
	assert.Equal(t, "(data->>? IN (?,?,?))", c.Clause())
	assert.Equal(t, []any{"id", 1, 2, 3}, c.Values())
}

func TestBetweenClause(t *testing.T) {
	t.Parallel()

	c := Between[int]("id", 1, 2)
	assert.Equal(t, "(data->>? BETWEEN ? AND ?)", c.Clause())
	assert.Equal(t, []any{"id", 1, 2}, c.Values())
}

func TestAndClauses(t *testing.T) {
	t.Parallel()

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

	c := And(clauseOne, clauseTwo)
	assert.Equal(t, "((data->>? = ?) AND (data->>? = ?))", c.Clause())
	assert.Equal(t, []any{"id", 1, "name", "test"}, c.Values())
}

func TestAndClausesFluent(t *testing.T) {
	t.Parallel()

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

	c := clauseOne.And(clauseTwo)
	assert.Equal(t, "((data->>? = ?) AND (data->>? = ?))", c.Clause())
	assert.Equal(t, []any{"id", 1, "name", "test"}, c.Values())
}

func TestOrClauses(t *testing.T) {
	t.Parallel()

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

	c := Or(clauseOne, clauseTwo)
	assert.Equal(t, "((data->>? = ?) OR (data->>? = ?))", c.Clause())
	assert.Equal(t, []any{"id", 1, "name", "test"}, c.Values())
}

func TestOrClausesFluent(t *testing.T) {
	t.Parallel()

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

	c := clauseOne.Or(clauseTwo)
	assert.Equal(t, "((data->>? = ?) OR (data->>? = ?))", c.Clause())
	assert.Equal(t, []any{"id", 1, "name", "test"}, c.Values())
}

func TestAndOrClauses(t *testing.T) {
	t.Parallel()

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

	c1 := And(clauseOne, clauseTwo)
	c2 := Or(c1, clauseThree)

	assert.Equal(t, "(((data->>? = ?) AND (data->>? = ?)) OR (data->>? = ?))", c2.Clause())
	assert.Equal(t, []any{"id", 1, "name", "test", "foo", "bar"}, c2.Values())
}

func TestAndOrClausesFluent(t *testing.T) {
	t.Parallel()

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

	c := clauseOne.And(clauseTwo).Or(clauseThree)

	assert.Equal(t, "(((data->>? = ?) AND (data->>? = ?)) OR (data->>? = ?))", c.Clause())
	assert.Equal(t, []any{"id", 1, "name", "test", "foo", "bar"}, c.Values())
}

func TestConditions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		condition      Clause
		expectedClause string
		expectedValue  any
	}{
		{
			condition:      Equal("id", 1),
			expectedClause: "(data->>? = ?)",
			expectedValue:  1,
		},
		{
			condition:      GreaterThan("id", 1),
			expectedClause: "(data->>? > ?)",
			expectedValue:  1,
		},
		{
			condition:      LessThan("id", 1),
			expectedClause: "(data->>? < ?)",
			expectedValue:  1,
		},
		{
			condition:      LessThanOrEqual("id", 1),
			expectedClause: "(data->>? <= ?)",
			expectedValue:  1,
		},
		{
			condition:      GreaterThanOrEqual("id", 1),
			expectedClause: "(data->>? >= ?)",
			expectedValue:  1,
		},
		{
			condition:      NotEqual("id", 1),
			expectedClause: "(data->>? != ?)",
			expectedValue:  1,
		},
		{
			condition:      Like("id", "%hello%"),
			expectedClause: "(data->>? LIKE ?)",
			expectedValue:  "%hello%",
		},
	}

	for _, test := range tests {
		assert.Equal(t, test.expectedClause, test.condition.Clause())
		assert.Equal(t, []any{"id", test.expectedValue}, test.condition.Values())
	}
}

func TestContains(t *testing.T) {
	t.Parallel()

	c := Contains("$.list", "one")

	assert.Equal(t, "(EXISTS (SELECT 1 FROM json_each(data->>?) WHERE json_each.value = ?))", c.Clause())
	assert.Equal(t, []any{"$.list", "one"}, c.Values())
}

func TestContainsAll(t *testing.T) {
	t.Parallel()

	c := ContainsAll("$.list", "one", "two")

	assert.Equal(t, "((EXISTS (SELECT 1 FROM json_each(data->>?) WHERE json_each.value = ?)) AND (EXISTS (SELECT 1 FROM json_each(data->>?) WHERE json_each.value = ?)))", c.Clause())
	assert.Equal(t, []any{"$.list", "one", "$.list", "two"}, c.Values())
}

func TestContainsAny(t *testing.T) {
	t.Parallel()

	c := ContainsAny("$.list", "one", "two")

	assert.Equal(t, "((EXISTS (SELECT 1 FROM json_each(data->>?) WHERE json_each.value = ?)) OR (EXISTS (SELECT 1 FROM json_each(data->>?) WHERE json_each.value = ?)))", c.Clause())
}

func TestTrueClause(t *testing.T) {
	t.Parallel()

	c := True("$.approved")

	assert.Equal(t, "(data->>? = ?)", c.Clause())
}

func TestFalseClause(t *testing.T) {
	t.Parallel()

	c := False("$.approved")

	assert.Equal(t, "(data->>? = ?)", c.Clause())
}

func TestCombinatorClause_AndOr(t *testing.T) {
	t.Parallel()

	c1 := Equal("id", 1)
	c2 := Equal("name", "test")

	// Test And on combinatorClause
	andClause := And(c1).And(c2)
	assert.Contains(t, []string{
		"((data->>? = ?) AND (data->>? = ?))",
		"(((data->>? = ?)) AND (data->>? = ?))",
	}, andClause.Clause())

	// Test Or on combinatorClause
	orClause := Or(c1).Or(c2)
	assert.Contains(t, []string{
		"((data->>? = ?) OR (data->>? = ?))",
		"(((data->>? = ?)) OR (data->>? = ?))",
	}, orClause.Clause())
}

func TestInCondition_AndOr(t *testing.T) {
	t.Parallel()

	c1 := In("id", 1, 2)
	c2 := Equal("name", "test")

	andClause := c1.And(c2)
	assert.Equal(t, "((data->>? IN (?,?)) AND (data->>? = ?))", andClause.Clause())

	orClause := c1.Or(c2)
	assert.Equal(t, "((data->>? IN (?,?)) OR (data->>? = ?))", orClause.Clause())
}

func TestBetweenCondition_AndOr(t *testing.T) {
	t.Parallel()

	c1 := Between("age", 20, 30)
	c2 := Equal("name", "test")

	andClause := c1.And(c2)
	assert.Equal(t, "((data->>? BETWEEN ? AND ?) AND (data->>? = ?))", andClause.Clause())

	orClause := c1.Or(c2)
	assert.Equal(t, "((data->>? BETWEEN ? AND ?) OR (data->>? = ?))", orClause.Clause())
}

func TestContainsCondition_AndOr(t *testing.T) {
	t.Parallel()

	c1 := Contains("tags", "go")
	c2 := Equal("name", "test")

	andClause := c1.And(c2)
	assert.Equal(t, "((EXISTS (SELECT 1 FROM json_each(data->>?) WHERE json_each.value = ?)) AND (data->>? = ?))", andClause.Clause())

	orClause := c1.Or(c2)
	assert.Equal(t, "((EXISTS (SELECT 1 FROM json_each(data->>?) WHERE json_each.value = ?)) OR (data->>? = ?))", orClause.Clause())
}

func TestCombinatorClause_Empty(t *testing.T) {
	t.Parallel()

	c := And()
	assert.Equal(t, "(1 == 1)", c.Clause())
}
