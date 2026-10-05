package nosqlite

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInClause(t *testing.T) {
	t.Parallel()

	c := In("id", "1", "2", "3")
	assert.Equal(t, "(data->>'id' IN (?,?,?))", c.Clause())
	assert.Equal(t, []any{"1", "2", "3"}, c.Values())

	c = In("id", 1, 2, 3)
	assert.Equal(t, "(data->>'id' IN (?,?,?))", c.Clause())
	assert.Equal(t, []any{1, 2, 3}, c.Values())
}

func TestBetweenClause(t *testing.T) {
	t.Parallel()

	c := Between[int]("id", 1, 2)
	assert.Equal(t, "(data->>'id' BETWEEN ? AND ?)", c.Clause())
	assert.Equal(t, []any{1, 2}, c.Values())
}

func TestAndClauses(t *testing.T) {
	t.Parallel()

	clauseOne := Equal("id", 1)
	clauseTwo := Equal("name", "test")

	c := And(clauseOne, clauseTwo)
	assert.Equal(t, "((data->>'id' = ?) AND (data->>'name' = ?))", c.Clause())
	assert.Equal(t, []any{1, "test"}, c.Values())
}

func TestAndClausesFluent(t *testing.T) {
	t.Parallel()

	clauseOne := Equal("id", 1)
	clauseTwo := Equal("name", "test")

	c := clauseOne.And(clauseTwo)
	assert.Equal(t, "((data->>'id' = ?) AND (data->>'name' = ?))", c.Clause())
	assert.Equal(t, []any{1, "test"}, c.Values())
}

func TestOrClauses(t *testing.T) {
	t.Parallel()

	clauseOne := Equal("id", 1)
	clauseTwo := Equal("name", "test")

	c := Or(clauseOne, clauseTwo)
	assert.Equal(t, "((data->>'id' = ?) OR (data->>'name' = ?))", c.Clause())
	assert.Equal(t, []any{1, "test"}, c.Values())
}

func TestOrClausesFluent(t *testing.T) {
	t.Parallel()

	clauseOne := Equal("id", 1)
	clauseTwo := Equal("name", "test")

	c := clauseOne.Or(clauseTwo)
	assert.Equal(t, "((data->>'id' = ?) OR (data->>'name' = ?))", c.Clause())
	assert.Equal(t, []any{1, "test"}, c.Values())
}

func TestAndOrClauses(t *testing.T) {
	t.Parallel()

	clauseOne := Equal("id", 1)
	clauseTwo := Equal("name", "test")
	clauseThree := Equal("foo", "bar")

	c1 := And(clauseOne, clauseTwo)
	c2 := Or(c1, clauseThree)

	assert.Equal(t, "(((data->>'id' = ?) AND (data->>'name' = ?)) OR (data->>'foo' = ?))", c2.Clause())
	assert.Equal(t, []any{1, "test", "bar"}, c2.Values())
}

func TestAndOrClausesFluent(t *testing.T) {
	t.Parallel()

	clauseOne := Equal("id", 1)
	clauseTwo := Equal("name", "test")
	clauseThree := Equal("foo", "bar")

	c := clauseOne.And(clauseTwo).Or(clauseThree)

	assert.Equal(t, "(((data->>'id' = ?) AND (data->>'name' = ?)) OR (data->>'foo' = ?))", c.Clause())
	assert.Equal(t, []any{1, "test", "bar"}, c.Values())
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
			expectedClause: "(data->>'id' = ?)",
			expectedValue:  1,
		},
		{
			condition:      GreaterThan("id", 1),
			expectedClause: "(data->>'id' > ?)",
			expectedValue:  1,
		},
		{
			condition:      LessThan("id", 1),
			expectedClause: "(data->>'id' < ?)",
			expectedValue:  1,
		},
		{
			condition:      LessThanOrEqual("id", 1),
			expectedClause: "(data->>'id' <= ?)",
			expectedValue:  1,
		},
		{
			condition:      GreaterThanOrEqual("id", 1),
			expectedClause: "(data->>'id' >= ?)",
			expectedValue:  1,
		},
		{
			condition:      NotEqual("id", 1),
			expectedClause: "(data->>'id' != ?)",
			expectedValue:  1,
		},
		{
			condition:      Like("id", "%hello%"),
			expectedClause: "(data->>'id' LIKE ?)",
			expectedValue:  "%hello%",
		},
	}

	for _, test := range tests {
		assert.Equal(t, test.expectedClause, test.condition.Clause())
		assert.Equal(t, []any{test.expectedValue}, test.condition.Values())
	}
}

func TestContains(t *testing.T) {
	t.Parallel()

	c := Contains("$.list", "one")

	assert.Equal(t, "(EXISTS (SELECT 1 FROM json_each(data->>'$.list') WHERE json_each.value = ?))", c.Clause())
	assert.Equal(t, []any{"one"}, c.Values())
}

func TestContainsAll(t *testing.T) {
	t.Parallel()

	c := ContainsAll("$.list", "one", "two")

	assert.Equal(t, "((EXISTS (SELECT 1 FROM json_each(data->>'$.list') WHERE json_each.value = ?)) AND (EXISTS (SELECT 1 FROM json_each(data->>'$.list') WHERE json_each.value = ?)))", c.Clause())
	assert.Equal(t, []any{"one", "two"}, c.Values())
}

func TestContainsAny(t *testing.T) {
	t.Parallel()

	c := ContainsAny("$.list", "one", "two")

	assert.Equal(t, "((EXISTS (SELECT 1 FROM json_each(data->>'$.list') WHERE json_each.value = ?)) OR (EXISTS (SELECT 1 FROM json_each(data->>'$.list') WHERE json_each.value = ?)))", c.Clause())
}

func TestTrueClause(t *testing.T) {
	t.Parallel()

	c := True("$.approved")

	assert.Equal(t, "(data->>'$.approved' = ?)", c.Clause())
}

func TestFalseClause(t *testing.T) {
	t.Parallel()

	c := False("$.approved")

	assert.Equal(t, "(data->>'$.approved' = ?)", c.Clause())
}

func TestCombinatorClause_AndOr(t *testing.T) {
	t.Parallel()

	c1 := Equal("id", 1)
	c2 := Equal("name", "test")

	// Test And on combinatorClause
	andClause := And(c1).And(c2)
	assert.Contains(t, []string{
		"((data->>'id' = ?) AND (data->>'name' = ?))",
		"(((data->>'id' = ?)) AND (data->>'name' = ?))",
	}, andClause.Clause())

	// Test Or on combinatorClause
	orClause := Or(c1).Or(c2)
	assert.Contains(t, []string{
		"((data->>'id' = ?) OR (data->>'name' = ?))",
		"(((data->>'id' = ?)) OR (data->>'name' = ?))",
	}, orClause.Clause())
}

func TestInCondition_AndOr(t *testing.T) {
	t.Parallel()

	c1 := In("id", 1, 2)
	c2 := Equal("name", "test")

	andClause := c1.And(c2)
	assert.Equal(t, "((data->>'id' IN (?,?)) AND (data->>'name' = ?))", andClause.Clause())

	orClause := c1.Or(c2)
	assert.Equal(t, "((data->>'id' IN (?,?)) OR (data->>'name' = ?))", orClause.Clause())
}

func TestBetweenCondition_AndOr(t *testing.T) {
	t.Parallel()

	c1 := Between("age", 20, 30)
	c2 := Equal("name", "test")

	andClause := c1.And(c2)
	assert.Equal(t, "((data->>'age' BETWEEN ? AND ?) AND (data->>'name' = ?))", andClause.Clause())

	orClause := c1.Or(c2)
	assert.Equal(t, "((data->>'age' BETWEEN ? AND ?) OR (data->>'name' = ?))", orClause.Clause())
}

func TestContainsCondition_AndOr(t *testing.T) {
	t.Parallel()

	c1 := Contains("tags", "go")
	c2 := Equal("name", "test")

	andClause := c1.And(c2)
	assert.Equal(t, "((EXISTS (SELECT 1 FROM json_each(data->>'tags') WHERE json_each.value = ?)) AND (data->>'name' = ?))", andClause.Clause())

	orClause := c1.Or(c2)
	assert.Equal(t, "((EXISTS (SELECT 1 FROM json_each(data->>'tags') WHERE json_each.value = ?)) OR (data->>'name' = ?))", orClause.Clause())
}

func TestCombinatorClause_Empty(t *testing.T) {
	t.Parallel()

	c := And()
	assert.Equal(t, "(1 == 1)", c.Clause())
}

// TestClause_NonConformingField locks in the fallback half of jsonField's
// contract: a field containing characters outside validIndexField's
// allow-list (e.g. quotes that could otherwise alter the query's structure)
// is bound as a parameter rather than interpolated, for every Clause
// constructor. See TestTable_QueryOneInjectInField in table_test.go for the
// end-to-end proof that such a field neither errors nor widens a match.
func TestClause_NonConformingField(t *testing.T) {
	t.Parallel()

	const badField = "$.name' = 'x' OR '1'='1"

	tests := []struct {
		name      string
		condition Clause
	}{
		{"Equal", Equal(badField, "v")},
		{"In", In(badField, "v")},
		{"Between", Between(badField, 1, 2)},
		{"Contains", Contains(badField, "v")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.NotContains(t, tt.condition.Clause(), badField,
				"a non-conforming field must never be interpolated into the SQL text")
			assert.Contains(t, tt.condition.Values(), badField,
				"a non-conforming field must instead be bound as a parameter")
		})
	}
}
