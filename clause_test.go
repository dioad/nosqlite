package nosqlite

import (
	"testing"
)

func TestInClause(t *testing.T) {
	t.Parallel()

	c := In("id", "1", "2", "3")

	if got := c.Clause(); got != "(data->>? IN (?,?,?))" {
		t.Errorf("got = %v, want %v", got, "(data->>? IN (?,?,?))")
	}
	if got := c.Values(); got[0] != "id" {
		t.Errorf("got = %v, want field %q first", got, "id")
	}

	c = In("id", 1, 2, 3)

	if got := c.Clause(); got != "(data->>? IN (?,?,?))" {
		t.Errorf("got = %v, want %v", got, "(data->>? IN (?,?,?))")
	}
	if got := c.Values(); got[0] != "id" {
		t.Errorf("got = %v, want field %q first", got, "id")
	}
}

func TestBetweenClause(t *testing.T) {
	t.Parallel()

	c := Between[int]("id", 1, 2)

	if got := c.Clause(); got != "(data->>? BETWEEN ? AND ?)" {
		t.Errorf("got = %v, want %v", got, "(data->>? BETWEEN ? AND ?)")
	}

	if got := c.Values(); got[0] != "id" || got[1] != 1 || got[2] != 2 {
		t.Errorf("got = %v, want %v", got, []any{"id", 1, 2})
	}
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

	want := "((data->>? = ?) AND (data->>? = ?))"

	c := And(clauseOne, clauseTwo)
	if got := c.Clause(); got != want {
		t.Errorf("got = %v, want %v", got, want)
	}

	if got := c.Values(); got[0] != "id" || got[1] != 1 || got[2] != "name" || got[3] != "test" {
		t.Errorf("got = %v, want %v", got, []any{"id", 1, "name", "test"})
	}
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

	want := "((data->>? = ?) AND (data->>? = ?))"

	c := clauseOne.And(clauseTwo)
	if got := c.Clause(); got != want {
		t.Errorf("got = %v, want %v", got, want)
	}

	if got := c.Values(); got[0] != "id" || got[1] != 1 || got[2] != "name" || got[3] != "test" {
		t.Errorf("got = %v, want %v", got, []any{"id", 1, "name", "test"})
	}
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

	want := "((data->>? = ?) OR (data->>? = ?))"

	c := Or(clauseOne, clauseTwo)
	if got := c.Clause(); got != want {
		t.Errorf("got = %v, want %v", got, want)
	}

	if got := c.Values(); got[0] != "id" || got[1] != 1 || got[2] != "name" || got[3] != "test" {
		t.Errorf("got = %v, want %v", got, []any{"id", 1, "name", "test"})
	}
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

	want := "((data->>? = ?) OR (data->>? = ?))"

	c := clauseOne.Or(clauseTwo)
	if got := c.Clause(); got != want {
		t.Errorf("got = %v, want %v", got, want)
	}

	if got := c.Values(); got[0] != "id" || got[1] != 1 || got[2] != "name" || got[3] != "test" {
		t.Errorf("got = %v, want %v", got, []any{"id", 1, "name", "test"})
	}
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

	want := "(((data->>? = ?) AND (data->>? = ?)) OR (data->>? = ?))"
	c1 := And(clauseOne, clauseTwo)
	c2 := Or(c1, clauseThree)

	if got := c2.Clause(); got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	wantValues := []any{"id", 1, "name", "test", "foo", "bar"}
	if got := c2.Values(); got[1] != 1 || got[3] != "test" || got[5] != "bar" {
		t.Errorf("got %v, want %v", got, wantValues)
	}
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

	want := "(((data->>? = ?) AND (data->>? = ?)) OR (data->>? = ?))"

	c := clauseOne.And(clauseTwo).Or(clauseThree)

	if got := c.Clause(); got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	wantValues := []any{"id", 1, "name", "test", "foo", "bar"}
	if got := c.Values(); got[1] != 1 || got[3] != "test" || got[5] != "bar" {
		t.Errorf("got %v, want %v", got, wantValues)
	}
}

func TestConditions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		condition      Clause
		expectedClause string
		expectedValues []any
	}{
		{
			condition:      Equal("id", 1),
			expectedClause: "(data->>? = ?)",
			expectedValues: []any{1},
		},
		{
			condition:      GreaterThan("id", 1),
			expectedClause: "(data->>? > ?)",
			expectedValues: []any{1},
		},
		{
			condition:      LessThan("id", 1),
			expectedClause: "(data->>? < ?)",
			expectedValues: []any{1},
		},
		{
			condition:      LessThanOrEqual("id", 1),
			expectedClause: "(data->>? <= ?)",
			expectedValues: []any{1},
		},
		{
			condition:      GreaterThanOrEqual("id", 1),
			expectedClause: "(data->>? >= ?)",
			expectedValues: []any{1},
		},
		{
			condition:      NotEqual("id", 1),
			expectedClause: "(data->>? != ?)",
			expectedValues: []any{1},
		},
		{
			condition:      Like("id", "%hello%"),
			expectedClause: "(data->>? LIKE ?)",
			expectedValues: []any{"%hello%"},
		},
	}

	for _, test := range tests {
		if got := test.condition.Clause(); got != test.expectedClause {
			t.Errorf("got = %v, want %v", got, test.expectedClause)
		}

		if got := test.condition.Values(); got[0] != "id" || got[1] != test.expectedValues[0] {
			t.Errorf("got = %v, want field %q then %v", got, "id", test.expectedValues)
		}
	}
}

func TestContains(t *testing.T) {
	t.Parallel()

	c := Contains("$.list", "one")

	expected := "(EXISTS (SELECT 1 FROM json_each(data->>?) WHERE json_each.value = ?))"

	if got := c.Clause(); got != expected {
		t.Errorf("got = %v, want %v", got, expected)
	}
	if got := c.Values(); got[0] != "$.list" || got[1] != "one" {
		t.Errorf("got = %v, want %v", got, []any{"$.list", "one"})
	}
}

func TestContainsAll(t *testing.T) {
	t.Parallel()

	c := ContainsAll("$.list", "one", "two")

	expected := "((EXISTS (SELECT 1 FROM json_each(data->>?) WHERE json_each.value = ?)) AND (EXISTS (SELECT 1 FROM json_each(data->>?) WHERE json_each.value = ?)))"

	if got := c.Clause(); got != expected {
		t.Errorf("got = %v, want %v", got, expected)
	}
	want := []any{"$.list", "one", "$.list", "two"}
	if got := c.Values(); got[0] != want[0] || got[1] != want[1] || got[2] != want[2] || got[3] != want[3] {
		t.Errorf("got = %v, want %v", got, want)
	}
}

func TestContainsAny(t *testing.T) {
	t.Parallel()

	c := ContainsAny("$.list", "one", "two")

	expected := "((EXISTS (SELECT 1 FROM json_each(data->>?) WHERE json_each.value = ?)) OR (EXISTS (SELECT 1 FROM json_each(data->>?) WHERE json_each.value = ?)))"

	if got := c.Clause(); got != expected {
		t.Errorf("got = %v, want %v", got, expected)
	}
}

func TestTrueClause(t *testing.T) {
	t.Parallel()

	c := True("$.approved")

	expected := "(data->>? = ?)"

	if got := c.Clause(); got != expected {
		t.Errorf("got = %v, want %v", got, expected)
	}
}

func TestFalseClause(t *testing.T) {
	t.Parallel()

	c := False("$.approved")

	expected := "(data->>? = ?)"

	if got := c.Clause(); got != expected {
		t.Errorf("got = %v, want %v", got, expected)
	}
}

func TestCombinatorClause_AndOr(t *testing.T) {
	t.Parallel()

	c1 := Equal("id", 1)
	c2 := Equal("name", "test")

	// Test And on combinatorClause
	andClause := And(c1).And(c2)
	if got := andClause.Clause(); got != "((data->>? = ?) AND (data->>? = ?))" && got != "(((data->>? = ?)) AND (data->>? = ?))" {
		t.Errorf("got %v", got)
	}

	// Test Or on combinatorClause
	orClause := Or(c1).Or(c2)
	if got := orClause.Clause(); got != "((data->>? = ?) OR (data->>? = ?))" && got != "(((data->>? = ?)) OR (data->>? = ?))" {
		t.Errorf("got %v", got)
	}
}

func TestInCondition_AndOr(t *testing.T) {
	t.Parallel()

	c1 := In("id", 1, 2)
	c2 := Equal("name", "test")

	andClause := c1.And(c2)
	if got := andClause.Clause(); got != "((data->>? IN (?,?)) AND (data->>? = ?))" {
		t.Errorf("got %v", got)
	}

	orClause := c1.Or(c2)
	if got := orClause.Clause(); got != "((data->>? IN (?,?)) OR (data->>? = ?))" {
		t.Errorf("got %v", got)
	}
}

func TestBetweenCondition_AndOr(t *testing.T) {
	t.Parallel()

	c1 := Between("age", 20, 30)
	c2 := Equal("name", "test")

	andClause := c1.And(c2)
	if got := andClause.Clause(); got != "((data->>? BETWEEN ? AND ?) AND (data->>? = ?))" {
		t.Errorf("got %v", got)
	}

	orClause := c1.Or(c2)
	if got := orClause.Clause(); got != "((data->>? BETWEEN ? AND ?) OR (data->>? = ?))" {
		t.Errorf("got %v", got)
	}
}

func TestContainsCondition_AndOr(t *testing.T) {
	t.Parallel()

	c1 := Contains("tags", "go")
	c2 := Equal("name", "test")

	andClause := c1.And(c2)
	if got := andClause.Clause(); got != "((EXISTS (SELECT 1 FROM json_each(data->>?) WHERE json_each.value = ?)) AND (data->>? = ?))" {
		t.Errorf("got %v", got)
	}

	orClause := c1.Or(c2)
	if got := orClause.Clause(); got != "((EXISTS (SELECT 1 FROM json_each(data->>?) WHERE json_each.value = ?)) OR (data->>? = ?))" {
		t.Errorf("got %v", got)
	}
}

func TestCombinatorClause_Empty(t *testing.T) {
	t.Parallel()

	c := And()
	if got := c.Clause(); got != "(1 == 1)" {
		t.Errorf("got %v", got)
	}
}
