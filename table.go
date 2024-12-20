package nosqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/dioad/reflect"
)

// Table represents a table in the database
type Table[T any] struct {
	store *Store

	// Name of the table
	Name string
}

func tableName[T any]() string {
	t, _ := reflect.Name[T]()

	nameNoDots := strings.Replace(t, ".", "_", -1)

	return strings.ToLower(nameNoDots)
}

// NewTable creates a new table with the given type T
func NewTable[T any](ctx context.Context, store *Store) (*Table[T], error) {
	table := &Table[T]{
		store: store,
		Name:  tableName[T](),
	}

	err := table.CreateTable(ctx)
	if err != nil {
		return nil, err
	}
	return table, nil
}

func escapeFieldName(field string) string {
	_, after, _ := strings.Cut(field, ".")

	a := strings.ReplaceAll(after, ".", "__")
	a = strings.ReplaceAll(a, " ", "_")
	return a
}

func joinEscapedFieldNames(fields ...string) string {
	parts := make([]string, len(fields))
	for i, field := range fields {
		parts[i] = escapeFieldName(field)
	}

	return strings.Join(parts, "_")
}

func constructIndexName(tableName string, fields ...string) string {
	joinedParts := joinEscapedFieldNames(fields...)

	return fmt.Sprintf("idx_%s_%s", tableName, joinedParts)
}

func (n *Table[T]) indexName(fields ...string) string {
	return constructIndexName(n.Name, fields...)
}

// CreateTable creates the table if it does not exist
func (n *Table[T]) CreateTable(ctx context.Context) error {
	return n.createTableWithName(ctx, n.Name)
}

func (n *Table[T]) createTableWithName(ctx context.Context, tableName string) error {
	createStatement := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (data jsonb)", tableName)
	_, err := n.store.db.ExecContext(ctx, createStatement)
	return err
}

// Count returns the number of items in the table
func (n *Table[T]) Count(ctx context.Context) (uint64, error) {
	var c uint64
	count := n.store.db.QueryRowContext(ctx, fmt.Sprintf("%s COUNT(*) AS count FROM `%s`", "SELECT", n.Name))
	err := count.Scan(&c)
	return c, err
}

func (n *Table[T]) CreateIndexes(ctx context.Context, indexes ...[]string) ([]string, error) {
	var err error
	indexNames := make([]string, len(indexes))
	for i, fields := range indexes {
		indexNames[i], err = n.CreateIndex(ctx, fields...)
		if err != nil {
			return indexNames, fmt.Errorf("failed to create index for fields %v: %w", fields, err)
		}
	}
	return indexNames, nil
}

// CreateIndex creates an index on the given fields
func (n *Table[T]) CreateIndex(ctx context.Context, fields ...string) (string, error) {
	indexName := n.indexName(fields...)

	indexFields := make([]string, len(fields))
	for i, field := range fields {
		indexFields[i] = fmt.Sprintf("data->>'%s'", field)
	}

	indexes := strings.Join(indexFields, ", ")

	createIndexStatement := fmt.Sprintf("CREATE INDEX IF NOT EXISTS `%s` ON `%s` (%s)", indexName, n.Name, indexes)
	_, err := n.store.db.ExecContext(ctx, createIndexStatement)
	return indexName, err
}

// hasIndex returns true if the index exists
func (n *Table[T]) hasIndex(ctx context.Context, indexName string) (bool, error) {
	_, err := n.store.db.ExecContext(ctx, "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=? AND name=?", n.Name, indexName)
	if err != nil {
		return false, err
	}
	return true, nil
}

// Delete removes items from the table that match the given clause
func (n *Table[T]) Delete(ctx context.Context, clause Clause) error {
	deleteStatement := fmt.Sprintf("%s `%s` WHERE %s", "DELETE FROM", n.Name, clause.Clause())
	_, err := n.store.db.ExecContext(ctx, deleteStatement, clause.Values()...)
	return err
}

// Insert adds a new item to the table
func (n *Table[T]) Insert(ctx context.Context, data T) error {
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	insertStatement := fmt.Sprintf("%s `%s` (data) VALUES (?)", "INSERT INTO", n.Name)
	_, err = n.store.db.ExecContext(ctx, insertStatement, string(b))
	return err
}

// QueryOne returns a single item from the table
func (n *Table[T]) QueryOne(ctx context.Context, clause Clause) (*T, error) {
	var data string

	queryStatement := fmt.Sprintf("%s data FROM `%s` WHERE %s", "SELECT", n.Name, clause.Clause())
	values := clause.Values()
	row := n.store.db.QueryRowContext(ctx, queryStatement, values...)
	err := row.Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var result T
	err = json.Unmarshal([]byte(data), &result)
	return &result, err
}

func (n *Table[T]) All(ctx context.Context) ([]T, error) {
	return n.QueryMany(ctx, All())
}

// QueryMany returns multiple items from the table
// can we use http://doug-martin.github.io/goqu/ for this?
func (n *Table[T]) QueryMany(ctx context.Context, clause Clause) ([]T, error) {
	var data string
	var results []T

	queryStatement := fmt.Sprintf("%s data FROM `%s` WHERE %s", "SELECT", n.Name, clause.Clause())
	rows, err := n.store.db.QueryContext(ctx, queryStatement, clause.Values()...)
	if errors.Is(err, sql.ErrNoRows) {
		return results, nil
	}

	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		err = rows.Scan(&data)
		if err != nil {
			return nil, err
		}
		var result T
		err = json.Unmarshal([]byte(data), &result)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}

// Update changes one or more items in the table
func (n *Table[T]) Update(ctx context.Context, clause Clause, newVal T) error {
	b, err := json.Marshal(newVal)
	if err != nil {
		return err
	}
	updateStatement := fmt.Sprintf("%s %s SET data = ? WHERE %s", "UPDATE", n.Name, clause.Clause())
	params := append([]any{string(b)}, clause.Values()...)
	_, err = n.store.db.ExecContext(ctx, updateStatement, params...)
	return err
}
