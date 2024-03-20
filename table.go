package nosqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

// Table represents a table in the database
type Table[T any] struct {
	store *Store
}

func (n *Table[T]) getTableName() string {
	a := *new(T)
	t := reflect.TypeOf(a)
	return strings.ToLower(t.Name())
}

// NewTable creates a new table with the given type T
func NewTable[T any](ctx context.Context, store *Store) (*Table[T], error) {
	table := &Table[T]{store: store}

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
	return constructIndexName(n.getTableName(), fields...)
}

// CreateTable creates the table if it does not exist
func (n *Table[T]) CreateTable(ctx context.Context) error {
	return n.createTableWithName(ctx, n.getTableName())
}

func (n *Table[T]) createTableWithName(ctx context.Context, tableName string) error {
	createStatement := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (data jsonb)", tableName)
	_, err := n.store.db.ExecContext(ctx, createStatement, tableName)
	return err
}

// Count returns the number of items in the table
func (n *Table[T]) Count(ctx context.Context) (uint64, error) {
	var c uint64
	tableName := n.getTableName()
	count := n.store.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) AS count FROM `%s`", tableName))
	err := count.Scan(&c)
	return c, err
}

// CreateIndex creates an index on the given fields
func (n *Table[T]) CreateIndex(ctx context.Context, fields ...string) (string, error) {
	tableName := n.getTableName()
	indexName := n.indexName(fields...)

	indexFields := make([]string, len(fields))
	for i, field := range fields {
		indexFields[i] = fmt.Sprintf("data->>'%s'", field)
	}

	indexes := strings.Join(indexFields, ", ")

	createIndexStatement := fmt.Sprintf("CREATE INDEX IF NOT EXISTS `%s` ON `%s` (%s)", indexName, tableName, indexes)
	_, err := n.store.db.ExecContext(ctx, createIndexStatement)
	return indexName, err
}

// hasIndex returns true if the index exists
func (n *Table[T]) hasIndex(ctx context.Context, indexName string) (bool, error) {
	tableName := n.getTableName()
	_, err := n.store.db.ExecContext(ctx, "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=? AND name=?", tableName, indexName)
	if err != nil {
		return false, err
	}
	return true, nil
}

// Delete removes items from the table that match the given clause
func (n *Table[T]) Delete(ctx context.Context, clause Clause) error {
	tableName := n.getTableName()
	deleteStatement := fmt.Sprintf("DELETE FROM `%s` WHERE %s", tableName, clause.Clause())
	_, err := n.store.db.ExecContext(ctx, deleteStatement, clause.Values()...)
	return err
}

// Insert adds a new item to the table
func (n *Table[T]) Insert(ctx context.Context, data T) error {
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	tableName := n.getTableName()
	insertStatement := fmt.Sprintf("INSERT INTO `%s` (data) VALUES (?)", tableName)
	_, err = n.store.db.ExecContext(ctx, insertStatement, string(b))
	return err
}

// QueryOne returns a single item from the table
func (n *Table[T]) QueryOne(ctx context.Context, clause Clause) (*T, error) {
	//func (n *Table[T]) QueryOne(field, value string) (*T, error) {
	var data string
	//tag->'$.country.name'
	tableName := n.getTableName()
	queryStatement := fmt.Sprintf("SELECT data FROM `%s` WHERE %s", tableName, clause.Clause())
	row := n.store.db.QueryRowContext(ctx, queryStatement, clause.Values()...)
	err := row.Scan(&data)
	if err != nil {
		return nil, err
	}
	var result T
	err = json.Unmarshal([]byte(data), &result)
	return &result, err
}

// QueryMany returns multiple items from the table
// can we use http://doug-martin.github.io/goqu/ for this?
func (n *Table[T]) QueryMany(ctx context.Context, clause Clause) ([]T, error) {
	var data string
	var results []T
	tableName := n.getTableName()
	queryStatement := fmt.Sprintf("SELECT data FROM `%s` WHERE %s", tableName, clause.Clause())
	rows, err := n.store.db.QueryContext(ctx, queryStatement, clause.Values()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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
func (n *Table[T]) Update(ctx context.Context, field, value string, newVal T) error {
	b, err := json.Marshal(newVal)
	if err != nil {
		return err
	}
	tableName := n.getTableName()
	updateStatement := fmt.Sprintf("UPDATE %s SET data = ? WHERE data->>? = ?", tableName)
	_, err = n.store.db.ExecContext(ctx, updateStatement, string(b), field, value)
	return err
}
