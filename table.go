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

// TableWithTx represents a table within a transaction
type TableWithTx[T any] struct {
	tx   *Transaction
	name string
}

// WithTransaction returns a TableWithTx that operates within the given transaction
func (n *Table[T]) WithTransaction(tx *Transaction) *TableWithTx[T] {
	return &TableWithTx[T]{
		tx:   tx,
		name: n.Name,
	}
}

// Insert adds a new item to the table within the transaction.
func (t *TableWithTx[T]) Insert(ctx context.Context, data T) error {
	// Check if context is already canceled
	if ctx.Err() != nil {
		return fmt.Errorf("context error before insert: %w", ctx.Err())
	}

	b, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	insertStatement := fmt.Sprintf("%s `%s` (data) VALUES (?)", "INSERT INTO", t.name)
	_, err = t.tx.ExecContext(ctx, insertStatement, string(b))
	if err != nil {
		return fmt.Errorf("failed to insert data: %w", err)
	}

	return nil
}

// QueryOne returns a single item from the table within the transaction.
func (t *TableWithTx[T]) QueryOne(ctx context.Context, clause Clause) (*T, error) {
	var data string

	queryStatement := fmt.Sprintf("%s data FROM `%s` WHERE %s LIMIT 1", "SELECT", t.name, clause.Clause())
	values := clause.Values()
	row := t.tx.QueryRowContext(ctx, queryStatement, values...)
	err := row.Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}

	var result T
	err = json.Unmarshal([]byte(data), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal data: %w", err)
	}

	return &result, nil
}

// QueryMany returns multiple items from the table within the transaction.
func (t *TableWithTx[T]) QueryMany(ctx context.Context, clause Clause) ([]T, error) {
	return t.QueryManyWithPagination(ctx, clause, 0, 0)
}

// QueryManyWithPagination returns multiple items from the table with pagination within the transaction.
// The limit parameter controls the maximum number of items to return.
// The offset parameter controls the number of items to skip.
// If limit is 0, all matching items are returned.
func (t *TableWithTx[T]) QueryManyWithPagination(ctx context.Context, clause Clause, limit, offset uint64) ([]T, error) {
	var data string
	results := make([]T, 0)

	// Build the query with pagination if needed
	queryStatement := fmt.Sprintf("%s data FROM `%s` WHERE %s", "SELECT", t.name, clause.Clause())
	if limit > 0 {
		queryStatement += fmt.Sprintf(" LIMIT %d", limit)
	}
	if offset > 0 {
		queryStatement += fmt.Sprintf(" OFFSET %d", offset)
	}

	rows, err := t.tx.QueryContext(ctx, queryStatement, clause.Values()...)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&data)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		var result T
		err = json.Unmarshal([]byte(data), &result)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal data: %w", err)
		}
		results = append(results, result)
	}

	// Check for errors from iterating over rows
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error during row iteration: %w", err)
	}

	return results, nil
}

// All returns all items from the table within the transaction.
func (t *TableWithTx[T]) All(ctx context.Context) ([]T, error) {
	return t.QueryMany(ctx, All())
}

// Update changes one or more items in the table within the transaction.
func (t *TableWithTx[T]) Update(ctx context.Context, clause Clause, newVal T) error {
	// Check if context is already canceled
	if ctx.Err() != nil {
		return fmt.Errorf("context error before update: %w", ctx.Err())
	}

	b, err := json.Marshal(newVal)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	updateStatement := fmt.Sprintf("%s `%s` SET data = ? WHERE %s", "UPDATE", t.name, clause.Clause())
	params := append([]any{string(b)}, clause.Values()...)
	result, err := t.tx.ExecContext(ctx, updateStatement, params...)
	if err != nil {
		return fmt.Errorf("failed to update data: %w", err)
	}

	// Check if any rows were affected (optional)
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		// No rows were updated, but this isn't necessarily an error
		// The caller can check if the update affected any rows if needed
		return nil
	}

	return nil
}

// Delete removes items from the table within the transaction.
func (t *TableWithTx[T]) Delete(ctx context.Context, clause Clause) error {
	// Check if context is already canceled
	if ctx.Err() != nil {
		return fmt.Errorf("context error before delete: %w", ctx.Err())
	}

	deleteStatement := fmt.Sprintf("%s `%s` WHERE %s", "DELETE FROM", t.name, clause.Clause())
	result, err := t.tx.ExecContext(ctx, deleteStatement, clause.Values()...)
	if err != nil {
		return fmt.Errorf("failed to delete data: %w", err)
	}

	// Check if any rows were affected (optional)
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		// No rows were deleted, but this isn't necessarily an error
		// The caller can check if the delete affected any rows if needed
		return nil
	}

	return nil
}

// Count returns the number of items in the table within the transaction.
func (t *TableWithTx[T]) Count(ctx context.Context) (uint64, error) {
	var c uint64
	count := t.tx.QueryRowContext(ctx, fmt.Sprintf("%s COUNT(*) AS count FROM `%s`", "SELECT", t.name))
	err := count.Scan(&c)
	if err != nil {
		return 0, fmt.Errorf("failed to count rows: %w", err)
	}
	return c, nil
}

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

// Delete removes items from the table that match the given clause.
// Returns nil if successful, even if no rows were deleted.
func (n *Table[T]) Delete(ctx context.Context, clause Clause) error {
	// Check if context is already canceled
	if ctx.Err() != nil {
		return fmt.Errorf("context error before delete: %w", ctx.Err())
	}

	deleteStatement := fmt.Sprintf("%s `%s` WHERE %s", "DELETE FROM", n.Name, clause.Clause())
	result, err := n.store.db.ExecContext(ctx, deleteStatement, clause.Values()...)
	if err != nil {
		return fmt.Errorf("failed to delete data: %w", err)
	}

	// Check if any rows were affected (optional)
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		// No rows were deleted, but this isn't necessarily an error
		// The caller can check if the delete affected any rows if needed
		return nil
	}

	return nil
}

// Insert adds a new item to the table.
// The data is serialized to JSON and stored in the 'data' column.
func (n *Table[T]) Insert(ctx context.Context, data T) error {
	// Check if context is already canceled
	if ctx.Err() != nil {
		return fmt.Errorf("context error before insert: %w", ctx.Err())
	}

	b, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	insertStatement := fmt.Sprintf("%s `%s` (data) VALUES (?)", "INSERT INTO", n.Name)
	_, err = n.store.db.ExecContext(ctx, insertStatement, string(b))
	if err != nil {
		return fmt.Errorf("failed to insert data: %w", err)
	}

	return nil
}

// QueryOne returns a single item from the table that matches the given clause.
// Returns nil if no item matches the clause.
func (n *Table[T]) QueryOne(ctx context.Context, clause Clause) (*T, error) {
	var data string

	queryStatement := fmt.Sprintf("%s data FROM `%s` WHERE %s LIMIT 1", "SELECT", n.Name, clause.Clause())
	values := clause.Values()
	row := n.store.db.QueryRowContext(ctx, queryStatement, values...)
	err := row.Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}

	var result T
	err = json.Unmarshal([]byte(data), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal data: %w", err)
	}

	return &result, nil
}

func (n *Table[T]) All(ctx context.Context) ([]T, error) {
	return n.QueryMany(ctx, All())
}

// QueryMany returns multiple items from the table that match the given clause.
// Returns an empty slice if no items match the clause.
func (n *Table[T]) QueryMany(ctx context.Context, clause Clause) ([]T, error) {
	return n.QueryManyWithPagination(ctx, clause, 0, 0)
}

// QueryManyWithPagination returns multiple items from the table with pagination.
// The limit parameter controls the maximum number of items to return.
// The offset parameter controls the number of items to skip.
// If limit is 0, all matching items are returned.
// Returns an empty slice if no items match the clause.
func (n *Table[T]) QueryManyWithPagination(ctx context.Context, clause Clause, limit, offset uint64) ([]T, error) {
	var data string
	results := make([]T, 0)

	// Build the query with pagination if needed
	queryStatement := fmt.Sprintf("%s data FROM `%s` WHERE %s", "SELECT", n.Name, clause.Clause())
	if limit > 0 {
		queryStatement += fmt.Sprintf(" LIMIT %d", limit)
	} else {
		queryStatement += " LIMIT -1"
	}
	if offset > 0 {
		queryStatement += fmt.Sprintf(" OFFSET %d", offset)
	}

	rows, err := n.store.db.QueryContext(ctx, queryStatement, clause.Values()...)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			// Log the error but don't override the original error if there was one
			if err == nil {
				err = closeErr
			}
		}
	}()

	for rows.Next() {
		err = rows.Scan(&data)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		var result T
		err = json.Unmarshal([]byte(data), &result)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal data: %w", err)
		}
		results = append(results, result)
	}

	// Check for errors from iterating over rows
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error during row iteration: %w", err)
	}

	return results, nil
}

// Update changes one or more items in the table that match the given clause.
// The new data is serialized to JSON and replaces the existing data.
func (n *Table[T]) Update(ctx context.Context, clause Clause, newVal T) error {
	// Check if context is already canceled
	if ctx.Err() != nil {
		return fmt.Errorf("context error before update: %w", ctx.Err())
	}

	b, err := json.Marshal(newVal)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	updateStatement := fmt.Sprintf("%s `%s` SET data = ? WHERE %s", "UPDATE", n.Name, clause.Clause())
	params := append([]any{string(b)}, clause.Values()...)
	result, err := n.store.db.ExecContext(ctx, updateStatement, params...)
	if err != nil {
		return fmt.Errorf("failed to update data: %w", err)
	}

	// Check if any rows were affected (optional)
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		// No rows were updated, but this isn't necessarily an error
		// The caller can check if the update affected any rows if needed
		return nil
	}

	return nil
}
