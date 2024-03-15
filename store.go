package nosqlite

import "database/sql"

// Store represents a store for the database
type Store struct {
	db *sql.DB
}

// NewStore creates a new store with the given file path
func NewStore(filePath string) (*Store, error) {
	db, err := sql.Open("sqlite3", filePath)
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

// NewStoreWithDB creates a new store with the given database
func NewStoreWithDB(db *sql.DB) *Store {
	return &Store{db: db}
}

// Close closes the database
func (s *Store) Close() error {
	return s.db.Close()
}
