package nosqlite

import (
	"database/sql"

	_ "github.com/glebarez/go-sqlite/compat"
)

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

	return NewStoreWithDB(db)
}

// NewStoreWithDB creates a new store with the given database
func NewStoreWithDB(db *sql.DB) (*Store, error) {
	// PRAGMA busy_timeout = 5000;
	_, err := db.Exec("PRAGMA busy_timeout = 5000")
	if err != nil {
		return nil, err
	}

	// PRAGMA synchronous = NORMAL;
	_, err = db.Exec("PRAGMA synchronous = NORMAL")
	if err != nil {
		return nil, err
	}

	// PRAGMA journal_mode = WAL;
	_, err = db.Exec("PRAGMA journal_mode = WAL")
	if err != nil {
		return nil, err
	}

	return &Store{db: db}, nil
}

func (s *Store) Ping() error {
	return s.db.Ping()
}

// Close closes the database
func (s *Store) Close() error {
	return s.db.Close()
}
