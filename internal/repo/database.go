package repo

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

const DATABASE_TYPE = "sqlite3"

const pragma = `
	PRAGMA journal_mode = WAL; -- Use checkpoints instead of atomic commits 
	PRAGMA cache_size = -62500; -- 64MB, maximum database disk pages size to be held per database files
	PRAGMA busy_timeout = 5000; -- Sleep if SQLITE_BUSY is returned
	PRAGMA synchronous = NORMAL; -- Only sync at critical moments, recommended when using WAL
`

const schema = `
	CREATE TABLE metadata (
		bucket 		TEXT NOT NULL,
		name 		TEXT NOT NULL,
		size		INTEGER NOT NULL,
		updated 	TIMESTAMP NOT NULL,
		created		TIMESTAMP NOT NULL,
		parent		TEXT,
		storage_class TEXT NOT NULL CHECK (storage_class IN ('STANDARD', 'NEARLINE', 'COLDLINE', 'ARCHIVE')),
		FOREIGN KEY (parent) REFERENCES parent(name),
		PRIMARY KEY (bucket, name)
	);
	
	CREATE TABLE directory (
		bucket			TEXT NOT NULL,
		name			TEXT NOT NULL,
		count			INTEGER DEFAULT 0,
		size_standard 	INTEGER DEFAULT 0,
		size_nearline 	INTEGER DEFAULT 0,
		size_coldline	INTEGER DEFAULT 0,
		size_archive 	INTEGER DEFAULT 0,
		parent			TEXT,
		FOREIGN KEY (parent) REFERENCES directory(name),
		PRIMARY KEY (bucket, name)
	);
`

type Database struct {
	*sqlx.DB
	url                string
	maxOpenConnections int
}

func NewDatabase(url string, maxOpenConnections int) *Database {
	db := &Database{
		url:                url,
		maxOpenConnections: maxOpenConnections,
	}

	return db
}

// Connect to a database at Database.url
// If database file does not exist, a new db file will be created at Database.url
func (db *Database) Connect(ctx context.Context) error {
	dbCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var err error
	db.DB, err = sqlx.ConnectContext(dbCtx, DATABASE_TYPE, db.url)
	if err != nil {
		return err
	}

	db.SetMaxOpenConns(db.maxOpenConnections)

	return nil
}

// Setup executes all PRAGMA configs to setup database behavior
func (db *Database) Setup() error {
	if _, err := db.Exec(pragma); err != nil {
		return err
	}
	return nil
}

// CreateTables creates and executes database schema defined
func (db *Database) CreateTables() error {
	if _, err := db.Exec(schema); err != nil {
		return err
	}
	return nil
}

// PingTable checks if database schema has been created by pinging metadata table
func (db *Database) PingTable() (bool, error) {
	var tableExists bool
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type="table" AND name="metadata");`).Scan(&tableExists); err != nil {
		return false, err
	}

	return tableExists, nil
}

// CreateIndexes creates relevant indexes to improve query performance
func (db *Database) CreateIndexes() error {
	query := `
		CREATE INDEX idx_metadata_parent    ON metadata(parent);
		CREATE INDEX idx_directory_parent   ON directory(parent);
		CREATE INDEX idx_directory_name		ON directory(name);
		VACUUM; -- Repackage database to clean empty space
	`

	if _, err := db.Exec(query); err != nil {
		return err
	}
	return nil
}
