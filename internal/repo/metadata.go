package repo

import (
	"database/sql"
	"errors"
	"time"

	"github.com/GoogleCloudPlatform/gcs-metadata-server/internal/model"
)

type Metadata struct {
	*Database
}

type MetadataRepository interface {
	Get(bucket string, name string) (*model.Metadata, error)
	Insert(*model.Metadata) error
	Update(bucket, name, storageClass string, size int64, updated time.Time) error
	Delete(bucket, name string) error
}

func NewMetadataRepository(db *Database) MetadataRepository {
	return &Metadata{db}
}

// Get returns metadata object information. It returns an empty struct if metadata does not exist.
func (m *Metadata) Get(bucket, name string) (*model.Metadata, error) {
	query := `
		SELECT name, parent, size, storage_class, created, updated
		FROM metadata
		WHERE bucket = ? AND name = ?;	
	`

	// Select record and ignore empty results for services not to depend on database errors
	var metadata model.Metadata
	if err := m.DB.Get(&metadata, query, bucket, name); err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	return &metadata, nil
}

func (m *Metadata) Insert(obj *model.Metadata) error {
	query := `
		INSERT INTO metadata 
		(bucket, name, size, parent, storage_class, created, updated)	
		VALUES (?, ?, ?, ?, ?, ?, ?);
	`

	if len(obj.Bucket) == 0 || len(obj.Name) == 0 {
		return errors.New("bucket or name argument is empty")
	}

	if _, err := m.DB.Exec(query,
		obj.Bucket,
		obj.Name,
		obj.Size,
		getParentDir(obj.Name),
		obj.StorageClass,
		obj.Created,
		obj.Updated); err != nil {
		return err
	}
	return nil
}

func (m *Metadata) Update(bucket, name, storageClass string, size int64, updated time.Time) error {
	query := `
		UPDATE metadata
		SET storage_class = $1,
		    size 		  = $2,
			updated 	  = $3
		WHERE bucket = $4 AND name = $5;
	`

	res, err := m.DB.Exec(query, storageClass, size, updated, bucket, name)
	if err != nil {
		return err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected == 0 {
		return errors.New("no rows affected")
	}

	return nil
}

func (m *Metadata) Delete(bucket string, name string) error {
	query := `
		DELETE FROM metadata
		WHERE bucket = ? AND name = ?;	
	`

	res, err := m.DB.Exec(query, bucket, name)
	if err != nil {
		return err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected == 0 {
		return errors.New("no rows affected")
	}

	return nil
}
