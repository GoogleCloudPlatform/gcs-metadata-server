package repo

import (
	"errors"
	"time"

	"github.com/BrandonY/gcs-metadata-server/internal/model"
)

type Metadata struct {
	*Database
}

type MetadataRepository interface {
	Insert(*model.Metadata) error
	Update(bucket string, name string, size int64, updated time.Time) error
	Delete(bucket string, name string) error
}

func NewMetadataRepository(db *Database) MetadataRepository {
	return &Metadata{db}
}

func (m *Metadata) Insert(obj *model.Metadata) error {
	query := `
		INSERT INTO metadata 
		(bucket, name, size, storage_class, created, updated)	
		VALUES (?, ?, ?, ?, ?, ?);
	`

	if len(obj.Bucket) == 0 || len(obj.Name) == 0 {
		return errors.New("bucket or name argument is empty")
	}

	if _, err := m.DB.Exec(query,
		obj.Bucket,
		obj.Name,
		obj.Size,
		obj.StorageClass,
		obj.Created,
		obj.Updated); err != nil {
		return err
	}
	return nil
}

func (m *Metadata) Update(bucket string, name string, size int64, updated time.Time) error {
	query := `
		UPDATE metadata
		SET size = ?,
			updated = ?
		WHERE bucket = ? AND name = ?;
	`

	res, err := m.DB.Exec(query, size, updated, bucket, name)
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
