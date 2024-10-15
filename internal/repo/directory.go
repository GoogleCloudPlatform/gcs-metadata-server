package repo

import (
	"errors"
	"fmt"
	"strings"

	"github.com/GoogleCloudPlatform/gcs-metadata-server/internal/model"
)

type Directory struct {
	*Database
}

type DirectoryRepository interface {
	Insert(dir model.Directory) error
	Delete(bucket string, name string) error
	UpsertParentDirs(storageClass StorageClass, bucket string, objName string, newSize int64, newCount int64) error
	UpsertArchiveParentDirs(oldStorageClass StorageClass, newStorageClass StorageClass, bucket, objName string, size int64) error
}

func NewDirectoryRepository(db *Database) DirectoryRepository {
	return &Directory{db}
}

// getParentDir returns the parent directory of dir
func getParentDir(dir string) string {
	trimmedDir := strings.TrimSuffix(dir, "/")

	// Handle root
	if trimmedDir == "" {
		return "/"
	}

	lastIndex := strings.LastIndex(trimmedDir, "/")
	if lastIndex == -1 {
		return "/" // File in root directory
	}

	// Remove remaining portion of last directory
	return trimmedDir[:lastIndex+1]
}

// UpsertArchiveParentDirs reallocates storage class size on all parent directories for an object update by object versioning.
//
// If directories do not exist, they will be created using newStorageClass and a default count of 1
// as a safeguard for dirty reads during seeding process.
func (d *Directory) UpsertArchiveParentDirs(oldStorageClass StorageClass, newStorageClass StorageClass, bucket, objName string, size int64) error {
	oldStorageColumn := "size_" + strings.ToLower(string(oldStorageClass))
	newStorageColumn := "size_" + strings.ToLower(string(newStorageClass))

	query := fmt.Sprintf(`
		INSERT INTO directory (bucket, name, %[2]s, parent, count)
		VALUES ($1, $2, $3, $4, 1)
		ON CONFLICT(bucket, name)
		DO UPDATE
		SET %[1]s = %[1]s - $3,
			%[2]s = %[2]s + $3;
	`, oldStorageColumn, newStorageColumn)

	if len(bucket) == 0 || len(objName) == 0 {
		return errors.New("bucket or name argument is empty")
	}

	dirName := getParentDir(objName)

	tx, err := d.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() // no-op if commit succeeds

	for {
		if _, err = tx.Exec(query, bucket, dirName, size, getParentDir(dirName)); err != nil {
			return err
		}

		// Last directory to update is root
		if dirName == "/" {
			break
		}
		dirName = getParentDir(dirName)
	}

	return tx.Commit()
}

// UpsertParentDirs updates all parent directories of an object name in one transaction
func (d *Directory) UpsertParentDirs(storageClass StorageClass, bucket string, objName string, newSize int64, newCount int64) error {
	storageColumn := "size_" + strings.ToLower(string(storageClass))
	query := fmt.Sprintf(`
			INSERT INTO directory (bucket, name, %[1]s, count, parent)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT(bucket, name)
			DO UPDATE
			SET %[1]s = %[1]s + $3,
				count = count + $4;
	`, storageColumn)

	if len(bucket) == 0 || len(objName) == 0 {
		return errors.New("bucket or name argument is empty")
	}

	dirName := getParentDir(objName)

	tx, err := d.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() // no-op if commit succeeds

	for {
		if _, err = tx.Exec(query, bucket, dirName, newSize, newCount, getParentDir(dirName)); err != nil {
			return err
		}

		// Last directory to update is root
		if dirName == "/" {
			break
		}
		dirName = getParentDir(dirName)
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

// Insert a single directory
func (d *Directory) Insert(dir model.Directory) error {
	query := `
		INSERT INTO directory (bucket, name, parent)		
		VALUES (?, ?, ?)	
	`

	if len(dir.Name) == 0 || len(dir.Bucket) == 0 {
		return errors.New("bucket or name argument is empty")
	}

	parentDir := getParentDir(dir.Name)

	if _, err := d.DB.Exec(query,
		dir.Bucket,
		dir.Name,
		parentDir); err != nil {
		return err
	}
	return nil
}

// Delete a single directory
func (d *Directory) Delete(bucket string, name string) error {
	query := `
		DELETE FROM directory
		WHERE bucket = ? AND name = ?;	
	`

	res, err := d.DB.Exec(query, bucket, name)

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
