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
	UpsertParentDirs(bucket string, objName string, newSize int64, newCount int64) error
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

// UpsertParentDirs updates all parent directories of an object name in one transaction
func (d *Directory) UpsertParentDirs(bucket string, objName string, newSize int64, newCount int64) error {
	// getUpsertQuery returns a full query string to insert or update a directory
	getUpsertQuery := func(bucket string, name string, size int64, count int64) string {
		parentDir := getParentDir(name)

		return fmt.Sprintf(`
			INSERT INTO directory (bucket, name, size, count, parent)
			VALUES ('%s','%s', %d, %d,'%s')
			ON CONFLICT(bucket, name)
			DO UPDATE
			SET size = size + %d,
				count = count + %d;
			`, bucket, name, size, count, parentDir, size, count)
	}

	if len(bucket) == 0 || len(objName) == 0 {
		return errors.New("bucket or name argument is empty")
	}

	dirName := getParentDir(objName)
	query := "BEGIN IMMEDIATE;"

	for {
		query += getUpsertQuery(bucket, dirName, newSize, newCount)

		// Last directory to update is root
		if dirName == "/" {
			break
		}
		dirName = getParentDir(dirName)
	}

	query += "COMMIT;"

	if _, err := d.DB.Exec(query); err != nil {
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
		dir.Size,
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
