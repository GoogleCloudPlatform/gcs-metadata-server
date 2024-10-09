package repo

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/GoogleCloudPlatform/gcs-metadata-server/internal/model"
)

type SortType string

const (
	SortBySize      SortType = "size"
	SortByCount     SortType = "count"
	defaultLocation          = LocationUS
)

type Explore struct {
	*Database
}

type ExploreRepository interface {
	GetPathContents(path string, sort SortType) ([]*model.Metadata, error)
	GetPathSummary(path string) (*model.Summary, error)
}

func NewExploreRepository(db *Database) ExploreRepository {
	return &Explore{db}
}

// GetPath retrieves all directory contents of a given path including itself
// It excludes directories whose size is 0
func (e *Explore) GetPathContents(path string, sortBy SortType) ([]*model.Metadata, error) {
	type contentRow struct {
		Name         string `db:"name"`
		NameLength   int    `db:"name_length"`
		StorageClass string `db:"storage_class"`
		SizeStandard int64  `db:"size_standard"`
		SizeNearline int64  `db:"size_nearline"`
		SizeColdline int64  `db:"size_coldline"`
		SizeArchive  int64  `db:"size_archive"`
		Size         int64  `db:"size"`
		Count        int64  `db:"count"`
		Parent       string `db:"parent"`
	}

	if path == "/" {
		path = "" // handle root
	}

	queryContent := `
		SELECT
			name, 
			LENGTH(name) AS name_length,
			size_standard, 
			size_nearline, 
			size_coldline, 
			size_archive, 
			(size_standard + 
			size_nearline  + 
			size_coldline  + 
			size_archive) AS size, 
			count,
			'' as storage_class,
			parent
		FROM directory
		WHERE
			name LIKE $1 || '%' AND
			NOT name LIKE $1 || '%/%/' AND
			size > 0
		UNION ALL
		SELECT 
			name, 
			LENGTH(name) AS name_length,
			0 as size_standard, 
			0 as size_nearline, 
			0 as size_coldline, 
			0 as size_archive, 
			size, 
			0 as count,
			storage_class,
			'' as parent 
		FROM metadata
		WHERE
			name LIKE $1 || '%' AND
			NOT name LIKE $1 || '%/%'
	`

	if sortBy != SortByCount && sortBy != SortBySize {
		return nil, errors.New("invalid sort parameter")
	}
	queryContent += fmt.Sprintf(" ORDER BY %s DESC, name_length", sortBy)
	queryContent += " LIMIT 100;"

	rows, err := e.DB.Queryx(queryContent, path)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	var pathContents []*model.Metadata
	for rows.Next() {
		var row contentRow
		if err := rows.StructScan(&row); err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}

		metadata := &model.Metadata{
			Name:         row.Name,
			Size:         row.Size,
			Count:        row.Count,
			StorageClass: row.StorageClass,
			Parent:       row.Parent,
		}

		// Calculate costs of every object and directory
		// defaultLocation is used for all results until storing location from bucket is implemented
		if len(metadata.StorageClass) > 0 { // object
			cost, err := getObjectCost(defaultLocation, StorageClass(metadata.StorageClass), metadata.Size)
			if err != nil {
				return nil, err
			}
			metadata.Cost = cost
		} else { // directory
			totalCost, err := getDirectoryCost(defaultLocation, row.SizeStandard, row.SizeNearline, row.SizeColdline, row.SizeArchive)
			if err != nil {
				return nil, err
			}
			metadata.Cost = totalCost
		}

		pathContents = append(pathContents, metadata)
	}

	return pathContents, nil
}

func (e *Explore) GetPathSummary(path string) (*model.Summary, error) {
	var summary model.Summary

	query := `
		SELECT
			name,
			size_standard,
			size_nearline,
			size_coldline,
			size_archive	
		FROM
			directory
		WHERE
			name = $1;
	`

	row := e.DB.QueryRowx(query, path)
	if err := row.StructScan(&summary); err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	// Compute pricing
	storageClasses := []struct {
		class StorageClass
		size  *int64
		cost  *float64
	}{
		{StorageStandard, &summary.Size.Standard, &summary.Cost.Standard},
		{StorageNearline, &summary.Size.Nearline, &summary.Cost.Nearline},
		{StorageColdline, &summary.Size.Coldline, &summary.Cost.Coldline},
		{StorageArchive, &summary.Size.Archive, &summary.Cost.Archive},
	}

	for _, sc := range storageClasses {
		cost, err := getObjectCost(defaultLocation, sc.class, *sc.size)
		if err != nil {
			return nil, err
		}
		*sc.cost = cost
	}

	return &summary, nil
}
