package repo

import (
	"errors"

	"github.com/GoogleCloudPlatform/gcs-metadata-server/internal/model"
)

type SortType string

const (
	SortBySize  SortType = "size"
	SortByCount SortType = "count"
)

type Explore struct {
	*Database
}

type ExploreRepository interface {
	GetPathContents(path string, sort SortType) ([]*model.Metadata, error)
}

func NewExploreRepository(db *Database) ExploreRepository {
	return &Explore{db}
}

// GetPath retrieves all directory contents of a given path including itself
// It excludes directories whose size is 0
func (e *Explore) GetPathContents(path string, sort SortType) ([]*model.Metadata, error) {
	if path == "/" {
		path = "" // handle root
	}

	queryContent := `
		SELECT name, size, parent, count -- Include self 
		FROM directory WHERE name = $1
		UNION ALL
		SELECT name, size, parent, count -- Include subdirectories
		FROM directory
		WHERE
			name LIKE $1 || '%/' AND
			NOT name LIKE $1 || '%/%/' AND
			size > 0
		UNION ALL
		SELECT name, size, '' as parent, 0 as count -- Include subfiles
		FROM metadata
		WHERE
			name LIKE $1 || '%' AND
			NOT name LIKE $1 || '%/%'
	`

	if sort == SortByCount {
		queryContent += " ORDER BY count DESC"
	} else if sort == SortBySize {
		queryContent += " ORDER BY size DESC"
	} else {
		return nil, errors.New("invalid sort parameter")
	}
	queryContent += " LIMIT 100;"

	rows, err := e.DB.Queryx(queryContent, path)
	if err != nil {
		return nil, errors.New("query error: " + err.Error())
	}
	defer rows.Close()

	var pathContents []*model.Metadata
	for rows.Next() {
		var metadata model.Metadata
		if err := rows.StructScan(&metadata); err != nil {
			return nil, errors.New("scan error: " + err.Error())
		}
		pathContents = append(pathContents, &metadata)
	}

	return pathContents, nil
}
