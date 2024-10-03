package repo

import (
	"github.com/GoogleCloudPlatform/gcs-metadata-server/internal/model"
)

type Explore struct {
	*Database
}

type ExploreRepository interface {
	GetPathContents(path, sort string) ([]*model.Metadata, error)
}

func NewExploreRepository(db *Database) ExploreRepository {
	return &Explore{db}
}

// GetPath retrieves all directory contents of a given path
// It excludes directories whose size is 0 
func (e *Explore) GetPathContents(path, sort string) ([]*model.Metadata, error) {
	return nil, nil
}
