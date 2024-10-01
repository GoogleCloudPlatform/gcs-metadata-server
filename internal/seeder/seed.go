package seeder

import (
	"context"

	"cloud.google.com/go/storage"
	"github.com/BrandonY/gcs-metadata-server/internal/repo"
)

type SeedService struct {
	client        *storage.Client
	bucketId      string
	directoryRepo repo.DirectoryRepository
	metadataRepo  repo.MetadataRepository
}

func NewSeedService(client *storage.Client, bucketId string, directoryRepo repo.DirectoryRepository, metadataRepo repo.MetadataRepository) *SeedService {
	return &SeedService{
		client:        client,
		bucketId:      bucketId,
		directoryRepo: directoryRepo,
		metadataRepo:  metadataRepo,
	}
}

// Seed initiates the seeding process by traversing bucket and inserting into db
func (s *SeedService) Start(ctx context.Context) error {
	return nil
}
