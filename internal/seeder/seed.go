package seeder

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/storage"
	"github.com/BrandonY/gcs-metadata-server/internal/model"
	"github.com/BrandonY/gcs-metadata-server/internal/repo"
	"google.golang.org/api/iterator"
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

func newMetadata(obj *storage.ObjectAttrs) *model.Metadata {
	return &model.Metadata{
		Bucket:       obj.Bucket,
		Name:         obj.Name,
		Size:         obj.Size,
		StorageClass: obj.StorageClass,
		Created:      obj.Created,
		Updated:      obj.Updated,
	}

}

type objectIterator interface {
	Next() (*storage.ObjectAttrs, error)
}

// Seed initiates the seeding process by traversing bucket and inserting into db
func (s *SeedService) Start(ctx context.Context) error {
	b := s.client.Bucket(s.bucketId)
	if _, err := b.Attrs(ctx); err != nil {
		return err
	}

	it := b.Objects(ctx, nil)
	if err := s.insertFromIterator(it); err != nil {
		return err
	}
	return nil
}

// insertFromIterator traverses iterator while inserting all containing items into db
func (s *SeedService) insertFromIterator(it objectIterator) error {
	for {
		obj, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}

			return fmt.Errorf("Error retrieving iterator object: %v", err)
		}

		metadata := newMetadata(obj)

		if err := s.metadataRepo.Insert(metadata); err != nil {
			log.Printf("Error inserting metadata: %v", err)
		}

		if err := s.directoryRepo.UpsertParentDirs(metadata.Bucket, metadata.Name, metadata.Size, 1); err != nil {
			log.Printf("Error upserting directories: %v", err)
		}
	}
	return nil
}
