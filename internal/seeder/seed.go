package seeder

import (
	"context"

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
		if err == iterator.Done {
			break
		}

		if err != nil {
			return err
		}

		metadata := newMetadata(obj)

		s.metadataRepo.Insert(metadata)
		s.directoryRepo.UpsertParentDirs(metadata.Bucket, metadata.Name, metadata.Size, 1)
	}
	return nil
}
