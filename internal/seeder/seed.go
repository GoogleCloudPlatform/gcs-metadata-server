package seeder

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/GoogleCloudPlatform/gcs-metadata-server/internal/model"
	"github.com/GoogleCloudPlatform/gcs-metadata-server/internal/repo"
	"google.golang.org/api/iterator"
)

type SeedService struct {
	client        *storage.Client
	bucketId      string
	directoryRepo repo.DirectoryRepository
	metadataRepo  repo.MetadataRepository
	batchWriter   *repo.BatchWriter
}

func NewSeedService(client *storage.Client, bucketId string, db *repo.Database, batchSize int, flushInterval time.Duration) *SeedService {
	return &SeedService{
		client:      client,
		bucketId:    bucketId,
		batchWriter: repo.NewBatchWriter(db, batchSize, flushInterval),
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

	// Start the batch writer
	s.batchWriter.Start(ctx)
	defer s.batchWriter.Stop()

	if err := s.seed(ctx, b); err != nil {
		return err
	}

	return nil
}

// seed traverses the bucket recursively and sends directories to the worker pool.
func (s *SeedService) seed(ctx context.Context, b *storage.BucketHandle) error {
	dirChan := make(chan string)
	var wg sync.WaitGroup

	semaphore := make(chan struct{}, 1000)

	// Start with the root directory
	wg.Add(1)
	semaphore <- struct{}{}
	go s.traverseDirectory(ctx, b, "", dirChan, &wg, semaphore)

	wg.Wait()
	close(dirChan)

	return nil
}

func (s *SeedService) traverseRecursive(ctx context.Context, b *storage.BucketHandle, dir string, dirChan chan<- string, wg *sync.WaitGroup, semaphore chan struct{}) {
	defer wg.Done()
	defer func() { <-semaphore }() // Release after the recursive call finishes

	it := b.Objects(ctx, &storage.Query{Prefix: dir, Delimiter: "/", IncludeFoldersAsPrefixes: true})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			fmt.Printf("Error iterating through objects: %v\n", err)
			return // Stop listing directories on error
		}

		if attrs.Prefix != "" {
			wg.Add(1)
			semaphore <- struct{}{}

			go s.traverseDirectory(ctx, b, attrs.Prefix, dirChan, wg, semaphore)
		} else {
			s.batchWriter.Add(newMetadata(attrs))
		}
	}
}

// insertFromIterator traverses iterator while inserting all containing items into db
func (s *SeedService) insertFromIterator(it objectIterator) error {
	for {
		obj, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}

			return fmt.Errorf("error retrieving iterator object: %v", err)
		}

		if obj.Name != "" {
			// Add metadata to the batch writer
			s.batchWriter.Add(newMetadata(obj))
		}
	}
	return nil
}
