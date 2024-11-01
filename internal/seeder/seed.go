package seeder

import (
	"context"
	"log"
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
	batchWriter   repo.BatchWriterRepository
	maxSemaphores int
}

const rootDir = ""

func NewSeedService(client *storage.Client, bucketId string, db *repo.Database, maxSemaphores, batchSize int, flushInterval time.Duration) *SeedService {
	return &SeedService{
		client:        client,
		bucketId:      bucketId,
		batchWriter:   repo.NewBatchWriter(db, batchSize, flushInterval),
		maxSemaphores: maxSemaphores,
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

// seed traverses the bucket recursively while sending messages to batch writer
func (s *SeedService) seed(ctx context.Context, b *storage.BucketHandle) error {
	var wg sync.WaitGroup

	semaphore := make(chan struct{}, s.maxSemaphores)

	// Start with the root directory until no more directories are found
	wg.Add(1) // Prevent race condition by allocating waitgroup in advance
	go s.traverseRecursive(ctx, b, rootDir, &wg, semaphore)

	wg.Wait()
	return nil
}

// traverseRecursive iterates through bucket directories and invokes a new goroutine for each new directory found
// and a new goroutine to do a batch insert for each new object found
func (s *SeedService) traverseRecursive(ctx context.Context, b *storage.BucketHandle, dir string, wg *sync.WaitGroup, semaphore chan struct{}) {
	defer wg.Done()

	semaphore <- struct{}{}
	defer func() { <-semaphore }()

	it := b.Objects(ctx, &storage.Query{Prefix: dir, Delimiter: "/", IncludeFoldersAsPrefixes: true})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Printf("Error iterating through objects: %v\n", err)
			return
		}

		if attrs.Prefix != "" {
			wg.Add(1)
			go s.traverseRecursive(ctx, b, attrs.Prefix, wg, semaphore)
		} else {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s.batchWriter.Add(newMetadata(attrs))
			}()
		}
	}
}
