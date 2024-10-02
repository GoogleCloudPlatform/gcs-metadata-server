package seeder

import (
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/GoogleCloudPlatform/gcs-metadata-server/internal/model"
	"github.com/GoogleCloudPlatform/gcs-metadata-server/internal/repo"
	"google.golang.org/api/iterator"
)

func TestReadFromIterator(t *testing.T) {
	testCases := []struct {
		name string
		it   *testObjectIterator
	}{
		{
			name: "Succeeds iterating through objects",
			it: &testObjectIterator{
				items: []*storage.ObjectAttrs{
					{
						Bucket:       "mock",
						Name:         "mock",
						Size:         1,
						StorageClass: "mock",
						Created:      time.Now(),
						Updated:      time.Now(),
					},
					{
						Bucket:       "mock",
						Name:         "dir/mock",
						Size:         1,
						StorageClass: "mock",
						Created:      time.Now(),
						Updated:      time.Now(),
					},
				},
			},
		},
		{
			name: "Succeeds if iterator is empty",
			it: &testObjectIterator{
				items: []*storage.ObjectAttrs{},
			},
		},
		{
			name: "Does not return errors if item data is malformed",
			it: &testObjectIterator{
				items: []*storage.ObjectAttrs{
					{
						Bucket:       "mock",
						Size:         -1,
						StorageClass: "mock",
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockMetadataRepo := &mockMetadataRepository{}
			mockDirRepo := &mockDirectoryRepository{}

			s := &SeedService{
				metadataRepo:  mockMetadataRepo,
				directoryRepo: mockDirRepo,
			}

			err := s.insertFromIterator(tc.it)
			if err != nil {
				t.Fatal(err)
			}

			if mockMetadataRepo.calls != len(tc.it.items) {
				t.Errorf("Metadata Insert calls mismatch: got %d, want %d", mockMetadataRepo.calls, len(tc.it.items))
			}

			if mockDirRepo.calls != len(tc.it.items) {
				t.Errorf("Directory Upsert calls mismatch: got %d, want %d", mockDirRepo.calls, len(tc.it.items))
			}
		})
	}
}

type testObjectIterator struct {
	items []*storage.ObjectAttrs
	index int
}

func (t *testObjectIterator) Next() (*storage.ObjectAttrs, error) {
	if t.index >= len(t.items) {
		return nil, iterator.Done
	}

	obj := t.items[t.index]
	t.index++
	return obj, nil
}

type mockMetadataRepository struct {
	repo.MetadataRepository
	calls int
}

func (m *mockMetadataRepository) Insert(metadata *model.Metadata) error {
	m.calls++
	return nil
}

type mockDirectoryRepository struct {
	repo.DirectoryRepository
	calls int
}

func (d *mockDirectoryRepository) UpsertParentDirs(bucket string, objName string, newSize int64, newCount int64) error {
	d.calls++
	return nil
}
