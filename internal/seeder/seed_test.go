package seeder

import (
	"errors"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/GoogleCloudPlatform/gcs-metadata-server/internal/model"
	"github.com/GoogleCloudPlatform/gcs-metadata-server/internal/repo"
	"google.golang.org/api/iterator"
)

func TestInsertFromIterator(t *testing.T) {
	testCases := []struct {
		name       string
		it         *testObjectIterator
		wantCalls  int
		wantErr    bool
		wantErrMsg string
	}{
		{
			name: "Inserts all objects from the iterator",
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
			wantCalls: 2,
			wantErr:   false,
		},
		{
			name: "Does not insert if object name is empty",
			it: &testObjectIterator{
				items: []*storage.ObjectAttrs{
					{
						Bucket:       "mock",
						Name:         "",
						Size:         1,
						StorageClass: "mock",
						Created:      time.Now(),
						Updated:      time.Now(),
					},
				},
			},
			wantCalls: 0,
			wantErr:   false,
		},
		{
			name: "Handles iterator errors",
			it: &testObjectIterator{
				items: []*storage.ObjectAttrs{},
				err:   errors.New("iterator error"),
			},
			wantCalls: 0,
			wantErr:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockBatchWriter := &mockBatchWriter{}
			s := &SeedService{
				batchWriter: mockBatchWriter,
			}

			if err := s.insertFromIterator(tc.it); err != nil {
				if tc.wantErr {
					return
				}
				t.Fatal(err)
			}

			if tc.wantErr {
				t.Errorf("Expected error but did pass")
			}

			if mockBatchWriter.calls != tc.wantCalls {
				t.Errorf("BatchWriter Add calls mismatch: got %d, want %d", mockBatchWriter.calls, tc.wantCalls)
			}
		})
	}
}

type mockBatchWriter struct {
	repo.BatchWriterRepository
	calls int
}

func (m *mockBatchWriter) Add(metadata *model.Metadata) {
	m.calls++
}

type testObjectIterator struct {
	items []*storage.ObjectAttrs
	index int
	err   error
}

func (t *testObjectIterator) Next() (*storage.ObjectAttrs, error) {
	if t.err != nil {
		return nil, t.err
	}

	if t.index >= len(t.items) {
		return nil, iterator.Done
	}

	obj := t.items[t.index]
	t.index++
	return obj, nil
}
