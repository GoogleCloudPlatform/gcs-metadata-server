package repo

import (
	"context"
	"testing"
	"time"

	"github.com/GoogleCloudPlatform/gcs-metadata-server/internal/model"
)

func TestGetPathContents(t *testing.T) {
	db := NewDatabase(":memory:", 1)
	db.Connect(context.Background())
	defer db.Close()

	if err := db.Setup(); err != nil {
		t.Fatal(err)
	}

	if err := db.CreateTables(); err != nil {
		t.Fatal(err)
	}

	exploreRepo := NewExploreRepository(db)
	metadataRepo := NewMetadataRepository(db)
	dirRepo := NewDirectoryRepository(db)

	// Insert mock data
	metadata := []model.Metadata{
		{Bucket: "mock", Name: "file1", Size: 10, StorageClass: "STANDARD", Created: time.Now(), Updated: time.Now()},
		{Bucket: "mock", Name: "file2", Size: 1, StorageClass: "STANDARD", Created: time.Now(), Updated: time.Now()},
		{Bucket: "mock", Name: "mock-1/file3", Size: 1, StorageClass: "STANDARD", Created: time.Now(), Updated: time.Now()},
		{Bucket: "mock", Name: "mock-1//file4", Size: 2, StorageClass: "STANDARD", Created: time.Now(), Updated: time.Now()},
	}

	for _, m := range metadata {
		if err := metadataRepo.Insert(&m); err != nil {
			t.Fatal(err)
		}
		if err := dirRepo.UpsertParentDirs(m.Bucket, m.Name, m.Size, 1); err != nil {
			t.Fatal(err)
		}
	}

	testCases := []struct {
		name    string
		path    string
		sort    string
		want    []*model.Metadata
		wantErr bool
	}{
		{
			"Get root directory contents sorted by size",
			"/",
			"size",
			[]*model.Metadata{
				{Name: "/", Size: 14, Count: 4},
				{Name: "file1", Size: 10, Count: 0},
				{Name: "mock-1/", Size: 3, Count: 2},
				{Name: "file2", Size: 1, Count: 0},
			},
			false,
		},
		{
			"Get root directory contents sorted by count",
			"/",
			"count",
			[]*model.Metadata{
				{Name: "/", Size: 14, Count: 4},
				{Name: "mock-1/", Size: 3, Count: 2},
				{Name: "file1", Size: 10, Count: 0},
				{Name: "file2", Size: 1, Count: 0},
			},
			false,
		},
		{
			"Get nested directory contents sorted by size",
			"mock-1/",
			"size",
			[]*model.Metadata{
				{Name: "mock-1/", Size: 3, Count: 2},
				{Name: "mock-1//", Size: 2, Count: 1},
				{Name: "mock-1/file3", Size: 1, Count: 0},
			},
			false,
		},
		{
			"Get trailing slash directory",
			"mock-1//",
			"size",
			[]*model.Metadata{
				{Name: "mock-1//", Size: 2, Count: 1},
				{Name: "mock-1//file4", Size: 2, Count: 0},
			},
			false,
		},
		{
			"Returns empty for non-existent directory",
			"non-existent/",
			"size",
			nil,
			false,
		},
		{
			"Returns error for invalid sort parameter",
			"/",
			"invalid",
			nil,
			true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := exploreRepo.GetPathContents(tc.path, tc.sort)
			if err != nil {
				if tc.wantErr {
					return
				}
				t.Fatal(err)
			}

			if tc.wantErr {
				t.Fatalf("Expected error but did pass")
			}

			if len(got) != len(tc.want) {
				t.Fatalf("Return count mismatch: got %d, want %d", len(got), len(tc.want))
			}

			for i := range got {
				if got[i].Name != tc.want[i].Name {
					t.Errorf("Return order mismatch: got %v, want %v", got[i].Name, tc.want[i].Name)
				}

				if got[i].Size != tc.want[i].Size {
					t.Errorf("Return size mismatch: got %d, want %d", got[i].Size, tc.want[i].Size)
				}

				if got[i].Count != tc.want[i].Count {
					t.Errorf("Return count mismatch: got %d, want %d", got[i].Count, tc.want[i].Count)
				}
			}
		})
	}
}
