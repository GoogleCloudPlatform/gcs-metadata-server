package repo

import (
	"context"
	"fmt"
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
		{Bucket: "mock", Name: "file1", Size: 10 * bytesPerGB, Cost: 0.23, StorageClass: "STANDARD", Created: time.Now(), Updated: time.Now()},
		{Bucket: "mock", Name: "file2", Size: 1 * bytesPerGB, Cost: 0.023, StorageClass: "STANDARD", Created: time.Now(), Updated: time.Now()},
		{Bucket: "mock", Name: "mock-1/file3", Size: 1 * bytesPerGB, Cost: 0.007, StorageClass: "COLDLINE", Created: time.Now(), Updated: time.Now()},
		{Bucket: "mock", Name: "mock-1//file4", Size: 2 * bytesPerGB, Cost: 0.005, StorageClass: "ARCHIVE", Created: time.Now(), Updated: time.Now()},
	}

	for _, m := range metadata {
		if err := metadataRepo.Insert(&m); err != nil {
			t.Fatal(err)
		}
		if err := dirRepo.UpsertParentDirs(StorageClass(m.StorageClass), m.Bucket, m.Name, m.Size, 1); err != nil {
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
				{Name: "/", Size: 14 * bytesPerGB, Count: 4, Cost: 0.23 + 0.023 + 0.007 + 0.005, StorageClass: "", Parent: ""},
				{Name: "file1", Size: 10 * bytesPerGB, Count: 0, Cost: 0.23, StorageClass: "STANDARD", Parent: ""},
				{Name: "mock-1/", Size: 3 * bytesPerGB, Count: 2, Cost: 0.007 + 0.005, StorageClass: "", Parent: "/"},
				{Name: "file2", Size: 1 * bytesPerGB, Count: 0, Cost: 0.023, StorageClass: "STANDARD", Parent: ""},
			},
			false,
		},
		{
			"Get root directory contents sorted by count",
			"/",
			"count",
			[]*model.Metadata{
				{Name: "/", Size: 14 * bytesPerGB, Count: 4, Cost: 0.23 + 0.023 + 0.007 + 0.005, StorageClass: "", Parent: ""},
				{Name: "mock-1/", Size: 3 * bytesPerGB, Count: 2, Cost: 0.007 + 0.005, StorageClass: "", Parent: "/"},
				{Name: "file1", Size: 10 * bytesPerGB, Count: 0, Cost: 0.23, StorageClass: "STANDARD", Parent: ""},
				{Name: "file2", Size: 1 * bytesPerGB, Count: 0, Cost: 0.023, StorageClass: "STANDARD", Parent: ""},
			},
			false,
		},
		{
			"Get nested directory contents sorted by size",
			"mock-1/",
			"size",
			[]*model.Metadata{
				{Name: "mock-1/", Size: 3 * bytesPerGB, Count: 2, Cost: 0.007 + 0.005, StorageClass: "", Parent: "/"},
				{Name: "mock-1//", Size: 2 * bytesPerGB, Count: 1, Cost: 0.005, StorageClass: "", Parent: "mock-1/"},
				{Name: "mock-1/file3", Size: 1 * bytesPerGB, Count: 0, Cost: 0.007, StorageClass: "COLDLINE", Parent: "mock-1/"},
			},
			false,
		},
		{
			"Get trailing slash directory",
			"mock-1//",
			"size",
			[]*model.Metadata{
				{Name: "mock-1//", Size: 2 * bytesPerGB, Count: 1, Cost: 0.005, StorageClass: "", Parent: "mock-1/"},
				{Name: "mock-1//file4", Size: 2 * bytesPerGB, Count: 0, Cost: 0.005, StorageClass: "ARCHIVE", Parent: "mock-1//"},
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
			got, err := exploreRepo.GetPathContents(tc.path, SortType(tc.sort))
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

				if fmt.Sprintf("%.3f", got[i].Cost) != fmt.Sprintf("%.3f", tc.want[i].Cost) {
					t.Errorf("Return cost mismatch: got %f, want %f", got[i].Cost, tc.want[i].Cost)
				}
			}
		})
	}
}

func TestGetPathSummary(t *testing.T) {
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
		{Bucket: "mock", Name: "file1", Size: 10 * bytesPerGB, Cost: 0.23, StorageClass: "STANDARD", Created: time.Now(), Updated: time.Now()},
		{Bucket: "mock", Name: "file2", Size: 1 * bytesPerGB, Cost: 0.023, StorageClass: "STANDARD", Created: time.Now(), Updated: time.Now()},
		{Bucket: "mock", Name: "mock-1/file3", Size: 1 * bytesPerGB, Cost: 0.007, StorageClass: "COLDLINE", Created: time.Now(), Updated: time.Now()},
		{Bucket: "mock", Name: "mock-1//file4", Size: 2 * bytesPerGB, Cost: 0.005, StorageClass: "ARCHIVE", Created: time.Now(), Updated: time.Now()},
	}

	for _, m := range metadata {
		if err := metadataRepo.Insert(&m); err != nil {
			t.Fatal(err)
		}
		if err := dirRepo.UpsertParentDirs(StorageClass(m.StorageClass), m.Bucket, m.Name, m.Size, 1); err != nil {
			t.Fatal(err)
		}
	}

	testCases := []struct {
		name    string
		path    string
		want    *model.Summary
		wantErr bool
	}{
		{
			"Get root directory summary",
			"/",
			&model.Summary{
				Path: "/",
				Cost: model.Cost{
					Standard: 0.253,
					Nearline: 0,
					Coldline: 0.007,
					Archive:  0.005,
				},
				Size: model.Size{
					Standard: 11 * bytesPerGB,
					Nearline: 0,
					Coldline: 1 * bytesPerGB,
					Archive:  2 * bytesPerGB,
				},
			},
			false,
		},
		{
			"Get nested directory summary",
			"mock-1/",
			&model.Summary{
				Path: "mock-1/",
				Cost: model.Cost{
					Standard: 0,
					Nearline: 0,
					Coldline: 0.007,
					Archive:  0.005,
				},
				Size: model.Size{
					Standard: 0,
					Nearline: 0,
					Coldline: 1 * bytesPerGB,
					Archive:  2 * bytesPerGB,
				},
			},
			false,
		},
		{
			"Get trailing slash directory summary",
			"mock-1//",
			&model.Summary{
				Path: "mock-1//",
				Cost: model.Cost{
					Standard: 0,
					Nearline: 0,
					Coldline: 0,
					Archive:  0.005,
				},
				Size: model.Size{
					Standard: 0,
					Nearline: 0,
					Coldline: 0,
					Archive:  2 * bytesPerGB,
				},
			},
			false,
		},
		{
			"Returns empty for non-existent directory",
			"non-existent/",
			&model.Summary{},
			false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := exploreRepo.GetPathSummary(tc.path)
			if err != nil {
				if tc.wantErr {
					return
				}
				t.Fatal(err)
			}

			if tc.wantErr {
				t.Fatalf("Expected error but did pass")
			}

			if got.Path != tc.want.Path {
				t.Errorf("Path mismatch: got %s, want %s", got.Path, tc.want.Path)
			}

			// Compare cost and size
			storageClasses := []StorageClass{
				StorageStandard,
				StorageNearline,
				StorageColdline,
				StorageArchive,
			}

			gotCosts := []float64{
				got.Cost.Standard,
				got.Cost.Nearline,
				got.Cost.Coldline,
				got.Cost.Archive,
			}

			wantCosts := []float64{
				tc.want.Cost.Standard,
				tc.want.Cost.Nearline,
				tc.want.Cost.Coldline,
				tc.want.Cost.Archive,
			}

			for i := range gotCosts {
				if fmt.Sprintf("%.3f", gotCosts[i]) != fmt.Sprintf("%.3f", wantCosts[i]) {
					t.Errorf("%s cost mismatch: got %f, want %f", storageClasses[i], gotCosts[i], wantCosts[i])
				}
			}

			gotSizes := []int64{
				got.Size.Standard,
				got.Size.Nearline,
				got.Size.Coldline,
				got.Size.Archive,
			}

			wantSizes := []int64{
				tc.want.Size.Standard,
				tc.want.Size.Nearline,
				tc.want.Size.Coldline,
				tc.want.Size.Archive,
			}

			for i := range gotSizes {
				if gotSizes[i] != wantSizes[i] {
					t.Errorf("%s size mismatch: got %d, want %d", storageClasses[i], gotSizes[i], wantSizes[i])
				}
			}
		})
	}
}
