package repo

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/GoogleCloudPlatform/gcs-metadata-server/internal/model"
)

func TestInsertMetadata(t *testing.T) {
	db := NewDatabase(":memory:", 1)
	db.Connect(context.Background())
	defer db.Close()

	if err := db.Setup(); err != nil {
		t.Fatal(err)
	}

	if err := db.CreateTables(); err != nil {
		t.Fatal(err)
	}

	metadataRepo := NewMetadataRepository(db)

	testCases := []struct {
		name     string
		metadata *model.Metadata
		wantErr  bool
	}{
		{
			"Inserts valid metadata",
			&model.Metadata{
				Bucket:       "mock",
				Name:         "mock/mock.txt",
				Size:         1,
				StorageClass: "STANDARD",
				Created:      time.Now(),
				Updated:      time.Now(),
			},
			false,
		},
		{
			"Does not insert empty fields",
			&model.Metadata{
				Bucket:       "mock",
				Name:         "",
				Size:         1,
				StorageClass: "STANDARD",
				Created:      time.Now(),
				Updated:      time.Now(),
			},
			true,
		},
		{
			"Does not insert duplicates",
			&model.Metadata{
				Bucket:       "mock",
				Name:         "mock/mock.txt",
				Size:         1,
				StorageClass: "STANDARD",
				Created:      time.Now(),
				Updated:      time.Now(),
			},
			true,
		},
		{"Fails to insert empty metadata", &model.Metadata{}, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if err := metadataRepo.Insert(tc.metadata); err != nil {
				if tc.wantErr {
					return
				}
				t.Fatal(err)
			}

			if tc.wantErr {
				log.Fatal("Expected error but did pass")
			}

			// Check if the metadata was inserted
			var exists bool
			if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM metadata WHERE name = ?)`, tc.metadata.Name).Scan(&exists); err != nil {
				t.Fatal(err)
			}

			if !exists {
				t.Fatalf("%s: %s was not inserted", tc.name, tc.metadata.Name)
			}
		})
	}
}
func TestUpdateMetadata(t *testing.T) {
	db := NewDatabase(":memory:", 1)
	db.Connect(context.Background())
	defer db.Close()

	if err := db.Setup(); err != nil {
		t.Fatal(err)
	}

	if err := db.CreateTables(); err != nil {
		t.Fatal(err)
	}

	metadataRepo := NewMetadataRepository(db)
	mockMetadata := &model.Metadata{
		Bucket:       "mock",
		Name:         "mock/mock.txt",
		Size:         1,
		StorageClass: "STANDARD",
		Created:      time.Now(),
		Updated:      time.Now(),
	}

	// Insert initial metadata
	if err := metadataRepo.Insert(mockMetadata); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		testName string
		metadata *model.Metadata
		wantErr  bool
	}{
		{
			"Updates existing metadata",
			&model.Metadata{Bucket: mockMetadata.Bucket, Name: mockMetadata.Name, Size: 100, Updated: time.Now()},
			false,
		},
		{
			"Fails to update non-existent metadata",
			&model.Metadata{Bucket: "fake-bucket", Name: "fake-name.txt", Size: 100, Updated: time.Now()},
			true,
		},
		{
			"Fails to update empty values",
			&model.Metadata{Bucket: "", Name: "", Size: 0, Updated: time.Now()},
			true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			if err := metadataRepo.Update(tc.metadata.Bucket, tc.metadata.Name, tc.metadata.Size, tc.metadata.Updated); err != nil {
				if tc.wantErr {
					return
				}
				log.Fatal(err)
			}

			if tc.wantErr {
				log.Fatal("Expected error but did pass")
			}

			// Verify metadata was updated
			var gotSize int64
			var gotUpdated time.Time

			row := db.QueryRow(`SELECT size, updated FROM metadata WHERE bucket = ? AND name = ?;`, tc.metadata.Bucket, tc.metadata.Name)

			err := row.Scan(&gotSize, &gotUpdated)
			if err != nil {
				log.Fatal()
			}

			if gotSize != tc.metadata.Size || gotUpdated.Unix() != tc.metadata.Updated.Unix() {
				t.Fatalf("got (%d, %v), want (%d, %v)", gotSize, gotUpdated, tc.metadata.Size, tc.metadata.Updated)
			}
		})
	}
}

func TestDeleteMetadata(t *testing.T) {
	db := NewDatabase(":memory:", 1)
	db.Connect(context.Background())
	defer db.Close()

	if err := db.Setup(); err != nil {
		t.Fatal(err)
	}

	if err := db.CreateTables(); err != nil {
		t.Fatal(err)
	}

	mockMetadata := &model.Metadata{
		Bucket:       "mock",
		Name:         "mock/mock.txt",
		Size:         1,
		StorageClass: "STANDARD",
		Created:      time.Now(),
		Updated:      time.Now(),
	}

	metadataRepo := NewMetadataRepository(db)
	metadataRepo.Insert(mockMetadata)

	testCases := []struct {
		name     string
		metadata *model.Metadata
		wantErr  bool
	}{
		{"Deletes metadata", mockMetadata, false},
		{"Fails deleting non-existent metadata", &model.Metadata{Bucket: "fake", Name: "fake"}, true},
		{"Fails to delete empty values", &model.Metadata{Bucket: "", Name: ""}, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if err := metadataRepo.Delete(tc.metadata.Bucket, tc.metadata.Name); err != nil {
				if tc.wantErr {
					return
				}
				t.Fatal(err)
			}

			if tc.wantErr {
				t.Fatal("Expected error but did pass")
			}

			var exists bool
			if err := db.QueryRow(`
				SELECT EXISTS(SELECT 1 FROM metadata WHERE bucket = ? AND name = ?)`, tc.metadata.Bucket, tc.metadata.Name).Scan(&exists); err != nil {
				t.Fatal(err)
			}

			if exists {
				t.Fatal("Metadata was not deleted")
			}
		})
	}

}
