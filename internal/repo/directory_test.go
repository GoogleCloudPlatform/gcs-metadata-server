package repo

import (
	"context"
	"log"
	"testing"

	"github.com/BrandonY/gcs-metadata-server/internal/model"
)

func TestGetParentDir(t *testing.T) {
	testCases := []struct {
		name string
		in   string
		want string
	}{
		{"Empty string", "", "/"},
		{"Blank string", " ", "/"},
		{"Root file", "mock", "/"},
		{"Root directory", "/", "/"},
		{"Root level directory", "mock/", "/"},
		{"Root level file", "mock", "/"},
		{"Root level directory with trailing slash", "//mock/", "//"},
		{"Dirty directory", "///mock-1//mock-2///", "///mock-1//mock-2//"},
		{"Empty nested directory", "///", "//"},
		{"Nested directory", "mock-1/mock", "mock-1/"},
		{"Deeply nested directory", "mock-1/mock-2/mock-3", "mock-1/mock-2/"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := getParentDir(tc.in)
			if got != tc.want {
				log.Fatalf("Parent dir return mismatch: got %s, want %s", got, tc.want)
			}
		})
	}
}

func TestUpsertParentDirs(t *testing.T) {
	testCases := []struct {
		name         string
		metadataInDB []*model.Metadata
		in           *model.Metadata
		wantDir      *model.Directory
		wantErr      bool
	}{
		{
			"Upserts nested directory file",
			[]*model.Metadata{
				{Bucket: "mock", Name: "mock-1/mock-2/file1", Size: 1},
				{Bucket: "mock", Name: "mock-1/mock-2/file2", Size: 2},
			},
			&model.Metadata{Bucket: "mock", Name: "file3", Size: 3},
			&model.Directory{Name: "/", Size: 6, Count: 3},
			false,
		},
		{
			"Upserts nested directory file 2",
			[]*model.Metadata{
				{Bucket: "mock", Name: "mock-1/mock-2/file1", Size: 1},
				{Bucket: "mock", Name: "mock-1/mock-2/file2", Size: 2},
				{Bucket: "mock", Name: "file3", Size: 3},
			},
			&model.Metadata{Bucket: "mock", Name: "mock-1/file4", Size: 1},
			&model.Directory{Name: "mock-1/", Size: 4, Count: 3},
			false,
		},
		{
			"Upserts root file directory",
			[]*model.Metadata{
				{Bucket: "mock", Name: "mock-1/mock-2/file1", Size: 1},
				{Bucket: "mock", Name: "mock-1/mock-2/file2", Size: 2},
			},
			&model.Metadata{Bucket: "mock", Name: "file3", Size: 3},
			&model.Directory{Name: "/", Size: 6, Count: 3},
			false,
		},
		{
			"Upserts trailing slash directory",
			[]*model.Metadata{
				{Bucket: "mock", Name: "///file", Size: 3},
			},
			&model.Metadata{Bucket: "mock", Name: "//test/file2", Size: 3},
			&model.Directory{Name: "//test/", Size: 3, Count: 1},
			false,
		},
		{
			"Fails upserting empty values",
			[]*model.Metadata{},
			&model.Metadata{},
			nil,
			true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db := NewDatabase(":memory:", 1)
			db.Connect(context.Background())
			defer db.Close()

			if err := db.Setup(); err != nil {
				t.Fatal(err)
			}

			if err := db.CreateTables(); err != nil {
				t.Fatal(err)
			}

			dirRepo := NewDirectoryRepository(db)

			for _, m := range tc.metadataInDB {
				if err := dirRepo.UpsertParentDirs(m.Bucket, m.Name, m.Size, 1); err != nil {
					log.Fatal(err)
				}
			}

			if err := dirRepo.UpsertParentDirs(tc.in.Bucket, tc.in.Name, tc.in.Size, 1); err != nil {
				if tc.wantErr {
					return
				}
				log.Fatal(err)
			}

			if tc.wantErr {
				log.Fatal("Expected error but did pass")
			}

			var gotCount int64
			var gotSize int64

			err := db.QueryRow(`SELECT count, size FROM directory WHERE name = ?`, tc.wantDir.Name).Scan(&gotCount, &gotSize)
			if err != nil {
				t.Fatal(err)
			}

			if gotCount != tc.wantDir.Count {
				t.Errorf("Directory count mismatch: got %d, want %d", gotCount, tc.wantDir.Count)
			}

			if gotSize != tc.wantDir.Size {
				t.Errorf("Directory size mismatch: got %d, want %d", gotSize, tc.wantDir.Size)
			}
		})
	}
}

func TestInsertDirectory(t *testing.T) {
	testCases := []struct {
		name    string
		dir     model.Directory
		wantErr bool
	}{
		{"Inserts root directory", model.Directory{Bucket: "mock", Name: "/"}, false},
		{"Inserts nested directory", model.Directory{Bucket: "mock", Name: "mock-1/mock-2/"}, false},
		{"Fails inserting invalid directory", model.Directory{Bucket: "", Name: ""}, true},
		{"Fails inserting directory with missing fields", model.Directory{}, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db := NewDatabase(":memory:", 1)
			db.Connect(context.Background())
			defer db.Close()

			if err := db.Setup(); err != nil {
				t.Fatal(err)
			}

			if err := db.CreateTables(); err != nil {
				t.Fatal(err)
			}

			dirRepo := NewDirectoryRepository(db)

			if err := dirRepo.Insert(tc.dir); err != nil {
				if tc.wantErr {
					return
				}
				t.Fatal(err)
			}

			if tc.wantErr {
				log.Fatal("Expected error but did pass")
			}

			// Check testcases that must have passed
			var gotName string

			if err := db.QueryRow(`SELECT name FROM directory WHERE name = ?`, tc.dir.Name).Scan(&gotName); err != nil {
				t.Fatal(err)
			}

			if gotName != tc.dir.Name {
				t.Fatalf("got %s, want %s", gotName, tc.dir.Name)
			}
		})
	}
}

func TestDeleteDirectory(t *testing.T) {
	db := NewDatabase(":memory:", 1)
	db.Connect(context.Background())
	defer db.Close()

	if err := db.Setup(); err != nil {
		t.Fatal(err)
	}

	if err := db.CreateTables(); err != nil {
		t.Fatal(err)
	}

	dirRepo := NewDirectoryRepository(db)

	dirs := []model.Directory{
		{Bucket: "mock", Name: "/"},
		{Bucket: "mock", Name: "mock-1/"},
	}

	for _, dir := range dirs {
		if err := dirRepo.Insert(dir); err != nil {
			log.Fatal(err)
		}
	}

	testCases := []struct {
		name      string
		bucket    string
		dirName   string
		wantError bool
	}{
		{"Deletes root directory", "mock", "/", false},
		{"Deletes nested directory", "mock", "mock-1/", false},
		{"Fails deleting non existent directory", "fake", "fake", true},
		{"Fails deleting empty fields", "", "", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if err := dirRepo.Delete(tc.bucket, tc.dirName); err != nil {
				if tc.wantError {
					return
				}
				t.Fatal(err)
			}

			if tc.wantError {
				log.Fatal("Expected error but did pass")
			}

			var exists bool
			if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM directory WHERE bucket = ? AND name = ?)`, tc.bucket, tc.dirName).Scan(&exists); err != nil {
				t.Fatal(err)
			}

			if exists {
				t.Fatalf("Directory %s was not deleted", tc.dirName)
			}
		})
	}
}
