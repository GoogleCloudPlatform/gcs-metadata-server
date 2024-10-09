package repo

import (
	"context"
	"log"
	"testing"

	"github.com/GoogleCloudPlatform/gcs-metadata-server/internal/model"
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
	type dir struct {
		Name         string
		SizeStandard int64 `db:"size_standard"`
		SizeNearline int64 `db:"size_nearline"`
		SizeColdline int64 `db:"size_coldline"`
		SizeArchive  int64 `db:"size_archive"`
		Count        int64
	}

	testCases := []struct {
		name         string
		metadataInDB []*model.Metadata
		in           *model.Metadata
		wantDirs     []*dir
		wantErr      bool
	}{
		{
			"Upserts nested directory file",
			[]*model.Metadata{
				{Bucket: "mock", Name: "mock-1/mock-2/file1", Size: 1, StorageClass: "STANDARD"},
				{Bucket: "mock", Name: "mock-1/mock-2/file2", Size: 2, StorageClass: "NEARLINE"},
			},
			&model.Metadata{Bucket: "mock", Name: "file3", Size: 3, StorageClass: "COLDLINE"},
			[]*dir{
				{Name: "/", SizeStandard: 1, SizeNearline: 2, SizeColdline: 3, Count: 3},
				{Name: "mock-1/", SizeStandard: 1, SizeNearline: 2, SizeColdline: 0, Count: 2},
				{Name: "mock-1/mock-2/", SizeStandard: 1, SizeNearline: 2, SizeColdline: 0, Count: 2},
			},
			false,
		},
		{
			"Upserts nested directory file 2",
			[]*model.Metadata{
				{Bucket: "mock", Name: "mock-1/mock-2/file1", Size: 1, StorageClass: "STANDARD"},
				{Bucket: "mock", Name: "mock-1/mock-2/file2", Size: 2, StorageClass: "NEARLINE"},
				{Bucket: "mock", Name: "file3", Size: 3, StorageClass: "STANDARD"},
			},
			&model.Metadata{Bucket: "mock", Name: "mock-1/file4", Size: 1, StorageClass: "ARCHIVE"},
			[]*dir{
				{Name: "mock-1/", SizeStandard: 1, SizeNearline: 2, SizeArchive: 1, Count: 3},
			},
			false,
		},
		{
			"Upserts root file directory",
			[]*model.Metadata{
				{Bucket: "mock", Name: "mock-1/mock-2/file1", Size: 1, StorageClass: "STANDARD"},
				{Bucket: "mock", Name: "mock-1/mock-2/file2", Size: 2, StorageClass: "NEARLINE"},
			},
			&model.Metadata{Bucket: "mock", Name: "file3", Size: 3, StorageClass: "COLDLINE"},
			[]*dir{
				{Name: "/", SizeStandard: 1, SizeNearline: 2, SizeColdline: 3, Count: 3},
			},
			false,
		},
		{
			"Upserts trailing slash directory",
			[]*model.Metadata{
				{Bucket: "mock", Name: "///file", Size: 3, StorageClass: "STANDARD"},
			},
			&model.Metadata{Bucket: "mock", Name: "//test/file2", Size: 3, StorageClass: "NEARLINE"},
			[]*dir{
				{Name: "//test/", SizeNearline: 3, Count: 1},
			},
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
				if err := dirRepo.UpsertParentDirs(StorageClass(m.StorageClass), m.Bucket, m.Name, m.Size, 1); err != nil {
					log.Fatal(err)
				}
			}

			if err := dirRepo.UpsertParentDirs(StorageClass(tc.in.StorageClass), tc.in.Bucket, tc.in.Name, tc.in.Size, 1); err != nil {
				if tc.wantErr {
					return
				}
				log.Fatal(err)
			}

			if tc.wantErr {
				log.Fatal("Expected error but did pass")
			}

			for _, wantDir := range tc.wantDirs {
				var gotDir dir
				err := db.QueryRowx(`SELECT count, size_standard, size_nearline, size_coldline, size_archive 
									FROM directory WHERE name = ?`, wantDir.Name).StructScan(&gotDir)
				if err != nil {
					t.Fatal(err)
				}

				if gotDir.Count != wantDir.Count {
					t.Errorf("Directory count mismatch: got %d, want %d", gotDir.Count, wantDir.Count)
				}

				if gotDir.SizeStandard != wantDir.SizeStandard {
					t.Errorf("Directory size standard mismatch: got %d, want %d", gotDir.SizeStandard, wantDir.SizeStandard)
				}

				if gotDir.SizeNearline != wantDir.SizeNearline {
					t.Errorf("Directory size nearline mismatch: got %d, want %d", gotDir.SizeNearline, wantDir.SizeNearline)
				}

				if gotDir.SizeColdline != wantDir.SizeColdline {
					t.Errorf("Directory size coldline mismatch: got %d, want %d", gotDir.SizeColdline, wantDir.SizeColdline)
				}

				if gotDir.SizeArchive != wantDir.SizeArchive {
					t.Errorf("Directory size archive mismatch: got %d, want %d", gotDir.SizeArchive, wantDir.SizeArchive)
				}
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
