package main

import (
	"context"
	"log"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"github.com/GoogleCloudPlatform/gcs-metadata-server/internal/repo"
	"github.com/GoogleCloudPlatform/gcs-metadata-server/internal/seeder"
	"github.com/jessevdk/go-flags"
)

type options struct {
	BucketId    string `short:"b" long:"bucket-id" description:"Bucket ID to fetch metadata from" required:"true"`
	DatabaseUrl string `short:"d" long:"database-url" description:"Database URL in which to store metadata" required:"true"`
}

const maxDbConnections = 1

func main() {
	var opts options
	if _, err := flags.Parse(&opts); err != nil {
		os.Exit(1)
	}

	log.Println("Starting seeding service")
	log.Println("Bucket ID:", opts.BucketId)
	log.Println("Database URL:", opts.DatabaseUrl)

	// Connect database
	ctx := context.Background()
	db := repo.NewDatabase(opts.DatabaseUrl, maxDbConnections)

	if err := db.Connect(ctx); err != nil {
		log.Fatalf("Error connecting to database: %v\n", err)
	}
	defer db.Close()

	if err := db.Setup(); err != nil {
		log.Fatalf("Error configuring database: %v\n", err)
	}

	if err := db.CreateTables(); err != nil {
		log.Fatalf("Error creating tables: %v\n", err)
	}

	// Connect to storage client
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Error creating storage client: %v\n", err)
	}

	// Instantiate repositories
	directoryRepo := repo.NewDirectoryRepository(db)
	metadataRepo := repo.NewMetadataRepository(db)

	seedService := seeder.NewSeedService(client, opts.BucketId, directoryRepo, metadataRepo)

	// Begin seeding
	start := time.Now()

	if err := seedService.Start(ctx); err != nil {
		log.Fatalf("Error while seeding: %v\n", err)
	}

	log.Printf("Seeding completed. Duration: %v\n", time.Since(start))
}
