package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/GoogleCloudPlatform/gcs-metadata-server/internal/api/router"
	"github.com/GoogleCloudPlatform/gcs-metadata-server/internal/repo"
	"github.com/jessevdk/go-flags"
)

type options struct {
	Port        int    `short:"p" long:"port" description:"Port for API to listen on" required:"true"`
	DatabaseUrl string `short:"d" long:"database-url" description:"Database URL in which to store metadata" required:"true"`
}

const maxDbConnections = 5

func main() {
	var opts options
	if _, err := flags.Parse(&opts); err != nil {
		os.Exit(1)
	}

	// Connect database
	ctx := context.Background()
	db := repo.NewDatabase(opts.DatabaseUrl, maxDbConnections)

	if err := db.Connect(ctx); err != nil {
		log.Fatalf("Error connecting to database: %v\n", err)
	}
	defer db.Close()

	if exists, err := db.PingTable(); !exists || err != nil {
		log.Fatalf("Database has not been initialized: %v\n", err)
	}

	// Start server
	router := router.New(db)
	server := http.Server{
		Addr:    fmt.Sprintf(":%d", opts.Port),
		Handler: router,
	}

	log.Println("Starting server on port", server.Addr)
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Error in server: %v\n", err)
	}
}
