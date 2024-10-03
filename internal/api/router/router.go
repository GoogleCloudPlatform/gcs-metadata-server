package router

import (
	"net/http"

	"github.com/GoogleCloudPlatform/gcs-metadata-server/internal/api/handler"
	"github.com/GoogleCloudPlatform/gcs-metadata-server/internal/repo"
)

func New(db *repo.Database) *http.ServeMux {
	mux := http.NewServeMux()

	exploreRepo := repo.NewExploreRepository(db)
	exploreHandler := handler.NewExploreHandler(exploreRepo)

	mux.HandleFunc("GET /explore/{path...}", exploreHandler.HandleExplore)

	return mux
}
