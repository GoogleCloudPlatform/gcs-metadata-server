package handler

import (
	"net/http"

	"github.com/GoogleCloudPlatform/gcs-metadata-server/internal/repo"
)

type ExploreHandler interface {
	Explore(w http.ResponseWriter, r *http.Request)
}

type exploreHandler struct {
	exploreRepo repo.ExploreRepository
}

func NewExploreHandler(exploreRepo repo.ExploreRepository) *exploreHandler {
	return &exploreHandler{exploreRepo}
}

func (e *exploreHandler) HandleExplore(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}
