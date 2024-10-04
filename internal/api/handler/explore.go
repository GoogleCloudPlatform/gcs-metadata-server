package handler

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/GoogleCloudPlatform/gcs-metadata-server/internal/model"
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
	// Normalize path param
	path := r.PathValue("path")
	if len(path) == 0 {
		path = "/"
	} else if path[len(path)-1] != '/' {
		path = path + "/"
	}

	// Validate sort query param
	sort := r.URL.Query().Get("sort")
	if len(sort) == 0 {
		sort = "size"
	} else if sort != "size" && sort != "count" {
		http.Error(w, "Invalid sort parameter, please use 'size' or 'count'", http.StatusBadRequest)
		return
	}

	contents, err := e.exploreRepo.GetPathContents(path, sort)
	if err != nil {
		log.Printf("Error retrieving path contents: %v", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	response := struct {
		Path     string            `json:"path"`
		Contents []*model.Metadata `json:"contents"`
	}{
		Path:     r.PathValue("path"),
		Contents: contents,
	}

	w.Header().Set("Access-Control-Allow-Origin", "*") // TODO: remove in production
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Internal error", http.StatusInternalServerError)
	}
}
