package handler

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

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
	// Normalize path param by adding slash(/) suffix if missing
	path := r.PathValue("path")
	if len(path) == 0 {
		path = "/"
	} else if !strings.HasSuffix(path, "/") {
		path = path + "/"
	}

	// Validate sort query param
	sortString := r.URL.Query().Get("sort")
	sort := repo.SortType(strings.ToLower(sortString))
	if len(sort) == 0 {
		sort = repo.SortBySize
	} else if sort != repo.SortBySize && sort != repo.SortByCount {
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
