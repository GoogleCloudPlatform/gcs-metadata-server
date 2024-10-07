package handler

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/GoogleCloudPlatform/gcs-metadata-server/internal/model"
	"github.com/GoogleCloudPlatform/gcs-metadata-server/internal/repo"
)

func TestHandleExplore(t *testing.T) {
	testCases := []struct {
		name       string
		path       string
		sort       string
		wantStatus int
	}{
		{
			"Valid path with trailing slash",
			"/mock/",
			"",
			http.StatusOK,
		},
		{
			"Valid path without trailing slash",
			"/mock",
			"",
			http.StatusOK,
		},
		{
			"Root path",
			"/",
			"",
			http.StatusOK,
		},
		{
			"Empty path (root)",
			"",
			"",
			http.StatusOK,
		},
		{
			"Valid sort parameter 'count'",
			"/mock/",
			"count",
			http.StatusOK,
		},
		{
			"Invalid sort parameter",
			"/mock/",
			"invalid",
			http.StatusBadRequest,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", "/explore/"+tc.path+"?sort="+tc.sort, nil)
			if err != nil {
				t.Fatal(err)
			}

			rr := httptest.NewRecorder()
			mockRepo := &mockExploreRepository{
				pathContents: []*model.Metadata{},
			}

			handler := NewExploreHandler(mockRepo)
			handler.HandleExplore(rr, req)

			if status := rr.Code; status != tc.wantStatus {
				t.Errorf("status code mismatch: got %v want %v",
					status, tc.wantStatus)
			}

			if tc.wantStatus != http.StatusOK {
				return
			}
		})
	}
}

type mockExploreRepository struct {
	pathContents []*model.Metadata
}

func (m *mockExploreRepository) GetPathContents(path string, sort repo.SortType) ([]*model.Metadata, error) {
	return m.pathContents, nil
}
