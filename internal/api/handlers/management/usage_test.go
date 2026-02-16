package management

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
)

func TestUsageHandlers_ContractAndImportPersistence(t *testing.T) {
	gin.SetMode(gin.TestMode)

	storePath := filepath.Join(t.TempDir(), "usage.db")
	_, err := usage.EnableSQLitePersistence(storePath)
	if err != nil {
		t.Fatalf("enable sqlite persistence: %v", err)
	}
	defer func() {
		_ = usage.CloseSQLitePersistence()
	}()

	stats := usage.NewRequestStatistics()
	h := &Handler{}
	h.SetUsageStatistics(stats)

	router := gin.New()
	router.GET("/usage", h.GetUsageStatistics)
	router.GET("/usage/export", h.ExportUsageStatistics)
	router.POST("/usage/import", h.ImportUsageStatistics)

	importPayload := map[string]any{
		"version": 1,
		"usage": map[string]any{
			"total_requests": 0,
			"success_count":  0,
			"failure_count":  0,
			"total_tokens":   0,
			"apis": map[string]any{
				"api-1": map[string]any{
					"total_requests": 0,
					"total_tokens":   0,
					"models": map[string]any{
						"model-1": map[string]any{
							"total_requests": 0,
							"total_tokens":   0,
							"details": []map[string]any{
								{
									"timestamp":  time.Now().UTC().Format(time.RFC3339Nano),
									"source":     "src",
									"auth_index": "idx",
									"failed":     false,
									"tokens": map[string]any{
										"input_tokens":     3,
										"output_tokens":    4,
										"reasoning_tokens": 0,
										"cached_tokens":    0,
										"total_tokens":     7,
									},
								},
							},
						},
					},
				},
			},
		},
	}
	body, _ := json.Marshal(importPayload)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/usage/import", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 on import, got %d body=%s", w.Code, w.Body.String())
	}

	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodGet, "/usage", nil)
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 on usage, got %d", w.Code)
	}
	var usageResp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &usageResp); err != nil {
		t.Fatalf("decode usage response: %v", err)
	}
	if _, ok := usageResp["usage"]; !ok {
		t.Fatalf("expected 'usage' field in response")
	}
	if _, ok := usageResp["failed_requests"]; !ok {
		t.Fatalf("expected 'failed_requests' field in response")
	}

	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodGet, "/usage/export", nil)
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 on export, got %d", w.Code)
	}
	var exportResp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &exportResp); err != nil {
		t.Fatalf("decode export response: %v", err)
	}
	if _, ok := exportResp["version"]; !ok {
		t.Fatalf("expected 'version' field in export response")
	}
	if _, ok := exportResp["exported_at"]; !ok {
		t.Fatalf("expected 'exported_at' field in export response")
	}
	if _, ok := exportResp["usage"]; !ok {
		t.Fatalf("expected 'usage' field in export response")
	}

	reloadedStats := usage.NewRequestStatistics()
	loadedStore, err := usage.OpenSQLiteStore(storePath)
	if err != nil {
		t.Fatalf("open sqlite store for verification: %v", err)
	}
	snapshot, err := loadedStore.LoadSnapshot()
	if err != nil {
		_ = loadedStore.Close()
		t.Fatalf("load sqlite snapshot: %v", err)
	}
	_ = loadedStore.Close()
	mergeResult := reloadedStats.MergeSnapshot(snapshot)
	if mergeResult.Added != 1 {
		t.Fatalf("expected persisted record count=1, got %d", mergeResult.Added)
	}
	if reloadedStats.Snapshot().TotalRequests != 1 {
		t.Fatalf("expected reloaded total_requests=1")
	}
}
