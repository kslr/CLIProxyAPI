package usage

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

func TestSQLitePlugin_HandleUsage_PersistsRecord(t *testing.T) {
	store, cleanup := openTempSQLiteStore(t)
	defer cleanup()
	SetSQLiteStore(store)
	defer SetSQLiteStore(nil)

	plugin := NewSQLitePlugin()
	record := coreusage.Record{
		Provider:    "openai",
		Model:       "gpt-test",
		APIKey:      "api-key-1",
		Source:      "src-1",
		AuthIndex:   "idx-1",
		RequestedAt: time.Now().UTC().Truncate(time.Nanosecond),
		Failed:      false,
		Detail: coreusage.Detail{
			InputTokens:  3,
			OutputTokens: 4,
			TotalTokens:  7,
		},
	}

	plugin.HandleUsage(context.Background(), record)

	snapshot, err := store.LoadSnapshot()
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if snapshot.TotalRequests != 1 {
		t.Fatalf("expected total_requests=1, got %d", snapshot.TotalRequests)
	}
	if snapshot.TotalTokens != 7 {
		t.Fatalf("expected total_tokens=7, got %d", snapshot.TotalTokens)
	}
}

func TestSQLitePlugin_HandleUsage_RespectsStatisticsEnabled(t *testing.T) {
	store, cleanup := openTempSQLiteStore(t)
	defer cleanup()
	SetSQLiteStore(store)
	defer SetSQLiteStore(nil)

	old := StatisticsEnabled()
	SetStatisticsEnabled(false)
	defer SetStatisticsEnabled(old)

	plugin := NewSQLitePlugin()
	plugin.HandleUsage(context.Background(), coreusage.Record{
		APIKey:      "api-key",
		Model:       "model",
		RequestedAt: time.Now().UTC(),
		Detail: coreusage.Detail{
			InputTokens:  1,
			OutputTokens: 1,
			TotalTokens:  2,
		},
	})

	snapshot, err := store.LoadSnapshot()
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if snapshot.TotalRequests != 0 {
		t.Fatalf("expected no persisted events, got %d", snapshot.TotalRequests)
	}
}

func TestEnableSQLitePersistence_ReplaysAndKeepsDedup(t *testing.T) {
	SetSQLiteStore(nil)

	dir := t.TempDir()
	path := filepath.Join(dir, "usage.db")
	store, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Nanosecond)
	_, err = store.Insert(PersistedUsageEvent{
		APIName: "api1",
		Model:   "m1",
		Detail: RequestDetail{
			Timestamp: now,
			Tokens:    TokenStats{InputTokens: 2, OutputTokens: 3, TotalTokens: 5},
		},
	})
	if err != nil {
		t.Fatalf("seed sqlite event: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close sqlite store: %v", err)
	}

	stats := NewRequestStatistics()
	oldDefault := defaultRequestStatistics
	defaultRequestStatistics = stats
	defer func() { defaultRequestStatistics = oldDefault }()

	result, err := EnableSQLitePersistence(path)
	if err != nil {
		t.Fatalf("enable sqlite persistence: %v", err)
	}
	defer func() {
		_ = CloseSQLitePersistence()
	}()

	if result.Added != 1 {
		t.Fatalf("expected recovered_added=1, got %d", result.Added)
	}
	snapshot := stats.Snapshot()
	if snapshot.TotalRequests != 1 {
		t.Fatalf("expected in-memory total_requests=1 after replay, got %d", snapshot.TotalRequests)
	}

	result2, err := EnableSQLitePersistence(path)
	if err != nil {
		t.Fatalf("re-enable sqlite persistence: %v", err)
	}
	if result2.Added != 0 {
		t.Fatalf("expected no additional records on replay, got %d", result2.Added)
	}
}
