package usage

import (
	"path/filepath"
	"testing"
	"time"
)

func TestSQLiteStore_InsertAndDedup(t *testing.T) {
	store, cleanup := openTempSQLiteStore(t)
	defer cleanup()

	now := time.Now().UTC().Truncate(time.Nanosecond)
	event := PersistedUsageEvent{
		APIName: "api-1",
		Model:   "model-1",
		Detail: RequestDetail{
			Timestamp: now,
			Source:    "src",
			AuthIndex: "a1",
			Tokens: TokenStats{
				InputTokens:  1,
				OutputTokens: 2,
				TotalTokens:  3,
			},
			Failed: false,
		},
	}

	inserted, err := store.Insert(event)
	if err != nil {
		t.Fatalf("insert first event: %v", err)
	}
	if !inserted {
		t.Fatalf("expected first insert to be applied")
	}

	inserted, err = store.Insert(event)
	if err != nil {
		t.Fatalf("insert duplicate event: %v", err)
	}
	if inserted {
		t.Fatalf("expected duplicate insert to be ignored")
	}

	snapshot, err := store.LoadSnapshot()
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if snapshot.TotalRequests != 1 {
		t.Fatalf("expected total_requests=1, got %d", snapshot.TotalRequests)
	}
}

func TestSQLiteStore_InsertEventsTransaction(t *testing.T) {
	store, cleanup := openTempSQLiteStore(t)
	defer cleanup()

	now := time.Now().UTC().Truncate(time.Nanosecond)
	events := []PersistedUsageEvent{
		{
			APIName: "api-a",
			Model:   "m1",
			Detail:  RequestDetail{Timestamp: now, Tokens: TokenStats{InputTokens: 1, OutputTokens: 1, TotalTokens: 2}},
		},
		{
			APIName: "api-b",
			Model:   "m2",
			Detail:  RequestDetail{Timestamp: now.Add(time.Second), Tokens: TokenStats{InputTokens: 2, OutputTokens: 2, TotalTokens: 4}, Failed: true},
		},
	}

	inserted, err := store.InsertEvents(events)
	if err != nil {
		t.Fatalf("insert events: %v", err)
	}
	if inserted != 2 {
		t.Fatalf("expected 2 inserted events, got %d", inserted)
	}

	snapshot, err := store.LoadSnapshot()
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if snapshot.TotalRequests != 2 {
		t.Fatalf("expected total_requests=2, got %d", snapshot.TotalRequests)
	}
	if snapshot.FailureCount != 1 {
		t.Fatalf("expected failure_count=1, got %d", snapshot.FailureCount)
	}
}

func TestSQLiteStore_InsertEventsRollbackOnFailure(t *testing.T) {
	store, cleanup := openTempSQLiteStore(t)
	defer cleanup()

	now := time.Now().UTC().Truncate(time.Nanosecond)
	events := []PersistedUsageEvent{
		{
			APIName: "api-ok",
			Model:   "m1",
			Detail:  RequestDetail{Timestamp: now, Tokens: TokenStats{TotalTokens: 1}},
		},
		{
			APIName: "",
			Model:   "m2",
			Detail:  RequestDetail{Timestamp: now.Add(time.Second), Tokens: TokenStats{TotalTokens: 2}},
		},
	}

	if _, err := store.InsertEvents(events); err == nil {
		t.Fatalf("expected transaction failure for invalid api_name")
	}

	snapshot, err := store.LoadSnapshot()
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if snapshot.TotalRequests != 0 {
		t.Fatalf("expected rollback to keep table empty, got total_requests=%d", snapshot.TotalRequests)
	}
}

func TestSQLiteStore_ReopenAndMigrate(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "usage.db")

	store, err := OpenSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("open sqlite store first time: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close sqlite store first time: %v", err)
	}

	store, err = OpenSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("reopen sqlite store: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close sqlite store second time: %v", err)
	}
}

func openTempSQLiteStore(t *testing.T) (*SQLiteStore, func()) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "usage.db")
	store, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	cleanup := func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite store: %v", err)
		}
	}
	return store, cleanup
}
