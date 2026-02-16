package usage

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	log "github.com/sirupsen/logrus"
)

// SQLitePlugin persists usage records to SQLite as an append-only event stream.
type SQLitePlugin struct{}

func NewSQLitePlugin() *SQLitePlugin { return &SQLitePlugin{} }

func (p *SQLitePlugin) HandleUsage(ctx context.Context, record coreusage.Record) {
	if p == nil {
		return
	}
	if !StatisticsEnabled() {
		return
	}
	store := GetSQLiteStore()
	if store == nil {
		return
	}

	timestamp := record.RequestedAt
	if timestamp.IsZero() {
		timestamp = time.Now().UTC()
	}
	detail := normaliseDetail(record.Detail)
	apiName := strings.TrimSpace(record.APIKey)
	if apiName == "" {
		apiName = resolveAPIIdentifier(ctx, record)
	}
	if apiName == "" {
		apiName = "unknown"
	}
	modelName := strings.TrimSpace(record.Model)
	if modelName == "" {
		modelName = "unknown"
	}
	failed := record.Failed
	if !failed {
		failed = !resolveSuccess(ctx)
	}

	_, err := store.Insert(PersistedUsageEvent{
		APIName: apiName,
		Model:   modelName,
		Detail: RequestDetail{
			Timestamp: timestamp,
			Source:    record.Source,
			AuthIndex: record.AuthIndex,
			Tokens:    detail,
			Failed:    failed,
		},
	})
	if err != nil {
		log.Warnf("usage sqlite plugin: failed to persist usage event: %v", err)
	}
}

var sqlitePersistenceState struct {
	mu               sync.RWMutex
	store            *SQLiteStore
	pluginRegistered bool
}

func SetSQLiteStore(store *SQLiteStore) {
	sqlitePersistenceState.mu.Lock()
	sqlitePersistenceState.store = store
	sqlitePersistenceState.mu.Unlock()
}

func GetSQLiteStore() *SQLiteStore {
	sqlitePersistenceState.mu.RLock()
	defer sqlitePersistenceState.mu.RUnlock()
	return sqlitePersistenceState.store
}

func EnableSQLitePersistence(path string) (MergeResult, error) {
	result := MergeResult{}

	store, err := OpenSQLiteStore(path)
	if err != nil {
		return result, err
	}
	snapshot, err := store.LoadSnapshot()
	if err != nil {
		_ = store.Close()
		return result, fmt.Errorf("load sqlite usage snapshot: %w", err)
	}
	result = GetRequestStatistics().MergeSnapshot(snapshot)

	sqlitePersistenceState.mu.Lock()
	prev := sqlitePersistenceState.store
	sqlitePersistenceState.store = store
	if !sqlitePersistenceState.pluginRegistered {
		coreusage.RegisterPlugin(NewSQLitePlugin())
		sqlitePersistenceState.pluginRegistered = true
	}
	sqlitePersistenceState.mu.Unlock()

	if prev != nil && prev != store {
		_ = prev.Close()
	}
	return result, nil
}

func CloseSQLitePersistence() error {
	sqlitePersistenceState.mu.Lock()
	store := sqlitePersistenceState.store
	sqlitePersistenceState.store = nil
	sqlitePersistenceState.mu.Unlock()
	if store != nil {
		return store.Close()
	}
	return nil
}
