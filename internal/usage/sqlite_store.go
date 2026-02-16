package usage

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const usageSQLiteSchemaVersion = 1

const usageSQLiteInsertSQL = `
INSERT OR IGNORE INTO usage_events (
	fingerprint,
	requested_at_ns,
	api_name,
	model,
	source,
	auth_index,
	input_tokens,
	output_tokens,
	reasoning_tokens,
	cached_tokens,
	total_tokens,
	failed
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`

type PersistedUsageEvent struct {
	APIName string
	Model   string
	Detail  RequestDetail
}

type SQLiteStore struct {
	db         *sql.DB
	insertStmt *sql.Stmt
}

func OpenSQLiteStore(path string) (*SQLiteStore, error) {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return nil, errors.New("usage sqlite path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(trimmed), 0o755); err != nil {
		return nil, fmt.Errorf("create sqlite directory: %w", err)
	}

	db, err := sql.Open("sqlite", trimmed)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)

	store := &SQLiteStore{db: db}
	if err := store.init(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *SQLiteStore) init() error {
	if s == nil || s.db == nil {
		return errors.New("sqlite store is nil")
	}
	if _, err := s.db.Exec(`PRAGMA journal_mode=WAL;`); err != nil {
		return fmt.Errorf("enable wal mode: %w", err)
	}
	if _, err := s.db.Exec(`PRAGMA busy_timeout=5000;`); err != nil {
		return fmt.Errorf("set busy timeout: %w", err)
	}
	if _, err := s.db.Exec(`PRAGMA synchronous=NORMAL;`); err != nil {
		return fmt.Errorf("set synchronous mode: %w", err)
	}
	if err := s.migrate(); err != nil {
		return err
	}

	stmt, err := s.db.Prepare(usageSQLiteInsertSQL)
	if err != nil {
		return fmt.Errorf("prepare usage insert statement: %w", err)
	}
	s.insertStmt = stmt
	return nil
}

func (s *SQLiteStore) migrate() error {
	var version int
	if err := s.db.QueryRow(`PRAGMA user_version;`).Scan(&version); err != nil {
		return fmt.Errorf("read user_version: %w", err)
	}
	if version > usageSQLiteSchemaVersion {
		return fmt.Errorf("unsupported sqlite schema version: %d", version)
	}
	if version == usageSQLiteSchemaVersion {
		return nil
	}

	if _, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS usage_events (
	fingerprint TEXT PRIMARY KEY,
	requested_at_ns INTEGER NOT NULL,
	api_name TEXT NOT NULL CHECK (api_name <> ''),
	model TEXT NOT NULL CHECK (model <> ''),
	source TEXT NOT NULL,
	auth_index TEXT NOT NULL,
	input_tokens INTEGER NOT NULL,
	output_tokens INTEGER NOT NULL,
	reasoning_tokens INTEGER NOT NULL,
	cached_tokens INTEGER NOT NULL,
	total_tokens INTEGER NOT NULL,
	failed INTEGER NOT NULL CHECK (failed IN (0, 1))
);
CREATE INDEX IF NOT EXISTS idx_usage_events_requested_at_ns ON usage_events(requested_at_ns);
CREATE INDEX IF NOT EXISTS idx_usage_events_api_model_time ON usage_events(api_name, model, requested_at_ns);
CREATE INDEX IF NOT EXISTS idx_usage_events_failed_time ON usage_events(failed, requested_at_ns);
`); err != nil {
		return fmt.Errorf("create usage schema: %w", err)
	}

	if _, err := s.db.Exec(fmt.Sprintf(`PRAGMA user_version=%d;`, usageSQLiteSchemaVersion)); err != nil {
		return fmt.Errorf("set user_version: %w", err)
	}
	return nil
}

func (s *SQLiteStore) Close() error {
	if s == nil {
		return nil
	}
	if s.insertStmt != nil {
		_ = s.insertStmt.Close()
	}
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *SQLiteStore) Insert(event PersistedUsageEvent) (bool, error) {
	if s == nil || s.insertStmt == nil {
		return false, errors.New("sqlite store unavailable")
	}
	normalised, err := normalisePersistedUsageEvent(event)
	if err != nil {
		return false, err
	}
	result, err := s.insertStmt.Exec(persistedUsageEventArgs(normalised)...)
	if err != nil {
		return false, fmt.Errorf("insert usage event: %w", err)
	}
	affected, _ := result.RowsAffected()
	return affected > 0, nil
}

func (s *SQLiteStore) InsertEvents(events []PersistedUsageEvent) (int64, error) {
	if s == nil || s.db == nil {
		return 0, errors.New("sqlite store unavailable")
	}
	if len(events) == 0 {
		return 0, nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return 0, fmt.Errorf("begin usage transaction: %w", err)
	}
	stmt, err := tx.Prepare(usageSQLiteInsertSQL)
	if err != nil {
		_ = tx.Rollback()
		return 0, fmt.Errorf("prepare usage transaction statement: %w", err)
	}

	var inserted int64
	for _, event := range events {
		normalised, normaliseErr := normalisePersistedUsageEvent(event)
		if normaliseErr != nil {
			_ = stmt.Close()
			_ = tx.Rollback()
			return 0, normaliseErr
		}
		result, execErr := stmt.Exec(persistedUsageEventArgs(normalised)...)
		if execErr != nil {
			_ = stmt.Close()
			_ = tx.Rollback()
			return 0, fmt.Errorf("insert usage event in transaction: %w", execErr)
		}
		affected, _ := result.RowsAffected()
		inserted += affected
	}
	if err := stmt.Close(); err != nil {
		_ = tx.Rollback()
		return 0, fmt.Errorf("close usage transaction statement: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit usage transaction: %w", err)
	}
	return inserted, nil
}

func (s *SQLiteStore) InsertSnapshot(snapshot StatisticsSnapshot) (int64, error) {
	events := make([]PersistedUsageEvent, 0)
	for apiName, apiSnapshot := range snapshot.APIs {
		apiName = strings.TrimSpace(apiName)
		if apiName == "" {
			continue
		}
		for modelName, modelSnapshot := range apiSnapshot.Models {
			modelName = strings.TrimSpace(modelName)
			if modelName == "" {
				modelName = "unknown"
			}
			for _, detail := range modelSnapshot.Details {
				events = append(events, PersistedUsageEvent{
					APIName: apiName,
					Model:   modelName,
					Detail:  detail,
				})
			}
		}
	}
	return s.InsertEvents(events)
}

func (s *SQLiteStore) LoadSnapshot() (StatisticsSnapshot, error) {
	snapshot := StatisticsSnapshot{
		APIs:           make(map[string]APISnapshot),
		RequestsByDay:  make(map[string]int64),
		RequestsByHour: make(map[string]int64),
		TokensByDay:    make(map[string]int64),
		TokensByHour:   make(map[string]int64),
	}
	if s == nil || s.db == nil {
		return snapshot, nil
	}

	rows, err := s.db.Query(`
SELECT
	requested_at_ns,
	api_name,
	model,
	source,
	auth_index,
	input_tokens,
	output_tokens,
	reasoning_tokens,
	cached_tokens,
	total_tokens,
	failed
FROM usage_events
ORDER BY requested_at_ns ASC;
`)
	if err != nil {
		return snapshot, fmt.Errorf("query usage events: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			requestedAtNS   int64
			apiName         string
			modelName       string
			source          string
			authIndex       string
			inputTokens     int64
			outputTokens    int64
			reasoningTokens int64
			cachedTokens    int64
			totalTokens     int64
			failedValue     int
		)
		if err := rows.Scan(
			&requestedAtNS,
			&apiName,
			&modelName,
			&source,
			&authIndex,
			&inputTokens,
			&outputTokens,
			&reasoningTokens,
			&cachedTokens,
			&totalTokens,
			&failedValue,
		); err != nil {
			return snapshot, fmt.Errorf("scan usage event: %w", err)
		}

		detail := RequestDetail{
			Timestamp: time.Unix(0, requestedAtNS).UTC(),
			Source:    source,
			AuthIndex: authIndex,
			Failed:    failedValue == 1,
			Tokens: normaliseTokenStats(TokenStats{
				InputTokens:     inputTokens,
				OutputTokens:    outputTokens,
				ReasoningTokens: reasoningTokens,
				CachedTokens:    cachedTokens,
				TotalTokens:     totalTokens,
			}),
		}

		apiSnapshot := snapshot.APIs[apiName]
		if apiSnapshot.Models == nil {
			apiSnapshot.Models = make(map[string]ModelSnapshot)
		}
		modelSnapshot := apiSnapshot.Models[modelName]
		modelSnapshot.TotalRequests++
		modelSnapshot.TotalTokens += detail.Tokens.TotalTokens
		modelSnapshot.Details = append(modelSnapshot.Details, detail)
		apiSnapshot.Models[modelName] = modelSnapshot
		apiSnapshot.TotalRequests++
		apiSnapshot.TotalTokens += detail.Tokens.TotalTokens
		snapshot.APIs[apiName] = apiSnapshot

		snapshot.TotalRequests++
		snapshot.TotalTokens += detail.Tokens.TotalTokens
		if detail.Failed {
			snapshot.FailureCount++
		} else {
			snapshot.SuccessCount++
		}
		dayKey := detail.Timestamp.Format("2006-01-02")
		hourKey := formatHour(detail.Timestamp.Hour())
		snapshot.RequestsByDay[dayKey]++
		snapshot.RequestsByHour[hourKey]++
		snapshot.TokensByDay[dayKey] += detail.Tokens.TotalTokens
		snapshot.TokensByHour[hourKey] += detail.Tokens.TotalTokens
	}
	if err := rows.Err(); err != nil {
		return snapshot, fmt.Errorf("iterate usage events: %w", err)
	}
	return snapshot, nil
}

func normalisePersistedUsageEvent(event PersistedUsageEvent) (PersistedUsageEvent, error) {
	event.APIName = strings.TrimSpace(event.APIName)
	if event.APIName == "" {
		return PersistedUsageEvent{}, errors.New("usage event api_name is empty")
	}
	event.Model = strings.TrimSpace(event.Model)
	if event.Model == "" {
		event.Model = "unknown"
	}
	event.Detail.Tokens = normaliseTokenStats(event.Detail.Tokens)
	if event.Detail.Timestamp.IsZero() {
		event.Detail.Timestamp = time.Now().UTC()
	}
	event.Detail.Timestamp = event.Detail.Timestamp.UTC()
	return event, nil
}

func persistedUsageEventArgs(event PersistedUsageEvent) []any {
	tokens := event.Detail.Tokens
	fingerprint := dedupKey(event.APIName, event.Model, RequestDetail{
		Timestamp: event.Detail.Timestamp,
		Source:    event.Detail.Source,
		AuthIndex: event.Detail.AuthIndex,
		Failed:    event.Detail.Failed,
		Tokens:    tokens,
	})
	failed := 0
	if event.Detail.Failed {
		failed = 1
	}
	return []any{
		fingerprint,
		event.Detail.Timestamp.UnixNano(),
		event.APIName,
		event.Model,
		event.Detail.Source,
		event.Detail.AuthIndex,
		tokens.InputTokens,
		tokens.OutputTokens,
		tokens.ReasoningTokens,
		tokens.CachedTokens,
		tokens.TotalTokens,
		failed,
	}
}
