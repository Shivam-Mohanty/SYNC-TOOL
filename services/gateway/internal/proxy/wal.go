package proxy

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// WALRecord represents an event stored locally when queue dispatch fails.
type WALRecord struct {
	Timestamp      time.Time `json:"timestamp"`
	IdempotencyKey string    `json:"idempotency_key"`
	Payload        string    `json:"payload"`
}

// WAL provides durable disk persistence for failed queue deliveries.
type WAL interface {
	Write(idempotencyKey string, payload []byte) error
	Close() error
}

// LocalWAL implements WAL by appending records to an on-disk log file.
type LocalWAL struct {
	filePath string
	mu       sync.Mutex
	file     *os.File
}

// NewLocalWAL creates or opens a local WAL log file.
func NewLocalWAL(filePath string) (*LocalWAL, error) {
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	return &LocalWAL{
		filePath: filePath,
		file:     f,
	}, nil
}

// Write appends an entry to the WAL file and fsyncs to disk.
func (w *LocalWAL) Write(idempotencyKey string, payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	rec := WALRecord{
		Timestamp:      time.Now().UTC(),
		IdempotencyKey: idempotencyKey,
		Payload:        string(payload),
	}

	line, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("failed to marshal WAL record: %w", err)
	}

	if _, err := w.file.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	return w.file.Sync()
}

// Close flushes and closes the WAL file handle.
func (w *LocalWAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}
