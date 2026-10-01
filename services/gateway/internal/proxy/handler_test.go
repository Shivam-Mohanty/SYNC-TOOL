package proxy_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/sync-tool/gateway/internal/auth"
	"github.com/sync-tool/gateway/internal/proxy"
	"github.com/sync-tool/gateway/internal/queue"
)

type MockPublisher struct {
	mu        sync.Mutex
	enqueued  []*queue.StreamEvent
	failTimes int
}

func (m *MockPublisher) PublishEvent(ctx context.Context, event *queue.StreamEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failTimes > 0 {
		m.failTimes--
		return fmt.Errorf("simulated redis failure")
	}
	m.enqueued = append(m.enqueued, event)
	return nil
}

func (m *MockPublisher) EnqueueWithRetry(ctx context.Context, event *queue.StreamEvent, maxRetries int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failTimes > 0 {
		m.failTimes--
		return fmt.Errorf("simulated redis retry failure")
	}
	m.enqueued = append(m.enqueued, event)
	return nil
}

type MockWAL struct {
	mu      sync.Mutex
	records map[string][]byte
}

func (m *MockWAL) Write(key string, payload []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.records == nil {
		m.records = make(map[string][]byte)
	}
	m.records[key] = payload
	return nil
}

func (m *MockWAL) Close() error { return nil }

func setupTestProxy(t *testing.T, upstreamHandler http.HandlerFunc) (*httptest.Server, *proxy.ProxyHandler, *MockPublisher, *MockWAL) {
	upstreamSrv := httptest.NewServer(upstreamHandler)

	customProviders := map[string]proxy.ProviderConfig{
		"test-openai": {
			BaseURL: upstreamSrv.URL,
			FinishDetector: func(chunk []byte) bool {
				return bytes.Contains(chunk, []byte("data: [DONE]"))
			},
		},
	}

	router := proxy.NewRouter(customProviders)
	keyResolver := auth.NewStaticKeyResolver(map[string]string{
		"ws-1:test-openai": "sk-test-key-12345",
	})
	publisher := &MockPublisher{}
	wal := &MockWAL{}

	handler := proxy.NewProxyHandler(proxy.HandlerConfig{
		Router:      router,
		KeyResolver: keyResolver,
		Publisher:   publisher,
		WAL:         wal,
		HTTPClient:  upstreamSrv.Client(),
	})

	return upstreamSrv, handler, publisher, wal
}

func TestProxyCleanStreamEnqueues(t *testing.T) {
	upstreamSrv, handler, publisher, _ := setupTestProxy(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)

		w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"Hello \"}}]}\n\n"))
		flusher.Flush()
		time.Sleep(10 * time.Millisecond)

		w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"World\"}}]}\n\n"))
		flusher.Flush()
		time.Sleep(10 * time.Millisecond)

		w.Write([]byte("data: [DONE]\n\n"))
		flusher.Flush()
	})
	defer upstreamSrv.Close()

	reqBody := `{"model":"gpt-4o","messages":[{"role":"user","content":"Hi"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewBufferString(reqBody))
	req.Header.Set("X-Workspace-Id", "ws-1")
	req.Header.Set("X-Provider", "test-openai")
	req.Header.Set("X-Branch-Id", "main")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	if len(publisher.enqueued) != 1 {
		t.Fatalf("expected 1 event enqueued, got %d", len(publisher.enqueued))
	}

	event := publisher.enqueued[0]
	if event.WorkspaceID != "ws-1" || event.BranchID != "main" {
		t.Errorf("unexpected event workspace/branch: %+v", event)
	}
	if len(event.Turns) < 2 {
		t.Errorf("expected at least 2 turns (user + assistant), got %d", len(event.Turns))
	}
}

func TestProxyIncompleteStreamDoesNotEnqueue(t *testing.T) {
	// Upstream closes stream unexpectedly without sending "data: [DONE]"
	upstreamSrv, handler, publisher, _ := setupTestProxy(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"Half sentence...\"}}]}\n\n"))
		// Premature EOF without [DONE]
	})
	defer upstreamSrv.Close()

	reqBody := `{"model":"gpt-4o","messages":[{"role":"user","content":"Hi"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewBufferString(reqBody))
	req.Header.Set("X-Workspace-Id", "ws-1")
	req.Header.Set("X-Provider", "test-openai")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	if len(publisher.enqueued) != 0 {
		t.Fatalf("expected 0 events enqueued on unclean termination, got %d", len(publisher.enqueued))
	}
}

func TestProxyClientDisconnectDiscardsBuffer(t *testing.T) {
	upstreamSrv, handler, publisher, _ := setupTestProxy(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)

		w.Write([]byte("data: chunk 1\n\n"))
		flusher.Flush()

		// Wait to allow client to cancel
		time.Sleep(100 * time.Millisecond)

		w.Write([]byte("data: [DONE]\n\n"))
		flusher.Flush()
	})
	defer upstreamSrv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	reqBody := `{"model":"gpt-4o","messages":[{"role":"user","content":"Hi"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewBufferString(reqBody)).WithContext(ctx)
	req.Header.Set("X-Workspace-Id", "ws-1")
	req.Header.Set("X-Provider", "test-openai")

	// Cancel context immediately
	cancel()

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	if len(publisher.enqueued) != 0 {
		t.Fatalf("expected 0 events enqueued on client disconnect, got %d", len(publisher.enqueued))
	}
}

func TestProxyFallbackToWALOnQueueFailure(t *testing.T) {
	upstreamSrv, handler, publisher, wal := setupTestProxy(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"OK\"}}]}\n\n"))
		flusher.Flush()
		w.Write([]byte("data: [DONE]\n\n"))
		flusher.Flush()
	})
	defer upstreamSrv.Close()

	// Simulate redis failure
	publisher.failTimes = 10

	reqBody := `{"model":"gpt-4o","messages":[{"role":"user","content":"Hi"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewBufferString(reqBody))
	req.Header.Set("X-Workspace-Id", "ws-1")
	req.Header.Set("X-Provider", "test-openai")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	wal.mu.Lock()
	defer wal.mu.Unlock()
	if len(wal.records) != 1 {
		t.Fatalf("expected 1 record persisted to WAL on queue failure, got %d", len(wal.records))
	}
}
