package proxy_test

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/sync-tool/gateway/internal/auth"
	"github.com/sync-tool/gateway/internal/proxy"
)

type concurrencyTokenRecorder struct {
	*httptest.ResponseRecorder
	mu             sync.Mutex
	firstTokenTime time.Time
}

func newConcurrencyTokenRecorder() *concurrencyTokenRecorder {
	return &concurrencyTokenRecorder{
		ResponseRecorder: httptest.NewRecorder(),
	}
}

func (r *concurrencyTokenRecorder) Write(b []byte) (int, error) {
	r.mu.Lock()
	if r.firstTokenTime.IsZero() && len(b) > 0 {
		r.firstTokenTime = time.Now()
	}
	r.mu.Unlock()
	return r.ResponseRecorder.Write(b)
}

func (r *concurrencyTokenRecorder) Flush() {
	r.ResponseRecorder.Flush()
}

func (r *concurrencyTokenRecorder) FirstTokenTime() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.firstTokenTime
}

func TestProxyConcurrency50StreamsTTFTUnder10ms(t *testing.T) {
	const concurrentStreams = 50

	var upstreamMu sync.Mutex
	upstreamSendTimes := make(map[string]time.Time)

	// Upstream SSE server simulating LLM streaming
	upstreamSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		streamID := r.Header.Get("X-Stream-Id")

		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, hasFlusher := w.(http.Flusher)

		// Record exact time upstream sends first token
		now := time.Now()
		upstreamMu.Lock()
		upstreamSendTimes[streamID] = now
		upstreamMu.Unlock()

		w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"First token\"}}]}\n\n"))
		if hasFlusher {
			flusher.Flush()
		}

		time.Sleep(1 * time.Millisecond)

		w.Write([]byte("data: [DONE]\n\n"))
		if hasFlusher {
			flusher.Flush()
		}
	}))
	defer upstreamSrv.Close()

	// High-throughput HTTP Transport with connection pooling (plan §8.10)
	pooledTransport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		MaxConnsPerHost:     100,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
	}
	httpClient := &http.Client{Transport: pooledTransport}

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
		HTTPClient:  httpClient,
	})

	// Warm connection pool
	warmReq, _ := http.NewRequest(http.MethodGet, upstreamSrv.URL, nil)
	if resp, err := httpClient.Do(warmReq); err == nil {
		resp.Body.Close()
	}

	var wg sync.WaitGroup
	wg.Add(concurrentStreams)

	overheads := make([]time.Duration, concurrentStreams)
	clientRecvTimes := make([]time.Time, concurrentStreams)

	// Barrier to trigger all 50 goroutines concurrently
	startBarrier := make(chan struct{})

	for i := 0; i < concurrentStreams; i++ {
		go func(idx int) {
			defer wg.Done()
			streamID := fmt.Sprintf("stream-%d", idx)
			reqBody := fmt.Sprintf(`{"model":"gpt-4o","messages":[{"role":"user","content":"Stream %d"}]}`, idx)
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewBufferString(reqBody))
			req.Header.Set("X-Workspace-Id", "ws-1")
			req.Header.Set("X-Provider", "test-openai")
			req.Header.Set("X-Branch-Id", "main")
			req.Header.Set("X-Stream-Id", streamID)

			rec := newConcurrencyTokenRecorder()

			<-startBarrier
			handler.ServeHTTP(rec, req)

			firstToken := rec.FirstTokenTime()
			if firstToken.IsZero() {
				t.Errorf("stream %d did not receive any token", idx)
				return
			}
			clientRecvTimes[idx] = firstToken
		}(i)
	}

	// Release barrier: launch all 50 concurrent SSE streams
	close(startBarrier)
	wg.Wait()

	// Verify all 50 streams completed and were enqueued
	publisher.mu.Lock()
	enqueuedCount := len(publisher.enqueued)
	publisher.mu.Unlock()

	if enqueuedCount != concurrentStreams {
		t.Fatalf("expected %d events enqueued, got %d", concurrentStreams, enqueuedCount)
	}

	// Calculate proxy transit overhead: Client Received Time - Upstream Dispatched Time
	for i := 0; i < concurrentStreams; i++ {
		streamID := fmt.Sprintf("stream-%d", i)
		upstreamMu.Lock()
		upstreamTime := upstreamSendTimes[streamID]
		upstreamMu.Unlock()

		clientTime := clientRecvTimes[i]
		overhead := clientTime.Sub(upstreamTime)
		if overhead < 0 {
			overhead = 0
		}
		overheads[i] = overhead
	}

	sort.Slice(overheads, func(i, j int) bool {
		return overheads[i] < overheads[j]
	})

	p50 := overheads[concurrentStreams*50/100]
	p90 := overheads[concurrentStreams*90/100]
	p99 := overheads[concurrentStreams*99/100]

	fmt.Println("\n================================================================================")
	fmt.Printf("CONCURRENCY BENCHMARK: %d Concurrent SSE Streams\n", concurrentStreams)
	fmt.Println("================================================================================")
	fmt.Printf("Proxy Transit TTFT Overhead p50: %v\n", p50)
	fmt.Printf("Proxy Transit TTFT Overhead p90: %v\n", p90)
	fmt.Printf("Proxy Transit TTFT Overhead p99: %v\n", p99)
	fmt.Println("================================================================================")

	// Target: p99 TTFT overhead < 10ms
	if p99 > 10*time.Millisecond {
		t.Errorf("p99 TTFT overhead exceeded target (target < 10ms, got %v)", p99)
	}
}
