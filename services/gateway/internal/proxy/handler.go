package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sync-tool/gateway/internal/auth"
	"github.com/sync-tool/gateway/internal/crypto"
	"github.com/sync-tool/gateway/internal/middleware"
	"github.com/sync-tool/gateway/internal/queue"
)

// ProxyHandler coordinates SSE streaming, BYOK key resolution, and queuing.
type ProxyHandler struct {
	router      *Router
	keyResolver auth.KeyResolver
	publisher   queue.QueuePublisher
	wal         WAL
	httpClient  *http.Client
	logger      *slog.Logger
}

// HandlerConfig holds configuration parameters for the proxy handler.
type HandlerConfig struct {
	Router      *Router
	KeyResolver auth.KeyResolver
	Publisher   queue.QueuePublisher
	WAL         WAL
	HTTPClient  *http.Client
	Logger      *slog.Logger
}

// NewProxyHandler creates an instance of ProxyHandler.
func NewProxyHandler(cfg HandlerConfig) *ProxyHandler {
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{
			Timeout: 0, // No client-level timeout for long-lived SSE streams
		}
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &ProxyHandler{
		router:      cfg.Router,
		keyResolver: cfg.KeyResolver,
		publisher:   cfg.Publisher,
		wal:         cfg.WAL,
		httpClient:  client,
		logger:      logger,
	}
}

// ServeHTTP handles the incoming proxy request, teeing chunks to the client.
func (h *ProxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	workspaceID := r.Header.Get("X-Workspace-Id")
	providerID := strings.ToLower(r.Header.Get("X-Provider"))
	branchID := r.Header.Get("X-Branch-Id")
	actorID := r.Header.Get("X-Actor-Id")
	transcriptID := r.Header.Get("X-Transcript-Id")

	if workspaceID == "" || providerID == "" {
		http.Error(w, "missing required headers: X-Workspace-Id and X-Provider", http.StatusBadRequest)
		return
	}
	if branchID == "" {
		branchID = "main"
	}
	if actorID == "" {
		actorID = "anonymous"
	}
	if transcriptID == "" {
		transcriptID = uuid.NewString()
	}

	prov, ok := h.router.Get(providerID)
	if !ok {
		http.Error(w, fmt.Sprintf("unsupported provider: %s", providerID), http.StatusBadRequest)
		return
	}

	// Read and buffer the client request body to extract turns and model
	reqBodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}
	r.Body.Close()

	// Parse model and prompt turns from request payload
	modelName, promptTurns := parseRequestPayload(providerID, reqBodyBytes)

	// BYOK Key Resolution
	apiKey, err := h.keyResolver.ResolveWorkspaceKey(ctx, workspaceID, providerID)
	if err != nil {
		h.logger.Error("key resolution failed", "workspace", workspaceID, "provider", providerID, "err", err)
		http.Error(w, "key resolution failed", http.StatusUnauthorized)
		return
	}
	// Zero plaintext key when the function frame returns (Security Rule #2)
	defer crypto.WipeBytes(apiKey)

	upstreamURL := prov.BaseURL + r.URL.Path
	upstreamReq, err := http.NewRequestWithContext(ctx, r.Method, upstreamURL, bytes.NewReader(reqBodyBytes))
	if err != nil {
		http.Error(w, "failed to build upstream request", http.StatusInternalServerError)
		return
	}

	upstreamReq.Header = middleware.SanitizeUpstreamHeaders(r.Header)
	applyProviderAuth(upstreamReq, providerID, apiKey)

	resp, err := h.httpClient.Do(upstreamReq)
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			h.logInterrupted(workspaceID, branchID, "client canceled during upstream dial")
			return
		}
		h.logger.Error("upstream connection error", "provider", providerID, "err", err)
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// Forward upstream headers to downstream client
	for k, v := range resp.Header {
		w.Header()[k] = v
	}
	w.WriteHeader(resp.StatusCode)

	flusher, hasFlusher := w.(http.Flusher)

	var streamBuffer bytes.Buffer
	scratch := make([]byte, 4096)
	isClean := false

outerLoop: // FIX #2: labeled break exits the for-loop, not just the select block
	for {
		select {
		case <-ctx.Done():
			// Client disconnected mid-stream; discard buffer and avoid enqueue
			h.logInterrupted(workspaceID, branchID, "client context canceled mid-stream")
			return
		default:
			n, readErr := resp.Body.Read(scratch)
			if n > 0 {
				chunk := scratch[:n]
				streamBuffer.Write(chunk)
				if _, writeErr := w.Write(chunk); writeErr != nil {
					h.logInterrupted(workspaceID, branchID, "write to client failed: "+writeErr.Error())
					return
				}
				if hasFlusher {
					flusher.Flush()
				}
				if prov.FinishDetector(chunk) {
					isClean = true
				}
			}
			if readErr != nil {
				// EOF or read error exits stream loop
				break outerLoop
			}
		}
	}

	// If the stream did not terminate with a confirmed finish token, discard (FIX #2, §8.1)
	if !isClean {
		h.logger.Warn("stream terminated without clean finish token; skipping queue", "workspace", workspaceID, "branch", branchID)
		return
	}

	// Completed cleanly: generate deterministic idempotency key (FIX #10)
	nowMs := time.Now().UnixMilli()
	idemKey := fmt.Sprintf("%s:%s:%d", workspaceID, branchID, nowMs)

	// Extract assistant completion turn from streamBuffer
	assistantTurn := extractCompletionTurn(providerID, streamBuffer.Bytes())
	var allTurns []queue.Turn
	allTurns = append(allTurns, promptTurns...)
	if assistantTurn.Content != "" {
		allTurns = append(allTurns, assistantTurn)
	}

	event := &queue.StreamEvent{
		IdempotencyKey: idemKey,
		WorkspaceID:    workspaceID,
		BranchID:       branchID,
		TranscriptID:   transcriptID,
		ActorID:        actorID,
		Provider:       providerID,
		Model:          modelName,
		Turns:          allTurns,
		Usage: queue.Usage{
			PromptTokens:     estimateTokens(promptTurns),
			CompletionTokens: estimateTokens([]queue.Turn{assistantTurn}),
		},
		CompletedAtMs: nowMs,
	}

	// Enqueue with retry (FIX #4)
	if h.publisher != nil {
		if err := h.publisher.EnqueueWithRetry(context.Background(), event, 3); err != nil {
			h.logger.Error("enqueue failed after retries; persisting to WAL", "workspace", workspaceID, "idem_key", idemKey, "err", err)
			if h.wal != nil {
				dataBytes, _ := json.Marshal(event)
				if walErr := h.wal.Write(idemKey, dataBytes); walErr != nil {
					h.logger.Error("CRITICAL: failed to persist to local WAL", "idem_key", idemKey, "err", walErr)
				}
			}
		}
	}
}

func (h *ProxyHandler) logInterrupted(workspaceID, branchID, reason string) {
	h.logger.Warn("stream interrupted; buffer discarded", "workspace", workspaceID, "branch", branchID, "reason", reason)
}

func applyProviderAuth(req *http.Request, providerID string, apiKey []byte) {
	keyStr := string(apiKey)
	switch providerID {
	case "anthropic":
		req.Header.Set("x-api-key", keyStr)
		if req.Header.Get("anthropic-version") == "" {
			req.Header.Set("anthropic-version", "2023-06-01")
		}
	case "gemini":
		req.Header.Set("x-goog-api-key", keyStr)
	default: // openai and compatible
		req.Header.Set("Authorization", "Bearer "+keyStr)
	}
}

func parseRequestPayload(provider string, body []byte) (model string, turns []queue.Turn) {
	if len(body) == 0 {
		return "unknown", nil
	}

	var parsed struct {
		Model    string `json:"model"`
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}

	if err := json.Unmarshal(body, &parsed); err == nil {
		model = parsed.Model
		for _, m := range parsed.Messages {
			// Note: system messages are stripped before enqueue (§3.3)
			if strings.ToLower(m.Role) != "system" && m.Content != "" {
				turns = append(turns, queue.Turn{
					Role:      m.Role,
					Content:   m.Content,
					Timestamp: time.Now().UnixMilli(),
				})
			}
		}
	}
	if model == "" {
		model = "default-" + provider
	}
	return model, turns
}

func extractCompletionTurn(provider string, buf []byte) queue.Turn {
	// Parse SSE stream chunks to gather assistant text
	lines := strings.Split(string(buf), "\n")
	var sb strings.Builder

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "[DONE]" || data == "" {
			continue
		}

		// Try OpenAI format: delta.content
		var openAIChunk struct {
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if err := json.Unmarshal([]byte(data), &openAIChunk); err == nil && len(openAIChunk.Choices) > 0 {
			sb.WriteString(openAIChunk.Choices[0].Delta.Content)
			continue
		}

		// Try Anthropic format: content_block_delta
		var anthropicChunk struct {
			Type  string `json:"type"`
			Delta struct {
				Text string `json:"text"`
			} `json:"delta"`
		}
		if err := json.Unmarshal([]byte(data), &anthropicChunk); err == nil && anthropicChunk.Delta.Text != "" {
			sb.WriteString(anthropicChunk.Delta.Text)
			continue
		}

		// Try Gemini format: candidates[0].content.parts[0].text
		var geminiChunk struct {
			Candidates []struct {
				Content struct {
					Parts []struct {
						Text string `json:"text"`
					} `json:"parts"`
				} `json:"content"`
			} `json:"candidates"`
		}
		if err := json.Unmarshal([]byte(data), &geminiChunk); err == nil && len(geminiChunk.Candidates) > 0 {
			if len(geminiChunk.Candidates[0].Content.Parts) > 0 {
				sb.WriteString(geminiChunk.Candidates[0].Content.Parts[0].Text)
			}
		}
	}

	return queue.Turn{
		Role:      "assistant",
		Content:   sb.String(),
		Timestamp: time.Now().UnixMilli(),
	}
}

func estimateTokens(turns []queue.Turn) int {
	totalChars := 0
	for _, t := range turns {
		totalChars += len(t.Content)
	}
	return totalChars / 4
}
