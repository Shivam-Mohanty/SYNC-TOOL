package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"
)

// Turn represents a single message in an LLM conversation.
type Turn struct {
	Role      string `json:"role"`
	Content   string `json:"content"`
	Timestamp int64  `json:"timestamp,omitempty"`
}

// Usage captures token counts for the conversation.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
}

// StreamEvent represents the complete event payload published to Redis Streams.
type StreamEvent struct {
	IdempotencyKey string `json:"idempotency_key"`
	WorkspaceID    string `json:"workspace_id"`
	BranchID       string `json:"branch_id"`
	TranscriptID   string `json:"transcript_id"`
	ActorID        string `json:"actor_id"`
	Provider       string `json:"provider"`
	Model          string `json:"model"`
	Turns          []Turn `json:"turns"`
	Usage          Usage  `json:"usage"`
	CompletedAtMs  int64  `json:"completed_at_ms"`
}

// QueuePublisher provides methods to enqueue stream events to Redis.
type QueuePublisher interface {
	PublishEvent(ctx context.Context, event *StreamEvent) error
	EnqueueWithRetry(ctx context.Context, event *StreamEvent, maxRetries int) error
}

// RedisPublisher implements QueuePublisher using Redis Streams.
type RedisPublisher struct {
	client     *redis.Client
	streamName string
}

// NewRedisPublisher initializes a Redis stream publisher.
func NewRedisPublisher(client *redis.Client, streamName string) *RedisPublisher {
	if streamName == "" {
		streamName = "queue:transcripts:completed"
	}
	return &RedisPublisher{
		client:     client,
		streamName: streamName,
	}
}

// PublishEvent pushes a serialized StreamEvent into the Redis Stream via XADD.
func (p *RedisPublisher) PublishEvent(ctx context.Context, event *StreamEvent) error {
	dataBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal stream event: %w", err)
	}

	return p.client.XAdd(ctx, &redis.XAddArgs{
		Stream: p.streamName,
		Values: map[string]interface{}{
			"data": string(dataBytes),
		},
	}).Err()
}

// EnqueueWithRetry attempts to publish with exponential backoff and jitter (FIX #4).
func (p *RedisPublisher) EnqueueWithRetry(ctx context.Context, event *StreamEvent, maxRetries int) error {
	if maxRetries <= 0 {
		maxRetries = 3
	}

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		err := p.PublishEvent(ctx, event)
		if err == nil {
			return nil
		}
		lastErr = err

		// Exponential backoff: base 50ms * 2^attempt + jitter
		backoffMs := 50 * math.Pow(2, float64(attempt))
		jitter := rand.Float64() * 25
		sleepDuration := time.Duration(backoffMs+jitter) * time.Millisecond

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleepDuration):
		}
	}

	return fmt.Errorf("exhausted %d retries: %w", maxRetries, lastErr)
}
