package middleware_test

import (
	"bytes"
	"net/http"
	"testing"

	"github.com/sync-tool/gateway/internal/middleware"
)

func TestSanitizeString(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{
			input:    "Error talking to OpenAI with key sk-1234567890abcdefghijklmnopqrstuvwxyz123456",
			expected: "Error talking to OpenAI with key [REDACTED]",
		},
		{
			input:    "Gemini key AIzaSyD-1234567890abcdefghijklmnopqrstu failed",
			expected: "Gemini key [REDACTED] failed",
		},
		{
			input:    "Anthropic key ant-api03-1234567890abcdefghijklmnopq",
			expected: "Anthropic key [REDACTED]",
		},
		{
			input:    "Authorization: Bearer my-secret-jwt-token-xyz",
			expected: "Authorization: Bearer [REDACTED]",
		},
	}

	for _, tc := range tests {
		actual := middleware.SanitizeString(tc.input)
		if actual != tc.expected {
			t.Errorf("SanitizeString(%q) = %q, expected %q", tc.input, actual, tc.expected)
		}
	}
}

func TestSanitizingWriter(t *testing.T) {
	var buf bytes.Buffer
	w := &middleware.SanitizingWriter{Target: &buf}

	_, _ = w.Write([]byte("Leaking sk-1234567890abcdefghijklmnopqrstuvwxyz in logs"))
	if bytes.Contains(buf.Bytes(), []byte("sk-1234567890")) {
		t.Fatalf("sanitizing writer failed to scrub key: %s", buf.String())
	}
}

func TestSanitizeUpstreamHeaders(t *testing.T) {
	h := make(http.Header)
	h.Set("Authorization", "Bearer client-secret")
	h.Set("X-Workspace-Id", "ws-123")
	h.Set("Content-Type", "application/json")

	sanitized := middleware.SanitizeUpstreamHeaders(h)
	if sanitized.Get("Authorization") != "" {
		t.Error("expected Authorization header to be stripped")
	}
	if sanitized.Get("X-Workspace-Id") != "" {
		t.Error("expected X-Workspace-Id header to be stripped")
	}
	if sanitized.Get("Content-Type") != "application/json" {
		t.Error("expected Content-Type to be preserved")
	}
}
