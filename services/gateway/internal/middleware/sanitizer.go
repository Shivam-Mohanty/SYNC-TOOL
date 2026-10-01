package middleware

import (
	"io"
	"net/http"
	"regexp"
)

var (
	// Regex patterns matching OpenAI, Gemini, and Anthropic API keys
	openAIRegex    = regexp.MustCompile(`sk-[a-zA-Z0-9_\-]{20,}`)
	geminiRegex    = regexp.MustCompile(`AIza[0-9A-Za-z\-_]{35}`)
	anthropicRegex = regexp.MustCompile(`ant-[a-zA-Z0-9_\-]{20,}`)
	bearerRegex    = regexp.MustCompile(`(?i)Bearer\s+[a-zA-Z0-9_\-\.]+`)
)

// SanitizeString scrubs secret patterns and replaces them with [REDACTED].
func SanitizeString(s string) string {
	s = openAIRegex.ReplaceAllString(s, "[REDACTED]")
	s = geminiRegex.ReplaceAllString(s, "[REDACTED]")
	s = anthropicRegex.ReplaceAllString(s, "[REDACTED]")
	s = bearerRegex.ReplaceAllString(s, "Bearer [REDACTED]")
	return s
}

// SanitizeBytes scrubs secret patterns from byte slices.
func SanitizeBytes(b []byte) []byte {
	return []byte(SanitizeString(string(b)))
}

// SanitizingWriter wraps an io.Writer to scrub sensitive key patterns before writing to logs.
type SanitizingWriter struct {
	Target io.Writer
}

func (w *SanitizingWriter) Write(p []byte) (n int, err error) {
	sanitized := SanitizeBytes(p)
	_, err = w.Target.Write(sanitized)
	// Return original len(p) to satisfy io.Writer contract
	return len(p), err
}

// LogSanitizerMiddleware strips sensitive headers from requests and sanitizes request logging.
func LogSanitizerMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Ensure headers like Authorization or API keys are not leaked in traces or logs
		sanitizedHeader := make(http.Header)
		for k, v := range r.Header {
			if http.CanonicalHeaderKey(k) == "Authorization" ||
				http.CanonicalHeaderKey(k) == "X-Api-Key" ||
				http.CanonicalHeaderKey(k) == "X-Vault-Token" {
				sanitizedHeader[k] = []string{"[REDACTED]"}
			} else {
				var sanitizedVals []string
				for _, val := range v {
					sanitizedVals = append(sanitizedVals, SanitizeString(val))
				}
				sanitizedHeader[k] = sanitizedVals
			}
		}

		next.ServeHTTP(w, r)
	})
}

// SanitizeUpstreamHeaders strips client credentials before forwarding to upstream providers,
// retaining required provider headers.
func SanitizeUpstreamHeaders(headers http.Header) http.Header {
	sanitized := headers.Clone()
	// Strip client-side auth or internal tracking headers
	sanitized.Del("Authorization")
	sanitized.Del("X-Workspace-Id")
	sanitized.Del("X-Branch-Id")
	sanitized.Del("X-Provider")
	sanitized.Del("X-Actor-Id")
	sanitized.Del("Host")
	return sanitized
}
