package proxy

import "bytes"

// ProviderConfig defines the routing and stream termination rules for an LLM provider.
type ProviderConfig struct {
	BaseURL        string
	FinishDetector func(chunk []byte) bool
}

var defaultProviders = map[string]ProviderConfig{
	"openai": {
		BaseURL: "https://api.openai.com",
		FinishDetector: func(b []byte) bool {
			return bytes.Contains(b, []byte("data: [DONE]"))
		},
	},
	"anthropic": {
		BaseURL: "https://api.anthropic.com",
		FinishDetector: func(b []byte) bool {
			return bytes.Contains(b, []byte(`"type":"message_stop"`))
		},
	},
	"gemini": {
		BaseURL: "https://generativelanguage.googleapis.com",
		FinishDetector: func(b []byte) bool {
			return bytes.Contains(b, []byte(`"finishReason":"STOP"`))
		},
	},
}

// Router provides thread-safe provider lookup and configuration.
type Router struct {
	providers map[string]ProviderConfig
}

// NewRouter initializes a Router with default provider definitions.
func NewRouter(customProviders map[string]ProviderConfig) *Router {
	p := make(map[string]ProviderConfig)
	for k, v := range defaultProviders {
		p[k] = v
	}
	for k, v := range customProviders {
		p[k] = v
	}
	return &Router{providers: p}
}

// Get resolves the ProviderConfig for a given provider ID.
func (r *Router) Get(providerID string) (ProviderConfig, bool) {
	cfg, ok := r.providers[providerID]
	return cfg, ok
}
