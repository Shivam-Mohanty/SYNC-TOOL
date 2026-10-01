package main

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/redis/go-redis/v9"
	"github.com/sync-tool/gateway/internal/auth"
	"github.com/sync-tool/gateway/internal/middleware"
	"github.com/sync-tool/gateway/internal/proxy"
	"github.com/sync-tool/gateway/internal/queue"
)

func main() {
	// Configure structured logger with sanitization
	sanitizingWriter := &middleware.SanitizingWriter{Target: os.Stdout}
	logger := slog.New(slog.NewJSONHandler(sanitizingWriter, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "localhost:6379"
	}

	vaultAddr := os.Getenv("VAULT_ADDR")
	if vaultAddr == "" {
		vaultAddr = "http://localhost:8200"
	}
	vaultToken := os.Getenv("VAULT_TOKEN")
	if vaultToken == "" {
		vaultToken = "root"
	}

	// 1. Initialize Redis Publisher
	rdb := redis.NewClient(&redis.Options{
		Addr: redisURL,
	})
	var publisher queue.QueuePublisher
	ctxPing, cancelPing := context.WithTimeout(context.Background(), 2*time.Second)
	if err := rdb.Ping(ctxPing).Err(); err != nil {
		logger.Warn("redis ping failed; publisher running in degraded mode", "err", err)
	} else {
		logger.Info("connected to redis", "addr", redisURL)
	}
	cancelPing()
	publisher = queue.NewRedisPublisher(rdb, "queue:transcripts:completed")

	// 2. Initialize WAL
	walPath := os.Getenv("WAL_PATH")
	if walPath == "" {
		walPath = "./data/wal/gateway.wal"
	}
	wal, err := proxy.NewLocalWAL(walPath)
	if err != nil {
		logger.Error("failed to initialize WAL fallback", "err", err)
	} else {
		defer wal.Close()
	}

	// 3. Key Resolver setup
	dbURL := os.Getenv("DATABASE_URL")
	var keyResolver auth.KeyResolver
	if dbURL != "" {
		db, err := sql.Open("postgres", dbURL)
		if err == nil {
			keyResolver = auth.NewVaultDBKeyResolver(db, vaultAddr, vaultToken)
			logger.Info("using Vault+Postgres key resolver")
		}
	}
	if keyResolver == nil {
		logger.Info("using static/env fallback key resolver")
		keyResolver = auth.NewStaticKeyResolver(map[string]string{
			"default:openai":    os.Getenv("OPENAI_API_KEY"),
			"default:gemini":    os.Getenv("GEMINI_API_KEY"),
			"default:anthropic": os.Getenv("ANTHROPIC_API_KEY"),
		})
	}

	// 4. Router and Handler setup
	providerRouter := proxy.NewRouter(nil)
	proxyHandler := proxy.NewProxyHandler(proxy.HandlerConfig{
		Router:      providerRouter,
		KeyResolver: keyResolver,
		Publisher:   publisher,
		WAL:         wal,
		Logger:      logger,
	})

	// 5. Chi HTTP Routing
	r := chi.NewRouter()
	r.Use(chimw.RequestID)
	r.Use(chimw.RealIP)
	r.Use(chimw.Recoverer)
	r.Use(middleware.LogSanitizerMiddleware)

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok","service":"sync-gateway"}`))
	})

	// Proxy routes: forwards /v1/* to upstream providers
	r.HandleFunc("/v1/*", proxyHandler.ServeHTTP)
	r.HandleFunc("/v1/chat/completions", proxyHandler.ServeHTTP)
	r.HandleFunc("/v1/messages", proxyHandler.ServeHTTP)

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      r,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 0, // SSE streaming requires unbounded write timeout
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		logger.Info("starting gateway server", "port", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("shutting down gateway server...")
	ctxShutdown, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()

	if err := server.Shutdown(ctxShutdown); err != nil {
		logger.Error("server forced to shutdown", "err", err)
	}

	logger.Info("gateway server exiting")
}
