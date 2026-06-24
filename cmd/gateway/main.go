package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/davey/api-gateway/internal/agent"
	"github.com/davey/api-gateway/internal/circuitbreaker"
	"github.com/davey/api-gateway/internal/health"
	"github.com/davey/api-gateway/internal/proxy"
	"github.com/davey/api-gateway/internal/ratelimit"
)

func main() {
	redisAddr := envOr("REDIS_ADDR", "localhost:6379")
	listenAddr := envOr("LISTEN_ADDR", ":8080")

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("redis connect failed: %v", err)
	}
	log.Printf("connected to Redis at %s", redisAddr)

	services := map[string][]string{
		"/service-a": splitEnv("SERVICE_A_BACKENDS", "http://service-a:8081"),
		"/service-b": splitEnv("SERVICE_B_BACKENDS", "http://service-b:8082"),
	}

	cbs := map[string]*circuitbreaker.CB{}
	for name := range services {
		cbs[name] = circuitbreaker.New(circuitbreaker.Config{
			FailureThreshold: 5,
			SuccessThreshold: 2,
			Timeout:          10 * time.Second,
		})
	}

	registry := health.NewRegistry(services, 5*time.Second)
	registry.Start()

	limiter := ratelimit.New(rdb, ratelimit.Config{
		WindowSeconds: 60,
		MaxRequests:   100,
		BucketSeconds: 10,
	})

	// Start AI control plane agent if ANTHROPIC_API_KEY is set.
	// The gateway runs fine without it — agent is an optional enhancement.
	if a := agent.New(limiter, cbs, registry); a != nil {
		ctx := context.Background()
		go a.Start(ctx, 30*time.Second)
	} else {
		log.Println("AI agent disabled — set ANTHROPIC_API_KEY to enable")
	}

	router := proxy.NewRouter(registry, cbs, limiter)

	log.Printf("gateway listening on %s", listenAddr)
	if err := http.ListenAndServe(listenAddr, router); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func splitEnv(key, fallback string) []string {
	v := os.Getenv(key)
	if v == "" {
		v = fallback
	}
	var out []string
	start := 0
	for i := 0; i <= len(v); i++ {
		if i == len(v) || v[i] == ',' {
			if s := v[start:i]; s != "" {
				out = append(out, s)
			}
			start = i + 1
		}
	}
	return out
}
