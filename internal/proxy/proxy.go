package proxy

import (
	"bytes"
	"errors"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/davey/api-gateway/internal/circuitbreaker"
	"github.com/davey/api-gateway/internal/health"
	"github.com/davey/api-gateway/internal/ratelimit"
)

const (
	maxRetries  = 3
	baseBackoff = 100 * time.Millisecond
	maxBackoff  = 2 * time.Second
)

// Router is an http.Handler that rate-limits, load-balances, and proxies
// requests to downstream services with circuit-breaker protection.
type Router struct {
	registry *health.Registry
	cbs      map[string]*circuitbreaker.CB
	limiter  *ratelimit.Limiter
	counters map[string]*atomic.Uint64 // round-robin counters per service
}

func NewRouter(
	registry *health.Registry,
	cbs map[string]*circuitbreaker.CB,
	limiter *ratelimit.Limiter,
) *Router {
	counters := make(map[string]*atomic.Uint64, len(cbs))
	for prefix := range cbs {
		counters[prefix] = new(atomic.Uint64)
	}
	return &Router{registry: registry, cbs: cbs, limiter: limiter, counters: counters}
}

func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.limiter.Middleware(http.HandlerFunc(r.route)).ServeHTTP(w, req)
}

func (r *Router) route(w http.ResponseWriter, req *http.Request) {
	prefix := r.matchPrefix(req.URL.Path)
	if prefix == "" {
		http.Error(w, "no route", http.StatusNotFound)
		return
	}

	cb := r.cbs[prefix]

	// Retry loop with exponential backoff + jitter.
	// Each attempt captures the response in a buffer so we never write a partial
	// upstream error to the client — we only flush on success or final failure.
	var lastBuf *bufferedResponse
	backoff := baseBackoff

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			// Don't retry into an open circuit.
			if err := cb.Allow(); err != nil {
				http.Error(w, "service unavailable (circuit open)", http.StatusServiceUnavailable)
				return
			}
			jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
			sleep := backoff + jitter
			if sleep > maxBackoff {
				sleep = maxBackoff
			}
			time.Sleep(sleep)
			backoff *= 2
		}

		if err := cb.Allow(); err != nil {
			http.Error(w, "service unavailable (circuit open)", http.StatusServiceUnavailable)
			return
		}

		backend, err := r.pickBackend(prefix)
		if err != nil {
			http.Error(w, "no healthy backends", http.StatusServiceUnavailable)
			return
		}

		buf := newBufferedResponse()
		r.proxyTo(backend, prefix, buf, req)
		lastBuf = buf

		if buf.statusCode >= 500 {
			cb.RecordFailure()
			log.Printf("[proxy] attempt %d failed for %s (status %d), retrying", attempt+1, prefix, buf.statusCode)
			continue
		}

		cb.RecordSuccess()
		buf.flush(w)
		return
	}

	// All retries exhausted — flush the last (failed) response to the client.
	log.Printf("[proxy] all retries exhausted for %s", prefix)
	if lastBuf != nil {
		lastBuf.flush(w)
	}
}

func (r *Router) proxyTo(backendAddr, prefix string, w http.ResponseWriter, req *http.Request) {
	target, err := url.Parse(backendAddr)
	if err != nil {
		http.Error(w, "bad backend URL", http.StatusInternalServerError)
		return
	}

	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("[proxy] upstream error: %v", err)
		http.Error(w, "upstream error", http.StatusBadGateway)
	}

	// Strip the service prefix so the backend sees a clean path.
	// e.g. /service-a/users → /users
	req2 := req.Clone(req.Context())
	req2.URL.Path = strings.TrimPrefix(req.URL.Path, prefix)
	if req2.URL.Path == "" {
		req2.URL.Path = "/"
	}
	req2.URL.Host = target.Host
	req2.URL.Scheme = target.Scheme
	req2.Host = target.Host

	proxy.ServeHTTP(w, req2)
}

// matchPrefix finds the longest matching service prefix for the given path.
func (r *Router) matchPrefix(path string) string {
	best := ""
	for prefix := range r.cbs {
		if strings.HasPrefix(path, prefix) && len(prefix) > len(best) {
			best = prefix
		}
	}
	return best
}

// pickBackend selects a healthy backend using round-robin.
func (r *Router) pickBackend(prefix string) (string, error) {
	backends := r.registry.Healthy(prefix)
	if len(backends) == 0 {
		return "", errors.New("no healthy backends")
	}
	n := r.counters[prefix].Add(1)
	return backends[int(n)%len(backends)], nil
}

// bufferedResponse captures headers and body in memory so failed upstream
// responses can be discarded and retried without touching the real connection.
type bufferedResponse struct {
	header     http.Header
	body       bytes.Buffer
	statusCode int
}

func newBufferedResponse() *bufferedResponse {
	return &bufferedResponse{
		header:     make(http.Header),
		statusCode: http.StatusOK,
	}
}

func (b *bufferedResponse) Header() http.Header        { return b.header }
func (b *bufferedResponse) WriteHeader(code int)       { b.statusCode = code }
func (b *bufferedResponse) Write(p []byte) (int, error) { return b.body.Write(p) }

func (b *bufferedResponse) flush(w http.ResponseWriter) {
	for k, v := range b.header {
		w.Header()[k] = v
	}
	w.WriteHeader(b.statusCode)
	w.Write(b.body.Bytes())
}
