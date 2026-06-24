package health

import (
	"log"
	"net/http"
	"sync"
	"time"
)

// Registry maintains the set of healthy backends for each service.
// It runs background health checks and is safe for concurrent reads.
type Registry struct {
	services map[string][]string // prefix → all configured backends
	interval time.Duration
	client   *http.Client

	mu      sync.RWMutex
	healthy map[string][]string // prefix → currently healthy backends
}

func NewRegistry(services map[string][]string, interval time.Duration) *Registry {
	healthy := make(map[string][]string, len(services))
	for k, v := range services {
		// Optimistically mark all backends healthy at startup so the gateway
		// can serve traffic immediately while the first health round completes.
		healthy[k] = append([]string{}, v...)
	}
	return &Registry{
		services: services,
		interval: interval,
		client:   &http.Client{Timeout: 2 * time.Second},
		healthy:  healthy,
	}
}

// Start launches the background health-check loop. Call once at startup.
func (r *Registry) Start() {
	go r.loop()
}

func (r *Registry) loop() {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for range ticker.C {
		r.checkAll()
	}
}

func (r *Registry) checkAll() {
	next := make(map[string][]string, len(r.services))
	for prefix, backends := range r.services {
		for _, addr := range backends {
			if r.isHealthy(addr) {
				next[prefix] = append(next[prefix], addr)
			} else {
				log.Printf("[health] %s unhealthy, removing from rotation", addr)
			}
		}
	}
	r.mu.Lock()
	r.healthy = next
	r.mu.Unlock()
}

func (r *Registry) isHealthy(addr string) bool {
	resp, err := r.client.Get(addr + "/health")
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// Healthy returns the current list of healthy backends for a service prefix.
// Returns nil if no healthy backends are available.
func (r *Registry) Healthy(prefix string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	backends := r.healthy[prefix]
	if len(backends) == 0 {
		return nil
	}
	// Return a copy so callers can't mutate the registry's slice.
	out := make([]string, len(backends))
	copy(out, backends)
	return out
}
