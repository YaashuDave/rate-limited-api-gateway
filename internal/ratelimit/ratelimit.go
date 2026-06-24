package ratelimit

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

type Config struct {
	WindowSeconds int
	MaxRequests   int
	BucketSeconds int
}

// clientStats tracks per-client request and block counts between agent cycles.
// Swap(0) atomically reads and resets — no lock needed.
type clientStats struct {
	requests atomic.Int64
	blocked  atomic.Int64
}

// ClientStatsSnapshot is what the agent reads each cycle.
type ClientStatsSnapshot struct {
	Requests int64
	Blocked  int64
}

type Limiter struct {
	rdb redis.UniversalClient
	cfg Config

	statsMu sync.RWMutex
	stats   map[string]*clientStats
}

func New(rdb redis.UniversalClient, cfg Config) *Limiter {
	return &Limiter{
		rdb:   rdb,
		cfg:   cfg,
		stats: make(map[string]*clientStats),
	}
}

func (l *Limiter) Allow(ctx context.Context, clientID string) (bool, error) {
	now := time.Now().Unix()
	currentBucket := (now / int64(l.cfg.BucketSeconds)) * int64(l.cfg.BucketSeconds)
	currentKey := l.bucketKey(clientID, currentBucket)

	pipe := l.rdb.Pipeline()
	incrCmd := pipe.Incr(ctx, currentKey)
	pipe.Expire(ctx, currentKey, time.Duration(l.cfg.WindowSeconds+l.cfg.BucketSeconds)*time.Second)
	if _, err := pipe.Exec(ctx); err != nil {
		return false, fmt.Errorf("redis pipeline: %w", err)
	}
	_ = incrCmd

	windowStart := now - int64(l.cfg.WindowSeconds)
	total := 0.0

	for bucketTS := currentBucket; bucketTS > windowStart-int64(l.cfg.BucketSeconds); bucketTS -= int64(l.cfg.BucketSeconds) {
		key := l.bucketKey(clientID, bucketTS)
		val, err := l.rdb.Get(ctx, key).Int()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			return false, fmt.Errorf("redis get: %w", err)
		}
		if bucketTS <= windowStart {
			overlap := float64(bucketTS+int64(l.cfg.BucketSeconds)) - float64(windowStart)
			weight := overlap / float64(l.cfg.BucketSeconds)
			total += float64(val) * weight
		} else {
			total += float64(val)
		}
	}

	limit := l.ClientLimit(ctx, clientID)
	return int(total) <= limit, nil
}

// ClientLimit returns the AI-set per-client override from Redis, or the global default.
func (l *Limiter) ClientLimit(ctx context.Context, clientID string) int {
	val, err := l.rdb.Get(ctx, l.overrideKey(clientID)).Int()
	if err != nil {
		return l.cfg.MaxRequests
	}
	return val
}

// SetClientOverride writes a per-client rate limit override. The agent calls this.
// TTL is 3× the window so the override survives a few missed cycles before expiring.
func (l *Limiter) SetClientOverride(ctx context.Context, clientID string, limit int) error {
	ttl := time.Duration(l.cfg.WindowSeconds*3) * time.Second
	return l.rdb.Set(ctx, l.overrideKey(clientID), limit, ttl).Err()
}

// DrainStats returns per-client request/block counts and atomically resets them to zero.
// Call once per agent cycle.
func (l *Limiter) DrainStats() map[string]ClientStatsSnapshot {
	l.statsMu.RLock()
	ids := make([]string, 0, len(l.stats))
	for id := range l.stats {
		ids = append(ids, id)
	}
	l.statsMu.RUnlock()

	result := make(map[string]ClientStatsSnapshot, len(ids))
	for _, id := range ids {
		l.statsMu.RLock()
		st, ok := l.stats[id]
		l.statsMu.RUnlock()
		if !ok {
			continue
		}
		reqs := st.requests.Swap(0)
		blk := st.blocked.Swap(0)
		if reqs > 0 || blk > 0 {
			result[id] = ClientStatsSnapshot{Requests: reqs, Blocked: blk}
		}
	}
	return result
}

func (l *Limiter) getOrCreateStats(clientID string) *clientStats {
	l.statsMu.RLock()
	st, ok := l.stats[clientID]
	l.statsMu.RUnlock()
	if ok {
		return st
	}
	l.statsMu.Lock()
	defer l.statsMu.Unlock()
	if st, ok = l.stats[clientID]; ok {
		return st
	}
	st = &clientStats{}
	l.stats[clientID] = st
	return st
}

func (l *Limiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientID := r.Header.Get("X-Client-ID")
		if clientID == "" {
			clientID = r.RemoteAddr
		}

		st := l.getOrCreateStats(clientID)
		st.requests.Add(1)

		allowed, err := l.Allow(r.Context(), clientID)
		if err != nil {
			allowed = true // fail open on Redis errors
		}

		if !allowed {
			st.blocked.Add(1)
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (l *Limiter) bucketKey(clientID string, bucketTS int64) string {
	return fmt.Sprintf("ratelimit:%s:%d", clientID, bucketTS)
}

func (l *Limiter) overrideKey(clientID string) string {
	return fmt.Sprintf("ratelimit:override:%s", clientID)
}
