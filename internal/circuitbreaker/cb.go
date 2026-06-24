package circuitbreaker

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var ErrOpen = errors.New("circuit breaker open")

type state int

const (
	stateClosed   state = iota
	stateOpen
	stateHalfOpen
)

type Config struct {
	FailureThreshold int
	SuccessThreshold int
	Timeout          time.Duration
}

type CB struct {
	cfg Config
	mu  sync.Mutex

	state            state
	consecutiveFails int
	consecutiveOK    int
	openedAt         time.Time

	// AI agent writes this to hot-reload the failure threshold without restart.
	// Zero means "use cfg.FailureThreshold".
	liveThreshold atomic.Int32
}

func New(cfg Config) *CB {
	return &CB{cfg: cfg, state: stateClosed}
}

// FailureThreshold returns the AI-adjusted threshold or the config default.
func (cb *CB) FailureThreshold() int {
	if v := cb.liveThreshold.Load(); v > 0 {
		return int(v)
	}
	return cb.cfg.FailureThreshold
}

// SetFailureThreshold lets the AI agent hot-reload the threshold.
func (cb *CB) SetFailureThreshold(n int) {
	cb.liveThreshold.Store(int32(n))
}

// ConsecutiveFails exposes the current failure streak for the AI agent to read.
func (cb *CB) ConsecutiveFails() int {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.consecutiveFails
}

func (cb *CB) Allow() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case stateClosed:
		return nil
	case stateOpen:
		if time.Since(cb.openedAt) >= cb.cfg.Timeout {
			cb.state = stateHalfOpen
			cb.consecutiveOK = 0
			return nil
		}
		return ErrOpen
	case stateHalfOpen:
		return nil
	}
	return nil
}

func (cb *CB) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.consecutiveFails = 0

	if cb.state == stateHalfOpen {
		cb.consecutiveOK++
		if cb.consecutiveOK >= cb.cfg.SuccessThreshold {
			cb.state = stateClosed
		}
	}
}

func (cb *CB) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.consecutiveOK = 0

	switch cb.state {
	case stateClosed:
		cb.consecutiveFails++
		// Uses live threshold so AI adjustments take effect immediately.
		if cb.consecutiveFails >= cb.FailureThreshold() {
			cb.state = stateOpen
			cb.openedAt = time.Now()
		}
	case stateHalfOpen:
		cb.state = stateOpen
		cb.openedAt = time.Now()
	}
}

func (cb *CB) State() string {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	switch cb.state {
	case stateClosed:
		return "closed"
	case stateOpen:
		return "open"
	case stateHalfOpen:
		return "half-open"
	}
	return "unknown"
}
