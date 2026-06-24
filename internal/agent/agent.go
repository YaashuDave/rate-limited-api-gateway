// Package agent implements the AI control plane that watches gateway metrics
// every 30 seconds and adjusts rate limits and circuit breaker thresholds live.
//
// Cost discipline:
//   - Smart gate: skips the Claude call when metrics are stable (~60% of cycles)
//   - Prompt caching: system prompt is cached after the first call (90% cheaper on reads)
//   - Haiku 4.5: $1/$5 per MTok — fast and sufficient for structured decisions
//   - No-op is cheap: if Claude decides nothing needs changing it doesn't call the
//     tool at all; output is ~5 tokens
package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"sync"
	"time"

	anthropic "github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"

	"github.com/davey/api-gateway/internal/circuitbreaker"
	"github.com/davey/api-gateway/internal/health"
	"github.com/davey/api-gateway/internal/ratelimit"
)

// ── types the agent reasons over ─────────────────────────────────────────────

type Adjustment struct {
	Type     string `json:"type"`      // "rate_limit" or "circuit_breaker"
	Target   string `json:"target"`    // clientID or service prefix
	NewValue int    `json:"new_value"` // new req/60s limit or CB failure threshold
	Reason   string `json:"reason"`
}

type ClientSnapshot struct {
	ID                string `json:"id"`
	CurrentLimit      int    `json:"current_limit"`
	RequestsThisCycle int64  `json:"requests_this_cycle"`
	BlockedThisCycle  int64  `json:"blocked_this_cycle"`
}

type ServiceSnapshot struct {
	Name               string `json:"name"`
	CBState            string `json:"cb_state"`
	CBFailureThreshold int    `json:"cb_failure_threshold"`
	ConsecutiveFails   int    `json:"consecutive_fails"`
	HealthyBackends    int    `json:"healthy_backends"`
}

type Snapshot struct {
	Cycle         int               `json:"cycle"`
	Timestamp     string            `json:"timestamp"`
	Clients       []ClientSnapshot  `json:"clients"`
	Services      []ServiceSnapshot `json:"services"`
	LastDecisions []Adjustment      `json:"last_decisions,omitempty"`
}

// ── system prompt (cached after the first call) ───────────────────────────────

const systemPrompt = `You are the AI control plane for a production API gateway. Every 30 seconds you receive a metrics snapshot and decide whether to adjust rate limits or circuit breaker thresholds.

HARD BOUNDS — never violate:
- Rate limits: 10–500 req/60s per client
- Max rate limit delta per cycle: ±30% of the current value (minimum ±5)
- CB failure threshold: 2–20 consecutive failures
- Max CB threshold delta per cycle: ±3

TIGHTEN a client rate limit when:
- Sudden spike (>3× their normal volume in a single cycle)
- Client is being blocked AND traffic looks anomalous (not a gradual ramp)

LOOSEN a client rate limit when:
- Client consistently uses >85% of quota with zero blocks for 2+ cycles
- The current limit is clearly too low for legitimate steady traffic

RAISE CB failure threshold when:
- A service has a non-zero but stable background error rate and the CB is tripping too often

LOWER CB failure threshold when:
- A service error rate is increasing — catch cascading failures faster

RULES:
1. If nothing needs changing, do NOT call apply_adjustments. Silence is correct.
2. One adjustment per target per cycle maximum.
3. Always give a concise reason.`

// ── the tool Claude must call to act ─────────────────────────────────────────

var adjustTool = anthropic.ToolParam{
	Name:        "apply_adjustments",
	Description: anthropic.String("Apply rate limit or circuit breaker adjustments to the live gateway. Only call this when a change is actually needed — silence means no changes."),
	InputSchema: anthropic.ToolInputSchemaParam{
		Properties: map[string]any{
			"adjustments": map[string]any{
				"type":        "array",
				"description": "Adjustments to apply. Empty array is not valid — if no changes are needed, do not call this tool.",
				"items": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"type":      map[string]any{"type": "string", "enum": []string{"rate_limit", "circuit_breaker"}},
						"target":    map[string]any{"type": "string", "description": "clientID for rate_limit; service prefix (e.g. /service-b) for circuit_breaker"},
						"new_value": map[string]any{"type": "integer", "description": "New rate limit (req/60s) or new CB failure threshold"},
						"reason":    map[string]any{"type": "string", "description": "One-sentence explanation"},
					},
					"required": []string{"type", "target", "new_value", "reason"},
				},
			},
		},
	},
}

// ── agent ─────────────────────────────────────────────────────────────────────

type Agent struct {
	limiter  *ratelimit.Limiter
	cbs      map[string]*circuitbreaker.CB
	registry *health.Registry
	client   *anthropic.Client

	mu            sync.Mutex
	cycle         int
	lastSnapshot  *Snapshot
	lastDecisions []Adjustment

	// cost tracking
	totalInputTokens  int64
	totalCacheHits    int64
	totalOutputTokens int64
	callsMade         int64
	callsSkipped      int64
}

// New creates an Agent. Returns nil (disabled) if ANTHROPIC_API_KEY is not set.
func New(
	limiter *ratelimit.Limiter,
	cbs map[string]*circuitbreaker.CB,
	registry *health.Registry,
) *Agent {
	key := os.Getenv("ANTHROPIC_API_KEY")
	if key == "" {
		return nil
	}
	c := anthropic.NewClient(option.WithAPIKey(key))
	return &Agent{
		limiter:  limiter,
		cbs:      cbs,
		registry: registry,
		client:   &c,
	}
}

// Start runs the agent loop. Call in a goroutine.
func (a *Agent) Start(ctx context.Context, interval time.Duration) {
	log.Printf("[agent] started — interval=%s model=claude-haiku-4-5 (smart-gated, prompt-cached)", interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			a.logCostSummary()
			return
		case <-ticker.C:
			a.runCycle(ctx)
		}
	}
}

func (a *Agent) runCycle(ctx context.Context) {
	a.mu.Lock()
	a.cycle++
	cycle := a.cycle
	a.mu.Unlock()

	snap := a.collect(cycle)

	if !a.shouldCall(snap) {
		a.mu.Lock()
		a.callsSkipped++
		a.mu.Unlock()
		log.Printf("[agent] cycle %d: metrics stable — skipping Claude call", cycle)
		return
	}

	decisions, usage, err := a.callClaude(ctx, snap)
	if err != nil {
		log.Printf("[agent] cycle %d: Claude error: %v", cycle, err)
		return
	}

	a.mu.Lock()
	a.callsMade++
	a.totalInputTokens += int64(usage.InputTokens)
	a.totalOutputTokens += int64(usage.OutputTokens)
	a.totalCacheHits += int64(usage.CacheReadInputTokens)
	a.lastDecisions = decisions
	a.lastSnapshot = snap
	a.mu.Unlock()

	if len(decisions) == 0 {
		log.Printf("[agent] cycle %d: Claude → no changes needed  (in=%d cached=%d out=%d ~$%.5f)",
			cycle, usage.InputTokens, usage.CacheReadInputTokens, usage.OutputTokens,
			cycleCost(usage))
		return
	}

	validated := a.validate(decisions, snap)
	a.apply(ctx, validated, cycle, usage)
}

// ── collect ───────────────────────────────────────────────────────────────────

func (a *Agent) collect(cycle int) *Snapshot {
	snap := &Snapshot{
		Cycle:     cycle,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}

	// Per-client metrics — DrainStats atomically resets counters to zero.
	stats := a.limiter.DrainStats()
	for clientID, st := range stats {
		limit := a.limiter.ClientLimit(context.Background(), clientID)
		snap.Clients = append(snap.Clients, ClientSnapshot{
			ID:                clientID,
			CurrentLimit:      limit,
			RequestsThisCycle: st.Requests,
			BlockedThisCycle:  st.Blocked,
		})
	}

	// Per-service CB metrics.
	for prefix, cb := range a.cbs {
		healthy := a.registry.Healthy(prefix)
		snap.Services = append(snap.Services, ServiceSnapshot{
			Name:               prefix,
			CBState:            cb.State(),
			CBFailureThreshold: cb.FailureThreshold(),
			ConsecutiveFails:   cb.ConsecutiveFails(),
			HealthyBackends:    len(healthy),
		})
	}

	a.mu.Lock()
	snap.LastDecisions = a.lastDecisions
	a.mu.Unlock()

	return snap
}

// ── smart gate ────────────────────────────────────────────────────────────────

// shouldCall returns true only when something meaningful has changed.
// This is the primary cost-saving mechanism — it skips ~60% of cycles.
func (a *Agent) shouldCall(snap *Snapshot) bool {
	a.mu.Lock()
	last := a.lastSnapshot
	cycle := a.cycle
	a.mu.Unlock()

	// Always call on first 2 cycles to establish a baseline.
	if last == nil || cycle <= 2 {
		return true
	}

	// CB state changed — always worth a call.
	for _, svc := range snap.Services {
		for _, lsvc := range last.Services {
			if svc.Name == lsvc.Name && svc.CBState != lsvc.CBState {
				log.Printf("[agent] gate: CB state changed %s %s→%s", svc.Name, lsvc.CBState, svc.CBState)
				return true
			}
		}
	}

	// Client signals worth a call.
	for _, c := range snap.Clients {
		if c.BlockedThisCycle > 0 {
			return true // someone got rate-limited
		}
		if c.CurrentLimit > 0 && float64(c.RequestsThisCycle)/float64(c.CurrentLimit) > 0.85 {
			return true // near quota
		}
		// Traffic spike >50% vs last cycle.
		for _, lc := range last.Clients {
			if lc.ID == c.ID && lc.RequestsThisCycle > 5 {
				delta := math.Abs(float64(c.RequestsThisCycle-lc.RequestsThisCycle)) / float64(lc.RequestsThisCycle)
				if delta > 0.5 {
					return true
				}
			}
		}
	}

	// Heartbeat: call every 5 cycles regardless so the agent stays aware.
	if cycle%5 == 0 {
		return true
	}

	return false
}

// ── Claude call ───────────────────────────────────────────────────────────────

func (a *Agent) callClaude(ctx context.Context, snap *Snapshot) ([]Adjustment, anthropic.Usage, error) {
	snapJSON, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return nil, anthropic.Usage{}, err
	}

	resp, err := a.client.Messages.New(ctx, anthropic.MessageNewParams{
		Model:     anthropic.ModelClaudeHaiku4_5_20251001,
		MaxTokens: 512, // decisions are short; cap prevents runaway output cost
		System: []anthropic.TextBlockParam{{
			Text: systemPrompt,
			// Cache-control marks the system prompt for caching.
			// First call: 1.25× write cost. All subsequent calls: 0.1× read cost (90% cheaper).
			CacheControl: anthropic.NewCacheControlEphemeralParam(),
		}},
		Tools: []anthropic.ToolUnionParam{{OfTool: &adjustTool}},
		Messages: []anthropic.MessageParam{
			anthropic.NewUserMessage(anthropic.NewTextBlock(
				fmt.Sprintf("Gateway metrics snapshot:\n\n%s\n\nApply any needed adjustments, or do nothing if metrics are within normal bounds.", string(snapJSON)),
			)),
		},
	})
	if err != nil {
		return nil, anthropic.Usage{}, fmt.Errorf("anthropic API: %w", err)
	}

	var decisions []Adjustment
	for _, block := range resp.Content {
		tu, ok := block.AsAny().(anthropic.ToolUseBlock)
		if !ok || tu.Name != "apply_adjustments" {
			continue
		}
		var payload struct {
			Adjustments []Adjustment `json:"adjustments"`
		}
		if err := json.Unmarshal([]byte(tu.JSON.Input.Raw()), &payload); err != nil {
			return nil, resp.Usage, fmt.Errorf("parse tool input: %w", err)
		}
		decisions = payload.Adjustments
	}

	return decisions, resp.Usage, nil
}

// ── guardrail validator ───────────────────────────────────────────────────────

// validate enforces hard bounds on every decision before it touches the live system.
// Invalid decisions are logged and dropped — never crash, never corrupt.
func (a *Agent) validate(decisions []Adjustment, snap *Snapshot) []Adjustment {
	var valid []Adjustment
	for _, d := range decisions {
		switch d.Type {
		case "rate_limit":
			current := clientCurrentLimit(d.Target, snap)
			if d.NewValue < 10 || d.NewValue > 500 {
				log.Printf("[agent] GUARDRAIL REJECTED: rate_limit %s → %d (out of 10–500)", d.Target, d.NewValue)
				continue
			}
			maxDelta := int(math.Max(float64(current)*0.30, 5))
			if d.NewValue > current+maxDelta {
				d.NewValue = current + maxDelta
				log.Printf("[agent] GUARDRAIL CAPPED: rate_limit %s → %d (+30%% ceiling)", d.Target, d.NewValue)
			} else if d.NewValue < current-maxDelta {
				d.NewValue = current - maxDelta
				log.Printf("[agent] GUARDRAIL CAPPED: rate_limit %s → %d (-30%% floor)", d.Target, d.NewValue)
			}
			valid = append(valid, d)

		case "circuit_breaker":
			cb, ok := a.cbs[d.Target]
			if !ok {
				log.Printf("[agent] GUARDRAIL REJECTED: circuit_breaker unknown service %q", d.Target)
				continue
			}
			if d.NewValue < 2 || d.NewValue > 20 {
				log.Printf("[agent] GUARDRAIL REJECTED: CB threshold %s → %d (out of 2–20)", d.Target, d.NewValue)
				continue
			}
			current := cb.FailureThreshold()
			if d.NewValue > current+3 {
				d.NewValue = current + 3
			} else if d.NewValue < current-3 {
				d.NewValue = current - 3
			}
			valid = append(valid, d)

		default:
			log.Printf("[agent] GUARDRAIL REJECTED: unknown adjustment type %q", d.Type)
		}
	}
	return valid
}

// ── executor ──────────────────────────────────────────────────────────────────

func (a *Agent) apply(ctx context.Context, decisions []Adjustment, cycle int, usage anthropic.Usage) {
	log.Printf("[agent] cycle %d: %d adjustments | in=%d cached=%d out=%d | ~$%.5f",
		cycle, len(decisions),
		usage.InputTokens, usage.CacheReadInputTokens, usage.OutputTokens,
		cycleCost(usage))

	for _, d := range decisions {
		switch d.Type {
		case "rate_limit":
			if err := a.limiter.SetClientOverride(ctx, d.Target, d.NewValue); err != nil {
				log.Printf("[agent] ERROR applying rate_limit for %s: %v", d.Target, err)
				continue
			}
			log.Printf("[agent] APPLIED  rate_limit   %-30s → %d req/60s | %s", d.Target, d.NewValue, d.Reason)

		case "circuit_breaker":
			if cb, ok := a.cbs[d.Target]; ok {
				cb.SetFailureThreshold(d.NewValue)
				log.Printf("[agent] APPLIED  cb_threshold %-30s → %d failures | %s", d.Target, d.NewValue, d.Reason)
			}
		}
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func clientCurrentLimit(clientID string, snap *Snapshot) int {
	for _, c := range snap.Clients {
		if c.ID == clientID {
			return c.CurrentLimit
		}
	}
	return 100
}

func cycleCost(u anthropic.Usage) float64 {
	// Haiku 4.5: $1/MTok input, $5/MTok output, $0.10/MTok cache read
	return float64(u.InputTokens)*1e-6 +
		float64(u.OutputTokens)*5e-6 +
		float64(u.CacheReadInputTokens)*0.1e-6
}

func (a *Agent) logCostSummary() {
	a.mu.Lock()
	defer a.mu.Unlock()

	total := a.callsMade + a.callsSkipped
	skipPct := 0.0
	if total > 0 {
		skipPct = float64(a.callsSkipped) / float64(total) * 100
	}
	inputCost := float64(a.totalInputTokens) * 1e-6
	outputCost := float64(a.totalOutputTokens) * 5e-6
	cacheSaved := float64(a.totalCacheHits) * (1e-6 - 0.1e-6) // saved vs uncached

	log.Printf("[agent] ── COST SUMMARY ───────────────────────────────")
	log.Printf("[agent] Cycles : %d called, %d skipped (%.0f%% skip rate)", a.callsMade, a.callsSkipped, skipPct)
	log.Printf("[agent] Tokens : %d input  %d cache-hits  %d output", a.totalInputTokens, a.totalCacheHits, a.totalOutputTokens)
	log.Printf("[agent] Cost   : $%.4f input + $%.4f output = $%.4f total", inputCost, outputCost, inputCost+outputCost)
	log.Printf("[agent] Saved  : ~$%.4f via prompt caching", cacheSaved)
	log.Printf("[agent] ─────────────────────────────────────────────────")
}
