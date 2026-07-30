package graph

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"time"
)

// Fixed State keys written when diverting via on_error.
const (
	StateKeyErrorNode = "error_node"
	StateKeyLastError = "last_error"
	StateKeyStatus    = "status"
	StatusFailed      = "failed"
)

// RetryPolicy controls automatic re-execution of a failed node attempt.
// Zero MaxAttempts (or omitting the policy) means a single attempt.
type RetryPolicy struct {
	// MaxAttempts includes the first try. Values <= 1 disable retry.
	MaxAttempts int
	// InitialInterval is the delay before the first retry (default 500ms).
	InitialInterval time.Duration
	// BackoffFactor multiplies the interval after each retry (default 2).
	BackoffFactor float64
	// MaxInterval caps the backoff delay (default 128s).
	MaxInterval time.Duration
	// Jitter adds up to 20% random noise to the delay when true (default true when policy set via Normalize).
	Jitter bool
	// RetryOn returns true if err should be retried. Nil uses DefaultRetryOn.
	RetryOn func(error) bool
}

// Normalize fills defaults. MaxAttempts < 1 becomes 1 (no retry beyond first).
func (p RetryPolicy) Normalize() RetryPolicy {
	if p.MaxAttempts < 1 {
		p.MaxAttempts = 1
	}
	if p.InitialInterval <= 0 {
		p.InitialInterval = 500 * time.Millisecond
	}
	if p.BackoffFactor <= 0 {
		p.BackoffFactor = 2
	}
	if p.MaxInterval <= 0 {
		p.MaxInterval = 128 * time.Second
	}
	if p.RetryOn == nil {
		p.RetryOn = DefaultRetryOn
	}
	return p
}

// NonRetryable is an optional marker for business errors that must not retry.
type NonRetryable interface {
	NonRetryable() bool
}

// NeverRetry reports errors that must never be retried.
func NeverRetry(err error) bool {
	if err == nil {
		return true
	}
	if IsInterrupted(err) {
		return true
	}
	if errors.Is(err, ErrLeaseLost) {
		return true
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var nr NonRetryable
	if errors.As(err, &nr) && nr.NonRetryable() {
		return true
	}
	return false
}

// DefaultRetryOn retries any error that is not NeverRetry.
func DefaultRetryOn(err error) bool {
	return !NeverRetry(err)
}

func (p RetryPolicy) shouldRetry(err error, attempt int) bool {
	p = p.Normalize()
	if attempt >= p.MaxAttempts {
		return false
	}
	if NeverRetry(err) {
		return false
	}
	return p.RetryOn(err)
}

func (p RetryPolicy) backoff(failedAttempt int) time.Duration {
	p = p.Normalize()
	// failedAttempt is 1-based index of the attempt that just failed.
	mult := math.Pow(p.BackoffFactor, float64(failedAttempt-1))
	d := time.Duration(float64(p.InitialInterval) * mult)
	if d > p.MaxInterval {
		d = p.MaxInterval
	}
	if p.Jitter && d > 0 {
		// Up to +20% jitter.
		j := time.Duration(rand.Int63n(int64(d)/5 + 1))
		d += j
	}
	return d
}

type attemptKey struct{}

// WithAttempt stores the 1-based attempt number for the current node call.
func WithAttempt(ctx context.Context, n int) context.Context {
	return context.WithValue(ctx, attemptKey{}, n)
}

// Attempt returns the 1-based attempt number (0 if unset).
func Attempt(ctx context.Context) int {
	v, _ := ctx.Value(attemptKey{}).(int)
	return v
}

// NodeOption configures a single node at AddNode time.
type NodeOption func(*nodeConfig)

type nodeConfig struct {
	retry RetryPolicy
	hasRetry bool
}

// WithRetry attaches a RetryPolicy to the node.
func WithRetry(p RetryPolicy) NodeOption {
	return func(c *nodeConfig) {
		c.retry = p.Normalize()
		c.hasRetry = true
	}
}
