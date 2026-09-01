package valkey

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/valkey-io/valkey-go/internal/util"
)

const (
	defaultMaxRetries    = 20
	defaultMaxRetryDelay = 1 * time.Second
)

// RetryDelayFn returns the delay that should be used before retrying the
// attempt. Will return a negative delay if the delay could not be determined or does not retry.
type RetryDelayFn func(attempts int, cmd Completed, err error) time.Duration

// defaultRetryDelayFn delays the next retry exponentially without considering the error.
// Max delay is 1 second.
// This "Equal Jitter" delay produced by this implementation is not monotonic increasing. ref: https://aws.amazon.com/ko/blogs/architecture/exponential-backoff-and-jitter/
func defaultRetryDelayFn(attempts int, _ Completed, _ error) time.Duration {
	base := 1 << min(defaultMaxRetries, attempts)
	jitter := util.FastRand(base)
	return min(defaultMaxRetryDelay, time.Duration(base+jitter)*time.Microsecond)
}

// DecorrelatedJitterDelayFn creates a decorrelated jitter backoff function for connection dialing:
// sleep = min(maxDelay, rand(base, prevSleep * 3))
// Ref: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
func DecorrelatedJitterDelayFn(base, maxDelay time.Duration) DialerRetryBackoffFn {
	if base <= 0 {
		base = 100 * time.Millisecond
	}
	if maxDelay <= 0 {
		maxDelay = 3 * time.Second
	}

	var mu sync.Mutex
	prev := base

	return func(attempt int) time.Duration {
		if attempt < 0 {
			return 0
		}
		mu.Lock()
		maxInterval := prev * 3
		if maxInterval > maxDelay {
			maxInterval = maxDelay
		}
		var next time.Duration
		if maxInterval > base {
			next = base + time.Duration(util.FastRand(int(maxInterval-base)))
		} else {
			next = base
		}
		prev = next
		mu.Unlock()

		return min(maxDelay, next)
	}
}

// DecorrelatedJitterRetryDelayFn creates a decorrelated jitter backoff function for command retries:
// sleep = min(maxDelay, rand(base, prevSleep * 3))
func DecorrelatedJitterRetryDelayFn(base, maxDelay time.Duration) RetryDelayFn {
	if base <= 0 {
		base = 10 * time.Millisecond
	}
	if maxDelay <= 0 {
		maxDelay = defaultMaxRetryDelay
	}

	var mu sync.Mutex
	prev := base

	return func(attempts int, _ Completed, _ error) time.Duration {
		if attempts <= 0 {
			return 0
		}
		mu.Lock()
		maxInterval := prev * 3
		if maxInterval > maxDelay {
			maxInterval = maxDelay
		}
		var next time.Duration
		if maxInterval > base {
			next = base + time.Duration(util.FastRand(int(maxInterval-base)))
		} else {
			next = base
		}
		prev = next
		mu.Unlock()

		return min(maxDelay, next)
	}
}

type retryHandler interface {
	// RetryDelay returns the delay that should be used before retrying the
	// attempt. Will return a negative delay if the delay could not be determined or does
	// not retry.
	// If the delay is zero, the next retry should be attempted immediately.
	RetryDelay(attempts int, cmd Completed, err error) time.Duration

	// WaitForRetry waits until the next retry should be attempted.
	WaitForRetry(ctx context.Context, duration time.Duration)

	// WaitOrSkipRetry waits until the next retry should be attempted
	// or returns false if the command should not be retried.
	// Returns false immediately if the command should not be retried.
	// Returns true after the delay if the command should be retried.
	WaitOrSkipRetry(ctx context.Context, attempts int, cmd Completed, err error) bool
}

type retryer struct {
	RetryDelayFn RetryDelayFn
}

var _ retryHandler = (*retryer)(nil)

func newRetryer(retryDelayFn RetryDelayFn) *retryer {
	return &retryer{RetryDelayFn: retryDelayFn}
}

func (r *retryer) RetryDelay(attempts int, cmd Completed, err error) time.Duration {
	return r.RetryDelayFn(attempts, cmd, err)
}

func (r *retryer) WaitForRetry(ctx context.Context, duration time.Duration) {
	if duration > 0 {
		if ch := ctx.Done(); ch != nil {
			tm := time.NewTimer(duration)
			defer tm.Stop()
			select {
			case <-ch:
			case <-tm.C:
			}
		} else {
			time.Sleep(duration)
		}
	}
}

func (r *retryer) WaitOrSkipRetry(
	ctx context.Context, attempts int, cmd Completed, err error,
) bool {
	if delay := r.RetryDelay(attempts, cmd, err); delay == 0 {
		runtime.Gosched()
		return true
	} else if delay > 0 {
		if dl, ok := ctx.Deadline(); !ok || time.Until(dl) > delay {
			r.WaitForRetry(ctx, delay)
			return true
		}
	}
	return false
}
