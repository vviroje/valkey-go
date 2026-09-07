package valkey

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type mockRetryHandler struct {
	RetryDelayFn        func(attempts int, _ Completed, err error) time.Duration
	WaitForRetryFn      func(ctx context.Context, duration time.Duration)
	WaitOrSkipRetryFunc func(ctx context.Context, attempts int, _ Completed, err error) bool
}

var _ retryHandler = (*mockRetryHandler)(nil)

func (m *mockRetryHandler) WaitOrSkipRetry(ctx context.Context, attempts int, cmd Completed, err error) bool {
	return m.WaitOrSkipRetryFunc(ctx, attempts, cmd, err)
}

func (m *mockRetryHandler) RetryDelay(attempts int, cmd Completed, err error) time.Duration {
	return m.RetryDelayFn(attempts, cmd, err)
}

func (m *mockRetryHandler) WaitForRetry(ctx context.Context, duration time.Duration) {
	m.WaitForRetryFn(ctx, duration)
}

func TestDefaultRetryDelay(t *testing.T) {
	for i := range 100 {
		err := errors.New("test")
		got := defaultRetryDelayFn(i, Completed{}, err)

		if got < 0 || got > defaultMaxRetryDelay {
			t.Errorf("defaultRetryDelayFn(%d, %v) = %v; want >= 0 and <= %v", i, err, got, defaultMaxRetryDelay)
		}
	}
}

func TestRetryer_RetryDelay(t *testing.T) {
	r := &retryer{
		RetryDelayFn: func(attempts int, _ Completed, err error) time.Duration {
			return time.Second
		},
	}

	got := r.RetryDelay(0, Completed{}, nil)
	if got != time.Second {
		t.Errorf("RetryDelay() = %v; want %v", got, time.Second)
	}
}

func TestRetryer_WaitForRetry(t *testing.T) {
	t.Run("context is canceled", func(t *testing.T) {
		r := &retryer{}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		start := time.Now()
		r.WaitForRetry(ctx, time.Second)
		elapsed := time.Since(start)

		if elapsed > 100*time.Millisecond {
			t.Errorf("WaitForRetry() took %v; want < 100ms", elapsed)
		}
	})

	t.Run("context deadline is before duration", func(t *testing.T) {
		r := &retryer{}

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		start := time.Now()
		r.WaitForRetry(ctx, time.Second)
		elapsed := time.Since(start)

		if elapsed > 100*time.Millisecond {
			t.Errorf("WaitForRetry() took %v; want < 100ms", elapsed)
		}
	})

	t.Run("wait until duration", func(t *testing.T) {
		r := &retryer{}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		start := time.Now()
		r.WaitForRetry(ctx, 50*time.Millisecond)
		elapsed := time.Since(start)

		if elapsed > 100*time.Millisecond {
			t.Errorf("WaitForRetry() took %v; want < 100ms", elapsed)
		}
	})

	t.Run("empty context", func(t *testing.T) {
		r := &retryer{}

		start := time.Now()
		r.WaitForRetry(context.Background(), 50*time.Millisecond)
		elapsed := time.Since(start)

		if elapsed > 100*time.Millisecond {
			t.Errorf("WaitForRetry() took %v; want < 100ms", elapsed)
		}
	})
}

func TestRetrier_WaitOrSkipRetry(t *testing.T) {
	t.Run("RetryDelayFn returns negative delay", func(t *testing.T) {
		r := &retryer{
			RetryDelayFn: func(attempts int, _ Completed, err error) time.Duration {
				return -1 * time.Second
			},
		}

		shouldRetry := r.WaitOrSkipRetry(nil, 0, Completed{}, nil)
		if shouldRetry {
			t.Error("WaitOrSkipRetry() = true; want false")
		}
	})

	t.Run("RetryDelayFn returns 0 delay", func(t *testing.T) {
		r := &retryer{
			RetryDelayFn: func(attempts int, _ Completed, err error) time.Duration {
				return 0
			},
		}

		shouldRetry := r.WaitOrSkipRetry(nil, 0, Completed{}, nil)
		if !shouldRetry {
			t.Error("WaitOrSkipRetry() = false; want true")
		}
	})

	t.Run("context is canceled", func(t *testing.T) {
		r := &retryer{
			RetryDelayFn: func(attempts int, _ Completed, err error) time.Duration {
				return time.Second
			},
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		shouldRetry := r.WaitOrSkipRetry(ctx, 0, Completed{}, nil)
		if !shouldRetry {
			t.Error("WaitOrSkipRetry() = false; want true")
		}
	})

	t.Run("context deadline is before delay", func(t *testing.T) {
		r := &retryer{
			RetryDelayFn: func(attempts int, _ Completed, err error) time.Duration {
				return time.Second
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		start := time.Now()
		shouldRetry := r.WaitOrSkipRetry(ctx, 0, Completed{}, nil)
		if shouldRetry {
			t.Error("WaitOrSkipRetry() = true; want false")
		}
		elapsed := time.Since(start)

		if elapsed > 100*time.Millisecond {
			t.Errorf("WaitOrSkipRetry() took %v; want < 100ms", elapsed)
		}
	})

	t.Run("wait until next retry", func(t *testing.T) {
		r := &retryer{
			RetryDelayFn: func(attempts int, _ Completed, err error) time.Duration {
				return 50 * time.Millisecond
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		start := time.Now()
		shouldRetry := r.WaitOrSkipRetry(ctx, 0, Completed{}, nil)
		if !shouldRetry {
			t.Error("WaitOrSkipRetry() = false; want true")
		}
		elapsed := time.Since(start)

		if elapsed > 100*time.Millisecond {
			t.Errorf("WaitOrSkipRetry() took %v; want < 100ms", elapsed)
		}
	})

	t.Run("empty context", func(t *testing.T) {
		r := &retryer{
			RetryDelayFn: func(attempts int, _ Completed, err error) time.Duration {
				return 50 * time.Millisecond
			},
		}

		start := time.Now()
		shouldRetry := r.WaitOrSkipRetry(context.Background(), 0, Completed{}, nil)
		if !shouldRetry {
			t.Error("WaitOrSkipRetry() = false; want true")
		}
		elapsed := time.Since(start)

		if elapsed > 100*time.Millisecond {
			t.Errorf("WaitOrSkipRetry() took %v; want < 100ms", elapsed)
		}
	})
}

func TestDecorrelatedJitterDelayFn(t *testing.T) {
	t.Run("default values when base or maxDelay <= 0", func(t *testing.T) {
		fn := DecorrelatedJitterDelayFn(0, 0)
		for i := 0; i < 50; i++ {
			d := fn(i)
			if d < 100*time.Millisecond || d > 3*time.Second {
				t.Fatalf("expected delay between 100ms and 3s, got %v", d)
			}
		}
	})

	t.Run("negative attempt returns 0", func(t *testing.T) {
		fn := DecorrelatedJitterDelayFn(50*time.Millisecond, 500*time.Millisecond)
		if d := fn(-1); d != 0 {
			t.Fatalf("expected 0 for negative attempt, got %v", d)
		}
	})

	t.Run("bounds and variation", func(t *testing.T) {
		base := 50 * time.Millisecond
		maxDelay := 500 * time.Millisecond
		fn := DecorrelatedJitterDelayFn(base, maxDelay)

		delays := make(map[time.Duration]bool)
		for i := 0; i < 100; i++ {
			d := fn(i)
			if d < base || d > maxDelay {
				t.Fatalf("attempt %d: delay %v out of bounds [%v, %v]", i, d, base, maxDelay)
			}
			delays[d] = true
		}
		if len(delays) < 10 {
			t.Fatalf("expected diverse delays due to jitter, got only %d unique values", len(delays))
		}
	})

	t.Run("concurrent safety", func(t *testing.T) {
		fn := DecorrelatedJitterDelayFn(10*time.Millisecond, 100*time.Millisecond)
		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(attempt int) {
				defer wg.Done()
				for j := 0; j < 50; j++ {
					d := fn(attempt)
					if d < 10*time.Millisecond || d > 100*time.Millisecond {
						t.Errorf("out of bounds: %v", d)
					}
				}
			}(i)
		}
		wg.Wait()
	})
}

func TestDecorrelatedJitterRetryDelayFn(t *testing.T) {
	t.Run("default values when base or maxDelay <= 0", func(t *testing.T) {
		fn := DecorrelatedJitterRetryDelayFn(0, 0)
		for i := 1; i < 50; i++ {
			d := fn(i, Completed{}, nil)
			if d < 10*time.Millisecond || d > defaultMaxRetryDelay {
				t.Fatalf("expected delay between 10ms and %v, got %v", defaultMaxRetryDelay, d)
			}
		}
	})

	t.Run("non-positive attempt returns 0", func(t *testing.T) {
		fn := DecorrelatedJitterRetryDelayFn(10*time.Millisecond, 500*time.Millisecond)
		if d := fn(0, Completed{}, nil); d != 0 {
			t.Fatalf("expected 0 for attempt 0, got %v", d)
		}
		if d := fn(-1, Completed{}, nil); d != 0 {
			t.Fatalf("expected 0 for attempt -1, got %v", d)
		}
	})

	t.Run("bounds and variation", func(t *testing.T) {
		base := 20 * time.Millisecond
		maxDelay := 300 * time.Millisecond
		fn := DecorrelatedJitterRetryDelayFn(base, maxDelay)

		delays := make(map[time.Duration]bool)
		for i := 1; i <= 100; i++ {
			d := fn(i, Completed{}, nil)
			if d < base || d > maxDelay {
				t.Fatalf("attempt %d: delay %v out of bounds [%v, %v]", i, d, base, maxDelay)
			}
			delays[d] = true
		}
		if len(delays) < 10 {
			t.Fatalf("expected diverse delays due to jitter, got only %d unique values", len(delays))
		}
	})

	t.Run("concurrent safety", func(t *testing.T) {
		fn := DecorrelatedJitterRetryDelayFn(10*time.Millisecond, 100*time.Millisecond)
		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(attempt int) {
				defer wg.Done()
				for j := 0; j < 50; j++ {
					d := fn(attempt, Completed{}, nil)
					if d < 10*time.Millisecond || d > 100*time.Millisecond {
						t.Errorf("out of bounds: %v", d)
					}
				}
			}(i + 1)
		}
		wg.Wait()
	})
}
