package sessionutil

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"
)

const (
	defaultInitRetryTimeout = 30 * time.Second
	defaultInitRetryBase    = 200 * time.Millisecond
	defaultInitRetryMax     = 3 * time.Second
)

// Init initializes session registration with bounded retry for transient etcd errors.
// NOTE: This file is provided as a direct source-file artifact (not a git patch file)
// for easier in-place replacement in Milvus source trees.
func (s *Session) Init(serverName string, address string, exclusive bool, triggerKill bool) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultInitRetryTimeout)
	defer cancel()

	err := retryWithExponentialBackoff(ctx, defaultInitRetryBase, defaultInitRetryMax, func() error {
		return s.registerService(serverName, address, exclusive, triggerKill)
	})
	if err != nil {
		panic(err)
	}
}

func retryWithExponentialBackoff(ctx context.Context, base, max time.Duration, fn func() error) error {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	delay := base
	var lastErr error

	for attempt := 1; ; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}

		lastErr = err
		if !isTransientEtcdErr(err) {
			return err
		}

		jitter := time.Duration(rng.Int63n(int64(delay / 2)))
		sleepFor := delay + jitter
		if sleepFor > max {
			sleepFor = max
		}

		select {
		case <-time.After(sleepFor):
		case <-ctx.Done():
			return fmt.Errorf("session init retry timeout after %d attempts: %w", attempt, lastErr)
		}

		delay *= 2
		if delay > max {
			delay = max
		}
	}
}

func isTransientEtcdErr(err error) bool {
	if err == nil {
		return false
	}

	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "etcdserver: leader changed") ||
		strings.Contains(msg, "etcdserver: no leader") ||
		strings.Contains(msg, "context deadline exceeded") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "transport is closing")
}
