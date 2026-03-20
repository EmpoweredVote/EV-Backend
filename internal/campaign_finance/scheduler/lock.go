package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-co-op/gocron-redis-lock/v2"
	"github.com/go-co-op/gocron/v2"
	"github.com/go-redsync/redsync/v4"
	"github.com/redis/go-redis/v9"
)

// BuildLocker constructs a Redis-backed gocron.Locker using the given Upstash URL.
// The URL format "rediss://..." triggers TLS automatically via redis.ParseURL.
// TTL should be ~2x the expected max job runtime so a crashed run's lock expires
// before the next scheduled trigger (per Phase 7 CONTEXT.md decision).
//
// On any failure (empty URL, parse error, ping failure), an error is returned
// and the caller should fall back to NewMutexLocker.
func BuildLocker(redisURL string, lockTTL time.Duration) (gocron.Locker, error) {
	if redisURL == "" {
		return nil, fmt.Errorf("redis URL is empty")
	}

	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parse redis URL: %w", err)
	}

	client := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	locker, err := redislock.NewRedisLocker(client,
		redsync.WithTries(1),
		redsync.WithExpiry(lockTTL),
	)
	if err != nil {
		return nil, fmt.Errorf("create redis locker: %w", err)
	}

	return locker, nil
}

// mutexLocker implements gocron.Locker using an in-process sync.Mutex.
// Used as a fallback when Redis is unavailable (e.g., local dev, missing env var).
// Only prevents duplicate runs within a single process — multi-instance protection
// requires Redis.
type mutexLocker struct {
	mu sync.Mutex
}

// NewMutexLocker returns a gocron.Locker backed by an in-process mutex.
// This fallback allows the scheduler to start even without Redis configured.
func NewMutexLocker() gocron.Locker {
	return &mutexLocker{}
}

func (l *mutexLocker) Lock(_ context.Context, key string) (gocron.Lock, error) {
	if !l.mu.TryLock() {
		return nil, fmt.Errorf("lock held for key %s", key)
	}
	return &mutexLock{mu: &l.mu}, nil
}

// mutexLock implements gocron.Lock. Unlock releases the in-process mutex.
type mutexLock struct {
	mu *sync.Mutex
}

func (l *mutexLock) Unlock(_ context.Context) error {
	l.mu.Unlock()
	return nil
}
