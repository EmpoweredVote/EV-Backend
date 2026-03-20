package scheduler

import (
	"context"
	"log"
	"net/http"
	"time"
)

// PingHealthcheck sends a HEAD request to checkURL+suffix.
// If checkURL is empty the call is a no-op, allowing dev environments to omit
// the HC_*_URL env vars without errors.
// Errors are logged but never returned — monitoring must not crash the scheduler.
func PingHealthcheck(ctx context.Context, checkURL string, suffix string) {
	if checkURL == "" {
		return
	}

	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, checkURL+suffix, nil)
	if err != nil {
		log.Printf("healthcheck: failed to build request for %s%s: %v", checkURL, suffix, err)
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("healthcheck: ping failed for %s%s: %v", checkURL, suffix, err)
		return
	}
	resp.Body.Close()
}

// RunWithHealthcheck wraps fn with Healthchecks.io start/success/fail pings.
// Pings /start before fn runs, "" (success) on nil return, /fail on error.
// The pings are best-effort — fn's error is always propagated to the caller.
func RunWithHealthcheck(ctx context.Context, checkURL string, fn func() error) error {
	PingHealthcheck(ctx, checkURL, "/start")
	err := fn()
	if err != nil {
		PingHealthcheck(ctx, checkURL, "/fail")
		return err
	}
	PingHealthcheck(ctx, checkURL, "")
	return nil
}
