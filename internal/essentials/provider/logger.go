package provider

import (
	"log"
	"time"
)

// LogRequest logs an API request being made.
func LogRequest(provider, method, url string, params map[string]interface{}) {
	if len(params) > 0 {
		log.Printf("[%s] %s %s params=%v", provider, method, url, params)
	} else {
		log.Printf("[%s] %s %s", provider, method, url)
	}
}

// LogResponse logs an API response received.
func LogResponse(provider string, statusCode int, duration time.Duration, resultCount int) {
	log.Printf("[%s] response status=%d duration=%dms results=%d",
		provider, statusCode, duration.Milliseconds(), resultCount)
}

// LogError logs an error from an API operation.
func LogError(provider, operation string, err error) {
	log.Printf("[%s] %s error: %v", provider, operation, err)
}

// LogTransform logs transformation of data.
func LogTransform(provider string, inputCount, outputCount int, duration time.Duration) {
	log.Printf("[%s] transformed %d -> %d records in %dms",
		provider, inputCount, outputCount, duration.Milliseconds())
}

// LogUpsert logs database upsert operations.
func LogUpsert(provider string, count int, duration time.Duration) {
	log.Printf("[%s] upserted %d records in %dms",
		provider, count, duration.Milliseconds())
}
