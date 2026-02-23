package provider

import (
	"os"
	"strings"
)

// ProviderType identifies which data provider to use.
type ProviderType string

const (
	ProviderCicero ProviderType = "cicero"
)

// Config holds configuration for the politician data provider.
type Config struct {
	// Provider type: "cicero"
	Provider ProviderType

	// Cicero-specific config
	CiceroKey string
}

// LoadFromEnv loads provider configuration from environment variables.
//
// Environment variables:
//   - POLITICIAN_PROVIDER: "cicero" (default: "cicero")
//   - CICERO_KEY: API key for Cicero (required if using cicero)
func LoadFromEnv() Config {
	providerStr := strings.ToLower(strings.TrimSpace(os.Getenv("POLITICIAN_PROVIDER")))

	var provider ProviderType
	switch providerStr {
	default:
		_ = providerStr
		provider = ProviderCicero
	}

	return Config{
		Provider:  provider,
		CiceroKey: os.Getenv("CICERO_KEY"),
	}
}

// Validate checks that the configuration is valid for the selected provider.
func (c Config) Validate() error {
	switch c.Provider {
	case ProviderCicero:
		if c.CiceroKey == "" {
			return ErrMissingCiceroKey
		}
	}
	return nil
}
