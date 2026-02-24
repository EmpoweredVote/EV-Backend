package provider

import (
	"os"
	"strings"
)

// ProviderType identifies which data provider to use.
type ProviderType string

const (
	ProviderCicero      ProviderType = "cicero"
	ProviderBallotReady ProviderType = "ballotready"
)

// Config holds configuration for the politician data provider.
type Config struct {
	// Provider type: "cicero" or "ballotready"
	Provider ProviderType

	// Cicero-specific config
	CiceroKey string

	// BallotReady-specific config
	BallotReadyKey      string
	BallotReadyEndpoint string
}

// LoadFromEnv loads provider configuration from environment variables.
//
// Environment variables:
//   - POLITICIAN_PROVIDER: "cicero" or "ballotready" (default: "ballotready")
//   - CICERO_KEY: API key for Cicero (required if using cicero)
//   - BALLOTREADY_API_KEY: API key for BallotReady (required if using ballotready)
//   - BALLOTREADY_ENDPOINT: GraphQL endpoint for BallotReady (optional, uses default if unset)
func LoadFromEnv() Config {
	providerStr := strings.ToLower(strings.TrimSpace(os.Getenv("POLITICIAN_PROVIDER")))

	var providerType ProviderType
	switch providerStr {
	case "cicero":
		providerType = ProviderCicero
	default:
		providerType = ProviderBallotReady
	}

	return Config{
		Provider:            providerType,
		CiceroKey:           os.Getenv("CICERO_KEY"),
		BallotReadyKey:      os.Getenv("BALLOTREADY_API_KEY"),
		BallotReadyEndpoint: os.Getenv("BALLOTREADY_ENDPOINT"),
	}
}

// Validate checks that the configuration is valid for the selected provider.
func (c Config) Validate() error {
	switch c.Provider {
	case ProviderCicero:
		if c.CiceroKey == "" {
			return ErrMissingCiceroKey
		}
	case ProviderBallotReady:
		if c.BallotReadyKey == "" {
			return ErrMissingBallotReadyKey
		}
	}
	return nil
}
