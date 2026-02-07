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

// DefaultBallotReadyEndpoint is the default GraphQL endpoint for BallotReady.
const DefaultBallotReadyEndpoint = "https://bpi.civicengine.com/graphql"

// LoadFromEnv loads provider configuration from environment variables.
//
// Environment variables:
//   - POLITICIAN_PROVIDER: "cicero" or "ballotready" (default: "cicero")
//   - CICERO_KEY: API key for Cicero (required if using cicero)
//   - BALLOTREADY_KEY: API key for BallotReady (required if using ballotready)
//   - BALLOTREADY_ENDPOINT: GraphQL endpoint for BallotReady (default: https://bpi.civicengine.com/graphql)
func LoadFromEnv() Config {
	providerStr := strings.ToLower(strings.TrimSpace(os.Getenv("POLITICIAN_PROVIDER")))

	var provider ProviderType
	switch providerStr {
	case "ballotready":
		provider = ProviderBallotReady
	default:
		provider = ProviderCicero
	}

	endpoint := strings.TrimSpace(os.Getenv("BALLOTREADY_ENDPOINT"))
	if endpoint == "" {
		endpoint = DefaultBallotReadyEndpoint
	}

	return Config{
		Provider:            provider,
		CiceroKey:           os.Getenv("CICERO_KEY"),
		BallotReadyKey:      os.Getenv("BALLOTREADY_KEY"),
		BallotReadyEndpoint: endpoint,
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
