package provider

import (
	"context"
	"errors"
	"fmt"
)

// Common errors
var (
	ErrMissingCiceroKey      = errors.New("CICERO_KEY environment variable is required for cicero provider")
	ErrMissingBallotReadyKey = errors.New("BALLOTREADY_KEY environment variable is required for ballotready provider")
	ErrUnknownProvider       = errors.New("unknown provider type")
)

// OfficialProvider is the interface that all politician data providers must implement.
// It abstracts the differences between Cicero API, BallotReady API, and any future providers.
type OfficialProvider interface {
	// Name returns the provider name for logging purposes.
	Name() string

	// FetchByZip fetches officials for a given ZIP code, optionally filtered by district types.
	// If districtTypes is empty, all district types are fetched.
	FetchByZip(ctx context.Context, zip string, districtTypes []string) ([]NormalizedOfficial, error)

	// FetchByState fetches state-level officials for a given state.
	// sampleZip is used for APIs that require a location parameter.
	FetchByState(ctx context.Context, state, sampleZip string) ([]NormalizedOfficial, error)

	// FetchFederal fetches federal-level officials (President, VP, Cabinet, Congress).
	FetchFederal(ctx context.Context) ([]NormalizedOfficial, error)

	// HealthCheck verifies the provider can connect to its data source.
	HealthCheck(ctx context.Context) error
}

// ProviderRegistry holds registered provider constructors.
// This allows new providers to be registered without modifying this file.
var providerRegistry = make(map[ProviderType]func(Config) (OfficialProvider, error))

// RegisterProvider registers a provider constructor for a given provider type.
// This should be called from init() in each provider package.
func RegisterProvider(providerType ProviderType, constructor func(Config) (OfficialProvider, error)) {
	providerRegistry[providerType] = constructor
}

// NewProvider creates a new OfficialProvider based on the configuration.
// It returns an error if the configuration is invalid or the provider is unknown.
func NewProvider(cfg Config) (OfficialProvider, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	constructor, ok := providerRegistry[cfg.Provider]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownProvider, cfg.Provider)
	}

	return constructor(cfg)
}

// AllDistrictTypes returns all known district types that can be queried.
// This is a superset used when no filtering is needed.
var AllDistrictTypes = []string{
	"NATIONAL_EXEC", "NATIONAL_UPPER", "NATIONAL_LOWER",
	"STATE_EXEC", "STATE_UPPER", "STATE_LOWER",
	"LOCAL_EXEC", "LOCAL", "COUNTY", "SCHOOL", "JUDICIAL",
}

// FederalDistrictTypes are district types for federal officials.
var FederalDistrictTypes = []string{"NATIONAL_EXEC", "NATIONAL_UPPER", "NATIONAL_LOWER"}

// StateDistrictTypes are district types for state officials.
var StateDistrictTypes = []string{"STATE_EXEC", "STATE_UPPER", "STATE_LOWER"}

// LocalDistrictTypes are district types for local officials.
var LocalDistrictTypes = []string{"LOCAL_EXEC", "LOCAL", "COUNTY", "SCHOOL", "JUDICIAL"}
