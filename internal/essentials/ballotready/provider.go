package ballotready

import (
	"context"
	"strings"
	"time"

	"github.com/EmpoweredVote/EV-Backend/internal/essentials/provider"
)

// BallotReadyProvider implements the OfficialProvider interface using the BallotReady API.
type BallotReadyProvider struct {
	client *Client
}

// Ensure BallotReadyProvider implements OfficialProvider.
var _ provider.OfficialProvider = (*BallotReadyProvider)(nil)

// init registers the BallotReady provider in the provider registry.
func init() {
	provider.RegisterProvider(provider.ProviderBallotReady, func(cfg provider.Config) (provider.OfficialProvider, error) {
		return NewProvider(cfg.BallotReadyKey, cfg.BallotReadyEndpoint), nil
	})
}

// NewProvider creates a new BallotReadyProvider.
func NewProvider(apiKey, endpoint string) *BallotReadyProvider {
	return &BallotReadyProvider{
		client: NewClient(apiKey, endpoint),
	}
}

// Client returns the underlying BallotReady API client.
// Used by the search handler for address-based lookups.
func (p *BallotReadyProvider) Client() *Client { return p.client }

// Name returns the provider name.
func (p *BallotReadyProvider) Name() string {
	return "ballotready"
}

// FetchByZip fetches officials for a ZIP code, optionally filtered by district types.
func (p *BallotReadyProvider) FetchByZip(ctx context.Context, zip string, districtTypes []string) ([]provider.NormalizedOfficial, error) {
	start := time.Now()

	nodes, err := p.client.FetchOfficeHoldersByZip(ctx, zip)
	if err != nil {
		return nil, err
	}

	// Filter by district types if specified
	if len(districtTypes) > 0 {
		nodes = FilterByDistrictTypes(nodes, districtTypes)
	}

	result := TransformBatch(nodes)
	provider.LogTransform("ballotready", len(nodes), len(result), time.Since(start))

	return result, nil
}

// FetchByState fetches state-level officials for a given state.
func (p *BallotReadyProvider) FetchByState(ctx context.Context, state, sampleZip string) ([]provider.NormalizedOfficial, error) {
	start := time.Now()

	nodes, err := p.client.FetchOfficeHoldersByZip(ctx, sampleZip)
	if err != nil {
		return nil, err
	}

	// Filter to state-level positions for the target state
	stateNodes := make([]OfficeHolderNode, 0)
	for _, node := range nodes {
		dt := districtTypeForNode(node)
		isStateLevel := dt == "STATE_EXEC" || dt == "STATE_UPPER" || dt == "STATE_LOWER" ||
			dt == "NATIONAL_UPPER" || dt == "NATIONAL_LOWER"
		if !isStateLevel {
			continue
		}

		// Check if it's for the target state
		if node.Position != nil && strings.EqualFold(node.Position.State, state) {
			stateNodes = append(stateNodes, node)
		}
	}

	result := TransformBatch(stateNodes)
	provider.LogTransform("ballotready", len(nodes), len(result), time.Since(start))

	return result, nil
}

// FetchFederal fetches federal-level officials.
func (p *BallotReadyProvider) FetchFederal(ctx context.Context) ([]provider.NormalizedOfficial, error) {
	start := time.Now()

	// Use DC ZIP for federal officials (they're nationwide)
	const federalZip = "20001"

	nodes, err := p.client.FetchOfficeHoldersByZip(ctx, federalZip)
	if err != nil {
		return nil, err
	}

	// Filter to federal positions only
	federalNodes := FilterByDistrictTypes(nodes, provider.FederalDistrictTypes)

	result := TransformBatch(federalNodes)
	provider.LogTransform("ballotready", len(nodes), len(result), time.Since(start))

	return result, nil
}

// HealthCheck verifies the provider can connect to the BallotReady API.
func (p *BallotReadyProvider) HealthCheck(ctx context.Context) error {
	return p.client.HealthCheck(ctx)
}
