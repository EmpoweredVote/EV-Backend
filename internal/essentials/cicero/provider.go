package cicero

import (
	"context"
	"strings"
	"time"

	"github.com/EmpoweredVote/EV-Backend/internal/essentials/provider"
)

// CiceroProvider implements the OfficialProvider interface using the Cicero API.
type CiceroProvider struct {
	client *Client
}

// Ensure CiceroProvider implements OfficialProvider.
var _ provider.OfficialProvider = (*CiceroProvider)(nil)

// init registers the Cicero provider in the provider registry.
func init() {
	provider.RegisterProvider(provider.ProviderCicero, func(cfg provider.Config) (provider.OfficialProvider, error) {
		return NewProvider(cfg.CiceroKey), nil
	})
}

// NewProvider creates a new CiceroProvider with the given API key.
func NewProvider(apiKey string) *CiceroProvider {
	return &CiceroProvider{
		client: NewClient(apiKey),
	}
}

// Name returns the provider name.
func (p *CiceroProvider) Name() string {
	return "cicero"
}

// FetchByZip fetches officials for a ZIP code, optionally filtered by district types.
func (p *CiceroProvider) FetchByZip(ctx context.Context, zip string, districtTypes []string) ([]provider.NormalizedOfficial, error) {
	start := time.Now()

	officials, err := p.client.FetchOfficialsByZip(ctx, zip, districtTypes)
	if err != nil {
		return nil, err
	}

	result := TransformBatch(officials)
	provider.LogTransform("cicero", len(officials), len(result), time.Since(start))

	return result, nil
}

// FetchByState fetches state-level officials for a given state.
func (p *CiceroProvider) FetchByState(ctx context.Context, state, sampleZip string) ([]provider.NormalizedOfficial, error) {
	start := time.Now()

	officials, err := p.client.FetchOfficialsByZip(ctx, sampleZip, provider.StateDistrictTypes)
	if err != nil {
		return nil, err
	}

	// Filter to only officials from the target state
	stateOfficials := make([]CiceroOfficial, 0)
	for _, off := range officials {
		if strings.EqualFold(off.Office.RepresentingState, state) ||
			strings.EqualFold(off.Office.District.State, state) {
			stateOfficials = append(stateOfficials, off)
		}
	}

	result := TransformBatch(stateOfficials)
	provider.LogTransform("cicero", len(officials), len(result), time.Since(start))

	return result, nil
}

// FetchFederal fetches federal-level officials.
func (p *CiceroProvider) FetchFederal(ctx context.Context) ([]provider.NormalizedOfficial, error) {
	start := time.Now()

	// Use DC ZIP for federal officials query (federal officials are the same everywhere)
	const federalZip = "20001"

	officials, err := p.client.FetchOfficialsByZip(ctx, federalZip, provider.FederalDistrictTypes)
	if err != nil {
		return nil, err
	}

	result := TransformBatch(officials)
	provider.LogTransform("cicero", len(officials), len(result), time.Since(start))

	return result, nil
}

// HealthCheck verifies the provider can connect to the Cicero API.
func (p *CiceroProvider) HealthCheck(ctx context.Context) error {
	return p.client.HealthCheck(ctx)
}
