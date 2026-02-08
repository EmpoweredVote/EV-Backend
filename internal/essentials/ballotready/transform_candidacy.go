package ballotready

import (
	"github.com/EmpoweredVote/EV-Backend/internal/essentials/provider"
)

// TransformCandidaciesToNormalized converts BallotReady candidacy data to normalized format.
func TransformCandidaciesToNormalized(personWithCandidacies *PersonWithCandidacies) []provider.NormalizedCandidacy {
	if personWithCandidacies == nil || len(personWithCandidacies.Candidacies) == 0 {
		return nil
	}

	normalized := make([]provider.NormalizedCandidacy, 0, len(personWithCandidacies.Candidacies))

	for _, candidacy := range personWithCandidacies.Candidacies {
		nc := provider.NormalizedCandidacy{
			CandidacyExternalID: candidacy.ID,
			Withdrawn:           candidacy.Withdrawn,
			Result:              candidacy.Result,
		}

		// Party
		if candidacy.Party != nil {
			nc.PartyName = candidacy.Party.Name
		}

		// Race and Election details
		if candidacy.Race != nil {
			nc.IsPrimary = candidacy.Race.IsPrimary
			nc.IsRunoff = candidacy.Race.IsRunoff
			nc.IsUnexpiredTerm = candidacy.Race.IsUnexpiredTerm

			if candidacy.Race.Position != nil {
				nc.PositionName = candidacy.Race.Position.Name
			}

			if candidacy.Race.Election != nil {
				nc.ElectionName = candidacy.Race.Election.Name
				nc.ElectionDate = candidacy.Race.Election.Day
			}
		}

		// Endorsements
		nc.Endorsements = transformEndorsements(candidacy.Endorsements, nc.CandidacyExternalID, nc.ElectionDate)

		// Stances
		nc.Stances = transformStances(candidacy.Stances, nc.CandidacyExternalID, nc.ElectionDate)

		normalized = append(normalized, nc)
	}

	return normalized
}

// transformEndorsements converts BallotReady endorsements to normalized format.
func transformEndorsements(endorsements []EndorsementBR, candidacyID, electionDate string) []provider.NormalizedEndorsement {
	if len(endorsements) == 0 {
		return nil
	}

	normalized := make([]provider.NormalizedEndorsement, 0, len(endorsements))

	for _, end := range endorsements {
		ne := provider.NormalizedEndorsement{
			CandidacyExternalID: candidacyID,
			EndorserString:      end.EndorserString,
			Recommendation:      end.Recommendation,
			Status:              end.Status,
			ElectionDate:        electionDate,
		}

		// Organization details (if available)
		if end.Endorser != nil {
			ne.Organization = &provider.NormalizedEndorserOrganization{
				ExternalID:  end.Endorser.ID,
				Name:        end.Endorser.Name,
				Description: end.Endorser.Description,
				LogoURL:     end.Endorser.LogoURL,
				IssueName:   end.Endorser.IssueName,
				State:       end.Endorser.State,
			}
		}

		normalized = append(normalized, ne)
	}

	return normalized
}

// transformStances converts BallotReady stances to normalized format.
func transformStances(stances []StanceBR, candidacyID, electionDate string) []provider.NormalizedStance {
	if len(stances) == 0 {
		return nil
	}

	normalized := make([]provider.NormalizedStance, 0, len(stances))

	for _, stance := range stances {
		ns := provider.NormalizedStance{
			CandidacyExternalID: candidacyID,
			Statement:           stance.Statement,
			ReferenceURL:        stance.ReferenceURL,
			Locale:              stance.Locale,
			ElectionDate:        electionDate,
		}

		// Issue details
		if stance.Issue != nil {
			ns.Issue = transformIssue(stance.Issue)
		}

		normalized = append(normalized, ns)
	}

	return normalized
}

// transformIssue converts BallotReady issue to normalized format (with recursive parent support).
func transformIssue(issue *IssueBR) *provider.NormalizedIssue {
	if issue == nil {
		return nil
	}

	ni := &provider.NormalizedIssue{
		ExternalID:   issue.ID,
		Name:         issue.Name,
		Key:          issue.Key,
		ExpandedText: issue.ExpandedText,
	}

	// Recursively handle parent issue
	if issue.Parent != nil {
		ni.Parent = transformIssue(issue.Parent)
	}

	return ni
}
