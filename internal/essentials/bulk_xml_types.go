package essentials

import "encoding/xml"

// ============================================================================
// GPO BillStatus XML types
// Source: https://www.govinfo.gov/bulkdata/BILLSTATUS/{congress}/{type}/
// ============================================================================

// BillStatusXML is the root element of a GPO bulk data bill status file.
type BillStatusXML struct {
	XMLName xml.Name     `xml:"billStatus"`
	Version string       `xml:"version"`
	Bill    BillXML      `xml:"bill"`
}

// BillXML represents the <bill> element inside a GPO BillStatus file.
type BillXML struct {
	Number          string              `xml:"number"`
	Type            string              `xml:"type"`             // HR, S, HJRES, SJRES, HRES, SRES, HCONRES, SCONRES
	Congress        int                 `xml:"congress"`
	IntroducedDate  string              `xml:"introducedDate"`   // YYYY-MM-DD
	Title           string              `xml:"title"`            // Display title
	OriginChamber   string              `xml:"originChamber"`    // "House" or "Senate"
	LegislationURL  string              `xml:"legislationUrl"`
	Sponsors        []BillSponsorXML    `xml:"sponsors>item"`
	Cosponsors      []BillCosponsorXML  `xml:"cosponsors>item"`
	Titles          []BillTitleXML      `xml:"titles>item"`
	Summaries       []BillSummaryXML    `xml:"summaries>summary"`
	Actions         []BillActionXML     `xml:"actions>item"`
	LatestAction    BillLatestActionXML `xml:"latestAction"`
	Subjects        BillSubjectsXML     `xml:"subjects"`
	PolicyArea      BillPolicyAreaXML   `xml:"policyArea"`
	Laws            []BillLawXML        `xml:"laws>item"`
}

// BillSponsorXML represents a bill's primary sponsor.
type BillSponsorXML struct {
	BioguideID string `xml:"bioguideId"`
	FullName   string `xml:"fullName"`
	FirstName  string `xml:"firstName"`
	LastName   string `xml:"lastName"`
	Party      string `xml:"party"`
	State      string `xml:"state"`
	District   string `xml:"district"` // absent for senators
	IsByRequest string `xml:"isByRequest"` // "Y" or "N"
}

// BillCosponsorXML represents a bill cosponsor entry.
type BillCosponsorXML struct {
	BioguideID         string `xml:"bioguideId"`
	FullName           string `xml:"fullName"`
	FirstName          string `xml:"firstName"`
	LastName           string `xml:"lastName"`
	Party              string `xml:"party"`
	State              string `xml:"state"`
	District           string `xml:"district"`
	SponsorshipDate    string `xml:"sponsorshipDate"`    // YYYY-MM-DD
	IsOriginalCosponsor string `xml:"isOriginalCosponsor"` // "True" or "False"
}

// BillTitleXML represents a bill title entry.
type BillTitleXML struct {
	TitleType     string `xml:"titleType"`
	TitleTypeCode string `xml:"titleTypeCode"`
	Title         string `xml:"title"`
}

// BillSummaryXML represents a CRS summary entry.
type BillSummaryXML struct {
	VersionCode string              `xml:"versionCode"`
	ActionDate  string              `xml:"actionDate"` // YYYY-MM-DD
	ActionDesc  string              `xml:"actionDesc"` // e.g., "Introduced in House"
	UpdateDate  string              `xml:"updateDate"`
	CData       BillSummaryCDataXML `xml:"cdata"`
}

// BillSummaryCDataXML wraps the HTML-encoded text inside a summary.
type BillSummaryCDataXML struct {
	Text string `xml:"text"` // HTML-encoded summary text
}

// BillActionXML represents a bill action (status change).
type BillActionXML struct {
	ActionDate   string                    `xml:"actionDate"` // YYYY-MM-DD
	ActionTime   string                    `xml:"actionTime"` // HH:MM:SS, optional
	Text         string                    `xml:"text"`
	Type         string                    `xml:"type"` // IntroReferral, Committee, Floor, President
	ActionCode   string                    `xml:"actionCode"`
	SourceSystem BillActionSourceSystemXML `xml:"sourceSystem"`
	RecordedVotes []BillRecordedVoteXML    `xml:"recordedVotes>recordedVote"`
}

// BillActionSourceSystemXML identifies where the action originated.
type BillActionSourceSystemXML struct {
	Code string `xml:"code"` // "2"=House floor, "9"=Library of Congress; may be absent for Senate
	Name string `xml:"name"`
}

// BillRecordedVoteXML links an action to a roll call vote.
type BillRecordedVoteXML struct {
	RollNumber    int    `xml:"rollNumber"`
	URL           string `xml:"url"`
	Chamber       string `xml:"chamber"`
	Congress      int    `xml:"congress"`
	Date          string `xml:"date"`
	SessionNumber int    `xml:"sessionNumber"`
}

// BillLatestActionXML holds the latest action summary.
type BillLatestActionXML struct {
	ActionDate string `xml:"actionDate"` // YYYY-MM-DD
	Text       string `xml:"text"`
}

// BillSubjectsXML holds legislative subjects and policy area.
type BillSubjectsXML struct {
	Subjects   []BillSubjectXML  `xml:"legislativeSubjects>item"`
	PolicyArea BillPolicyAreaXML `xml:"policyArea"`
}

// BillSubjectXML is a single subject tag.
type BillSubjectXML struct {
	Name string `xml:"name"`
}

// BillPolicyAreaXML is the broad policy area for a bill.
type BillPolicyAreaXML struct {
	Name string `xml:"name"`
}

// BillLawXML holds public/private law information.
type BillLawXML struct {
	Type   string `xml:"type"`   // "Public Law" or "Private Law"
	Number string `xml:"number"` // e.g., "119-21"
}

// ============================================================================
// House Clerk Roll Call Vote XML types
// Source: https://clerk.house.gov/evs/{year}/roll{NNN}.xml
// ============================================================================

// HouseRollCallVoteXML is the root element of a House clerk vote file.
type HouseRollCallVoteXML struct {
	XMLName  xml.Name              `xml:"rollcall-vote"`
	Metadata HouseVoteMetadataXML  `xml:"vote-metadata"`
	VoteData []HouseRecordedVoteXML `xml:"vote-data>recorded-vote"`
}

// HouseVoteMetadataXML holds the header info for a House roll call.
type HouseVoteMetadataXML struct {
	Majority     string                `xml:"majority"`
	Congress     int                   `xml:"congress"`
	Session      string                `xml:"session"`       // "1st" or "2nd"
	Chamber      string                `xml:"chamber"`
	RollCallNum  int                   `xml:"rollcall-num"`
	LegisNum     string                `xml:"legis-num"`     // e.g., "H R 153", "H CON RES 14", "QUORUM"
	VoteQuestion string                `xml:"vote-question"`
	VoteType     string                `xml:"vote-type"`     // "YEA-AND-NAY", "2/3 YEA-AND-NAY", "QUORUM"
	VoteResult   string                `xml:"vote-result"`   // "Passed", "Failed"
	ActionDate   string                `xml:"action-date"`   // "D-Mon-YYYY" format
	VoteDesc     string                `xml:"vote-desc"`
	VoteTotals   HouseVoteTotalsXML    `xml:"vote-totals"`
}

// HouseVoteTotalsXML holds vote counts.
type HouseVoteTotalsXML struct {
	TotalsByVote HouseVoteTotalsByVoteXML `xml:"totals-by-vote"`
}

// HouseVoteTotalsByVoteXML is the overall vote tally.
type HouseVoteTotalsByVoteXML struct {
	YeaTotal      int `xml:"yea-total"`
	NayTotal      int `xml:"nay-total"`
	PresentTotal  int `xml:"present-total"`
	NotVotingTotal int `xml:"not-voting-total"`
}

// HouseRecordedVoteXML represents one member's recorded vote.
type HouseRecordedVoteXML struct {
	Legislator HouseLegislatorXML `xml:"legislator"`
	Vote       string             `xml:"vote"` // "Yea", "Nay", "Present", "Not Voting"
}

// HouseLegislatorXML holds the member info from a House recorded vote.
// The bioguide ID is in the name-id attribute.
type HouseLegislatorXML struct {
	NameID    string `xml:"name-id,attr"`    // Bioguide ID (e.g., "A000370")
	SortField string `xml:"sort-field,attr"`
	Party     string `xml:"party,attr"`       // "D", "R", "I"
	State     string `xml:"state,attr"`
	Role      string `xml:"role,attr"`        // "legislator" or "speaker"
	Name      string `xml:",chardata"`        // Display name
}

// ============================================================================
// Senate Clerk Roll Call Vote XML types
// Source: https://www.senate.gov/legislative/LIS/roll_call_votes/...
// ============================================================================

// SenateRollCallVoteXML is the root element of a Senate clerk vote file.
type SenateRollCallVoteXML struct {
	XMLName          xml.Name                `xml:"roll_call_vote"`
	Congress         int                     `xml:"congress"`
	Session          int                     `xml:"session"`
	CongressYear     int                     `xml:"congress_year"`
	VoteNumber       int                     `xml:"vote_number"`
	VoteDate         string                  `xml:"vote_date"`           // "Month D, YYYY,  HH:MM AM/PM"
	VoteQuestionText string                  `xml:"vote_question_text"`
	VoteDocumentText string                  `xml:"vote_document_text"`  // Human-readable reference
	VoteResultText   string                  `xml:"vote_result_text"`
	Question         string                  `xml:"question"`
	VoteTitle        string                  `xml:"vote_title"`
	MajorityReq      string                  `xml:"majority_requirement"` // "1/2", "3/5", "2/3"
	VoteResult       string                  `xml:"vote_result"`          // e.g., "Nomination Confirmed"
	Document         SenateDocumentXML       `xml:"document"`
	Amendment        SenateAmendmentXML      `xml:"amendment"`
	Count            SenateVoteCountXML      `xml:"count"`
	Members          []SenateMemberVoteXML   `xml:"members>member"`
}

// SenateDocumentXML identifies the bill/nomination being voted on.
type SenateDocumentXML struct {
	DocumentCongress int    `xml:"document_congress"`
	DocumentType     string `xml:"document_type"`   // "S.", "H.R.", "PN", "S.Amdt.", "S.J.Res."
	DocumentNumber   string `xml:"document_number"` // numeric or empty
	DocumentName     string `xml:"document_name"`
	DocumentTitle    string `xml:"document_title"`
}

// SenateAmendmentXML holds amendment info when the vote is on an amendment.
type SenateAmendmentXML struct {
	AmendmentNumber string `xml:"amendment_number"` // e.g., "S.Amdt. 1029"
	AmendmentToDocumentNumber string `xml:"amendment_to_document_number"`
	AmendmentPurpose string `xml:"amendment_purpose"`
}

// SenateVoteCountXML holds overall vote tallies.
type SenateVoteCountXML struct {
	Yeas    int    `xml:"yeas"`
	Nays    int    `xml:"nays"`
	Present string `xml:"present"` // may be empty
	Absent  string `xml:"absent"`  // may be empty
}

// SenateMemberVoteXML represents one senator's vote.
type SenateMemberVoteXML struct {
	MemberFull  string `xml:"member_full"`   // "LastName (P-ST)"
	LastName    string `xml:"last_name"`
	FirstName   string `xml:"first_name"`
	Party       string `xml:"party"`          // "D", "R", "I"
	State       string `xml:"state"`          // 2-letter state code
	VoteCast    string `xml:"vote_cast"`      // "Yea", "Nay", "Not Voting", "Present"
	LISMemberID string `xml:"lis_member_id"` // e.g., "S428"
}
