package ballotready

// GraphQL request and response types for the BallotReady/CivicEngine API.
// These types match the actual schema at https://bpi.civicengine.com/graphql.

// GraphQLRequest represents a GraphQL query request.
type GraphQLRequest struct {
	Query     string                 `json:"query"`
	Variables map[string]interface{} `json:"variables,omitempty"`
}

// GraphQLResponse is the top-level GraphQL response.
type GraphQLResponse struct {
	Data   *ResponseData  `json:"data"`
	Errors []GraphQLError `json:"errors,omitempty"`
}

// GraphQLError represents a GraphQL error.
type GraphQLError struct {
	Message    string                 `json:"message"`
	Locations  []ErrorLocation        `json:"locations,omitempty"`
	Path       []interface{}          `json:"path,omitempty"`
	Extensions map[string]interface{} `json:"extensions,omitempty"`
}

// ErrorLocation represents a location in the GraphQL query.
type ErrorLocation struct {
	Line   int `json:"line"`
	Column int `json:"column"`
}

// ResponseData contains the query results.
type ResponseData struct {
	OfficeHolders *OfficeHolderConnection `json:"officeHolders"`
}

// OfficeHolderConnection is the Relay-style paginated connection.
type OfficeHolderConnection struct {
	Edges    []OfficeHolderEdge `json:"edges"`
	PageInfo PageInfo           `json:"pageInfo"`
}

// OfficeHolderEdge wraps a node with a cursor for pagination.
type OfficeHolderEdge struct {
	Cursor string           `json:"cursor"`
	Node   OfficeHolderNode `json:"node"`
}

// PageInfo contains pagination metadata.
type PageInfo struct {
	HasNextPage bool   `json:"hasNextPage"`
	EndCursor   string `json:"endCursor"`
}

// OfficeHolderNode represents a single officeholder record.
type OfficeHolderNode struct {
	ID                string `json:"id"`
	DatabaseID        int    `json:"databaseId"`
	IsCurrent         bool   `json:"isCurrent"`
	IsAppointed       bool   `json:"isAppointed"`
	IsVacant          bool   `json:"isVacant"`
	OfficeTitle       string `json:"officeTitle"`
	StartAt           string `json:"startAt"`
	EndAt             string `json:"endAt"`
	TotalYearsInOffice int    `json:"totalYearsInOffice"`

	Person    *Person    `json:"person"`
	Parties   []Party    `json:"parties"`
	Position  *Position  `json:"position"`
	Addresses []Address  `json:"addresses"`
	Contacts  []Contact  `json:"contacts"`
	URLs      []URLEntry `json:"urls"`
}

// Person contains personal information about an officeholder.
type Person struct {
	ID         string `json:"id"`
	DatabaseID int    `json:"databaseId"`
	FirstName  string `json:"firstName"`
	MiddleName string `json:"middleName"`
	LastName   string `json:"lastName"`
	Suffix     string `json:"suffix"`
	Nickname   string `json:"nickname"`
	FullName   string `json:"fullName"`
	Slug       string `json:"slug"`
	BioText    string `json:"bioText"`
	BioguideID string `json:"bioguideId"`

	Images      []Image      `json:"images"`
	Contacts    []Contact    `json:"contacts"`
	Degrees     []Degree     `json:"degrees"`
	Experiences []Experience `json:"experiences"`
	URLs        []URLEntry   `json:"urls"`
}

// Image represents a profile photo.
type Image struct {
	URL  string `json:"url"`
	Type string `json:"type"`
}

// Degree represents an educational degree.
type Degree struct {
	ID       string `json:"id"`
	Degree   string `json:"degree"`
	Major    string `json:"major"`
	School   string `json:"school"`
	GradYear int    `json:"gradYear"`
}

// Experience represents work or office history.
type Experience struct {
	ID           string `json:"id"`
	Title        string `json:"title"`
	Organization string `json:"organization"`
	Type         string `json:"type"`
	Start        string `json:"start"`
	End          string `json:"end"`
}

// Party represents a political party affiliation.
type Party struct {
	Name      string `json:"name"`
	ShortName string `json:"shortName"`
}

// Position represents the elected/appointed position.
type Position struct {
	ID                 string              `json:"id"`
	DatabaseID         int                 `json:"databaseId"`
	Name               string              `json:"name"`
	Level              string              `json:"level"`
	Tier               interface{}         `json:"tier"`
	State              string              `json:"state"`
	Judicial           bool                `json:"judicial"`
	Appointed          bool                `json:"appointed"`
	SubAreaName        string              `json:"subAreaName"`
	SubAreaValue       string              `json:"subAreaValue"`
	NormalizedPosition *NormalizedPosition `json:"normalizedPosition"`
	ElectionFrequencies []ElectionFrequency `json:"electionFrequencies"`
}

// NormalizedPosition contains the standardized position name and description.
type NormalizedPosition struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	MTFCC       string `json:"mtfcc"`
}

// ElectionFrequency describes how often elections occur for a position.
type ElectionFrequency struct {
	Frequency     []int `json:"frequency"`
	ReferenceYear int   `json:"referenceYear"`
}

// Address represents a contact address for an officeholder.
type Address struct {
	AddressLine1 string `json:"addressLine1"`
	AddressLine2 string `json:"addressLine2"`
	City         string `json:"city"`
	State        string `json:"state"`
	Zip          string `json:"zip"`
	Type         string `json:"type"`
}

// Contact represents contact information (email, phone, fax).
type Contact struct {
	Email string `json:"email"`
	Phone string `json:"phone"`
	Fax   string `json:"fax"`
	Type  string `json:"type"`
}

// URLEntry represents a URL associated with an officeholder.
type URLEntry struct {
	URL  string `json:"url"`
	Type string `json:"type"`
}
