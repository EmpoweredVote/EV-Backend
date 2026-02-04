package treasury

import (
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

// City represents a municipality with budget data
type City struct {
	ID         uuid.UUID `gorm:"type:uuid;primaryKey;default:uuid_generate_v4()" json:"id"`
	Name       string    `gorm:"uniqueIndex;not null" json:"name"`
	State      string    `gorm:"not null" json:"state"`
	Population int       `json:"population"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`

	Budgets []Budget `gorm:"foreignKey:CityID" json:"budgets,omitempty"`
}

func (City) TableName() string {
	return "treasury.cities"
}

// Budget represents a fiscal year budget for a city
type Budget struct {
	ID          uuid.UUID      `gorm:"type:uuid;primaryKey;default:uuid_generate_v4()" json:"id"`
	CityID      uuid.UUID      `gorm:"type:uuid;not null;index:idx_budget_city_year,unique" json:"city_id"`
	FiscalYear  int            `gorm:"not null;index:idx_budget_city_year,unique" json:"fiscal_year"`
	DatasetType string         `gorm:"not null;default:'operating'" json:"dataset_type"` // operating, revenue, salaries
	TotalBudget float64        `gorm:"not null" json:"total_budget"`
	DataSource  string         `json:"data_source"`
	Hierarchy   pq.StringArray `gorm:"type:text[]" json:"hierarchy"`
	GeneratedAt time.Time      `json:"generated_at"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`

	City       City             `gorm:"foreignKey:CityID" json:"city,omitempty"`
	Categories []BudgetCategory `gorm:"foreignKey:BudgetID" json:"categories,omitempty"`
}

func (Budget) TableName() string {
	return "treasury.budgets"
}

// BudgetCategory represents a hierarchical category within a budget
type BudgetCategory struct {
	ID               uuid.UUID  `gorm:"type:uuid;primaryKey;default:uuid_generate_v4()" json:"id"`
	BudgetID         uuid.UUID  `gorm:"type:uuid;not null;index" json:"budget_id"`
	ParentID         *uuid.UUID `gorm:"type:uuid;index" json:"parent_id,omitempty"`
	Name             string     `gorm:"not null" json:"name"`
	Amount           float64    `gorm:"not null" json:"amount"`
	Percentage       float64    `json:"percentage"`
	Color            string     `json:"color"`
	Description      string     `json:"description,omitempty"`
	WhyMatters       string     `json:"why_matters,omitempty"`
	HistoricalChange *float64   `json:"historical_change,omitempty"`
	ItemCount        int        `gorm:"default:0" json:"items"`
	SortOrder        int        `gorm:"default:0" json:"sort_order"`
	Depth            int        `gorm:"default:0" json:"depth"` // 0=root, 1=child, etc.
	LinkKey          string     `json:"link_key,omitempty"`     // For transaction linking

	Budget        Budget            `gorm:"foreignKey:BudgetID" json:"-"`
	Parent        *BudgetCategory   `gorm:"foreignKey:ParentID" json:"-"`
	Subcategories []BudgetCategory  `gorm:"foreignKey:ParentID" json:"subcategories,omitempty"`
	LineItems     []BudgetLineItem  `gorm:"foreignKey:CategoryID" json:"line_items,omitempty"`
}

func (BudgetCategory) TableName() string {
	return "treasury.budget_categories"
}

// BudgetLineItem represents an individual line item within a category
type BudgetLineItem struct {
	ID             uuid.UUID `gorm:"type:uuid;primaryKey;default:uuid_generate_v4()" json:"id"`
	CategoryID     uuid.UUID `gorm:"type:uuid;not null;index" json:"category_id"`
	Description    string    `gorm:"not null" json:"description"`
	ApprovedAmount float64   `json:"approved_amount"`
	ActualAmount   float64   `json:"actual_amount"`

	// Salary-specific fields
	BasePay   *float64 `json:"base_pay,omitempty"`
	Benefits  *float64 `json:"benefits,omitempty"`
	Overtime  *float64 `json:"overtime,omitempty"`
	Other     *float64 `json:"other,omitempty"`
	StartDate *string  `json:"start_date,omitempty"`

	// Transaction-specific fields
	Vendor          *string `json:"vendor,omitempty"`
	Date            *string `json:"date,omitempty"`
	PaymentMethod   *string `json:"payment_method,omitempty"`
	InvoiceNumber   *string `json:"invoice_number,omitempty"`
	Fund            *string `json:"fund,omitempty"`
	ExpenseCategory *string `json:"expense_category,omitempty"`

	Category BudgetCategory `gorm:"foreignKey:CategoryID" json:"-"`
}

func (BudgetLineItem) TableName() string {
	return "treasury.budget_line_items"
}
