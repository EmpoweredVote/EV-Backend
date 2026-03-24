package treasury

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// DatasetSummary represents an available dataset for a municipality
type DatasetSummary struct {
	FiscalYear  int    `json:"fiscal_year"`
	DatasetType string `json:"dataset_type"`
}

// MunicipalityResponse extends Municipality with available dataset metadata
type MunicipalityResponse struct {
	ID                uuid.UUID        `json:"id"`
	Name              string           `json:"name"`
	State             string           `json:"state"`
	EntityType        string           `json:"entity_type"`
	Population        int              `json:"population"`
	HeroImageURL      *string          `json:"hero_image_url,omitempty"`
	AvailableDatasets []DatasetSummary `json:"available_datasets"`
}

// ListMunicipalities returns all municipalities with available dataset metadata
func ListMunicipalities(w http.ResponseWriter, r *http.Request) {
	var municipalities []Municipality
	if err := db.DB.Find(&municipalities).Error; err != nil {
		http.Error(w, "Failed to fetch municipalities: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Fetch all budget summaries in a single query
	var budgetSummaries []struct {
		MunicipalityID uuid.UUID `gorm:"column:municipality_id"`
		FiscalYear     int       `gorm:"column:fiscal_year"`
		DatasetType    string    `gorm:"column:dataset_type"`
	}
	if err := db.DB.Model(&Budget{}).Select("municipality_id, fiscal_year, dataset_type").Find(&budgetSummaries).Error; err != nil {
		http.Error(w, "Failed to fetch budget summaries: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Group budget summaries by municipality_id
	datasetMap := make(map[uuid.UUID][]DatasetSummary)
	for _, b := range budgetSummaries {
		datasetMap[b.MunicipalityID] = append(datasetMap[b.MunicipalityID], DatasetSummary{
			FiscalYear:  b.FiscalYear,
			DatasetType: b.DatasetType,
		})
	}

	// Build response with available_datasets per municipality
	responses := make([]MunicipalityResponse, 0, len(municipalities))
	for _, m := range municipalities {
		datasets := datasetMap[m.ID]
		if datasets == nil {
			datasets = []DatasetSummary{}
		}
		responses = append(responses, MunicipalityResponse{
			ID:                m.ID,
			Name:              m.Name,
			State:             m.State,
			EntityType:        m.EntityType,
			Population:        m.Population,
			HeroImageURL:      m.HeroImageURL,
			AvailableDatasets: datasets,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(responses)
}

// GetMunicipality returns a single municipality by ID
func GetMunicipality(w http.ResponseWriter, r *http.Request) {
	municipalityID := chi.URLParam(r, "municipality_id")

	var municipality Municipality
	if err := db.DB.Preload("Budgets").First(&municipality, "id = ?", municipalityID).Error; err != nil {
		http.Error(w, "Municipality not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(municipality)
}

// ListBudgets returns budgets with optional filtering by municipality and year
func ListBudgets(w http.ResponseWriter, r *http.Request) {
	query := db.DB.Model(&Budget{}).Preload("Municipality")

	// Filter by municipality name, municipality_id, or city_id (backward compat)
	if cityName := r.URL.Query().Get("city"); cityName != "" {
		var municipality Municipality
		if err := db.DB.First(&municipality, "name = ?", cityName).Error; err != nil {
			http.Error(w, "Municipality not found", http.StatusNotFound)
			return
		}
		query = query.Where("municipality_id = ?", municipality.ID)
	} else if muniID := r.URL.Query().Get("municipality_id"); muniID != "" {
		query = query.Where("municipality_id = ?", muniID)
	} else if cityID := r.URL.Query().Get("city_id"); cityID != "" {
		// Backward compat: city_id param maps to municipality_id column
		query = query.Where("municipality_id = ?", cityID)
	}

	// Filter by fiscal year
	if yearStr := r.URL.Query().Get("year"); yearStr != "" {
		year, err := strconv.Atoi(yearStr)
		if err != nil {
			http.Error(w, "Invalid year format", http.StatusBadRequest)
			return
		}
		query = query.Where("fiscal_year = ?", year)
	}

	// Filter by dataset type
	if datasetType := r.URL.Query().Get("dataset"); datasetType != "" {
		query = query.Where("dataset_type = ?", datasetType)
	}

	var budgets []Budget
	if err := query.Find(&budgets).Error; err != nil {
		http.Error(w, "Failed to fetch budgets: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(budgets)
}

// GetBudget returns a single budget with its municipality
func GetBudget(w http.ResponseWriter, r *http.Request) {
	budgetID := chi.URLParam(r, "budget_id")

	var budget Budget
	if err := db.DB.Preload("Municipality").First(&budget, "id = ?", budgetID).Error; err != nil {
		http.Error(w, "Budget not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(budget)
}

// GetBudgetCategories returns the category tree for a budget
func GetBudgetCategories(w http.ResponseWriter, r *http.Request) {
	budgetID := chi.URLParam(r, "budget_id")

	// First get all categories for this budget
	var categories []BudgetCategory
	if err := db.DB.Where("budget_id = ?", budgetID).
		Order("depth ASC, sort_order ASC").
		Preload("LineItems").
		Find(&categories).Error; err != nil {
		http.Error(w, "Failed to fetch categories: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Build the tree structure
	tree := buildCategoryTree(categories)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tree)
}

// buildCategoryTree converts a flat list of categories into a nested tree.
// Uses pointer-based assembly so deep subcategories propagate correctly,
// then serializes to value slices at the end.
func buildCategoryTree(categories []BudgetCategory) []BudgetCategory {
	// Map for pointer-based lookup
	categoryMap := make(map[uuid.UUID]*BudgetCategory)
	childrenMap := make(map[uuid.UUID][]*BudgetCategory) // parentID → children pointers
	var rootIDs []uuid.UUID

	// First pass: populate the map, identify roots
	for i := range categories {
		categories[i].Subcategories = nil
		categoryMap[categories[i].ID] = &categories[i]
		if categories[i].ParentID == nil {
			rootIDs = append(rootIDs, categories[i].ID)
		} else {
			pid := *categories[i].ParentID
			childrenMap[pid] = append(childrenMap[pid], &categories[i])
		}
	}

	// Recursive function to build tree by value from pointers
	var buildTree func(id uuid.UUID) BudgetCategory
	buildTree = func(id uuid.UUID) BudgetCategory {
		cat := *categoryMap[id]
		cat.Subcategories = []BudgetCategory{}
		for _, child := range childrenMap[id] {
			cat.Subcategories = append(cat.Subcategories, buildTree(child.ID))
		}
		return cat
	}

	roots := make([]BudgetCategory, 0, len(rootIDs))
	for _, id := range rootIDs {
		roots = append(roots, buildTree(id))
	}
	return roots
}

// CreateMunicipality creates a new municipality (admin only)
func CreateMunicipality(w http.ResponseWriter, r *http.Request) {
	var municipality Municipality
	if err := json.NewDecoder(r.Body).Decode(&municipality); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if municipality.Name == "" || municipality.State == "" {
		http.Error(w, "Name and state are required", http.StatusBadRequest)
		return
	}

	// Validate entity_type if provided; default to "city" if empty
	if municipality.EntityType == "" {
		municipality.EntityType = "city"
	} else {
		validTypes := map[string]bool{"city": true, "county": true, "township": true}
		if !validTypes[municipality.EntityType] {
			http.Error(w, "entity_type must be one of: city, county, township", http.StatusBadRequest)
			return
		}
	}

	if err := db.DB.Create(&municipality).Error; err != nil {
		http.Error(w, "Failed to create municipality: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(municipality)
}

// UpdateMunicipality updates an existing municipality (admin only)
func UpdateMunicipality(w http.ResponseWriter, r *http.Request) {
	municipalityID := chi.URLParam(r, "municipality_id")

	var municipality Municipality
	if err := db.DB.First(&municipality, "id = ?", municipalityID).Error; err != nil {
		http.Error(w, "Municipality not found", http.StatusNotFound)
		return
	}

	var updates struct {
		Name       *string `json:"name,omitempty"`
		State      *string `json:"state,omitempty"`
		EntityType *string `json:"entity_type,omitempty"`
		Population *int    `json:"population,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	updateMap := make(map[string]interface{})
	if updates.Name != nil {
		updateMap["name"] = *updates.Name
	}
	if updates.State != nil {
		updateMap["state"] = *updates.State
	}
	if updates.EntityType != nil {
		validTypes := map[string]bool{"city": true, "county": true, "township": true}
		if !validTypes[*updates.EntityType] {
			http.Error(w, "entity_type must be one of: city, county, township", http.StatusBadRequest)
			return
		}
		updateMap["entity_type"] = *updates.EntityType
	}
	if updates.Population != nil {
		updateMap["population"] = *updates.Population
	}

	if err := db.DB.Model(&municipality).Updates(updateMap).Error; err != nil {
		http.Error(w, "Failed to update municipality: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Municipality updated successfully")
}

// DeleteMunicipality deletes a municipality and all associated data (admin only)
func DeleteMunicipality(w http.ResponseWriter, r *http.Request) {
	municipalityID := chi.URLParam(r, "municipality_id")

	if err := db.DB.Delete(&Municipality{}, "id = ?", municipalityID).Error; err != nil {
		http.Error(w, "Failed to delete municipality: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// CreateBudget creates a new budget (admin only)
func CreateBudget(w http.ResponseWriter, r *http.Request) {
	var budget Budget
	if err := json.NewDecoder(r.Body).Decode(&budget); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if budget.MunicipalityID == uuid.Nil || budget.FiscalYear == 0 {
		http.Error(w, "municipality_id and fiscal_year are required", http.StatusBadRequest)
		return
	}

	if err := db.DB.Create(&budget).Error; err != nil {
		http.Error(w, "Failed to create budget: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(budget)
}

// ImportBudget imports a full budget from JSON (admin only)
func ImportBudget(w http.ResponseWriter, r *http.Request) {
	var importRequest struct {
		CityName             string           `json:"city_name"`
		CityState            string           `json:"city_state"`
		EntityType           string           `json:"entity_type"`            // optional, defaults to "city"
		Population           int              `json:"population"`
		FiscalYear           int              `json:"fiscal_year"`
		FiscalYearStartMonth int              `json:"fiscal_year_start_month"` // optional, defaults to 1
		DatasetType          string           `json:"dataset_type"`
		TotalBudget          float64          `json:"total_budget"`
		DataSource           string           `json:"data_source"`
		Hierarchy            []string         `json:"hierarchy"`
		Categories           []CategoryImport `json:"categories"`
	}

	if err := json.NewDecoder(r.Body).Decode(&importRequest); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Default entity_type to "city" if not provided
	if importRequest.EntityType == "" {
		importRequest.EntityType = "city"
	}

	// Default fiscal_year_start_month to 1 (January) if not provided
	if importRequest.FiscalYearStartMonth == 0 {
		importRequest.FiscalYearStartMonth = 1
	}

	tx := db.DB.Begin()
	if tx.Error != nil {
		http.Error(w, "Failed to start transaction", http.StatusInternalServerError)
		return
	}

	// Find or create municipality
	var municipality Municipality
	if err := tx.Where("name = ? AND state = ?", importRequest.CityName, importRequest.CityState).First(&municipality).Error; err != nil {
		municipality = Municipality{
			Name:       importRequest.CityName,
			State:      importRequest.CityState,
			EntityType: importRequest.EntityType,
			Population: importRequest.Population,
		}
		if err := tx.Create(&municipality).Error; err != nil {
			tx.Rollback()
			http.Error(w, "Failed to create municipality: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// Check if budget already exists for this municipality/year/dataset
	var existingBudget Budget
	err := tx.Where("municipality_id = ? AND fiscal_year = ? AND dataset_type = ?",
		municipality.ID, importRequest.FiscalYear, importRequest.DatasetType).First(&existingBudget).Error
	if err == nil {
		tx.Rollback()
		http.Error(w, fmt.Sprintf("Budget already exists for %s %d (%s)",
			importRequest.CityName, importRequest.FiscalYear, importRequest.DatasetType), http.StatusConflict)
		return
	}

	// Create budget
	budget := Budget{
		MunicipalityID:       municipality.ID,
		FiscalYear:           importRequest.FiscalYear,
		FiscalYearStartMonth: importRequest.FiscalYearStartMonth,
		DatasetType:          importRequest.DatasetType,
		TotalBudget:          importRequest.TotalBudget,
		DataSource:           importRequest.DataSource,
		Hierarchy:            importRequest.Hierarchy,
	}
	if err := tx.Create(&budget).Error; err != nil {
		tx.Rollback()
		http.Error(w, "Failed to create budget: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Import categories recursively
	if err := importCategories(tx, budget.ID, nil, importRequest.Categories, 0); err != nil {
		tx.Rollback()
		http.Error(w, "Failed to import categories: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if err := tx.Commit().Error; err != nil {
		http.Error(w, "Failed to commit transaction: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("Imported budget for %s %d (%s) with %d root categories",
		importRequest.CityName, importRequest.FiscalYear, importRequest.DatasetType, len(importRequest.Categories))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":          "success",
		"budget_id":       budget.ID,
		"municipality_id": municipality.ID,
	})
}

// CategoryImport represents the structure for importing categories.
// JSON tags accept both camelCase (from pre-built Bloomington JSONs) and snake_case.
// Go's encoding/json only supports one tag, so we use camelCase to match the source files.
type CategoryImport struct {
	Name             string           `json:"name"`
	Amount           float64          `json:"amount"`
	Percentage       float64          `json:"percentage"`
	Color            string           `json:"color"`
	Description      string           `json:"description,omitempty"`
	WhyMatters       string           `json:"whyMatters,omitempty"`
	HistoricalChange *float64         `json:"historicalChange,omitempty"`
	Items            int              `json:"items,omitempty"`
	LinkKey          string           `json:"linkKey,omitempty"`
	Subcategories    []CategoryImport `json:"subcategories,omitempty"`
	LineItems        []LineItemImport `json:"lineItems,omitempty"`
}

// LineItemImport represents the structure for importing line items
type LineItemImport struct {
	Description    string   `json:"description"`
	ApprovedAmount float64  `json:"approvedAmount"`
	ActualAmount   float64  `json:"actualAmount"`
	BasePay        *float64 `json:"basePay,omitempty"`
	Benefits       *float64 `json:"benefits,omitempty"`
	Overtime       *float64 `json:"overtime,omitempty"`
	Other          *float64 `json:"other,omitempty"`
	StartDate      *string  `json:"startDate,omitempty"`
	Vendor         *string  `json:"vendor,omitempty"`
	Date           *string  `json:"date,omitempty"`
	PaymentMethod  *string  `json:"paymentMethod,omitempty"`
	InvoiceNumber  *string  `json:"invoiceNumber,omitempty"`
	Fund           *string  `json:"fund,omitempty"`
}

func importCategories(tx *gorm.DB, budgetID uuid.UUID, parentID *uuid.UUID, categories []CategoryImport, depth int) error {
	for i, cat := range categories {
		category := BudgetCategory{
			BudgetID:         budgetID,
			ParentID:         parentID,
			Name:             cat.Name,
			Amount:           cat.Amount,
			Percentage:       cat.Percentage,
			Color:            cat.Color,
			Description:      cat.Description,
			WhyMatters:       cat.WhyMatters,
			HistoricalChange: cat.HistoricalChange,
			ItemCount:        cat.Items,
			SortOrder:        i,
			Depth:            depth,
			LinkKey:          cat.LinkKey,
		}

		if err := tx.Create(&category).Error; err != nil {
			return err
		}

		// Import line items
		for _, item := range cat.LineItems {
			lineItem := BudgetLineItem{
				CategoryID:     category.ID,
				Description:    item.Description,
				ApprovedAmount: item.ApprovedAmount,
				ActualAmount:   item.ActualAmount,
				BasePay:        item.BasePay,
				Benefits:       item.Benefits,
				Overtime:       item.Overtime,
				Other:          item.Other,
				StartDate:      item.StartDate,
				Vendor:         item.Vendor,
				Date:           item.Date,
				PaymentMethod:  item.PaymentMethod,
				InvoiceNumber:  item.InvoiceNumber,
				Fund:           item.Fund,
			}
			if err := tx.Create(&lineItem).Error; err != nil {
				return err
			}
		}

		// Recursively import subcategories
		if len(cat.Subcategories) > 0 {
			if err := importCategories(tx, budgetID, &category.ID, cat.Subcategories, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

// UpdateBudget updates an existing budget (admin only)
func UpdateBudget(w http.ResponseWriter, r *http.Request) {
	budgetID := chi.URLParam(r, "budget_id")

	var budget Budget
	if err := db.DB.First(&budget, "id = ?", budgetID).Error; err != nil {
		http.Error(w, "Budget not found", http.StatusNotFound)
		return
	}

	var updates struct {
		TotalBudget *float64 `json:"total_budget,omitempty"`
		DataSource  *string  `json:"data_source,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	updateMap := make(map[string]interface{})
	if updates.TotalBudget != nil {
		updateMap["total_budget"] = *updates.TotalBudget
	}
	if updates.DataSource != nil {
		updateMap["data_source"] = *updates.DataSource
	}

	if err := db.DB.Model(&budget).Updates(updateMap).Error; err != nil {
		http.Error(w, "Failed to update budget: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Budget updated successfully")
}

// DeleteBudget deletes a budget and all associated categories (admin only)
func DeleteBudget(w http.ResponseWriter, r *http.Request) {
	budgetID := chi.URLParam(r, "budget_id")

	tx := db.DB.Begin()

	// Delete line items first
	if err := tx.Exec(`
		DELETE FROM treasury.budget_line_items
		WHERE category_id IN (SELECT id FROM treasury.budget_categories WHERE budget_id = ?)
	`, budgetID).Error; err != nil {
		tx.Rollback()
		http.Error(w, "Failed to delete line items: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Delete categories
	if err := tx.Delete(&BudgetCategory{}, "budget_id = ?", budgetID).Error; err != nil {
		tx.Rollback()
		http.Error(w, "Failed to delete categories: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Delete budget
	if err := tx.Delete(&Budget{}, "id = ?", budgetID).Error; err != nil {
		tx.Rollback()
		http.Error(w, "Failed to delete budget: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if err := tx.Commit().Error; err != nil {
		http.Error(w, "Failed to commit: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
