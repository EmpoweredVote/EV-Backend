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

// ListCities returns all cities with budget data
func ListCities(w http.ResponseWriter, r *http.Request) {
	var cities []City

	if err := db.DB.Find(&cities).Error; err != nil {
		http.Error(w, "Failed to fetch cities: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(cities)
}

// GetCity returns a single city by ID
func GetCity(w http.ResponseWriter, r *http.Request) {
	cityID := chi.URLParam(r, "city_id")

	var city City
	if err := db.DB.Preload("Budgets").First(&city, "id = ?", cityID).Error; err != nil {
		http.Error(w, "City not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(city)
}

// ListBudgets returns budgets with optional filtering by city and year
func ListBudgets(w http.ResponseWriter, r *http.Request) {
	query := db.DB.Model(&Budget{}).Preload("City")

	// Filter by city name or ID
	if cityName := r.URL.Query().Get("city"); cityName != "" {
		var city City
		if err := db.DB.First(&city, "name = ?", cityName).Error; err != nil {
			http.Error(w, "City not found", http.StatusNotFound)
			return
		}
		query = query.Where("city_id = ?", city.ID)
	} else if cityID := r.URL.Query().Get("city_id"); cityID != "" {
		query = query.Where("city_id = ?", cityID)
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

// GetBudget returns a single budget with its city
func GetBudget(w http.ResponseWriter, r *http.Request) {
	budgetID := chi.URLParam(r, "budget_id")

	var budget Budget
	if err := db.DB.Preload("City").First(&budget, "id = ?", budgetID).Error; err != nil {
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

// buildCategoryTree converts a flat list of categories into a nested tree
func buildCategoryTree(categories []BudgetCategory) []BudgetCategory {
	// Create a map for quick lookup
	categoryMap := make(map[uuid.UUID]*BudgetCategory)
	var roots []BudgetCategory

	// First pass: populate the map
	for i := range categories {
		categories[i].Subcategories = []BudgetCategory{}
		categoryMap[categories[i].ID] = &categories[i]
	}

	// Second pass: build the tree
	for i := range categories {
		cat := &categories[i]
		if cat.ParentID == nil {
			roots = append(roots, *cat)
		} else {
			if parent, ok := categoryMap[*cat.ParentID]; ok {
				parent.Subcategories = append(parent.Subcategories, *cat)
			}
		}
	}

	// Update roots with their children from the map
	for i := range roots {
		if mapped, ok := categoryMap[roots[i].ID]; ok {
			roots[i].Subcategories = mapped.Subcategories
		}
	}

	return roots
}

// CreateCity creates a new city (admin only)
func CreateCity(w http.ResponseWriter, r *http.Request) {
	var city City
	if err := json.NewDecoder(r.Body).Decode(&city); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if city.Name == "" || city.State == "" {
		http.Error(w, "Name and state are required", http.StatusBadRequest)
		return
	}

	if err := db.DB.Create(&city).Error; err != nil {
		http.Error(w, "Failed to create city: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(city)
}

// UpdateCity updates an existing city (admin only)
func UpdateCity(w http.ResponseWriter, r *http.Request) {
	cityID := chi.URLParam(r, "city_id")

	var city City
	if err := db.DB.First(&city, "id = ?", cityID).Error; err != nil {
		http.Error(w, "City not found", http.StatusNotFound)
		return
	}

	var updates struct {
		Name       *string `json:"name,omitempty"`
		State      *string `json:"state,omitempty"`
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
	if updates.Population != nil {
		updateMap["population"] = *updates.Population
	}

	if err := db.DB.Model(&city).Updates(updateMap).Error; err != nil {
		http.Error(w, "Failed to update city: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "City updated successfully")
}

// DeleteCity deletes a city and all associated data (admin only)
func DeleteCity(w http.ResponseWriter, r *http.Request) {
	cityID := chi.URLParam(r, "city_id")

	if err := db.DB.Delete(&City{}, "id = ?", cityID).Error; err != nil {
		http.Error(w, "Failed to delete city: "+err.Error(), http.StatusInternalServerError)
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

	if budget.CityID == uuid.Nil || budget.FiscalYear == 0 {
		http.Error(w, "city_id and fiscal_year are required", http.StatusBadRequest)
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
		CityName    string           `json:"city_name"`
		CityState   string           `json:"city_state"`
		Population  int              `json:"population"`
		FiscalYear  int              `json:"fiscal_year"`
		DatasetType string           `json:"dataset_type"`
		TotalBudget float64          `json:"total_budget"`
		DataSource  string           `json:"data_source"`
		Hierarchy   []string         `json:"hierarchy"`
		Categories  []CategoryImport `json:"categories"`
	}

	if err := json.NewDecoder(r.Body).Decode(&importRequest); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	tx := db.DB.Begin()
	if tx.Error != nil {
		http.Error(w, "Failed to start transaction", http.StatusInternalServerError)
		return
	}

	// Find or create city
	var city City
	if err := tx.Where("name = ? AND state = ?", importRequest.CityName, importRequest.CityState).First(&city).Error; err != nil {
		city = City{
			Name:       importRequest.CityName,
			State:      importRequest.CityState,
			Population: importRequest.Population,
		}
		if err := tx.Create(&city).Error; err != nil {
			tx.Rollback()
			http.Error(w, "Failed to create city: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// Check if budget already exists for this city/year/dataset
	var existingBudget Budget
	err := tx.Where("city_id = ? AND fiscal_year = ? AND dataset_type = ?",
		city.ID, importRequest.FiscalYear, importRequest.DatasetType).First(&existingBudget).Error
	if err == nil {
		tx.Rollback()
		http.Error(w, fmt.Sprintf("Budget already exists for %s %d (%s)",
			importRequest.CityName, importRequest.FiscalYear, importRequest.DatasetType), http.StatusConflict)
		return
	}

	// Create budget
	budget := Budget{
		CityID:      city.ID,
		FiscalYear:  importRequest.FiscalYear,
		DatasetType: importRequest.DatasetType,
		TotalBudget: importRequest.TotalBudget,
		DataSource:  importRequest.DataSource,
		Hierarchy:   importRequest.Hierarchy,
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
		"status":    "success",
		"budget_id": budget.ID,
		"city_id":   city.ID,
	})
}

// CategoryImport represents the structure for importing categories
type CategoryImport struct {
	Name             string           `json:"name"`
	Amount           float64          `json:"amount"`
	Percentage       float64          `json:"percentage"`
	Color            string           `json:"color"`
	Description      string           `json:"description,omitempty"`
	WhyMatters       string           `json:"why_matters,omitempty"`
	HistoricalChange *float64         `json:"historical_change,omitempty"`
	Items            int              `json:"items,omitempty"`
	LinkKey          string           `json:"link_key,omitempty"`
	Subcategories    []CategoryImport `json:"subcategories,omitempty"`
	LineItems        []LineItemImport `json:"line_items,omitempty"`
}

// LineItemImport represents the structure for importing line items
type LineItemImport struct {
	Description    string   `json:"description"`
	ApprovedAmount float64  `json:"approved_amount"`
	ActualAmount   float64  `json:"actual_amount"`
	BasePay        *float64 `json:"base_pay,omitempty"`
	Benefits       *float64 `json:"benefits,omitempty"`
	Overtime       *float64 `json:"overtime,omitempty"`
	Other          *float64 `json:"other,omitempty"`
	StartDate      *string  `json:"start_date,omitempty"`
	Vendor         *string  `json:"vendor,omitempty"`
	Date           *string  `json:"date,omitempty"`
	PaymentMethod  *string  `json:"payment_method,omitempty"`
	InvoiceNumber  *string  `json:"invoice_number,omitempty"`
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
