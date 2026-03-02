package campaign_finance

import (
	_ "embed"
	"html/template"
	"net/http"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/EmpoweredVote/EV-Backend/internal/utils"
	"github.com/google/uuid"
)

//go:embed templates/sources_form.html
var sourcesFormHTML string

var sourcesFormTmpl = template.Must(template.New("sources_form").Parse(sourcesFormHTML))

// SourcesFormData is the template context passed to sources_form.html.
type SourcesFormData struct {
	Sources    []PoliticianSource
	EditRecord *PoliticianSource
	Flash      string
}

// SourcesFormHandler handles GET /campaign-finance/admin/sources/form
func SourcesFormHandler(w http.ResponseWriter, r *http.Request) {
	data := SourcesFormData{}

	// Flash message from redirect query param
	data.Flash = r.URL.Query().Get("msg")

	// Build query with optional filters
	query := db.DB.Model(&PoliticianSource{})
	if ss := r.URL.Query().Get("source_system"); ss != "" {
		query = query.Where("source_system = ?", ss)
	}
	if rs := r.URL.Query().Get("research_status"); rs != "" {
		query = query.Where("research_status = ?", rs)
	}
	if err := query.Order("created_at DESC").Find(&data.Sources).Error; err != nil {
		http.Error(w, "Failed to load sources", http.StatusInternalServerError)
		return
	}

	// Pre-fill edit form if ?edit=UUID present
	if editID := r.URL.Query().Get("edit"); editID != "" {
		id, err := uuid.Parse(editID)
		if err == nil {
			var rec PoliticianSource
			if dbErr := db.DB.First(&rec, "id = ?", id).Error; dbErr == nil {
				data.EditRecord = &rec
			}
		}
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := sourcesFormTmpl.Execute(w, data); err != nil {
		http.Error(w, "Template render error", http.StatusInternalServerError)
	}
}

// SourcesFormSubmitHandler handles POST /campaign-finance/admin/sources/form
// Creates a new source or updates an existing one based on the hidden id field.
func SourcesFormSubmitHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}

	idStr := r.FormValue("id")
	politicianIDStr := r.FormValue("essentials_politician_id")
	sourceSystem := r.FormValue("source_system")
	externalID := r.FormValue("external_id")
	researchStatus := r.FormValue("research_status")
	notes := r.FormValue("notes")

	if politicianIDStr == "" || sourceSystem == "" {
		http.Redirect(w, r, "/campaign-finance/admin/sources/form?msg=essentials_politician_id+and+source_system+are+required", http.StatusSeeOther)
		return
	}

	politicianID, err := uuid.Parse(politicianIDStr)
	if err != nil {
		http.Redirect(w, r, "/campaign-finance/admin/sources/form?msg=Invalid+politician+UUID", http.StatusSeeOther)
		return
	}

	if researchStatus == "" {
		researchStatus = "needs_research"
	}

	userIDStr, _ := utils.GetUserIDFromContext(r.Context())

	// Update if id provided and non-empty, else create
	if idStr != "" {
		id, err := uuid.Parse(idStr)
		if err != nil {
			http.Redirect(w, r, "/campaign-finance/admin/sources/form?msg=Invalid+source+UUID", http.StatusSeeOther)
			return
		}

		var existing PoliticianSource
		if dbErr := db.DB.First(&existing, "id = ?", id).Error; dbErr != nil {
			http.Redirect(w, r, "/campaign-finance/admin/sources/form?msg=Source+not+found", http.StatusSeeOther)
			return
		}
		oldRecord := existing

		existing.EssentialsPoliticianID = politicianID
		existing.SourceSystem = sourceSystem
		existing.ExternalID = externalID
		existing.ResearchStatus = researchStatus
		existing.Notes = notes

		if dbErr := db.DB.Save(&existing).Error; dbErr != nil {
			http.Redirect(w, r, "/campaign-finance/admin/sources/form?msg=Failed+to+update+source", http.StatusSeeOther)
			return
		}

		writeAuditLog(existing.ID, userIDStr, "UPDATE", oldRecord, existing)
		http.Redirect(w, r, "/campaign-finance/admin/sources/form?msg=Source+updated+successfully", http.StatusSeeOther)
		return
	}

	// Create new
	newSource := PoliticianSource{
		ID:                     uuid.New(),
		EssentialsPoliticianID: politicianID,
		SourceSystem:           sourceSystem,
		ExternalID:             externalID,
		ResearchStatus:         researchStatus,
		Notes:                  notes,
	}

	if dbErr := db.DB.Create(&newSource).Error; dbErr != nil {
		http.Redirect(w, r, "/campaign-finance/admin/sources/form?msg=Failed+to+create+source", http.StatusSeeOther)
		return
	}

	writeAuditLog(newSource.ID, userIDStr, "CREATE", nil, newSource)
	http.Redirect(w, r, "/campaign-finance/admin/sources/form?msg=Source+created+successfully", http.StatusSeeOther)
}

// SourcesFormDeleteHandler handles POST /campaign-finance/admin/sources/form/delete
func SourcesFormDeleteHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}

	idStr := r.FormValue("id")
	if idStr == "" {
		http.Redirect(w, r, "/campaign-finance/admin/sources/form?msg=No+ID+provided", http.StatusSeeOther)
		return
	}

	id, err := uuid.Parse(idStr)
	if err != nil {
		http.Redirect(w, r, "/campaign-finance/admin/sources/form?msg=Invalid+source+UUID", http.StatusSeeOther)
		return
	}

	var existing PoliticianSource
	if dbErr := db.DB.First(&existing, "id = ?", id).Error; dbErr != nil {
		http.Redirect(w, r, "/campaign-finance/admin/sources/form?msg=Source+not+found", http.StatusSeeOther)
		return
	}

	if dbErr := db.DB.Delete(&existing).Error; dbErr != nil {
		http.Redirect(w, r, "/campaign-finance/admin/sources/form?msg=Failed+to+delete+source", http.StatusSeeOther)
		return
	}

	userIDStr, _ := utils.GetUserIDFromContext(r.Context())
	writeAuditLog(existing.ID, userIDStr, "DELETE", existing, nil)

	http.Redirect(w, r, "/campaign-finance/admin/sources/form?msg=Source+deleted+successfully", http.StatusSeeOther)
}
