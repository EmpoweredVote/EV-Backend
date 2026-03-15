package essentials

import (
	"fmt"
	"log"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
)

type scotusJustice struct {
	FirstName        string
	MiddleInitial    string
	LastName         string
	NameSuffix       string
	FullName         string
	CourtRole        string // "Chief Justice" or "Associate Justice"
	AppointedBy      string
	PresidentParty   string
	ConfirmationVote string
	AppointmentDate  string // YYYY-MM-DD
	PhotoOriginURL   string // supremecourt.gov URL (backup)
}

var scotusJustices = []scotusJustice{
	{
		FirstName: "John", MiddleInitial: "G.", LastName: "Roberts", NameSuffix: "Jr.",
		FullName: "John G. Roberts Jr.", CourtRole: "Chief Justice",
		AppointedBy: "George W. Bush", PresidentParty: "Republican",
		ConfirmationVote: "78-22", AppointmentDate: "2005-09-29",
		PhotoOriginURL: "https://www.supremecourt.gov/about/biographies/CJRoberts.aspx",
	},
	{
		FirstName: "Clarence", LastName: "Thomas",
		FullName: "Clarence Thomas", CourtRole: "Associate Justice",
		AppointedBy: "George H.W. Bush", PresidentParty: "Republican",
		ConfirmationVote: "52-48", AppointmentDate: "1991-10-23",
		PhotoOriginURL: "https://www.supremecourt.gov/about/biographies/JThomas.aspx",
	},
	{
		FirstName: "Samuel", MiddleInitial: "A.", LastName: "Alito", NameSuffix: "Jr.",
		FullName: "Samuel A. Alito Jr.", CourtRole: "Associate Justice",
		AppointedBy: "George W. Bush", PresidentParty: "Republican",
		ConfirmationVote: "58-42", AppointmentDate: "2006-01-31",
		PhotoOriginURL: "https://www.supremecourt.gov/about/biographies/JAlito.aspx",
	},
	{
		FirstName: "Sonia", LastName: "Sotomayor",
		FullName: "Sonia Sotomayor", CourtRole: "Associate Justice",
		AppointedBy: "Barack Obama", PresidentParty: "Democratic",
		ConfirmationVote: "68-31", AppointmentDate: "2009-08-08",
		PhotoOriginURL: "https://www.supremecourt.gov/about/biographies/JSotomayor.aspx",
	},
	{
		FirstName: "Elena", LastName: "Kagan",
		FullName: "Elena Kagan", CourtRole: "Associate Justice",
		AppointedBy: "Barack Obama", PresidentParty: "Democratic",
		ConfirmationVote: "63-37", AppointmentDate: "2010-08-07",
		PhotoOriginURL: "https://www.supremecourt.gov/about/biographies/JKagan.aspx",
	},
	{
		FirstName: "Neil", MiddleInitial: "M.", LastName: "Gorsuch",
		FullName: "Neil M. Gorsuch", CourtRole: "Associate Justice",
		AppointedBy: "Donald Trump", PresidentParty: "Republican",
		ConfirmationVote: "54-45", AppointmentDate: "2017-04-10",
		PhotoOriginURL: "https://www.supremecourt.gov/about/biographies/JGorsuch.aspx",
	},
	{
		FirstName: "Brett", MiddleInitial: "M.", LastName: "Kavanaugh",
		FullName: "Brett M. Kavanaugh", CourtRole: "Associate Justice",
		AppointedBy: "Donald Trump", PresidentParty: "Republican",
		ConfirmationVote: "50-48", AppointmentDate: "2018-10-06",
		PhotoOriginURL: "https://www.supremecourt.gov/about/biographies/JKavanaugh.aspx",
	},
	{
		FirstName: "Amy", LastName: "Coney Barrett",
		FullName: "Amy Coney Barrett", CourtRole: "Associate Justice",
		AppointedBy: "Donald Trump", PresidentParty: "Republican",
		ConfirmationVote: "52-48", AppointmentDate: "2020-10-27",
		PhotoOriginURL: "https://www.supremecourt.gov/about/biographies/JBarrett.aspx",
	},
	{
		FirstName: "Ketanji", LastName: "Brown Jackson",
		FullName: "Ketanji Brown Jackson", CourtRole: "Associate Justice",
		AppointedBy: "Joe Biden", PresidentParty: "Democratic",
		ConfirmationVote: "53-47", AppointmentDate: "2022-06-30",
		PhotoOriginURL: "https://www.supremecourt.gov/about/biographies/JJackson.aspx",
	},
}

// ImportSCOTUS seeds all 9 current Supreme Court justices into the database.
// Idempotent: safe to re-run. Matches justices by full_name to avoid duplicates.
func ImportSCOTUS(dryRun bool) error {
	log.Println("=== SCOTUS Import ===")

	// 1. Ensure Government record exists
	var gov Government
	if err := db.DB.Where("name = ? AND type = ?", "United States Federal Government", "federal").
		FirstOrCreate(&gov, Government{
			Name:  "United States Federal Government",
			Type:  "federal",
			State: "US",
		}).Error; err != nil {
		return fmt.Errorf("create government: %w", err)
	}
	log.Printf("Government: %s (id=%s)", gov.Name, gov.ID)

	// 2. Ensure Chamber exists (negative ExternalID to avoid conflict with external sources)
	var chamber Chamber
	if err := db.DB.Where("external_id = ?", -100).FirstOrCreate(&chamber, Chamber{
		ExternalID:        -100,
		GovernmentID:      gov.ID,
		Name:              "U.S. Supreme Court",
		NameFormal:        "Supreme Court of the United States",
		OfficialCount:     9,
		ElectionFrequency: "life_tenure",
		VacancyRules:      "Presidential nomination with Senate confirmation",
	}).Error; err != nil {
		return fmt.Errorf("create chamber: %w", err)
	}
	log.Printf("Chamber: %s (id=%s)", chamber.Name, chamber.ID)

	// 3. Ensure District exists
	var district District
	if err := db.DB.Where("external_id = ?", -100).FirstOrCreate(&district, District{
		ExternalID:   -100,
		DistrictType: "NATIONAL_JUDICIAL",
		Label:        "United States",
		State:        "US",
		GeoID:        "US",
		IsJudicial:   true,
		NumOfficials: 9,
	}).Error; err != nil {
		return fmt.Errorf("create district: %w", err)
	}
	log.Printf("District: %s type=%s (id=%s)", district.Label, district.DistrictType, district.ID)

	// 4. Ensure GovernmentBody exists (for display name/URL in queries)
	var govBody GovernmentBody
	if err := db.DB.Where("state = ? AND geo_id = ? AND body_key = ?", "US", "US", "Supreme Court of the United States").
		FirstOrCreate(&govBody, GovernmentBody{
			State:       "US",
			GeoID:       "US",
			BodyKey:     "Supreme Court of the United States",
			DisplayName: "U.S. Supreme Court",
			WebsiteURL:  "https://www.supremecourt.gov",
		}).Error; err != nil {
		return fmt.Errorf("create government body: %w", err)
	}
	log.Printf("GovernmentBody: %s (id=%s)", govBody.DisplayName, govBody.ID)

	if dryRun {
		log.Println("[DRY RUN] Would create/update justices:")
		for _, j := range scotusJustices {
			log.Printf("  - %s (%s)", j.FullName, j.CourtRole)
		}
		return nil
	}

	// 5. Upsert each justice
	created, updated := 0, 0
	for i, j := range scotusJustices {
		externalID := -200 - i // -200, -201, ... -208

		// Find or create Politician
		var pol Politician
		result := db.DB.Where("full_name = ? AND source = ?", j.FullName, "manual_scotus").First(&pol)
		isNew := result.Error != nil

		if isNew {
			pol = Politician{
				ExternalID:     externalID,
				FirstName:      j.FirstName,
				MiddleInitial:  j.MiddleInitial,
				LastName:       j.LastName,
				NameSuffix:     j.NameSuffix,
				FullName:       j.FullName,
				Party:          "Nonpartisan",
				IsAppointed:    true,
				IsActive:       true,
				IsIncumbent:    true,
				Source:         "manual_scotus",
				DataSource:     "manual",
				PhotoOriginURL: j.PhotoOriginURL,
			}
			if j.AppointmentDate != "" {
				pol.AppointmentDate = &j.AppointmentDate
			}
			if err := db.DB.Create(&pol).Error; err != nil {
				return fmt.Errorf("create politician %s: %w", j.FullName, err)
			}
			created++
		} else {
			// Update existing record
			updates := map[string]interface{}{
				"is_active":        true,
				"is_incumbent":     true,
				"photo_origin_url": j.PhotoOriginURL,
			}
			if j.AppointmentDate != "" {
				updates["appointment_date"] = j.AppointmentDate
			}
			db.DB.Model(&pol).Updates(updates)
			updated++
		}

		// Upsert Office
		var office Office
		polID := pol.ID
		if err := db.DB.Where("politician_id = ? AND chamber_id = ?", pol.ID, chamber.ID).
			FirstOrCreate(&office, Office{
				PoliticianID:           &polID,
				ChamberID:              chamber.ID,
				DistrictID:             district.ID,
				Title:                  j.CourtRole,
				RepresentingState:      "US",
				Seats:                  1,
				NormalizedPositionName: j.CourtRole,
				IsAppointedPosition:    true,
			}).Error; err != nil {
			return fmt.Errorf("create office for %s: %w", j.FullName, err)
		}

		// Update politician's OfficeID
		db.DB.Model(&pol).Update("office_id", office.ID)

		// Upsert JudgeDetail
		var jd JudgeDetail
		if err := db.DB.Where("politician_id = ?", pol.ID).FirstOrCreate(&jd, JudgeDetail{
			PoliticianID:             pol.ID,
			AppointedBy:              j.AppointedBy,
			AppointingPresidentParty: j.PresidentParty,
			ConfirmationVote:         j.ConfirmationVote,
			CourtRole:                j.CourtRole,
		}).Error; err != nil {
			return fmt.Errorf("create judge detail for %s: %w", j.FullName, err)
		}

		// Update JudgeDetail if it already existed (in case data changed)
		db.DB.Model(&jd).Updates(map[string]interface{}{
			"appointed_by":               j.AppointedBy,
			"appointing_president_party": j.PresidentParty,
			"confirmation_vote":          j.ConfirmationVote,
			"court_role":                 j.CourtRole,
		})

		// Upsert PoliticianImage — CDN URL follows the standard pattern:
		// https://<project>.supabase.co/storage/v1/object/public/politician_photos/<uuid>/default.jpg
		// Photos must be uploaded to Supabase Storage separately (public domain US govt works).
		cdnURL := fmt.Sprintf("https://kxsdzaojfaibhuzmclfq.supabase.co/storage/v1/object/public/politician_photos/%s/default.jpg", pol.ID)
		var img PoliticianImage
		db.DB.Where("politician_id = ? AND type = ?", pol.ID, "default").
			FirstOrCreate(&img, PoliticianImage{
				PoliticianID: pol.ID,
				URL:          cdnURL,
				Type:         "default",
				PhotoLicense: "us_government_work",
			})

		log.Printf("  [%d/9] %s %s — %s", i+1,
			map[bool]string{true: "CREATED", false: "UPDATED"}[isNew],
			j.FullName, j.CourtRole)
	}

	log.Printf("=== SCOTUS Import Complete: %d created, %d updated ===", created, updated)
	return nil
}
