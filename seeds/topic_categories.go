package seeds

import (
	"fmt"
	"log"

	"github.com/DoyleJ11/auth-system/internal/compass"
	"github.com/DoyleJ11/auth-system/internal/db"
)

var topicToCategories = map[string][]string{
	"Affordable Housing and Homelessness": {"Economic Policy and Labor"},
	"American Tariff Policy": {"Economic Policy and Labor"},
	"Artificial Intelligence and Job Displacement": {"Technology, Data, and Innovation", "Economic Policy and Labor"},
	"Book Bans and Curriculum Restrictions in Schools": {"Governance, Democracy, and Institutional Reform", "Education and Youth Policy"},
	"Campaign Finance Reform": {"Governance, Democracy, and Institutional Reform"},
	"Civil Rights and Social Justice": {"Civil Rights and Social Policy"},
	"Climate Change and Environmental Protection": {"Environment and Climate Policy"},
	"Corporate Regulation and Consumer Protection": {"Economic Policy and Labor"},
	"Criminal Justice Reform and Policing": {"Civil Rights and Social Policy", "Public Safety and Law Enforcement"},
	"DEI (Diversity, Equity, Inclusion) Programs and Workplace Policy": {"Civil Rights and Social Policy"},
	"Defense Treaties and NATO Commitments": {"Foreign Policy and National Security"},
	"Economic Inequality and Job Security": {"Economic Policy and Labor"},
	"Education Quality and Funding": {"Education and Youth Policy"},
	"Food Security and Agricultural Policy": {"Environment and Climate Policy"},
	"Foreign Aid and International Development": {"Foreign Policy and National Security"},
	"Foreign Policy and National Security": {"Foreign Policy and National Security"},
	"Gender Identity and Trans Rights": {"Civil Rights and Social Policy"},
	"Global Refugee and Asylum Policies": {"Foreign Policy and National Security"},
	"Government Role in Childcare and Parental Leave": {"Healthcare and Social Safety Nets"},
	"Gun Laws and Public Safety": {"Public Safety and Law Enforcement"},
	"Healthcare Access and Affordability": {"Healthcare and Social Safety Nets"},
	"Healthcare for Veterans and Military Support": {"Healthcare and Social Safety Nets"},
	"Immigration Policy": {"Civil Rights and Social Policy", "Foreign Policy and National Security"},
	"Infrastructure and Transportation": {"Economic Policy and Labor"},
	"Internet Access and Digital Equity": {"Technology, Data, and Innovation"},
	"Israel-Palestine Conflict / War in Gaza": {"Foreign Policy and National Security"},
	"January 6th and the 2020 Election": {"Governance, Democracy, and Institutional Reform"},
	"Mental Health and Addiction Services": {"Healthcare and Social Safety Nets", "Civil Rights and Social Policy"},
	"Misinformation and the Role of Algorithms in Democracy": {"Governance, Democracy, and Institutional Reform", "Technology, Data, and Innovation"},
	"National Debt and Deficit Spending": {"Economic Policy and Labor"},
	"Natural Disasters and Climate Resilience Funding": {"Environment and Climate Policy"},
	"Online Censorship and Platform Accountability": {"Technology, Data, and Innovation", "Governance, Democracy, and Institutional Reform"},
	"Pandemic Preparedness and Vaccine Mandates": {"Healthcare and Social Safety Nets"},
	"Policing and Criminal Justice Reform": {"Civil Rights and Social Policy", "Public Safety and Law Enforcement"},
	"Policing in Schools": {"Civil Rights and Social Policy", "Education and Youth Policy", "Public Safety and Law Enforcement"},
	"Reproductive Rights and Abortion Access": {"Civil Rights and Social Policy"},
	"Religious Freedom and the Role of Religion in Government": {"Civil Rights and Social Policy", "Governance, Democracy, and Institutional Reform"},
	"Space Exploration and Federal Investment": {"Technology, Data, and Innovation"},
	"Supply Chain Resilience and Domestic Manufacturing": {"Economic Policy and Labor"},
	"Support for Small Businesses and Entrepreneurship": {"Economic Policy and Labor"},
	"Taxation and Government Spending": {"Economic Policy and Labor"},
	"Technology and Data Privacy": {"Technology, Data, and Innovation"},
	"Term Limits and Congressional Reform": {"Governance, Democracy, and Institutional Reform"},
	"TikTok and Foreign-Owned Social Media Regulation": {"Technology, Data, and Innovation"},
	"U.S. Military Support to Ukraine": {"Foreign Policy and National Security"},
	"U.S.-China Relations and Taiwan": {"Foreign Policy and National Security"},
	"Universal Basic Income (UBI)": {"Economic Policy and Labor"},
	"Voting Rights and Electoral Integrity": {"Governance, Democracy, and Institutional Reform"},
	"Wages and Labor Rights": {"Economic Policy and Labor"},
	"Water Access and Drought Management": {"Environment and Climate Policy"},
}


func SeedTopicCategories() error {
	var topics []compass.Topic
	var categories []compass.Category

	if err := db.DB.Find(&topics).Error; err != nil {
		return fmt.Errorf("failed to fetch topics: %w", err)
	}
	if err := db.DB.Find(&categories).Error; err != nil {
		return fmt.Errorf("failed to fetch categories: %w", err)
	}

	// Build quick lookup maps
	topicMap := make(map[string]*compass.Topic)
	for i := range topics {
		topicMap[topics[i].Title] = &topics[i]
	}

	categoryMap := make(map[string]*compass.Category)
	for i := range categories {
		categoryMap[categories[i].Title] = &categories[i]
	}

	// Assign categories to topics
	for topicTitle, categoryTitles := range topicToCategories {
		topic, ok := topicMap[topicTitle]
		if !ok {
			log.Printf("⚠️ Topic not found: %s", topicTitle)
			continue
		}

		var cats []*compass.Category
		for _, catTitle := range categoryTitles {
			cat, ok := categoryMap[catTitle]
			if !ok {
				log.Printf("⚠️ Category not found: %s", catTitle)
				continue
			}
			cats = append(cats, cat)
		}

		err := db.DB.Model(topic).Association("Categories").Replace(cats)
		if err != nil {
			log.Printf("❌ Failed assigning categories to %s: %v", topicTitle, err)
		} else {
			log.Printf("✅ Assigned categories to %s", topicTitle)
		}
	}

	return nil
}