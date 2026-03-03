package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

type imageRecord struct {
	ID           string
	PoliticianID string
	URL          string
	Type         string // "default", "thumb"
}

func main() {
	databaseURL := os.Getenv("DATABASE_URL")
	supabaseURL := os.Getenv("SUPABASE_URL")
	// Use the anon JWT key for storage uploads (with permissive storage policies)
	anonKey := os.Getenv("SUPABASE_ANON_KEY")

	if databaseURL == "" || supabaseURL == "" || anonKey == "" {
		log.Fatal("Required env vars: DATABASE_URL, SUPABASE_URL, SUPABASE_ANON_KEY")
	}

	bucket := "politician_photos"

	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Database ping failed: %v", err)
	}

	// Fetch all image records
	rows, err := db.Query(`
		SELECT pi.id, pi.politician_id::text, pi.url, pi.type
		FROM essentials.politician_images pi
		ORDER BY pi.politician_id, pi.type
	`)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	var images []imageRecord
	for rows.Next() {
		var img imageRecord
		if err := rows.Scan(&img.ID, &img.PoliticianID, &img.URL, &img.Type); err != nil {
			log.Fatalf("Scan failed: %v", err)
		}
		images = append(images, img)
	}

	log.Printf("Found %d image records to migrate", len(images))

	newBaseURL := fmt.Sprintf("%s/storage/v1/object/public/%s", supabaseURL, bucket)
	uploadBaseURL := fmt.Sprintf("%s/storage/v1/object/%s", supabaseURL, bucket)

	client := &http.Client{Timeout: 30 * time.Second}

	var migrated, skipped, failed int

	for i, img := range images {
		// Skip images already in the new bucket
		if strings.Contains(img.URL, "kxsdzaojfaibhuzmclfq.supabase.co") &&
			strings.Contains(img.URL, bucket) {
			skipped++
			continue
		}

		// Determine file extension from URL
		ext := fileExtension(img.URL)
		if ext == "" {
			ext = ".jpg" // default
		}

		// Build storage path: {politician_id}/{type}{ext}
		storagePath := fmt.Sprintf("%s/%s%s", img.PoliticianID, img.Type, ext)

		log.Printf("[%d/%d] Downloading %s ...", i+1, len(images), truncateURL(img.URL, 80))

		// Download the image
		data, contentType, err := downloadImage(client, img.URL)
		if err != nil {
			log.Printf("  FAILED to download: %v", err)
			failed++
			continue
		}

		if len(data) == 0 {
			log.Printf("  FAILED: empty response")
			failed++
			continue
		}

		// If we got a better content type from the response, use it
		if contentType != "" && ext == ".jpg" {
			if betterExt := extFromContentType(contentType); betterExt != "" {
				ext = betterExt
				storagePath = fmt.Sprintf("%s/%s%s", img.PoliticianID, img.Type, ext)
			}
		}

		// Upload to Supabase storage
		uploadURL := fmt.Sprintf("%s/%s", uploadBaseURL, storagePath)
		if contentType == "" {
			contentType = "image/jpeg"
		}

		err = uploadToStorage(client, uploadURL, anonKey, data, contentType)
		if err != nil {
			log.Printf("  FAILED to upload: %v", err)
			failed++
			continue
		}

		// Build new public URL
		newURL := fmt.Sprintf("%s/%s", newBaseURL, storagePath)

		// Update database record
		_, err = db.Exec(`UPDATE essentials.politician_images SET url = $1 WHERE id = $2`, newURL, img.ID)
		if err != nil {
			log.Printf("  FAILED to update DB: %v", err)
			failed++
			continue
		}

		log.Printf("  OK -> %s (%d bytes)", storagePath, len(data))
		migrated++

		// Rate limit to avoid hammering source servers
		time.Sleep(100 * time.Millisecond)
	}

	log.Printf("\n=== Migration Complete ===")
	log.Printf("Migrated: %d", migrated)
	log.Printf("Skipped (already migrated): %d", skipped)
	log.Printf("Failed: %d", failed)
	log.Printf("Total: %d", len(images))
}

func downloadImage(client *http.Client, url string) ([]byte, string, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("User-Agent", "EmpoweredVote-PhotoMigrator/1.0")

	resp, err := client.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, "", fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", err
	}

	return data, resp.Header.Get("Content-Type"), nil
}

func uploadToStorage(client *http.Client, url, anonKey string, data []byte, contentType string) error {
	req, err := http.NewRequest("POST", url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("apikey", anonKey)
	req.Header.Set("Authorization", "Bearer "+anonKey)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("x-upsert", "true") // overwrite if exists

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

func fileExtension(url string) string {
	// Strip query params
	u := url
	if idx := strings.Index(u, "?"); idx != -1 {
		u = u[:idx]
	}
	ext := path.Ext(u)
	if ext == ".jpg" || ext == ".jpeg" || ext == ".png" || ext == ".webp" || ext == ".gif" {
		return ext
	}
	return ""
}

func extFromContentType(ct string) string {
	ct = strings.Split(ct, ";")[0]
	ct = strings.TrimSpace(ct)
	exts, _ := mime.ExtensionsByType(ct)
	if len(exts) > 0 {
		for _, e := range exts {
			if e == ".jpg" || e == ".jpeg" || e == ".png" || e == ".webp" {
				return e
			}
		}
		return exts[0]
	}
	return ""
}

func truncateURL(url string, maxLen int) string {
	if len(url) <= maxLen {
		return url
	}
	return url[:maxLen] + "..."
}
