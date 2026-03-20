package calaccess

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/EmpoweredVote/EV-Backend/internal/campaign_finance"
)

const (
	calAccessZIPURL      = "https://campaignfinance.cdn.sos.ca.gov/dbwebexport.zip"
	etagMetadataKey      = "cal_access_zip_etag"
	totalRowsMetadataKey = "cal_access_last_total_rows"
)

// loadStoredETag fetches the previously saved ETag from DataSourceMetadata.
// Returns "" if no record is found.
func loadStoredETag(dbConn *gorm.DB) string {
	var meta campaign_finance.DataSourceMetadata
	err := dbConn.Where("source_system = ?", etagMetadataKey).First(&meta).Error
	if err != nil {
		return ""
	}
	return meta.Notes
}

// saveETag upserts the ETag string into DataSourceMetadata for cal_access_zip_etag.
func saveETag(dbConn *gorm.DB, etag string) {
	now := time.Now()
	meta := campaign_finance.DataSourceMetadata{
		SourceSystem:   etagMetadataKey,
		LastSyncAt:     &now,
		LastSyncStatus: "ok",
		Notes:          etag,
	}
	dbConn.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "source_system"}},
		DoUpdates: clause.AssignmentColumns([]string{"last_sync_at", "last_sync_status", "notes", "updated_at"}),
	}).Create(&meta)
}

// saveTotalRows upserts the total row count into DataSourceMetadata for cal_access_last_total_rows.
func saveTotalRows(dbConn *gorm.DB, totalRows int) {
	now := time.Now()
	meta := campaign_finance.DataSourceMetadata{
		SourceSystem:   totalRowsMetadataKey,
		LastSyncAt:     &now,
		LastSyncStatus: "ok",
		Notes:          fmt.Sprintf("%d", totalRows),
	}
	dbConn.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "source_system"}},
		DoUpdates: clause.AssignmentColumns([]string{"last_sync_at", "last_sync_status", "notes", "updated_at"}),
	}).Create(&meta)
}

// DownloadZIP downloads the Cal-Access bulk ZIP using conditional GET (If-None-Match).
//
// Returns:
//   - localPath: path to a temp file containing the ZIP (caller must call cleanupZIP)
//   - etag: the ETag received from the server (may be "" if server did not send one)
//   - downloadedAt: time.Now() captured just before the HTTP request
//   - skipped: true if server returned 304 Not Modified
//   - err: non-nil on network or I/O failure
func DownloadZIP(ctx context.Context, dbConn *gorm.DB) (localPath string, etag string, downloadedAt time.Time, skipped bool, err error) {
	storedETag := loadStoredETag(dbConn)
	downloadedAt = time.Now()

	result, resultETag, err := doRequest(ctx, storedETag)
	if err != nil {
		// Retry once without If-None-Match on network error.
		log.Printf("calaccess: DownloadZIP initial request failed (%v), retrying without ETag", err)
		result, resultETag, err = doRequest(ctx, "")
		if err != nil {
			return "", "", downloadedAt, false, fmt.Errorf("calaccess: DownloadZIP retry failed: %w", err)
		}
	}

	if result == nil {
		// 304 Not Modified.
		return "", storedETag, downloadedAt, true, nil
	}
	defer result.Close()

	if resultETag == "" {
		log.Printf("calaccess: DownloadZIP: server did not return ETag header — skipping ETag save")
	}

	// Stream body to a temp file.
	tmpFile, err := os.CreateTemp("", "cal_access_*.zip")
	if err != nil {
		return "", "", downloadedAt, false, fmt.Errorf("calaccess: DownloadZIP: create temp file: %w", err)
	}

	if _, err = io.Copy(tmpFile, result); err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		return "", "", downloadedAt, false, fmt.Errorf("calaccess: DownloadZIP: stream to temp file: %w", err)
	}
	if err = tmpFile.Close(); err != nil {
		os.Remove(tmpFile.Name())
		return "", "", downloadedAt, false, fmt.Errorf("calaccess: DownloadZIP: close temp file: %w", err)
	}

	return tmpFile.Name(), resultETag, downloadedAt, false, nil
}

// doRequest executes a single HTTP GET for the Cal-Access ZIP.
// If storedETag is non-empty the If-None-Match header is set.
// Returns (nil, "", nil) on 304. Returns (body io.ReadCloser, etag, nil) on 200.
func doRequest(ctx context.Context, storedETag string) (io.ReadCloser, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, calAccessZIPURL, nil)
	if err != nil {
		return nil, "", fmt.Errorf("calaccess: build request: %w", err)
	}

	if storedETag != "" {
		req.Header.Set("If-None-Match", storedETag)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("calaccess: HTTP request: %w", err)
	}

	switch resp.StatusCode {
	case http.StatusNotModified: // 304
		resp.Body.Close()
		return nil, "", nil
	case http.StatusOK: // 200
		return resp.Body, resp.Header.Get("ETag"), nil
	default:
		resp.Body.Close()
		return nil, "", fmt.Errorf("calaccess: unexpected HTTP status %d", resp.StatusCode)
	}
}

// cleanupZIP removes the temp ZIP file downloaded by DownloadZIP.
// Safe to call with an empty path (no-op).
func cleanupZIP(localPath string) {
	if localPath == "" {
		return
	}
	if err := os.Remove(localPath); err != nil && !os.IsNotExist(err) {
		log.Printf("calaccess: cleanupZIP: failed to remove %s: %v", localPath, err)
	}
}
