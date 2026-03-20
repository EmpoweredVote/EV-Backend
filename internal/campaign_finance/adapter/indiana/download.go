package indiana

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
	// indianaZIPURLTemplate is the annual contribution data ZIP from the Indiana
	// Campaign Finance portal. %d is the four-digit year (e.g. 2024).
	indianaZIPURLTemplate = "https://campaignfinance.in.gov/PublicSite/Docs/BulkDataDownloads/%d_ContributionData.csv.zip"
	// etagMetadataKeyTemplate is the DataSourceMetadata source_system key for
	// the per-year ETag. Formatted with the year integer before use.
	etagMetadataKeyTemplate = "indiana_zip_etag_%d"
)

func etagKey(year int) string { return fmt.Sprintf(etagMetadataKeyTemplate, year) }

// loadStoredETag reads the previously saved ETag for a given year from
// DataSourceMetadata. Returns empty string if no record exists.
func loadStoredETag(dbConn *gorm.DB, year int) string {
	var meta campaign_finance.DataSourceMetadata
	err := dbConn.Where("source_system = ?", etagKey(year)).First(&meta).Error
	if err != nil {
		return ""
	}
	return meta.Notes
}

// saveETag upserts the ETag value for a given year into DataSourceMetadata.
func saveETag(dbConn *gorm.DB, year int, etag string) {
	now := time.Now()
	meta := campaign_finance.DataSourceMetadata{
		SourceSystem:   etagKey(year),
		LastSyncAt:     &now,
		LastSyncStatus: "ok",
		Notes:          etag,
	}
	dbConn.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "source_system"}},
		DoUpdates: clause.AssignmentColumns([]string{"last_sync_at", "last_sync_status", "notes", "updated_at"}),
	}).Create(&meta)
}

// DownloadZIP fetches the Indiana annual contribution ZIP for the given year,
// using ETag caching to skip re-download when the server returns 304.
//
// Returns:
//   - localPath: temp file path (caller must cleanupZIP when done)
//   - etag: ETag from response (empty if server did not send one)
//   - downloadedAt: timestamp of the request
//   - skipped: true if server returned 304 Not Modified
//   - err: non-nil on network or HTTP error
func DownloadZIP(ctx context.Context, dbConn *gorm.DB, year int) (localPath string, etag string, downloadedAt time.Time, skipped bool, err error) {
	storedETag := loadStoredETag(dbConn, year)
	downloadedAt = time.Now()
	url := fmt.Sprintf(indianaZIPURLTemplate, year)

	body, resultETag, err := doRequest(ctx, url, storedETag)
	if err != nil {
		// Retry without ETag in case of cache-related server error.
		body, resultETag, err = doRequest(ctx, url, "")
		if err != nil {
			return "", "", downloadedAt, false, err
		}
	}
	// 304 Not Modified — body is nil.
	if body == nil {
		return "", storedETag, downloadedAt, true, nil
	}
	defer body.Close()

	if resultETag == "" {
		log.Printf("indiana: DownloadZIP year=%d: server returned no ETag header; next run will re-download", year)
	}

	tmpFile, err := os.CreateTemp("", "indiana_*.zip")
	if err != nil {
		return "", "", downloadedAt, false, err
	}
	if _, err = io.Copy(tmpFile, body); err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		return "", "", downloadedAt, false, err
	}
	tmpFile.Close()
	return tmpFile.Name(), resultETag, downloadedAt, false, nil
}

// doRequest sends a GET request to url with an optional If-None-Match header.
// Returns (nil, "", nil) on 304 Not Modified.
func doRequest(ctx context.Context, url, storedETag string) (io.ReadCloser, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, "", err
	}
	if storedETag != "" {
		req.Header.Set("If-None-Match", storedETag)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, "", err
	}
	switch resp.StatusCode {
	case http.StatusNotModified:
		resp.Body.Close()
		return nil, "", nil
	case http.StatusOK:
		return resp.Body, resp.Header.Get("ETag"), nil
	default:
		resp.Body.Close()
		return nil, "", fmt.Errorf("indiana: unexpected HTTP status %d for %s", resp.StatusCode, url)
	}
}

// cleanupZIP removes the temp file created by DownloadZIP. Safe to call with empty path.
func cleanupZIP(localPath string) {
	if localPath == "" {
		return
	}
	if err := os.Remove(localPath); err != nil && !os.IsNotExist(err) {
		log.Printf("indiana: cleanupZIP: failed to remove %s: %v", localPath, err)
	}
}
