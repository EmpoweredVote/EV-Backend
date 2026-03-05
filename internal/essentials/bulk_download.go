package essentials

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

// defaultDataDir returns the default cache directory for bulk downloads.
func defaultDataDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		home = "."
	}
	return filepath.Join(home, ".ev-backend", "bulk-data")
}

// ensureDir creates directory path if it doesn't exist.
func ensureDir(dir string) error {
	return os.MkdirAll(dir, 0o755)
}

// downloadFile downloads a URL to a local path. Uses If-Modified-Since for
// incremental updates — returns (true, nil) if downloaded, (false, nil) if
// not modified, or (false, err) on failure.
func downloadFile(url, destPath string) (downloaded bool, err error) {
	if err := ensureDir(filepath.Dir(destPath)); err != nil {
		return false, fmt.Errorf("creating directory for %s: %w", destPath, err)
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return false, fmt.Errorf("creating request for %s: %w", url, err)
	}

	// If file already exists, use If-Modified-Since
	if info, statErr := os.Stat(destPath); statErr == nil {
		req.Header.Set("If-Modified-Since", info.ModTime().UTC().Format(http.TimeFormat))
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("fetching %s: %w", url, err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		// Download the file
	case http.StatusNotModified:
		return false, nil
	case http.StatusNotFound:
		return false, fmt.Errorf("404 not found: %s", url)
	default:
		return false, fmt.Errorf("HTTP %d for %s", resp.StatusCode, url)
	}

	tmpPath := destPath + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return false, fmt.Errorf("creating temp file %s: %w", tmpPath, err)
	}

	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return false, fmt.Errorf("writing %s: %w", tmpPath, err)
	}
	f.Close()

	// Set mtime from server's Last-Modified header for future If-Modified-Since
	if lm := resp.Header.Get("Last-Modified"); lm != "" {
		if t, parseErr := time.Parse(http.TimeFormat, lm); parseErr == nil {
			os.Chtimes(tmpPath, t, t)
		}
	}

	if err := os.Rename(tmpPath, destPath); err != nil {
		os.Remove(tmpPath)
		return false, fmt.Errorf("renaming %s to %s: %w", tmpPath, destPath, err)
	}

	return true, nil
}

// downloadResult holds the outcome for one URL in a concurrent download batch.
type downloadResult struct {
	URL        string
	LocalPath  string
	Downloaded bool
	Err        error
}

// downloadConcurrent downloads multiple URLs with bounded concurrency.
// Returns results for every URL (check .Err for failures).
func downloadConcurrent(urls []string, destDir string, concurrency int) []downloadResult {
	if concurrency <= 0 {
		concurrency = 5
	}

	results := make([]downloadResult, len(urls))
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	for i, u := range urls {
		wg.Add(1)
		go func(idx int, rawURL string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			// Derive local filename from URL path
			filename := filepath.Base(rawURL)
			localPath := filepath.Join(destDir, filename)

			downloaded, err := downloadFile(rawURL, localPath)
			results[idx] = downloadResult{
				URL:        rawURL,
				LocalPath:  localPath,
				Downloaded: downloaded,
				Err:        err,
			}
		}(i, u)
	}

	wg.Wait()
	return results
}

// billTypes lists all GPO bill status types (lowercase used in URLs).
var billTypes = []string{"hr", "s", "hjres", "sjres", "hconres", "sconres", "hres", "sres"}

// listBillStatusFiles discovers available XML files from a GPO bulk data index page.
// URL pattern: https://www.govinfo.gov/bulkdata/BILLSTATUS/{congress}/{type}/
// Returns the list of full URLs to individual bill XML files.
func listBillStatusFiles(congress int, billType string) ([]string, error) {
	indexURL := fmt.Sprintf("https://www.govinfo.gov/bulkdata/BILLSTATUS/%d/%s", congress, billType)

	resp, err := http.Get(indexURL)
	if err != nil {
		return nil, fmt.Errorf("fetching GPO index %s: %w", indexURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil // No bills of this type for this congress
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d for GPO index %s", resp.StatusCode, indexURL)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading GPO index %s: %w", indexURL, err)
	}

	// Parse XML file links from the HTML/XML index page
	// GPO index pages list files as href="BILLSTATUS-119hr1.xml" or similar
	re := regexp.MustCompile(`href="(BILLSTATUS-\d+[a-z]+\d+\.xml)"`)
	matches := re.FindAllStringSubmatch(string(body), -1)

	var urls []string
	for _, m := range matches {
		urls = append(urls, indexURL+"/"+m[1])
	}

	return urls, nil
}

// discoverHouseVotes discovers available House clerk roll call XML files for a year.
// URL pattern: https://clerk.house.gov/evs/{year}/roll{NNN}.xml
// Iterates roll call numbers starting from 1 until 404.
func discoverHouseVotes(year int) ([]string, error) {
	var urls []string

	for rollNum := 1; ; rollNum++ {
		url := fmt.Sprintf("https://clerk.house.gov/evs/%d/roll%03d.xml", year, rollNum)

		// HEAD request to check existence without downloading
		resp, err := http.Head(url)
		if err != nil {
			// Network error — stop discovery
			if rollNum == 1 {
				return nil, fmt.Errorf("checking House votes for %d: %w", year, err)
			}
			break
		}
		resp.Body.Close()

		if resp.StatusCode == http.StatusNotFound {
			break
		}
		if resp.StatusCode == http.StatusOK {
			urls = append(urls, url)
		}

		// Safety valve: log progress every 100
		if rollNum%100 == 0 {
			log.Printf("[bulk-download] Discovered %d House roll calls for %d so far...", len(urls), year)
		}
	}

	return urls, nil
}

// discoverSenateVotes discovers available Senate clerk roll call XML files.
// URL pattern: https://www.senate.gov/legislative/LIS/roll_call_votes/vote{congress}{session}/vote_{congress}_{session}_{NNNNN}.xml
func discoverSenateVotes(congress, session int) ([]string, error) {
	var urls []string

	for voteNum := 1; ; voteNum++ {
		url := fmt.Sprintf(
			"https://www.senate.gov/legislative/LIS/roll_call_votes/vote%d%d/vote_%d_%d_%05d.xml",
			congress, session, congress, session, voteNum,
		)

		resp, err := http.Head(url)
		if err != nil {
			if voteNum == 1 {
				return nil, fmt.Errorf("checking Senate votes for congress %d session %d: %w", congress, session, err)
			}
			break
		}
		resp.Body.Close()

		if resp.StatusCode == http.StatusNotFound {
			break
		}
		if resp.StatusCode == http.StatusOK {
			urls = append(urls, url)
		}

		if voteNum%100 == 0 {
			log.Printf("[bulk-download] Discovered %d Senate roll calls for congress %d session %d so far...", len(urls), congress, session)
		}
	}

	return urls, nil
}

// congressYears returns the two calendar years covered by a congress number.
// The 119th Congress covers 2025-2026.
func congressYears(congress int) (int, int) {
	startYear := 2025 - 2*(119-congress)
	return startYear, startYear + 1
}

// downloadBillStatusFiles downloads all bill status XML files for a congress+type
// to the given data directory. Returns paths to downloaded/cached XML files.
func downloadBillStatusFiles(congress int, billType string, dataDir string, concurrency int) ([]string, error) {
	destDir := filepath.Join(dataDir, "bills", fmt.Sprintf("%d", congress), billType)

	log.Printf("[bulk-download] Discovering bill status files for %dth Congress, type=%s", congress, billType)
	urls, err := listBillStatusFiles(congress, billType)
	if err != nil {
		return nil, err
	}
	if len(urls) == 0 {
		log.Printf("[bulk-download] No %s bills found for %dth Congress", strings.ToUpper(billType), congress)
		return nil, nil
	}
	log.Printf("[bulk-download] Found %d %s bill status files for %dth Congress", len(urls), strings.ToUpper(billType), congress)

	results := downloadConcurrent(urls, destDir, concurrency)

	var paths []string
	downloaded, cached, errors := 0, 0, 0
	for _, r := range results {
		if r.Err != nil {
			log.Printf("[bulk-download] ERROR downloading %s: %v", r.URL, r.Err)
			errors++
			continue
		}
		if r.Downloaded {
			downloaded++
		} else {
			cached++
		}
		paths = append(paths, r.LocalPath)
	}

	log.Printf("[bulk-download] %s %dth Congress: %d downloaded, %d cached, %d errors",
		strings.ToUpper(billType), congress, downloaded, cached, errors)
	return paths, nil
}

// downloadHouseVoteFiles downloads all House clerk XML files for a congress
// to the given data directory. Returns paths to downloaded/cached XML files.
func downloadHouseVoteFiles(congress int, dataDir string, concurrency int) ([]string, error) {
	year1, year2 := congressYears(congress)
	var allPaths []string

	for _, year := range []int{year1, year2} {
		destDir := filepath.Join(dataDir, "votes", "house", fmt.Sprintf("%d", year))

		log.Printf("[bulk-download] Discovering House votes for %d...", year)
		urls, err := discoverHouseVotes(year)
		if err != nil {
			log.Printf("[bulk-download] WARN: House vote discovery failed for %d: %v", year, err)
			continue
		}
		if len(urls) == 0 {
			log.Printf("[bulk-download] No House votes found for %d", year)
			continue
		}
		log.Printf("[bulk-download] Found %d House roll calls for %d", len(urls), year)

		results := downloadConcurrent(urls, destDir, concurrency)
		for _, r := range results {
			if r.Err != nil {
				log.Printf("[bulk-download] ERROR: %v", r.Err)
				continue
			}
			allPaths = append(allPaths, r.LocalPath)
		}
	}

	return allPaths, nil
}

// downloadSenateVoteFiles downloads all Senate clerk XML files for a congress
// to the given data directory. Returns paths to downloaded/cached XML files.
func downloadSenateVoteFiles(congress int, dataDir string, concurrency int) ([]string, error) {
	var allPaths []string

	for session := 1; session <= 2; session++ {
		destDir := filepath.Join(dataDir, "votes", "senate", fmt.Sprintf("%d-%d", congress, session))

		log.Printf("[bulk-download] Discovering Senate votes for %dth Congress session %d...", congress, session)
		urls, err := discoverSenateVotes(congress, session)
		if err != nil {
			log.Printf("[bulk-download] WARN: Senate vote discovery failed for congress %d session %d: %v", congress, session, err)
			continue
		}
		if len(urls) == 0 {
			log.Printf("[bulk-download] No Senate votes found for %dth Congress session %d", congress, session)
			continue
		}
		log.Printf("[bulk-download] Found %d Senate roll calls for %dth Congress session %d", len(urls), congress, session)

		results := downloadConcurrent(urls, destDir, concurrency)
		for _, r := range results {
			if r.Err != nil {
				log.Printf("[bulk-download] ERROR: %v", r.Err)
				continue
			}
			allPaths = append(allPaths, r.LocalPath)
		}
	}

	return allPaths, nil
}
