package scheduler

import (
	"os"
	"time"
)

// FilingDeadline represents an annual FEC filing deadline by month and day.
// No year field — deadlines recur every year and the window logic uses the
// year from the time.Time argument passed to IsActiveFilingPeriod.
type FilingDeadline struct {
	Month time.Month
	Day   int
}

// fecDeadlines lists the recurring annual FEC filing deadlines.
// Includes quarterly deadlines (Jan 31, Apr 15, Jul 15, Oct 15) plus
// pre-primary (Mar 1) and pre-general (Oct 24) election-year deadlines.
// All six are included every year — the 30-day window overlap in non-election
// years is acceptable because we are polling FEC (not incurring per-poll cost).
var fecDeadlines = []FilingDeadline{
	{Month: time.January, Day: 31},  // Q4 annual report
	{Month: time.March, Day: 1},     // Pre-primary report (election years)
	{Month: time.April, Day: 15},    // Q1 report
	{Month: time.July, Day: 15},     // Q2 / mid-year report
	{Month: time.October, Day: 15},  // Q3 report
	{Month: time.October, Day: 24},  // Pre-general report (election years)
}

// IsActiveFilingPeriod returns true when now falls within 30 days before or
// 7 days after any FEC filing deadline, OR when the FEC_FORCE_HIGH_FREQ
// environment variable is set to "true".
//
// During active filing periods the scheduler uses 15-minute polling cadence.
// Outside these windows a quiet-period skip (via ShouldSkipQuietPeriod) reduces
// polling to effectively once-daily.
func IsActiveFilingPeriod(now time.Time) bool {
	if os.Getenv("FEC_FORCE_HIGH_FREQ") == "true" {
		return true
	}

	year := now.Year()
	for _, d := range fecDeadlines {
		// Construct the deadline date in the same timezone as now.
		deadline := time.Date(year, d.Month, d.Day, 23, 59, 59, 0, now.Location())
		windowStart := deadline.AddDate(0, 0, -30)
		windowEnd := deadline.AddDate(0, 0, 7)

		if !now.Before(windowStart) && !now.After(windowEnd) {
			return true
		}
	}
	return false
}

// ShouldSkipQuietPeriod returns true when the scheduler should skip a 15-minute
// cron trigger because we are outside an active filing period AND the FEC adapter
// ran successfully within the last 24 hours.
//
// This converts the always-running 15-minute cron into an effective once-daily
// cadence during quiet periods without needing a separate cron job.
func ShouldSkipQuietPeriod(now time.Time, lastSuccessfulRun time.Time) bool {
	if IsActiveFilingPeriod(now) {
		// Active period — never skip; use full 15-minute cadence.
		return false
	}
	// Quiet period: skip if we have a recent successful run.
	return !lastSuccessfulRun.IsZero() && now.Sub(lastSuccessfulRun) < 24*time.Hour
}
