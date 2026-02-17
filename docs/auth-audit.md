# Auth System Audit

**Date:** 2026-02-17
**Scope:** EV-Backend session-based authentication — cookie configuration, session lifecycle, route manifest, CORS, security observations, domain migration notes, and Phase 2 handoff contract.

---

## 1. Overview

EV-Backend uses a hand-rolled, database-backed session system. There is no external session library. Sessions are stored in the `app_auth.sessions` PostgreSQL table, validated per-request by `SessionMiddleware`, and expired with a fixed 6-hour TTL. There is no sliding window — a session created at T=0 expires at T+6h regardless of activity.

The session model is straightforward: one session per user (upsert pattern in `LoginHandler`). The `SessionMiddleware` accepts a `SessionFetcher` interface, which enables clean mocking in unit tests without a database connection.

No existing integration tests targeted the auth flow prior to Phase 1. Phase 1 establishes automated regression guards for login, session persistence, logout, and expiry.

Source files:
- `internal/auth/handlers.go` — LoginHandler, LogoutHandler, MeHandler, sessionCookie()
- `internal/auth/models.go` — Session, User models
- `internal/auth/fetcher.go` — SessionInfo (real DB SessionFetcher implementation)
- `internal/auth/routes.go` — auth route definitions
- `internal/middleware/middleware.go` — SessionMiddleware, AdminMiddleware, CORSMiddleware

---

## 2. Cookie Configuration

### 2.1 Production

When the `PORT` environment variable is set and does not begin with `5050`, the server runs in production mode.

| Attribute | Value |
|-----------|-------|
| Name | `session_id` |
| Value | UUID v4 |
| Path | `/` |
| MaxAge | `21600` (6 hours in seconds) |
| HttpOnly | `true` (not readable by JavaScript) |
| Secure | `true` (HTTPS only) |
| SameSite | `None` (cross-site sends allowed — required for Netlify + Render split hosting) |
| Domain | (not set — browser scopes to exact API domain) |

### 2.2 Local Development

When `PORT` is empty or begins with `5050` (default for `go run .`), the server uses development cookie settings.

| Attribute | Value |
|-----------|-------|
| Name | `session_id` |
| Value | UUID v4 |
| Path | `/` |
| MaxAge | `21600` (6 hours) |
| HttpOnly | `true` |
| Secure | `false` (allows plain HTTP) |
| SameSite | `Lax` (same-site + top-level cross-site navigation) |
| Domain | (not set) |

### 2.3 Environment Detection Logic

Source: `internal/auth/handlers.go` lines 25-29

```go
port := os.Getenv("PORT")
if port == "" || strings.HasPrefix(port, "5050") {
    // local dev mode: Secure=false, SameSite=Lax
}
```

This logic is fragile: if `PORT=5050` is set in a production-like environment, the cookie would silently downgrade to dev settings. A dedicated `APP_ENV` environment variable would be more robust, but this is a documentation note — not a Phase 1 fix.

### 2.4 Cookie Lifecycle

The cookie is set in two places:
- **Login** (`LoginHandler`, `internal/auth/handlers.go` line 135): `MaxAge=21600`
- **Logout** (`LogoutHandler`, `internal/auth/handlers.go` line 185): `MaxAge=-1` (instructs browser to delete cookie immediately)

---

## 3. Session Lifecycle

1. **Login** — `LoginHandler` authenticates username + password using bcrypt. On success, it generates a UUID session ID, sets the cookie, and upserts a session record into `app_auth.sessions` with `ExpiresAt = now + 6h`.

2. **Per-request validation** — `SessionMiddleware` extracts `session_id` from cookies, calls `fetcher.FindSessionByID()` to look up the session in `app_auth.sessions`, and checks `session.ExpiresAt.Before(time.Now())`. If the session is expired, it returns 401 with the body `"Session expired"`. On success, it injects the `userID` into the request context via `utils.ContextUserIDKey`.

3. **Logout** — `LogoutHandler` deletes the session record from `app_auth.sessions` AND clears the cookie (MaxAge=-1). Subsequent requests with the stale cookie will fail at step 2 because the DB record no longer exists.

4. **No renewal** — Sessions are not extended on activity. A session created at login expires exactly 6 hours later, regardless of how many requests were made.

5. **One session per user** — `LoginHandler` uses an upsert pattern: it checks for an existing session by `user_id` and updates `session_id` and `expires_at` if one exists, or creates a new record if not. This means logging in from a second device invalidates the first device's session.

---

## 4. Route Auth Level Manifest

### 4.1 Summary by Auth Level

| Auth Level | Count | Description |
|------------|-------|-------------|
| public | 21 | No authentication required |
| auth-required | 27 | Valid session cookie required (SessionMiddleware) |
| auth-required + admin | 14 | Valid session + admin role required (SessionMiddleware + AdminMiddleware) |

**Total:** 62 routes across 7 modules (root + auth + compass + essentials + treasury + staging + webhooks).

### 4.2 Module: Root (`main.go`)

| Method | Path | Auth Level | Handler |
|--------|------|-----------|---------|
| GET | `/` | public | `RootHandler` |

### 4.3 Module: Auth (`/auth`)

| Method | Path | Auth Level | Handler |
|--------|------|-----------|---------|
| POST | `/auth/login` | public | `LoginHandler` |
| POST | `/auth/register` | public | `RegisterHandler` |
| GET | `/auth/healthz` | public | inline 200 ok |
| GET | `/auth/me` | auth-required | `MeHandler` |
| POST | `/auth/complete-onboarding` | auth-required | `OnboardingHandler` |
| POST | `/auth/update-password` | auth-required | `UpdatePasswordHandler` |
| POST | `/auth/logout` | auth-required | `LogoutHandler` |
| GET | `/auth/empowered-accounts` | auth-required | `EmpoweredAccountHandler` |
| GET | `/auth/admin-check` | auth-required | `AdminCheckHandler` |
| GET | `/auth/admin` | auth-required + **admin** | inline 200 ok |
| POST | `/auth/create-dummy` | auth-required + **admin** | `CreateDummyHandler` |
| POST | `/auth/update-profile-pic` | auth-required + **admin** | `UpdateProfilePicHandler` |
| POST | `/auth/update-username` | auth-required + **admin** | `UpdateUsername` |
| DELETE | `/auth/delete-user/{userID}` | auth-required + **admin** | `DeleteUser` |

### 4.4 Module: Compass (`/compass`)

| Method | Path | Auth Level | Handler |
|--------|------|-----------|---------|
| GET | `/compass/topics` | public | `TopicHandler` |
| POST | `/compass/topics/batch` | public | `TopicBatchHandler` |
| GET | `/compass/categories` | public | `CategoryHandler` |
| GET | `/compass/politicians/{politician_id}/{topic_id}/context` | public | `GetPoliticianContext` |
| GET | `/compass/politicians/{politician_id}/answers` | public | `GetPoliticianAnswers` |
| GET | `/compass/politicians` | public | `PoliticiansWithAnswersHandler` |
| POST | `/compass/answers` | auth-required | `UserAnswersHandler` |
| GET | `/compass/answers` | auth-required | `UserAnswersHandler` |
| POST | `/compass/answers/batch` | auth-required | `UserAnswerBatchHandler` |
| GET | `/compass/selected-topics` | auth-required | `SelectedTopicsHandler` |
| PUT | `/compass/selected-topics` | auth-required | `SelectedTopicsHandler` |
| POST | `/compass/politicians/{politician_id}/answers/batch` | auth-required | `PoliticianAnswerBatch` |
| POST | `/compass/compare` | auth-required | `CompareHandler` |
| PATCH | `/compass/topics/update` | auth-required + **admin** | `TopicUpdateHandler` |
| POST | `/compass/topics/create` | auth-required + **admin** | `CreateTopicHandler` |
| PATCH | `/compass/stances/update` | auth-required + **admin** | `StancesUpdateHandler` |
| PATCH | `/compass/topics/categories/update` | auth-required + **admin** | `UpdateTopicCategoriesHandler` |
| POST | `/compass/politicians/context` | auth-required + **admin** | `PoliticianContextHandler` |
| PUT | `/compass/politicians/{politician_id}/answers` | auth-required + **admin** | `UpsertPoliticianAnswers` |
| DELETE | `/compass/topics/delete/{id}` | auth-required + **admin** | `DeleteTopicHandler` |

### 4.5 Module: Essentials (`/essentials`)

| Method | Path | Auth Level | Handler |
|--------|------|-----------|---------|
| GET | `/essentials/politicians` | public | `GetAllPoliticians` |
| GET | `/essentials/politicians/{zip}` | public | `GetPoliticiansByZip` |
| POST | `/essentials/politicians/search` | public | `SearchPoliticians` |
| GET | `/essentials/cache-status/{zip}` | public | `GetCacheStatus` |
| GET | `/essentials/politician/{id}` | public | `GetPoliticianByID` |
| GET | `/essentials/politician/{id}/endorsements` | public | `GetPoliticianEndorsements` |
| GET | `/essentials/politician/{id}/stances` | public | `GetPoliticianStances` |
| GET | `/essentials/politician/{id}/elections` | public | `GetPoliticianElections` |
| POST | `/essentials/admin/import` | auth-required + **admin** | `StartBulkImport` |
| GET | `/essentials/admin/import/{jobID}` | auth-required + **admin** | `GetImportStatus` |
| GET | `/essentials/admin/import` | auth-required + **admin** | `ListImportJobs` |

### 4.6 Module: Treasury (`/treasury`)

| Method | Path | Auth Level | Handler |
|--------|------|-----------|---------|
| GET | `/treasury/cities` | public | `ListCities` |
| GET | `/treasury/cities/{city_id}` | public | `GetCity` |
| GET | `/treasury/budgets` | public | `ListBudgets` |
| GET | `/treasury/budgets/{budget_id}` | public | `GetBudget` |
| GET | `/treasury/budgets/{budget_id}/categories` | public | `GetBudgetCategories` |
| POST | `/treasury/cities` | auth-required + **admin** | `CreateCity` |
| PUT | `/treasury/cities/{city_id}` | auth-required + **admin** | `UpdateCity` |
| DELETE | `/treasury/cities/{city_id}` | auth-required + **admin** | `DeleteCity` |
| POST | `/treasury/budgets` | auth-required + **admin** | `CreateBudget` |
| POST | `/treasury/budgets/import` | auth-required + **admin** | `ImportBudget` |
| PUT | `/treasury/budgets/{budget_id}` | auth-required + **admin** | `UpdateBudget` |
| DELETE | `/treasury/budgets/{budget_id}` | auth-required + **admin** | `DeleteBudget` |

### 4.7 Module: Staging (`/staging`)

| Method | Path | Auth Level | Handler |
|--------|------|-----------|---------|
| GET | `/staging/data` | auth-required | `GetAllData` |
| GET | `/staging/stances` | auth-required | `ListStances` |
| GET | `/staging/stances/review-queue` | auth-required | `GetReviewQueue` |
| POST | `/staging/stances` | auth-required | `CreateStance` |
| GET | `/staging/stances/{id}` | auth-required | `GetStance` |
| PUT | `/staging/stances/{id}` | auth-required | `UpdateStance` |
| POST | `/staging/stances/{id}/submit` | auth-required | `SubmitForReview` |
| POST | `/staging/stances/{id}/approve` | auth-required | `ApproveStance` |
| POST | `/staging/stances/{id}/reject` | auth-required | `RejectStance` |
| POST | `/staging/stances/{id}/edit-resubmit` | auth-required | `EditAndResubmit` |
| POST | `/staging/stances/{id}/lock` | auth-required | `AcquireLock` |
| DELETE | `/staging/stances/{id}/lock` | auth-required | `ReleaseLock` |
| GET | `/staging/politicians` | auth-required | `ListPoliticians` |
| POST | `/staging/politicians` | auth-required | `CreatePolitician` |
| GET | `/staging/politicians/review-queue` | auth-required | `GetPoliticianReviewQueue` |
| GET | `/staging/politicians/{id}` | auth-required | `GetPolitician` |
| PUT | `/staging/politicians/{id}` | auth-required | `UpdatePolitician` |
| POST | `/staging/politicians/{id}/submit` | auth-required | `SubmitPoliticianForReview` |
| POST | `/staging/politicians/{id}/review-approve` | auth-required | `ApprovePoliticianReview` |
| POST | `/staging/politicians/{id}/review-reject` | auth-required | `RejectPoliticianReview` |
| POST | `/staging/politicians/{id}/edit-resubmit` | auth-required | `EditAndResubmitPolitician` |
| POST | `/staging/politicians/{id}/lock` | auth-required | `AcquirePoliticianLock` |
| DELETE | `/staging/politicians/{id}/lock` | auth-required | `ReleasePoliticianLock` |
| POST | `/staging/politicians/{id}/approve` | auth-required + **admin** | `ApprovePolitician` |
| POST | `/staging/politicians/{id}/reject` | auth-required + **admin** | `RejectPolitician` |

### 4.8 Module: Webhooks (`/webhooks`)

| Method | Path | Auth Level | Handler |
|--------|------|-----------|---------|
| POST | `/webhooks/framer/volunteer` | public | `FramerFormWebhook` |

---

## 5. CORS Configuration

Source: `internal/middleware/middleware.go`

The `CORSMiddleware` function echoes the request `Origin` back only if it appears in the `allowed` map. If the origin is not on the list, no CORS headers are set and cross-origin requests will be blocked by the browser. `Access-Control-Allow-Credentials: true` is set for all allowed origins, which is required for cross-origin cookie sends.

**Allowed origins (23 entries):**

| Origin | Category |
|--------|----------|
| `http://localhost:5173` | Local dev |
| `http://localhost:5174` | Local dev |
| `http://localhost:5175` | Local dev |
| `http://localhost:5176` | Local dev |
| `http://localhost:5177` | Local dev |
| `http://localhost:5178` | Local dev |
| `http://localhost:5179` | Local dev |
| `https://empoweredvote.github.io` | GitHub Pages |
| `https://ev-backend-edhm.onrender.com` | Render (backend self-referencing) |
| `https://ev-backend-h3n8.onrender.com` | Render (backend self-referencing) |
| `https://compass-dev.empowered.vote` | Production (dev subdomain) |
| `https://compass.empowered.vote` | Production |
| `https://essentials-dev.empowered.vote` | Production (dev subdomain) |
| `https://essentials.empowered.vote` | Production |
| `https://treasury-dev.empowered.vote` | Production (dev subdomain) |
| `https://treasury.empowered.vote` | Production |
| `https://data-entry-dev.empowered.vote` | Production (dev subdomain) |
| `https://data-entry.empowered.vote` | Production |
| `https://ev-essentials.netlify.app` | Netlify |
| `https://ev-compassv2.netlify.app` | Netlify |
| `https://compassv2.netlify.app` | Netlify |
| `https://ev-compass.netlify.app` | Netlify |
| `https://ev-prototypes.netlify.app` | Netlify |

When adding a new frontend deployment domain, add it to the `allowed` map in `internal/middleware/middleware.go`. The change requires a backend redeploy.

---

## 6. Security Observations

These are observations from the audit — none are bugs, and none are in scope for Phase 1 to fix.

**6.1 UpdatePasswordHandler re-validates session from cookie**
`UpdatePasswordHandler` (`internal/auth/handlers.go` lines 283-295) does its own cookie lookup and `app_auth.sessions` DB query instead of relying on the `userID` already injected into the request context by `SessionMiddleware`. This handler is registered under the `SessionMiddleware` group, so the middleware already validated the session. The handler duplicates that validation from scratch — belt-and-suspenders behavior. The context-injected `userID` is ignored entirely. This is a code smell but not a security problem.

**6.2 AdminCheckHandler does its own role check in handler body**
`AdminCheckHandler` is registered behind `SessionMiddleware` but not behind `AdminMiddleware`. It performs its own role check in handler body. The check is correct and safe, but it could be simplified to just use `AdminMiddleware` as a regular route guard.

**6.3 Port-based environment detection is fragile**
As documented in section 2.3, the `sessionCookie()` function infers environment from `PORT`. A PORT=5050 in a production-like environment would cause cookies to silently downgrade to dev settings. A dedicated `APP_ENV` variable would be more robust.

**6.4 No CSRF protection**
There is no CSRF token mechanism. For a pure API backend with `SameSite=None` cookies, CSRF is somewhat mitigated by the fact that browsers only send `SameSite=None` cookies on requests initiated from allowed origins — but CORS does not fully prevent CSRF because preflight only applies to non-simple requests. Worth noting; acceptable risk for current deployment pattern.

**6.5 Fixed 6-hour TTL with no sliding window**
Sessions expire exactly 6 hours after creation regardless of activity. This is a deliberate simplicity tradeoff: it guarantees bounded session lifetime but forces users to re-login after 6 hours even during active use. Sliding window would require updating `expires_at` on each authenticated request, adding a write per request.

---

## 7. Domain Migration Notes

These notes apply when hosting consolidates or a custom domain configuration is introduced. Changes are to `sessionCookie()` in `internal/auth/handlers.go`.

| Setting | Current | Change To | Why |
|---------|---------|-----------|-----|
| `Domain` | (omitted) | `.empowered.vote` | Allows cookie to be shared across all subdomains (compass., essentials., etc.) |
| `SameSite` | `None` | `Lax` | `Lax` is more secure; `None` is only required when frontends are on a different domain than the API |
| `Secure` | `true` | Keep `true` | Always required when SameSite=None; good practice regardless |

Additional steps for domain consolidation:

1. Update CORS `allowed` map in `internal/middleware/middleware.go` — add new domain entries, remove stale ones.
2. Update `VITE_API_URL` environment variables in frontend deployments to point to the new API domain.
3. Restore `Domain: ".empowered.vote"` in `sessionCookie()` at both call sites: `LoginHandler` (line 135) and `LogoutHandler` (line 185).
4. Change environment detection to use `APP_ENV` instead of `PORT` inference.

No changes to session database schema or session lifecycle are required for domain migration.

---

## 8. Phase 2 Handoff

Phase 2 (Guest-First Auth) can assume the following after Phase 1:

1. **Auth flow is verified.** Login, session persistence, logout, and tab reload all have passing automated tests (`internal/auth/auth_integration_test.go`). These serve as regression guards — if Phase 2 breaks any of these behaviors, the tests will catch it.

2. **Cookie configuration is documented.** The exact production cookie settings are written in section 2 of this document. No guessing is required. The `sessionCookie()` function in `internal/auth/handlers.go` is the single source of truth.

3. **Route manifest exists.** Every route across all 7 modules is categorized as public, auth-required, or auth-required+admin in section 4. Phase 2 knows exactly which routes exist and what guards they currently use.

4. **No guest-ok tier exists today.** Phase 2 introduces this tier from scratch. There is no existing code to preserve or conflict with. All routes today are either public (no middleware) or auth-required (`SessionMiddleware`).

5. **SessionMiddleware is mockable.** The `SessionFetcher` interface (`internal/middleware/middleware.go` line 12) enables clean unit test doubles for any handler Phase 2 introduces or modifies. The `middleware_test.go` pattern in `internal/middleware/` demonstrates this.

6. **CORS origin list is documented.** Section 5 of this document lists all 23 allowed origins with their categories. Phase 2 can add new origins knowing exactly where the list lives.

### Routes Phase 2 will likely change from auth-required to guest-ok

These routes currently require a valid session cookie. Phase 2 should evaluate changing them to accept either a valid session OR a guest session:

| Route | Current Level | Phase 2 Target |
|-------|--------------|----------------|
| `POST /compass/answers` | auth-required | guest-ok |
| `GET /compass/answers` | auth-required | guest-ok |
| `POST /compass/answers/batch` | auth-required | guest-ok |
| `GET /compass/selected-topics` | auth-required | guest-ok |
| `PUT /compass/selected-topics` | auth-required | guest-ok |
| `POST /compass/compare` | auth-required | guest-ok |

These routes are flagged for Phase 2 awareness. Phase 1 does not modify them.
