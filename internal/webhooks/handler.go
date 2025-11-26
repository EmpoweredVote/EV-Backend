package webhooks

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
)

func FramerFormWebhook(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MiB
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "payload too large or unreadable", http.StatusRequestEntityTooLarge)
		return
	}
	defer r.Body.Close()

	sig := r.Header.Get("Framer-Signature")
	sid := r.Header.Get("Framer-Webhook-Submission-Id")
	if sid == "" {
		http.Error(w, "missing submission id", http.StatusBadRequest)
		return
	}

	secret := os.Getenv("FRAMER_WEBHOOK_SECRET")
	if secret == "" {
		http.Error(w, "server misconfigured", http.StatusInternalServerError)
		return
	}
	if !verifyFramer(sig, sid, raw, secret) {
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}

	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	name := str(m, "Name", "name")
	email := str(m, "Email", "email")
	role := str(m, "Role", "role")
	message := str(m, "About You", "about_you", "about", "message")
	subscribed := boolAny(m, "Newsletter", "newsletter", "subscribed")

	if err := db.DB.Exec(`
    insert into inbox.form_submissions
        (submission_id, payload, name, email, role, message, subscribed)
    values
        (? , ?::jsonb, ? , ? , ? , ? , ?)
    on conflict (submission_id) do nothing
`, sid, string(raw), name, email, role, message, subscribed).Error; err != nil {
		http.Error(w, "db insert failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"ok":true}`))
}

func verifyFramer(sig, sid string, raw []byte, secret string) bool {
	if !strings.HasPrefix(sig, "sha256=") {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(raw)
	mac.Write([]byte(sid))
	expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(sig), []byte(expected))
}

func toBool(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case string:
		s := strings.ToLower(strings.TrimSpace(x))
		return s == "true" || s == "on" || s == "1" || s == "yes"
	default:
		return false
	}
}

func str(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if s, ok := v.(string); ok {
				return s
			}
		}
	}
	return ""
}

func boolAny(m map[string]any, keys ...string) bool {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			return toBool(v)
		}
	}
	return false
}
