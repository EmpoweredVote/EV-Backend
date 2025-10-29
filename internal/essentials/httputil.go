package essentials

import (
	"fmt"
	"net/http"
)

func addServerTiming(w http.ResponseWriter, kv ...[2]string) {
	// kv: [][2]string{{"dbread","12.3"}, {"upsert","210.0"}}
	if len(kv) == 0 {
		return
	}
	val := ""
	for i, p := range kv {
		if i > 0 {
			val += ", "
		}
		val += fmt.Sprintf("%s;dur=%s", p[0], p[1])
	}
	// Additive header (can call multiple times if you want)
	w.Header().Add("Server-Timing", val)
}
