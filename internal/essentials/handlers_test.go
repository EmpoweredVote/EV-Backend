package essentials

import (
	"testing"
)

func TestJudgeDetailHasEnrichedFields(t *testing.T) {
	jd := JudgeDetail{}
	// These fields must exist on the struct (compile-time check)
	_ = jd.ElectionType
	_ = jd.AreasOfFocus
	_ = jd.DateSeated
}
