package processors

import (
	"testing"

	importitems "debtster_import/internal/repository/imports"
)

func TestDefaultUserTypeIsPHPModelName(t *testing.T) {
	expected := importitems.PHPModelMap[importitems.ModelTypeUsers]
	if defaultUserType != expected {
		t.Fatalf("defaultUserType mismatch: got %q expected %q", defaultUserType, expected)
	}
}
