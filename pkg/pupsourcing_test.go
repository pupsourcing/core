package pupsourcing_test

import (
	"testing"

	pupsourcing "github.com/pupsourcing/core/pkg"
)

func TestVersion(t *testing.T) {
	version := pupsourcing.Version()
	if version == "" {
		t.Error("Version() should return a non-empty string")
	}
}
