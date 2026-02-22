package main

import (
	"testing"
	"time"
)

func TestRun_DispatcherRunnerExample(t *testing.T) {
	t.Parallel()

	if err := run(800 * time.Millisecond); err != nil {
		t.Fatalf("example run failed: %v", err)
	}
}
