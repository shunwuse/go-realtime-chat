package main

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package when any test leaks a goroutine:
// every pump, loop and server opened in tests must be stopped by cleanup.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
