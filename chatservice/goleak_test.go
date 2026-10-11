package main

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package when any test leaks a goroutine:
// every bufconn server and client opened in tests must be stopped by cleanup.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
