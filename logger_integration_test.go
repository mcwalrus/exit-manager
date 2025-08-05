package exitmanager

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	httpexit "github.com/mcwalrus/exit-manager/http-exit"
)

func TestNoOpLoggerBehavior(t *testing.T) {
	// Test that exit manager works without explicit logger setup
	em := newExitManager()
	testExit := &testExitHandler{done: make(chan struct{})}
	em.exit = testExit

	// Don't set a logger - should use no-op logger by default
	err := em.AcquireShutdownLock()
	if err != nil {
		t.Fatalf("Failed to acquire shutdown lock: %v", err)
	}

	em.ReleaseShutdownLock()
	em.RegisterCleanup(func() {
		// Simple cleanup
	})

	// Trigger shutdown
	em.Shutdown()

	// Wait for shutdown to complete
	select {
	case <-testExit.Done():
		// Shutdown completed successfully
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not complete within timeout")
	}

	// Verify exit code is 0 (successful)
	if testExit.code != 0 {
		t.Errorf("Expected exit code 0, got %d", testExit.code)
	}
}

func TestLoggerWithStructuredOutput(t *testing.T) {
	// Test that structured logging works correctly
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	em := newExitManager()
	testExit := &testExitHandler{done: make(chan struct{})}
	em.exit = testExit

	// Set the logger
	em.SetLogger(logger)

	// Test lock operations
	err := em.AcquireShutdownLock()
	if err != nil {
		t.Fatalf("Failed to acquire shutdown lock: %v", err)
	}
	em.ReleaseShutdownLock()

	// Register cleanup and trigger shutdown
	em.RegisterCleanup(func() {
		// Simple cleanup
	})
	em.Shutdown()

	// Wait for shutdown
	select {
	case <-testExit.Done():
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not complete within timeout")
	}

	// Verify structured logging output
	logOutput := buf.String()
	if !strings.Contains(logOutput, `"subsystem":"exit-manager"`) {
		t.Errorf("Expected subsystem in logs, got: %s", logOutput)
	}

	if !strings.Contains(logOutput, "shutdown lock acquired") {
		t.Errorf("Expected lock acquisition log, got: %s", logOutput)
	}

	if !strings.Contains(logOutput, "shutdown initiated") {
		t.Errorf("Expected shutdown initiation log, got: %s", logOutput)
	}
}

func TestHTTPExitManagerNoOpLogger(t *testing.T) {
	// Test HTTP exit manager with no-op logger
	httpEM := httpexit.Global()

	// Don't set a logger - should use no-op logger by default
	httpEM.RegisterPreShutdown(func() {
		// Simple pre-shutdown hook
	})

	// Trigger shutdown
	httpEM.Shutdown()

	// Wait for completion
	select {
	case <-httpEM.Done():
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP shutdown did not complete within timeout")
	}
}

func TestSetLoggerNil(t *testing.T) {
	// Test that setting logger to nil creates no-op logger
	em := newExitManager()
	testExit := &testExitHandler{done: make(chan struct{})}
	em.exit = testExit

	// First set a real logger
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	em.SetLogger(logger)

	// Then set to nil - should switch to no-op
	em.SetLogger(nil)

	// Test operations still work
	err := em.AcquireShutdownLock()
	if err != nil {
		t.Fatalf("Failed to acquire shutdown lock with nil logger: %v", err)
	}

	em.ReleaseShutdownLock()
	em.Shutdown()

	select {
	case <-testExit.Done():
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not complete with nil logger")
	}
}

// testExitHandler for testing
type testExitHandler struct {
	code int
	done chan struct{}
}

func (teh *testExitHandler) Exit(code int) {
	teh.code = code
	close(teh.done)
}

func (teh *testExitHandler) Done() <-chan struct{} {
	return teh.done
}
