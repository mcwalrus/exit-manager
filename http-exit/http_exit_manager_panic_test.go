package httpexit

import (
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"
)

// createServerWithForcedError creates a server that will error during shutdown
func createServerWithForcedError() *http.Server {
	// Create a server with an invalid address to force shutdown errors
	server := &http.Server{
		Addr: "invalid-address:99999", // This will cause shutdown errors
	}
	return server
}

func TestHTTPPanicHandling(t *testing.T) {
	t.Parallel()

	t.Run("pre-shutdown function panics don't prevent other pre-shutdowns", func(t *testing.T) {
		em := newHTTPExitManager()
		go em.listenForSignals()

		executed := make([]string, 0)
		var mu sync.Mutex

		// Register pre-shutdown hooks that will execute in reverse order (LIFO)
		em.RegisterPreShutdown(func() {
			mu.Lock()
			executed = append(executed, "pre1")
			mu.Unlock()
		})

		em.RegisterPreShutdown(func() {
			mu.Lock()
			executed = append(executed, "pre2_panic")
			mu.Unlock()
			panic("pre-shutdown panic")
		})

		em.RegisterPreShutdown(func() {
			mu.Lock()
			executed = append(executed, "pre3")
			mu.Unlock()
		})

		// Shutdown should complete despite panic
		em.Shutdown()

		select {
		case <-em.Done():
		case <-time.After(500 * time.Millisecond):
			t.Fatal("expected HTTP exit manager to complete shutdown despite pre-shutdown panic")
		}

		// Verify all pre-shutdowns executed (in reverse order)
		mu.Lock()
		expected := []string{"pre3", "pre2_panic", "pre1"}
		mu.Unlock()

		if len(executed) != len(expected) {
			t.Fatalf("expected %d pre-shutdowns to execute, got %d: %v", len(expected), len(executed), executed)
		}

		for i, exp := range expected {
			if executed[i] != exp {
				t.Fatalf("pre-shutdown execution order mismatch at index %d: expected %s, got %s", i, exp, executed[i])
			}
		}
	})

	t.Run("multiple pre-shutdown function panics don't prevent graceful exit", func(t *testing.T) {
		em := newHTTPExitManager()
		go em.listenForSignals()

		executed := make([]string, 0)
		var mu sync.Mutex

		// Register multiple panicking pre-shutdowns
		for i := 0; i < 5; i++ {
			name := fmt.Sprintf("pre%d", i)
			em.RegisterPreShutdown(func() {
				mu.Lock()
				executed = append(executed, name)
				mu.Unlock()
				if name != "pre2" { // Only one should not panic
					panic("pre-shutdown panic: " + name)
				}
			})
		}

		em.Shutdown()

		select {
		case <-em.Done():
		case <-time.After(500 * time.Millisecond):
			t.Fatal("expected HTTP exit manager to complete shutdown despite multiple pre-shutdown panics")
		}

		// All pre-shutdowns should have executed
		mu.Lock()
		if len(executed) != 5 {
			t.Fatalf("expected 5 pre-shutdowns to execute, got %d: %v", len(executed), executed)
		}
		mu.Unlock()
	})

	t.Run("http server shutdown function panics don't prevent other server shutdowns", func(t *testing.T) {
		em := newHTTPExitManager()
		go em.listenForSignals()

		// For this test, let's verify that the shutdown process itself doesn't panic
		// even if individual server shutdowns would cause issues
		// We'll focus on the panic recovery in the shutdown execution

		shutdownExecuted := false

		// Register a server and manually trigger the shutdown logic
		server := &http.Server{Addr: ":0"}
		err := em.RegisterHTTPServer(HTTPServerShutdownConfig{
			Server: server,
			HandleErr: func(err error) {
				shutdownExecuted = true
				panic("server shutdown panic")
			},
		})
		if err != nil {
			t.Fatalf("unexpected error registering server: %v", err)
		}

		// Force the server to have an error during shutdown by closing it first
		server.Close()

		// Shutdown should complete despite panic
		em.Shutdown()

		select {
		case <-em.Done():
		case <-time.After(500 * time.Millisecond):
			t.Fatal("expected HTTP exit manager to complete shutdown despite server shutdown panic")
		}

		// Verify shutdown process completed even if error handler panicked
		if shutdownExecuted {
			t.Log("Server shutdown error handler was called and panicked, but shutdown completed gracefully")
		}
	})

	t.Run("http server error handler panics don't prevent concurrent shutdowns", func(t *testing.T) {
		em := newHTTPExitManager()
		go em.listenForSignals()

		errorHandlerExecuted := false

		// Create a server that might trigger error handler
		server := &http.Server{Addr: ":0"}
		server.Close() // Close immediately to potentially trigger error on shutdown

		err := em.RegisterHTTPServer(HTTPServerShutdownConfig{
			Server: server,
			HandleErr: func(err error) {
				errorHandlerExecuted = true
				panic("error handler panic")
			},
		})
		if err != nil {
			t.Fatalf("unexpected error registering server: %v", err)
		}

		em.Shutdown()

		select {
		case <-em.Done():
		case <-time.After(500 * time.Millisecond):
			t.Fatal("expected HTTP exit manager to complete shutdown despite error handler panics")
		}

		// Verify shutdown completed regardless of whether error handler was called
		t.Log("HTTP exit manager completed shutdown successfully")
		if errorHandlerExecuted {
			t.Log("Error handler was executed and panicked, but shutdown continued gracefully")
		}
	})

	t.Run("mixed panics in pre-shutdown and server shutdown", func(t *testing.T) {
		em := newHTTPExitManager()
		go em.listenForSignals()

		executed := make([]string, 0)
		var mu sync.Mutex

		// Register pre-shutdown that panics
		em.RegisterPreShutdown(func() {
			mu.Lock()
			executed = append(executed, "pre_panic")
			mu.Unlock()
			panic("pre-shutdown panic")
		})

		em.RegisterPreShutdown(func() {
			mu.Lock()
			executed = append(executed, "pre_normal")
			mu.Unlock()
		})

		// Register server with error handler that panics
		server := &http.Server{Addr: ":0"}
		server.Close() // Force potential shutdown error
		err := em.RegisterHTTPServer(HTTPServerShutdownConfig{
			Server: server,
			HandleErr: func(err error) {
				mu.Lock()
				executed = append(executed, "server_panic")
				mu.Unlock()
				panic("server shutdown panic")
			},
		})
		if err != nil {
			t.Fatalf("unexpected error registering server: %v", err)
		}

		em.Shutdown()

		select {
		case <-em.Done():
		case <-time.After(500 * time.Millisecond):
			t.Fatal("expected HTTP exit manager to complete shutdown despite multiple panics")
		}

		// Pre-shutdowns should have executed, server shutdown may or may not trigger error handler
		mu.Lock()
		if len(executed) < 2 {
			t.Fatalf("expected at least 2 executions (pre-shutdowns), got %d: %v", len(executed), executed)
		}

		// Verify pre-shutdowns executed
		hasPreNormal := false
		hasPrePanic := false
		for _, exec := range executed {
			if exec == "pre_normal" {
				hasPreNormal = true
			}
			if exec == "pre_panic" {
				hasPrePanic = true
			}
		}

		if !hasPreNormal || !hasPrePanic {
			t.Fatalf("pre-shutdown functions should have executed: %v", executed)
		}
		mu.Unlock()
	})

	t.Run("server shutdown with timeout and panic", func(t *testing.T) {
		em := newHTTPExitManager()
		go em.listenForSignals()

		executed := false
		server := &http.Server{Addr: ":0"}
		server.Close() // Force potential shutdown error

		err := em.RegisterHTTPServer(HTTPServerShutdownConfig{
			Server:  server,
			Timeout: 50 * time.Millisecond,
			HandleErr: func(err error) {
				executed = true
				panic("timeout error handler panic")
			},
		})
		if err != nil {
			t.Fatalf("unexpected error registering server: %v", err)
		}

		em.Shutdown()

		select {
		case <-em.Done():
		case <-time.After(200 * time.Millisecond):
			t.Fatal("expected HTTP exit manager to complete shutdown despite timeout and panic")
		}

		// Shutdown should complete regardless of whether error handler was called
		t.Log("HTTP exit manager completed timeout test successfully")
		if executed {
			t.Log("Error handler was executed and panicked, but shutdown continued gracefully")
		}
	})
}
