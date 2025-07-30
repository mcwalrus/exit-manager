package httpexit

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testExitManager returns a fresh ExitManager instance for testing.
func testHTTPExitManager(t *testing.T) *HTTPExitManager {
	t.Helper()
	return newHTTPExitManager()
}

// mockServer creates a mock HTTP server for testing.
type mockServer struct {
	*http.Server
	shutdownCalled int32
	shutdownErr    error
	shutdownDelay  time.Duration
}

func newMockServer() *mockServer {
	return &mockServer{
		Server: &http.Server{},
	}
}

func (ms *mockServer) Shutdown(ctx context.Context) error {
	atomic.AddInt32(&ms.shutdownCalled, 1)
	if ms.shutdownDelay > 0 {
		time.Sleep(ms.shutdownDelay)
	}
	return ms.shutdownErr
}

func (ms *mockServer) wasShutdownCalled() bool {
	return atomic.LoadInt32(&ms.shutdownCalled) > 0
}

func TestRegisterPreShutdown(t *testing.T) {
	t.Parallel()

	t.Run("single pre-shutdown hook executes", func(t *testing.T) {
		em := testHTTPExitManager(t)
		var executed bool

		em.RegisterPreShutdown(func() {
			executed = true
		})

		em.Shutdown()
		<-em.Done()

		if !executed {
			t.Error("pre-shutdown hook was not executed")
		}
	})

	t.Run("multiple pre-shutdown hooks execute in LIFO order", func(t *testing.T) {
		em := testHTTPExitManager(t)
		var order []int

		em.RegisterPreShutdown(func() {
			order = append(order, 1)
		})
		em.RegisterPreShutdown(func() {
			order = append(order, 2)
		})
		em.RegisterPreShutdown(func() {
			order = append(order, 3)
		})

		em.Shutdown()
		<-em.Done()

		expected := []int{3, 2, 1}
		if len(order) != len(expected) {
			t.Fatalf("expected %d hooks to execute, got %d", len(expected), len(order))
		}
		for i, v := range expected {
			if order[i] != v {
				t.Errorf("expected hook order %v, got %v", expected, order)
				break
			}
		}
	})
}

func TestRegisterHTTPServer(t *testing.T) {
	t.Parallel()

	t.Run("single server shutdown", func(t *testing.T) {
		em := testHTTPExitManager(t)
		server := newMockServer()

		err := em.RegisterHTTPServer(HTTPServerShutdownConfig{
			Server: server.Server,
		})
		if err != nil {
			t.Fatalf("unexpected error registering server: %v", err)
		}

		em.Shutdown()
		<-em.Done()

		if !server.wasShutdownCalled() {
			t.Error("server shutdown was not called")
		}
	})

	t.Run("nil server returns error", func(t *testing.T) {
		em := testHTTPExitManager(t)

		err := em.RegisterHTTPServer(HTTPServerShutdownConfig{
			Server: nil,
		})
		if err == nil {
			t.Error("expected error when registering nil server")
		}
	})

	t.Run("registration after shutdown returns error", func(t *testing.T) {
		em := testHTTPExitManager(t)
		server := newMockServer()

		em.Shutdown()

		err := em.RegisterHTTPServer(HTTPServerShutdownConfig{
			Server: server.Server,
		})
		if err == nil {
			t.Error("expected error when registering server after shutdown")
		}
	})

	t.Run("error handling", func(t *testing.T) {
		em := testHTTPExitManager(t)
		server := newMockServer()
		server.shutdownErr = fmt.Errorf("shutdown error")

		var handledErr error
		err := em.RegisterHTTPServer(HTTPServerShutdownConfig{
			Server: server.Server,
			HandleErr: func(err error) {
				handledErr = err
			},
		})
		if err != nil {
			t.Fatalf("unexpected error registering server: %v", err)
		}

		em.Shutdown()
		<-em.Done()

		if handledErr == nil {
			t.Error("error handler was not called")
		}
		if handledErr.Error() != "shutdown error" {
			t.Errorf("unexpected error: %v", handledErr)
		}
	})
}

func TestConcurrency(t *testing.T) {
	t.Parallel()

	t.Run("multiple shutdown calls are safe", func(t *testing.T) {
		em := testHTTPExitManager(t)
		var shutdownCount int32

		em.RegisterPreShutdown(func() {
			atomic.AddInt32(&shutdownCount, 1)
		})

		var wg sync.WaitGroup
		n := 5

		// Call shutdown concurrently multiple times
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				em.Shutdown()
			}()
		}

		wg.Wait()
		<-em.Done()

		// Pre-shutdown hook should only execute once
		if count := atomic.LoadInt32(&shutdownCount); count != 1 {
			t.Errorf("expected shutdown to execute once, got %d times", count)
		}
	})
}

func TestDoneAndIsShutdown(t *testing.T) {
	t.Parallel()

	t.Run("Done channel closes after shutdown completes", func(t *testing.T) {
		em := testHTTPExitManager(t)

		// Done channel should not be closed initially
		select {
		case <-em.Done():
			t.Error("Done channel should not be closed before shutdown")
		default:
		}

		if em.IsShutdown() {
			t.Error("IsShutdown should return false before shutdown")
		}

		em.Shutdown()

		if !em.IsShutdown() {
			t.Error("IsShutdown should return true after shutdown")
		}

		// Done channel should close
		select {
		case <-em.Done():
		case <-time.After(100 * time.Millisecond):
			t.Error("Done channel should close after shutdown")
		}
	})
}

func TestGlobal(t *testing.T) {
	t.Run("Global returns same instance", func(t *testing.T) {
		// Reset global state for this test
		once = sync.Once{}
		manager = nil

		em1 := Global()
		em2 := Global()

		if em1 != em2 {
			t.Error("Global should return the same instance")
		}
	})
}
