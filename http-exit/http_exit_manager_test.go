package httpexit

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// testExitManager returns a fresh ExitManager instance for testing.
func testHTTPExitManager(t *testing.T) *HTTPExitManager {
	t.Helper()

	em := newHTTPExitManager()
	go em.listenForSignals()

	t.Cleanup(func() {
		em.Shutdown()
	})

	return em
}

func (em *HTTPExitManager) hasShutdown() bool {
	em.mu.Lock()
	defer em.mu.Unlock()
	return em.notified
}

var httpHandler = http.HandlerFunc(
	func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

func serverIsClosed(server *httptest.Server) bool {
	_, err := http.Get(server.URL)
	return err != nil
}

func waitForServerReady(server *httptest.Server, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(server.URL)
		if err == nil {
			resp.Body.Close()
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
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
		server := httptest.NewServer(httpHandler)
		t.Cleanup(server.Close)

		if !waitForServerReady(server, 100*time.Millisecond) {
			t.Fatal("server is not alive")
		}

		err := em.RegisterHTTPServer(HTTPServerShutdownConfig{
			Server: server.Config,
		})
		if err != nil {
			t.Fatalf("unexpected error registering server: %v", err)
		}

		em.Shutdown()
		<-em.Done()

		if !serverIsClosed(server) {
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
		server := httptest.NewServer(httpHandler)
		t.Cleanup(server.Close)

		em.Shutdown()

		err := em.RegisterHTTPServer(HTTPServerShutdownConfig{
			Server: server.Config,
		})
		if err == nil {
			t.Error("expected error when registering server after shutdown")
		}
	})

	// For the server to recognise the context on Shutdown() we need to use a listener
	// with an idle connection, otherwise the server will ignore the context and not
	// return an error.
	t.Run("error handling on server.Shutdown", func(t *testing.T) {
		em := testHTTPExitManager(t)

		// Handler config
		timeout := 3 * time.Second
		connEstablished := make(chan struct{})

		// Handler to keep the connection alive and prevent graceful shutdown
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(connEstablished)
			w.Header().Set("Content-Length", "100")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("partial"))
			time.Sleep(timeout)
		})

		// Create server with handler
		server := httptest.NewServer(handler)
		t.Cleanup(server.Close)

		// Wait a moment for server to start
		time.Sleep(50 * time.Millisecond)

		// Make request to establish connection that will block shutdown
		// Try to read response (will be interrupted by server shutdown)
		go func() {
			resp, err := http.Get(server.URL)
			if err != nil {
				return
			}
			defer resp.Body.Close()
			buf := make([]byte, 100)
			_, _ = resp.Body.Read(buf)
		}()

		// Wait for connection to be established
		select {
		case <-connEstablished:
		case <-time.After(100 * time.Millisecond):
			t.Fatal("connection was not established")
		}

		// Create cancelled context for shutdown
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		// Register server with cancelled context
		var handledErr error
		err := em.RegisterHTTPServer(HTTPServerShutdownConfig{
			Ctx:    ctx,
			Server: server.Config,
			HandleErr: func(err error) {
				handledErr = err
			},
		})
		if err != nil {
			t.Fatalf("unexpected error registering server: %v", err)
		}

		em.Shutdown()
		<-em.Done()

		// Verify error handler was called with context cancellation error
		if handledErr == nil {
			t.Error("error handler was not called")
		}
		if !errors.Is(handledErr, context.Canceled) {
			t.Errorf("expected context.Canceled error, got: %v", handledErr)
		}
	})
}

func TestShutdown(t *testing.T) {
	t.Parallel()

	t.Run("Done closes after shutdown completes", func(t *testing.T) {
		em := testHTTPExitManager(t)

		select {
		case <-em.Done():
			t.Error("Done channel should not be closed before shutdown")
		default:
		}

		if em.hasShutdown() {
			t.Error("hasShutdown should return false before shutdown")
		}

		em.Shutdown()

		if !em.hasShutdown() {
			t.Error("hasShutdown should return true after shutdown")
		}

		// Done channel should close
		select {
		case <-em.Done():
		case <-time.After(100 * time.Millisecond):
			t.Error("Done channel should close after shutdown")
		}
	})

	t.Run("multiple shutdown calls are safe", func(t *testing.T) {
		em := testHTTPExitManager(t)
		var wg sync.WaitGroup
		n := 5

		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				em.Shutdown()
			}()
		}

		wg.Wait()
		select {
		case <-em.Done():
		case <-time.After(100 * time.Millisecond):
			t.Error("Done channel should close after multiple shutdown calls")
		}
	})
}
