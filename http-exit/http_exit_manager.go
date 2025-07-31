// Package httpexit provides HTTP server shutdown coordination for graceful application exits.
//
// The HTTP exit manager integrates with the base exit manager to handle graceful
// shutdown of HTTP servers. It supports:
//   - Pre-shutdown hooks for releasing long-lived connections (WebSockets, etc.)
//   - Concurrent shutdown of multiple HTTP servers with individual timeouts
//   - Integration with the base exit manager for unified shutdown coordination
//
// Basic usage:
//
//	func main() {
//		// Create HTTP server
//		server := &http.Server{Addr: ":8080", Handler: handler}
//
//		// Register server for graceful shutdown
//		httpEM := httpexit.Global()
//		err := httpEM.RegisterHTTPServer(httpexit.HTTPServerShutdownConfig{
//			Server:  server,
//			Timeout: 30 * time.Second,
//			HandleErr: func(err error) {
//				log.Printf("Server shutdown error: %v", err)
//			},
//		})
//		if err != nil {
//			log.Fatal(err)
//		}
//
//		// Register pre-shutdown hook to close WebSocket connections
//		httpEM.RegisterPreShutdown(func() {
//			log.Println("Closing WebSocket connections...")
//			// Close active WebSocket connections
//		})
//
//		// Start server
//		go func() {
//			if err := server.ListenAndServe(); err != http.ErrServerClosed {
//				log.Fatal(err)
//			}
//		}()
//
//		// Integration with base exit manager
//		baseEM := exitmanager.Global()
//		baseEM.RegisterHTTPExitManager(httpEM)
//
//		// Wait for shutdown signal
//		<-baseEM.Notify()
//		log.Println("Shutdown initiated...")
//	}
package httpexit

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// HTTPExitManager manages HTTP server shutdown events and coordinates graceful shutdowns.
//
// The HTTP exit manager provides two phases of shutdown coordination:
//  1. Pre-shutdown: Hooks for releasing hijacked connections (WebSockets, long-polling, etc.)
//  2. HTTP shutdown: Concurrent graceful shutdown of registered HTTP servers
//
// Use Global() to get the singleton instance that integrates with the base exit manager.
type HTTPExitManager struct {
	mu            *sync.Mutex
	once          *sync.Once
	notified      bool
	preShutdowns  []func()
	httpShutdowns []func()
	notify        chan struct{}
	shutdown      chan struct{}
	done          chan struct{}
}

// HTTPServerShutdownConfig provides control over HTTP server shutdown behavior.
//
// Each registered server can have individual shutdown timeouts and error handling.
// This allows fine-grained control over shutdown behavior for different server types.
type HTTPServerShutdownConfig struct {
	// Ctx is the base context for shutdown. If nil, context.Background() is used.
	// The context may be wrapped with a timeout if Timeout > 0 and can be cancelled
	// to allow for early shutdown.
	Ctx context.Context

	// Server is the HTTP server to shutdown gracefully on exit.
	// Must not be nil.
	Server *http.Server

	// Timeout specifies how long to wait for the server to shutdown gracefully.
	// If Timeout > 0, the shutdown context gets a deadline.
	// If Timeout <= 0, no timeout is applied (waits indefinitely).
	// Individual server timeouts should complete well within the base exit manager's timeout.
	Timeout time.Duration

	// HandleErr is called if Server.Shutdown(ctx) returns an error.
	// Can be nil to ignore shutdown errors and avoid fatal shutdowns on [http.ErrServerClosed] errors.
	// Useful for logging or custom error handling per server.
	HandleErr func(error)
}

var (
	once    sync.Once
	manager *HTTPExitManager
)

// newHTTPExitManager returns a new HTTP exit manager instance.
// Used internally by Global() to create the singleton instance.
func newHTTPExitManager() *HTTPExitManager {
	return &HTTPExitManager{
		mu:       &sync.Mutex{},
		once:     &sync.Once{},
		shutdown: make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Global returns the singleton HTTP exit manager instance.
//
// This provides a global access point for HTTP server shutdown coordination.
// The same instance is returned on subsequent calls, making it safe to call
// from multiple packages and goroutines.
//
// Example:
//
//	httpEM := httpexit.Global()
//	httpEM.RegisterHTTPServer(httpexit.HTTPServerShutdownConfig{
//		Server: server,
//		Timeout: 30 * time.Second,
//	})
func Global() *HTTPExitManager {
	once.Do(func() {
		em := newHTTPExitManager()
		go em.listenForSignals()
		manager = em
	})
	return manager
}

// RegisterPreShutdown registers a function to execute before HTTP server shutdowns begin.
//
// Pre-shutdown hooks are useful for releasing hijacked connections that prevent
// graceful HTTP server shutdown, such as:
//   - WebSocket connections
//   - Server-sent events (SSE) streams
//   - Long-polling HTTP requests
//   - Any other hijacked HTTP connections
//
// These functions execute in reverse registration order (LIFO) to allow proper
// cleanup ordering. All pre-shutdown hooks complete before any HTTP servers
// begin their shutdown process.
//
// If the exit manager is already notified of shutdown, registration is ignored.
//
// Example:
//
//	httpEM.RegisterPreShutdown(func() {
//		log.Println("Closing WebSocket connections...")
//		for _, conn := range activeWebSockets {
//			conn.Close()
//		}
//	})
func (em *HTTPExitManager) RegisterPreShutdown(f func()) {
	em.mu.Lock()
	if !em.notified {
		em.preShutdowns = append(em.preShutdowns, f)
	}
	em.mu.Unlock()
}

// RegisterHTTPServer registers an HTTP server for graceful shutdown coordination.
//
// The server will be shutdown gracefully when the exit manager is notified.
// All registered servers are shutdown concurrently for faster overall shutdown.
//
// Returns an error if:
//   - The exit manager has already been notified of shutdown
//   - The provided server is nil
//
// Configuration options:
//   - Server: The HTTP server to shutdown (required, must not be nil)
//   - Timeout: Maximum time to wait for graceful shutdown (0 = no timeout)
//   - HandleErr: Optional error handler for shutdown errors
//   - Ctx: Base context for shutdown (nil = context.Background())
//
// Important: If your server handles long-lived connections (WebSockets, SSE,
// long-polling), set a reasonable timeout to prevent indefinite blocking.
// Consider registering pre-shutdown hooks to close such connections first.
//
// Example:
//
//	err := httpEM.RegisterHTTPServer(httpexit.HTTPServerShutdownConfig{
//		Server:  server,
//		Timeout: 30 * time.Second,
//		HandleErr: func(err error) {
//			log.Printf("Server shutdown error: %v", err)
//		},
//	})
//	if err != nil {
//		log.Fatal(err)
//	}
func (em *HTTPExitManager) RegisterHTTPServer(cfg HTTPServerShutdownConfig) error {
	em.mu.Lock()
	defer em.mu.Unlock()

	if em.notified {
		return fmt.Errorf("httpexit.ExitManager: notified of shutdown")
	}
	if cfg.Server == nil {
		return fmt.Errorf("httpexit.ExitManager: cannot register nil http.Server")
	}

	em.httpShutdowns = append(
		em.httpShutdowns,
		func() {
			handleErr := cfg.HandleErr
			ctx := cfg.Ctx
			if ctx == nil {
				ctx = context.Background()
			}
			var cancel context.CancelFunc
			if cfg.Timeout > 0 {
				ctx, cancel = context.WithTimeout(context.Background(), cfg.Timeout)
				defer cancel()
			}
			err := cfg.Server.Shutdown(ctx)
			if handleErr != nil && err != nil {
				handleErr(err)
			}
		},
	)

	return nil
}

// Notify returns a channel that closes when the HTTP exit manager is notified
// of shutdown. This is used to coordinate shutdown with the base exit manager.
func (em *HTTPExitManager) Notify() <-chan struct{} {
	return em.notify
}

// Shutdown initiates the HTTP server shutdown process.
//
// This method coordinates the two-phase shutdown:
//  1. Execute all pre-shutdown hooks in reverse registration order (LIFO)
//  2. Shutdown all registered HTTP servers concurrently
//
// The method is safe to call multiple times and from multiple goroutines.
// Only the first call triggers the actual shutdown process.
//
// Shutdown sequence:
//  1. Mark as notified (prevents new registrations)
//  2. Execute pre-shutdown hooks to release hijacked connections
//  3. Shutdown all HTTP servers concurrently with their configured timeouts
//  4. Close the done channel to signal completion
//
// This method is typically called by the base exit manager during application
// shutdown, but can be called directly for testing or manual shutdown.
func (em *HTTPExitManager) Shutdown() {
	em.mu.Lock()
	if em.notified {
		em.mu.Unlock()
		return
	}
	em.notified = true
	close(em.shutdown)
	em.mu.Unlock()
}

// Done returns a channel that closes when the shutdown process is complete.
//
// This is useful for testing and for coordinating with other shutdown processes.
// The channel closes after all pre-shutdown hooks and HTTP server shutdowns
// have completed.
//
// Example:
//
//	httpEM := httpexit.Global()
//	httpEM.Shutdown()
//	<-httpEM.Done() // Wait for shutdown to complete
func (em *HTTPExitManager) Done() <-chan struct{} {
	return em.done
}

// listenForSignals handles signal registration and coordinates the http server's shutdown process.
func (em *HTTPExitManager) listenForSignals() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigCh:
	case <-em.shutdown:
	}

	em.once.Do(func() {
		em.mu.Lock()
		em.notified = true
		preShutdowns := append([]func(){}, em.preShutdowns...)
		httpShutdowns := append([]func(){}, em.httpShutdowns...)
		close(em.notify)
		em.mu.Unlock()

		defer func() {
			close(em.done)
		}()

		if len(preShutdowns) == 0 && len(httpShutdowns) == 0 {
			return
		}

		// Execute pre-shutdown functions in reverse order (LIFO)
		for i := len(preShutdowns) - 1; i >= 0; i-- {
			preShutdowns[i]()
		}

		// Shutdown all HTTP servers concurrently
		wg := &sync.WaitGroup{}
		wg.Add(len(httpShutdowns))
		for _, shutdown := range httpShutdowns {
			go func(f func()) {
				defer wg.Done()
				f()
			}(shutdown)
		}
		wg.Wait()
	})
}
