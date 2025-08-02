// Package exitmanager provides graceful shutdown coordination for Go applications.
//
// ExitManager coordinates graceful shutdowns by:
//   - Listening for SIGINT/SIGTERM signals or programmatic shutdown requests
//   - Preventing shutdown during critical operations via shutdown locks
//   - Executing cleanup functions in reverse registration order
//   - Supporting configurable shutdown timeouts
//
// Basic usage:
//
//	func main() {
//		em := exitmanager.Global()
//
//		// Register cleanup functions
//		em.RegisterCleanup(func() {
//			log.Println("Cleaning up...")
//		})
//
//		// Start workers
//		for i := 0; i < 3; i++ {
//			go doWork(em, i)
//		}
//
//		// Wait for shutdown signal
//		<-em.Notify()
//		log.Println("Shutdown initiated, waiting for workers...")
//
//		// Optional: programmatic shutdown
//		// em.Shutdown()
//	}
//
//	func doWork(em *exitmanager.ExitManager, id int) {
//		for {
//			// Protect critical operations
//			if err := em.AcquireShutdownLock(); err != nil {
//				log.Printf("Worker %d: shutdown in progress, exiting", id)
//				return
//			}
//			defer em.ReleaseShutdownLock()
//
//			// Perform work
//			time.Sleep(time.Second)
//			log.Printf("Worker %d completed task", id)
//		}
//	}
package exitmanager

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	httpexit "github.com/mcwalrus/exit-manager/http-exit"
)

// ExitManager coordinates graceful application shutdowns.
// Use Global() to get the singleton instance.
type ExitManager struct {
	mu              *sync.RWMutex
	locks           int
	notified        bool
	once            *sync.Once
	timeout         time.Duration
	timeoutMode     TimeoutMode
	locksCh         chan struct{}
	notifyCh        chan struct{}
	shutdown        chan struct{}
	cleanups        []func()
	httpExitManager httpExitManager
	exit            exitHandler
}

var (
	once    sync.Once
	manager *ExitManager
)

// newExitManager returns a new exit manager instance.
func newExitManager() *ExitManager {
	return &ExitManager{
		mu:       &sync.RWMutex{},
		once:     &sync.Once{},
		locksCh:  make(chan struct{}),
		notifyCh: make(chan struct{}),
		shutdown: make(chan struct{}),
		exit:     osExitHandler{},
	}
}

// exitHandler provides an interface for process termination.
// Allows replacement with test implementations for testing.
type exitHandler interface {
	Exit(code int)
	Done() <-chan struct{}
}

type osExitHandler struct{}

func (ehi osExitHandler) Exit(code int) {
	os.Exit(code)
}
func (ehi osExitHandler) Done() <-chan struct{} {
	return nil
}

// Global returns the singleton ExitManager instance.
//
// The first call registers signal handlers for SIGINT and SIGTERM.
// Subsequent calls return the same instance. Safe for concurrent access.
//
// Example:
//
//	em := exitmanager.Global()
//	em.RegisterCleanup(func() {
//		log.Println("Shutting down...")
//	})
//
//	// Waiting for Ctrl+C or Shutdown()
//	<-em.Notify()
//
//	// Or programmatic shutdown
//	// em.Shutdown()
func Global() *ExitManager {
	once.Do(func() {
		em := newExitManager()
		go em.listenForSignals()
		manager = em
	})
	return manager
}

// TimeoutMode determines the behavior when the timeout expires.
//
// For most applications, TimeoutModeGraceful will be best. If you are
// concerned about process hanging during cleanup, release of locks, or
// other issues, use TimeoutModeForceful.
type TimeoutMode int

const (
	TimeoutModeNone     TimeoutMode = iota // Default mode, which is no timeout
	TimeoutModeGraceful                    // Timeout is only applied to the cleanup functions
	TimeoutModeForceful                    // Timeout is applied to the entire shutdown process
)

// SetTimeout configures the maximum shutdown duration before forced exit.
//
// If timeout is set to zero or less, the exit manager waits indefinitely for graceful shutdown.
// If timeout expires, exits with code 1 and can interrupt cleanup.
// The timeout covers the entire shutdown process:
//   - Waiting for shutdown locks to be released
//   - Executing cleanup functions
//
// Consider the time required for cleanup functions to ensure successful shutdown.
func (em *ExitManager) SetTimeout(mode TimeoutMode, timeout time.Duration) {
	em.mu.Lock()
	em.timeoutMode = mode
	em.timeout = timeout
	em.mu.Unlock()
}

// Locks returns the current number of active shutdown locks.
// Useful for monitoring shutdown progress. A non-zero value indicates
// critical operations are still preventing shutdown completion.
//
// Example:
//
//	if em.Locks() > 0 {
//		log.Printf("Waiting for %d operations to complete", em.Locks())
//	}
func (em *ExitManager) Locks() int {
	em.mu.RLock()
	defer em.mu.RUnlock()
	return em.locks
}

// AcquireShutdownLock prevents shutdown until the lock is released.
//
// Returns an error if shutdown has already been initiated, allowing
// the caller to abort the operation gracefully. Each successful call
// must be paired with exactly one call to ReleaseShutdownLock().
// Multiple locks can be acquired; shutdown waits for all to be released.
//
// Example:
//
//	if err := em.AcquireShutdownLock(); err != nil {
//		return err // Shutdown in progress
//	}
//	defer em.ReleaseShutdownLock()
//	// Perform critical operation...
// func (em *ExitManager) AcquireShutdownLock() error {
// 	em.mu.Lock()
// 	if em.notified {
// 		em.mu.Unlock()
// 		return errors.New("exit manager: shutdown in progress")
// 	} else {
// 		em.locks++
// 		em.mu.Unlock()
// 		return nil
// 	}
// }

var ErrShutdownInProgress = errors.New("exit manager: shutdown in progress")

// AcquireShutdownLock increments the shutdown lock counter.
//
// Returns ErrShutdownInProgress if shutdown has already been initiated.
// Must be paired with exactly one call to ReleaseShutdownLock().
// Critical operations should acquire a lock to prevent shutdown
// from proceeding until the operation completes.
//
// Example:
//
//	if err := em.AcquireShutdownLock(); err != nil {
//		return err // Shutdown in progress
//	}
//	defer em.ReleaseShutdownLock()
//	// Perform critical operation...
func (em *ExitManager) AcquireShutdownLock() error {
	em.mu.Lock()
	if em.notified {
		em.mu.Unlock()
		return ErrShutdownInProgress
	} else {
		em.locks++
		em.mu.Unlock()
		return nil
	}
}

// WithShutdownLock executes fn while holding a shutdown lock.
//
// Automatically acquires and releases the shutdown lock around
// the function execution. Returns ErrShutdownInProgress if
// shutdown has already been initiated.
//
// If fn panics, the panic is recovered and the shutdown lock is still
// properly released. The panic is then re-raised to maintain
// expected panic behavior while ensuring graceful shutdown continues.
func (em *ExitManager) WithShutdownLock(fn func() error) (err error) {
	if err := em.AcquireShutdownLock(); err != nil {
		return err
	}
	defer em.ReleaseShutdownLock()

	defer func() {
		if r := recover(); r != nil {
			// Re-raise the panic after ensuring cleanup
			panic(r)
		}
	}()

	return fn()
}

// ReleaseShutdownLock decrements the shutdown lock counter.
//
// Must be called exactly once for each successful AcquireShutdownLock().
// If this releases the last lock and shutdown has been initiated,
// the shutdown process proceeds to execute cleanup functions.
func (em *ExitManager) ReleaseShutdownLock() {
	em.mu.Lock()
	if em.locks > 0 {
		em.locks--
	}
	if em.locks == 0 && em.notified {
		select {
		case <-em.locksCh:
		default:
			close(em.locksCh)
		}
	}
	em.mu.Unlock()
}

// Notify returns a channel that closes when shutdown is initiated.
//
// The channel closes exactly once on the first shutdown signal
// (SIGINT/SIGTERM or Shutdown()). Remains closed thereafter,
// so multiple readers can safely select on it.
//
// Example:
//
//	go func() {
//		<-em.Notify()
//		log.Println("Shutdown initiated, stopping background work")
//		// Exit goroutine gracefully
//	}()
func (em *ExitManager) Notify() <-chan struct{} {
	return em.notifyCh
}

// Shutdown programmatically initiates the shutdown process:
//
//  1. Close the notification channel (Notify())
//  2. Wait for all shutdown locks to be released
//  3. Execute cleanup functions in reverse registration order
//  4. Exit the process
//
// Shutdown has the same effect as receiving SIGINT/SIGTERM.
// The method is non-blocking and is safe to call multiple times.
// The programatic shutdown might be useful for handling critical errors
// more gracefully than a panic or direct exit.
func (em *ExitManager) Shutdown() {
	em.mu.Lock()
	select {
	case <-em.shutdown:
	default:
		em.notified = true
		close(em.shutdown)
	}
	em.mu.Unlock()
}

// RegisterCleanup registers a function to execute during shutdown.
//
// Cleanup functions execute in LIFO order after all shutdown locks
// are released. The exit manager waits for all cleanup functions
// to complete before terminating (unless timeout expires).
// Cleanup functions should be quick, handle errors internally,
// and not acquire new shutdown locks.
//
// Example:
//
//	em.RegisterCleanup(func() {
//		log.Println("Closing database...")
//		db.Close()
//	})
func (em *ExitManager) RegisterCleanup(f func()) {
	em.mu.Lock()
	em.cleanups = append(em.cleanups, f)
	em.mu.Unlock()
}

// NotifyContext returns a context registered to be cancelled on shutdown.
//
// Integrates with Go's context cancellation patterns where the returned
// cancel function should still be called to free resources.
// If shutdown has already begun, the context is already cancelled.
// You can consider context cancellation as an alternative way to stop
// long-running operations rather than the Notify() channel. The pattern
// is similar to [signal.NotifyContext], but the context will also receive
// the shutdown signal with calls to [ExitManager.Shutdown].
//
// Example:
//
//	ctx, cancel := em.NotifyContext(context.Background())
//	defer cancel()
//
//	select {
//	case <-ctx.Done():
//		// Shutdown initiated
//	case <-workComplete:
//		// Normal completion
//	}
func (em *ExitManager) NotifyContext(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)

	em.mu.RLock()
	if em.notified {
		em.mu.RUnlock()
		cancel()
		return ctx, cancel
	}
	em.mu.RUnlock()

	done := make(chan struct{})
	once := sync.Once{}
	cleanup := func() {
		once.Do(func() {
			close(done)
			cancel()
		})
	}

	go func() {
		select {
		case <-done:
		case <-ctx.Done():
			cleanup()
		case <-em.Notify():
			cleanup()
		}
	}()

	return ctx, cleanup
}

// WithCancel provides the same functionality as [ExitManager.NotifyContext].
func (em *ExitManager) WithCancel(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := em.NotifyContext(ctx)
	return ctx, cancel
}

// httpExitManager defines the interface for HTTP server shutdown coordination.
// This allows the base exit manager to coordinate with HTTP-specific shutdown logic
// without creating a direct dependency on the HTTP exit manager package.
type httpExitManager interface {
	Shutdown()
	Done() <-chan struct{}
}

// RegisterHTTPExitManager registers and returns the HTTP exit manager for [net/http.Server]
// shutdown coordination.
//
// This integrates with [net/http.Server.Shutdown] shutdown coordination with the base exit manager,
// ensuring that HTTP servers are gracefully shutdown first, before checking locks
// and executing other cleanup functions. This is a global instance and can be accessed
// safely across concurrent goroutines.
//
// Example:
//
//	// Register and configure HTTP exit manager
//	em := exitmanager.Global()
//	httpEM := em.RegisterHTTPExitManager()
//	httpEM.RegisterHTTPServer(httpexit.HTTPServerShutdownConfig{
//		Server: server,
//		Timeout: 30 * time.Second,
//	})
//
//	// Wait for shutdown
//	<-em.Notify()
func (em *ExitManager) RegisterHTTPExitManager() *httpexit.HTTPExitManager {
	httpEM := httpexit.Global()

	em.mu.Lock()
	em.httpExitManager = httpEM
	em.mu.Unlock()

	go func() {
		<-httpEM.Notify()
		em.Shutdown()
	}()

	return httpEM
}

// listenForSignals handles signal registration and coordinates the shutdown process.
func (em *ExitManager) listenForSignals() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigCh:
	case <-em.shutdown:
	}

	em.once.Do(func() {
		em.mu.Lock()
		em.notified = true
		cleanups := append([]func(){}, em.cleanups...)
		httpEM := em.httpExitManager
		close(em.notifyCh)
		locks := em.locks
		em.mu.Unlock()

		done := make(chan struct{})
		startedCleanup := make(chan struct{})
		go func() {
			if httpEM != nil {
				<-httpEM.Done()
			}
			if locks > 0 {
				<-em.locksCh
			}
			close(startedCleanup)
			for i := len(cleanups) - 1; i >= 0; i-- {
				func() {
					defer func() {
						if r := recover(); r != nil {
							// Log the panic but continue with remaining cleanup functions
							// This ensures one panicking cleanup doesn't prevent others from running
							// Users should handle their own errors, but we provide graceful degradation
						}
					}()
					cleanups[i]()
				}()
			}
			close(done)
		}()

		// no timeout set
		if em.timeoutMode == TimeoutModeNone || em.timeout <= 0 {
			<-done
			em.exit.Exit(0)
			return
		}

		// timeout mode is graceful
		if em.timeoutMode == TimeoutModeGraceful {
			<-startedCleanup
			select {
			case <-done:
				em.exit.Exit(0)
			case <-time.After(em.timeout):
				em.exit.Exit(1)
			}
			return
		}

		// timeout mode is forceful
		select {
		case <-done:
			em.exit.Exit(0)
		case <-time.After(em.timeout):
			em.exit.Exit(1)
		}
	})
}
