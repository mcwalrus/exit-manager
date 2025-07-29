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
	"slices"
	"sync"
	"syscall"
	"time"
)

// ExitManager coordinates graceful application shutdowns.
// Use Global() to get the singleton instance.
type ExitManager struct {
	mu       *sync.RWMutex
	locks    int
	notified bool
	once     *sync.Once
	timeout  time.Duration
	locksCh  chan struct{}
	notifyCh chan struct{}
	shutdown chan struct{}
	cleanups []func()
	exit     exitHandler
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
func Global() *ExitManager {
	once.Do(func() {
		em := newExitManager()
		go em.listenForSignals()
		manager = em
	})
	return manager
}

// SetTimeout configures the maximum shutdown duration before forced exit.
//
// If timeout is set to zero or less, the exit manager waits indefinitely for graceful shutdown.
// If timeout expires, exits with code 1 and can interrupt cleanup.
// The timeout covers the entire shutdown process:
//   - Waiting for shutdown locks to be released
//   - Executing cleanup functions
//
// Consider the time required for cleanup functions to ensure successful shutdown.
func (em *ExitManager) SetTimeout(timeout time.Duration) {
	em.mu.Lock()
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
func (em *ExitManager) AcquireShutdownLock() error {
	em.mu.Lock()
	if em.notified {
		em.mu.Unlock()
		return errors.New("exit manager: shutdown in progress")
	} else {
		em.locks++
		em.mu.Unlock()
		return nil
	}
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

// WithCancel returns a context registered to be cancelled on shutdown.
//
// Integrates with Go's context cancellation patterns where the returned
// cancel function should still be called to free resources.
// If shutdown has already begun, the context is already cancelled.
// You can consider context cancellation as an alternative way to stop
// long-running operations rather than the Notify() channel.
//
// Example:
//
//	ctx, cancel := em.WithCancel(context.Background())
//	defer cancel()
//
//	select {
//	case <-ctx.Done():
//		// Shutdown initiated
//	case <-workComplete:
//		// Normal completion
//	}
func (em *ExitManager) WithCancel(ctx context.Context) (context.Context, context.CancelFunc) {
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
		case <-em.Notify():
			cleanup()
		}
	}()

	return ctx, cleanup
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
		close(em.notifyCh)
		locks := em.locks
		em.mu.Unlock()

		done := make(chan struct{})
		go func() {
			if locks > 0 {
				<-em.locksCh
			}

			slices.Reverse(cleanups)
			for _, f := range cleanups {
				f()
			}
			close(done)
		}()

		// no timeout
		if em.timeout <= 0 {
			<-done
			em.exit.Exit(0)
			return
		}

		// timeout set
		select {
		case <-done:
			em.exit.Exit(0)
		case <-time.After(em.timeout):
			em.exit.Exit(1)
		}
	})
}
