// Package exitmanager provides graceful shutdown coordination for Go applications.
//
// Basic usage:
//
//	func main() {
//		em := exitmanager.Global()
//
//		// Register cleanup
//		em.RegisterCleanup(func() {
//		    log.Println("Cleaning up...")
//		})
//
//		// Handles multiple go-routinues
//		for i := 0; i < 3; i++ {
//			go doWork(i)
//	   	}
//
//		// Call Shutdown once all go-routines have started
//		time.Sleep(10 * time.Millisecond)
//		em.Shutdown()
//	}
//
//	func doWork(i int) {
//		// Protect critical operation
//		if err := em.AcquireShutdownLock(); err != nil {
//			return // Handle shutdown in progress
//		}
//		defer em.ReleaseShutdownLock()
//
//		// Do critical work...
//		time.Sleep(i * time.Second)
//		log.Printf("Print i: %d\n", i)
//
//		return
//	}
package exitmanager

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"
)

// ExitManager manages graceful shutdowns for the application.
//
// The exit manager will:
//   - Listen for SIGINT/SIGTERM or Shutdown() signals
//   - Prevents shutdown during critical operations via locks
//   - Executes cleanup functions in reverse registration order
//   - Supports timeout-based forced exits
//
// The exit manager also can notify go-routines of initiated shutdown,
// return the number of locks held, and cancel any registered contexts on
// shutdown.
type ExitManager struct {
	mu       *sync.RWMutex
	locks    int
	notified bool
	timeout  time.Duration
	locksCh  chan struct{}
	notifyCh chan struct{}
	shutdown chan struct{}
	serverWg *sync.WaitGroup
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
		locksCh:  make(chan struct{}),
		notifyCh: make(chan struct{}),
		shutdown: make(chan struct{}),
		serverWg: &sync.WaitGroup{},
		exit:     osExitHandler{},
	}
}

// exitHandler provides the interface to exit the application.
// This interface type can be replace or hijacked for testing.
type exitHandler interface {
	Exit(code int)
}

// osExitHandler implements the exitHandler with "os" handling.
type osExitHandler struct{}

func (ehi osExitHandler) Exit(code int) {
	os.Exit(code)
}

// Global returns the global ExitManager instance.
//
// The first call to Global creates a singleton exit manager and starts listening for
// SIGINT and SIGTERM signals. The exit manager to be safely accessed from anywhere in
// the application.
//
// Example:
//
//	em := exitmanager.Global()
//	em.RegisterCleanup(func() {
//	    log.Println("Shutting down...")
//	})
func Global() *ExitManager {
	once.Do(func() {
		em := newExitManager()
		go em.listenForSignals()
		manager = em
	})
	return manager
}

// SetTimeout configures the maximum time to wait during shutdown before forcefully exiting.
//
// The timeout applies to the entire shutdown process, including:
//   - Waiting for all shutdown locks to be released
//   - Executing all registered cleanup functions
//
// If timeout is <= 0, the process will wait indefinitely for graceful shutdown.
// If timeout expires, the process exits with exit code 1, potentially interrupting cleanup.
func (em *ExitManager) SetTimeout(timeout time.Duration) {
	em.mu.Lock()
	em.timeout = timeout
	em.mu.Unlock()
}

// Locks returns the current number of active shutdown locks.
//
// This can be useful for debugging or monitoring the shutdown state.
// A non-zero value indicates that critical operations are still in progress
// and preventing shutdown from completing.
//
// Example:
//
//	if em.Locks() > 0 {
//	    log.Printf("Waiting for %d operations to complete", em.Locks())
//	}
func (em *ExitManager) Locks() int {
	em.mu.Lock()
	defer em.mu.Unlock()
	return em.locks
}

// AcquireShutdownLock prevents the shutdown process from proceeding until lock is released.
// The method returns an error if the shutdown process has already been initiated, in which
// case the calling function should abort the operation handling gracefully.
//
// This method should be called before starting any critical operation that must complete
// before the process can safely exit. Each successful call must be paired with exactly one
// call to ReleaseShutdownLock(). Multiple locks can be retrived by the method where the
// exit manager will wait until all locks are released before exiting.
//
// Example #1
//
//	if err := em.AcquireShutdownLock(); err != nil {
//	    return err
//	}
//	defer em.ReleaseShutdownLock()
//
//	// Perform critical operation...
//
// Example #2
//
//	func doWork(i int) {
//		if err := em.AcquireShutdownLock(); err != nil {
//			return
//		}
//		defer em.ReleaseShutdownLock()
//		// Do critical work...
//	}
//
//	for i := 0; i < 3; i++ {
//		go doWork(i)
//	}
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
// This must be called exactly once for each successful call to AcquireShutdownLock().
// If this was the last lock and shutdown has been initiated, the shutdown process
// will proceed to execute cleanup functions.
//
// It is safe to call this method even if no locks were acquired, though this
// represents a programming error.
//
// Example:
//
//	if err := em.AcquireShutdownLock(); err != nil {
//	    return // Shutdown in process, avoids critical operation
//	}
//	defer em.ReleaseShutdownLock()
//	// ... perform critical operation
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

// Notify returns a receive-only channel that closes when shutdown is initiated.
//
// The channel is closed exactly once when the first shutdown signal is received,
// either by SIGINT/SIGTERM or via Shutdown(). This allows different parts of the
// application to detect and respond to shutdown events.
//
// The channel remains closed for the lifetime of the exit manager, so multiple
// readers can safely select on it.
//
// Example:
//
//	go func() {
//	    <-em.Notify()
//	    log.Println("Shutdown signal received, stopping background work")
//	    // ... cleanup logic
//	}()
func (em *ExitManager) Notify() <-chan struct{} {
	return em.notifyCh
}

// RegisterHTTPServerOnShutdown registers a http.Server to Shutdown on notified event.
// The exit manager will wait until all servers have shutdown successfully before shutting down.
// If the error return by the call to server.Shutdown is other than http.ErrServerClosed, handleErr should take appropriate action.
// If the exit manager is already notified, the server will be shutdown immediately. Timeout can be ignored if 0 or less.
func (em *ExitManager) RegisterHTTPServerOnShutdown(server *http.Server, timeout time.Duration, handleErr func(err error)) {
	if handleErr == nil {
		handleErr = func(err error) {}
	}

	// consider when exit manager is already notified
	em.mu.RLock()
	if em.notified {
		em.mu.RUnlock()

		ctx := context.Background()
		if timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(context.Background(), timeout)
			defer cancel()
		}

		err := server.Shutdown(ctx)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			handleErr(err)
		}
		return
	}
	em.mu.RUnlock()

	// configure graceful exit for server for when notified
	em.serverWg.Add(1)
	go func() {
		<-em.Notify()

		ctx := context.Background()
		if timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(context.Background(), timeout)
			defer cancel()
		}

		err := server.Shutdown(ctx)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			handleErr(err)
		}
		em.serverWg.Done()
	}()
}

// Shutdown programmatically initiates the shutdown process.
// This has the same effect as receiving a SIGINT or SIGTERM signal.
// The method can be called multiple times safely where subsequent calls have no effect.
//
// On shutdown, the exit manager will:
//  1. Close the notification channel returned by Notify()
//  2. Wait for all shutdown locks to be released
//  3. Execute cleanup functions in reverse registration order
//  4. Exit the process
//
// Note the Shutdown method is non-blocking so you will have to wait if called on the main
// routine.
func (em *ExitManager) Shutdown() {
	em.mu.Lock()
	select {
	case <-em.shutdown:
	default:
		close(em.shutdown)
	}
	em.mu.Unlock()
}

// RegisterCleanup registers a function to be executed during shutdown.
//
// Cleanup functions are executed in LIFO (Last In, First Out) order after all
// shutdown locks have been released. The exit manager waits for all cleanup
// functions to complete before terminating the process, unless a timeout expires.
//
// Cleanup functions should:
//   - Complete quickly and not block indefinitely
//   - Handle their own error recovery
//   - Not acquire new shutdown locks
//
// Example:
//
//	// Register cleanup functions
//	em.RegisterCleanup(func() {
//	    log.Println("Closing database connections...")
//	    db.Close()
//	})
//
//	em.RegisterCleanup(func() {
//	    log.Println("Stopping HTTP server...")
//	    server.Shutdown(ctx)
//	})
func (em *ExitManager) RegisterCleanup(f func()) {
	em.mu.Lock()
	defer em.mu.Unlock()
	em.cleanups = append(em.cleanups, f)
}

// WithCancel returns a context that is automatically cancelled when shutdown begins.
//
// This provides integration with Go's context cancellation patterns. The returned
// cancel function should still be called to free resources, similar to context.WithCancel.
//
// If shutdown has already been initiated, the returned context will already be cancelled.
//
// Example:
//
//	ctx, cancel := em.WithCancel(context.Background())
//	defer cancel()
//
//	select {
//	case <-ctx.Done():
//	    // Shutdown initiated, cleanup and exit
//	case <-workComplete:
//	    // Normal completion
//	}
func (em *ExitManager) WithCancel(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	em.mu.RLock()
	if em.notified {
		em.mu.RUnlock()
		cancel()
		return ctx, cancel
	}
	em.mu.RUnlock()

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

// listenForSignals registers signal handler and manages the shutdown process.
func (em *ExitManager) listenForSignals() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigCh:
	case <-em.shutdown:
	}

	em.mu.Lock()
	em.notified = true
	close(em.notifyCh)
	em.mu.Unlock()

	done := make(chan struct{})
	go func() {
		for em.locks > 0 {
			<-em.locksCh
		}
		em.serverWg.Wait()
		cleanups := append([]func(){}, em.cleanups...)
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
	}

	// timeout set
	select {
	case <-done:
		em.exit.Exit(0)
	case <-time.After(em.timeout):
		em.exit.Exit(1)
	}
}
