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
	"io"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// ExitManager coordinates graceful application shutdowns.
// Use [Global] to get the singleton instance.
type ExitManager struct {
	mu             *sync.RWMutex
	locks          int
	notified       bool
	logger         *slog.Logger
	timeout        time.Duration
	timeoutMode    TimeoutMode
	mSignalsMode   MultipleSignalsMode
	locksCh        chan struct{}
	notifyCh       chan struct{}
	shutdown       chan struct{}
	forcefulExit   chan struct{}
	cleanups       []func()
	contextCancels []context.CancelFunc
	flush          func()
	exit           exitHandler
}

var (
	once              sync.Once
	manager           *ExitManager
	signalsMu         sync.RWMutex
	configuredSignals []os.Signal
)

// newExitManager returns a new exit manager instance.
func newExitManager() *ExitManager {
	return &ExitManager{
		mu:           &sync.RWMutex{},
		locksCh:      make(chan struct{}),
		notifyCh:     make(chan struct{}),
		shutdown:     make(chan struct{}),
		forcefulExit: make(chan struct{}),
		exit:         osExitHandler{},
		logger:       noopLogger(),
	}
}

// noopLogger returns a no-op logger that discards all output.
func noopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// exitHandler provides an interface for process termination.
// Allows replacement with test implementations for testing.
type exitHandler interface {
	Exit(code int)
	Done() <-chan struct{}
}

// osExitHandler implements os.Exit as the default handler.
type osExitHandler struct{}

func (ehi osExitHandler) Exit(code int) {
	os.Exit(code)
}
func (ehi osExitHandler) Done() <-chan struct{} {
	return nil
}

// SetSignals configures which signals the exit manager should listen for.
// This must be called before the first call to [Global]. If not called,
// the default signals SIGINT and SIGTERM will be used.
//
// Example:
//
//	exitmanager.SetSignals(syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
//	em := exitmanager.Global()
func SetSignals(signals ...os.Signal) {
	signalsMu.Lock()
	defer signalsMu.Unlock()
	configuredSignals = signals
}

// Global returns the singleton ExitManager instance.
//
// The first call registers signal handlers for the signals configured via
// [SetSignals], or SIGINT and SIGTERM by default if [SetSignals] was not called.
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
	TimeoutModeNone     TimeoutMode = iota // Default mode, which is no timeout (waits indefinitely)
	TimeoutModeGraceful                    // Timeout is only applied to the cleanup functions execution
	TimeoutModeForceful                    // Timeout is applied to the entire shutdown process
)

// SetTimeout configures the shutdown timeout behavior based on the specified mode.
// If timeout is 0 or negative, the timeout is disabled. The timeout is applied
// to the entire shutdown process.
//
// Example:
//
//	em.SetTimeout(exitmanager.TimeoutModeGraceful, 30*time.Second)
func (em *ExitManager) SetTimeout(mode TimeoutMode, timeout time.Duration) {
	em.mu.Lock()
	em.timeoutMode = mode
	em.timeout = timeout
	em.mu.Unlock()
}

// MultipleSignalsMode determines how the exit manager responds to
// additional shutdown signals (SIGINT/SIGTERM) received after the
// initial shutdown has been initiated.
type MultipleSignalsMode int

const (
	MultipleSignalsModeEnsureLocksRelease MultipleSignalsMode = iota // Additional signals will exit once all shutdown locks are released (can exit before cleanups completes)
	MultipleSignalsModeForcefulExit                                  // Additional signals cause immediate exit (bypasses locks and registered cleanups)
	MultipleSignalsModeIgnore                                        // Additional signals are ignored, graceful shutdown continues
)

// SetMultipleSignalsMode configures how the exit manager responds to
// additional shutdown signals received after shutdown has been initiated.
//
// Example:
//
//	em.SetMultipleSignalsMode(exitmanager.MultipleSignalsModeForcefulExit)
func (em *ExitManager) SetMultipleSignalsMode(mode MultipleSignalsMode) {
	em.mu.Lock()
	em.mSignalsMode = mode
	em.mu.Unlock()
}

// SetLogger configures the structured logger for the exit manager.
// The logger will be used to log shutdown process stages, lock operations,
// and HTTP server registration events with the subsystem "exit-manager".
// If logger is nil, a no-op logger will be used.
//
// If flush is provided, it will be called when the shutdown process
// completes. This is useful for third party loggers that need to be
// flushed to ensure all logs are written to the underlying writer.
// If flush is nil, no operation to flush will be performed.
func (em *ExitManager) SetLogger(logger *slog.Logger, flush func()) {
	em.mu.Lock()
	if logger != nil {
		em.logger = logger.With("subsystem", "exit-manager")
	} else {
		em.logger = noopLogger()
	}
	em.flush = flush
	em.mu.Unlock()
}

// Locks returns the current number of active shutdown locks.
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
		em.logger.Debug("shutdown lock acquired", "locks", em.locks)
		em.mu.Unlock()
		return nil
	}
}

// WithShutdownLock executes fn while holding a shutdown lock.
//
// Automatically acquires and releases the shutdown lock around
// the function execution. Returns ErrShutdownInProgress if
// shutdown has already been initiated. If fn panics, the method
// remembers to release the lock.
func (em *ExitManager) WithShutdownLock(fn func() error) (err error) {
	if err := em.AcquireShutdownLock(); err != nil {
		return err
	}
	defer em.ReleaseShutdownLock()

	defer func() {
		if r := recover(); r != nil {
			em.logger.Error("panic in WithShutdownLock", "panic", r)
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
		em.logger.Debug("shutdown lock released", "locks", em.locks)
	}
	if em.locks == 0 && em.notified {
		em.logger.Debug("all shutdown locks released")
		select {
		case <-em.locksCh:
		default:
			close(em.locksCh)
		}
	}
	em.mu.Unlock()
}

// Notify returns a read-only channel that closes when shutdown is initiated
// by the exit manager. This allows go-routines to register and handle the
// shutdown process if required. Note, the exit manager may not wait for the
// go-routine to complete before exiting. If this is a concern, use
// [RegisterCleanup] for complex cleanup operations.
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
//  2. Cancels all context registered with [NotifyContext]
//  3. Wait for all shutdown locks to be released
//  4. Execute cleanup functions in reverse registration order
//  5. Exit the process
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

	em.mu.Lock()
	if em.notified {
		em.mu.Unlock()
		cancel()
		return ctx, cancel
	}

	em.contextCancels = append(em.contextCancels, cancel)
	em.mu.Unlock()

	return ctx, cancel
}

// flushLogger flushes the logger if a flush function is provided.
// This is useful for third party loggers that need to be flushed
// to ensure all logs are written to the underlying writer.
func flushLogger(logger *slog.Logger, flush func()) {
	if flush != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					logger.Error("flush logger panicked", "panic", r)
				}
			}()
			flush()
		}()
	}
}

// listenForSignals handles signal registration and coordinates the shutdown process.
func (em *ExitManager) listenForSignals() {
	sigCh := make(chan os.Signal, 1)

	signalsMu.RLock()
	var signals []os.Signal
	if len(configuredSignals) > 0 {
		signals = make([]os.Signal, len(configuredSignals))
		copy(signals, configuredSignals)
	}
	signalsMu.RUnlock()

	if len(signals) == 0 {
		signals = []os.Signal{syscall.SIGINT, syscall.SIGTERM}
	}

	signal.Notify(sigCh, signals...)

	var shutdownSource string
	select {
	case <-sigCh:
		shutdownSource = "signal"
	case <-em.shutdown:
		shutdownSource = "programmatic"
	}

	em.mu.Lock()
	em.notified = true
	mode := em.mSignalsMode
	cleanups := append([]func(){}, em.cleanups...)
	contextCancels := append([]context.CancelFunc{}, em.contextCancels...)
	logger := em.logger
	flush := em.flush
	close(em.notifyCh)
	em.mu.Unlock()

	logger.Info("shutdown initiated",
		"source", shutdownSource,
		"cleanup_functions", len(cleanups),
		"context_cancels", len(contextCancels),
	)

	// Continue listening for additional signals
	done := make(chan struct{})
	startedCleanup := make(chan struct{})
	go func() {
		for range sigCh {
			switch mode {
			case MultipleSignalsModeIgnore:
				logger.Info("additional signal received, ignoring", "mode", "ignore")
			case MultipleSignalsModeEnsureLocksRelease:
				logger.Info("additional signal received, exit once shutdown locks are released", "mode", "ensure locks release")
				<-startedCleanup
				close(em.forcefulExit)
				return
			case MultipleSignalsModeForcefulExit:
				logger.Info("additional signal received, forcing immediate exit", "mode", "forceful")
				close(em.forcefulExit)
				return
			}
		}
	}()

	go func() {
		defer func() {
			logger.Info("shutdown completed")
			close(done)
		}()

		// Cancel all registered contexts first
		if len(contextCancels) > 0 {
			logger.Info("cancelling context cancels", "count", len(contextCancels))
			for _, cancel := range contextCancels {
				cancel()
			}
		}

		// Wait for shutdown locks to be released or forceful exit
		em.mu.RLock()
		locks := em.locks
		em.mu.RUnlock()

		if locks > 0 {
			logger.Info("waiting for shutdown locks to be released", "locks", locks)
			<-em.locksCh
		}

		close(startedCleanup)

		// Execute cleanup functions in reverse order (LIFO)
		if len(cleanups) > 0 {
			logger.Info("executing cleanup functions", "count", len(cleanups))
			for i := len(cleanups) - 1; i >= 0; i-- {
				func() {
					defer func() {
						if r := recover(); r != nil {
							logger.Error("cleanup function panicked", "panic", r)
						}
					}()
					cleanups[i]()
				}()
			}
			logger.Info("cleanup functions completed")
		}
	}()

	// no timeout set
	em.mu.RLock()
	timeoutMode := em.timeoutMode
	timeout := em.timeout
	em.mu.RUnlock()

	if timeoutMode == TimeoutModeNone || timeout <= 0 {
		select {
		case <-done:
			logger.Info("graceful shutdown completed")
			flushLogger(logger, flush)
			em.exit.Exit(0)
		case <-em.forcefulExit:
			logger.Info("forceful exit completed")
			flushLogger(logger, flush)
			em.exit.Exit(1)
		}
		return
	}

	// timeout mode is graceful
	if em.timeoutMode == TimeoutModeGraceful {
		<-startedCleanup
		select {
		case <-done:
			logger.Info("graceful shutdown completed")
			flushLogger(logger, flush)
			em.exit.Exit(0)
		case <-time.After(timeout):
			logger.Error("graceful shutdown timeout expired", "timeout", timeout)
			flushLogger(logger, flush)
			em.exit.Exit(1)
		case <-em.forcefulExit:
			logger.Info("forceful exit completed")
			flushLogger(logger, flush)
			em.exit.Exit(1)
		}
		return
	}

	// timeout mode is forceful
	select {
	case <-done:
		logger.Info("graceful shutdown completed")
		flushLogger(logger, flush)
		em.exit.Exit(0)
	case <-time.After(timeout):
		logger.Error("forceful shutdown timeout expired", "timeout", timeout)
		flushLogger(logger, flush)
		em.exit.Exit(1)
	case <-em.forcefulExit:
		logger.Info("forceful exit completed")
		flushLogger(logger, flush)
		em.exit.Exit(1)
	}
}
