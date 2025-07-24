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

// TimeoutConfig sets configuration for handling timeouts.
type TimeoutConfig struct {
	Soft time.Duration // Wait for graceful completion
	Hard time.Duration // Force exit after this total time
}

// ExitManager manages graceful shutdowns for applications.
// It provides mechanisms to coordinate shutdown signals, manage in-flight operations,
// and execute cleanup functions in an orderly manner.
type ExitManager struct {
	mu         *sync.Mutex
	cond       *sync.Cond
	notified   bool
	notifyCh   chan struct{}
	locks      int
	timeouts   TimeoutConfig
	signalOnce sync.Once
	cleanups   []func()
	inFlight   int
}

var (
	once    sync.Once
	manager *ExitManager
)

// New will return a singleton ExitManager instance.
// Only the first call to New will create and register the exit manager.
// The manager automatically starts listening for process SIGINT and SIGTERM signals.
func New() *ExitManager {
	once.Do(func() {
		em := &ExitManager{
			mu:       &sync.Mutex{},
			notifyCh: make(chan struct{}),
			timeouts: TimeoutConfig{},
		}
		em.cond = sync.NewCond(em.mu)
		go em.listenForSignals()
		manager = em
	})
	return manager
}

// SetTimeout updates the shutdown timeout for the exit manager. The timeout determines how long to wait for all locks to be
// released and cleanup functions to complete before forcefully exiting. Time timeout is only supported for
// By default, there is no timeout, giving processes unlimited time to clean up.
// Setting timeout to 0 or less disables timeout.
func (em *ExitManager) SetTimeouts(timeouts TimeoutConfig) {
	em.mu.Lock()
	em.timeouts = timeouts
	em.mu.Unlock()
}

// AcquireShutdownLock attempts to acquire a shutdown lock from the exit manager, and will return an error if the exit manager
// is already notified to shutdown. Multiple locks can be held simultaneously across different goroutines and processes. Each
// successful AcquireShutdownLock call must be paired with ReleaseShutdownLock. This lock prevents shutdown to occur until all
// critical operations are complete.
func (em *ExitManager) AcquireShutdownLock() error {
	em.mu.Lock()
	if em.notified {
		em.mu.Unlock()
		return errors.New("exit manager has been notified: process shutdown")
	} else {
		em.locks++
		em.mu.Unlock()
		return nil
	}
}

// ReleaseShutdownLock decrements the shutdown lock counter. When the lock count reaches zero and the exit manager has been notified,
// it broadcasts to waiting goroutines that shutdown can proceed. This should be called to release every lock acquired with
// AcquireShutdownLock.
func (em *ExitManager) ReleaseShutdownLock() {
	em.mu.Lock()
	if em.locks > 0 {
		em.locks--
	}
	if em.locks == 0 && em.notified {
		em.cond.Broadcast()
	}
	em.mu.Unlock()
}

// Notify returns a receive-only channel that is closed when the exit manager receives a shutdown signal (SIGINT or SIGTERM).
// This can be used to detect shutdown events across different parts of the application.
// The channel is closed once when the first shutdown signal is received.
func (em *ExitManager) Notify() <-chan struct{} {
	return em.notifyCh
}

// Shutdown provides the signal interrupt to the current process initating the exit manager process.
// This works for linux compabatible operating systems (does not work for Windows).
func (em *ExitManager) Shutdown() error {
	p, _ := os.FindProcess(os.Getpid())
	_ = p.Signal(os.Interrupt)
	return nil
}

// WithCancel returns a context that is cancelled when the exit manager receives a shutdown signal.
// If the exit manager has already recieved a shutdown signal, a cancelled context will be returned.
// The cancel function should still be called to clean as with context.WithCancel.
func (em *ExitManager) WithCancel(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	if em.notified {
		cancel()
		return ctx, cancel
	}

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

// RegisterCleanup registers a cleanup function to be executed during shutdown.
// Cleanup functions are executed in LIFO (Last In, First Out) order during the shutdown process.
// The exit manager waits for all cleanup functions to complete before terminating, unless the configured timeout expires.
func (em *ExitManager) RegisterCleanup(f func()) {
	em.mu.Lock()
	defer em.mu.Unlock()
	em.cleanups = append(em.cleanups, f)
}

// HTTPHealthCheckMiddleware provides an http.Handler to handle shutdown HTTP health check endpoint.
// You can register this to inform load balancers that they are no longer taking requests on health check endpoints.
// This is differ
func (em *ExitManager) HTTPHealthCheckMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-em.Notify():
				http.Error(w, "Service Unavailable: shutting down", http.StatusServiceUnavailable)
				return
			default:
				next.ServeHTTP(w, r)
			}
		})
	}
}

// HTTPGracefulShutdownMiddleware provides http.Handler to handle requests gracefully across http services.
// If withCancel is true, requests will be cancelled on exit manager notify event.
// This differs from http.Server shutdown which ...
func (em *ExitManager) HTTPGracefulShutdownMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			em.mu.Lock()
			if em.notified {
				em.mu.Unlock()
				http.Error(w, "Service Unavailable: shutting down", http.StatusServiceUnavailable)
				return
			}
			em.inFlight++
			em.mu.Unlock()

			defer func() {
				em.mu.Lock()
				em.inFlight--
				if em.inFlight == 0 && em.notified {
					em.cond.Broadcast()
				}
				em.mu.Unlock()
			}()

			next.ServeHTTP(w, r)
		})
	}
}

// HTTPGracefulShutdownMiddleware provides http.Handler to handle requests gracefully across http services.
// If withCancel is true, requests will be cancelled on exit manager notify event.
// This differs from http.Server shutdown which ...
func (em *ExitManager) HTTPGracefulShutdownWithCancelMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			em.mu.Lock()
			if em.notified {
				em.mu.Unlock()
				http.Error(w, "Service Unavailable: shutting down", http.StatusServiceUnavailable)
				return
			}
			em.inFlight++
			em.mu.Unlock()

			defer func() {
				em.mu.Lock()
				em.inFlight--
				if em.inFlight == 0 && em.notified {
					em.cond.Broadcast()
				}
				em.mu.Unlock()
			}()

			ctx, cancel := em.WithCancel(r.Context())
			defer cancel()

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// listenForSignals registers signal handler and manages the shutdown process.
func (em *ExitManager) listenForSignals() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	em.signalOnce.Do(func() {
		em.mu.Lock()
		em.notified = true
		close(em.notifyCh)
		em.mu.Unlock()

		softDone := make(chan struct{})
		hardDone := make(chan struct{})
		go func() {
			for em.locks > 0 {
				em.cond.Wait()
			}
			for em.inFlight > 0 {
				em.cond.Wait()
			}
			close(softDone)
			em.runCleanups()
			close(hardDone)
		}()

		// no timeouts
		timeouts := em.timeouts
		if timeouts.Hard <= 0 && timeouts.Soft <= 0 {
			<-hardDone
			os.Exit(0)
		}

		// without hard timeout
		if timeouts.Hard <= 0 {
			<-softDone
			select {
			case <-hardDone:
				os.Exit(0)
			case <-time.After(timeouts.Soft):
				os.Exit(1)
			}
		}

		// using hard timeout
		select {
		case <-hardDone:
			os.Exit(0)
		case <-time.After(timeouts.Hard):
			os.Exit(1)
		}
	})
}

// runCleanups executes all registered cleanup functions in last in, first out (LIFO) order.
func (em *ExitManager) runCleanups() {
	em.mu.Lock()
	cleanups := append([]func(){}, em.cleanups...)
	slices.Reverse(cleanups)
	em.mu.Unlock()
	for _, f := range cleanups {
		f()
	}
}
