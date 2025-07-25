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

// ExitManager manages graceful shutdowns for programs.
// It provides mechanisms to coordinate shutdown once all AcquireShutdownLocks are released.
// Once they are released, the manager executes registered cleanup functions before exiting.
type ExitManager struct {
	mu       *sync.Mutex
	locks    int
	notified bool
	timeout  time.Duration
	locksCh  chan struct{}
	notifyCh chan struct{}
	shutdown chan struct{}
	cleanups []func()
}

var (
	once    sync.Once
	manager *ExitManager
)

// Global will return a singleton ExitManager instance.
// The first call to Global will create and register the exit manager.
// The manager automatically starts listening for process SIGINT and SIGTERM signals.
func Global() *ExitManager {
	once.Do(func() {
		em := &ExitManager{
			mu:       &sync.Mutex{},
			locksCh:  make(chan struct{}),
			notifyCh: make(chan struct{}),
			shutdown: make(chan struct{}),
		}
		go em.listenForSignals()
		manager = em
	})
	return manager
}

// SetTimeout updates the shutdown timeout for the exit manager. The timeout determines how long to wait for all locks to be
// released and cleanup functions to complete before forcefully exiting. Time timeout is only supported as a hard limit and may
// exit while ShutdownLocks are held. By default, there is no timeout, processes have unlimited time to clean up. A timeout less
// than 0 disables timeout.
func (em *ExitManager) SetTimeout(timeout time.Duration) {
	em.mu.Lock()
	em.timeout = timeout
	em.mu.Unlock()
}

// Locks returns the number of locks acquired by exit manager.
func (em *ExitManager) Locks() int {
	em.mu.Lock()
	defer em.mu.Unlock()
	return em.locks
}

// AcquireShutdownLock attempts to acquire a shutdown lock, returning an error if the exit manager is already notified to shutdown.
// Shutdown locks prevents shutdown to occur until all critical operations are complete. Multiple locks can be held across routines
// simultaneously. Each successful AcquireShutdownLock call must be paired with ReleaseShutdownLock.
func (em *ExitManager) AcquireShutdownLock() error {
	em.mu.Lock()
	if em.notified {
		em.mu.Unlock()
		return errors.New("exit manager: notified for process shutdown")
	} else {
		em.locks++
		em.mu.Unlock()
		return nil
	}
}

// ReleaseShutdownLock realases the shutdown lock counter.
// This should be called to release every lock acquired with AcquireShutdownLock.
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

// Notify returns a receive-only channel that is closed when the exit manager receives a shutdown signal.
// This method can be used to detect shutdown events across different parts of the application.
// The channel is closed once when the first shutdown signal is received.
func (em *ExitManager) Notify() <-chan struct{} {
	return em.notifyCh
}

// Shutdown signals the exit manager to exit the process via shutdown sequence.
// Method can be called multiple times without any affect.
func (em *ExitManager) Shutdown() {
	em.mu.Lock()
	select {
	case <-em.shutdown:
	default:
		close(em.shutdown)
	}
	em.mu.Unlock()
}

// RegisterCleanup registers a cleanup function to be executed during shutdown.
// Cleanup functions are executed in LIFO (Last In, First Out) order during the shutdown process.
// The exit manager waits for all cleanup functions to complete before terminating, unless the configured timeout expires.
func (em *ExitManager) RegisterCleanup(f func()) {
	em.mu.Lock()
	defer em.mu.Unlock()
	em.cleanups = append(em.cleanups, f)
}

// WithCancel returns a context that is cancelled when the exit manager is notified to shutdown.
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

// listenForSignals registers signal handler and manages the shutdown process.
func (em *ExitManager) listenForSignals() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// wait for signal
	select {
	case <-sigCh:
	case <-em.shutdown:
	}

	// notify listeners / manager
	em.mu.Lock()
	em.notified = true
	close(em.notifyCh)
	em.mu.Unlock()

	// begin exit process, completed on close(done)
	done := make(chan struct{})
	go func() {
		for em.locks > 0 {
			<-em.locksCh
		}
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
		os.Exit(0)
	}

	// timeout set
	select {
	case <-done:
		os.Exit(0)
	case <-time.After(em.timeout):
		os.Exit(1)
	}
}
