package exitmanager

import (
	"context"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"
)

// Singleton logic
var (
	instance *ExitManager
	once     sync.Once
)

// ExitManager manages graceful shutdowns.
type ExitManager struct {
	mu         sync.RWMutex
	cond       *sync.Cond
	locks      int
	notified   bool
	notifyCh   chan struct{}
	timeout    time.Duration
	signalOnce sync.Once
	cleanups   []func()
}

// RegisterExitManager registers the singleton ExitManager. Can only be called once.
func Register(mgr *ExitManager) {
	once.Do(func() {
		instance = mgr
	})
}

// GetExitManager returns the registered singleton ExitManager, or nil if not registered.
func GetExitManager() *ExitManager {
	return instance
}

// NewExitManager creates a new ExitManager with the given timeout.
func New(timeout time.Duration) *ExitManager {
	em := &ExitManager{
		notifyCh: make(chan struct{}),
		timeout:  timeout,
	}
	em.cond = sync.NewCond(&em.mu)
	go em.listenForSignals()
	return em
}

// Update timeout.
func (em *ExitManager) SetTimeout(timeout time.Duration) {
	em.mu.Lock()
	defer em.mu.Unlock()
	em.timeout = timeout
}

// TryLock will soft lock the exit manager if the exit manager is not already exiting.
func (em *ExitManager) TryLock() bool {
	if em.isExiting() {
		defer em.mu.RUnlock()
		em.locks++
		return true
	} else {
		return false
	}
}

// Unlock will remove the soft lock to the exit manager.
func (em *ExitManager) Unlock() {
	em.mu.RLock()
	defer em.mu.RUnlock()
	if em.locks > 0 {
		em.locks--
	}
	if em.locks == 0 && em.notified {
		em.cond.Broadcast()
	}
}

// Cancel context which can signal shutdown across via context.WithCancel(ctx).
func (em *ExitManager) Cancel(ctx context.Context) context.Context {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		<-em.Notify()
		cancel()
	}()
	return ctx
}

func (em *ExitManager) Cleanup(f func()) {
	em.mu.Lock()
	defer em.mu.Unlock()
	em.cleanups = append(em.cleanups, f)
}

func (em *ExitManager) isExiting() bool {
	em.mu.Lock()
	defer em.mu.Unlock()
	return em.notified
}

// Notify can be watched on chan close event, indicating either signal has been recieved by the exit-manager.
func (em *ExitManager) Notify() <-chan struct{} {
	return em.notifyCh
}

func (em *ExitManager) listenForSignals() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh // Wait for signal

	em.signalOnce.Do(func() {
		em.mu.Lock()
		em.notified = true
		close(em.notifyCh) // Notify all listeners
		em.runCleanups()
		if em.locks == 0 {
			em.mu.Unlock()
			os.Exit(0)
		}
		// Wait for locks to be released or timeout
		done := make(chan struct{})
		go func() {
			for em.locks > 0 {
				em.cond.Wait()
			}
			close(done)
		}()
		timeout := em.timeout
		em.mu.Unlock()

		select {
		case <-done:
			os.Exit(0)
		case <-time.After(timeout):
			os.Exit(1)
		}
	})
}

// Helper to run all cleanup functions
func (em *ExitManager) runCleanups() {
	em.mu.Lock()
	cleanups := append([]func(){}, slices.Reverse(em.cleanups)...) // copy to avoid holding lock during execution
	em.mu.Unlock()
	for _, f := range cleanups {
		f()
	}
}
