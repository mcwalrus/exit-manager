package exitmanager

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"
)

var (
	managers []*ExitManager
	once     sync.Once
	l        *sync.Mutex
)

// ExitManager manages graceful shutdowns.
type ExitManager struct {
	mu         *sync.RWMutex
	cond       *sync.Cond
	locks      int
	notified   bool
	notifyCh   chan struct{}
	timeout    time.Duration
	signalOnce sync.Once
	cleanups   []func()
	inFlight   int // Track in-flight requests
}

// Register ExitManager on multiple events. I don't think this is needed by leaving for now.
func Register(mgr *ExitManager) {
	once.Do(func() {
		l = &sync.Mutex{}
	})
	l.Lock()
	defer l.Unlock()
	managers = append(managers, mgr)
}

// NewExitManager creates a new ExitManager with the given timeout.
func New(timeout time.Duration) *ExitManager {
	em := &ExitManager{
		mu:       &sync.RWMutex{},
		notifyCh: make(chan struct{}),
		timeout:  timeout,
	}
	em.cond = sync.NewCond(em.mu)
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
		return false
	}
	defer em.mu.RUnlock()
	em.locks++
	return true
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

// WithCancelOnShutdown cancels context which can signal shutdown across via context.WithCancel(ctx).
func (em *ExitManager) WithCancelOnShutdown(ctx context.Context) context.Context {
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

// HttpHealthCheckMiddleware provides an http.Handler to handle shutdown HTTP health check endpoint.
// You can register this to inform load balancers that they are no longer taking requests on health check endpoints.
// This is differ
func (em *ExitManager) HttpHealthCheckMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-em.Notify():
				http.Error(w, "Service Unavailable: shutting down", http.StatusServiceUnavailable)
				return
			default:
			}
			next.ServeHTTP(w, r)
		})
	}
}

// HttpRequestMiddleware provides http.Handler to handle requests gracefully across http services.
func (em *ExitManager) HttpRequestMiddleware() func(http.Handler) http.Handler {
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

			ctx := em.WithCancelOnShutdown(r.Context())
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
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

// Helper to run all cleanup functions in FILO order.
func (em *ExitManager) runCleanups() {
	em.mu.Lock()
	cleanups := append([]func(){}, em.cleanups...) // copy to avoid holding lock during execution
	slices.Reverse(cleanups)
	em.mu.Unlock()
	for _, f := range cleanups {
		f()
	}
}
