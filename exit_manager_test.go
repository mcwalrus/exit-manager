package exitmanager

import (
	"context"
	"sync"
	"testing"
	"time"
)

// testExitManager returns an ExitManager instance for use in tests.
// This version is "hijacked" so it won't actually call os.Exit, and instead
// lets you inspect the exit code via a test exit handler. It also ensures
// cleanup after the test is done.
func testExitManager(t *testing.T) *ExitManager {
	t.Helper()

	em := newExitManager()
	go em.listenForSignals()
	em.hijackExitHandler()

	t.Cleanup(func() {
		em.Shutdown()
		for em.Locks() > 0 {
			em.ReleaseShutdownLock()
		}
	})

	return em
}

func (em *ExitManager) hijackExitHandler() {
	em.exit = &exitRecorder{
		mu:          &sync.Mutex{},
		hasExitedCh: make(chan struct{}),
	}
}

// exitRecorder is a test helper which implements the exitHandler interface.
// It records exit attempts (code and count) instead of terminating the process,
// allowing tests to verify exit behaviour. The exitRecorder can be used to notify
// the test that the exit manager has exited with Done().
type exitRecorder struct {
	mu          *sync.Mutex
	code        int
	nExits      int
	hasExitedCh chan (struct{})
}

func (er *exitRecorder) Exit(code int) {
	er.mu.Lock()
	er.nExits++
	er.code = code
	close(er.hasExitedCh)
	er.mu.Unlock()
}

func (er *exitRecorder) Done() <-chan struct{} {
	return er.hasExitedCh
}

// checkManagerExitCode verifies the exit manager has exited with the expected code.
func checkManagerExitCode(t *testing.T, em *ExitManager, code int) {
	t.Helper()

	er, ok := (em.exit).(*exitRecorder)
	if !ok {
		t.Fatalf("required em.hijackExitHandler() for testing")
	}
	er.mu.Lock()
	defer er.mu.Unlock()

	if er.nExits == 0 {
		t.Fatalf("exit manager has not been recorded to exit yet")
	}
	if er.nExits > 1 {
		t.Fatalf("exit manager was expected to exit once, but exited %d times", er.nExits)
	}
	if er.code != code {
		t.Fatalf("exit manager returned different exit code: %d != %d (recorded, expected)", er.code, code)
	}
}

func TestAcquireShutdownLock(t *testing.T) {
	t.Parallel()

	t.Run("successful lock acquisition and release", func(t *testing.T) {
		em := testExitManager(t)

		if err := em.AcquireShutdownLock(); err != nil {
			t.Fatalf("expected lock acquisition to succeed, got error: %v", err)
		}
		if em.Locks() != 1 {
			t.Fatalf("expected 1 lock, got %d", em.Locks())
		}

		em.ReleaseShutdownLock()
		if em.Locks() != 0 {
			t.Fatalf("expected 0 locks after release, got %d", em.Locks())
		}
	})

	t.Run("multiple lock acquisitions held in parallel", func(t *testing.T) {
		em := testExitManager(t)

		for i := 0; i < 3; i++ {
			if err := em.AcquireShutdownLock(); err != nil {
				t.Fatalf("expected lock acquisition to succeed, got error: %v", err)
			}
		}
		if em.Locks() != 3 {
			t.Fatalf("expected %d locks, got %d", 3, em.Locks())
		}
	})

	t.Run("multiple locks block cleanup until all locks are released", func(t *testing.T) {
		em := testExitManager(t)

		// Acquiring three locks
		for i := 0; i < 3; i++ {
			if err := em.AcquireShutdownLock(); err != nil {
				t.Fatalf("expected lock acquisition to succeed, got error: %v", err)
			}
		}
		em.Shutdown()

		// Releasing two locks
		for i := 0; i < 2; i++ {
			em.ReleaseShutdownLock()
			select {
			case <-em.exit.Done():
				t.Fatal("cleanup should blocked until all locks are released")
			case <-time.After(100 * time.Millisecond):
			}
		}

		// Release final lock
		em.ReleaseShutdownLock()
		select {
		case <-em.exit.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected exit manager to have exited after all locks are released")
		}

		checkManagerExitCode(t, em, 0)
	})

	t.Run("lock acquired after shutdown returns an error", func(t *testing.T) {
		em := testExitManager(t)

		em.Shutdown()
		if err := em.AcquireShutdownLock(); err == nil {
			t.Fatalf("expected lock acquisition to fail after shutdown started, got nil error")
		}

		select {
		case <-em.exit.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected exit manager to have exited after shutdown started")
		}

		checkManagerExitCode(t, em, 0)
	})

	t.Run("concurrent lock acquisitions is thread safe", func(t *testing.T) {
		em := testExitManager(t)

		var wg sync.WaitGroup
		n := 50
		wg.Add(n)

		errs := make(chan error, n)
		for i := 0; i < n; i++ {
			go func() {
				err := em.AcquireShutdownLock()
				errs <- err
				wg.Done()
			}()
		}
		wg.Wait()

		close(errs)
		success := 0
		for err := range errs {
			if err == nil {
				success++
			}
		}

		if success != n {
			t.Fatalf("expected %d successful lock acquisitions, got %d", n, success)
		}
		if em.Locks() != n {
			t.Fatalf("expected %d locks, got %d", n, em.Locks())
		}

		wg.Add(n)
		errs = make(chan error, n)
		for i := 0; i < n; i++ {
			go func() {
				em.ReleaseShutdownLock()
				wg.Done()
			}()
		}
		wg.Wait()

		if em.Locks() != 0 {
			t.Fatalf("expected 0 locks, got %d", em.Locks())
		}
	})
}

func TestTimeout(t *testing.T) {
	t.Parallel()

	t.Run("timeout does not affect normal exit", func(t *testing.T) {
		em := testExitManager(t)
		em.SetTimeout(50 * time.Millisecond)

		// Shutdown should exit normally
		em.Shutdown()
		select {
		case <-em.exit.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected exit manager to have exited after shutdown started")
		}

		checkManagerExitCode(t, em, 0)
	})

	t.Run("timeout is respected on shutdown for held locks", func(t *testing.T) {
		em := testExitManager(t)
		em.SetTimeout(50 * time.Millisecond)

		// Acquire lock
		_ = em.AcquireShutdownLock()
		em.Shutdown()

		// Shutdown should block until timeout expires
		select {
		case <-em.exit.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected exit manager to have exited after shutdown started")
		}

		checkManagerExitCode(t, em, 1)
	})

	t.Run("timeout is respected on shutdown for blocked cleanup", func(t *testing.T) {
		em := testExitManager(t)
		em.SetTimeout(50 * time.Millisecond)

		// Block cleanup
		em.RegisterCleanup(func() {
			time.Sleep(200 * time.Millisecond)
		})

		// Shutdown should block until timeout expires
		em.Shutdown()
		select {
		case <-em.exit.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected exit manager to have exited after shutdown started")
		}

		checkManagerExitCode(t, em, 1)
	})
}

func TestNotify(t *testing.T) {
	t.Parallel()

	t.Run("wait for Shutdown()", func(t *testing.T) {
		em := testExitManager(t)

		select {
		case <-em.Notify():
			t.Fatalf("needed to wait for Shutdown() by exit handler")
		case <-time.After(100 * time.Millisecond):
		}

		em.Shutdown()
	})

	t.Run("Notify() is closed after Shutdown()", func(t *testing.T) {
		em := testExitManager(t)

		em.Shutdown()
		select {
		case <-em.Notify():
		case <-time.After(10 * time.Millisecond):
			t.Fatal("Notify() channel was not closed after Shutdown()")
		}

		select {
		case <-em.exit.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected exit manager to have exited after shutdown started")
		}

		checkManagerExitCode(t, em, 0)
	})

	t.Run("multiple Notify() listeners in parallel", func(t *testing.T) {
		em := testExitManager(t)

		// timeout to verify all routines were notified in time
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		// go routines to verify listeners are notified after shutdown
		wg := &sync.WaitGroup{}
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func() {
				select {
				case <-em.Notify():
				case <-ctx.Done():
					time.Sleep(100 * time.Millisecond)
				}
				wg.Done()
			}()
		}

		em.Shutdown()
		wg.Wait()

		// Context should not be cancelled before all routines were notified
		select {
		case <-ctx.Done():
			t.Fatalf("context cancelled before all routinues were notified")
		default:
		}

		// Exit manager should exit after shutdown
		select {
		case <-em.exit.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected exit manager to have exited after shutdown started")
		}

		checkManagerExitCode(t, em, 0)
	})
}

func TestWithCancel(t *testing.T) {
	t.Parallel()

	t.Run("context cancellation waits for notified shutdown", func(t *testing.T) {
		em := testExitManager(t)
		ctx, cancel := em.WithCancel(context.Background())
		t.Cleanup(cancel)

		// Context should not be cancelled before shutdown
		select {
		case <-ctx.Done():
			t.Fatal("context should not be cancelled before shutdown")
		case <-time.After(10 * time.Millisecond):
		}

		em.Shutdown()

		// Context should be cancelled after shutdown
		select {
		case <-ctx.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("context was not cancelled after shutdown")
		}

		// Exit manager should exit after shutdown
		select {
		case <-em.exit.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected exit manager to have exited after shutdown started")
		}

		checkManagerExitCode(t, em, 0)
	})

	t.Run("context is returned cancelled on notified exit manager", func(t *testing.T) {
		em := testExitManager(t)

		// Shutdown immediately exits
		em.Shutdown()

		// WithCancel should return a cancelled context
		ctx, cancel := em.WithCancel(context.Background())
		t.Cleanup(cancel)
		select {
		case <-ctx.Done():
		case <-time.After(10 * time.Millisecond):
			t.Fatalf("context should be immediately cancelled if shutdown already occurred")
		}

		// Exit manager should exit after shutdown
		select {
		case <-em.exit.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected exit manager to have exited after shutdown started")
		}

		checkManagerExitCode(t, em, 0)
	})

	t.Run("mutliple contexts cancelled", func(t *testing.T) {
		em := testExitManager(t)
		ctx1, cancel1 := em.WithCancel(context.Background())
		ctx2, cancel2 := em.WithCancel(context.Background())
		t.Cleanup(cancel1)
		t.Cleanup(cancel2)

		var wg sync.WaitGroup
		wg.Add(2)

		// go routines to verify contexts are cancelled after shutdown
		go func() {
			defer wg.Done()
			select {
			case <-ctx1.Done():
			case <-time.After(100 * time.Millisecond):
				t.Error("ctx1 was not cancelled after shutdown")
			}
		}()
		go func() {
			defer wg.Done()
			select {
			case <-ctx2.Done():
			case <-time.After(100 * time.Millisecond):
				t.Error("ctx2 was not cancelled after shutdown")
			}
		}()

		// Shutdown now
		em.Shutdown()
		wg.Wait()

		// verify exit manager exited after shutdown started
		select {
		case <-em.exit.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected exit manager to have exited after shutdown started")
		}

		checkManagerExitCode(t, em, 0)
	})
}
