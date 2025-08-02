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
	closed      bool
}

func (er *exitRecorder) Exit(code int) {
	er.mu.Lock()
	defer er.mu.Unlock()
	er.nExits++
	er.code = code
	if !er.closed {
		close(er.hasExitedCh)
		er.closed = true
	}
}

func (er *exitRecorder) Done() <-chan struct{} {
	return er.hasExitedCh
}

// checkNoExit verifies the exit manager has not exited.
func checkNoExit(t *testing.T, em *ExitManager) {
	t.Helper()

	er, ok := (em.exit).(*exitRecorder)
	if !ok {
		t.Fatalf("required em.hijackExitHandler() for testing")
	}
	er.mu.Lock()
	defer er.mu.Unlock()

	if er.closed {
		t.Fatalf("exit manager was expected to not exit, but exited")
	}
	if er.nExits > 0 {
		t.Fatalf("exit manager was expected to not exit, but exited %d times", er.nExits)
	}
}

// checkExitCode verifies the exit manager has exited with the expected code.
func checkExitCode(t *testing.T, em *ExitManager, code int) {
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

		err := em.AcquireShutdownLock()
		if err != nil {
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
			err := em.AcquireShutdownLock()
			if err != nil {
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
			err := em.AcquireShutdownLock()
			if err != nil {
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

		checkExitCode(t, em, 0)
	})

	t.Run("lock acquired after shutdown returns an error", func(t *testing.T) {
		em := testExitManager(t)

		em.Shutdown()
		err := em.AcquireShutdownLock()
		if err == nil {
			t.Fatalf("expected lock acquisition to fail after shutdown started, got nil error")
		}

		select {
		case <-em.exit.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected exit manager to have exited after shutdown started")
		}

		checkExitCode(t, em, 0)
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

	t.Run("timeout mode none does not affect normal exit", func(t *testing.T) {
		em := testExitManager(t)
		em.SetTimeout(TimeoutModeNone, 50*time.Millisecond)

		// Shutdown should exit normally
		em.Shutdown()
		select {
		case <-em.exit.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected exit manager to have exited after shutdown started")
		}

		checkExitCode(t, em, 0)
	})

	t.Run("timeout mode none waits indefinitely for locks", func(t *testing.T) {
		em := testExitManager(t)
		em.SetTimeout(TimeoutModeNone, 50*time.Millisecond)

		// Acquire lock
		_ = em.AcquireShutdownLock()
		em.Shutdown()

		// Should wait indefinitely, not timeout
		select {
		case <-em.exit.Done():
			t.Fatal("expected exit manager to wait indefinitely, but it exited")
		case <-time.After(100 * time.Millisecond):
		}

		checkNoExit(t, em)
	})

	t.Run("timeout mode graceful applies timeout only to cleanup", func(t *testing.T) {
		em := testExitManager(t)
		em.SetTimeout(TimeoutModeGraceful, 50*time.Millisecond)

		// Acquire lock
		_ = em.AcquireShutdownLock()

		// Add slow cleanup
		em.RegisterCleanup(func() {
			time.Sleep(100 * time.Millisecond)
		})

		em.Shutdown()

		// Should wait for lock release (no timeout here)
		select {
		case <-em.exit.Done():
			t.Fatal("expected exit manager to wait for lock release")
		case <-time.After(25 * time.Millisecond):
			// Expected - still waiting for lock
		}

		// Release lock - now cleanup should start and timeout
		em.ReleaseShutdownLock()
		select {
		case <-em.exit.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected exit manager to have exited after cleanup timeout")
		}

		checkExitCode(t, em, 1)
	})

	t.Run("timeout mode forceful applies timeout to entire shutdown", func(t *testing.T) {
		em := testExitManager(t)
		em.SetTimeout(TimeoutModeForceful, 50*time.Millisecond)

		// Acquire lock
		_ = em.AcquireShutdownLock()
		em.Shutdown()

		// Should timeout even while waiting for locks
		select {
		case <-em.exit.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected exit manager to have timed out during lock wait")
		}

		checkExitCode(t, em, 1)
	})

	t.Run("timeout mode graceful allows normal completion", func(t *testing.T) {
		em := testExitManager(t)
		em.SetTimeout(TimeoutModeGraceful, 50*time.Millisecond)

		// Fast cleanup
		em.RegisterCleanup(func() {
			time.Sleep(20 * time.Millisecond)
		})

		em.Shutdown()
		select {
		case <-em.exit.Done():
		case <-time.After(150 * time.Millisecond):
			t.Fatal("expected exit manager to have exited normally")
		}

		checkExitCode(t, em, 0)
	})

	t.Run("timeout mode forceful allows normal completion", func(t *testing.T) {
		em := testExitManager(t)
		em.SetTimeout(TimeoutModeForceful, 100*time.Millisecond)

		// Fast cleanup
		em.RegisterCleanup(func() {
			time.Sleep(20 * time.Millisecond)
		})

		em.Shutdown()
		select {
		case <-em.exit.Done():
		case <-time.After(150 * time.Millisecond):
			t.Fatal("expected exit manager to have exited normally")
		}

		checkExitCode(t, em, 0)
	})

	t.Run("zero timeout duration disables timeout regardless of mode", func(t *testing.T) {
		em := testExitManager(t)
		em.SetTimeout(TimeoutModeForceful, 0) // Zero duration should disable timeout

		// Acquire lock
		_ = em.AcquireShutdownLock()
		em.Shutdown()

		// Should wait indefinitely despite forceful mode
		select {
		case <-em.exit.Done():
			t.Fatal("expected exit manager to wait indefinitely with zero timeout")
		case <-time.After(100 * time.Millisecond):
		}

		// Release lock to allow cleanup
		em.ReleaseShutdownLock()
		select {
		case <-em.exit.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected exit manager to have exited after locks released")
		}

		checkExitCode(t, em, 0)
	})

	t.Run("negative timeout duration disables timeout", func(t *testing.T) {
		em := testExitManager(t)
		em.SetTimeout(TimeoutModeForceful, -5*time.Second)

		// Acquire lock
		_ = em.AcquireShutdownLock()
		em.Shutdown()

		// Should wait indefinitely despite negative timeout
		select {
		case <-em.exit.Done():
			t.Fatal("expected exit manager to wait indefinitely with negative timeout")
		case <-time.After(100 * time.Millisecond):
		}

		// Release lock to allow cleanup
		em.ReleaseShutdownLock()
		select {
		case <-em.exit.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected exit manager to have exited after locks released")
		}

		checkExitCode(t, em, 0)
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

		checkExitCode(t, em, 0)
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

		checkExitCode(t, em, 0)
	})
}

func TestNotifyContext(t *testing.T) {
	t.Parallel()

	t.Run("context cancellation waits for notified shutdown", func(t *testing.T) {
		em := testExitManager(t)
		ctx, cancel := em.NotifyContext(context.Background())
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

		checkExitCode(t, em, 0)
	})

	t.Run("context is returned cancelled on notified exit manager", func(t *testing.T) {
		em := testExitManager(t)

		// Shutdown immediately exits
		em.Shutdown()

		// NotifyContext should return a cancelled context
		ctx, cancel := em.NotifyContext(context.Background())
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

		checkExitCode(t, em, 0)
	})

	t.Run("mutliple contexts cancelled", func(t *testing.T) {
		em := testExitManager(t)
		ctx1, cancel1 := em.NotifyContext(context.Background())
		ctx2, cancel2 := em.NotifyContext(context.Background())
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

		checkExitCode(t, em, 0)
	})

	t.Run("parent context cancellation", func(t *testing.T) {
		em := testExitManager(t)

		// Create a parent context that can be cancelled
		parentCtx, parentCancel := context.WithCancel(context.Background())
		defer parentCancel()

		// Child context is registered with the exit manager
		ctx, cancel := em.NotifyContext(parentCtx)
		t.Cleanup(cancel)

		// Child context is not cancelled initially
		select {
		case <-ctx.Done():
			t.Fatal("context should not be cancelled initially")
		case <-time.After(10 * time.Millisecond):
		}

		// Cancel parent context
		parentCancel()

		// Child context is cancelled on parent cancellation
		select {
		case <-ctx.Done():
			if ctx.Err() != context.Canceled {
				t.Fatalf("expected context.Canceled, got %v", ctx.Err())
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatal("context should be cancelled when parent context is cancelled")
		}
		// verify the exit manager is not shutdown from parent context cancellation
		select {
		case <-em.Notify():
			t.Fatal("exit manager should not be shutdown from parent context cancellation")
		case <-time.After(10 * time.Millisecond):
		}

		checkNoExit(t, em)
	})
}
