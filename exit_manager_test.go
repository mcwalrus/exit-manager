package exitmanager

import (
	"context"
	"fmt"
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

func TestPanicHandling(t *testing.T) {
	t.Parallel()

	t.Run("WithShutdownLock panics are recovered and re-raised", func(t *testing.T) {
		em := testExitManager(t)

		// This should panic after releasing the lock
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic to be re-raised")
			} else if r != "test panic" {
				t.Fatalf("unexpected panic value: %v", r)
			}
		}()

		err := em.WithShutdownLock(func() error {
			panic("test panic")
		})

		// Should not reach here
		t.Fatalf("function should have panicked, but returned error: %v", err)
	})

	t.Run("WithShutdownLock releases lock even when panicking", func(t *testing.T) {
		em := testExitManager(t)

		// Acquire lock first to verify it increases
		err := em.AcquireShutdownLock()
		if err != nil {
			t.Fatalf("unexpected error acquiring lock: %v", err)
		}
		if em.Locks() != 1 {
			t.Fatalf("expected 1 lock, got %d", em.Locks())
		}

		// WithShutdownLock should panic but still release its lock
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expected panic")
				}
			}()
			_ = em.WithShutdownLock(func() error {
				if em.Locks() != 2 {
					t.Fatalf("expected 2 locks during function execution, got %d", em.Locks())
				}
				panic("test panic")
			})
		}()

		// Should be back to 1 lock (the manually acquired one)
		if em.Locks() != 1 {
			t.Fatalf("expected 1 lock after panic recovery, got %d", em.Locks())
		}

		// Release the manually acquired lock
		em.ReleaseShutdownLock()
		if em.Locks() != 0 {
			t.Fatalf("expected 0 locks after release, got %d", em.Locks())
		}
	})

	t.Run("cleanup function panics don't prevent other cleanups", func(t *testing.T) {
		em := testExitManager(t)

		executed := make([]string, 0)
		var mu sync.Mutex

		// Register cleanups that will execute in reverse order (LIFO)
		em.RegisterCleanup(func() {
			mu.Lock()
			executed = append(executed, "cleanup1")
			mu.Unlock()
		})

		em.RegisterCleanup(func() {
			mu.Lock()
			executed = append(executed, "cleanup2_panic")
			mu.Unlock()
			panic("cleanup panic")
		})

		em.RegisterCleanup(func() {
			mu.Lock()
			executed = append(executed, "cleanup3")
			mu.Unlock()
		})

		// Shutdown should complete despite panic
		em.Shutdown()

		select {
		case <-em.exit.Done():
		case <-time.After(500 * time.Millisecond):
			t.Fatal("expected exit manager to complete shutdown despite cleanup panic")
		}

		checkExitCode(t, em, 0)

		// Verify all cleanups executed (in reverse order)
		mu.Lock()
		expected := []string{"cleanup3", "cleanup2_panic", "cleanup1"}
		mu.Unlock()

		if len(executed) != len(expected) {
			t.Fatalf("expected %d cleanups to execute, got %d: %v", len(expected), len(executed), executed)
		}

		for i, exp := range expected {
			if executed[i] != exp {
				t.Fatalf("cleanup execution order mismatch at index %d: expected %s, got %s", i, exp, executed[i])
			}
		}
	})

	t.Run("multiple cleanup function panics don't prevent graceful exit", func(t *testing.T) {
		em := testExitManager(t)

		executed := make([]string, 0)
		var mu sync.Mutex

		// Register multiple panicking cleanups
		for i := 0; i < 5; i++ {
			name := fmt.Sprintf("cleanup%d", i)
			em.RegisterCleanup(func() {
				mu.Lock()
				executed = append(executed, name)
				mu.Unlock()
				if name != "cleanup2" { // Only one should not panic
					panic("cleanup panic: " + name)
				}
			})
		}

		em.Shutdown()

		select {
		case <-em.exit.Done():
		case <-time.After(500 * time.Millisecond):
			t.Fatal("expected exit manager to complete shutdown despite multiple cleanup panics")
		}

		checkExitCode(t, em, 0)

		// All cleanups should have executed
		mu.Lock()
		if len(executed) != 5 {
			t.Fatalf("expected 5 cleanups to execute, got %d: %v", len(executed), executed)
		}
		mu.Unlock()
	})

	t.Run("cleanup panics with timeout mode graceful", func(t *testing.T) {
		em := testExitManager(t)
		em.SetTimeout(TimeoutModeGraceful, 100*time.Millisecond)

		executed := false
		em.RegisterCleanup(func() {
			executed = true
			time.Sleep(50 * time.Millisecond) // Sleep within timeout
			panic("cleanup panic")
		})

		em.Shutdown()

		select {
		case <-em.exit.Done():
		case <-time.After(200 * time.Millisecond):
			t.Fatal("expected exit manager to complete shutdown")
		}

		if !executed {
			t.Fatal("cleanup function should have executed")
		}

		// Should exit successfully despite panic (panic doesn't affect timeout)
		checkExitCode(t, em, 0)
	})

	t.Run("cleanup panics with timeout mode forceful", func(t *testing.T) {
		em := testExitManager(t)
		em.SetTimeout(TimeoutModeForceful, 50*time.Millisecond)

		em.RegisterCleanup(func() {
			panic("cleanup panic")
		})

		em.Shutdown()

		select {
		case <-em.exit.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected exit manager to complete shutdown")
		}

		// Should exit successfully despite panic
		checkExitCode(t, em, 0)
	})
}

// triggerForcefulExit simulates receiving an additional signal in forceful exit mode
// by closing the forcefulExit channel. This is a test helper to verify forceful exit behavior.
func triggerForcefulExit(em *ExitManager) {
	em.mu.Lock()
	defer em.mu.Unlock()
	select {
	case <-em.forcefulExit:
	default:
		close(em.forcefulExit)
	}
}

func TestMultipleSignalsMode(t *testing.T) {
	t.Parallel()

	t.Run("set multiple signals mode standard", func(t *testing.T) {
		em := testExitManager(t)
		em.SetMultipleSignalsMode(MultipleSignalsModeEnsureLocksRelease)

		// Standard mode should allow normal graceful shutdown
		em.Shutdown()
		select {
		case <-em.exit.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected exit manager to have exited after shutdown started")
		}

		checkExitCode(t, em, 0)
	})

	t.Run("set multiple signals mode ignore", func(t *testing.T) {
		em := testExitManager(t)
		em.SetMultipleSignalsMode(MultipleSignalsModeIgnore)

		// Ignore mode should allow normal graceful shutdown
		em.Shutdown()
		select {
		case <-em.exit.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected exit manager to have exited after shutdown started")
		}

		checkExitCode(t, em, 0)
	})

	t.Run("set multiple signals mode forceful exit", func(t *testing.T) {
		em := testExitManager(t)
		em.SetMultipleSignalsMode(MultipleSignalsModeForcefulExit)

		// Forceful exit mode should allow normal graceful shutdown initially
		em.Shutdown()
		select {
		case <-em.exit.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected exit manager to have exited after shutdown started")
		}

		checkExitCode(t, em, 0)
	})

	t.Run("standard mode continues graceful shutdown with locks", func(t *testing.T) {
		em := testExitManager(t)
		em.SetMultipleSignalsMode(MultipleSignalsModeEnsureLocksRelease)

		// Acquire a lock
		_ = em.AcquireShutdownLock()
		em.Shutdown()

		// Should wait for lock release (standard behavior)
		select {
		case <-em.exit.Done():
			t.Fatal("expected exit manager to wait for lock release")
		case <-time.After(50 * time.Millisecond):
		}

		// Release lock - should complete gracefully
		em.ReleaseShutdownLock()
		select {
		case <-em.exit.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected exit manager to have exited after lock release")
		}

		checkExitCode(t, em, 0)
	})

	t.Run("ignore mode continues graceful shutdown with locks", func(t *testing.T) {
		em := testExitManager(t)
		em.SetMultipleSignalsMode(MultipleSignalsModeIgnore)

		// Acquire a lock
		_ = em.AcquireShutdownLock()
		em.Shutdown()

		// Should wait for lock release (ignore mode doesn't change this)
		select {
		case <-em.exit.Done():
			t.Fatal("expected exit manager to wait for lock release")
		case <-time.After(50 * time.Millisecond):
		}

		// Release lock - should complete gracefully
		em.ReleaseShutdownLock()
		select {
		case <-em.exit.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected exit manager to have exited after lock release")
		}

		checkExitCode(t, em, 0)
	})

	t.Run("forceful exit mode exits immediately when triggered", func(t *testing.T) {
		em := testExitManager(t)
		em.SetMultipleSignalsMode(MultipleSignalsModeForcefulExit)

		// Acquire a lock to prevent normal shutdown
		_ = em.AcquireShutdownLock()
		em.Shutdown()

		// Should wait for lock release initially
		select {
		case <-em.exit.Done():
			t.Fatal("expected exit manager to wait for lock release")
		case <-time.After(50 * time.Millisecond):
		}

		// Simulate receiving an additional signal (forceful exit)
		triggerForcefulExit(em)

		// Should exit immediately with code 1, bypassing lock release
		select {
		case <-em.exit.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected exit manager to have exited immediately after forceful exit")
		}

		checkExitCode(t, em, 1)
	})

	t.Run("forceful exit mode exits immediately even during cleanup", func(t *testing.T) {
		em := testExitManager(t)
		em.SetMultipleSignalsMode(MultipleSignalsModeForcefulExit)

		// Register a slow cleanup function
		em.RegisterCleanup(func() {
			time.Sleep(200 * time.Millisecond)
		})

		em.Shutdown()

		// Wait a bit to ensure cleanup starts
		time.Sleep(50 * time.Millisecond)

		// Simulate receiving an additional signal (forceful exit)
		triggerForcefulExit(em)

		// Should exit immediately, potentially before cleanup completes
		select {
		case <-em.exit.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected exit manager to have exited immediately after forceful exit")
		}

		checkExitCode(t, em, 1)
		// Cleanup may or may not have executed depending on timing
	})

	t.Run("standard mode allows normal completion even after mode change", func(t *testing.T) {
		em := testExitManager(t)
		em.SetMultipleSignalsMode(MultipleSignalsModeForcefulExit)
		em.SetMultipleSignalsMode(MultipleSignalsModeEnsureLocksRelease) // Change to standard

		_ = em.AcquireShutdownLock()
		em.Shutdown()

		// Should wait for lock release (standard mode)
		select {
		case <-em.exit.Done():
			t.Fatal("expected exit manager to wait for lock release in standard mode")
		case <-time.After(50 * time.Millisecond):
		}

		// Release lock - should complete gracefully
		em.ReleaseShutdownLock()
		select {
		case <-em.exit.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected exit manager to have exited after lock release")
		}

		checkExitCode(t, em, 0)
	})

	t.Run("mode can be changed during shutdown", func(t *testing.T) {
		em := testExitManager(t)
		em.SetMultipleSignalsMode(MultipleSignalsModeEnsureLocksRelease)

		_ = em.AcquireShutdownLock()
		em.Shutdown()

		// Change mode during shutdown
		em.SetMultipleSignalsMode(MultipleSignalsModeForcefulExit)

		// Trigger forceful exit
		triggerForcefulExit(em)

		// Should exit immediately
		select {
		case <-em.exit.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected exit manager to have exited immediately after forceful exit")
		}

		checkExitCode(t, em, 1)
	})

	t.Run("ignore mode continues graceful shutdown normally", func(t *testing.T) {
		em := testExitManager(t)
		em.SetMultipleSignalsMode(MultipleSignalsModeIgnore)

		_ = em.AcquireShutdownLock()
		em.Shutdown()

		// Should wait for lock release (ignore mode)
		select {
		case <-em.exit.Done():
			t.Fatal("expected exit manager to wait for lock release in ignore mode")
		case <-time.After(50 * time.Millisecond):
		}

		// Release lock - should complete gracefully
		em.ReleaseShutdownLock()
		select {
		case <-em.exit.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected exit manager to have exited after lock release")
		}

		checkExitCode(t, em, 0)
	})

	t.Run("default mode is standard", func(t *testing.T) {
		em := testExitManager(t)

		// Don't set any mode - should default to standard
		_ = em.AcquireShutdownLock()
		em.Shutdown()

		// Should wait for lock release (standard mode is default)
		select {
		case <-em.exit.Done():
			t.Fatal("expected exit manager to wait for lock release in standard mode")
		case <-time.After(50 * time.Millisecond):
		}

		// Release lock - should complete gracefully
		em.ReleaseShutdownLock()
		select {
		case <-em.exit.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected exit manager to have exited after lock release")
		}

		checkExitCode(t, em, 0)
	})
}
