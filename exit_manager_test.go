package exitmanager

import (
	"context"
	"sync"
	"testing"
	"time"
)

// newTestExitManager returns a hijacked test ExitManager.
// This avoids the case where the ExitManager will call os.Exit(code) during tests.
// You can also record the exit manager exit code with the registed exitHandlerRecorder set.
// While testing, please call em.Shutdown() to cleanup after em.listenForSignals().
func newTestExitManager() *ExitManager {
	em := newExitManager()
	go em.listenForSignals()
	em.hijackExitHandler()
	return em
}

func (em *ExitManager) hijackExitHandler() {
	em.exit = &exitHandlerRecorder{}
}

// exitHandlerRecorder records the exit manager on leaving.
type exitHandlerRecorder struct {
	code      int
	hasExited bool
}

func (ehr *exitHandlerRecorder) Exit(code int) {
	ehr.code = code
	ehr.hasExited = true
}

// checkManagerExitCode tests for the expected exit code from a hijacked exit manager.
func checkManagerExitCode(t *testing.T, em *ExitManager, code int) {
	t.Helper()

	ehr, ok := (em.exit).(*exitHandlerRecorder)
	if !ok {
		t.Fatalf("required em.hijackExitHandler() for test")
	}

	if !ehr.hasExited {
		t.Errorf("exit manager has not been recorded to exit yet...")
		t.FailNow()
	}

	if ehr.code != code {
		t.Errorf("exit manager returned different exit code: %d != %d (recorded, expected)", ehr.code, code)
		t.FailNow()
	}
}

func TestNotify(t *testing.T) {
	t.Parallel()

	t.Run("wait for Shutdown()", func(t *testing.T) {
		em := newTestExitManager()

		select {
		case <-em.Notify():
			t.Fatalf("needed to wait for Shutdown() by exit handler")
		case <-time.After(100 * time.Millisecond):
		}

		em.Shutdown()
	})

	t.Run("listen after Shutdown()", func(t *testing.T) {
		em := newTestExitManager()

		em.Shutdown()
		select {
		case <-em.Notify():
		case <-time.After(10 * time.Millisecond):
			t.Fatal("Notify() channel was not closed after Shutdown()")
		}

		checkManagerExitCode(t, em, 0)
	})

	t.Run("multiple listeners", func(t *testing.T) {
		em := newTestExitManager()

		wg := &sync.WaitGroup{}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func() {
				select {
				case <-em.Notify():
				case <-ctx.Done():
					time.Sleep(10 * time.Millisecond)
				}
				wg.Done()
			}()
		}

		em.Shutdown()
		wg.Wait()

		select {
		case <-ctx.Done():
			t.Errorf("context cancelled before all routinues were notified")
		default:
		}

		checkManagerExitCode(t, em, 0)
	})
}
