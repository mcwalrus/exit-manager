package exitmanager

import (
	"context"
	"sync"
	"testing"
	"time"
)

// exitHandlerRecorder records the exit manager on leaving.
type exitHandlerRecorder struct {
	code      int
	hasExited bool
}

func (ehr *exitHandlerRecorder) Exit(code int) {
	ehr.code = code
	ehr.hasExited = true
}

func (em *ExitManager) hijackExitHandler() {
	em.exit = &exitHandlerRecorder{}
}

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
	em := newExitManager()
	go em.listenForSignals()
	em.hijackExitHandler()

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
}
